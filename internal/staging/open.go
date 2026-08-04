package staging

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"net/url"
	"strings"
	"time"

	"github.com/pressly/goose/v3"

	// The pure-Go SQLite driver, registered under the name "sqlite". It is pure
	// Go specifically so icelake's own runtime needs no cgo: a C toolchain is a
	// test-time dependency of this project, never a build-time one for a caller.
	_ "modernc.org/sqlite"

	"github.com/gvkhna/icelake/internal/errdef"
)

// migrationsFS carries the schema into the binary, which is what lets goose run
// with no files on disk at run time. There is no goose CLI anywhere in this
// project's workflow and no manual migration step for an operator to remember.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationsDir is the subdirectory of [migrationsFS] the migrations live in.
const migrationsDir = "migrations"

// driverName is the name modernc.org/sqlite registers itself under.
const driverName = "sqlite"

// busyTimeout is the "few seconds" ARCHITECTURE.md asks for. Its job is to turn
// the brief lock contention between two tables' inserts into a short wait inside
// the driver rather than an SQLITE_BUSY error surfacing to a caller who did
// nothing wrong. It is deliberately not configurable: a caller has no way to
// know a better number than "longer than any insert into a bounded table could
// possibly take", and exposing it would invite tuning a symptom.
const busyTimeout = 5 * time.Second

// The values the pragmas above must read back as. PRAGMA synchronous answers
// numerically: 2 is FULL.
const (
	wantJournalMode = "wal"
	wantSynchronous = 2
)

// Store is an open staging database: one file per process, shared by every
// table.
//
// Sharing is deliberate. SQLite allows one writer at a time, so two tables'
// inserts serialize against each other — which costs nothing here, because an
// insert is a single-row append into a table with one index and the thing that
// actually takes time in this design is the upload, which holds no SQLite lock
// at all. Against that non-cost sit one file to back up, one migration run at
// startup, and a replay pass that reads every table's pending rows at once
// rather than one file's worth at a time.
//
// A Store is safe for concurrent use: every method is a single statement or a
// single transaction, and the pool underneath serializes writers with the busy
// timeout absorbing the contention. Concurrency here means many goroutines in
// one process against one file. Two *processes* sharing one staging.db is
// unsafe for reasons no locking discipline inside this package could fix — both
// would replay the same rows and commit them twice — and is ruled out by the
// one-process, one-staging.db operational rule.
type Store struct {
	db   *sql.DB
	q    *Queries
	path string
}

// Open opens (creating it if absent) the staging database at path, configures
// it, and applies every pending migration before returning.
//
// The three pragmas ARCHITECTURE.md requires are set and then *asserted*, not
// fired and hoped for: WAL, so the replay and flush paths can read while the
// insert path writes without either blocking the other; synchronous=FULL rather
// than WAL's usual NORMAL, which is the one place this design pays for
// durability instead of speed, because NORMAL can lose the last transactions on
// a power loss and "the record is durable once Insert returns" is the entire
// reason this layer exists; and a busy timeout.
//
// Migrations run automatically on every open. There is no separate manual
// migration step, and no way to skip this. If a migration fails, Open fails and
// returns a [errdef.StagingError] with Kind [errdef.StagingKindMigrate] — icelake
// never serves writes against a database it is not sure of the schema state of.
// A failed migration is rolled back by goose and never recorded as applied, so
// the database is left exactly as the previous binary left it and stays usable
// by that binary. "Validating" the database means precisely one thing: goose's
// own migration call returned no error. It deliberately does not mean a data
// scan or an integrity check, which is the startup cost that scales with file
// size that this whole project exists to avoid.
//
// The caller owns the returned Store and must Close it.
func Open(ctx context.Context, path string) (*Store, error) {
	sub, err := fs.Sub(migrationsFS, migrationsDir)
	if err != nil {
		// Unreachable: the directory is embedded at build time.
		return nil, errdef.NewStagingError(errdef.StagingKindMigrate, path, "embedded migrations are unreadable", err)
	}
	return open(ctx, path, sub)
}

// open is Open with the migration filesystem supplied rather than embedded, so
// that the fail-loudly-on-a-broken-migration path can be exercised against a
// real database file without shipping a broken migration. The seam is
// unexported: production has exactly one migration set, the embedded one.
func open(ctx context.Context, path string, fsys fs.FS) (*Store, error) {
	if path == "" {
		return nil, errdef.NewStagingError(errdef.StagingKindOpen, path, "staging path is empty", nil)
	}
	// The driver splits a DSN at its first '?' to find query parameters, so a
	// path containing one would silently be truncated to everything before it.
	// Refused rather than mangled: a staging database opened at the wrong path
	// is an empty one, and an empty staging database looks exactly like a
	// successful start with nothing to replay.
	if strings.ContainsRune(path, '?') {
		return nil, errdef.NewStagingError(errdef.StagingKindOpen, path, "staging path contains '?', which the SQLite driver reads as the start of DSN parameters", nil)
	}

	db, err := sql.Open(driverName, dsn(path))
	if err != nil {
		return nil, errdef.NewStagingError(errdef.StagingKindOpen, path, "opening the staging database", err)
	}

	if err := configure(ctx, db, path); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := migrate(ctx, db, fsys, path); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db, q: New(db), path: path}, nil
}

// Close closes the staging database. It is safe to call once; the Store must
// not be used afterwards.
//
// Nothing is flushed or drained here: every method on Store commits before it
// returns, so there is never uncommitted work for Close to lose. Closing while
// a flush worker might still prune rows is a lifecycle error one layer up —
// Store.Close closes every writer first, then this.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return errdef.NewStagingError(errdef.StagingKindClose, s.path, "closing the staging database", err)
	}
	return nil
}

// Path returns the staging database's path, so an error raised by a layer above
// can name the same file this Store was opened against.
func (s *Store) Path() string { return s.path }

// dsn builds the driver DSN. The pragmas travel as DSN parameters rather than
// as statements issued after the fact because database/sql owns a pool of
// connections and hands out whichever is free: a pragma set once on one
// connection would leave every other connection running on the driver's
// defaults. In the DSN they are applied to every connection the pool ever
// opens, which is the only form of "this database is in WAL mode with
// synchronous=FULL" that is actually true of every statement.
//
// The DSN deliberately carries no "file:" prefix. With one, SQLite parses the
// path itself as a URI and percent-decodes it, so a perfectly ordinary path
// containing a '%' would open a different file; without one, the driver strips
// the query string and passes the remaining path through untouched.
func dsn(path string) string {
	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds()))
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	// Every transaction this package opens is a write transaction, and a
	// deferred BEGIN that upgrades to a write lock mid-transaction is the one
	// contention case the busy timeout cannot absorb: SQLite returns SQLITE_BUSY
	// immediately rather than waiting, because waiting could deadlock. Taking
	// the write lock at BEGIN turns that case back into an ordinary wait.
	q.Set("_txlock", "immediate")
	return path + "?" + q.Encode()
}

// configure verifies that the pragmas in the DSN actually took effect.
//
// They are read back on a single pooled connection, which is a fair proof for
// all of them: every connection is opened from the same DSN by the same driver,
// so a pragma that took on one took on all of them, and a pragma that silently
// did not would have failed here identically on whichever connection was
// checked. What this catches is the real failure — a driver or DSN form that
// accepts an unknown pragma silently, which would leave the durability
// guarantee resting on a string nothing ever validated.
func configure(ctx context.Context, db *sql.DB, path string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return errdef.NewStagingError(errdef.StagingKindOpen, path, "connecting to the staging database", err)
	}
	defer func() { _ = conn.Close() }()

	var journalMode string
	if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		return errdef.NewStagingError(errdef.StagingKindOpen, path, "reading back journal_mode", err)
	}
	if !strings.EqualFold(journalMode, wantJournalMode) {
		return errdef.NewStagingError(errdef.StagingKindOpen, path,
			fmt.Sprintf("journal_mode is %q, not %q", journalMode, wantJournalMode), nil)
	}

	var synchronous int
	if err := conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return errdef.NewStagingError(errdef.StagingKindOpen, path, "reading back synchronous", err)
	}
	if synchronous != wantSynchronous {
		return errdef.NewStagingError(errdef.StagingKindOpen, path,
			fmt.Sprintf("synchronous is %d, not %d (FULL)", synchronous, wantSynchronous), nil)
	}

	var busy int
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busy); err != nil {
		return errdef.NewStagingError(errdef.StagingKindOpen, path, "reading back busy_timeout", err)
	}
	if int64(busy) != busyTimeout.Milliseconds() {
		return errdef.NewStagingError(errdef.StagingKindOpen, path,
			fmt.Sprintf("busy_timeout is %dms, not %dms", busy, busyTimeout.Milliseconds()), nil)
	}

	return nil
}

// migrate applies every pending migration.
//
// goose is used strictly as a library. The provider is built and discarded
// here rather than kept on the Store: its Close closes the *sql.DB it was
// handed, which this package owns, and nothing after startup has any business
// changing the schema.
func migrate(ctx context.Context, db *sql.DB, fsys fs.FS, path string) error {
	p, err := goose.NewProvider(goose.DialectSQLite3, db, fsys, goose.WithLogger(goose.NopLogger()))
	if err != nil {
		return errdef.NewStagingError(errdef.StagingKindMigrate, path, "preparing the migration provider", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return errdef.NewStagingError(errdef.StagingKindMigrate, path, "applying migrations", err)
	}
	return nil
}
