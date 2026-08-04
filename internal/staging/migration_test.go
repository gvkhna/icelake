package staging

import (
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/gvkhna/icelake/internal/errdef"
)

// This file is TESTING.md scenario 4 — the startup migration test, and this
// milestone's proof. It sits in that document's second sanctioned category, a
// package-internal behaviour test of a layer that owns one local file, and it
// holds both of the claim shapes that category allows.
//
// Five of the six hold the first shape, with the caller-facing half proven end
// to end through the public API. TestFreshDatabaseIsMigratedOnOpen and
// TestReopeningAMigratedDatabaseIsIdempotent are held there only indirectly —
// every storage-facing scenario in that document opens icelake against a fresh
// staging.db and would fail outright if migrations had not run — and the limit
// is written down in TESTING.md's scenario 4 rather than left to look like
// coverage. TestOpenPutsTheDatabaseInWALMode is held by the root package's own
// inspection helpers, which read a live staging.db through a second, independent
// SQLite handle while a writer is running against it, which is a thing only WAL
// mode permits. TestOpenRefusesAPathItWouldMisread and
// TestAMigrationThatCannotApplyToThisDatabaseFailsTheOpen are held directly, by
// the StagingError and StagingErrorMigrate cases in the root package's
// errors_test.go, where the same two refusals come back out of icelake.Open as
// the public alias with their Kind readable. What this file adds to those is the
// part the public surface cannot see: which migration version is recorded, which
// tables and columns the file really has afterwards, and that a failed migration
// rolled back whole rather than half-applying.
//
// TestABrokenMigrationFailsTheOpen holds the second shape. Reaching it needs a
// deliberately broken migration substituted for the shipped set, which no public
// API can express and no caller can produce — the defect it guards against is a
// future migration of icelake's own that does not run, and the invariant it
// defends is that a process never starts against a database in an unknown state,
// because a half-migrated staging file is indistinguishable from a clean one and
// a clean one means "nothing to replay".
//
// Every test here runs against a real staging.db file on a real disk, opened by
// the real code path a process uses at startup, and asserts on what is actually
// in that file afterwards. Nothing is mocked and nothing runs in memory: the
// claim being proved is about what survives a process, so an in-memory database
// would prove nothing.
//
// The two claims, from the document:
//
//   - Starting against a fresh (or older-schema) staging.db applies all pending
//     goose migrations automatically, with no separate manual migration command
//     — confirmed by checking the resulting schema/version, not by any row-level
//     data check.
//   - Starting with a migration deliberately made to fail causes startup itself
//     to fail, rather than starting up against a database in an unknown state.

// latestVersion is the highest migration version this package ships. It is
// derived from the embedded files rather than written down, because a test that
// restated the number would pass unchanged when a new migration was added and
// never applied.
func latestVersion(t *testing.T) int64 {
	t.Helper()

	names, err := fs.Glob(migrationsFS, migrationsDir+"/*.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("no embedded migrations found: %v", err)
	}
	var max int64
	for _, name := range names {
		v := versionOf(t, filepath.Base(name))
		if v > max {
			max = v
		}
	}
	return max
}

// versionOf parses the leading version number out of a migration filename, the
// same way goose does.
func versionOf(t *testing.T, name string) int64 {
	t.Helper()

	var v int64
	for _, c := range name {
		if c < '0' || c > '9' {
			break
		}
		v = v*10 + int64(c-'0')
	}
	if v == 0 {
		t.Fatalf("migration %q has no leading version number", name)
	}
	return v
}

// inspect opens a second, independent handle on a staging database and hands it
// to fn. It is how these tests read the file back: through SQLite itself,
// against the bytes on disk, rather than through the Store that wrote them.
func inspect(t *testing.T, path string, fn func(db *sql.DB)) {
	t.Helper()

	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatalf("opening %s for inspection: %v", path, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing inspection handle: %v", err)
		}
	}()
	fn(db)
}

// appliedVersion reads the highest migration version goose has recorded as
// applied. A migration that failed is never recorded, so this is the fact that
// distinguishes "the schema is what this binary expects" from "something ran
// and we hope it worked".
func appliedVersion(t *testing.T, path string) int64 {
	t.Helper()

	var version int64
	inspect(t, path, func(db *sql.DB) {
		row := db.QueryRow("SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version WHERE is_applied = 1")
		if err := row.Scan(&version); err != nil {
			t.Fatalf("reading goose version: %v", err)
		}
	})
	return version
}

// tableColumns reads a table's column names straight out of SQLite's own
// schema, in declaration order.
func tableColumns(t *testing.T, path, table string) []string {
	t.Helper()

	var cols []string
	inspect(t, path, func(db *sql.DB) {
		rows, err := db.Query("SELECT name FROM pragma_table_info(?) ORDER BY cid", table)
		if err != nil {
			t.Fatalf("reading columns of %s: %v", table, err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scanning column name: %v", err)
			}
			cols = append(cols, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterating columns of %s: %v", table, err)
		}
	})
	return cols
}

// TestFreshDatabaseIsMigratedOnOpen is the first half of scenario 4: a process
// starting against a database that does not exist yet ends up with the full
// schema, with no migration step anyone had to run.
func TestFreshDatabaseIsMigratedOnOpen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "staging.db")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("test setup: %s already exists", path)
	}

	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got, want := appliedVersion(t, path), latestVersion(t); got != want {
		t.Errorf("applied migration version = %d, want %d", got, want)
	}

	wantStaging := []string{
		"id", "namespace", "table_name", "batch_key", "schema_fp",
		"payload", "byte_len", "created_at", "quarantined_at", "quarantine_error",
	}
	if got := tableColumns(t, path, "staging"); !slices.Equal(got, wantStaging) {
		t.Errorf("staging columns = %v, want %v", got, wantStaging)
	}
	wantSchemas := []string{"fp", "descriptor"}
	if got := tableColumns(t, path, "staging_schemas"); !slices.Equal(got, wantSchemas) {
		t.Errorf("staging_schemas columns = %v, want %v", got, wantSchemas)
	}
}

// TestOpenPutsTheDatabaseInWALMode proves the journal mode ARCHITECTURE.md
// requires is a property of the file afterwards, not just of the connection that
// set it. WAL is what lets the replay and flush paths read while the insert path
// writes, so a database that silently stayed in rollback-journal mode would
// quietly serialize the whole write path.
func TestOpenPutsTheDatabaseInWALMode(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "staging.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var mode string
	inspect(t, path, func(db *sql.DB) {
		if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
			t.Fatalf("reading journal_mode: %v", err)
		}
	})
	if mode != wantJournalMode {
		t.Errorf("journal_mode on disk = %q, want %q", mode, wantJournalMode)
	}
}

// TestReopeningAMigratedDatabaseIsIdempotent covers the "or older-schema" half
// of scenario 4's first claim from the other side: the ordinary restart, where
// every migration is already applied. It must not re-run anything, must not
// fail, and must not disturb what is staged.
func TestReopeningAMigratedDatabaseIsIdempotent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "staging.db")
	first, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	id, err := first.Append(t.Context(), fillRow(t, 1))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	versionAfterFirst := appliedVersion(t, path)

	second, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer closeStore(t, second)

	if got := appliedVersion(t, path); got != versionAfterFirst {
		t.Errorf("applied version after reopen = %d, want %d", got, versionAfterFirst)
	}
	rows, err := second.Rows(t.Context(), []int64{id})
	if err != nil {
		t.Fatalf("Rows after reopen: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("row %d did not survive the reopen: got %+v", id, rows)
	}
}

// TestABrokenMigrationFailsTheOpen is scenario 4's second claim, and the reason
// this milestone's proof is a migration test at all.
//
// The sequence is the real one an operator would hit: a database already in
// service, then a deploy whose migration set is broken. The assertions are that
// the open fails with a typed error rather than a message to grep, that the
// broken migration was not recorded as applied, and — the part that matters
// operationally — that the database is left exactly usable by the binary that
// was already running against it, with its staged records intact.
func TestABrokenMigrationFailsTheOpen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "staging.db")

	// A previous binary has been running against this database and has accepted
	// a record that is not yet flushed.
	before, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open before the bad deploy: %v", err)
	}
	id, err := before.Append(t.Context(), fillRow(t, 1))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := before.Close(); err != nil {
		t.Fatalf("Close before the bad deploy: %v", err)
	}
	goodVersion := appliedVersion(t, path)

	// The bad deploy: every shipped migration, plus one more that does not run.
	broken, err := open(t.Context(), path, brokenMigrations(t))
	if broken != nil {
		t.Errorf("open returned a usable store despite a failed migration")
		closeStore(t, broken)
	}
	if err == nil {
		t.Fatal("open with a broken migration returned no error")
	}
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("open error is not a StagingError: %#v", err)
	}
	if se.Kind != errdef.StagingKindMigrate {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindMigrate)
	}
	if se.Path != path {
		t.Errorf("StagingError.Path = %q, want %q", se.Path, path)
	}

	// The database is not in an unknown state: the broken migration is not
	// recorded, so the schema is still the one the previous binary expects.
	if got := appliedVersion(t, path); got != goodVersion {
		t.Errorf("applied version after the failed migration = %d, want %d", got, goodVersion)
	}
	// And not half-applied either. The broken migration's first statement did
	// succeed; the table it created must be gone, because the migration ran in
	// one transaction and that transaction rolled back.
	if tableExists(t, path, probeTable) {
		t.Errorf("table %s survives, so the failed migration was only half rolled back", probeTable)
	}

	// And the previous binary can still use it, with its record still staged.
	after, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open after the failed migration: %v", err)
	}
	defer closeStore(t, after)

	rows, err := after.Rows(t.Context(), []int64{id})
	if err != nil {
		t.Fatalf("Rows after the failed migration: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("staged row %d did not survive the failed migration: got %+v", id, rows)
	}
}

// TestAMigrationThatCannotApplyToThisDatabaseFailsTheOpen is the same claim
// against the migrations icelake actually ships, with no substituted filesystem
// anywhere: the shipped migration is run against a database that already has an
// unrelated table called staging, which is what a file that is not icelake's
// looks like. The point of having both tests is that this one can never be
// wrong about what the real migration set does.
func TestAMigrationThatCannotApplyToThisDatabaseFailsTheOpen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "someone-elses.db")
	inspect(t, path, func(db *sql.DB) {
		if _, err := db.Exec("CREATE TABLE staging (whatever TEXT)"); err != nil {
			t.Fatalf("test setup: %v", err)
		}
	})

	s, err := Open(t.Context(), path)
	if s != nil {
		t.Errorf("Open returned a usable store against a foreign database")
		closeStore(t, s)
	}
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("Open error is not a StagingError: %#v", err)
	}
	if se.Kind != errdef.StagingKindMigrate {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindMigrate)
	}
}

// TestOpenRefusesAPathItWouldMisread covers the one path shape the SQLite
// driver would silently truncate at its first '?', which would open an empty
// database somewhere else — and an empty staging database is indistinguishable
// from a clean start with nothing to replay, which is the worst possible way to
// lose staged records.
func TestOpenRefusesAPathItWouldMisread(t *testing.T) {
	t.Parallel()

	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "sta?ging.db"))
	if s != nil {
		t.Errorf("Open returned a store for a path it cannot represent")
		closeStore(t, s)
	}
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("Open error is not a StagingError: %#v", err)
	}
	if se.Kind != errdef.StagingKindOpen {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindOpen)
	}
}

// brokenMigrations returns the shipped migration set plus one deliberately
// broken migration after it.
//
// The shipped files are copied out of the embedded filesystem rather than
// retyped, so this fixture cannot drift from what the binary really applies: the
// only difference between it and production is the one file that does not run.
func brokenMigrations(t *testing.T) fs.FS {
	t.Helper()

	shipped, err := fs.Sub(migrationsFS, migrationsDir)
	if err != nil {
		t.Fatalf("reading embedded migrations: %v", err)
	}
	names, err := fs.Glob(shipped, "*.sql")
	if err != nil {
		t.Fatalf("listing embedded migrations: %v", err)
	}

	out := fstest.MapFS{}
	var max int64
	for _, name := range names {
		body, err := fs.ReadFile(shipped, name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		out[name] = &fstest.MapFile{Data: body}
		if v := versionOf(t, name); v > max {
			max = v
		}
	}

	// Two statements, the first of which succeeds and the second of which does
	// not — the shape a real migration goes wrong in, and the only shape that
	// can prove atomicity. A one-statement failure leaves nothing behind
	// whether or not the migration ran in a transaction, so it would assert the
	// rollback rather than test it; here, the probe table exists for the length
	// of the migration and must be gone afterwards.
	broken := "-- +goose Up\n" +
		"-- +goose StatementBegin\n" +
		"CREATE TABLE " + probeTable + " (x INTEGER);\n" +
		"-- +goose StatementEnd\n" +
		"-- +goose StatementBegin\n" +
		"CREATE INDEX idx_broken ON staging (no_such_column);\n" +
		"-- +goose StatementEnd\n"
	out[migrationFilename(max+1)] = &fstest.MapFile{Data: []byte(broken)}
	return out
}

// probeTable is created by the broken migration's first statement. Its absence
// afterwards is what proves the failed migration was rolled back whole.
const probeTable = "broken_probe"

// tableExists reports whether SQLite's own schema carries a table by that name.
func tableExists(t *testing.T, path, table string) bool {
	t.Helper()

	var n int
	inspect(t, path, func(db *sql.DB) {
		row := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table)
		if err := row.Scan(&n); err != nil {
			t.Fatalf("looking for table %s: %v", table, err)
		}
	})
	return n > 0
}

// migrationFilename spells a version number the way goose expects to read it.
func migrationFilename(version int64) string {
	digits := []byte{}
	for v := version; v > 0; v /= 10 {
		digits = append([]byte{byte('0' + v%10)}, digits...)
	}
	for len(digits) < 5 {
		digits = append([]byte{'0'}, digits...)
	}
	return string(digits) + "_broken.sql"
}
