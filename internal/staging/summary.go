package staging

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/gvkhna/icelake/internal/errdef"
)

// Summary is what a read-only look at one staging database can say about it:
// the records accepted and not yet committed, the rows quarantine has taken
// out of replay, and the spool files that have never reached a bucket.
type Summary struct {
	// WaitingRecords and WaitingBytes are the accepted-but-uncommitted rows —
	// the work a drain or the next run's replay will commit.
	WaitingRecords int64
	WaitingBytes   int64

	// QuarantinedRecords are the rows that will never be replayed. They still
	// count toward the staging ceiling until an operator removes them.
	QuarantinedRecords int64

	// SpoolBacklogFiles and SpoolBacklogBytes are the local Parquet files that
	// have never been uploaded and catalog-committed. In bucket mode that is
	// work the next writer open or Store.DrainSpool will move; in local-only
	// mode every file is a backlog file by definition, because nothing is ever
	// uploaded.
	SpoolBacklogFiles int64
	SpoolBacklogBytes int64
}

// Summarize reads one staging database and returns its [Summary], writing no
// row and no schema.
//
// It exists for inspection while something else may own the database — a check
// command against a live daemon — which is exactly the situation [Open] is
// wrong for: Open migrates on every call, and a migration is a write. This
// path opens with SQLite's query_only pragma on every connection, runs its
// aggregate reads, and closes. query_only refuses any statement that would
// modify the database, so "inspection changes no data" is enforced by the
// engine rather than by this package's discipline. The file description stays
// writable underneath, deliberately: a reader that arrives after a kill -9
// must be able to recover the write-ahead log the dead process left, which is
// SQLite writing its own recovery state and never a row. WAL and the busy
// timeout are set exactly as [Open] sets them, so a read here waits out a
// concurrent writer's lock instead of failing it or blocking it.
//
// The file must already exist; a missing one is refused, which is what keeps
// asking about a never-started deployment from creating its database. (The
// check is a stat before the open, because SQLite's open reports a missing
// file only by creating it; a file removed in the instant between the two
// would come back as an empty database whose summary is zeros — stated rather
// than hidden, and the closest to "never writes the file" the driver's open
// semantics allow.)
//
// A table the schema does not have yet reads as empty rather than failing:
// mid-first-migration, or against a database an older binary wrote, a table
// that does not exist is a table that has never held a row, and zero is the
// true count. A read that fails for any other reason is reported as
// [errdef.StagingKindRead]; nothing is ever migrated from here.
func Summarize(ctx context.Context, path string) (Summary, error) {
	if path == "" {
		return Summary{}, errdef.NewStagingError(errdef.StagingKindOpen, path, "staging path is empty", nil)
	}
	// The same driver rule Open guards against: a '?' in the path would be
	// read as the start of DSN parameters and open a different file.
	if strings.ContainsRune(path, '?') {
		return Summary{}, errdef.NewStagingError(errdef.StagingKindOpen, path, "staging path contains '?', which the SQLite driver reads as the start of DSN parameters", nil)
	}
	if _, err := os.Stat(path); err != nil {
		return Summary{}, errdef.NewStagingError(errdef.StagingKindOpen, path, "the staging database does not exist or cannot be read", err)
	}

	db, err := sql.Open(driverName, readOnlyDSN(path))
	if err != nil {
		return Summary{}, errdef.NewStagingError(errdef.StagingKindOpen, path, "opening the staging database read-only", err)
	}
	defer func() { _ = db.Close() }()

	q := New(db)

	var staged StagedSummaryRow
	haveStaging, err := inSchema(ctx, db, "staging")
	if err != nil {
		return Summary{}, errdef.NewStagingError(errdef.StagingKindRead, path, "reading the schema", err)
	}
	if haveStaging {
		if staged, err = q.StagedSummary(ctx); err != nil {
			return Summary{}, errdef.NewStagingError(errdef.StagingKindRead, path, "summarizing staged rows", err)
		}
	} else {
		staged = StagedSummaryRow{WaitingRecords: int64(0), WaitingBytes: int64(0), QuarantinedRecords: int64(0)}
	}

	var spool SpoolBacklogSummaryRow
	haveSpool, err := inSchema(ctx, db, "spool_files")
	if err != nil {
		return Summary{}, errdef.NewStagingError(errdef.StagingKindRead, path, "reading the schema", err)
	}
	if haveSpool {
		if spool, err = q.SpoolBacklogSummary(ctx); err != nil {
			return Summary{}, errdef.NewStagingError(errdef.StagingKindRead, path, "summarizing the spool backlog", err)
		}
	} else {
		spool = SpoolBacklogSummaryRow{Files: 0, Bytes: int64(0)}
	}

	var s Summary
	for _, field := range []struct {
		dst  *int64
		raw  any
		what string
	}{
		{&s.WaitingRecords, staged.WaitingRecords, "waiting records"},
		{&s.WaitingBytes, staged.WaitingBytes, "waiting bytes"},
		{&s.QuarantinedRecords, staged.QuarantinedRecords, "quarantined records"},
		{&s.SpoolBacklogBytes, spool.Bytes, "spool backlog bytes"},
	} {
		n, err := asInt64(field.raw)
		if err != nil {
			return Summary{}, errdef.NewStagingError(errdef.StagingKindRead, path, "reading "+field.what, err)
		}
		*field.dst = n
	}
	s.SpoolBacklogFiles = spool.Files

	return s, nil
}

// inSchema asks sqlite_master whether a table is in the schema. It is the
// one statement this package runs outside queries.sql, because sqlc types
// queries against the migrated schema and sqlite_master is exactly the
// question of whether that schema is there yet.
func inSchema(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", name).Scan(&n)

	return n > 0, err
}

// readOnlyDSN is [dsn] with query_only added and synchronous left alone: a
// connection that never writes rows has nothing for synchronous to make
// durable, and asserting pragmas is Open's job, not a reader's.
func readOnlyDSN(path string) string {
	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds()))
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "query_only(1)")

	return path + "?" + q.Encode()
}
