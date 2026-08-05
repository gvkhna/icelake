// Package inspect answers "is this deployment set up correctly, and what is
// waiting" — by reading, and only by reading.
//
// It is the library half of the icelake command's check subcommand, and it is
// a package rather than command code for the same reason every other decision
// in this repository sits below the commands: what a staging database's rows
// mean, how a warehouse prefix is laid out, and what a table's current
// metadata file is are library knowledge, and a command that owned its own
// copy of any of them would be a second implementation waiting to disagree.
//
// [Inspect] makes at most four kinds of probe, each of which can fail without
// taking the others with it:
//
//   - the staging database, opened with SQLite's query_only pragma so that
//     writing is refused by the engine itself, summarized, and closed;
//   - the bucket, with one bounded listing that proves the endpoint, the
//     credentials and the bucket agree;
//   - each declared table, by picking its current metadata file exactly the
//     way catalog rebuild does (the shared rule lives in internal/metascan)
//     and reading the commit history's tip out of it;
//   - the ClickHouse mirror, with one ping, only when one is configured.
//
// Nothing anywhere in this package writes: no object, no file, no row, no DDL.
// That is what makes it safe to run at any time, including against a live
// daemon — the staging read waits out the writer's lock exactly as the
// daemon's own connections do, and the bucket reads are the same listings any
// query engine makes.
//
// Results come back as a [Report] of plain values with one error field per
// probe, rather than as a rendered document, because what a caller does with
// them is its own decision: the check subcommand prints its report, and an
// embedder is free to alarm on [Staging.QuarantinedRecords] instead.
package inspect
