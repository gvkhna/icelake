// Package chmirror is the optional ClickHouse mirror: the disposable index
// ARCHITECTURE.md's "The ClickHouse mirror" section decides, fed from the flush
// path's encode seam.
//
// What it is, in one sentence, because every rule below follows from it: the
// Parquet collection in the bucket is the record, the ClickHouse table is a
// reproduction of it that exists to be queried quickly and to be thrown away,
// and nothing here may ever fail or lose lake data — the one cost the mirror
// may impose on a flush is the bounded wait internal/flush's mirrorTimeout
// states, never an unbounded one and never an error.
//
// Three things about the shape of this package are load-bearing.
//
// It receives an [arrow.RecordBatch] rather than rows. The flush worker calls
// [Table.Insert] at the one moment the record a data file was just written from
// is still alive, so the values that reach ClickHouse are the same in-memory
// columns Parquet got — already carrying the millisecond-to-microsecond
// widening schemamap.FileSchema applies — rather than a second derivation of
// them. That identity is the whole design, and it is proven by TESTING.md's
// scenario 17.
//
// It never composes a value into SQL. The DDL is generated from the table's
// declared schema, column by column, and the insert is handed to the driver's
// batch API with an explicit column list, so the only statement text this
// package builds is identifiers. AGENTS.md's Database Access rule records this
// as a sanctioned exception to the sqlc rule and states the three facts that
// make it one.
//
// It reports and never retries. Every failure here is returned to the caller as
// an [errdef.ClickHouseError]; the flush worker turns all but one kind into a
// notification and carries on. The one kind that is a refusal is a conflict —
// a name that cannot be mapped, or a live table the add-only reconciliation
// will not repair — because that is the only failure that is a statement about
// configuration rather than about a server being unreachable.
package chmirror
