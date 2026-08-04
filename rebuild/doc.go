// Package rebuild reconstructs a local Iceberg SQL catalog from object storage
// alone.
//
// The catalog icelake writes through is a disposable cache, not irreplaceable
// state: everything needed to rebuild it lives in the warehouse bucket. This
// package lists the warehouse prefix, discovers namespaces and tables from the
// two-segment key layout, picks each table's current metadata file, and
// repopulates the catalog from that scan. It is an operator-invoked,
// writer-stopped recovery action, never a live concurrent-commit mechanism.
//
// # Why the writer has to be stopped
//
// [Rebuild] decides what is current for a table by listing that table's
// metadata directory and taking the highest-numbered file it can read. That is
// a correct answer only while nothing is committing: a writer that landed a new
// metadata file mid-scan would leave the rebuilt catalog one commit behind, and
// the next commit through that catalog would then fail its compare-and-swap.
// There is no race to lose here because there is nothing else running, which is
// the whole distinction between this and a live listing-based catalog.
//
// # What it never does
//
// It writes nothing to the bucket. No metadata file is created, no snapshot is
// added, no object is rewritten — every table keeps exactly the history it
// already had, and running rebuild twice produces the same catalog twice.
//
// # Safety
//
// Three refusals are part of the contract rather than implementation detail.
// An existing catalog database is never overwritten unless
// [RebuildOptions.Replace] says so. Any prefix under the warehouse that the
// <namespace>/<table> layout cannot explain fails the whole run rather than
// being passed over — a table discovery cannot see is the one failure a
// recovery tool must never have. And a warehouse prefix that yields no tables
// at all is refused rather than reported as an empty success, which is what a
// mistyped prefix or the wrong bucket actually hits: the alternative is an
// empty catalog that reports success, after which the next writer creates fresh
// tables over tables that already exist elsewhere.
package rebuild
