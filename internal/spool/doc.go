// Package spool is the local Parquet spool: the cache directory a flushed
// batch's bytes are written to before they are uploaded, and the retention
// sweep that eventually removes them.
//
// The bytes it writes are the bytes the upload sends — the identical buffer, not
// a re-encoding, not a projection, not a second compression pass — so the local
// file and the bucket object are byte-for-byte equal and a difference between
// them is a bug rather than a version skew. The file's name is the batch key, so
// the layout mirrors the warehouse layout exactly:
//
//	<CacheDir>/<namespace>/<table>/data/<batch-key>.parquet
//	<WarehousePrefix>/<namespace>/<table>/data/<batch-key>.parquet
//
// An operator comparing the two needs no lookup table, and a retried batch
// overwrites the same local file for the same reason it overwrites the same
// object: the key is a content hash over the batch's canonical pre-encoding
// form, so the same records under the same shape always produce the same name
// and the same bytes.
//
// This is a write-side artifact and not a query-serving system. icelake never
// reads these files back to answer anything: there is no index, no manifest, no
// snapshot and no schema resolution across files. That DuckDB can read_parquet()
// the directory is a property of having written Parquet files into a directory
// tree, not a service this package offers — two files written under two shapes
// are two Parquet files with two schemas, and reconciling them is the reader's
// problem. The Iceberg table remains the queryable artifact.
//
// # The write is file-first, database-second
//
// [Spool.Write] puts the bytes in a temporary file, fsyncs it, renames it into
// place and fsyncs the directory; only then does one transaction record the
// batch's schema descriptor and its file row. A crash between the two leaves a
// file no row names, which is precisely what a retry of the same batch overwrites
// and re-records, since the file's name is the key of a batch whose rows are
// still staged. Both halves are idempotent, so nothing about that ordering needs
// a repair path.
//
// What that ordering rests on is delegated rather than proven, and it is worth
// naming so the crash test at the seam is not read as proving more than it does.
// "A file under its final name is a whole file" is a property of POSIX rename
// and fsync — a rename cannot half-happen, and the directory fsync is what makes
// the new name survive the machine rather than merely this process — accepted on
// the same terms as the staging database's synchronous=FULL: decades of use, not
// a test in this repo. Killing a process proves recovery from a process crash,
// which is the half icelake owns; the file system's own power-loss behaviour is
// the half it does not.
//
// # What retention may delete, and what defends everything else
//
// [Spool.Sweep] evicts by age first and then by size, oldest first, and it can
// only ever see files that have been uploaded *and* catalog-committed — the
// statement it reads carries `committed_at IS NOT NULL`. That is the whole of
// the protection for a file the bucket has never confirmed: it is not defended
// by policy code that could be edited wrong, it is invisible to the statement
// that does the deleting. It follows, and is documented rather than reported as
// a bug, that a cache can sit far over its configured bound: a bucket unreachable
// for a day leaves a day of uncommitted files no sweep will touch, and the cache
// shrinks by itself once the backlog commits.
//
// Two smaller rules travel with that one. A file already gone from disk is not an
// error — a committed spool file is disposable, and an operator who deleted one
// by hand has done nothing wrong. And a file this package did not write is never
// touched, because eviction deletes exactly the paths its own table names and
// never lists the directory to decide.
//
// A sweep that fails reports to nobody, which is a decision rather than an
// omission: an eviction that fails means the cache is larger than it was asked to
// be and nothing else — the file it could not delete is committed, so its records
// are in the table and in the bucket — and the next tick tries again. A failure
// that persists ends as a full disk, which is a condition an operator's own host
// monitoring already owns.
package spool
