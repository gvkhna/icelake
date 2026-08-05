// Package icelake turns batches of typed Go records into Apache Iceberg tables
// on S3-compatible object storage, encoding each batch as one ZSTD-compressed
// Parquet data file and committing it to the table's catalog.
//
// A caller declares each table's shape, opens a process-level Store, then opens
// one writer per table. Records are durably staged locally before Insert
// returns, accumulate in memory until a size or time threshold trips, and are
// then encoded, uploaded and committed on a background goroutine.
//
// # Two ways to declare a table
//
// A shape can be a tagged Go struct carrying permanent field IDs, opened with
// [OpenWriter], or a runtime schema document, parsed with [ParseSchemaDocument]
// and opened with [OpenDynamicWriter] — whose rows are decoded records rather
// than Go values and whose front doors are [DynamicWriter.InsertJSON] and
// [DynamicWriter.InsertCBOR]. The second
// exists because OpenWriter needs a Go type at compile time, so every table it
// can write must have been known to whoever built the binary; a program whose
// tables belong to its operator has no type to name.
//
// # Reading a stream of records
//
// [IngestStream] is the whole ingest loop for a program that has a reader rather
// than values: framing, chunking, one staging transaction per table per chunk,
// the rewrite that keeps "everything before the bad record is durable" exact,
// and backpressure that holds the reader while the staging ceiling is full. Each
// record's encoding is decided from its own first byte — a JSON object, or a
// CBOR map — so one stream can carry both, interleaved, with nothing configured.
// The icelake command is a translation over this call and nothing more.
//
// The declaration is the only part of this library that reads a Go type at all.
// Everything after it — the canonical encoding, staging, batching, sealing,
// replay, quarantine, flushing and the catalog commit — is the same code, and
// the two sources produce the same schema fingerprint for the same shape, so one
// table can be written through either door and records staged by one can be
// replayed by the other.
//
// # The local Parquet spool
//
// Setting [Config.CacheDir] keeps every batch's Parquet bytes on local disk as
// well, under a layout that mirrors the warehouse layout exactly, so the local
// file and the bucket object are byte-for-byte equal and DuckDB can
// read_parquet() the directory. It is a write-side artifact rather than a
// query-serving system: icelake never reads those files back to answer
// anything, and the Iceberg table remains the queryable one. A file is deleted
// only once it has been uploaded *and* catalog-committed, and then only when it
// is past [Config.CacheMaxAge] or the cache is over [Config.CacheMaxBytes]; a
// file the bucket has never confirmed is never evicted, however far over its
// bound the cache goes.
//
// # Local-only mode
//
// Setting [Config.LocalOnly] opens a store with no bucket, no catalog and no
// upload, where the spool file is the whole of what a confirmed flush means. It
// reaches no network at all — the same declaration, staging, batching, replay
// and flush machinery, with the upload and the commit removed — and it exists so
// that the first five minutes of using this library need no object-storage
// credentials at all. What that mode writes is not stranded there: opening the
// same StagingPath and the same CacheDir in bucket mode uploads and commits
// every file that never reached a bucket, per table as each writer opens, or
// through [Store.DrainSpool] for a table no writer reopens.
//
// See ARCHITECTURE.md and SCHEMA.md in the repository for the design decisions
// behind this package.
package icelake
