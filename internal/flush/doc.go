// Package flush turns a sealed batch into a committed Iceberg snapshot.
//
// It owns batch sealing, Parquet encoding, upload to object storage, and the
// catalog commit, plus the worker goroutine that drives all of it. One [Worker]
// serves one table, and it is a single goroutine draining a queue in seal order
// — so "flushes for one table are strictly serialized, one in flight at a time"
// is a structural property of there being one worker rather than a lock
// discipline somebody has to maintain.
//
// # What a flush does, in order
//
//  1. Read the batch's rows back from staging by primary key. The staged
//     payloads are authoritative: for a batch recovered from a previous run
//     they are all that is left of the records.
//  2. If the batch is not sealed yet, re-encode any row whose recorded shape
//     differs from the current one, compute the batch's content hash over the
//     payloads the seal will leave behind, and seal — durably, in one
//     transaction, before anything is encoded. Sealing first is what makes
//     crash recovery reproduce the *same* batch, which is what makes the
//     content-hash object key idempotent in practice rather than in theory.
//  3. If the batch is already sealed, its stored key is used as-is and is never
//     recomputed. Recomputing would produce a different key whenever the
//     declared schema changed between seal and replay, stranding the object
//     already uploaded under the old name.
//  4. Decode the payloads, hand them to the caller's record builder — the one
//     generic step, injected because only the caller knows the declaration type
//     — and encode the resulting Arrow record as ZSTD-compressed Parquet.
//  5. If a spool is configured, write those exact bytes to
//     <CacheDir>/<namespace>/<table>/data/<key>.parquet, file first and database
//     second. The upload sends the same buffer, which is what makes the local
//     file and the object byte-for-byte equal.
//  6. Upload it to <prefix>/<namespace>/<table>/data/<key>.parquet.
//  7. Commit the data file to the table, mark the spool file committed, then
//     prune the batch's rows from staging. Nothing is pruned before a commit is
//     confirmed, and the mark is after the commit rather than before it, so the
//     harmless direction of a crash is a file kept too long.
//
// In local-only mode steps 6 and 7 do not happen at all: the spool write is the
// flush's last durable act and the rows are pruned against the file on disk,
// which is the whole of what a confirmed flush means where there is no bucket.
//
// # What it deliberately does not own
//
// The flush *trigger* is not here. Deciding that a batch is full, or old
// enough, or that the flush floor has not elapsed yet, needs the in-memory
// batch state that lives beside the writer, so the timer loop sits there and
// this package is told what to flush and when to start.
//
// # How a flush fails
//
// One trigger buys one *cycle*: up to [Retry.MaxAttempts] attempts at the head
// batch, sleeping a jittered, doubling, capped backoff between them through the
// injected clock, which makes the sleep interruptible by a shutdown. What
// happens at the end of a cycle depends on what failed.
//
//   - A transient failure — staging, the upload, the commit — leaves the batch
//     sealed at the head of the queue and stops. The next trigger starts a fresh
//     budget against the same batch, so a table stuck on an outage heals by
//     itself once the outage ends, without spinning against a dead endpoint in
//     between. [Options.OnFlushError] is called once for the cycle.
//   - A batch that cannot be *encoded* is quarantined instead: marked whole in
//     staging, taken out of the queue so what is behind it can move, reported
//     the same way, and never replayed. The same bytes would fail identically
//     forever, so retrying is not a second chance, it is a wedged table.
//
// Nothing is dropped in any of it, because nothing is pruned from staging until
// a commit lands, and quarantine marks rows rather than deleting them.
package flush
