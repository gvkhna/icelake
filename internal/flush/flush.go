package flush

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/spool"
	"github.com/gvkhna/icelake/internal/staging"
)

// Row is one staged record a batch is made of, without its payload. The
// payload is read back from staging by primary key when the batch is flushed —
// which is the same path a batch recovered from a previous run takes, so a
// fresh batch and a replayed one are the same kind of work rather than two.
type Row struct {
	// ID is the staged row's primary key.
	ID int64
	// ByteLen is its payload length, carried so the ceiling can be released by
	// the same number it was charged.
	ByteLen int64
}

// Batch is one unit of work for a [Worker]: the rows, in batch order, and the
// key they were sealed under if they already were.
type Batch struct {
	// Key is the stored batch key for a batch sealed before this process
	// started, and empty for one this process is about to seal. A stored key is
	// authoritative and is never recomputed: the declared schema may have
	// changed since the seal, and recomputing would name a different object,
	// stranding the one already uploaded.
	Key string
	// Rows are the batch's rows in batch order, which is staged-id order.
	Rows []Row
}

// Records returns how many records the batch holds.
func (b Batch) Records() int { return len(b.Rows) }

// Bytes returns the batch's total staged payload length.
func (b Batch) Bytes() int64 {
	var n int64
	for _, r := range b.Rows {
		n += r.ByteLen
	}
	return n
}

// RecordBuilder turns a batch's decoded canonical rows into the Arrow record a
// data file is written from.
//
// It is injected rather than implemented here because it is the one step that
// depends on the caller's declaration type: rows become values of that type,
// the row-data reflection layer builds columns from them, and each column is
// adapted to the type the declared schema gives it. Everything either side of
// that step is the same for every table, which is why everything either side of
// it lives in this package.
//
// The returned record is released by the caller of this function.
type RecordBuilder func(rows []canon.Row) (arrow.RecordBatch, error)

// Options describe everything one table's [Worker] needs. Every field is
// required unless it says otherwise.
type Options struct {
	// Namespace and Table identify the table. They are already validated by the
	// time a worker exists.
	Namespace string
	Table     string

	// Bucket is the object storage bucket, and Prefix is the warehouse prefix
	// every table hangs under.
	Bucket string
	Prefix string

	// ZSTDLevel is the compression level data files are written with.
	ZSTDLevel int

	// Staging is the shared staging store.
	Staging *staging.Store

	// Spool is the local Parquet cache, or nil when no cache is configured.
	// When it is set, a flush writes the encoded bytes to disk before it uploads
	// them and marks the file committed once the catalog has confirmed the
	// commit — the same bytes, written down twice, on their way somewhere.
	// Optional.
	Spool *spool.Spool

	// LocalOnly ends a flush one step earlier: the spool write is the last
	// durable act, and the batch's rows are pruned against the file on disk
	// rather than against a commit. Nothing on this path touches a bucket or a
	// catalog, and in this mode S3, IcebergTable and Reload are all nil.
	//
	// It requires Spool, which configuration already guarantees: a local-only
	// store must have a cache directory, because there is nowhere else for the
	// data to go.
	LocalOnly bool

	// Descriptor is the current schema shape: what rows are sealed under, and
	// what a batch key's schema component is taken over.
	Descriptor *canon.Descriptor

	// FileSchema is the Arrow schema data files are written with — the declared
	// schema with millisecond timestamps widened to the microseconds Iceberg
	// defines.
	FileSchema *arrow.Schema

	// Build turns decoded rows into an Arrow record.
	Build RecordBuilder

	// S3 uploads data files. It is icelake's own client, constructed with
	// request and response checksum calculation disabled, because R2 answers
	// the checksum headers newer SDKs send by default with 501 Not Implemented.
	S3 *s3.Client

	// IcebergTable is the loaded, reconciled table this worker commits to. The
	// worker replaces its own copy with the one each commit returns, and is the
	// only goroutine that ever touches it.
	IcebergTable *table.Table

	// Reload fetches the table again from the catalog. It is called when the
	// catalog commit itself fails — not when the earlier step that stages the
	// data file into the transaction does, which cannot have moved the table's
	// metadata — because the reason a commit fails on its requirements is
	// usually that the handle in hand describes metadata something else has
	// already moved past, and every later attempt would fail the same way,
	// forever, against the same stale base. Optional; without it a retry simply
	// reuses the handle it had.
	Reload func(ctx context.Context) (*table.Table, error)

	// Ctx is the store-level context every background flush runs under — never
	// an Insert's context, which has long since returned. The worker derives
	// its own cancellable context from it.
	Ctx context.Context

	// Retry is the bounded retry policy one flush cycle runs under, already
	// resolved from the configuration's defaults.
	Retry Retry

	// Clock is icelake's injected time source. This package reads exactly two
	// things from it: the retry sleep between attempts, which is why the sleep
	// is interruptible and why a backoff schedule can be driven deterministically
	// in a test, and the moment a quarantine is recorded at.
	Clock Clock

	// OnFlushError is the caller's notification hook, called once for each
	// failure a caller must act on: a cycle that spent its whole attempt
	// budget, a batch that was quarantined, and a batch that could not be
	// marked. Optional.
	OnFlushError func(errdef.FlushError)

	// OnCommitted is called after a batch's rows are pruned from staging, with
	// what that batch held, so the ceiling counters can be released by exactly
	// what they were charged. Optional.
	OnCommitted func(records int, bytes int64)

	// Mirror is the optional ClickHouse mirror, or nil when none is configured.
	// When it is set, every batch this worker encodes is offered to it at the
	// encode seam — see [Worker.mirror] for why there and nowhere else.
	// Optional.
	Mirror Mirror

	// OnClickHouseError is called with every failure the mirror reports. It is
	// deliberately not OnFlushError: that one means the batch is still in
	// staging and will be retried, and this one means the batch is on its way
	// into the lake exactly as it should be while only the disposable index
	// fell behind. Optional.
	OnClickHouseError func(errdef.ClickHouseError)

	// OnQuarantined is called after a batch's rows are marked unflushable, with
	// what that batch held. It is deliberately not OnCommitted: the rows stay in
	// staging and keep counting toward the ceiling, so what is released is the
	// writer's own pending count and nothing else. Optional.
	OnQuarantined func(records int, bytes int64)

	// OnCycle is called with the outcome of every flush cycle this worker
	// completes: nil when the batch committed, and the cycle's own typed
	// failure otherwise. Optional.
	//
	// It is deliberately not OnFlushError. That one is the caller's
	// notification and fires only for the failures a caller must act on; this
	// one is the writer's own status bookkeeping, fires for every cycle
	// including the ones cut short by a shutdown, and is what lets a status
	// snapshot say whether the last flush worked without anything having been
	// wired up by the caller at all.
	OnCycle func(err error)
}

// Mirror is the optional ClickHouse mirror, as this package needs it: one door,
// taking the batch's key and the Arrow record its data file was just written
// from.
//
// It is an interface rather than the concrete type so that this package keeps
// knowing nothing about ClickHouse — the driver, the DDL and the reconciliation
// all live in internal/chmirror, and what crosses into the flush path is one
// call and one typed error.
type Mirror interface {
	// Insert writes one batch into the mirror. It is called on the flush
	// worker's own goroutine, with the record still alive, and its error is
	// reported rather than acted on.
	Insert(ctx context.Context, batchKey string, rec arrow.RecordBatch) error
}

// mirrorTimeout bounds how long one batch's mirror work may take.
//
// It exists because the mirror insert is synchronous on the flush worker's
// goroutine, and "a ClickHouse failure never blocks the lake" has to be true of
// a server that has stopped answering as well as one that refuses. A bounded
// wait is the cost this design accepts in exchange for not having an unbounded
// queue: the alternative is firing each batch's insert into its own goroutine,
// which is a queue with no size, no ordering and a record held alive in each
// one. Thirty seconds is far below any sane flush interval and far above any
// healthy insert.
const mirrorTimeout = 30 * time.Second

// Clock is the part of icelake's injected clock this package reads. The public
// Clock interface satisfies it, so a worker is handed the same time source
// every other part of the library reads.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// Sleep blocks for d, returning early with ctx.Err() if ctx is done first.
	Sleep(ctx context.Context, d time.Duration) error
}

// DataKey is the object key a batch's data file is uploaded to:
// <prefix>/<namespace>/<table>/data/<hash>.parquet.
//
// The two segments between the prefix and the table's own directories are not a
// naming nicety — catalog rebuild discovers namespaces and tables by listing
// exactly those two levels — which is why namespace and table names are
// validated to contain nothing that could add a third.
//
// It is exported because the transition drain uploads spool files this package
// never sealed, and a second spelling of this layout is exactly the kind of
// duplicate that drifts.
func DataKey(prefix, namespace, table, batchKey string) string {
	return path.Join(prefix, namespace, table, "data", batchKey+".parquet")
}

// DataURI is the same object as a location the Iceberg catalog can resolve.
func DataURI(bucket, prefix, namespace, table, batchKey string) string {
	return "s3://" + path.Join(bucket, DataKey(prefix, namespace, table, batchKey))
}

// dataKey is [DataKey] for one worker's own table.
func (o Options) dataKey(batchKey string) string {
	return DataKey(o.Prefix, o.Namespace, o.Table, batchKey)
}

// dataURI is [DataURI] for one worker's own table.
func (o Options) dataURI(batchKey string) string {
	return DataURI(o.Bucket, o.Prefix, o.Namespace, o.Table, batchKey)
}

// flushErr wraps whatever went wrong into the typed error that crosses the
// public boundary, carrying the identity of what failed.
func (o Options) flushErr(batchKey string, records, attempts int, err error) errdef.FlushError {
	return errdef.FlushError{
		Namespace: o.Namespace,
		Table:     o.Table,
		BatchKey:  batchKey,
		Records:   records,
		Attempts:  attempts,
		Err:       err,
	}
}

// failureKind says which half of the flush path failed, which is the only thing
// that decides whether trying again could ever help.
//
// The split is the one ARCHITECTURE.md draws: an upload or a commit fails
// because something outside this process was unreachable or had moved on, and a
// later attempt against the same bytes may well succeed. A batch that cannot be
// *encoded* fails on its own content — the same rows, decoded by the same
// recorded shape, produce the same failure forever — so it is quarantined
// rather than retried, which is what stops a doomed batch becoming an infinite
// retry loop at the head of a table's queue.
type failureKind int

const (
	// failureNone is a batch that flushed.
	failureNone failureKind = iota
	// failureEncode is a batch this process cannot turn into a data file:
	// decoding a staged payload by its recorded shape, remapping it onto the
	// current one, hashing the batch, building the Arrow record, or writing the
	// Parquet bytes.
	failureEncode
	// failureTransient is everything else — reading or writing staging, the
	// upload, the catalog commit, the prune — where a later attempt is a
	// different attempt rather than the same one repeated.
	failureTransient
)

// unencodable marks an error as one that no number of retries can fix. It is
// attached where the failure happens, rather than inferred later from the error
// type, because the same error type can arrive from either side: a decode
// failure is the batch's own content, and a staging read failure that produced
// nothing to decode is not.
type unencodable struct{ err error }

// Error implements the error interface, adding nothing of its own: the marker
// exists to be recognised, not to be read.
func (e unencodable) Error() string { return e.err.Error() }

// Unwrap exposes the error that was marked, so the marker never breaks a
// caller's errors.Is or errors.As.
func (e unencodable) Unwrap() error { return e.err }

// classify strips the marker off an error and reports which kind of failure it
// was, so nothing downstream — least of all a caller reading FlushError.Err —
// ever meets this package's private wrapper.
func classify(err error) (failureKind, error) {
	if err == nil {
		return failureNone, nil
	}
	var marked unencodable
	if errors.As(err, &marked) {
		return failureEncode, marked.err
	}

	return failureTransient, err
}

// Crash is a test-only hook fired at the named points of the flush path, and is
// nil in production.
//
// The points it is fired at are the moments a crash means something different:
// after a batch is durably sealed but before anything is encoded, after its
// data file is written to the local spool but before it is uploaded, after it is
// uploaded but before the commit that makes it visible, and after that commit
// but before its rows are pruned. Recovery from each is a distinct claim, and
// none of them can be tested without killing a real process at exactly that
// instant.
//
// The hook sites are here from the first version of this code on purpose.
// Retrofitting them later is exactly the kind of invasive change that gets
// skipped, leaving those recovery paths permanently untested.
var Crash func(point string)

// The named crash points. They are exported so a test helper names the same
// string the code fires.
const (
	// PointAfterSealBeforeEncode fires once the batch key is durably stamped on
	// the batch's rows and before a single byte is encoded.
	PointAfterSealBeforeEncode = "after-seal-before-encode"
	// PointAfterUploadBeforeCommit fires once the data file is in the bucket
	// and before the catalog commit that references it.
	PointAfterUploadBeforeCommit = "after-upload-before-commit"
	// PointAfterSpoolBeforeUpload fires once the batch's Parquet file is on
	// local disk and before a single byte of it has been uploaded.
	//
	// It is the only window in which the spool's file-first, database-second
	// ordering is observable at all: at that instant a Parquet file exists that
	// no bucket has ever seen, and the row in spool_files that names it may or
	// may not exist yet. Recovery from it is the ordinary retry — the batch's
	// rows are still staged and its key names them, so the next run writes that
	// same name again and re-records it. The bytes are identical too whenever
	// the declaration has not moved in between, which is every case but the one
	// where a replay re-encodes a sealed batch onto a new shape under its old
	// stored key.
	PointAfterSpoolBeforeUpload = "after-spool-before-upload"
	// PointAfterCommitBeforePrune fires once the commit has landed and before
	// the batch's rows leave staging.
	//
	// It is the window in which the table already holds a batch that replay
	// will nonetheless be offered again — the state the already-committed check
	// exists for, and the only one of the three crash points from which a
	// replay can meet it. (The state itself is reachable other ways, a restored
	// backup of staging.db among them; what this point provides is a way to
	// arrive at it by killing a process, which is what a crash test needs.)
	// TESTING.md names two points as a minimum; this is the third, and it
	// exists because the scenario-2 claim that "replay must not double-count a
	// batch that had already been confirmed committed before a crash" is not
	// reachable from either of the other two.
	PointAfterCommitBeforePrune = "after-commit-before-prune"
)

// crash fires the test-only hook if one is installed.
func crash(point string) {
	if Crash != nil {
		Crash(point)
	}
}

// prepare reads a batch's rows back from staging and returns the key the data
// file must be uploaded under, the decoded rows in batch order, and the row ids
// to prune once the commit lands.
//
// It is where the two kinds of batch converge. An unsealed batch is made
// fingerprint-homogeneous, hashed and sealed here — in that order, so the key
// names the payload bytes the seal leaves behind. An already-sealed batch keeps
// its stored key untouched and is only decoded, remapping any row written under
// an older shape onto the current one by field id.
//
// Every failure that is the batch's own content rather than its surroundings —
// a payload that will not decode under the shape recorded for it, a value that
// cannot be remapped onto the current shape, a batch that cannot be hashed — is
// marked [unencodable] on the way out, because those are the failures a retry
// would only repeat. Reading staging and sealing are not: those are the
// database answering, and a database can answer differently next time.
func (w *Worker) prepare(ctx context.Context, b Batch) (key string, rows []canon.Row, ids []int64, err error) {
	ids = make([]int64, len(b.Rows))
	for i, r := range b.Rows {
		ids[i] = r.ID
	}

	staged, err := w.opts.Staging.Rows(ctx, ids)
	if err != nil {
		return "", nil, nil, err
	}

	current := w.opts.Descriptor
	rows = make([]canon.Row, len(staged))

	if b.Key != "" {
		for i, r := range staged {
			recorded, err := w.descriptor(ctx, r.SchemaFP)
			if err != nil {
				return "", nil, nil, err
			}
			row, err := canon.Decode(w.opts.Table, recorded, r.Payload)
			if err != nil {
				return "", nil, nil, unencodable{err}
			}
			if recorded.Fingerprint() != current.Fingerprint() {
				if row, err = canon.Remap(w.opts.Table, recorded, current, row); err != nil {
					return "", nil, nil, unencodable{err}
				}
			}
			rows[i] = row
		}
		return b.Key, rows, ids, nil
	}

	payloads := make([][]byte, len(staged))
	sealRows := make([]staging.SealRow, len(staged))
	for i, r := range staged {
		sealRows[i] = staging.SealRow{ID: r.ID}
		payloads[i] = r.Payload

		if r.SchemaFP != current.Fingerprint() {
			recorded, err := w.descriptor(ctx, r.SchemaFP)
			if err != nil {
				return "", nil, nil, err
			}
			// Re-encoded under the current shape in the same transaction that
			// stamps the batch key, so a sealed batch is always
			// fingerprint-homogeneous and the hash below is taken over bytes
			// that agree with the schema component of its own input.
			recoded, err := canon.Transcode(w.opts.Table, recorded, current, r.Payload)
			if err != nil {
				return "", nil, nil, unencodable{err}
			}
			payloads[i], sealRows[i].Payload = recoded, recoded
		}
	}

	key, err = canon.BatchHash(w.opts.Namespace, w.opts.Table, current, payloads)
	if err != nil {
		return "", nil, nil, unencodable{err}
	}
	if err := w.opts.Staging.Seal(ctx, staging.SealRequest{
		Namespace:  w.opts.Namespace,
		Table:      w.opts.Table,
		BatchKey:   key,
		Descriptor: current,
		Rows:       sealRows,
	}); err != nil {
		return "", nil, nil, err
	}
	crash(PointAfterSealBeforeEncode)

	for i, p := range payloads {
		if rows[i], err = canon.Decode(w.opts.Table, current, p); err != nil {
			return "", nil, nil, unencodable{err}
		}
	}

	return key, rows, ids, nil
}

// descriptor returns the recorded shape a fingerprint names, caching what it
// reads. Only the worker goroutine calls it, so the cache needs no lock.
func (w *Worker) descriptor(ctx context.Context, fp string) (*canon.Descriptor, error) {
	if d, ok := w.shapes[fp]; ok {
		return d, nil
	}
	d, err := w.opts.Staging.Schema(ctx, fp)
	if err != nil {
		return nil, err
	}
	w.shapes[fp] = d
	return d, nil
}

// upload writes the encoded data file to the bucket under the batch's key.
//
// A retried upload rewrites the same object rather than adding a second one,
// because the key is a content hash over the batch's canonical form: the same
// records, in the same order, under the same shape always produce the same
// name.
func (w *Worker) upload(ctx context.Context, key string, body []byte) error {
	return PutObject(ctx, w.opts.S3, w.opts.Bucket, w.opts.dataKey(key), body)
}

// commit registers the uploaded data file with the table and keeps the handle
// the commit produced.
//
// The data file is handed over by location rather than by pre-computed
// statistics: iceberg-go reads the file's own Parquet footer to derive its row
// count and per-column bounds, and checks the file's schema against the table's
// while it is there. That read is a real cost — the file icelake just uploaded
// is read back on every flush — and it buys the library's own compatibility
// check and its own statistics, neither of which is reachable any other way
// from outside the module.
//
// A failed catalog commit refreshes the handle before returning. Without that,
// a commit that failed on its requirements would be retried against exactly the
// base metadata that failed, and would keep failing for the life of the
// process — a table that can never commit again, with its records piling up in
// staging behind it. The refresh is best-effort: if the catalog cannot be
// reached either, the original failure is the one worth reporting.
//
// The step above it, which stages the data file into the transaction, is
// deliberately not refreshed on failure: it reads the uploaded file and checks
// it against the schema in hand, and nothing it can fail on is fixed by a newer
// view of the table.
func (w *Worker) commit(ctx context.Context, key string) error {
	committed, err := AddFile(ctx, w.opts.IcebergTable, w.opts.dataURI(key))
	if err != nil {
		w.refresh(ctx)

		return err
	}
	w.opts.IcebergTable = committed

	return nil
}

// AddFile registers one uploaded data file with a table and returns the handle
// the commit produced.
//
// It is the one place this library turns an object into a row of a table, shared
// by the flush path above and by the transition drain, which commits files this
// process never sealed. ignoreDuplicates is passed as true, which skips a full
// walk of the current snapshot's manifests on every commit — an O(table size)
// cost paid per flush, forever. The duplicate case it would have caught is
// checked deliberately, by [AlreadyCommitted], and only where a duplicate is
// actually possible: a batch being re-committed after a crash or a failed
// attempt.
func AddFile(ctx context.Context, tbl *table.Table, uri string) (*table.Table, error) {
	txn := tbl.NewTransaction()
	if err := txn.AddFiles(ctx, []string{uri}, nil, true); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// refresh reloads the table handle from the catalog, ignoring a failure to do
// so: it runs on a path that is already reporting an error.
func (w *Worker) refresh(ctx context.Context) {
	if w.opts.Reload == nil {
		return
	}
	if reloaded, err := w.opts.Reload(ctx); err == nil && reloaded != nil {
		w.opts.IcebergTable = reloaded
	}
}

// alreadyCommitted reports whether the table's current snapshot already
// references the batch's data file.
//
// It exists for the one case that can produce a genuine duplicate: a batch
// being re-committed after a crash or a failed attempt, whose upload had
// already landed and whose commit may also have. Walking the current snapshot's
// manifests is a pure metadata read with no scan planning in the way, and only
// data files count — the same machinery also carries delete files, which are
// irrelevant here and would be actively wrong to treat as data.
//
// It is deliberately not run on the normal path: it is a scan of every manifest
// in the current snapshot, which grows with the table, and a batch sealed and
// committed once in this process cannot collide with anything.
func (w *Worker) alreadyCommitted(ctx context.Context, key string) (bool, error) {
	return AlreadyCommitted(ctx, w.opts.IcebergTable, w.opts.dataURI(key))
}

// AlreadyCommitted reports whether a table's current snapshot already references
// a data file location.
//
// It is exported for the transition drain, which asks the same question about a
// file a previous run may already have committed, and asking it two ways would
// be two things to keep right. See [Worker.alreadyCommitted] for when the flush
// path asks it and why it does not ask on the normal path.
func AlreadyCommitted(ctx context.Context, tbl *table.Table, want string) (bool, error) {
	snapshot := tbl.CurrentSnapshot()
	if snapshot == nil {
		return false, nil
	}
	fio, err := tbl.FS(ctx)
	if err != nil {
		return false, err
	}
	manifests, err := snapshot.Manifests(fio)
	if err != nil {
		return false, err
	}

	for _, m := range manifests {
		// Streamed rather than fetched whole: this walk answers a yes/no
		// question and has no reason to hold every entry of every manifest in
		// memory to answer it.
		for entry, err := range m.Entries(fio, true) {
			if err != nil {
				return false, err
			}
			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			if df.FilePath() == want {
				return true, nil
			}
		}
	}

	return false, nil
}

// prune removes a committed batch's rows from staging and drops any schema
// descriptor nothing references any more.
//
// Nothing is pruned before the commit is confirmed, which is the property that
// makes losing the process at any point safe. Descriptor cleanup is a separate
// call because it cannot be done by primary key — it has to look across the
// whole table to know a fingerprint has no rows left — and a descriptor that
// lingers a little longer costs a few bytes nothing reads.
func (w *Worker) prune(ctx context.Context, ids []int64) error {
	if err := w.opts.Staging.Delete(ctx, ids); err != nil {
		return err
	}
	if _, err := w.opts.Staging.PruneSchemas(ctx); err != nil {
		return err
	}
	return nil
}

// process makes one attempt at taking a batch all the way from staged rows to a
// committed snapshot, and reports which kind of failure stopped it.
//
// One attempt, not one batch: the retry cycle around it is [Worker.cycle],
// which decides whether to try again, to give up until the next trigger, or to
// quarantine a batch this process can never encode.
func (w *Worker) process(j *job) (failureKind, error) {
	ctx := w.ctx

	fail := func(key string, err error) (failureKind, error) {
		kind, stripped := classify(err)

		return kind, w.opts.flushErr(key, j.batch.Records(), j.attempts, stripped)
	}

	key, rows, ids, err := w.prepare(ctx, j.batch)
	if err != nil {
		return fail(j.batch.Key, err)
	}
	// From here the batch is sealed under this key whatever happens next, so a
	// retry of it must re-commit rather than re-seal.
	j.batch.Key = key

	// A re-commit is the only way the same key can already be in the table: a
	// batch sealed and uploaded by a previous run, or by any earlier attempt of
	// this one — including one from an earlier cycle, which is why this asks
	// whether the batch has ever been attempted rather than counting the
	// attempts of the cycle it is in.
	//
	// There is no table to ask in local-only mode, and nothing to ask about: a
	// batch that was already spooled is simply re-spooled under the same name,
	// and the row recording it is re-recorded with whatever shape and size the
	// bytes that just landed actually have.
	if !w.opts.LocalOnly && (j.attempted || j.replayed) {
		done, err := w.alreadyCommitted(ctx, key)
		if err != nil {
			return fail(key, err)
		}
		if done {
			// The commit is confirmed here just as surely as on the normal path
			// below — this is the branch a crash between the commit and the
			// prune comes back through — so the file becomes evictable here
			// too. Marking at only the other site would leave a file that was
			// committed by a dead process permanently unevictable.
			if err := w.markSpooled(ctx, key); err != nil {
				return fail(key, err)
			}
			if err := w.prune(ctx, ids); err != nil {
				return fail(key, err)
			}
			w.committed(j)

			return failureNone, nil
		}
	}

	// Everything from here to the upload is the batch's own content becoming
	// bytes: a failure in it is the same failure on every future attempt.
	record, err := w.opts.Build(rows)
	if err != nil {
		return fail(key, unencodable{err})
	}
	body, err := encodeParquet(record, w.opts.FileSchema, w.opts.ZSTDLevel)
	// The mirror tap, at the one instant the record the data file was just
	// written from is still alive. It is inside the encode step's own brackets
	// rather than after them, which is the whole of what makes the rows
	// ClickHouse gets the rows Parquet gets.
	if err == nil {
		w.mirror(ctx, key, record)
	}
	record.Release()
	if err != nil {
		return fail(key, unencodable{err})
	}

	// The spool sits in the one seam that exists between having the bytes and
	// sending them, which is what lets it write down the identical buffer rather
	// than a re-encoding of it.
	if w.opts.Spool != nil {
		if err := w.opts.Spool.Write(ctx, w.opts.Namespace, w.opts.Table, key,
			w.opts.Descriptor, body, w.opts.Clock.Now()); err != nil {
			return fail(key, err)
		}
		crash(PointAfterSpoolBeforeUpload)
	}

	// In local-only mode the flush ends here. The spool file is the batch's
	// durable record — there is no upload, no commit and no snapshot — so the
	// rows are pruned against the file on disk, which is the whole of what a
	// confirmed flush means where there is no bucket.
	if w.opts.LocalOnly {
		if err := w.prune(ctx, ids); err != nil {
			return fail(key, err)
		}
		w.committed(j)

		return failureNone, nil
	}

	if err := w.upload(ctx, key, body); err != nil {
		return fail(key, err)
	}
	crash(PointAfterUploadBeforeCommit)

	if err := w.commit(ctx, key); err != nil {
		return fail(key, err)
	}
	crash(PointAfterCommitBeforePrune)

	if err := w.markSpooled(ctx, key); err != nil {
		return fail(key, err)
	}
	if err := w.prune(ctx, ids); err != nil {
		return fail(key, err)
	}
	w.committed(j)

	return failureNone, nil
}

// mirror offers one encoded batch to the ClickHouse mirror, and reports what
// happens without ever letting it change what happens next.
//
// Where it is called from is the design rather than a convenience. It fires
// after the Parquet bytes exist — so a batch this process cannot encode is
// quarantined and never reaches the mirror — and before the record is released,
// which is the only window in which the mirror can be handed the same in-memory
// columns the data file was written from rather than a second derivation of
// them. It fires identically in bucket mode, in local-only mode and on a
// replayed batch, because every one of those goes through this function. The
// one path that does not is the transition drain, which uploads spool files
// written by an earlier run without re-encoding them — a batch flushed while
// the mirror was configured was already offered to it at its own encode, and
// one flushed before the mirror existed is the rebuild-from-the-lake case.
//
// It returns nothing, and that is the point. A mirror failure is reported
// through the caller's notification hook and the flush proceeds: the batch is
// uploaded, committed and pruned exactly as it would have been. What repairs
// the mirror is not this call trying again — it is the batch key. A retry or a
// replay re-fires this tap with the identical rows under the identical
// deduplication token, and the server collapses the duplicate; a batch that was
// missed entirely comes back when the mirror is rebuilt from the lake, which is
// what a disposable index is for.
func (w *Worker) mirror(ctx context.Context, key string, record arrow.RecordBatch) {
	if w.opts.Mirror == nil {
		return
	}

	// Bounded, because a server that has stopped answering must not be able to
	// hold a flush open indefinitely. See [mirrorTimeout].
	ctx, cancel := context.WithTimeout(ctx, mirrorTimeout)
	defer cancel()

	err := w.opts.Mirror.Insert(ctx, key, record)
	if err == nil || w.opts.OnClickHouseError == nil {
		return
	}

	var typed errdef.ClickHouseError
	if !errors.As(err, &typed) {
		// Unreachable through internal/chmirror, which types every error it
		// returns. Kept so that the callback's own contract — it receives a
		// ClickHouseError — is true of anything that ever implements the
		// interface, rather than true of today's only implementation.
		typed = errdef.NewClickHouseError(errdef.ClickHouseKindInsert,
			w.opts.Namespace, w.opts.Table, "", "mirroring the batch", err)
	}
	typed.BatchKey, typed.Records = key, int(record.NumRows())

	// The same containment OnFlushError gets in the worker's notify: the
	// callback runs on its own goroutine with a recover, and the wait on it is
	// abandoned when the worker's context ends. Without this, a panic in the
	// caller's callback unwinds the flush worker — the mirror taking the
	// process down — and a callback that blocks holds the flush open past the
	// bounded wait the mirror itself is held to.
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			// Deliberately swallowed, exactly as notify swallows it: there is
			// no caller left to hand it to.
			_ = recover()
		}()
		w.opts.OnClickHouseError(typed)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// markSpooled records that a batch's spool file has been uploaded and
// catalog-committed, which is the moment retention may evict it.
//
// It runs after the commit and before the prune, at both of the two places a
// commit is confirmed. A crash in the window it opens leaves a committed file
// nothing will ever evict, which is the harmless direction: a file kept too long
// costs disk, and a file evicted too early would cost a later drain a batch it
// would have to re-upload from nothing.
func (w *Worker) markSpooled(ctx context.Context, key string) error {
	if w.opts.Spool == nil {
		return nil
	}

	return w.opts.Spool.MarkCommitted(ctx, w.opts.Namespace, w.opts.Table, key, w.opts.Clock.Now())
}

// committed releases what a committed batch was holding against the ceiling.
func (w *Worker) committed(j *job) {
	if w.opts.OnCommitted != nil {
		w.opts.OnCommitted(j.batch.Records(), j.batch.Bytes())
	}
}

// String makes a batch readable in an error message without exposing its rows.
func (b Batch) String() string {
	if b.Key == "" {
		return fmt.Sprintf("unsealed batch of %d records", len(b.Rows))
	}
	return fmt.Sprintf("batch %s of %d records", b.Key, len(b.Rows))
}
