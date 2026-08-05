package icelake

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/gvkhna/icelake/internal/chmirror"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/s3cfg"
)

// DefaultRegion is the region icelake signs requests for when [Config.Region]
// is empty. It is what Cloudflare R2 requires; the field stays overridable so a
// test substrate, or any other S3-compatible endpoint, can pass its own.
const DefaultRegion = s3cfg.DefaultRegion

// DefaultMinFlushInterval is the flush floor applied when
// [Config.MinFlushInterval] is zero: the shortest time icelake will let pass
// between two threshold-driven flushes of one table.
//
// It is deliberately far below any sane production FlushInterval. Its job is
// not to shape normal behaviour but to stop a bug or a misconfiguration turning
// into a flood of tiny, separately-billed writes against object storage — the
// exact anti-pattern this library exists to end.
const DefaultMinFlushInterval = time.Second

// The failure-handling defaults, applied when [Config.FlushMaxAttempts],
// [Config.RetryBaseDelay] or [Config.RetryMaxDelay] is zero.
//
// Together they are one flush cycle of five attempts with sleeps drawn from up
// to one, two, four and eight seconds — at most fifteen seconds of waiting, and
// around half that on average, since the jitter is full. That is deliberately
// short: the cycle is not the last chance a batch gets, only the last chance
// before the next flush trigger arrives and starts a fresh budget against the
// same batch.
const (
	// DefaultFlushMaxAttempts is how many attempts one flush cycle makes when
	// [Config.FlushMaxAttempts] is zero.
	DefaultFlushMaxAttempts = 5
	// DefaultRetryBaseDelay is the first retry's delay ceiling when
	// [Config.RetryBaseDelay] is zero.
	DefaultRetryBaseDelay = time.Second
	// DefaultRetryMaxDelay is the cap on every retry delay when
	// [Config.RetryMaxDelay] is zero.
	DefaultRetryMaxDelay = time.Minute
)

// DefaultClickHouseDatabase is the ClickHouse database the mirror uses when
// [ClickHouseConfig.Database] is empty.
//
// One database for every mirrored table rather than one per namespace, decided
// on the operation that matters most for a disposable index: dropping it. One
// name is the whole of "throw the mirror away and let it be rebuilt", and one
// GRANT is the whole of the permission surface.
const DefaultClickHouseDatabase = chmirror.DefaultDatabase

// ClickHouseConfig turns on the optional ClickHouse mirror: every batch icelake
// encodes is also inserted into a ClickHouse table, in the same act that writes
// the Parquet data file.
//
// **The mirror is a disposable index and the lake is the truth.** The Parquet
// collection in the bucket is the record; the ClickHouse table is a
// reproduction of it that exists to be queried quickly and to be thrown away.
// Nothing about it can fail or lose lake data: a server that is unreachable
// costs a notification through [Config.OnClickHouseError] and nothing else,
// the flush commits regardless, and [Writer.Flush] and [Writer.Close] never
// report a mirror failure at all. What it can cost is bounded time: the
// mirror insert is synchronous on the flush worker and capped at thirty
// seconds per batch, so a server that accepts connections and then stops
// answering delays each flush — and each batch of a closing writer's drain —
// by up to that cap. A server that is down outright refuses immediately and
// costs nothing.
//
// Freshness is flush cadence and nothing faster. A row appears in ClickHouse
// when its batch is flushed, not when it is accepted, so a table that needs a
// fresher mirror gets a shorter [FlushPolicy] of its own rather than a separate
// setting — the mirror is fed by the flush rather than beside it.
//
// A table lands at <Database>.<namespace>__<table>, and every row carries a
// _batch_key column holding the batch's content hash: the same string that
// names that batch's object in the warehouse and its file in the local cache,
// which is what lets a query join the mirror back to the lake. Views,
// aggregations and sorting keys are the caller's own layer and icelake will
// never create one.
//
// It works in both bucket mode and local-only mode, because the seam it fires
// at is one local-only runs too.
type ClickHouseConfig struct {
	// Addr is the server's native-protocol address, host:port. Required, and
	// it is the switch: a nil [Config.ClickHouse] means no mirror, and a
	// non-nil one with no address is a caller who thinks they configured
	// something they did not.
	Addr string

	// Database is the database the mirror's tables live in. Optional; empty
	// means [DefaultClickHouseDatabase]. It is created if it does not exist.
	Database string

	// Username is the user to connect as. Required.
	//
	// Required rather than defaulted for the reason [Config.Credentials]
	// refuses to fall back to the ambient AWS chain: a mirror that connected as
	// whatever user a server happens to default to is a mirror writing
	// somewhere nobody chose.
	Username string

	// Password is that user's password. Optional; empty means the user has
	// none, which is a real configuration rather than a missing one.
	Password string

	// TLS says whether to connect over TLS. Optional; false means a plain
	// connection, which is what a server on a private network usually is.
	TLS bool
}

// Config is the process-level configuration [Open] validates and holds.
//
// Every field in the local-state, object-storage, batching and resource-bound
// groups is required and is rejected at zero. There are no silent defaults for
// the knobs that decide how much data sits where: a zero threshold is a bug in
// the caller's configuration, and substituting something sensible would hide it.
// Optional fields say so and state their default.
//
// LocalOnly re-scopes what "required" means for the object-storage group and for
// CatalogPath, and it does so by requiring their *absence* rather than by
// defaulting them: a store told to reach no bucket must carry no bucket
// configuration at all. The spool's four fields are the other deliberate
// exception to the no-zero rule — an empty CacheDir means no spool, which is a
// real choice rather than a missing one. Both matrices are set out on the fields
// themselves and enforced together, so a caller learns every violation at once.
type Config struct {
	// StagingPath is the local write-ahead staging database. Required.
	//
	// One file per process, shared by every table. It holds records that have
	// been accepted but not yet committed to a table, so it is the one file an
	// operator must have a backup story for — unlike CatalogPath, it is not
	// reconstructable from the bucket.
	StagingPath string

	// CatalogPath is the local Iceberg SQL catalog database. Required in bucket
	// mode, and must be empty when LocalOnly is set — a local-only store opens
	// no catalog, so a path here is a caller who thinks they configured
	// something they did not.
	//
	// It records which metadata file is current for each table. It is
	// deliberately disposable: it can be rebuilt from the bucket at any time,
	// which is why it is a separate file from StagingPath rather than a second
	// table in it.
	CatalogPath string

	// Endpoint is the S3-compatible endpoint's base URL, including scheme.
	// Required in bucket mode, and must be empty when LocalOnly is set.
	Endpoint string

	// Bucket is the bucket every table's data and metadata lives in. Required in
	// bucket mode, and must be empty when LocalOnly is set.
	Bucket string

	// WarehousePrefix is the key prefix each table hangs under. Required in
	// bucket mode, and must be empty when LocalOnly is set.
	//
	// The layout is <WarehousePrefix>/<namespace>/<table>/, exactly two path
	// segments between the prefix and each table's own metadata/ and data/
	// directories. That is not a naming nicety: catalog rebuild discovers
	// namespaces and tables by listing those two levels, so the layout is
	// load-bearing correctness.
	WarehousePrefix string

	// Region is the region requests are signed for. Optional; empty means
	// [DefaultRegion].
	Region string

	// AccessKeyID and SecretAccessKey are static credentials. In bucket mode,
	// supply these or Credentials, never both and never neither. When LocalOnly
	// is set both must be empty, and each is named separately if it is not.
	AccessKeyID     string
	SecretAccessKey string

	// Credentials is a pre-built credential provider, for a caller that already
	// has a credential-sourcing story — an environment chain, an assumed-role
	// provider, a secrets manager. In bucket mode, supply this or the two key
	// strings, never both and never neither; when LocalOnly is set it must be
	// nil.
	//
	// icelake never falls back to the ambient AWS default credential chain. An
	// ingestion library quietly picking up whatever credentials happen to be in
	// the environment is how a service writes to the wrong bucket.
	Credentials aws.CredentialsProvider

	// CacheDir enables the local Parquet spool: the directory a flushed batch's
	// Parquet bytes are written to before they are uploaded, and kept in until
	// retention evicts them. Optional in bucket mode, where empty means no
	// spool; required when LocalOnly is set, because there is nowhere else for
	// the data to go.
	//
	// The bytes are the uploaded bytes — the identical buffer, not a
	// re-encoding — and the layout mirrors the warehouse layout exactly:
	// <CacheDir>/<namespace>/<table>/data/<batch-key>.parquet against
	// <WarehousePrefix>/<namespace>/<table>/data/<batch-key>.parquet. So the
	// local file and the bucket object are byte-for-byte equal, an operator
	// comparing the two needs no lookup table, and DuckDB can read_parquet() the
	// directory. That readability is a property of having written Parquet files
	// into a directory tree and is deliberately not a service icelake offers:
	// there is no manifest, no snapshot and no schema resolution across files —
	// two files written under two shapes are two Parquet files with two schemas,
	// and reconciling them is the reader's problem. The Iceberg table remains
	// the queryable artifact.
	//
	// Setting it in bucket mode requires setting at least one of the two
	// retention bounds below. An unbounded local cache has to be asked for by
	// name, through LocalOnly, rather than fallen into by leaving two fields
	// blank.
	CacheDir string

	// CacheMaxBytes bounds the total size of the *committed* files in CacheDir.
	// Optional; zero means unbounded by size. It requires a CacheDir to bound,
	// and must be zero when LocalOnly is set.
	CacheMaxBytes int64

	// CacheMaxAge bounds how long a *committed* file stays in CacheDir.
	// Optional; zero means unbounded by age. It requires a CacheDir to bound,
	// and must be zero when LocalOnly is set.
	//
	// Both bounds are over committed files only, and the protection for
	// everything else is the shape of the query rather than a rule in the
	// sweeper: a file is invisible to the statement that does the deleting until
	// it has been uploaded *and* committed. Two consequences follow and neither
	// is a bug. A cache can sit far over its configured bound — a bucket that
	// has been unreachable for a day leaves a day of uncommitted files that no
	// sweep will touch, and the cache shrinks by itself once the backlog
	// commits. And in local-only mode nothing is ever committed, which is why a
	// bound there is a knob that cannot turn and is refused rather than ignored.
	CacheMaxAge time.Duration

	// LocalOnly opens a store with no bucket, no catalog and no upload. It is an
	// explicit opt-in and is deliberately never inferred from an empty Endpoint:
	// a configuration file that lost its endpoint would otherwise become a
	// local-only store that looks like it is working.
	//
	// What it skips is everything that touches a network, and nothing else.
	// [Open] builds no object-storage client and opens no catalog database;
	// [OpenWriter] skips the table load, the creation and the whole schema
	// reconciliation loop, because there is no live schema to reconcile against;
	// no retention sweeper runs. Everything else is the same code: name
	// validation, the declaration and its cross-check, staging, the ceiling, the
	// seal, the batch hash, replay, quarantine, Flush and Close.
	//
	// A flush ends one step earlier than it does in bucket mode: the spool write
	// is the last durable act, and a batch's rows are pruned from staging
	// against the spool file rather than against a commit. That is the one place
	// the durability story changes shape, and it is worth stating plainly — in
	// bucket mode a pruned record exists in an Iceberg snapshot, and here it
	// exists in a Parquet file on one local disk. The record is still durable;
	// it is no longer redundant.
	//
	// A local-only backlog is drained by reopening the same StagingPath and the
	// same CacheDir in bucket mode: every spool file that has never reached a
	// bucket is uploaded and committed, per table, by [OpenWriter], and by
	// [Store.DrainSpool] for a table no writer reopens. Every Store method that
	// genuinely needs a bucket returns [ErrLocalOnly] here rather than a
	// confusing failure from a client that was never built, and DrainSpool is
	// the only such method.
	LocalOnly bool

	// ClickHouse turns on the optional ClickHouse mirror. Optional; nil means
	// off, which is what every store that does not name it gets. See
	// [ClickHouseConfig] for what the mirror is and what it deliberately is
	// not.
	ClickHouse *ClickHouseConfig

	// FlushMaxRecords is how many records may accumulate in one table's
	// in-memory batch before it is flushed. Required, positive.
	//
	// It is a threshold a batch is checked against, never a size a batch is cut
	// to, and the difference is only visible through [Writer.InsertBatch]: the
	// threshold is checked once, after the whole slice has joined the in-memory
	// batch, so a single call handing over a million records seals one batch of
	// a million however small this number is. That is deliberate rather than an
	// oversight — the records became durable in one transaction and the caller
	// handed them over as one unit, so splitting them across seals afterwards
	// would invent a boundary nothing in the call asked for — and it is worth
	// knowing because it means the flush size a table actually produces is
	// bounded below by the largest batch its caller inserts, not by this field.
	// A caller that wants this number to be the flush size inserts in slices no
	// larger than it.
	FlushMaxRecords int

	// FlushMaxBytes is how many canonical payload bytes may accumulate in one
	// table's in-memory batch before it is flushed. Required, positive.
	//
	// It is the knob to plan memory against, and the figure people get wrong:
	// during a flush one batch exists as staged rows, as an Arrow record and as
	// an encoded Parquet buffer more or less at once, so allow roughly three to
	// four times this much headroom while a flush is in flight, and keep it at
	// or below 256 MiB unless the deployment has been measured. The spool does
	// not add to that figure, because it writes the same buffer the upload
	// sends; the commit path's re-read of the uploaded object does.
	FlushMaxBytes int64

	// FlushInterval is the longest a batch may sit unflushed even if neither
	// size threshold has tripped. Required, positive.
	//
	// It is also what guarantees a flush trigger arrives for a table whose
	// inserts have stopped, which is how a table that failed to reach the
	// bucket eventually retries by itself.
	//
	// Upload cadence is this field and FlushMaxBytes together and is not a
	// separate mechanism: "upload every thirty minutes or every gigabyte,
	// whichever comes first" is FlushInterval: 30 * time.Minute with
	// FlushMaxBytes: 1 << 30, because the flush trigger already means whichever
	// trips first.
	FlushInterval time.Duration

	// ZSTDLevel is the ZSTD compression level Parquet data files are written
	// with, 1 to 22. Required.
	//
	// Low and fast (1) for development and tests, where iteration speed beats
	// ratio; high (19 and up) for production, where storage cost and ratio beat
	// CPU. This is a pure parameter change and never a code-path change.
	ZSTDLevel int

	// StagingMaxRecords and StagingMaxBytes bound the total staged-but-not-yet
	// committed volume across every table in the process. Required, positive.
	//
	// Once either is reached, Insert returns [ErrStagingFull] and the record is
	// not accepted. This is a memory bound as much as a disk bound: a table
	// that cannot reach the bucket accumulates at most this much in staging
	// before it starts refusing.
	//
	// StagingMaxBytes should be at least two to three times FlushMaxBytes: a
	// batch has to be able to fill while the previous one is still uploading,
	// and a ceiling below that refuses records for a reason the caller cannot
	// act on. A ceiling below a flush threshold outright is refused, because a
	// batch could never fill before the ceiling refused it.
	StagingMaxRecords int64
	StagingMaxBytes   int64

	// FlushMaxAttempts is how many attempts one flush cycle makes at a batch
	// before it gives up and waits for the next flush trigger. Optional; zero
	// means [DefaultFlushMaxAttempts], and one means no retry at all.
	//
	// Giving up is not dropping. The batch stays sealed in staging with its
	// batch key intact, at the head of its table's queue so nothing sealed
	// after it can commit ahead of it, and the next trigger — from the flush
	// interval if from nothing else — starts a fresh budget against the same
	// batch. That is what makes a table stuck on an outage heal by itself when
	// the outage ends, rather than either wedging or spinning against a dead
	// endpoint.
	FlushMaxAttempts int

	// RetryBaseDelay is the delay ceiling for the sleep after the first failed
	// attempt in a cycle, doubling for each attempt after it. Optional; zero
	// means [DefaultRetryBaseDelay].
	RetryBaseDelay time.Duration

	// RetryMaxDelay caps every retry delay however many attempts have been
	// made. Optional; zero means [DefaultRetryMaxDelay].
	//
	// It must not be below the resolved RetryBaseDelay, which is checked
	// against the values the store will actually run on rather than against the
	// literal fields: a zero base means one second, so a cap below one second
	// with no base set is refused rather than silently flattening the schedule.
	RetryMaxDelay time.Duration

	// OnFlushError is called once for each flush failure a caller may need to
	// act on: a cycle that spent its whole attempt budget on one batch, a batch
	// quarantined because it cannot be encoded, and the double fault of a batch
	// that could not even be marked. A cycle cut short by a shutdown is not one
	// of them — that error goes back to Close directly. Optional.
	//
	// It is notification, not control: nothing icelake does next depends on
	// what it returns, and the batch it describes is already safe in staging.
	// Returning from it promptly is the caller's side of the bargain, and a
	// real one: it runs on the flush worker's own goroutine, so a callback that
	// takes a second delays that table's next flush by a second. It must not
	// block, and it must not call back into the writer — Flush and Close wait
	// on that same goroutine, so re-entering either from inside the callback
	// deadlocks. A panic in it is contained rather than fatal, and a callback
	// that never returns is abandoned at shutdown rather than waited for;
	// neither costs the batch anything, because a flush error never means a
	// record was lost.
	//
	// A caller who supplies nothing here is still not silently starved:
	// sustained flush failure fills staging and Insert starts returning
	// [ErrStagingFull], which is a hard, synchronous signal on the caller's own
	// hot path. Flush and Close also return flush errors directly.
	OnFlushError func(FlushError)

	// OnClickHouseError is called once for each failure of the optional
	// ClickHouse mirror. Optional, and it is the only channel a mirror failure
	// on the flush path has.
	//
	// It is deliberately a second callback rather than a widening of
	// OnFlushError, because the two mean opposite things. A [FlushError] says
	// the batch is still in staging and will be retried; a [ClickHouseError]
	// says the batch is on its way into the lake exactly as it should be and
	// only the disposable index fell behind, so there is nothing for the caller
	// to do about the data and nothing icelake will do either. The two also
	// travel differently on purpose: Flush and Close return a FlushError
	// synchronously to the caller who asked, and a mirror failure reaches
	// neither of them — a Close that failed because ClickHouse was down would
	// be ClickHouse blocking the lake, which is the one thing the mirror may
	// never do.
	//
	// It usually runs on the flush worker's own goroutine under exactly the
	// terms OnFlushError states: return promptly, do not block, and do not
	// call back into the writer. A panic in it is contained rather than fatal,
	// and a callback that never returns is abandoned when the worker stops.
	// Two reports arrive on the caller's own goroutine instead, and holding a
	// lock the callback also takes would therefore deadlock: the open-time
	// reachability report, delivered synchronously inside [OpenWriter] before
	// it returns, and a close-time report of a connection pool that would not
	// close, delivered inside [Store.Close].
	//
	// A caller who supplies nothing here still gets the one refusal that cannot
	// heal by itself: a mirror table icelake will not reconcile, and a table
	// name it cannot map, are returned from [OpenWriter] as a
	// [ClickHouseError] with Kind [ClickHouseKindConflict], because those are
	// statements about configuration rather than about a server being
	// unreachable.
	OnClickHouseError func(ClickHouseError)

	// MinFlushInterval is the flush floor: the shortest time icelake will let
	// pass between two threshold-driven flushes of one table. Optional; zero
	// means [DefaultMinFlushInterval].
	//
	// It is a floor beneath whatever FlushInterval and FlushMaxRecords say, not
	// a competing schedule. When a trigger fires before the floor has elapsed,
	// the batch simply keeps accumulating and is flushed as one larger batch
	// instead of several tiny ones. It deliberately does not have to sit below
	// FlushInterval. An explicit Flush or Close ignores it entirely — the floor
	// exists to coalesce automatic triggers, never to delay a caller who asked.
	MinFlushInterval time.Duration

	// Clock is the single time source every timer, deadline and timestamp
	// inside icelake reads. Optional; nil means the real clock.
	//
	// It exists for the test suite and is not a general extension point.
	// Production callers leave it nil and never think about it again.
	Clock Clock
}

// FlushPolicy overrides a [Config]'s batching thresholds for one table.
//
// It exists because two tables in one process can have wildly different row
// sizes — a scalar fact table and a raw-payload archive table are the standing
// example — and forcing both onto one byte threshold means picking a number
// that is wrong for one of them.
//
// Every field is required within a policy, exactly as in Config: a policy is
// supplied as a whole or not at all, so that a zero field is a mistake rather
// than an ambiguous request to inherit one value. MinFlushInterval keeps
// Config's optional-and-defaulted meaning.
//
// The failure-handling knobs are deliberately not here. A policy overrides how
// much data one table collects before it flushes, which genuinely differs
// between a scalar fact table and a raw-payload archive; how long to keep
// trying when the endpoint is unreachable is a property of the endpoint, and
// every table in a process shares one of those.
type FlushPolicy struct {
	// FlushMaxRecords is this table's record threshold. Required, positive.
	FlushMaxRecords int
	// FlushMaxBytes is this table's byte threshold. Required, positive.
	FlushMaxBytes int64
	// FlushInterval is this table's time threshold. Required, positive.
	FlushInterval time.Duration
	// ZSTDLevel is this table's compression level, 1 to 22. Required.
	ZSTDLevel int
	// MinFlushInterval is this table's flush floor. Optional; zero means
	// [DefaultMinFlushInterval].
	MinFlushInterval time.Duration
}

// TableConfig identifies one table and, optionally, overrides how it batches. T
// is the caller's tagged declaration struct; a table declared by a runtime
// schema document is opened with [DynamicTableConfig] instead, which is this
// struct with a stated shape in place of a Go type.
type TableConfig[T any] struct {
	// Namespace is the table's namespace. Required.
	//
	// It maps to the first of the two path segments in the warehouse key
	// layout, and should be whatever the owning service already calls itself,
	// so two services' tables cannot collide by construction.
	Namespace string

	// Table is the table name. Required.
	Table string

	// Flush overrides the store's batching thresholds for this table. Optional;
	// nil means inherit them. A nil pointer is unambiguous in a way a zero
	// value would not be, given zero is otherwise a rejected value.
	Flush *FlushPolicy

	// OnAccept is called with every record this table accepts, synchronously,
	// on the caller's own goroutine, immediately after the staging write
	// commits and before Insert returns. Optional.
	//
	// It exists for one problem: an application that also keeps its own local
	// mirror of recent data — a hot store, an index, a cache — otherwise has
	// two independent "this record now exists" decisions in its own code, and
	// two of them drift. A field is added to one path and not the other; a row
	// lands in one store and not the other after a partial failure. OnAccept
	// removes the second decision point rather than trying to keep two of them
	// in sync: the mirror write happens inside the call that already accepted
	// the record here.
	//
	// An error it returns is returned by Insert too, wrapped in a
	// [MirrorError] — visibly, on the caller's own hot path, exactly like any
	// other error in code the caller wrote. What it does not do is undo
	// anything: staging committed before the callback ran, so the record is
	// archived whatever the callback says. icelake does not retry it, does not
	// store what it returned, and has no idea what the callback did. Indexing,
	// retention and querying that mirror stay entirely the caller's problem.
	//
	// It runs outside every lock icelake holds, so it may call back into this
	// writer — Pending and Status are the useful ones — and a slow callback
	// delays only the goroutine that called Insert, never another one. A panic
	// in it is deliberately not recovered: it happens on the caller's own
	// goroutine, in the caller's own code, where the caller's own defer can
	// catch it. That is the opposite of [Config.OnFlushError], which runs on a
	// background goroutine the caller cannot see and is contained there.
	//
	// Calling Insert from inside it is forbidden: an Insert that accepts a
	// record calls this callback, which would call Insert again, forever. It is
	// not detected — a library check on the caller's own goroutine would cost
	// every insert something to catch a mistake that shows up as an immediate
	// stack overflow the first time it runs.
	//
	// It fires once per record accepted through Insert, and never for a record
	// replayed from staging by a later run: replay re-flushes records that were
	// already accepted, and firing again would tell the mirror about a record it
	// was already told about.
	//
	// That leaves one window a caller with a mirror it cannot afford to have
	// gaps in must know about, and it is a consequence of the ordering that
	// makes the guarantee above true rather than a bug: the record is staged
	// first and the callback runs second, so a process that dies in between
	// leaves a record icelake will replay and commit and the mirror was never
	// told about. icelake cannot close that window — the alternative ordering
	// trades it for the worse one, where a mirror is told about records that
	// were never archived — and it deliberately does not try to reconcile the
	// mirror afterwards, because the mirror's contents are outside its scope
	// entirely. A mirror that must not miss records needs its own
	// reconciliation against the table, which is a query, which is the caller's
	// side of the boundary.
	OnAccept func(row T) error
}

// resolved is a validated, defaults-applied view of the batching knobs one
// table actually runs on. It exists so that no code path downstream has to
// remember whether it is looking at a Config field, a FlushPolicy override, or
// a zero that means a default.
type resolved struct {
	maxRecords  int
	maxBytes    int64
	interval    time.Duration
	zstdLevel   int
	floor       time.Duration
	fieldPrefix string
}

// flushSettings resolves one table's thresholds against the store's.
func (c Config) flushSettings(p *FlushPolicy) resolved {
	if p == nil {
		return resolved{
			maxRecords:  c.FlushMaxRecords,
			maxBytes:    c.FlushMaxBytes,
			interval:    c.FlushInterval,
			zstdLevel:   c.ZSTDLevel,
			floor:       cmpOrDefault(c.MinFlushInterval, DefaultMinFlushInterval),
			fieldPrefix: "Config.",
		}
	}
	return resolved{
		maxRecords:  p.FlushMaxRecords,
		maxBytes:    p.FlushMaxBytes,
		interval:    p.FlushInterval,
		zstdLevel:   p.ZSTDLevel,
		floor:       cmpOrDefault(p.MinFlushInterval, DefaultMinFlushInterval),
		fieldPrefix: "TableConfig.Flush.",
	}
}

// cmpOrDefault returns d when v is zero, which is the one shape of optionality
// this configuration allows.
func cmpOrDefault(v, d time.Duration) time.Duration {
	if v == 0 {
		return d
	}
	return v
}

// retryPolicy is a validated, defaults-applied view of the failure-handling
// knobs, for the same reason [resolved] exists for the batching ones: nothing
// downstream should have to remember whether a zero means a default.
type retryPolicy struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

// retrySettings resolves the failure-handling knobs against their documented
// defaults. It is process-wide: unlike the batching thresholds there is no
// per-table override to fold in.
func (c Config) retrySettings() retryPolicy {
	attempts := c.FlushMaxAttempts
	if attempts == 0 {
		attempts = DefaultFlushMaxAttempts
	}

	return retryPolicy{
		maxAttempts: attempts,
		baseDelay:   cmpOrDefault(c.RetryBaseDelay, DefaultRetryBaseDelay),
		maxDelay:    cmpOrDefault(c.RetryMaxDelay, DefaultRetryMaxDelay),
	}
}

// chSettings is the mirror half of a Config, in the shape internal/chmirror
// takes. It is only ever called with a non-nil ClickHouse config, which
// validation has already checked.
func (c Config) chSettings() chmirror.Settings {
	return chmirror.Settings{
		Addr:     c.ClickHouse.Addr,
		Database: c.ClickHouse.Database,
		Username: c.ClickHouse.Username,
		Password: c.ClickHouse.Password,
		TLS:      c.ClickHouse.TLS,
	}
}

// validate collects every problem with a Config and returns them as one
// [ConfigError] wrapping the join of one [ConfigFieldError] per violation.
//
// All violations are collected rather than the first one returned: a caller
// fixing a configuration file should learn everything wrong with it in one run.
//
// Scope: these are the rules [Open] itself depends on to build a working store,
// and every rule TESTING.md's scenario 7 names is exercised as a table of cases
// (the rules it does not name — the byte-denominated twins of the named
// thresholds, and the remaining required-string fields — share the same code
// path and are asserted only through the combined case) — including the
// multi-violation case that proves the joined error rather than
// first-error-wins. The retry policy's own four rules joined that matrix at M8,
// and the local-only and spool rules — which are their own function, [Config.validateMode],
// because the LocalOnly switch re-scopes what several other fields mean — joined
// it at M11 with TESTING.md's scenario 12, in the same change as the fields they
// validate, per the rule that an error is part of a feature's surface rather
// than a follow-up.
func (c Config) validate() error {
	var violations []error
	bad := func(field, rule string) {
		violations = append(violations, errdef.ConfigFieldError{Field: field, Rule: rule})
	}

	if c.StagingPath == "" {
		bad("StagingPath", "must not be empty")
	}

	violations = append(violations, c.validateMode(bad)...)

	if c.MinFlushInterval < 0 {
		bad("MinFlushInterval", "must not be negative")
	}
	if c.FlushMaxAttempts < 0 {
		bad("FlushMaxAttempts", "must not be negative")
	}
	if c.RetryBaseDelay < 0 {
		bad("RetryBaseDelay", "must not be negative")
	}
	if c.RetryMaxDelay < 0 {
		bad("RetryMaxDelay", "must not be negative")
	}
	// Compared after defaults are applied, so an unset base and a cap below one
	// second is the violation it looks like rather than a schedule quietly
	// flattened to the cap. Skipped when either is already negative, so one
	// mistake is reported once.
	if r := c.retrySettings(); c.RetryBaseDelay >= 0 && c.RetryMaxDelay >= 0 && r.baseDelay > r.maxDelay {
		bad("RetryBaseDelay", "must not exceed RetryMaxDelay")
	}
	// The mirror's own rules, joined into the same error as everything else and
	// checked in both modes: the seam the mirror fires at is one local-only
	// runs too, so there is no mode in which naming a ClickHouse server is a
	// mistake.
	if c.ClickHouse != nil {
		if c.ClickHouse.Addr == "" {
			bad("ClickHouse.Addr", "must not be empty; a ClickHouse config with no address is a mirror that was asked for and not configured")
		}
		if c.ClickHouse.Username == "" {
			bad("ClickHouse.Username", "must not be empty; icelake never connects as whatever user the server defaults to")
		}
	}
	if c.StagingMaxRecords <= 0 {
		bad("StagingMaxRecords", "must be positive")
	}
	if c.StagingMaxBytes <= 0 {
		bad("StagingMaxBytes", "must be positive")
	}

	violations = append(violations, c.flushSettings(nil).validate(c)...)

	if len(violations) == 0 {
		return nil
	}
	return errdef.ConfigError{Err: errors.Join(violations...)}
}

// validateMode checks everything the LocalOnly switch re-scopes: the catalog
// path, the four object-storage fields, the two credential forms, and the
// spool's own three.
//
// It is one function rather than two because the matrix is one decision. Each
// rule is its own [ConfigFieldError] inside the single joined [ConfigError], and
// each forbidden-in-local-only field is named separately, because a store told
// to reach no bucket while holding a bucket's credentials is a caller who thinks
// they configured something they did not — and being told "one of these seven
// should be empty" would not tell them which.
//
// Each field yields at most one violation, which is the same discipline the
// retry-schedule check follows: a bound set in local-only mode is refused by the
// mode rule and is deliberately not also refused for having no CacheDir to
// bound, so one mistake reads as one problem. The outcome the design states —
// that a retention bound with no CacheDir is rejected whatever the mode — holds
// either way, because in local-only mode a bound is rejected outright.
func (c Config) validateMode(bad func(field, rule string)) []error {
	forbidden := []struct{ field, value string }{
		{"CatalogPath", c.CatalogPath},
		{"Endpoint", c.Endpoint},
		{"Bucket", c.Bucket},
		{"WarehousePrefix", c.WarehousePrefix},
		{"AccessKeyID", c.AccessKeyID},
		{"SecretAccessKey", c.SecretAccessKey},
	}

	if c.LocalOnly {
		if c.CacheDir == "" {
			bad("CacheDir", "must not be empty when LocalOnly is set; there is nowhere else for the data to go")
		}
		for _, f := range forbidden {
			if f.value != "" {
				bad(f.field, "must be empty when LocalOnly is set")
			}
		}
		if c.Credentials != nil {
			bad("Credentials", "must be nil when LocalOnly is set")
		}
		// Retention only ever evicts a file that has been uploaded and
		// committed, and nothing is uploaded in this mode, so a bound here is a
		// knob that cannot turn rather than a second policy.
		if c.CacheMaxBytes != 0 {
			bad("CacheMaxBytes", "must be zero when LocalOnly is set; nothing is ever committed, so nothing is ever evictable")
		}
		if c.CacheMaxAge != 0 {
			bad("CacheMaxAge", "must be zero when LocalOnly is set; nothing is ever committed, so nothing is ever evictable")
		}

		return nil
	}

	for _, f := range forbidden[:4] {
		if f.value == "" {
			bad(f.field, "must not be empty")
		}
	}

	violations := c.s3Settings().ValidateCredentials()

	switch {
	case c.CacheMaxBytes < 0:
		bad("CacheMaxBytes", "must not be negative")
	case c.CacheMaxBytes > 0 && c.CacheDir == "":
		bad("CacheMaxBytes", "must not be set without a CacheDir to bound")
	}
	switch {
	case c.CacheMaxAge < 0:
		bad("CacheMaxAge", "must not be negative")
	case c.CacheMaxAge > 0 && c.CacheDir == "":
		bad("CacheMaxAge", "must not be set without a CacheDir to bound")
	}
	// An unbounded local cache has to be asked for by name, through LocalOnly,
	// rather than fallen into by leaving two fields blank.
	if c.CacheDir != "" && c.CacheMaxBytes <= 0 && c.CacheMaxAge <= 0 {
		bad("CacheDir", "must be accompanied by CacheMaxBytes or CacheMaxAge; an unbounded cache is asked for through LocalOnly")
	}

	return violations
}

// validate checks one table's resolved thresholds, including the two rules that
// relate them to the process-wide ceiling.
func (r resolved) validate(c Config) []error {
	var violations []error
	bad := func(field, rule string) {
		violations = append(violations, errdef.ConfigFieldError{Field: r.fieldPrefix + field, Rule: rule})
	}

	if r.maxRecords <= 0 {
		bad("FlushMaxRecords", "must be positive")
	}
	if r.maxBytes <= 0 {
		bad("FlushMaxBytes", "must be positive")
	}
	if r.interval <= 0 {
		bad("FlushInterval", "must be positive")
	}
	if r.zstdLevel < 1 || r.zstdLevel > 22 {
		bad("ZSTDLevel", "must be between 1 and 22")
	}
	if r.floor < 0 {
		bad("MinFlushInterval", "must not be negative")
	}

	// A ceiling below a flush threshold is nonsensical rather than merely
	// small: the batch could never fill before the ceiling refused it, so the
	// writer would be dead on arrival.
	if c.StagingMaxRecords > 0 && r.maxRecords > 0 && c.StagingMaxRecords < int64(r.maxRecords) {
		bad("FlushMaxRecords", "must not exceed Config.StagingMaxRecords; a batch could never fill before the ceiling refused it")
	}
	if c.StagingMaxBytes > 0 && r.maxBytes > 0 && c.StagingMaxBytes < r.maxBytes {
		bad("FlushMaxBytes", "must not exceed Config.StagingMaxBytes; a batch could never fill before the ceiling refused it")
	}

	return violations
}

// validateTable checks a per-table override, if there is one. It returns a
// [ConfigError] shaped exactly like [Config.validate]'s, so a caller handles
// one error type whichever call rejected.
func (c Config) validateTable(p *FlushPolicy) error {
	if p == nil {
		return nil
	}
	violations := c.flushSettings(p).validate(c)
	if len(violations) == 0 {
		return nil
	}
	return errdef.ConfigError{Err: errors.Join(violations...)}
}
