package icelake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icebergutils "github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/catalogdb"
	"github.com/gvkhna/icelake/internal/chmirror"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/s3cfg"
	"github.com/gvkhna/icelake/internal/spool"
	"github.com/gvkhna/icelake/internal/staging"
)

// sweepInterval is how often the retention sweeper looks at the cache.
//
// It is a constant rather than configuration deliberately. A caller who wants a
// tighter bound on local disk sets a tighter bound; how often icelake checks
// whether the bound is met is a symptom knob, and exposing it would invite
// tuning the symptom. It is read through the injected clock like every other
// timer here, so a test drives it deterministically rather than waiting.
const sweepInterval = time.Minute

// Store is the process-level handle: one staging database, one catalog
// database, one object-storage client, shared by every table opened through it.
//
// Resources are shared because the caller passed the same Store, not because
// something matched two path strings behind the caller's back. A Store does no
// schema work of its own and never fails because of one table's declaration.
//
// A store opened with [Config.LocalOnly] has neither the catalog database nor
// the client: it reaches no network at all, so those fields are nil and every
// path that would use one is skipped rather than being handed a stub. What it
// does have, and must have, is the spool — which in that mode is where the data
// goes rather than where a copy of it is kept.
//
// A Store is safe for concurrent use, and must be closed.
type Store struct {
	cfg   Config
	clock Clock

	staging *staging.Store
	// spool is the local Parquet cache, or nil when no CacheDir is configured.
	// It is required in local-only mode and optional in bucket mode.
	spool     *spool.Spool
	catalogDB *sql.DB
	catalog   *sqlcat.Catalog
	s3        *s3.Client
	awsCfg    aws.Config
	// clickhouse is the optional mirror's connection pool, shared by every
	// mirrored table exactly as the staging store and the object-storage client
	// are, and nil when no mirror is configured. It is built without reaching
	// the network: a store that could not open because ClickHouse was down
	// would be ClickHouse deciding whether the lake starts.
	clickhouse *chmirror.Conn
	// transport is the HTTP transport every request icelake makes goes
	// through — its own uploads and, because the same configuration travels on
	// the context, iceberg-go's metadata reads and writes too. Owning it is
	// what lets Close hand its sockets back instead of leaving them idle for
	// the transport's own keep-alive window.
	transport *http.Transport

	// ctx is the store-level context every background flush runs under. It is
	// deliberately not derived from Open's context, which has long since
	// returned by the time a flush happens, and it carries the AWS
	// configuration iceberg-go's own file IO reads.
	ctx    context.Context
	cancel context.CancelFunc

	// sweeperStop closes to stop the retention sweeper; sweeperDone closes once
	// it has. Both are nil when no sweeper runs, which is every store with no
	// cache and every local-only store.
	sweeperStop chan struct{}
	sweeperDone chan struct{}

	mu sync.Mutex
	// records and bytes are the in-memory ceiling counters, seeded by the
	// ordered pass Open makes over staging and moved by every accept and every
	// confirmed commit.
	records int64
	bytes   int64
	// writers is every live writer opened through this store, keyed by table so
	// that Close can drain them all and so that a second writer on a table that
	// already has one is refused rather than silently allowed to fight it.
	writers map[staging.TableID]closable
	closed  bool
}

// closable is the part of a writer Store.Close needs, which is the only reason
// a store knows about writers at all. It exists because Writer is generic and a
// store holds writers of many different declaration types.
type closable interface {
	close(ctx context.Context) error
}

// Open validates cfg, opens the staging database, and takes one ordered pass
// over staging. That pass does two things nothing else does: it seeds the
// ceiling counters before any writer exists, and it is the first thing that
// would refuse a staging database left in a state this build will not replay.
//
// In bucket mode it also opens the catalog database and builds the
// object-storage client, exactly once, and starts the retention sweeper when a
// cache is configured. In local-only mode it does none of those three: there is
// no bucket to reach, no catalog to record which metadata file is current, and
// nothing that is ever evictable, so those fields stay nil and no goroutine
// beyond the writers' own runs.
//
// The pass also builds a replay index, and Open deliberately does not keep it:
// it reads the counters off the result and lets it go. Which rows are pending
// for which table is answered later, by [OpenWriter], from its own fresh scan —
// see [Store.pendingFor], which explains why an index built here could not
// answer it correctly.
//
// Corrected at the completeness audit's fifth round (2026-08-01), because
// this comment described the design M4 replaced rather than the one M4
// shipped. It said Open takes "the single ordered pass over staging that both
// seeds the ceiling counters and indexes which rows are pending for which
// table", which was the original arrangement: one snapshot at startup,
// consumed a table at a time. That was reversed at M4 for a reason recorded
// in full at ARCHITECTURE.md's "Decision (2026-07-31, M4)" — a startup
// snapshot cannot contain rows staged during a writer's own life, so
// reopening a table after a Close that drained incompletely handed the new
// writer nothing and stranded those records for the life of the process. The
// code was changed then and has been right ever since; this sentence, the
// matching one on OpenWriter, and the API sketch at the top of
// ARCHITECTURE.md were not, and the three of them went on describing a
// discarded design through every milestone from M5 to M9. The tell for the
// next reader is worth naming: Store has no field holding an index, so a
// comment claiming Open builds one for later use is claiming something the
// struct two dozen lines above cannot support.
//
// It touches no file and makes no network call until cfg has been fully
// validated, and it does no schema work at all: a table's shape is only ever
// consulted by [OpenWriter], so one table's bad declaration cannot stop a
// process from starting.
//
// Pending migrations are applied to the staging database automatically, and a
// failed migration fails this call — icelake never serves writes against a
// database whose schema state it is unsure of.
//
// The caller owns the returned Store and must Close it.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}

	s := &Store{
		cfg:     cfg,
		clock:   clock,
		writers: make(map[staging.TableID]closable),
	}

	// Everything that reaches a network is built here and nowhere else, which
	// is what makes local-only mode one branch rather than a second write path:
	// with no client and no catalog, the paths that would use them are skipped
	// and every other line of this library is the same line.
	background := context.WithoutCancel(ctx)
	if !cfg.LocalOnly {
		s.awsCfg, s.transport = cfg.awsConfig()
		s.s3 = s3.NewFromConfig(s.awsCfg, func(o *s3.Options) {
			// The endpoint is a host this library was told to use, with no
			// wildcard DNS behind it in the test substrate and no need for
			// virtual-host addressing in production either, so every request
			// addresses the bucket path-style — the same way iceberg-go's own
			// file IO is told to address it.
			o.UsePathStyle = true
		})
		background = icebergutils.WithAwsConfig(background, &s.awsCfg)
	}
	s.ctx, s.cancel = context.WithCancel(background)

	st, err := staging.Open(ctx, cfg.StagingPath)
	if err != nil {
		s.cancel()

		return nil, err
	}
	s.staging = st

	// One ordered pass over a table the ceiling already bounds, reading no
	// payloads. It seeds the ceiling counters, and it is the first thing that
	// would refuse a staging database an older or buggier build left in a state
	// this one will not replay.
	index, err := st.Scan(ctx)
	if err != nil {
		s.cancel()
		_ = st.Close()

		return nil, err
	}
	s.records, s.bytes = index.Counters().Records, index.Counters().Bytes

	if cfg.CacheDir != "" {
		s.spool = spool.New(cfg.CacheDir, st)
	}

	// The mirror is built in both modes, and before the local-only return
	// below, because the seam it fires at is the encode seam and local-only
	// runs that seam too. Nothing here reaches the network.
	if cfg.ClickHouse != nil {
		conn, err := chmirror.Open(cfg.chSettings())
		if err != nil {
			s.cancel()
			_ = st.Close()

			return nil, err
		}
		s.clickhouse = conn
	}

	if cfg.LocalOnly {
		return s, nil
	}

	db, err := catalogdb.Open(cfg.CatalogPath)
	if err != nil {
		s.cancel()
		s.closeMirror()
		_ = st.Close()

		return nil, err
	}
	s.catalogDB = db

	cat, err := catalogdb.New(db, cfg.CatalogPath, iceberg.Properties{
		"warehouse":               "s3://" + cfg.Bucket + "/" + cfg.WarehousePrefix,
		forceVirtualAddressingKey: "false",
	})
	if err != nil {
		s.cancel()
		s.closeMirror()
		_ = db.Close()
		_ = st.Close()

		return nil, err
	}
	s.catalog = cat

	s.startSweeper()

	return s, nil
}

// awsConfig builds the AWS configuration every request icelake makes is signed
// with — its own uploads and, through the context, the reads and writes
// iceberg-go performs against the table's own metadata.
//
// The construction itself lives in internal/s3cfg, shared with the
// catalog-rebuild tool: a recovery tool that reached the same bucket with
// subtly different settings than the writer that filled it would be a worse
// version of the problem those settings exist to solve.
func (c Config) awsConfig() (aws.Config, *http.Transport) {
	return c.s3Settings().Build()
}

// s3Settings is the object-storage half of a Config, in the shared shape both
// the write path and catalog rebuild validate and build clients from.
func (c Config) s3Settings() s3cfg.Settings {
	return s3cfg.Settings{
		Endpoint:        c.Endpoint,
		Region:          c.Region,
		AccessKeyID:     c.AccessKeyID,
		SecretAccessKey: c.SecretAccessKey,
		Credentials:     c.Credentials,
	}
}

// icebergCtx returns ctx carrying the AWS configuration, which is how every
// call into iceberg-go reaches the same endpoint with the same credentials and
// the same disabled checksum headers as icelake's own uploads.
func (s *Store) icebergCtx(ctx context.Context) context.Context {
	return icebergutils.WithAwsConfig(ctx, &s.awsCfg)
}

// Close drains and stops every writer this store opened, then stops the
// retention sweeper if one is running, then closes the databases it opened —
// two in bucket mode, one in local-only mode, which opens no catalog. The order
// is the point: staging is never closed while a flush worker might still be
// pruning it or a sweep might still be reading it.
//
// It returns the first error encountered, having still closed everything: a
// drain that could not finish leaves its records safe in staging for the next
// run, which is the whole point of them being there.
//
// Close is the one call an embedding service needs in its shutdown path.
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		return ErrClosed
	}
	s.closed = true
	writers := s.writers
	s.writers = nil
	s.mu.Unlock()

	var first error
	for _, w := range writers { //nolint:gocritic // rangeValCopy: an interface value, not a struct
		if err := w.close(ctx); err != nil && !errors.Is(err, ErrClosed) && first == nil {
			first = err
		}
	}

	// Before the databases and before the context that a sweep in flight is
	// running under: a sweep reads and deletes rows in the staging database, and
	// one still running against a closed handle is a shutdown-ordering bug
	// rather than a race the code can absorb.
	s.stopSweeper()

	// Only now: a worker that was still running would otherwise lose the
	// database it prunes through.
	s.cancel()

	if s.catalogDB != nil {
		if err := s.catalogDB.Close(); err != nil && first == nil {
			first = errdef.NewCatalogError(errdef.CatalogKindClose, s.cfg.CatalogPath, "closing the catalog database", err)
		}
	}
	if err := s.staging.Close(); err != nil && first == nil {
		first = err
	}

	// The mirror's pool goes back last, and a failure to close it is reported
	// through the mirror's own notification channel rather than returned:
	// "a mirror failure never reaches Close" is a sentence this design makes
	// in public, and a close-time pool error is still a mirror failure. By
	// this point every writer has already drained, so nothing is lost either
	// way — the report exists for the operator's log, not for control flow.
	if err := s.closeMirrorErr(); err != nil && s.cfg.OnClickHouseError != nil {
		var typed errdef.ClickHouseError
		if errors.As(err, &typed) {
			func() {
				defer func() { _ = recover() }()
				s.cfg.OnClickHouseError(typed)
			}()
		}
	}

	// The sockets go back now rather than at the end of the transport's own
	// idle window, so a program that opens and closes stores does not
	// accumulate connections to an endpoint it has finished with. A local-only
	// store never built one.
	if s.transport != nil {
		s.transport.CloseIdleConnections()
	}

	return first
}

// mirrorFor prepares one table's ClickHouse mirror, or returns nil when no
// mirror is configured.
//
// The return is what a writer hands to its flush worker, and the error is the
// one half of a mirror failure that is allowed to refuse an open: a conflict.
// Everything else — an unreachable server, a CREATE that the server would not
// run — is reported through the caller's notification hook and swallowed here,
// because the table's mirror is not ready and the next flush will try the whole
// preparation again. A worker holding an unprepared mirror is the normal state
// after a server outage, and it heals with no operator action at all.
func (s *Store) mirrorFor(ctx context.Context, namespace, name string, desc *canon.Descriptor, ttl *MirrorTTL) (*chmirror.Table, error) {
	// Read under the lock: Store.Close clears the field, and an OpenWriter
	// racing a Close must see either the live connection or nil, not a torn
	// read. Every other shared field on Store is immutable after Open; this
	// one is not, so it takes the same lock the lifecycle flag does.
	s.mu.Lock()
	conn := s.clickhouse
	s.mu.Unlock()
	if conn == nil {
		return nil, nil //nolint:nilnil // no mirror configured is not a failure; nil is the whole answer.
	}

	var chTTL *chmirror.TTL
	if ttl != nil {
		chTTL = &chmirror.TTL{Column: ttl.Column, Seconds: int64(ttl.After / time.Second)}
	}
	table, err := chmirror.NewTable(conn, namespace, name, desc, chTTL)
	if err != nil {
		return nil, err
	}

	// Bounded for the same reason the flush tap is bounded: a server that
	// accepts the connection and then stops answering must not be able to hold
	// OpenWriter open for the driver's own read timeout, which is minutes.
	// The bound is the mirror's one number, mirrorEnsureTimeout, and refusing
	// to wait longer costs nothing correctness-wise — the writer opens with an
	// unprepared mirror and the next flush retries the whole preparation.
	ensureCtx, cancel := context.WithTimeout(ctx, mirrorEnsureTimeout)
	defer cancel()
	if err := table.Ensure(ensureCtx); err != nil {
		var typed errdef.ClickHouseError
		if errors.As(err, &typed) && typed.Kind == errdef.ClickHouseKindConflict {
			return nil, err
		}
		if s.cfg.OnClickHouseError != nil {
			var reported errdef.ClickHouseError
			if errors.As(err, &reported) {
				// Contained exactly as the flush worker contains it. This is
				// the one site where the callback runs on the caller's own
				// goroutine — inside OpenWriter, synchronously — and the
				// godoc on OnClickHouseError says so; a panic here must not
				// turn an unreachable mirror into a failed open.
				func() {
					defer func() { _ = recover() }()
					s.cfg.OnClickHouseError(reported)
				}()
			}
		}
	}

	return table, nil
}

// mirrorEnsureTimeout bounds the open-time mirror preparation, for the same
// reason internal/flush bounds the per-batch insert: the mirror is never
// allowed to hold the lake, and OpenWriter is part of the lake.
const mirrorEnsureTimeout = 30 * time.Second

// closeMirrorErr releases the mirror's connection pool, reporting a failure to
// do so. It is a no-op when no mirror is configured.
func (s *Store) closeMirrorErr() error {
	s.mu.Lock()
	conn := s.clickhouse
	s.clickhouse = nil
	s.mu.Unlock()
	if conn == nil {
		return nil
	}

	return conn.Close()
}

// closeMirror is [Store.closeMirrorErr] on a path that is already reporting a
// different failure, where a second one would only obscure the first.
func (s *Store) closeMirror() { _ = s.closeMirrorErr() }

// startSweeper starts the retention sweeper if there is anything for it to
// sweep. It is called by [Open] in bucket mode only.
//
// The sweeper is one goroutine per *store*, deliberately not one per writer, and
// the two counts describe different things. A writer runs a timer loop and a
// flush worker because both read one table's own state; the sweeper reads no
// table's state at all. It enforces a bound on a directory every table in the
// process shares, so per-writer sweepers would be N goroutines enforcing one
// global size cap from N partial views of it — the same argument the staging
// ceiling settled in the other direction, that the thing an operator wants
// bounded is total local disk.
func (s *Store) startSweeper() {
	if s.spool == nil {
		return
	}

	s.sweeperStop = make(chan struct{})
	s.sweeperDone = make(chan struct{})
	// The timer is asked for here, on Open's own goroutine, rather than inside
	// the goroutine below. That is not tidiness: it makes "this store's sweeper
	// has taken a timer from the clock" true by the time Open returns, rather
	// than true a moment later. A caller cannot open a writer before Open
	// returns, so the sweeper's timer is necessarily the first one an injected
	// clock is ever asked for — which is what lets a test drive the sweeper
	// without also driving every writer's interval loop, and lets it do so as a
	// fact about the ordering rather than as a bet on a goroutine being quick.
	go s.runSweeper(s.clock.NewTimer(sweepInterval))
}

// stopSweeper stops the sweeper and waits for it, if one is running.
//
// The wait is deliberately not bounded by Close's context, unlike the writers'
// drain above it, and the difference is what the two are waiting for. A drain is
// waiting on a network — an upload and a catalog commit that a dead endpoint can
// stall for as long as it likes — which is exactly why a caller is allowed to
// give up on it and leave the records safe in staging. A sweep touches nothing
// but local disk and the local staging database: a handful of os.Remove calls
// and one delete per file, over a set the caller's own retention bound keeps
// small. Bounding it would buy a caller nothing it can act on, and would trade
// it for the one thing this ordering exists to prevent — a sweep still deleting
// rows in a database the next line is about to close.
func (s *Store) stopSweeper() {
	if s.sweeperStop == nil {
		return
	}
	close(s.sweeperStop)
	<-s.sweeperDone
}

// runSweeper is the retention sweeper goroutine.
//
// It ticks on the injected clock, like every other timer in this library, at a
// fixed unexported interval that is deliberately not configurable: a caller who
// wants a tighter bound sets a tighter bound, and a sweep interval is a symptom
// knob of the kind this design refuses elsewhere.
//
// The timer is handed in rather than asked for here, so that taking it happens
// on Open's goroutine before Open returns; [Store.startSweeper] says why that
// ordering is worth making certain.
//
// What a sweep returns is discarded, and that is the design rather than an
// omission. An eviction that fails means the cache is larger than it was asked
// to be and nothing else — the file it could not delete is committed, so its
// records are in the table and in the bucket, nothing is stuck, no batch is
// waiting — and the next tick tries again. Routing it through
// [Config.OnFlushError] would cost that callback its enumerated event list and
// its documented goroutine; adding a second callback would put a whole new
// notification surface in Config for a failure with one possible consequence,
// which is a full disk, which an operator's own host monitoring already owns.
func (s *Store) runSweeper(timer Timer) {
	defer close(s.sweeperDone)
	defer timer.Stop()

	for {
		select {
		case <-timer.C():
		case <-s.sweeperStop:
			return
		}

		// Discarded on purpose, for the reason above.
		_ = s.spool.Sweep(s.ctx, s.clock.Now(), s.cfg.CacheMaxAge, s.cfg.CacheMaxBytes)

		timer.Reset(sweepInterval)
	}
}

// pendingFor reads what one table has waiting in staging, by making its own
// ordered pass over the staging database.
//
// The pass is per open rather than once per process, and that is the fix to a
// real bug rather than a preference. A single startup snapshot can only ever
// describe the moment the process started: it cannot see rows staged during a
// writer's own life, so a writer whose drain ran out of time and was then
// reopened — the natural response to a Close that returned an error — would be
// handed nothing, and its undrained records would sit in staging, accepted and
// undelivered, for the rest of the process's life with no error anywhere.
//
// Reading staging again also makes the result impossible to consume by
// accident: the index it builds is local to this call, so an open that fails
// after it discards a slice, and the next open sees exactly the same rows. The
// cost is one scan of a ceiling-bounded table per writer opened instead of one
// per process, paid at startup, needing no index on (namespace, table_name)
// that the design does not already refuse to add.
//
// It is safe to read staging here without holding a lock over it, and the
// reason is worth stating because it is the property the whole arrangement
// rests on: a table's slot in the store is held from the moment its writer is
// registered until that writer's worker goroutine has exited for certain, so a
// table this call is allowed to scan for has nothing staging rows for it and
// nothing pruning them. A table that does have a live writer — including one
// part-way through a drain — cannot be opened again at all, and never reaches
// this call.
func (s *Store) pendingFor(ctx context.Context, namespace, name string) ([]staging.PendingBatch, error) {
	index, err := s.staging.Scan(ctx)
	if err != nil {
		return nil, err
	}

	return index.Claim(namespace, name), nil
}

// checkOpen refuses a store that has already been closed.
//
// It is [Store.claimTable] without the per-table half, for the entry point
// that has no one table in hand: [Store.DrainSpool] walks whatever the spool
// says has a backlog, so there is no name to claim yet, but the reason the
// store's own state is checked first is the same one — a closed store has to answer
// [ErrClosed] rather than whatever a closed database happens to say about the
// first query it is handed.
func (s *Store) checkOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	return nil
}

// claimTable takes a table's slot in this store, refusing a store that has
// closed and a table somebody already owns.
//
// The slot is the whole of icelake's single-owner rule, and since M11 it is
// taken *before* any work happens rather than checked before and registered
// after. Everything that walks one table's files — a writer being opened, the
// spool drain that writer runs first, and [Store.DrainSpool] — holds this claim
// for the whole of what it does, so a table has exactly one owner from the first
// moment either path touches it. Two owners at once is not a lock-ordering
// nicety: both would commit the same backlog files, both would create the same
// namespace in the same local catalog, and the loser would surface a raw
// duplicate-key failure from SQLite rather than anything a caller can match on.
//
// It answers the two sentinels in the order a caller needs them: a closed store
// says so whatever else is wrong, and only then is the table's own availability
// judged. Neither answer costs a file, a request or a derived schema — this is a
// map lookup and a flag read under the store's own lock — which is what keeps a
// refusal immediate rather than something that waits on the network to be told.
//
// The claim is released by [Store.releaseTable] on every path that does not
// reach [Store.swapClaim], which is what turns it into the writer that will hold
// the slot from then on.
func (s *Store) claimTable(namespace, name string) error {
	return s.attach(tableClaim{}, namespace, name)
}

// releaseTable gives a table's slot back. It is safe to call on a slot this
// caller no longer holds — a store that closed took every slot with it — because
// the delete is by key and a missing key is not an error.
func (s *Store) releaseTable(namespace, name string) { s.detach(namespace, name) }

// swapClaim replaces the claim a caller is holding with the writer that is now
// going to own the table, in one step under the lock that guards the registry.
//
// It is deliberately not a release followed by a fresh attach. That pair leaves
// a window in which the table has no owner, and the window is not theoretical:
// an [OpenWriter] that had just finished draining and was about to load its
// table would open that gap for exactly as long as it takes a concurrent
// [Store.DrainSpool] to claim the slot and start loading the same table, at
// which point both are creating namespaces and committing snapshots against one
// local catalog. Swapping under one hold of the lock makes the handover
// unrepresentable rather than unlikely.
//
// A store that closed while the claim was held answers [ErrClosed], and the
// caller unwinds: [Store.Close] has already taken the registry, so there is no
// slot to give the writer and nothing for it to be closed by later.
func (s *Store) swapClaim(w closable, namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	s.writers[staging.TableID{Namespace: namespace, Table: name}] = w

	return nil
}

// attach puts an owner in a table's slot, refusing a store that has closed and
// a table that already has one.
//
// It is the primitive under [Store.claimTable]: since M11 the first thing an
// open does is take the slot, so this is what refuses a second opener rather
// than what registers the finished writer — that is [Store.swapClaim], which
// takes over a slot this already holds.
//
// **Corrected at M11, because the comment here used to call attach "the last
// thing that can refuse an open" and "the only thing in that sequence that
// mutates the store", and neither survived a writer's open learning to drain a
// spool backlog.** The drain uploads objects and commits Iceberg snapshots
// before the writer exists at all, so an [OpenWriter] that fails afterwards can
// very much have left a trace — a permanent one, in a bucket. It is the right
// trace: those commits are exactly what the drain exists to make, they are
// idempotent, and a retry re-walks a backlog that is now shorter. What is no
// longer available is the reassurance the old sentence offered, so it is
// withdrawn rather than reworded, and the property that survives is stated
// instead: a failed open leaves no *writer*, and gives the table's slot back.
//
// The in-use check is the one shape of the single-writer-per-table rule icelake
// can actually enforce. Two writers on one table inside one process would batch
// the same staged rows and commit against each other's metadata, and the loser
// would keep failing against a snapshot the winner has already moved past.
// Across two processes nothing here can see the other side; within one, this is
// a map lookup, so it is checked.
func (s *Store) attach(w closable, namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	id := staging.TableID{Namespace: namespace, Table: name}
	if _, live := s.writers[id]; live {
		return fmt.Errorf("icelake: table %s.%s: %w", namespace, name, ErrTableInUse)
	}
	s.writers[id] = w

	return nil
}

// detach forgets a writer that has closed, so its table can be opened again.
func (s *Store) detach(namespace, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.writers, staging.TableID{Namespace: namespace, Table: name})
}

// accept charges one record against the ceiling, reporting whether there is
// room.
//
// The counters are held in memory and seeded once at startup, off the totals
// [Open]'s single ordered pass accumulates as it goes — cheap precisely because
// the table they measure is itself bounded by the ceiling they enforce. (That
// clause said "from a single aggregate over staging" until the completeness
// audit's fifth round, 2026-08-01. There is such an aggregate query in the
// staging package, and this path does not use it: the seeding comes from the
// pass Open already makes, which returns the same two numbers for free.) They
// are process-wide, across every table, because the thing an operator actually
// needs to bound is total local disk and total memory, not either one per
// table.
func (s *Store) accept(bytes int64) bool { return s.acceptBatch(1, bytes) }

// acceptBatch charges a whole batch against the ceiling at once, reporting
// whether the whole of it fits.
//
// The check and the charge are one hold of the lock over the batch's totals,
// which is what makes the refusal all-or-nothing: there is no moment at which
// part of a batch is charged while the rest is still being decided about, so a
// caller that is refused knows it still owns every record it handed over rather
// than an unstated prefix of them. ARCHITECTURE.md's M15 decision under Config
// holds why that is worth the headroom it occasionally wastes — a batch that
// would have fitted all but its last few records is refused whole, and those
// bytes stay unused until a flush commits, which is a bounded and
// self-correcting cost where an unclear ownership boundary is neither.
func (s *Store) acceptBatch(records int, bytes int64) bool {
	if records == 0 {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.records+int64(records) > s.cfg.StagingMaxRecords || s.bytes+bytes > s.cfg.StagingMaxBytes {
		return false
	}
	s.records += int64(records)
	s.bytes += bytes

	return true
}

// release gives back what a committed batch was holding.
func (s *Store) release(records int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.records -= int64(records)
	s.bytes -= bytes
	if s.records < 0 {
		s.records = 0
	}
	if s.bytes < 0 {
		s.bytes = 0
	}
}
