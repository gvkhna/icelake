package icelake

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/table"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/flush"
	"github.com/gvkhna/icelake/internal/schemamap"
	"github.com/gvkhna/icelake/internal/staging"
)

// Writer accepts records for one table. T is whatever a record is on the front
// door that opened it: the caller's tagged declaration struct through
// [OpenWriter], and [Record] through [OpenDynamicWriter].
//
// Every method is safe to call from concurrent goroutines. What is not promised
// is ordering between concurrent callers: rows are ordered by arrival, and a
// caller needing a particular order per key must impose it itself, exactly as
// it would with any other concurrent sink.
//
// A Writer runs two goroutines: a timer that owns the interval trigger and the
// flush floor, and a worker that drains sealed batches one at a time. Neither
// outlives Close, and neither outlives the Store's own Close.
//
// The count is per writer and stays two. A store with a cache configured in
// bucket mode runs one more goroutine of its own — the retention sweeper — and
// it is deliberately not counted here: it reads no table's state and belongs to
// the Store's lifetime rather than to any writer's.
type Writer[T any] struct {
	store *Store
	decl  *schemamap.Declaration
	desc  *canon.Descriptor
	// codec converts a caller's records to canonical rows and a batch of those
	// back into an Arrow record. It is the one part of a writer that differs
	// between a table declared as a Go struct and one declared by a runtime
	// schema document; everything else here is the same object doing the same
	// work whichever front door opened it.
	codec rowCodec[T]
	// file is the Arrow schema data files are written with, taken from the codec
	// rather than derived a second time: a table has one file schema, and the
	// record a codec builds is validated against the same object the flush path
	// writes with.
	file     *arrow.Schema
	settings resolved
	worker   *flush.Worker
	clock    Clock
	// onAccept is the caller's mirror hook, called on the caller's own
	// goroutine inside Insert and outside every lock this writer holds.
	onAccept func(row T) error

	// timerStop closes to stop the timer goroutine; timerDone closes once it
	// has.
	timerStop chan struct{}
	timerDone chan struct{}
	stopOnce  sync.Once
	// closeDone closes once a Close has finished draining and stopping both
	// goroutines. A second caller — the usual one being Store.Close arriving
	// behind a caller's own Close — waits for it before being told the writer
	// is already closed, so that the store never closes the databases out from
	// under a drain that is still running.
	closeDone chan struct{}

	mu     sync.Mutex
	closed bool
	// batch is the current in-memory batch: staged ids and their payload
	// lengths, never the payloads themselves. The payloads are already durable
	// in staging, and reading them back at flush is what makes a batch this
	// process accepted and a batch recovered from a previous run the same kind
	// of work.
	batch      []flush.Row
	batchBytes int64
	// lastSealAt is when a batch was last handed to the worker, which is what
	// the flush floor measures against.
	lastSealAt time.Time
	// queuedRecords and queuedBytes are what has been sealed and handed over
	// but not yet committed. Together with the current batch they are what
	// Pending reports.
	queuedRecords int
	queuedBytes   int64

	// The counters and windows Status reports, all of them under the same lock
	// as the batch state they describe, so that a snapshot of a table is a
	// snapshot of one moment rather than of several. One narrow, harmless
	// exception: the ceiling release after a commit's prune and the commit
	// counter's own increment are two acquisitions on the write side, so a
	// concurrent Status can — for well under a microsecond — see a batch's
	// records already released while FlushesCommitted has not yet heard about
	// the commit. It under-reports and self-corrects; removing it would mean
	// counting the commit before the prune, which lies in the worse direction.
	accepted     uint64
	flushes      uint64
	floorEngaged uint64
	lastCommitAt time.Time
	lastFlushErr error
	flushTimes   []time.Time
	records      []T
}

// OpenWriter opens one table.
//
// It validates the namespace and table names, derives the declared schema from
// T and cross-checks the two independent reflections of it, drains any spool
// backlog this table has, runs the table's whole startup lifecycle against the
// catalog — creating the table if it does not exist, reconciling its schema if
// it does — and takes its own ordered pass over staging to claim that table's
// pending rows.
//
// The drain runs *before* the lifecycle, which matters and is not the obvious
// order: each backlog file is committed against the shape its own descriptor
// records, walking the table forward one file at a time, and only then is the
// table reconciled to the current declaration. See [Store.DrainSpool], which
// does the same walk for a table no writer reopens. In local-only mode there is
// nothing to drain into and no table lifecycle at all: [Config.LocalOnly]
// describes exactly what is skipped, which is everything that touches a network
// and nothing else.
//
// That pass is per open rather than per process, and the index it builds is
// local to this call; [Store.pendingFor] holds the full reasoning, and
// ARCHITECTURE.md's "Decision (2026-07-31, M4)" holds the bug that forced it.
//
// Corrected at the completeness audit's fifth round (2026-08-01): this
// comment said OpenWriter "claims that table's pending rows from the replay
// index [Open] built", which was the pre-M4 design and has not been how this
// function works since. The inline comment sitting directly on top of the
// call this sentence describes — "What this table has waiting in staging,
// read fresh" — has said the opposite since M4, in this same function, about
// fifty lines below the sentence contradicting it. That is the shape of this
// defect worth remembering: the stale claim was in the doc comment a caller
// reads through go doc, and the true one was in an implementation note only
// someone already inside the function would see, so every reader who could
// most easily have caught it was the one reader who had no reason to look.
//
// A failure here blocks this one table and nothing else. Every other writer in
// the process is unaffected, and the Store itself stays perfectly usable. What
// a caller does with that error is its own decision, and both answers are
// legitimate: a service whose tables are independent can carry on with the
// writers that opened, and one whose tables are only meaningful together can
// treat any failure as fatal to startup. What icelake will not do is decide for
// the caller by half-opening a writer against a table it could not verify, so a
// failed call returns no writer at all.
//
// Records waiting in staging for this table are picked up here — those a
// previous run left, and those a previous writer in this same process accepted
// and could not drain before it was closed. A batch that was already sealed is
// re-uploaded under its stored key, unchanged; rows that were never sealed
// re-enter batching and are flushed under the current configuration's
// thresholds. Rows recorded under a shape this declaration cannot represent
// fail this call rather than being quietly dropped.
func OpenWriter[T any](ctx context.Context, s *Store, tc TableConfig[T]) (*Writer[T], error) {
	return openWriter(ctx, s, writerPlan[T]{
		namespace: tc.Namespace,
		table:     tc.Table,
		flush:     tc.Flush,
		mirrorTTL: tc.MirrorTTL,
		onAccept:  tc.OnAccept,
		derive: func() (*schemamap.Declaration, *canon.Descriptor, rowCodec[T], error) {
			decl, err := schemamap.Declare[T](tc.Namespace, tc.Table)
			if err != nil {
				return nil, nil, nil, err
			}
			desc, err := canon.Describe(tc.Table, decl.IcebergSchema())
			if err != nil {
				return nil, nil, nil, err
			}
			bind, err := newBinder[T](tc.Table, desc, schemamap.FileSchema(decl.ArrowSchema()))
			if err != nil {
				return nil, nil, nil, err
			}

			return decl, desc, bind, nil
		},
	})
}

// writerPlan is everything [openWriter] needs that differs between the two ways
// a table's shape can be stated.
//
// It is a struct rather than a long parameter list because the interesting field
// is the last one, and a caller reading this should see immediately that the two
// front doors differ in exactly one thing: where the declaration comes from and
// which codec reads a caller's records for it. Everything else — the claim, the
// spool drain, the table lifecycle, the staged rows, the worker, the goroutines
// — is shared, which is what makes a dynamic writer the same writer rather than
// a parallel one.
type writerPlan[T any] struct {
	// namespace and table identify the table. They are validated by whichever
	// derivation the plan carries, since both sources validate names the same
	// way and with the same error.
	namespace string
	table     string
	// flush overrides the store's batching thresholds for this table, or is nil.
	flush *FlushPolicy
	// mirrorTTL is this table's declared mirror row expiry, or nil. It is
	// carried to the ClickHouse mirror and nowhere else; with no mirror
	// configured it is deliberately inert, so one schema document serves a
	// deployment with the mirror on and one with it off.
	mirrorTTL *MirrorTTL
	// onAccept is the caller's mirror hook, or nil.
	onAccept func(row T) error
	// derive produces the declaration, the descriptor taken over it, and the
	// codec that converts records for them. It is pure computation: it opens no
	// file, makes no request and changes nothing.
	derive func() (*schemamap.Declaration, *canon.Descriptor, rowCodec[T], error)
}

// openWriter is the whole of opening a writer, shared by [OpenWriter] and
// [OpenDynamicWriter].
//
// The two exported constructors are front doors onto this: one derives a
// declaration from a Go type and one from a runtime schema document, and from
// the moment a *Declaration exists neither this function nor anything below it
// can tell which. That is the property SCHEMA.md's second-declaration-source
// decision rests on, and keeping it structural — one function, one claim
// lifecycle, one worker construction — is what stops it from becoming a
// resemblance that drifts.
func openWriter[T any](ctx context.Context, s *Store, plan writerPlan[T]) (*Writer[T], error) {
	// First, before anything reaches for a file, the catalog or the network: a
	// closed store and a table that already has an owner both have to answer
	// with the sentinel that says so, immediately, rather than with whatever
	// error the first resource they touched happens to produce.
	//
	// This is a claim rather than a question, and it is held from here until the
	// writer below takes the slot over. Everything this call does to a table —
	// draining its spool backlog, creating it, reconciling it, claiming its
	// staged rows — has to be the only thing doing it, and a check that merely
	// looked would leave every step after it unguarded. [Store.claimTable] holds
	// the reasoning; [Store.DrainSpool] holds the outcome matrix for whoever
	// arrives second.
	if err := s.claimTable(plan.namespace, plan.table); err != nil {
		return nil, err
	}
	// Released on every path out of here except the one that reaches
	// [Store.swapClaim], which hands the slot to the writer instead.
	claimed := true
	defer func() {
		if claimed {
			s.releaseTable(plan.namespace, plan.table)
		}
	}()

	if err := s.cfg.validateTable(plan.flush); err != nil {
		return nil, err
	}

	decl, desc, codec, err := plan.derive()
	if err != nil {
		return nil, err
	}

	// The transition drain, and its position is the load-bearing part rather
	// than an implementation detail. A table with spool files that have never
	// reached a bucket is walked *before* it is loaded, created or reconciled
	// against the current declaration, because AddFiles re-reads the file it is
	// handed and checks it against the live table — so committing a file written
	// under an older shape to a table already moved forward to today's
	// declaration is the one step in this design that might simply be refused.
	// Running the drain first removes the question: each file is committed
	// against its own recorded shape, and the ordinary lifecycle below then
	// brings the table to the current declaration in one more forward step of
	// the same loop.
	//
	// It does nothing at all in local-only mode, where there is no bucket to
	// drain into, and nothing when no cache is configured, where there is no
	// backlog to drain.
	//
	// It runs under the claim taken at the top of this function, as does the
	// openTable below it, which is the part that matters: the two are one
	// uninterrupted stretch of work against one table, and a concurrent
	// [Store.DrainSpool] cannot be inside any of it.
	if err := s.drainTable(ctx, plan.namespace, plan.table); err != nil {
		return nil, err
	}

	// Local-only mode skips the whole table lifecycle: no catalog, no table to
	// load or create, and no live schema to reconcile a declaration against. The
	// declaration is still fully derived and cross-checked above, because it is
	// what the canonical encoding, the schema fingerprint and every batch key
	// are computed from; what it is not, in that mode, is checked against a live
	// table. The transition drain is where a shape declared locally is finally
	// judged by a real catalog.
	var tbl *table.Table
	if !s.cfg.LocalOnly {
		if tbl, err = s.openTable(s.icebergCtx(ctx), decl); err != nil {
			return nil, err
		}
	}

	// What this table has waiting in staging, read fresh: rows a previous run
	// left, and rows a previous writer in this same process accepted and could
	// not drain. The result is local to this call, so an open that fails below
	// leaves them exactly where they are for the next one.
	pending, err := s.pendingFor(ctx, plan.namespace, plan.table)
	if err != nil {
		return nil, err
	}
	if err := checkPendingShapes(ctx, s, plan.table, desc, pending); err != nil {
		return nil, err
	}

	w := &Writer[T]{
		store:     s,
		decl:      decl,
		desc:      desc,
		codec:     codec,
		file:      codec.FileSchema(),
		settings:  s.cfg.flushSettings(plan.flush),
		clock:     s.clock,
		onAccept:  plan.onAccept,
		timerStop: make(chan struct{}),
		timerDone: make(chan struct{}),
		closeDone: make(chan struct{}),
	}
	w.lastSealAt = w.clock.Now()

	// The mirror's own preparation, and its failure is split by what the failure
	// means rather than by where it happened. A conflict — a name that cannot
	// be mapped, or a live mirror table the add-only reconciliation will not
	// repair — is the operator asking for something icelake will not do: it
	// cannot heal by itself, nothing has been accepted yet, and it blocks this
	// one table exactly as a ReconcileError does. A server that is unreachable
	// is not that, and refusing here would mean a ClickHouse outage stopping
	// the lake from accepting records, which is the one thing the mirror may
	// never do — so it is reported, the writer opens, and the whole preparation
	// is tried again at the next flush.
	mirrorTable, err := s.mirrorFor(ctx, plan.namespace, plan.table, desc, plan.mirrorTTL)
	if err != nil {
		return nil, err
	}
	// Assigned through a nil check rather than directly, because a typed nil
	// pointer in an interface is not a nil interface: handing the worker one
	// would make every flush of every store without a mirror call a method on
	// nothing.
	var mirror flush.Mirror
	if mirrorTable != nil {
		mirror = mirrorTable
	}

	retry := s.cfg.retrySettings()
	// A local-only worker is handed no client, no table and no way to reload
	// one, because there are none: it stops at the spool write, which is that
	// mode's whole durability story.
	var reload func(ctx context.Context) (*table.Table, error)
	if !s.cfg.LocalOnly {
		reload = func(ctx context.Context) (*table.Table, error) {
			return s.catalog.LoadTable(s.icebergCtx(ctx), table.Identifier{plan.namespace, plan.table})
		}
	}
	w.worker = flush.NewWorker(flush.Options{
		Namespace:    plan.namespace,
		Table:        plan.table,
		Bucket:       s.cfg.Bucket,
		Prefix:       s.cfg.WarehousePrefix,
		ZSTDLevel:    w.settings.zstdLevel,
		Staging:      s.staging,
		Spool:        s.spool,
		LocalOnly:    s.cfg.LocalOnly,
		Descriptor:   desc,
		FileSchema:   w.file,
		Build:        codec.Build,
		S3:           s.s3,
		IcebergTable: tbl,
		Reload:       reload,
		Ctx:          s.ctx,
		Clock:        s.clock,
		Retry: flush.Retry{
			MaxAttempts: retry.maxAttempts,
			BaseDelay:   retry.baseDelay,
			MaxDelay:    retry.maxDelay,
		},
		Mirror:       mirror,
		OnFlushError: s.cfg.OnFlushError,
		OnClickHouseError: func(err errdef.ClickHouseError) {
			if s.cfg.OnClickHouseError != nil {
				s.cfg.OnClickHouseError(err)
			}
		},
		OnCommitted:   w.committed,
		OnQuarantined: w.quarantined,
		OnCycle:       w.cycleEnded,
	})

	// The slot this call has held since its first line becomes the writer's, in
	// one step under the registry's lock rather than as a release and a fresh
	// attach: a table is never unowned between the two. The only thing that can
	// refuse here is a store that closed underneath the open.
	if err := s.swapClaim(w, plan.namespace, plan.table); err != nil {
		w.worker.Close(ctx)

		return nil, err
	}
	claimed = false

	w.replay(pending)
	go w.runTimer()

	return w, nil
}

// checkPendingShapes refuses to open a table whose staged rows were written
// under a shape the current declaration cannot represent.
//
// The check is up front and per distinct recorded shape rather than per row,
// and it covers already-sealed batches too, because the alternative is
// discovering the same thing during a background flush that has no caller to
// return it to. The operator's next action differs by kind, which is why the
// kind is part of the error.
func checkPendingShapes(ctx context.Context, s *Store, name string, current *canon.Descriptor, pending []staging.PendingBatch) error {
	seen := make(map[string]bool)
	for _, b := range pending {
		for _, r := range b.Rows {
			if r.SchemaFP == current.Fingerprint() || seen[r.SchemaFP] {
				continue
			}
			seen[r.SchemaFP] = true

			recorded, err := s.staging.Schema(ctx, r.SchemaFP)
			if err != nil {
				return err
			}
			if err := canon.CompatibleShapes(name, recorded, current); err != nil {
				return err
			}
		}
	}

	return nil
}

// replay puts a previous run's staged rows back to work.
//
// Already-sealed batches are queued first, in seal order, so a table's
// snapshots stay in the order its records were written — they upload under
// their stored keys, which is what makes the retry of an interrupted flush an
// overwrite of the same object rather than a second one. Rows that were never
// sealed become the current in-memory batch and are free to be re-batched under
// whatever thresholds this run is configured with.
func (w *Writer[T]) replay(pending []staging.PendingBatch) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, b := range pending {
		rows := make([]flush.Row, len(b.Rows))
		var bytes int64
		for i, r := range b.Rows {
			rows[i] = flush.Row{ID: r.ID, ByteLen: r.ByteLen}
			bytes += r.ByteLen
		}

		if b.Key == "" {
			w.batch = append(w.batch, rows...)
			w.batchBytes += bytes

			continue
		}

		w.queuedRecords += len(rows)
		w.queuedBytes += bytes
		w.worker.Enqueue(flush.Batch{Key: b.Key, Rows: rows}, true)
	}
}

// Insert accepts one record.
//
// It returns once the record is durably committed to the local staging
// database — that is the guarantee, and it is why a crash between accepting a
// record and committing its batch cannot lose it. Everything after that
// (batching, Parquet encoding, upload, the catalog commit) happens in the
// background, and a caller never waits on the network here.
//
// Insert refuses rather than blocks when the staging ceiling is reached: it
// returns [ErrStagingFull] and the record does not enter staging, so the caller
// knows precisely which records it still owns. Blocking would turn a storage
// problem into an invisible latency problem in the caller's own hot path, and
// could deadlock a caller that inserts from the goroutine that would otherwise
// drain something.
//
// A staging failure is returned, never absorbed. There is no in-memory fallback
// buffer for "the durability layer is broken", because such a buffer would
// silently downgrade the exact guarantee that layer exists to provide.
//
// A record carrying a value its declared column cannot represent — a string
// that is not valid UTF-8, a decimal that does not fit its declared precision,
// a value longer than the canonical encoding can address — is refused here with
// a [PoisonError] naming the field, before anything is staged. Refusing at the
// boundary is the point: the caller still has the record and the context to fix
// it, where the same value discovered at flush time would land in the data file
// unnoticed, since nothing below this checks a value's domain.
//
// On a [DynamicWriter] one further refusal comes first, because a record there
// has not been through a compiler: a [Record] whose keys or values do not
// describe a row of this table — an unknown key, a required column with nothing
// in it, a JSON value the column cannot hold or can only hold by losing
// something — is refused with a [RecordError], and the poison check above then
// runs on what survives. SCHEMA.md's coercion table is the whole of what is
// accepted, and [Record] states the one thing a caller building a map by hand
// has to know about it: the values must be JSON-shaped, because that table is
// closed. Both refusals mean the same thing to a caller: the record was not
// accepted and is still theirs.
//
// The refusals are ordered, and the order is a promise rather than an accident
// of where each check happens to sit: a closed writer answers [ErrClosed]
// first, whatever the record contains, because the caller's next move is about
// the writer and not about that record. Only then is the record itself judged.
//
// If the table declared an [TableConfig.OnAccept] callback, it is called here —
// synchronously, on this goroutine, once the record is staged and before this
// call returns — and an error it returns is returned from here wrapped in a
// [MirrorError]. That error is the one refusal-shaped thing this call can
// answer that does not mean the record was refused: it was accepted before the
// callback ran, and it will be committed. Re-inserting on it writes the record
// twice.
func (w *Writer[T]) Insert(ctx context.Context, row T) error {
	// The lifecycle answer comes first, exactly as OpenWriter's own
	// claimTable does it: a writer that has been closed has to say so with
	// the sentinel that means it, rather than with whatever complaint the first
	// piece of work it attempted happened to produce. It costs one uncontended
	// lock on a path that ends in an fsync, and it is what makes ErrClosed's
	// documented promise true for every record rather than for well-formed ones.
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()
	if closed {
		return ErrClosed
	}

	// Binding, the poison check and encoding are all pure computation and are
	// deliberately done outside the lock: the mutex exists to order the staging
	// write and the in-memory batch, not to serialize work that touches neither.
	values, err := w.codec.Bind(row)
	if err != nil {
		return err
	}
	if err := checkPoison(w.decl.Table(), w.desc, values); err != nil {
		return err
	}

	payload, err := canon.Encode(w.decl.Table(), w.desc, values)
	if err != nil {
		return err
	}

	if err := w.stage(ctx, row, payload); err != nil {
		return err
	}

	// The mirror hook, outside the lock and outside anything else this writer
	// holds. Two things follow from that and both are deliberate. A callback
	// that is slow, or that calls back into this writer to read Pending or
	// Status, delays or blocks nothing but the goroutine that called Insert.
	// And a panic in it travels straight up the caller's own stack, where the
	// caller's own defer can catch it, rather than being swallowed by a library
	// that has nothing useful to do with it — the opposite of OnFlushError,
	// which runs on a goroutine the caller cannot see and is contained there
	// for exactly that reason.
	if w.onAccept != nil {
		if err := w.onAccept(row); err != nil {
			return errdef.MirrorError{Namespace: w.decl.Namespace(), Table: w.decl.Table(), Err: err}
		}
	}

	return nil
}

// InsertBatch accepts a whole slice of records as one unit.
//
// It returns once every record in the slice is durably committed to the local
// staging database, in one transaction — so the batch is atomic in the strongest
// sense available here: all of it is staged or none of it is, on a refusal, on a
// failure part way through, on a context cancelled mid-transaction, and across a
// crash between any two of its rows. There is no state in which a caller holds
// the slice and does not know which end of it icelake took. What this buys, and
// the reason it exists, is that the one fsync a staging transaction costs is paid
// for the batch rather than for each record in it: ARCHITECTURE.md's M15
// decisions hold the measurements, and the short form is that the rate goes from
// hundreds of records a second to six figures.
//
// [Writer.Insert]'s per-record promise is untouched. This is a second
// granularity, not a second guarantee, and a caller that hands over one record
// at a time still gets back "that record is on the disk" before the call
// returns.
//
// The refusals are the same ones Insert documents, in the same order, asked of
// the slice instead of the record:
//
// A closed writer answers [ErrClosed] first, whatever the batch contains and
// however malformed its first row is, because the caller's next move is about
// the writer and not about any record. That includes an empty slice: an empty
// batch on an open writer is a no-op returning nil and touching nothing, so a
// caller looping over a partition that came out empty needs no special case,
// but it is not a way to ask a closed writer a question it will answer with nil.
//
// Then every record is bound, poison-checked and encoded, in order, before
// anything at all is staged. The first record that fails refuses the whole
// batch, and the error is that record's own — a [PoisonError], a [RecordError]
// on a [DynamicWriter], an encoding failure — wrapped in a [BatchError] carrying
// its index, so the caller learns which of the rows it passed in is the problem
// and can fix it, drop it, or split around it. Nothing is staged on that path.
//
// Then the staging ceiling, charged for the whole batch at once: if the batch
// does not fit, [ErrStagingFull] is returned, not one of its records enters
// staging, and the caller still owns every one of them. The refusal is
// deliberately all-or-nothing rather than a partial fill, because "some
// unspecified prefix of your slice was accepted" is not an answer a caller can
// act on. It is returned bare rather than wrapped in a [BatchError], as are
// [ErrClosed] and any staging failure, because no row is at fault and naming one
// would be a lie.
//
// If the table declared an [TableConfig.OnAccept] callback, it is called once
// per record, in order, on this goroutine, after the whole batch is durable and
// outside every lock this writer holds. The first error one returns is returned
// from here wrapped in a [MirrorError], and it means what it means for Insert:
// the batch was accepted before the callback ran and will be committed, so
// re-inserting on it writes those records twice. The records after the failing
// one do not have the callback run for them, which is the same shape of answer
// Insert gives — the caller is told at the first thing that went wrong.
func (w *Writer[T]) InsertBatch(ctx context.Context, rows []T) error {
	// The lifecycle answer comes first, for the reason Insert gives: a closed
	// writer says so with the sentinel that means it, before the batch is looked
	// at, so ErrClosed's promise is true of every call rather than of the ones
	// carrying well-formed records.
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if len(rows) == 0 {
		return nil
	}

	// Every row is converted up front and outside the lock, exactly as Insert
	// does it for one, and for the same two reasons doubled: the work is pure
	// computation that the mutex has no business serializing, and a batch that
	// contains a record this table cannot represent must be refused before the
	// transaction opens rather than half way through it.
	payloads := make([][]byte, len(rows))
	var total int64
	for i := range rows {
		values, err := w.codec.Bind(rows[i])
		if err != nil {
			return errdef.BatchError{Index: i, Err: err}
		}
		if err := checkPoison(w.decl.Table(), w.desc, values); err != nil {
			return errdef.BatchError{Index: i, Err: err}
		}
		payload, err := canon.Encode(w.decl.Table(), w.desc, values)
		if err != nil {
			return errdef.BatchError{Index: i, Err: err}
		}
		payloads[i] = payload
		total += int64(len(payload))
	}

	if err := w.stageBatch(ctx, rows, payloads, total); err != nil {
		return err
	}

	// The mirror hook, per record and outside the lock, for the reasons Insert's
	// own call site sets out at length.
	if w.onAccept != nil {
		for i := range rows {
			if err := w.onAccept(rows[i]); err != nil {
				return errdef.MirrorError{Namespace: w.decl.Namespace(), Table: w.decl.Table(), Err: err}
			}
		}
	}

	return nil
}

// stageBatch is the locked half of [Writer.InsertBatch], and is [Writer.stage]
// for a whole slice: one ceiling charge, one durable staging transaction, and
// one trigger check for the records the batch adds to the in-memory batch.
//
// The trigger is checked once at the end rather than after each record, which is
// the only honest place for it: the records arrived together and became durable
// together, so there is no moment between two of them at which a threshold could
// meaningfully have been crossed.
func (w *Writer[T]) stageBatch(ctx context.Context, rows []T, payloads [][]byte, total int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Checked again under the lock that orders this against Close, exactly as
	// [Writer.stage] does: the check above decides which error a caller sees, and
	// this one keeps records out of a staging store whose writer is closing.
	if w.closed {
		return ErrClosed
	}
	if !w.store.acceptBatch(len(rows), total) {
		return fmt.Errorf("icelake: table %s.%s: %w", w.decl.Namespace(), w.decl.Table(), ErrStagingFull)
	}

	// One reading of the clock for the batch rather than one per record. The
	// field it fills is diagnostic and is never read by the write path, and the
	// records are becoming durable in one transaction — so one timestamp for
	// them is more truthful than a spread of them, as well as cheaper.
	now := w.clock.Now()
	appends := make([]staging.AppendRow, len(rows))
	for i, payload := range payloads {
		appends[i] = staging.AppendRow{
			Namespace:  w.decl.Namespace(),
			Table:      w.decl.Table(),
			Descriptor: w.desc,
			Payload:    payload,
			CreatedAt:  now,
		}
	}

	ids, err := w.store.staging.AppendBatch(ctx, appends)
	if err != nil {
		w.store.release(len(rows), total)

		return err
	}

	w.batch = slices.Grow(w.batch, len(ids))
	for i, id := range ids {
		w.batch = append(w.batch, flush.Row{ID: id, ByteLen: int64(len(payloads[i]))})
	}
	w.batchBytes += total
	// Accepted means durably staged, which is what has just happened to all of
	// them at once.
	w.rememberBatch(rows)

	if len(w.batch) >= w.settings.maxRecords || w.batchBytes >= w.settings.maxBytes {
		w.triggerLocked()
	}

	return nil
}

// stage is the locked half of [Writer.Insert]: the ceiling charge, the durable
// staging write, and the in-memory batch this record joins.
//
// It is its own function so that the lock is released by returning from it,
// which is what lets Insert call the caller's mirror hook afterwards without
// holding anything.
func (w *Writer[T]) stage(ctx context.Context, row T, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Checked again, under the lock that actually orders this against Close: the
	// check above is the one that decides which error a caller sees, and this
	// one is the one that keeps a record out of a staging store whose writer is
	// closing.
	if w.closed {
		return ErrClosed
	}
	if !w.store.accept(int64(len(payload))) {
		return fmt.Errorf("icelake: table %s.%s: %w", w.decl.Namespace(), w.decl.Table(), ErrStagingFull)
	}

	id, err := w.store.staging.Append(ctx, staging.AppendRow{
		Namespace:  w.decl.Namespace(),
		Table:      w.decl.Table(),
		Descriptor: w.desc,
		Payload:    payload,
		CreatedAt:  w.clock.Now(),
	})
	if err != nil {
		w.store.release(1, int64(len(payload)))

		return err
	}

	w.batch = append(w.batch, flush.Row{ID: id, ByteLen: int64(len(payload))})
	w.batchBytes += int64(len(payload))
	// Accepted means durably staged, which is exactly what has just happened
	// and what the record count in Status reports.
	w.remember(row)

	if len(w.batch) >= w.settings.maxRecords || w.batchBytes >= w.settings.maxBytes {
		w.triggerLocked()
	}

	return nil
}

// triggerLocked seals the current batch unless the flush floor has not elapsed.
//
// The floor does not drop or delay-forever anything: a trigger that arrives too
// soon simply leaves the batch accumulating, and the next trigger — from the
// timer if from nowhere else — seals one larger batch instead of several tiny
// ones back to back. That is the correction this whole library was built around
// applying, applied to itself.
//
// Every trigger the floor actually holds back is counted, and a trigger that
// found nothing to seal is deliberately not: the timer nudges the worker on
// every tick whether or not there is a batch, and counting those would turn an
// idle table into one that looks like it is fighting its own configuration.
func (w *Writer[T]) triggerLocked() {
	if len(w.batch) == 0 {
		return
	}
	if w.clock.Now().Sub(w.lastSealAt) < w.settings.floor {
		w.floorEngaged++

		return
	}
	w.sealLocked()
}

// sealLocked hands the current batch to the worker and swaps in a fresh empty
// one, immediately, so a flush never blocks an insert.
func (w *Writer[T]) sealLocked() {
	if len(w.batch) == 0 {
		return
	}

	batch := flush.Batch{Rows: w.batch}
	w.queuedRecords += len(w.batch)
	w.queuedBytes += w.batchBytes
	w.batch, w.batchBytes = nil, 0
	w.lastSealAt = w.clock.Now()

	w.worker.Enqueue(batch, false)
}

// committed is called by the worker once a batch's rows are pruned from
// staging. It runs on the worker goroutine and takes this writer's lock, which
// the worker never holds — the lock order is writer then worker, in one
// direction only.
func (w *Writer[T]) committed(records int, bytes int64) {
	w.mu.Lock()
	w.queuedRecords -= records
	w.queuedBytes -= bytes
	w.mu.Unlock()

	w.store.release(records, bytes)
}

// quarantined is called by the worker once a batch's rows have been marked
// unflushable. It runs on the worker goroutine, exactly as [Writer.committed]
// does, and takes the same lock in the same direction.
//
// It deliberately releases only this writer's own pending count, never the
// store's ceiling counters. The rows are still in staging — that is the whole
// point of quarantine — so they must keep occupying the space they occupy, and
// a quarantine that grows must eventually stop the writer loudly rather than
// silently freeing headroom. What is no longer true of them is that they are
// waiting to be flushed, which is what Pending reports.
func (w *Writer[T]) quarantined(records int, bytes int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.queuedRecords -= records
	w.queuedBytes -= bytes
}

// Pending reports what this writer has accepted but not yet committed: the
// records still in its in-memory batch plus every batch it has sealed and not
// yet had confirmed.
//
// It exists so a caller can throttle itself before it gets refused. The staging
// ceiling is the backstop, not the intended flow-control mechanism.
//
// A quarantined batch is not counted here, because it is no longer waiting for
// anything: it will never be flushed, and reporting it forever would turn a
// handful of unencodable records into a backlog that never drains. Its rows are
// still in staging and still count against the ceiling, where an operator can
// find them by their quarantine columns.
//
// [Writer.Status] reports the same two numbers, from these same fields under
// this same lock, alongside everything else a table knows about itself. This
// call stays because it is the one a caller reaches for on the hot path, and
// because it can be answered without copying two windows a throttling caller
// has no use for.
func (w *Writer[T]) Pending() (records int, bytes int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.pendingLocked()
}

// pendingLocked is what both [Writer.Pending] and [Writer.Status] report, in one
// place so the two cannot drift. The caller holds the lock.
func (w *Writer[T]) pendingLocked() (records int, bytes int64) {
	return len(w.batch) + w.queuedRecords, w.batchBytes + w.queuedBytes
}

// Flush seals whatever is in memory, however little, and returns only once
// every batch sealed at the moment of the call has been committed — or with the
// error of one that has been quarantined and therefore never will be.
//
// That synchronousness is the point: it makes Flush usable as a checkpoint, in
// a test or in a caller's own shutdown sequence, where a fire-and-forget
// trigger would not be. The flush floor is ignored — the floor exists to
// coalesce automatic triggers, never to delay a caller who asked.
//
// It is bounded by ctx. If ctx expires mid-drain, Flush returns an error and
// the undrained records stay safe in staging for the next run.
//
// A batch that cannot reach the bucket is retried inside this call, on the
// configured backoff schedule, and Flush returns the failure only once that
// whole cycle has been spent — reporting the first attempt would be a failure
// the caller had no business hearing about whenever the retry behind it
// succeeded. The batch is not lost when the cycle does fail: it keeps its place
// at the head of the queue, and a later Flush, or any other trigger, tries
// again with a fresh budget.
//
// A batch that was quarantined is the one failure that can be older than the
// call reporting it. Such a batch will never commit, so the first Flush or
// Close whose wait covers it returns its error even if the quarantine happened
// earlier, on a background trigger, with nobody waiting. It is reported exactly
// once and then the writer moves on: the table's queue is healthy again, and
// failing every later call forever over records no retry can save would make a
// bounded loss into a permanently broken writer. [Config.OnFlushError] is told
// independently, and the rows stay in staging, marked, for an operator.
func (w *Writer[T]) Flush(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()

		return ErrClosed
	}
	w.sealLocked()
	w.mu.Unlock()

	seq := w.worker.Sequence()
	w.worker.Kick()

	return w.worker.Wait(ctx, seq)
}

// Close stops the writer accepting records, seals whatever is in memory even if
// it is one row, drains every pending batch through upload and commit, and
// stops both goroutines.
//
// Subsequent Inserts return [ErrClosed]; so does a second Close. The drain is
// bounded by ctx: if it expires, Close returns an error and the records stay in
// staging, sealed, for the next run to pick up — the shutdown is bounded, but
// never at the cost of the durability guarantee.
//
// Like [Writer.Flush], it reports a batch quarantined earlier in this writer's
// life if nothing has reported it yet, rather than draining what is left and
// answering nil about records that will never be committed.
func (w *Writer[T]) Close(ctx context.Context) error { return w.close(ctx) }

// close is Close under the interface the Store drains through.
func (w *Writer[T]) close(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		// Wait for whoever got here first, so that a Store.Close arriving
		// behind a caller's own Close does not close the databases while that
		// drain is still using them.
		select {
		case <-w.closeDone:
		case <-ctx.Done():
		}

		return ErrClosed
	}
	w.closed = true
	w.sealLocked()
	w.mu.Unlock()

	defer close(w.closeDone)

	w.stopTimer()

	seq := w.worker.Sequence()
	w.worker.Kick()
	err := w.worker.Wait(ctx, seq)
	w.worker.Close(ctx)

	// The table is released only now, once the worker goroutine has exited for
	// certain — not when Close was entered.
	//
	// Releasing it earlier would let a writer be opened for this table while
	// this one's worker was still draining, and the new writer's own scan of
	// staging would hand it the batch already in flight. The two would then
	// both own it: the first commits and prunes, and the second is left holding
	// row ids that no longer exist, failing every attempt from then on. The
	// records survive that — the upload is idempotent and the catalog refuses
	// the second commit — but the writer does not. Holding the slot until the
	// worker is gone makes the overlap unrepresentable, while still leaving the
	// table free the moment Close returns, which is what a caller retrying a
	// failed shutdown needs.
	w.store.detach(w.decl.Namespace(), w.decl.Table())

	return err
}

// stopTimer stops the timer goroutine and waits for it, once.
func (w *Writer[T]) stopTimer() {
	w.stopOnce.Do(func() { close(w.timerStop) })
	<-w.timerDone
}

// runTimer is the timer goroutine: the interval trigger, and the guarantee that
// a flush trigger arrives even if inserts stop entirely.
//
// Every tick seals whatever is pending rather than only a batch that has
// already been waiting a full interval. That is what makes FlushInterval mean
// what it says — the longest a record may sit unflushed — instead of up to
// twice that, which is what measuring each batch's own age against a
// fixed-period timer would give in the worst case. The flush floor still
// applies, so a burst that lands just after a flush is coalesced rather than
// split.
//
// The tick also kicks the worker whether or not there was anything to seal.
// That is what lets a table that could not reach the bucket heal by itself: a
// batch whose previous attempt failed gets a fresh attempt on the next tick,
// without anything spinning against a dead endpoint in between.
func (w *Writer[T]) runTimer() {
	defer close(w.timerDone)

	timer := w.clock.NewTimer(w.settings.interval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C():
		case <-w.timerStop:
			return
		}

		w.mu.Lock()
		if !w.closed {
			w.triggerLocked()
		}
		w.mu.Unlock()

		w.worker.Kick()
		timer.Reset(w.settings.interval)
	}
}
