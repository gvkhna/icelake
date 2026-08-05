package flush

import (
	"context"
	"errors"
	"sync"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
)

// job is one queued batch and what has happened to it so far.
type job struct {
	batch Batch
	// seq is the sequence number the batch was enqueued under, which is how a
	// failure is reported to the waiters it concerns and to no others.
	seq uint64
	// replayed marks a batch recovered from a previous run, which arrives
	// already sealed and may already have been uploaded, or even committed,
	// before the process died.
	replayed bool
	// attempts counts the attempts made in the current cycle. It resets when a
	// new trigger starts a fresh attempt budget, which is what makes
	// FlushError.Attempts the number of attempts that budget spent.
	attempts int
	// attempted records that some attempt has been made on this batch in this
	// process, across every cycle. It is what tells a later cycle that the
	// batch may already be uploaded, or even committed, which the attempt count
	// alone can no longer say once it resets.
	attempted bool
}

// result is what one whole flush cycle did to a batch.
type result struct {
	// err is the failure the cycle ended on, already typed as a FlushError, or
	// nil if the batch committed.
	err error
	// quarantined marks a batch that was marked unflushable and taken out of
	// the queue, so the batches behind it are no longer waiting on something
	// that can never succeed.
	quarantined bool
	// report marks a failure the caller's flush-error callback must hear
	// about: a cycle that spent its whole attempt budget, a batch that was
	// quarantined, and a batch that could not even be marked. It is
	// deliberately false for a cycle cut short by shutdown — the budget was not
	// spent, nothing is stuck, and the error is already on its way back to
	// whoever called Close.
	report bool
}

// Worker drains one table's queue of batches, one at a time, in seal order.
//
// Serialization is structural: there is one goroutine, so a batch sealed later
// can never commit ahead of one sealed earlier, and a table's snapshots are
// therefore in the order its records were written. A batch that fails stays at
// the head of the queue — nothing behind it overtakes it, and nothing is lost,
// because its records stay in staging with their key intact until a commit is
// confirmed.
//
// Failure has three shapes here, and they are deliberately different. Inside
// one cycle, a transient failure is retried on the backoff schedule. When the
// cycle's budget is spent the batch keeps its place and the next trigger starts
// a fresh budget, so a table stuck on an outage heals by itself when the outage
// ends without spinning against a dead endpoint in between. And a batch that
// cannot be encoded at all is quarantined instead of retried, because the same
// bytes would fail the same way forever and the queue behind it would never
// move.
type Worker struct {
	opts   Options
	ctx    context.Context
	cancel context.CancelFunc

	// shapes caches recorded schema descriptors by fingerprint. Only the worker
	// goroutine reads or writes it.
	shapes map[string]*canon.Descriptor

	// wake carries a single pending signal. A buffered channel of size one is
	// exactly right: a signal that arrives while one is already pending is the
	// same instruction, and dropping it costs nothing.
	wake chan struct{}
	// done closes when the goroutine has exited.
	done chan struct{}

	mu sync.Mutex
	// queue holds the batches still to flush, head first.
	queue []*job
	// enqueued counts every batch ever queued, and completed counts every batch
	// committed. A waiter asks for a sequence number and waits for completed to
	// reach it.
	enqueued  uint64
	completed uint64
	// failure is the error from the head batch's most recent cycle, cleared by
	// the next success, and failures counts how many cycles have failed. The
	// count is what makes the error *generational*: a waiter records it on
	// arrival and only ever reports a failure recorded after that, so a caller
	// that healed whatever was broken and asked again is answered by its own
	// attempt rather than by the one that failed before it called.
	//
	// A failure is recorded when a cycle ends, never when one attempt inside it
	// fails. Reporting the first transient failure to a waiter would be a lie
	// whenever the retry that followed it succeeded a moment later.
	failure  error
	failures uint64
	// failureSeq is the sequence number of the batch the failure belongs to. A
	// waiter is only answered by a failure of a batch it is actually waiting
	// for: a later batch failing says nothing about an earlier one that has
	// already committed.
	failureSeq uint64
	// quarantined is the error of a batch that was marked unflushable, held
	// until a wait that covers it takes it, and quarantinedSeq is the sequence
	// number that batch was enqueued under.
	//
	// It is deliberately not the generational failure above. A failed attempt
	// is an event a later attempt can undo, so a caller that arrives afterwards
	// is answered by its own attempt; a quarantine is terminal, so a caller
	// that arrives afterwards must not be told the batch committed. It
	// therefore survives every later success and is cleared only by being
	// reported, exactly once.
	//
	// One slot is enough even though a second quarantine would overwrite an
	// unreported first: before the next batch can fail at all, its prepare
	// step does a staging read, a batch hash and a fsynced Seal transaction,
	// which is orders of magnitude longer than a woken waiter needs to take
	// the notice under this lock. A timing argument rather than a structural
	// one, so it is written down here rather than assumed silently.
	quarantined    error
	quarantinedSeq uint64
	// stopping means no more work will be accepted and the goroutine should
	// exit once the queue is drained or the context is cancelled.
	stopping bool
	// changed is closed and replaced on every state change, which is how
	// waiters bounded by their own context are woken without a condition
	// variable they could not select on.
	changed chan struct{}
}

// NewWorker starts a worker for one table. The caller must Close it.
func NewWorker(opts Options) *Worker {
	ctx, cancel := context.WithCancel(opts.Ctx)
	w := &Worker{
		opts:    opts,
		ctx:     ctx,
		cancel:  cancel,
		shapes:  make(map[string]*canon.Descriptor),
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		changed: make(chan struct{}),
	}
	go w.run()

	return w
}

// Enqueue adds a batch to the end of the queue and returns the sequence number
// that identifies it to [Worker.Wait]. Enqueuing never blocks: the queue is
// bounded by the staging ceiling rather than by a depth of its own, because a
// queued batch's records are still staged and therefore already counted.
func (w *Worker) Enqueue(b Batch, replayed bool) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.enqueued++
	w.queue = append(w.queue, &job{batch: b, seq: w.enqueued, replayed: replayed})
	w.broadcastLocked()
	w.signal()

	return w.enqueued
}

// Sequence returns the sequence number of the most recently enqueued batch,
// which is what a drain waits for.
func (w *Worker) Sequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.enqueued
}

// Kick asks the worker to look at its queue again. It is what turns a flush
// trigger into a fresh attempt at a batch whose previous attempt failed, so a
// table stuck on an outage heals by itself when the outage ends, without ever
// spinning against a dead endpoint in between.
func (w *Worker) Kick() { w.signal() }

// Wait blocks until every batch up to seq has left the queue committed, one of
// them has failed a whole cycle, one of them has been quarantined, or ctx is
// done.
//
// A failed cycle is returned rather than waited on further: the batch stays
// queued for the next trigger, and the caller — Flush or Close — learns
// synchronously what happened instead of waiting out a schedule it never asked
// for. What Wait does absorb is the retries *inside* one cycle, because
// reporting an attempt the worker is about to retry would be a failure the
// caller has no business hearing about if the next attempt succeeds.
//
// A quarantine is the one failure that outlives the moment it happened. The
// batch will never commit, so a wait that covers it must never answer nil —
// including one that starts long after the quarantine, which is the normal case
// when the trigger that met it came from the timer rather than from a caller.
// It is delivered to the first such wait and then cleared: reported exactly
// once, after which the writer moves on, because a table whose queue is healthy
// again must not keep failing every Flush and Close for the rest of the
// process's life over records no retry can save. The callback is told
// independently, so a caller that wired one hears about it either way.
func (w *Worker) Wait(ctx context.Context, seq uint64) error {
	w.mu.Lock()
	since := w.failures
	w.mu.Unlock()

	// answer reads the state once, reporting whether it settles this wait. It
	// runs under the lock, which is what makes taking the quarantine notice a
	// single atomic step: exactly one waiter can ever receive it.
	answer := func() (error, bool) {
		// Checked before anything else, and guarded by the sequence number it
		// belongs to: a waiter whose own batches all committed before the
		// quarantined one was enqueued is not concerned by it, and the notice
		// stays for whoever is.
		if w.quarantined != nil && w.quarantinedSeq <= seq {
			err := w.quarantined
			w.quarantined, w.quarantinedSeq = nil, 0

			return err, true
		}
		if w.failures > since && w.failure != nil && w.failureSeq <= seq {
			return w.failure, true
		}
		if w.completed >= seq {
			return nil, true
		}

		return nil, false
	}

	for {
		w.mu.Lock()
		if err, settled := answer(); settled {
			w.mu.Unlock()

			return err
		}
		changed := w.changed
		w.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-w.done:
			// The worker has exited; take one more look so a batch that
			// committed just before it stopped is reported as committed.
			w.mu.Lock()
			err, settled := answer()
			w.mu.Unlock()
			if settled {
				return err
			}

			return context.Canceled
		}
	}
}

// Close stops the worker and waits for its goroutine to exit.
//
// It does not drain: draining is [Worker.Wait]'s job, bounded by the caller's
// own context, and by the time Close is reached the caller has already decided
// whether to wait or to give up. If ctx expires while the goroutine is still
// finishing an upload or a commit, that work is cancelled — the batch stays
// sealed in staging, safe for the next run, which is exactly the tradeoff a
// bounded shutdown makes.
func (w *Worker) Close(ctx context.Context) {
	w.mu.Lock()
	w.stopping = true
	w.mu.Unlock()
	w.signal()

	select {
	case <-w.done:
	case <-ctx.Done():
		w.cancel()
		<-w.done
	}
	w.cancel()
}

// run is the worker goroutine.
func (w *Worker) run() {
	defer close(w.done)

	for {
		j, ok := w.head()
		if !ok {
			return
		}
		if j == nil {
			if w.idle() {
				return
			}

			continue
		}

		res := w.cycle(j)
		if w.opts.OnCycle != nil {
			// Before anything else the outcome reaches. Before finish, because
			// finish is what wakes a Flush or a Close, and a caller whose Flush
			// has returned must not then read a status that has not heard about
			// the commit it just waited for. And before the caller's own
			// callback, because this is the writer's own bookkeeping and must
			// not sit behind a hook icelake does not control the duration of.
			w.opts.OnCycle(res.err)
		}
		w.finish(j, res)
		if res.report {
			// Recorded first, reported second: a caller waiting on this batch
			// hears about the failure without waiting for a callback icelake
			// does not control.
			w.notify(res)
		}
		if res.err != nil && !res.quarantined && w.idle() {
			// A batch whose whole cycle failed is retried by the next trigger,
			// not immediately: spinning against an endpoint that is down would
			// turn an outage into a flood of requests. A quarantined batch is
			// the opposite case — it has left the queue, and whatever was
			// behind it should be attempted now rather than waited for.
			return
		}
	}
}

// cycle makes one flush cycle's worth of attempts at a batch: up to the
// configured budget, sleeping the backoff schedule in between, stopping early
// for a batch that can never be encoded and for a shutdown.
//
// The sleep goes through the injected clock and takes the worker's own context,
// so a shutdown interrupts a sleeping retry immediately rather than waiting out
// a sixty-second backoff. The batch stays sealed in staging either way.
func (w *Worker) cycle(j *job) result {
	j.attempts = 0

	for {
		j.attempts++
		kind, err := w.process(j)
		j.attempted = true

		switch kind {
		case failureNone:
			return result{}
		case failureEncode:
			return w.quarantine(j, err)
		case failureTransient:
		}

		if w.ctx.Err() != nil {
			return result{err: err}
		}
		if j.attempts >= w.opts.Retry.attempts() {
			return result{err: err, report: true}
		}
		if w.opts.Clock.Sleep(w.ctx, w.opts.Retry.delay(j.attempts, jitter)) != nil {
			return result{err: err}
		}
	}
}

// quarantine marks a batch that cannot be encoded, whole, and takes it out of
// the queue.
//
// This is the second line ARCHITECTURE.md describes, and it is narrow on
// purpose: the poison check at Insert is what catches a bad *value*, and this
// catches a *batch* the encoder itself refuses — a payload that will not decode
// under the shape recorded for it, a shape that cannot be remapped onto the
// current one. Retrying either forever would wedge the table behind a batch
// that can never succeed, and dropping it would be silent data loss, so the
// rows are marked in place instead: excluded from every future replay, still
// counting toward the staging ceiling so a growing quarantine eventually stops
// the writer loudly, and left as plain rows an operator can read, export or
// delete by hand.
//
// The whole batch is marked or none of it is, which the staging layer enforces:
// marking part of a sealed batch would leave a batch whose stored key no longer
// names its own bytes. If the marking itself fails, the batch stays queued and
// the failure is reported as any other — the next trigger will try again.
func (w *Worker) quarantine(j *job, cause error) result {
	ids := make([]int64, len(j.batch.Rows))
	for i, r := range j.batch.Rows {
		ids[i] = r.ID
	}

	if err := w.opts.Staging.Quarantine(w.ctx, ids, w.opts.Clock.Now(), cause); err != nil {
		// A double fault: a batch that cannot be encoded, in a database that
		// would not record that. It is reported like any other failure — the
		// caller hears about it and the batch keeps its place for the next
		// trigger — and the attempt count is the attempt that met it rather
		// than a budget nothing spent, because an unencodable batch is never
		// retried inside its cycle.
		return result{err: w.opts.flushErr(j.batch.Key, j.batch.Records(), j.attempts, err), report: true}
	}
	if w.opts.OnQuarantined != nil {
		w.opts.OnQuarantined(j.batch.Records(), j.batch.Bytes())
	}

	// The error that crosses to the caller says what actually happened to the
	// batch: not "staged and will be retried", which is every other failure's
	// meaning, but marked unflushable and waiting for a person.
	var flushErr errdef.FlushError
	if errors.As(cause, &flushErr) {
		flushErr.Quarantined = true

		return result{err: flushErr, quarantined: true, report: true}
	}

	return result{err: cause, quarantined: true, report: true}
}

// notify hands a failure to the caller's flush-error callback.
//
// The callback is notification and nothing else: the batch it describes is
// already safe in staging, and nothing icelake does next depends on what the
// callback returns. It is contained in three ways. It runs after the failure
// has been recorded, so a slow callback cannot delay the error reaching a Flush
// or a Close. A panic in it is recovered rather than taken as the process's
// last act, because a caller's logging bug must not be able to kill a writer on
// a goroutine the caller cannot see. And the worker stops waiting for it when
// the store's context is done, so a callback that never returns cannot hold a
// shutdown open — it keeps only its own goroutine, which is the one thing
// icelake cannot take back.
//
// What is *not* contained, and is the caller's side of the bargain rather than
// a bug here, is how long it takes: this worker waits for the callback before
// it looks at its queue again, so a callback that takes a second delays this
// table's next flush by a second. That is why the documented rule is that it
// must not block. The alternative — dispatching it and moving on — would trade
// a stall a caller controls for notifications that could arrive out of order
// and outlive the writer they describe.
//
// What the callback must not do is re-enter the writer. Flush and Close wait on
// this goroutine, so calling either from inside the callback deadlocks — the
// reason is the worker it would be waiting for, not any lock icelake holds
// here, which is none.
func (w *Worker) notify(res result) {
	if w.opts.OnFlushError == nil {
		return
	}
	var flushErr errdef.FlushError
	if !errors.As(res.err, &flushErr) {
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			// Deliberately swallowed: there is no caller left to hand it to,
			// and the flush path it interrupted has nothing to do differently.
			_ = recover()
		}()
		w.opts.OnFlushError(flushErr)
	}()

	select {
	case <-done:
	case <-w.ctx.Done():
	}
}

// head returns the batch to work on, nil if there is none, and false if the
// worker should exit.
func (w *Worker) head() (*job, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.ctx.Err() != nil {
		return nil, false
	}
	if len(w.queue) > 0 {
		return w.queue[0], true
	}
	if w.stopping {
		return nil, false
	}

	return nil, true
}

// idle waits for the next signal, reporting true if the worker should exit.
func (w *Worker) idle() bool {
	w.mu.Lock()
	stopping := w.stopping
	w.mu.Unlock()
	if stopping {
		return true
	}

	select {
	case <-w.wake:
	case <-w.ctx.Done():
		return true
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.stopping && len(w.queue) == 0
}

// finish records the outcome of one cycle and wakes every waiter.
//
// A batch leaves the queue when it commits and when it is quarantined, because
// both are final: one succeeded, and the other can never succeed. Only a
// transient failure keeps its place at the head, which is what stops anything
// sealed later from committing ahead of it.
//
// The two failures are recorded in two different places for that same reason. A
// transient one is generational — it belongs to the attempt that produced it,
// and a caller arriving afterwards gets its own answer — while a quarantine is
// terminal and is held until a wait that covers it takes it, so that no caller
// is ever told a quarantined batch committed.
func (w *Worker) finish(j *job, res result) {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch {
	case res.quarantined:
		w.quarantined, w.quarantinedSeq = res.err, j.seq
	case res.err != nil:
		w.failure, w.failureSeq = res.err, j.seq
		w.failures++
		w.broadcastLocked()

		return
	}

	if len(w.queue) > 0 && w.queue[0] == j {
		w.queue = w.queue[1:]
	}
	w.completed++
	if res.err == nil {
		w.failure, w.failureSeq = nil, 0
	}
	w.broadcastLocked()
}

// signal nudges the goroutine without blocking if a nudge is already pending.
func (w *Worker) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// broadcastLocked wakes every waiter. The caller holds the lock.
func (w *Worker) broadcastLocked() {
	close(w.changed)
	w.changed = make(chan struct{})
}
