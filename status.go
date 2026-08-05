package icelake

import "time"

// The two ring buffers [WriterStatus] carries are deliberately tiny and fixed.
// They exist so a person can see what a table is actually doing without
// standing up a query engine, not so anything can be reconstructed from them: a
// window that grew with traffic would be a time series and a record store, and
// this library has decided twice over not to be either.
const (
	// recentFlushes is how many commit times [Writer.Status] keeps.
	recentFlushes = 20
	// recentRecords is how many accepted records [Writer.Status] keeps.
	recentRecords = 5
)

// WriterStatus is a read-only snapshot of what one table is actually doing.
//
// It exists for one specific reason, and the reason is worth knowing because it
// is what bounds the type: object storage bills every write operation, and the
// entire premise of batching is keeping that count low. A bug or a
// misconfiguration that quietly causes far more, or far smaller, flushes than a
// table's configuration intends is a direct cost problem this design otherwise
// has no way to catch. RecordsAccepted over FlushesCommitted is roughly a
// table's average batch size — roughly, because the two are not counted over
// the same set of batches: a batch replayed from a previous run is committed by
// this writer without ever having been accepted by it, and an explicit Flush or
// Close commits a batch no threshold asked for. As a ratio to watch against a
// configured threshold it is the right number; as an identity it is not one.
// A table whose average is far below its threshold is either misconfigured or
// hitting the flush floor constantly, which FlushFloorEngaged says outright.
//
// It is not general observability, which stays out of scope: there is no
// metrics interface, no exporter, no levels. Everything here is about
// operations icelake itself issues. It has, and can have, no visibility into
// what DuckDB, PyIceberg or anything else querying the same bucket costs, since
// none of that goes through icelake at all.
//
// The snapshot is taken under the writer's own lock, so its numbers are one
// moment rather than several, and the two slices are the caller's own to keep
// or modify. What that copy cannot do is reach inside the records themselves:
// see RecentRecords.
type WriterStatus[T any] struct {
	// PendingRecords and PendingBytes are what this writer has accepted and not
	// yet had committed — the current in-memory batch plus every batch sealed
	// and not yet confirmed. They are exactly what [Writer.Pending] returns,
	// read from the same fields under the same lock, so the two can never
	// disagree.
	PendingRecords int
	PendingBytes   int64

	// RecordsAccepted is how many records this writer has accepted since it was
	// opened. A record is accepted once it is durably staged, so a record whose
	// OnAccept callback then failed is counted, and a record refused by the
	// staging ceiling or by the poison check is not.
	RecordsAccepted uint64

	// FlushesCommitted is how many batches this writer has had committed since
	// it was opened, including batches replayed from a previous run.
	FlushesCommitted uint64

	// LastCommitAt is when the most recent batch was committed, and the zero
	// time until one has been.
	LastCommitAt time.Time

	// LastFlushErr is the failure the most recent flush cycle ended on, and nil
	// when the most recent cycle committed. It is a report, not a delivery: a
	// failure a caller must act on also reaches [Config.OnFlushError] and the
	// next Flush or Close, and neither of those depends on anyone reading this.
	LastFlushErr error

	// RecentFlushTimes is when the last few batches were committed, oldest
	// first — a fixed-size window of at most twenty, so a caller can compute a
	// real recent flush rate without icelake keeping a time series of its own.
	RecentFlushTimes []time.Time

	// RecentRecords is the last few accepted records, by value, oldest first —
	// a fixed-size window of at most five.
	//
	// It answers "what is this table actually storing right now" by direct
	// inspection rather than by trusting a claim about it, the way a developer
	// might tail a log. It is deliberately not a query surface: it is tiny, it
	// is unindexed, it is memory-only, and it does not survive a restart.
	//
	// The copy is shallow, and for a declaration type with a reference field —
	// a []byte payload column being the obvious one — that has two consequences
	// worth knowing, neither of which icelake can fix generically, because
	// copying an arbitrary T deeply is not something a library can do:
	//
	// A record here shares memory with the value the caller inserted. Writing
	// through a []byte field of a returned record changes what the window
	// shows, and, if the caller still holds that slice, changing the caller's
	// slice changes the window. Neither can reach what was staged — the record
	// was encoded to canonical bytes before this window ever saw it, and that
	// is what gets flushed — so this affects only what a later Status displays.
	//
	// And the window keeps those references alive: up to five records' worth of
	// whatever they point at cannot be collected until five more records
	// displace them. For a table whose rows carry large payloads that is a real
	// if small retention, and it is the price of the window existing at all.
	RecentRecords []T

	// FlushFloorEngaged is how many times the flush floor has held a trigger
	// back on this table: a size or interval trigger arrived with records
	// waiting, less than [Config.MinFlushInterval] after the last seal, and the
	// batch kept accumulating instead of being sealed then and there.
	//
	// A trigger that found nothing to seal is not counted, because the floor
	// held nothing back. A table whose count climbs steadily is configured for
	// less traffic than it is getting: nothing is being lost or delayed
	// indefinitely — the next trigger past the floor seals one larger batch —
	// but its thresholds are no longer the thing deciding when it flushes.
	FlushFloorEngaged uint64
}

// Status returns a snapshot of what this table is doing: what is pending, what
// has been accepted and committed since the writer opened, when the last few
// commits happened, the last few records accepted, and how often the flush
// floor has held a trigger back.
//
// It is a read, safe from any goroutine and safe to call from inside
// [TableConfig.OnAccept]. It stays readable after [Writer.Close], which is
// deliberate: the numbers a table finished on are the ones worth looking at.
func (w *Writer[T]) Status() WriterStatus[T] {
	w.mu.Lock()
	defer w.mu.Unlock()

	records, bytes := w.pendingLocked()

	return WriterStatus[T]{
		PendingRecords:    records,
		PendingBytes:      bytes,
		RecordsAccepted:   w.accepted,
		FlushesCommitted:  w.flushes,
		LastCommitAt:      w.lastCommitAt,
		LastFlushErr:      w.lastFlushErr,
		RecentFlushTimes:  append([]time.Time(nil), w.flushTimes...),
		RecentRecords:     append([]T(nil), w.records...),
		FlushFloorEngaged: w.floorEngaged,
	}
}

// remember adds one accepted record to the small window [Writer.Status]
// reports. The caller holds the lock.
func (w *Writer[T]) remember(row T) {
	w.accepted++
	w.records = append(w.records, row)
	if len(w.records) > recentRecords {
		w.records = w.records[len(w.records)-recentRecords:]
	}
}

// rememberBatch is [Writer.remember] for a whole batch at once, and it is a
// separate function only so that a large batch does not walk its own rows to
// keep a handful of them.
//
// Everything the window can hold is in the batch's own tail, so the loop starts
// there rather than at the front: a ten-thousand-row batch appends the last few
// and counts the rest, instead of appending ten thousand values into a slice
// that discards all but the same few. What a reader of [WriterStatus] sees is
// identical either way — the last recentRecords records this writer accepted, in
// order — which is what makes this an implementation detail rather than a change
// of behaviour. The caller holds the lock.
func (w *Writer[T]) rememberBatch(rows []T) {
	w.accepted += uint64(len(rows))

	from := 0
	if len(rows) > recentRecords {
		from = len(rows) - recentRecords
	}
	w.records = append(w.records, rows[from:]...)
	if len(w.records) > recentRecords {
		w.records = w.records[len(w.records)-recentRecords:]
	}
}

// noteCommit records that a batch committed, at the time the injected clock
// says. The caller holds the lock.
func (w *Writer[T]) noteCommit(at time.Time) {
	w.flushes++
	w.lastCommitAt = at
	w.flushTimes = append(w.flushTimes, at)
	if len(w.flushTimes) > recentFlushes {
		w.flushTimes = w.flushTimes[len(w.flushTimes)-recentFlushes:]
	}
}

// cycleEnded records the outcome of one whole flush cycle: a cycle that ended
// with the batch committed counts a commit and clears the last error, and one
// that failed records the failure.
//
// It is called on the worker goroutine, once per cycle rather than once per
// attempt inside one, for the same reason the worker reports failures that way:
// an attempt the worker is about to retry is not a failure the reader of a
// status surface should be looking at.
//
// Both halves happen under one acquisition of the lock, deliberately. Recording
// the commit in one place and the error in another would leave a window in
// which a reader saw a commit counted against the previous cycle's error still
// standing — a snapshot of two moments, which is exactly what this type
// promises not to be.
func (w *Writer[T]) cycleEnded(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastFlushErr = err
	if err == nil {
		w.noteCommit(w.clock.Now())
	}
}
