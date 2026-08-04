package icelake_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TestOnAcceptFiresOncePerAcceptedRecord is the first clause of TESTING.md's
// scenario 9: the mirror hook fires exactly once per accepted record,
// synchronously, before Insert returns.
//
// The check inside the loop is the part that matters. Asserting the collected
// rows only at the end would pass just as happily if the callback were buffered
// and drained at flush time, which is the opposite of the guarantee: the whole
// value of the hook is that the caller's mirror write happens inside the call
// that accepted the record, so there is one decision point rather than two.
func TestOnAcceptFiresOncePerAcceptedRecord(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	// The mirror: the simplest possible stand-in for a caller's own hot store,
	// written to from inside Insert and read by the test after every call.
	var mirrored []Fill

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: "market",
		Table:     "fills",
		OnAccept: func(row Fill) error {
			mirrored = append(mirrored, row)

			return nil
		},
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	const records = 25
	for i := range records {
		row := makeFill(i)
		if err := writer.Insert(ctx, row); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}

		if len(mirrored) != i+1 {
			t.Fatalf("after Insert %d the mirror holds %d records, want %d; the callback is buffered or deferred rather than synchronous",
				i, len(mirrored), i+1)
		}
		if mirrored[i] != row {
			t.Fatalf("the mirror's record %d is %+v, want the row that was just inserted %+v", i, mirrored[i], row)
		}
	}

	// A record the writer refuses never reached staging, so it was never
	// accepted, so the mirror must not have been told about it. Which refusal it
	// is belongs to scenario 7; what is being checked here is only that the
	// callback's "once per accepted record" means accepted.
	poisoned := makeFill(records)
	poisoned.Symbol = string([]byte{0xff, 0xfe})
	if err := writer.Insert(ctx, poisoned); err == nil {
		t.Fatal("Insert accepted a record with an invalid-UTF-8 string, want a refusal")
	}
	if len(mirrored) != records {
		t.Errorf("the mirror holds %d records after a refused Insert, want the %d that were accepted", len(mirrored), records)
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The mirror is the record of what was accepted; the table is the record of
	// what was archived. They must agree.
	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, "SELECT count(*) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total)
	if total != len(mirrored) {
		t.Errorf("the table holds %d rows and the mirror %d; the two sides of the one decision point disagree", total, len(mirrored))
	}
}

// TestOnAcceptErrorSurfacesButTheRecordIsStillArchived is scenario 9's second
// clause, and the concrete proof of the decision behind the whole hook: a
// failing mirror never costs icelake's own durability.
//
// The callback fails for a specific subset of rows. Insert returns an error for
// exactly those rows and for no others — and every record, including every one
// whose callback failed, is read back out of the table afterwards by DuckDB.
// The error is its own type for the reason that matters operationally: a caller
// that treated it like a refusal and re-inserted the record would write it
// twice.
func TestOnAcceptErrorSurfacesButTheRecordIsStillArchived(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	// The caller's own error, from the caller's own code: what a mirror write
	// against a local store that is down would produce.
	mirrorDown := errors.New("the local mirror is unavailable")
	failing := func(i int) bool { return i%7 == 3 }

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: "market",
		Table:     "fills",
		OnAccept: func(row Fill) error {
			if failing(int(row.SequenceID)) {
				return mirrorDown
			}

			return nil
		},
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	const records = 60
	var failed int
	for i := range records {
		err := writer.Insert(ctx, makeFill(i))

		if !failing(i) {
			if err != nil {
				t.Fatalf("Insert %d, whose callback succeeded, returned %v", i, err)
			}

			continue
		}

		failed++
		if !errors.Is(err, mirrorDown) {
			t.Fatalf("Insert %d returned %v, want the error its callback returned, reachable through errors.Is", i, err)
		}
		var mirror icelake.MirrorError
		if !errors.As(err, &mirror) {
			t.Fatalf("Insert %d returned %v (%T), want a MirrorError: the record was accepted, so a caller must be able to tell this from a refusal",
				i, err, err)
		}
		if mirror.Namespace != "market" || mirror.Table != "fills" {
			t.Errorf("MirrorError names %s.%s, want market.fills", mirror.Namespace, mirror.Table)
		}
	}
	if failed == 0 {
		t.Fatal("no callback failed; the test proves nothing")
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Nothing was held back for the rows whose mirror write failed: staging is
	// empty and the table holds every record exactly once.
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after a clean close, want 0", got)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))

	var total, distinct int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*), count(DISTINCT sequence_id) FROM %s", table), &total, &distinct)
	if total != records || distinct != records {
		t.Errorf("the table holds %d rows (%d distinct), want %d of each", total, distinct, records)
	}

	// And specifically the rows whose callback failed, named rather than
	// inferred from the total.
	var committed int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE sequence_id %% 7 = 3", table), &committed)
	if committed != failed {
		t.Errorf("%d of the %d records whose mirror callback failed are in the table; a failing mirror must never cost a record",
			committed, failed)
	}
}

// TestOnAcceptDoesNotHoldTheWriter proves the two re-entry promises the
// callback's documentation makes, both of which follow from it running outside
// every lock the writer holds.
//
// They are worth a test rather than a comment because both are the kind of
// claim that is true until someone moves one line: running the callback inside
// the lock that orders the staging write would break the first outright and
// deadlock the second, and neither failure would show up anywhere else in this
// suite.
func TestOnAcceptDoesNotHoldTheWriter(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	t.Run("a slow callback delays only its own goroutine", func(t *testing.T) {
		// The first record's callback parks until the test releases it. Every
		// other record's returns at once.
		parked := make(chan struct{})
		entered := make(chan struct{})
		var once sync.Once

		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
			Namespace: "market",
			Table:     "slow_callback",
			OnAccept: func(row Fill) error {
				if row.SequenceID != 0 {
					return nil
				}
				once.Do(func() { close(entered) })
				<-parked

				return nil
			},
		})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		defer func() {
			if err := writer.Close(ctx); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()

		stuck := make(chan error, 1)
		go func() { stuck <- writer.Insert(ctx, makeFill(0)) }()
		<-entered

		// A second goroutine, inserting while the first is parked inside its
		// callback. It must complete: the callback holds nothing.
		done := make(chan error, 1)
		go func() { done <- writer.Insert(ctx, makeFill(1)) }()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("the insert that ran alongside a parked callback returned %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("an insert made while another goroutine sat inside its OnAccept callback never returned; the callback is holding the writer's lock")
		}

		close(parked)
		if err := <-stuck; err != nil {
			t.Errorf("the parked insert returned %v", err)
		}
	})

	t.Run("the callback may call back into the writer", func(t *testing.T) {
		var writer *icelake.Writer[Fill]
		var seen icelake.WriterStatus[Fill]

		w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
			Namespace: "market",
			Table:     "reentrant_callback",
			OnAccept: func(_ Fill) error {
				// Every read the documentation says is safe, plus the one write
				// that is: Flush waits on the worker, not on this goroutine.
				seen = writer.Status()
				_, _ = writer.Pending()

				return writer.Flush(ctx)
			},
		})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		writer = w

		done := make(chan error, 1)
		go func() { done <- writer.Insert(ctx, makeFill(0)) }()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("an Insert whose callback called Status, Pending and Flush returned %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("an Insert whose callback called back into the writer never returned; re-entering the writer from OnAccept deadlocks")
		}

		// The status the callback saw was taken after its own record was
		// staged, which is what "the callback runs after the record is
		// accepted" means from inside the callback.
		if seen.RecordsAccepted != 1 {
			t.Errorf("the callback saw Status.RecordsAccepted = %d, want 1: its own record was already accepted", seen.RecordsAccepted)
		}
		if err := writer.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

// TestStatusAgreesWithReality is scenario 9's third clause: the numbers Status
// reports are the numbers that actually happened.
//
// Every one of them is checked against something the test knows independently —
// how many records it inserted, how many batches that must have made at the
// configured threshold, and which rows it wrote last.
func TestStatusAgreesWithReality(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	cfg.FlushMaxRecords = 10
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// A writer that has done nothing reports nothing, rather than a plausible
	// zero-ish shape assembled from whatever was lying around.
	if empty := writer.Status(); empty.RecordsAccepted != 0 || empty.FlushesCommitted != 0 ||
		!empty.LastCommitAt.IsZero() || empty.LastFlushErr != nil ||
		len(empty.RecentFlushTimes) != 0 || len(empty.RecentRecords) != 0 {
		t.Errorf("a writer that has accepted nothing reports %+v", empty)
	}

	// Below the threshold: nothing has been sealed, so what is pending is
	// exactly what was inserted — and Pending and Status must say the same
	// thing, since one is documented as a subset of the other.
	const staged = 3
	for i := range staged {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	pendingRecords, pendingBytes := writer.Pending()
	status := writer.Status()
	if status.PendingRecords != pendingRecords || status.PendingBytes != pendingBytes {
		t.Errorf("Status reports (%d records, %d bytes) pending and Pending reports (%d, %d)",
			status.PendingRecords, status.PendingBytes, pendingRecords, pendingBytes)
	}
	if status.PendingRecords != staged || status.RecordsAccepted != staged {
		t.Errorf("after %d inserts below the threshold, Status reports %d pending and %d accepted",
			staged, status.PendingRecords, status.RecordsAccepted)
	}

	const records = 30
	const perBatch = 10 // cfg.FlushMaxRecords
	for i := staged; i < records; i++ {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	// Flush rather than a sleep: it returns only once every batch sealed by the
	// inserts above has been committed, so the counts below are read at a
	// moment the test defined rather than raced for.
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	status = writer.Status()

	if status.RecordsAccepted != records {
		t.Errorf("Status.RecordsAccepted = %d, want the %d records inserted", status.RecordsAccepted, records)
	}
	if want := uint64(records / perBatch); status.FlushesCommitted != want {
		t.Errorf("Status.FlushesCommitted = %d, want %d (%d records at a threshold of %d)",
			status.FlushesCommitted, want, records, perBatch)
	}
	if status.PendingRecords != 0 || status.PendingBytes != 0 {
		t.Errorf("Status reports (%d, %d) pending after a Flush that returned, want (0, 0)",
			status.PendingRecords, status.PendingBytes)
	}
	if status.LastFlushErr != nil {
		t.Errorf("Status.LastFlushErr = %v after three clean commits, want nil", status.LastFlushErr)
	}
	if status.FlushFloorEngaged != 0 {
		t.Errorf("Status.FlushFloorEngaged = %d with the floor configured out of the way, want 0", status.FlushFloorEngaged)
	}

	// One commit time per commit, oldest first, and the last of them is the one
	// LastCommitAt names.
	if len(status.RecentFlushTimes) != int(status.FlushesCommitted) {
		t.Errorf("Status.RecentFlushTimes holds %d times for %d commits", len(status.RecentFlushTimes), status.FlushesCommitted)
	}
	for i := 1; i < len(status.RecentFlushTimes); i++ {
		if status.RecentFlushTimes[i].Before(status.RecentFlushTimes[i-1]) {
			t.Errorf("Status.RecentFlushTimes[%d] is before [%d]; the window is not oldest-first", i, i-1)
		}
	}
	if n := len(status.RecentFlushTimes); n > 0 && !status.RecentFlushTimes[n-1].Equal(status.LastCommitAt) {
		t.Errorf("Status.LastCommitAt is %v and the newest commit time in the window is %v",
			status.LastCommitAt, status.RecentFlushTimes[n-1])
	}

	// The record window is the last five accepted rows, by value, in order —
	// with far more than five written, so this is the window having dropped the
	// earlier ones rather than simply never having had them.
	if len(status.RecentRecords) != 5 {
		t.Fatalf("Status.RecentRecords holds %d records after %d inserts, want the last 5", len(status.RecentRecords), records)
	}
	for i, got := range status.RecentRecords {
		if want := makeFill(records - 5 + i); got != want {
			t.Errorf("Status.RecentRecords[%d] is %+v, want %+v", i, got, want)
		}
	}

	// The snapshot is a copy: what a caller does with it cannot reach back into
	// the writer.
	status.RecentRecords[0] = makeFill(999)
	if again := writer.Status(); again.RecentRecords[0] == makeFill(999) {
		t.Error("mutating a returned snapshot changed the writer's own window; Status must hand back a copy")
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestFlushFloorEngagedCountsOnlyHeldBackTriggers proves the other half of the
// counter's definition: an idle table's counter does not move.
//
// This is the failure the definition exists to prevent, and it is invisible to
// every other test here. The timer nudges the worker on every tick whether or
// not there is anything to seal — that is deliberate, and is what lets a stuck
// table heal — so a counter that incremented on any trigger arriving before the
// floor had elapsed would climb steadily on a table nobody is writing to, and
// the number an operator reads as "your thresholds no longer match your
// traffic" would instead mean "this table has been running a while". A burst
// test cannot catch that: inflation there only makes an already-large count
// larger.
//
// The two phases are what make it airtight. Ticks arriving at an empty batch
// must count nothing; the same tick stream, once one record is waiting behind a
// floor that has not elapsed, must count something. The second phase is the
// evidence that the first phase's zero was a decision rather than a table where
// nothing happened at all.
func TestFlushFloorEngagedCountsOnlyHeldBackTriggers(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	clock := newSteppedClock()
	cfg := testConfig(t, bucket)
	cfg.Clock = clock
	// A tick every few milliseconds against a floor that will never elapse,
	// because the clock does not move unless this test moves it.
	cfg.FlushInterval = 5 * time.Millisecond
	cfg.MinFlushInterval = time.Hour
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "idle"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer func() {
		if err := writer.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	// Phase one: nothing is inserted at all, and many ticks go by.
	time.Sleep(500 * time.Millisecond)

	if idle := writer.Status(); idle.FlushFloorEngaged != 0 {
		t.Errorf("Status.FlushFloorEngaged = %d on a table that has never been written to; the floor held nothing back, so it engaged %d times too many",
			idle.FlushFloorEngaged, idle.FlushFloorEngaged)
	}

	// Phase two: one record, still under the same unelapsed floor. Now every
	// tick has something to hold back, and the count must move — which is what
	// proves the ticks in phase one were arriving and being ignored on purpose.
	if err := writer.Insert(ctx, makeFill(0)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for writer.Status().FlushFloorEngaged < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("Status.FlushFloorEngaged reached %d with a record waiting behind an unelapsed floor; the interval trigger is not arriving at all, so phase one proved nothing",
				writer.Status().FlushFloorEngaged)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := len(dataFiles(t, bucket, cfg.WarehousePrefix, "market", "idle")); got != 0 {
		t.Errorf("%d data files exist while the floor is holding the only record back, want 0", got)
	}
}

// TestFlushFloorCoalescesABurst is scenario 9's fourth clause: the floor
// coalesces a burst instead of splitting it.
//
// The configuration is inverted from every other scenario's on purpose — a
// floor far above the flush interval, so the floor is the binding constraint
// rather than an irrelevance — and time is frozen through the injected clock,
// so "the floor has not elapsed yet" is exact rather than a bet on how long a
// hundred fsynced inserts take on the machine running the suite.
//
// Both halves of the floor's promise are proven here, because the second is
// what makes the first safe: a burst that would have sealed ten tiny batches
// seals none while the floor holds, and then, once the floor passes, the very
// next trigger seals one batch containing all of them. Nothing is dropped and
// nothing is delayed forever — the correction this library exists to apply,
// applied to itself.
func TestFlushFloorCoalescesABurst(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	clock := newSteppedClock()
	cfg := testConfig(t, bucket)
	cfg.Clock = clock
	cfg.FlushMaxRecords = 10
	cfg.FlushInterval = 100 * time.Millisecond
	cfg.MinFlushInterval = time.Hour
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// Ten times the record threshold: ten size triggers, plus whatever interval
	// triggers arrive while the burst is being written.
	const records = 100
	const perBatch = 10 // cfg.FlushMaxRecords
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	held := writer.Status()
	if held.FlushFloorEngaged < records/perBatch {
		t.Errorf("Status.FlushFloorEngaged = %d after %d size triggers under a floor no time has passed against, want at least %d",
			held.FlushFloorEngaged, records/perBatch, records/perBatch)
	}
	if held.FlushesCommitted != 0 || held.PendingRecords != records {
		t.Errorf("with the floor holding, Status reports %d commits and %d pending, want 0 and %d",
			held.FlushesCommitted, held.PendingRecords, records)
	}
	if got := len(dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")); got != 0 {
		t.Errorf("%d data files exist while the floor is holding every trigger back, want 0", got)
	}

	// Time passes. The next interval trigger finds a floor that has elapsed and
	// seals everything accumulated behind it — no Flush, no Close, nothing but
	// the writer's own timer.
	clock.Advance(2 * time.Hour)

	deadline := time.Now().Add(30 * time.Second)
	for len(dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the coalesced batch never committed once the floor had elapsed")
		}
		time.Sleep(25 * time.Millisecond)
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	final := writer.Status()

	// Fewer flushes than triggers, which is the whole claim: every trigger the
	// floor held back plus the one it did not is what was asked for, and one
	// commit is what happened.
	//
	// The sum is the trigger count here because of how this test is arranged
	// and not in general: nothing was replayed into this writer, and the Close
	// above sealed nothing because the timer had already emptied the batch, so
	// every commit this writer counted came from a threshold trigger.
	triggers := final.FlushFloorEngaged + final.FlushesCommitted
	if final.FlushesCommitted != 1 {
		t.Errorf("Status.FlushesCommitted = %d, want 1: the burst must coalesce into one batch, not several", final.FlushesCommitted)
	}
	if final.FlushesCommitted >= triggers {
		t.Errorf("%d flushes for %d trigger events; the floor coalesced nothing", final.FlushesCommitted, triggers)
	}
	if final.FlushFloorEngaged == 0 {
		t.Error("Status.FlushFloorEngaged = 0, want the count of triggers the floor held back")
	}

	files := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(files) != 1 {
		t.Errorf("the burst produced %d data files, want the 1 the floor coalesced it into: %s", len(files), describe(files))
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the coalesced batch committed, want 0", got)
	}

	// Nothing lost and nothing duplicated across the coalescing.
	duck := openDuckDB(t, bucket)
	var total, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total, &distinct)
	if total != records || distinct != records {
		t.Errorf("the coalesced batch reads back as %d rows (%d distinct), want %d of each", total, distinct, records)
	}
}
