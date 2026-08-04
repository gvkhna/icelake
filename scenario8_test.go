package icelake_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/flush"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// This file is TESTING.md scenario 8, "Flush failure, retry, and self-healing",
// plus the quarantine half of ARCHITECTURE.md's failure-handling section, which
// no numbered scenario covers.
//
// The substrate is real throughout: the same MinIO container, the same staging
// file, the same DuckDB read-back as every other scenario. What the tests below
// control is the network — through the proxy that already fronts MinIO for the
// other failure tests — and, where the claim is about the backoff schedule
// itself, the clock, which is the one seam TESTING.md's philosophy allows and
// the only way a delay sequence is observable at all.
//
// The scenario's five clauses map onto the tests here as:
//
//   - Insert keeps succeeding while the table cannot flush, until the ceiling
//     refuses — TestBoundedRetryReportsAndTheTableSelfHeals.
//   - Retries are bounded and OnFlushError fires once per cycle with the
//     table, key, record count and cause — the same test, plus
//     TestRetryBackoffFollowsTheConfiguredSchedule for the delays themselves.
//   - Nothing is lost: the sealed batch is still staged with its key — the
//     same test.
//   - The table self-heals once the substrate returns — the same test.
//   - Ordering holds through the failure — the same test, checked by reading
//     the table back at every metadata version it ever had.
//
// The exact delay sequence and its jitter bounds are the sanctioned unit test
// and live in internal/flush/retry_test.go, per TESTING.md's closed list of
// calculations whose output matters.

// TestBoundedRetryReportsAndTheTableSelfHeals drives one table through a whole
// outage: a batch that cannot commit, more records accepted behind it, the
// ceiling refusing at the end of that, and then the endpoint returning and the
// table healing by itself with nothing lost, nothing duplicated and nothing
// committed out of order.
func TestBoundedRetryReportsAndTheTableSelfHeals(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)

	clock := &recordingClock{}
	callback := &flushErrors{}

	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	cfg.Clock = clock
	cfg.OnFlushError = callback.record
	// Four records to a batch and twelve to the whole process's staging store,
	// so one outage produces several queued batches and then a refusal, which
	// is the arc this scenario describes.
	cfg.FlushMaxRecords = 4
	cfg.StagingMaxRecords = 12

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// A batch that commits normally first, so everything below happens against
	// a table that already has a snapshot rather than an empty one.
	for i := range 4 {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush of the first batch: %v", err)
	}
	if got := snapshotCount(t, bucket, cfg.WarehousePrefix, "market", "fills"); got != 1 {
		t.Fatalf("the table has %d snapshots before the outage, want 1", got)
	}

	// From here nothing reaches the object store.
	proxy.SetReject(true)

	// Records keep being accepted while the table cannot flush — they are
	// durable in staging, which is the entire point — up to the ceiling. Twelve
	// fit; the thirteenth is refused.
	const staged = 12
	for i := 4; i < 4+staged; i++ {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d while the table could not flush: %v", i, err)
		}
	}
	refused := makeFill(4 + staged)
	if err := writer.Insert(ctx, refused); !errors.Is(err, icelake.ErrStagingFull) {
		t.Fatalf("Insert at the ceiling returned %v, want ErrStagingFull", err)
	}

	// The retry is bounded, and what a caller gets back is the typed error with
	// the batch's own identity in it.
	err = writer.Flush(ctx)
	var flushErr icelake.FlushError
	if !errors.As(err, &flushErr) {
		t.Fatalf("Flush against an unreachable store returned %v (%T), want a FlushError", err, err)
	}
	if flushErr.Namespace != "market" || flushErr.Table != "fills" {
		t.Errorf("FlushError names table %s.%s, want market.fills", flushErr.Namespace, flushErr.Table)
	}
	if flushErr.BatchKey == "" || flushErr.Records != 4 {
		t.Errorf("FlushError describes %d records under key %q, want the stuck batch's 4 records under its own key",
			flushErr.Records, flushErr.BatchKey)
	}
	if flushErr.Attempts != cfg.FlushMaxAttempts {
		t.Errorf("FlushError reports %d attempts, want the whole configured budget of %d",
			flushErr.Attempts, cfg.FlushMaxAttempts)
	}
	if flushErr.Err == nil {
		t.Error("FlushError carries no underlying cause")
	}

	// Nothing was lost and nothing committed ahead: the stuck batch is still in
	// staging under its key, every record accepted during the outage is still
	// there too, and the bucket has not moved since before the outage.
	if got := stagedRows(t, cfg.StagingPath); got != staged {
		t.Errorf("staging holds %d rows during the outage, want all %d accepted", got, staged)
	}
	if got := sealedRows(t, cfg.StagingPath); got != 4 {
		t.Errorf("%d staged rows carry a batch key, want the stuck batch's 4", got)
	}
	// Exactly one key, and it is the one the caller was told about. The batches
	// queued behind the stuck one are not even durably sealed yet: the trigger
	// decides a batch's boundary and the worker records it, and the worker has
	// not reached them — which is the same one-at-a-time property that keeps
	// them from committing early, seen from the staging side.
	keys := stagedBatchKeys(t, cfg.StagingPath)
	if len(keys) != 1 || keys[0] != flushErr.BatchKey {
		t.Errorf("staging holds batch keys %v, want only the stuck batch's %q", keys, flushErr.BatchKey)
	}
	if files := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(files) != 1 {
		t.Errorf("the bucket holds %d data files during the outage, want only the one committed before it: %s",
			len(files), describe(files))
	}
	if got := snapshotCount(t, bucket, cfg.WarehousePrefix, "market", "fills"); got != 1 {
		t.Errorf("the table has %d snapshots during the outage, want the 1 from before it — nothing may commit ahead of a stuck batch", got)
	}

	// The store answers again, with no restart and no reconfiguration: the next
	// trigger retries the same batch with a fresh budget and commits it, and
	// everything queued behind it follows in order.
	proxy.SetReject(false)
	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := writer.Flush(drain); err != nil {
		t.Fatalf("Flush once the store answered again: %v", err)
	}
	if err := writer.Close(drain); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the table healed, want 0", got)
	}

	const total = 4 + staged
	duck := openDuckDB(t, bucket)
	var rows, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &rows, &distinct)
	if rows != total || distinct != total {
		t.Errorf("the table holds %d rows with %d distinct ids, want %d of each — every record across the episode, exactly once",
			rows, distinct, total)
	}

	// Ordering held all the way through. Every record's sequence id is its
	// position in the order they were written, and batches are contiguous runs
	// of them, so a table whose commits landed in seal order shows a *prefix*
	// of that order at every snapshot it ever had. A batch that had committed
	// ahead of the stuck one would leave a gap at some version, which is
	// exactly what this cannot pass with.
	assertEverySnapshotIsAPrefix(t, duck, bucket, cfg.WarehousePrefix, "market", "fills")

	// The callback fired once per failed cycle rather than once per attempt.
	// Each cycle of the configured budget sleeps exactly once — two attempts,
	// one delay between them — so the number of delays the worker asked the
	// clock for is the number of cycles it ran, and that is what the number of
	// notifications has to equal.
	sleeps, events := clock.Sleeps(), callback.Events()
	if len(events) == 0 {
		t.Fatal("the flush-error callback never fired, though a whole attempt budget was spent")
	}
	if len(events) != len(sleeps) {
		t.Errorf("the callback fired %d times across %d retry cycles, want exactly once per cycle", len(events), len(sleeps))
	}
	ceiling := cfg.RetryBaseDelay
	for i, d := range sleeps {
		if d < 0 || d > ceiling {
			t.Errorf("retry sleep %d was %s, want a full-jitter delay between 0 and the %s first-retry ceiling", i, d, ceiling)
		}
	}
	for i, e := range events {
		if e.Namespace != "market" || e.Table != "fills" || e.BatchKey != flushErr.BatchKey || e.Records != 4 {
			t.Errorf("callback event %d = {%s.%s %s %d records}, want the stuck batch's own identity",
				i, e.Namespace, e.Table, e.BatchKey, e.Records)
		}
		if e.Attempts != cfg.FlushMaxAttempts || e.Err == nil {
			t.Errorf("callback event %d reports %d attempts and cause %v, want the whole budget of %d and a cause",
				i, e.Attempts, e.Err, cfg.FlushMaxAttempts)
		}
	}
}

// TestRetryBackoffFollowsTheConfiguredSchedule proves the schedule the unit
// test pins is the schedule the worker actually sleeps.
//
// The unit test in internal/flush asserts what the calculation returns; this
// one asserts that a real flush failure, against the real substrate, asks the
// injected clock for exactly that sequence — one delay fewer than the attempt
// budget, each one the doubling, capped ceiling the configuration describes.
//
// The draw is pinned at its maximum through the same kind of test-only hook the
// crash points use, and that is what makes this a test of the schedule rather
// than of its cap. Left random, every delay could only be checked against the
// window it came from, and a schedule that had lost its doubling entirely would
// pass every such check.
func TestRetryBackoffFollowsTheConfiguredSchedule(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)

	flush.Jitter = func() float64 { return 1 }
	t.Cleanup(func() { flush.Jitter = nil })

	clock := &recordingClock{}
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	cfg.Clock = clock
	cfg.FlushMaxAttempts = 4
	cfg.RetryBaseDelay = 50 * time.Millisecond
	cfg.RetryMaxDelay = 150 * time.Millisecond

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	for i := range 3 {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	proxy.SetReject(true)
	var flushErr icelake.FlushError
	if err := writer.Flush(ctx); !errors.As(err, &flushErr) {
		t.Fatalf("Flush against an unreachable store returned %v (%T), want a FlushError", err, err)
	}
	if flushErr.Attempts != cfg.FlushMaxAttempts {
		t.Fatalf("the cycle made %d attempts, want the configured %d", flushErr.Attempts, cfg.FlushMaxAttempts)
	}

	// One delay between each pair of attempts and none after the last: sleeping
	// once more before reporting would delay the caller's error for nothing.
	// The ceilings are 50ms, then 100ms, then the 150ms cap — doubling from the
	// base and capped, exactly as configured — and each delay is drawn
	// uniformly from zero up to its own ceiling.
	want := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 150 * time.Millisecond}
	sleeps := clock.Sleeps()
	if len(sleeps) != len(want) {
		t.Fatalf("the cycle slept %d times (%v), want %d — one delay fewer than its %d attempts",
			len(sleeps), sleeps, len(want), cfg.FlushMaxAttempts)
	}
	if !slices.Equal(sleeps, want) {
		t.Errorf("the cycle slept %v, want exactly %v — the base doubled and then capped", sleeps, want)
	}

	// The records are untouched by any of it, and the table heals.
	proxy.SetReject(false)
	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := writer.Close(drain); err != nil {
		t.Fatalf("Close once the store answered again: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the drain, want 0", got)
	}
}

// TestShutdownInterruptsASleepingRetry proves the sleep is interruptible rather
// than merely bounded: a shutdown does not have to wait out a backoff.
//
// The delay is made unmissable — the clock's sleep returns only when its
// context is done — so this cannot pass by the sleep happening to be short. If
// the worker slept on anything other than the store's own context, or ignored
// what the sleep returned, Close would never come back and this test would hang
// rather than fail, which is the honest failure mode for this claim.
func TestShutdownInterruptsASleepingRetry(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)

	clock := &blockingClock{entered: make(chan struct{})}
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	cfg.Clock = clock
	cfg.FlushMaxRecords = 4
	// A production-shaped policy: a minute of backoff is exactly what a
	// shutdown must not have to sit through.
	cfg.FlushMaxAttempts = 5
	cfg.RetryBaseDelay = time.Second
	cfg.RetryMaxDelay = time.Minute

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	proxy.SetReject(true)
	const records = 4
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	// The fourth insert sealed a batch, whose first attempt fails and whose
	// retry is now waiting on the clock.
	select {
	case <-clock.entered:
	case <-time.After(time.Minute):
		t.Fatal("the flush worker never reached a retry sleep")
	}

	budget, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := writer.Close(budget); err == nil {
		t.Error("Close during an outage returned no error, want the flush failure")
	}
	// Returning at all is the whole assertion: the sleep under it ends only
	// when its context does, so a worker that ignored the shutdown would hang
	// here rather than fail slowly.
	t.Logf("Close returned %s after being called while a retry was sleeping", time.Since(start))

	if !clock.Interrupted() {
		t.Error("the retry sleep returned on its own rather than being interrupted by the shutdown")
	}

	// Bounded, but never at the cost of the durability guarantee: the records
	// are still staged, still sealed, ready for the next run.
	if got := stagedRows(t, cfg.StagingPath); got != records {
		t.Errorf("staging holds %d rows after the interrupted shutdown, want all %d", got, records)
	}
	if got := sealedRows(t, cfg.StagingPath); got != records {
		t.Errorf("%d staged rows carry their batch key, want all %d", got, records)
	}
}

// TestAStuckTableDoesNotStarveAnotherTable is the other half of the ordering
// claim: a batch stuck at the head of one table's queue holds up that table and
// nothing else.
//
// It is a structural property — one worker goroutine per table, and the queue
// each drains is its own — but "structural" is what an unenforced claim always
// sounds like, and two tables sharing one store, one staging file and one
// object-storage client is exactly the arrangement in which it could quietly
// stop being true. The stuck table is left parked inside a retry sleep while
// the second table writes and commits through the same store.
func TestAStuckTableDoesNotStarveAnotherTable(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)

	clock := &blockingClock{entered: make(chan struct{})}
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	cfg.Clock = clock
	cfg.FlushMaxRecords = 4
	cfg.FlushMaxAttempts = 5
	cfg.RetryBaseDelay = time.Minute
	cfg.RetryMaxDelay = time.Minute

	store := openStore(t, cfg)
	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	events, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		t.Fatalf("OpenWriter for the second table: %v", err)
	}

	// One table is taken down mid-flush and left there: its batch's first
	// attempt failed and its retry is now waiting on the clock.
	proxy.SetReject(true)
	const stuck = 4
	for i := range stuck {
		if err := fills.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	select {
	case <-clock.entered:
	case <-time.After(time.Minute):
		t.Fatal("the first table's worker never reached a retry sleep")
	}
	proxy.SetReject(false)

	// The second table writes and commits normally, with the first still stuck.
	const healthy = 3
	for i := range healthy {
		if err := events.Insert(ctx, makeEvent(i)); err != nil {
			t.Fatalf("Insert into the second table %d: %v", i, err)
		}
	}
	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := events.Flush(drain); err != nil {
		t.Fatalf("Flush of the second table while the first is stuck: %v", err)
	}

	duck := openDuckDB(t, bucket)
	var rows int
	duck.queryRow(t, "SELECT count(*) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "archive", "events")), &rows)
	if rows != healthy {
		t.Errorf("the second table holds %d rows, want %d — a stuck table must not starve another", rows, healthy)
	}
	if err := events.Close(drain); err != nil {
		t.Fatalf("Close of the second table: %v", err)
	}

	// And the stuck table is exactly as stuck as it was: nothing committed, and
	// every record still safe in staging under its batch key.
	if files := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(files) != 0 {
		t.Errorf("the stuck table has %d data files, want none: %s", len(files), describe(files))
	}
	budget, cancelBudget := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelBudget()
	if err := fills.Close(budget); err == nil {
		t.Error("closing the stuck table returned no error, want the flush failure")
	}
	if got := sealedRows(t, cfg.StagingPath); got != stuck {
		t.Errorf("%d staged rows carry a batch key, want the stuck table's %d", got, stuck)
	}
}

// TestFlushErrorCallbackCannotBreakTheFlushPath covers the two ways a caller's
// callback could otherwise take a writer down with it.
//
// The callback is notification and nothing else. It runs on a background
// goroutine the caller cannot see, so a panic in it must not become the
// process's last act, and a callback that never returns must not be able to
// hold a shutdown open. Neither costs the batch anything either way, because a
// flush error never means a record was lost.
func TestFlushErrorCallbackCannotBreakTheFlushPath(t *testing.T) {
	t.Run("a callback that panics", func(t *testing.T) {
		testsubstrate.RequireDocker(t)
		bucket := testsubstrate.StartMinIO(t)

		ctx := context.Background()
		proxy := startSlowProxy(t, bucket.Endpoint, 0)

		callback := &flushErrors{panics: true}
		cfg := testConfig(t, bucket)
		cfg.Endpoint = proxy.Endpoint
		cfg.OnFlushError = callback.record

		store := openStore(t, cfg)
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}

		const records = 5
		for i := range records {
			if err := writer.Insert(ctx, makeFill(i)); err != nil {
				t.Fatalf("Insert %d: %v", i, err)
			}
		}

		proxy.SetReject(true)
		var flushErr icelake.FlushError
		if err := writer.Flush(ctx); !errors.As(err, &flushErr) {
			t.Fatalf("Flush against an unreachable store returned %v (%T), want a FlushError", err, err)
		}
		waitFor(t, "the flush-error notification", func() bool { return callback.Len() > 0 })

		// The writer is unharmed: the same batch commits on the next trigger,
		// exactly as it would have with no callback at all.
		proxy.SetReject(false)
		drain, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		if err := writer.Close(drain); err != nil {
			t.Fatalf("Close after the callback panicked: %v", err)
		}

		duck := openDuckDB(t, bucket)
		var rows int
		duck.queryRow(t, "SELECT count(*) FROM "+
			duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &rows)
		if rows != records {
			t.Errorf("the table holds %d rows, want %d — a broken callback must cost nothing", rows, records)
		}
	})

	t.Run("a callback that never returns", func(t *testing.T) {
		testsubstrate.RequireDocker(t)
		bucket := testsubstrate.StartMinIO(t)

		ctx := context.Background()
		proxy := startSlowProxy(t, bucket.Endpoint, 0)

		// Released at the end of the test, so the goroutine it holds is not
		// left blocked for the rest of the run. Nothing icelake does waits for
		// it: that is the claim.
		release := make(chan struct{})
		defer close(release)
		callback := &flushErrors{block: release}

		cfg := testConfig(t, bucket)
		cfg.Endpoint = proxy.Endpoint
		cfg.OnFlushError = callback.record

		store := openStore(t, cfg)
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}

		const records = 5
		for i := range records {
			if err := writer.Insert(ctx, makeFill(i)); err != nil {
				t.Fatalf("Insert %d: %v", i, err)
			}
		}

		proxy.SetReject(true)
		var flushErr icelake.FlushError
		if err := writer.Flush(ctx); !errors.As(err, &flushErr) {
			t.Fatalf("Flush against an unreachable store returned %v (%T), want a FlushError", err, err)
		}
		// The callback is entered and never leaves, which is the state the rest
		// of this subtest is about.
		waitFor(t, "the flush-error notification", func() bool { return callback.Len() > 0 })

		budget, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		start := time.Now()
		if err := writer.Close(budget); err == nil {
			t.Error("Close during an outage returned no error, want the flush failure")
		}
		// As above, returning at all is the assertion: the callback is still
		// inside itself and will be until this test ends.
		t.Logf("Close returned %s after a callback that never returns", time.Since(start))

		if got := stagedRows(t, cfg.StagingPath); got != records {
			t.Errorf("staging holds %d rows, want all %d — the records are safe whatever the callback does", got, records)
		}
	})
}

// TestUnencodableBatchIsQuarantinedAndFreesTheQueue is the quarantine half of
// ARCHITECTURE.md's failure handling, which no numbered scenario covers.
//
// Quarantine is deliberately narrow: it catches a batch that cannot be
// *encoded*, which after M7's correction is a strict subset of what the poison
// check at Insert catches — an invalid-UTF-8 string and an out-of-precision
// decimal both encode perfectly well and are refused at the boundary instead.
// What is left for quarantine is a batch whose stored bytes the encoder itself
// refuses, which is what this test produces: a staged payload corrupted
// underneath the writer, the way a damaged file or an older, buggier binary
// could leave one.
//
// Every claim in that decision is asserted here: the batch is marked whole
// rather than retried forever, the queue behind it moves again, the marked rows
// never replay, and they keep counting against the staging ceiling so a growing
// quarantine stops the writer loudly rather than silently eating headroom.
func TestUnencodableBatchIsQuarantinedAndFreesTheQueue(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)

	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	cfg.FlushMaxRecords = 4

	// A first run that leaves a sealed, uncommitted batch behind — the state a
	// shutdown during an outage leaves, and the one whose rows a later run
	// picks back up. This store is opened and closed by hand rather than
	// through the usual helper, because closing it is expected to fail: its
	// drain cannot reach the store, which is the point.
	const quarantined = 4
	first, err := icelake.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writer, err := icelake.OpenWriter(ctx, first, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	proxy.SetReject(true)
	for i := range quarantined {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err == nil {
		t.Fatal("Flush against an unreachable store returned no error, want one")
	}
	shutdown, cancelShutdown := context.WithTimeout(ctx, 5*time.Second)
	defer cancelShutdown()
	_ = first.Close(shutdown)
	proxy.SetReject(false)

	if got := sealedRows(t, cfg.StagingPath); got != quarantined {
		t.Fatalf("%d staged rows carry a batch key, want all %d", got, quarantined)
	}
	ids := stagedIDs(t, cfg.StagingPath, "market", "fills")

	// One payload is replaced with bytes no decoder can read, underneath a
	// writer that is not running. The batch is now unflushable forever: the
	// same rows, decoded by the same recorded shape, fail identically on every
	// attempt.
	corruptStagedPayload(t, cfg.StagingPath, ids[0])

	callback := &flushErrors{}
	cfg.OnFlushError = callback.record
	second := openStore(t, cfg)
	recovered, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter on the next run: %v", err)
	}
	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Replay's own trigger is what meets the bad batch, with nobody waiting on
	// it — the ordinary case, since the trigger that finds one is usually the
	// timer's rather than a caller's. Waiting for the notification first is
	// what makes the Flush below a call that arrives *after* the batch was
	// already given up on, which is exactly the case that must still be told:
	// those records will never commit, and answering nil would be a writer
	// reporting success for data it has abandoned.
	//
	// (That the recovered writer claimed the staged rows at all is not asserted
	// through Pending here, deliberately: the worker may have quarantined them
	// before the assertion could read it, and a test that raced the writer to a
	// number would be a flake wearing a claim's clothes. What proves the claim
	// is that the batch quarantined below is those four rows.)
	waitFor(t, "the quarantine notification", func() bool { return callback.Len() > 0 })

	err = recovered.Flush(drain)

	var flushErr icelake.FlushError
	if !errors.As(err, &flushErr) {
		t.Fatalf("Flush after a batch was quarantined returned %v (%T), want a FlushError", err, err)
	}
	var encodingErr icelake.EncodingError
	if !errors.As(err, &encodingErr) {
		t.Fatalf("the flush failure was %v, want an EncodingError underneath it", err)
	}
	// Reported exactly once, and then the writer moves on: the queue is healthy
	// again, and failing every later call forever over records no retry can
	// save would turn a bounded loss into a permanently broken writer.
	if err := recovered.Flush(drain); err != nil {
		t.Errorf("a second Flush after the quarantine returned %v, want nil", err)
	}
	if events := callback.Events(); len(events) != 1 {
		t.Errorf("the callback fired %d times for one quarantined batch, want exactly once", len(events))
	} else if events[0].Records != quarantined || events[0].BatchKey == "" {
		t.Errorf("the callback reported %d records under key %q, want the whole batch under its own key",
			events[0].Records, events[0].BatchKey)
	}

	// The whole batch is marked, never part of it: a partly-marked batch would
	// leave a stored key that no longer names its own bytes.
	if got := quarantinedRows(t, cfg.StagingPath); got != quarantined {
		t.Errorf("%d staged rows are quarantined, want the whole batch of %d", got, quarantined)
	}
	if !quarantineCauseRecorded(t, cfg.StagingPath) {
		t.Error("a quarantined row records no cause; an operator has nothing to read")
	}
	// It is not retried forever, and it is no longer waiting for anything: the
	// writer's pending count drops even though the rows stay on disk.
	if pending, _ := recovered.Pending(); pending != 0 {
		t.Errorf("Pending() = %d after the batch was quarantined, want 0 — it will never be flushed", pending)
	}

	// The queue is free again: fresh records go straight through.
	const healthy = 3
	for i := 100; i < 100+healthy; i++ {
		if err := recovered.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d after the quarantine: %v", i, err)
		}
	}
	// Close waits the same way Flush does, so it reports a quarantine nobody
	// has heard about yet for the same reason. Here there is nothing left to
	// report, which is the other half of "exactly once".
	if err := recovered.Close(drain); err != nil {
		t.Fatalf("Close after the quarantine: %v", err)
	}

	duck := openDuckDB(t, bucket)
	var rows int
	duck.queryRow(t, "SELECT count(*) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &rows)
	if rows != healthy {
		t.Errorf("the table holds %d rows, want the %d written after the quarantine and none of the quarantined batch",
			rows, healthy)
	}

	// The marked rows never replay, in this run or any later one.
	reopened, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after the quarantine: %v", err)
	}
	if pending, _ := reopened.Pending(); pending != 0 {
		t.Errorf("Pending() = %d on a writer opened after the quarantine, want 0 — marked rows are excluded from every replay", pending)
	}
	if err := reopened.Close(drain); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != quarantined {
		t.Errorf("staging holds %d rows, want the %d quarantined ones left for an operator", got, quarantined)
	}

	// And they still occupy the staging ceiling, which is what makes a growing
	// quarantine stop the writer loudly instead of silently freeing headroom: a
	// process whose whole ceiling is exactly the quarantined rows accepts
	// nothing at all.
	if err := second.Close(drain); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}
	full := cfg
	full.FlushMaxRecords = quarantined
	full.StagingMaxRecords = quarantined
	third := openStore(t, full)
	tight, err := icelake.OpenWriter(ctx, third, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter against a full ceiling: %v", err)
	}
	if err := tight.Insert(ctx, makeFill(999)); !errors.Is(err, icelake.ErrStagingFull) {
		t.Errorf("Insert with the ceiling filled by quarantined rows returned %v, want ErrStagingFull", err)
	}
	if err := tight.Close(drain); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// assertEverySnapshotIsAPrefix reads the table back at every metadata version
// it ever had and requires the records visible at each to be exactly a prefix
// of the order they were written in.
//
// This is scenario 8's ordering clause, checked against the table's own commit
// history rather than against icelake's account of it. Records carry their
// write position as sequence_id and batches are contiguous runs of them, so a
// snapshot whose rows run 0..n-1 with no gap is a snapshot that committed in
// seal order. One batch overtaking another would leave a hole at whichever
// version it landed in.
func assertEverySnapshotIsAPrefix(tb testing.TB, duck *duck, bucket testsubstrate.Bucket, prefix, namespace, table string) {
	tb.Helper()

	checked, previous := 0, -1
	for _, location := range metadataVersions(tb, bucket, prefix, namespace, table) {
		if !readMetadata(tb, bucket, location).hasSnapshot() {
			continue
		}

		var rows, lowest, highest int
		duck.queryRow(tb, fmt.Sprintf(
			"SELECT count(*), coalesce(min(sequence_id), -1), coalesce(max(sequence_id), -1) FROM %s", duck.scan(location)),
			&rows, &lowest, &highest)

		if lowest != 0 || rows != highest+1 {
			tb.Errorf("at %s the table holds %d rows running %d..%d, want an unbroken prefix 0..%d of the order records were written in",
				location, rows, lowest, highest, rows-1)
		}
		if rows <= previous {
			tb.Errorf("at %s the table holds %d rows, no more than the %d visible at the previous commit", location, rows, previous)
		}
		previous = rows
		checked++
	}

	if checked < 2 {
		tb.Errorf("only %d committed metadata versions were checked; the ordering claim needs several commits to say anything", checked)
	}
}

// waitFor polls until cond holds, failing the test if it never does.
//
// It exists for exactly one thing: the flush-error callback is deliberately not
// waited on by the code that records a failure, so a Flush can return before
// the notification has been delivered. That is the design — a caller's callback
// must never delay an error reaching the caller — and it means a test that
// asserts on a notification has to wait for it rather than assume it. The
// budget is generous because the wait is for a goroutine that has already been
// started, never for a timer.
func waitFor(tb testing.TB, what string, cond func() bool) {
	tb.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stagedIDs returns one table's staged row ids in id order, which is insert
// order.
func stagedIDs(tb testing.TB, stagingPath, namespace, table string) []int64 {
	tb.Helper()

	rows := stagedRowsFor(tb, stagingPath, namespace, table)
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.id)
	}

	return out
}

// corruptStagedPayload replaces one staged row's payload with a byte no version
// of the canonical encoding claims, which is the smallest change that makes a
// row undecodable by any binary — including the one that wrote it.
//
// It writes to the real staging file with plain SQL, with no writer running, so
// what the next run meets is a genuinely damaged database rather than an error
// injected into the code path under test.
func corruptStagedPayload(tb testing.TB, stagingPath string, id int64) {
	tb.Helper()

	db := openStagingFile(tb, stagingPath)
	if _, err := db.Exec("UPDATE staging SET payload = ? WHERE id = ?", []byte{0x00}, id); err != nil {
		tb.Fatalf("corrupting staged row %d: %v", id, err)
	}
}

// quarantinedRows counts the staged rows marked as belonging to a batch that
// could not be encoded.
func quarantinedRows(tb testing.TB, stagingPath string) int {
	tb.Helper()

	db := openStagingFile(tb, stagingPath)

	var n int
	if err := db.QueryRow("SELECT count(*) FROM staging WHERE quarantined_at IS NOT NULL").Scan(&n); err != nil {
		tb.Fatalf("counting quarantined rows: %v", err)
	}

	return n
}

// quarantineCauseRecorded reports whether every quarantined row carries a
// recorded cause.
//
// It asks whether something was written for an operator to read, never what it
// says: asserting on the text would be exactly the message-matching this
// project's testing policy bans.
func quarantineCauseRecorded(tb testing.TB, stagingPath string) bool {
	tb.Helper()

	db := openStagingFile(tb, stagingPath)

	var missing int
	err := db.QueryRow(
		`SELECT count(*) FROM staging
		 WHERE quarantined_at IS NOT NULL AND coalesce(length(quarantine_error), 0) = 0`).Scan(&missing)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		tb.Fatalf("reading quarantine causes: %v", err)
	}

	return missing == 0
}
