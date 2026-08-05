package e2e

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TestConcurrentInsert is TESTING.md's scenario 6 concurrency claim: one Writer
// is safe to use from many goroutines at once.
//
// The suite runs under the race detector, so the interesting failure is caught
// by the detector rather than by an assertion; the assertion here is the other
// half — that every record survives the contention exactly once, across the
// many flushes that trip while the goroutines are still inserting.
//
// This is the "one writer object, many goroutines" promise. It is deliberately
// not a multi-writer-per-table test, which stays out of scope.
func TestConcurrentInsert(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	cfg.FlushMaxRecords = 25
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	const goroutines = 8
	const perGoroutine = 50
	const records = goroutines * perGoroutine

	var wg sync.WaitGroup
	errs := make(chan error, records)
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perGoroutine {
				// Sequence ids are unique across goroutines, so the read-back
				// count of distinct ids is a real statement about every record
				// arriving rather than about a total that could hide a swap.
				if err := writer.Insert(ctx, makeFill(g*perGoroutine+i)); err != nil {
					errs <- err

					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Insert: %v", err)
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))

	var total, distinct int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*), count(DISTINCT sequence_id) FROM %s", table), &total, &distinct)
	if total != records || distinct != records {
		t.Errorf("read back %d rows with %d distinct ids, want %d of each", total, distinct, records)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after a clean close, want 0", got)
	}
}

// TestFlushDoesNotBlockInsert proves the non-blocking-flush promise: when a
// threshold trips, the batch is sealed, a fresh one is swapped in immediately,
// and the upload happens on a background goroutine — so a caller never stalls
// on network I/O.
//
// The upload is made slow by putting a real, deliberately delayed network path
// in front of the real object store. The assertion is a loose latency bound
// rather than a benchmark: the question is only "did these inserts wait on the
// network", and an order-of-magnitude gap answers it without being flaky.
func TestFlushDoesNotBlockInsert(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	const uploadDelay = 2 * time.Second

	ctx := context.Background()
	// The proxy starts out fast so that creating the table is not itself a
	// two-second-per-request affair; the delay goes on once the writer is open
	// and the only thing left to reach the store is the flush.
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	cfg.FlushMaxRecords = 10
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	proxy.SetDelay(uploadDelay)

	// The tenth insert trips the threshold and seals a batch, whose upload now
	// takes at least the proxy's delay.
	for i := range 10 {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	start := time.Now()
	var slowest time.Duration
	for i := 10; i < 19; i++ {
		at := time.Now()
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
		if d := time.Since(at); d > slowest {
			slowest = d
		}
	}
	elapsed := time.Since(start)
	t.Logf("nine inserts issued during an in-flight flush took %s in total, slowest %s (the flush itself cannot finish in under %s)",
		elapsed, slowest, uploadDelay)

	if elapsed >= uploadDelay/2 {
		t.Errorf("inserts issued during an in-flight flush took %s, which is not meaningfully faster than the %s upload they must not have waited on",
			elapsed, uploadDelay)
	}

	// And the flush really was in flight while those inserts ran: its data file
	// had not landed yet, so the inserts genuinely overlapped an upload rather
	// than following a finished one.
	if files := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(files) != 0 {
		t.Errorf("the flush had already finished (%d data files), so the inserts did not overlap it", len(files))
	}

	// Let the network answer again and drain normally.
	proxy.SetDelay(0)
	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := writer.Close(drain); err != nil {
		t.Fatalf("Close: %v", err)
	}

	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, "SELECT count(*) FROM "+duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total)
	if total != 19 {
		t.Errorf("row count = %d, want 19", total)
	}
}

// TestFlushAndCloseDrain covers the rest of scenario 6's API contract: an
// explicit Flush commits below the threshold and is visible the moment it
// returns, a Close drains what is left, and a closed writer refuses everything
// afterwards.
func TestFlushAndCloseDrain(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// Five records: far below FlushMaxRecords, and well inside FlushInterval.
	for i := range 5 {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if records, bytes := writer.Pending(); records != 5 || bytes <= 0 {
		t.Errorf("Pending() = (%d, %d) before a flush, want 5 records and a positive size", records, bytes)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Immediately, with no polling: Flush returns only once everything sealed
	// at the moment of the call has been committed, which is what makes it
	// usable as a checkpoint.
	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, "SELECT count(*) FROM "+duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total)
	if total != 5 {
		t.Errorf("row count right after Flush returned = %d, want 5", total)
	}
	if records, bytes := writer.Pending(); records != 0 || bytes != 0 {
		t.Errorf("Pending() = (%d, %d) after a flush, want (0, 0)", records, bytes)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after a flush, want 0", got)
	}

	// The writer keeps working afterwards: Flush drains, it does not close.
	for i := 5; i < 8; i++ {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d after Flush: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	duck.queryRow(t, "SELECT count(*) FROM "+duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total)
	if total != 8 {
		t.Errorf("row count after Close = %d, want 8; Close must force-flush below the threshold", total)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after Close, want 0", got)
	}

	// Closed means closed: inserts are refused, and the drain happens once.
	if err := writer.Insert(ctx, makeFill(99)); !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("Insert after Close returned %v, want ErrClosed", err)
	}
	if err := writer.Flush(ctx); !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("Flush after Close returned %v, want ErrClosed", err)
	}
	if err := writer.Close(ctx); !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("second Close returned %v, want ErrClosed", err)
	}
	duck.queryRow(t, "SELECT count(*) FROM "+duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total)
	if total != 8 {
		t.Errorf("row count after a second Close = %d, want 8; the drain must happen exactly once", total)
	}
}

// TestShutdownTimeoutKeepsRecordsAndTheNextRunFlushesThem proves the last of
// scenario 6's claims, and with it the replay path.
//
// A Close whose context has already expired returns an error rather than
// waiting, and leaves every record intact in staging — the shutdown is bounded,
// but never at the cost of the durability guarantee. The next run then picks
// those records up from the staging file alone and commits them exactly once,
// which is the whole reason that file exists.
func TestShutdownTimeoutKeepsRecordsAndTheNextRunFlushesThem(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	// Fast while the table is created and the records are accepted; stalled
	// only for the drain, which is the thing under test.
	proxy := startSlowProxy(t, bucket.Endpoint, 0)

	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	first := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, first, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	const records = 12
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	// The upload cannot possibly finish inside the shutdown's budget, which is
	// what makes "the drain ran out of time" a fact rather than a race.
	proxy.SetDelay(time.Minute)

	// A shutdown budget the drain cannot possibly meet: sealing is local and
	// takes milliseconds, but the upload behind it is stalled for a minute.
	// (An already-expired context takes the same path with even less of it
	// completed, and leaves the same records behind.)
	shutdownBudget, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	closeErr := writer.Close(shutdownBudget)
	if closeErr == nil {
		t.Fatal("Close whose context expired mid-drain returned no error, want one")
	}
	// Which error matters as much as that there was one. Close's documentation
	// distinguishes a drain that ran out of time from a flush that failed, and
	// a caller acts on them differently: the first says start again with a
	// longer budget, the second says something about the endpoint is wrong. The
	// two are indistinguishable unless the deadline is passed back as itself,
	// so it is matched as itself rather than merely counted as non-nil.
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Errorf("Close returned %v (%T), want the caller's own expired deadline", closeErr, closeErr)
	}
	var flushErr icelake.FlushError
	if errors.As(closeErr, &flushErr) {
		t.Errorf("Close reported a shutdown that ran out of time as a flush failure: %v", closeErr)
	}
	// The store closes cleanly: its one writer has already been closed, so
	// there is nothing left to drain and nothing left to fail.
	shutdown, cancelShutdown := context.WithTimeout(ctx, time.Minute)
	defer cancelShutdown()
	if err := first.Close(shutdown); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	if got := stagedRows(t, cfg.StagingPath); got != records {
		t.Fatalf("staging holds %d rows after a shutdown that ran out of time, want all %d", got, records)
	}
	// The batch was sealed durably before anything was encoded, which is what
	// makes the next run rebuild exactly this batch rather than a
	// differently-bounded one — and therefore what makes its object key
	// idempotent.
	if got := sealedRows(t, cfg.StagingPath); got != records {
		t.Errorf("%d of the staged rows carry a batch key, want all %d", got, records)
	}
	if files := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(files) != 0 {
		t.Errorf("data files = %d, want 0; nothing should have been uploaded through a stalled connection", len(files))
	}

	// The key the batch was sealed under. The next run must upload to exactly
	// this object: a sealed batch's key is read back, never recomputed, because
	// recomputing it after any schema change would name a different object and
	// strand whatever had already been written under the old name.
	keys := stagedBatchKeys(t, cfg.StagingPath)
	if len(keys) != 1 {
		t.Fatalf("staging holds %d batch keys, want exactly 1", len(keys))
	}

	// The next run: same staging file, same bucket, a network that answers.
	proxy.SetDelay(0)
	second := openStore(t, cfg)
	recovered, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter on the next run: %v", err)
	}
	drain, cancelDrain := context.WithTimeout(ctx, time.Minute)
	defer cancelDrain()
	if err := recovered.Flush(drain); err != nil {
		t.Fatalf("Flush on the next run: %v", err)
	}
	if err := recovered.Close(drain); err != nil {
		t.Fatalf("Close on the next run: %v", err)
	}

	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the next run drained it, want 0", got)
	}

	files := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(files) != 1 {
		t.Fatalf("the next run wrote %d data files, want 1: %s", len(files), describe(files))
	}
	if want := keys[0] + ".parquet"; path.Base(files[0].key) != want {
		t.Errorf("the replayed batch was uploaded as %s, want %s — a sealed batch's stored key is authoritative",
			path.Base(files[0].key), want)
	}

	duck := openDuckDB(t, bucket)
	var total, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total, &distinct)
	if total != records || distinct != records {
		t.Errorf("the next run committed %d rows with %d distinct ids, want %d of each exactly once", total, distinct, records)
	}
}

// TestFailedOpenLeavesPendingRowsClaimable is the regression test for a real
// bug: a writer that failed to open had already consumed its table's replay
// claim, so the records staged by a previous run were held by nobody.
//
// Nothing surfaced. The successful open that followed found an empty claim,
// reported no pending records, and the rows sat in staging, counted against the
// ceiling and never flushed, for the life of the process. The fix is that
// claiming is the last thing an open does, after everything that can refuse it
// already has.
func TestFailedOpenLeavesPendingRowsClaimable(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint

	// A previous run that left records staged.
	first := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, first, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	const records = 25
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	proxy.SetDelay(time.Minute)
	budget, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := writer.Close(budget); err == nil {
		t.Fatal("Close whose context expired mid-drain returned no error, want one")
	}
	shutdown, cancelShutdown := context.WithTimeout(ctx, time.Minute)
	defer cancelShutdown()
	if err := first.Close(shutdown); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != records {
		t.Fatalf("staging holds %d rows, want %d", got, records)
	}
	proxy.SetDelay(0)

	// The next run gets the declaration wrong first — a mistake a deploy makes
	// and then corrects, in the same process.
	second := openStore(t, cfg)
	if _, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[FillWrongNewID]{Namespace: "market", Table: "fills"}); err == nil {
		t.Fatal("OpenWriter with a bad declaration succeeded, want a refusal")
	}

	// …and then gets it right. The pending rows must still be there to claim.
	fixed, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after the refused one: %v", err)
	}
	if pending, _ := fixed.Pending(); pending != records {
		t.Errorf("Pending() = %d after reopening behind a failed open, want the %d staged rows", pending, records)
	}

	drain, cancelDrain := context.WithTimeout(ctx, time.Minute)
	defer cancelDrain()
	if err := fixed.Close(drain); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the drain, want 0", got)
	}

	duck := openDuckDB(t, bucket)
	var total, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total, &distinct)
	if total != records || distinct != records {
		t.Errorf("committed %d rows with %d distinct ids, want %d of each", total, distinct, records)
	}
}

// TestSecondWriterForOneTableIsRefused proves the one half of the
// single-writer-per-table rule icelake can see: two writers on one table inside
// one store.
//
// Left unenforced, the second writer is not a harmless duplicate. Both would
// batch the same staged rows and commit against each other's metadata, and the
// loser would fail its requirements on every attempt from then on — a table
// that can never commit again, with its records piling up behind it.
func TestSecondWriterForOneTableIsRefused(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	first, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	_, err = icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if !errors.Is(err, icelake.ErrTableInUse) {
		t.Fatalf("second OpenWriter for the same table returned %v, want ErrTableInUse", err)
	}

	// Another table is unaffected: the rule is per table, not per store.
	other, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		t.Fatalf("OpenWriter for a different table: %v", err)
	}
	if err := other.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Closing releases the table, so a service that reopens one deliberately —
	// to change its declaration, say — is not blocked by the writer it just
	// closed.
	if err := first.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after closing the first writer: %v", err)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestFlushFailureIsRetriedByTheNextTrigger proves the part of failure handling
// that belongs to the API contract rather than to the retry policy: a flush
// that cannot reach the store fails its caller, keeps its batch sealed at the
// head of the queue, and commits it on the next trigger once the store answers
// again — from a table handle reloaded after the failure rather than the one
// that failed.
//
// It is unchanged by the bounded retry M8 added, and deliberately so: what a
// caller sees here is the same error about the same batch, after the cycle
// behind it has spent its attempts instead of stopping at one. The retry
// budget, the backoff schedule, the flush-error callback and the self-healing
// arc are scenario 8's, in scenario8_test.go; what this test holds is that
// nothing is lost, nothing is duplicated, and a table that failed once is not
// wedged.
func TestFlushFailureIsRetriedByTheNextTrigger(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// A first batch that commits normally, so the retry below happens against a
	// table that already has a snapshot rather than an empty one.
	if err := writer.Insert(ctx, makeFill(0)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// A second batch, flushed while the store is down.
	for i := 1; i < 5; i++ {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	proxy.SetReject(true)

	err = writer.Flush(ctx)

	var flushErr icelake.FlushError
	if !errors.As(err, &flushErr) {
		t.Fatalf("Flush against an unreachable store returned %v (%T), want a FlushError", err, err)
	}
	if flushErr.Records != 4 || flushErr.BatchKey == "" {
		t.Errorf("FlushError describes %d records under key %q, want 4 records under the batch's own key",
			flushErr.Records, flushErr.BatchKey)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 4 {
		t.Errorf("staging holds %d rows after the failed flush, want the 4 that could not commit", got)
	}
	if got := sealedRows(t, cfg.StagingPath); got != 4 {
		t.Errorf("%d of the staged rows still carry their batch key, want all 4", got)
	}

	// The store answers again; the next trigger retries the same batch, as a
	// fresh attempt against a table handle reloaded after the failure.
	proxy.SetReject(false)
	retry, cancelRetry := context.WithTimeout(ctx, time.Minute)
	defer cancelRetry()
	if err := writer.Flush(retry); err != nil {
		t.Fatalf("Flush after the store came back: %v", err)
	}
	if err := writer.Close(retry); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the retry committed, want 0", got)
	}

	duck := openDuckDB(t, bucket)
	var total, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total, &distinct)
	if total != 5 || distinct != 5 {
		t.Errorf("committed %d rows with %d distinct ids, want 5 of each exactly once", total, distinct)
	}
}

// TestReopenAfterAFailedDrainRecoversInTheSameProcess is the regression test
// for the durability hole a close-then-reopen used to fall into.
//
// A drain that runs out of time leaves its records staged — that is the design,
// and Close says so. The natural next move for a caller that got an error back
// from Close is to open the table again and try once more, in the same process.
// That used to hand the new writer nothing: the rows had been staged after the
// process started, so the startup snapshot had never seen them, and the writer
// reported no pending records at all while six durably-accepted records sat in
// staging for the life of the process. Nothing failed; the records simply
// stopped existing as far as the program was concerned.
func TestReopenAfterAFailedDrainRecoversInTheSameProcess(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	const records = 6
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	// The drain cannot finish, so Close reports that and leaves the records.
	proxy.SetDelay(time.Minute)
	budget, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := writer.Close(budget); err == nil {
		t.Fatal("Close whose context expired mid-drain returned no error, want one")
	}
	if got := stagedRows(t, cfg.StagingPath); got != records {
		t.Fatalf("staging holds %d rows after the failed drain, want %d", got, records)
	}

	// Same process, same store: open the table again and try once more.
	proxy.SetDelay(0)
	reopened, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after the failed drain: %v", err)
	}
	if pending, _ := reopened.Pending(); pending != records {
		t.Fatalf("Pending() = %d on the reopened writer, want the %d records the first one could not drain", pending, records)
	}

	drain, cancelDrain := context.WithTimeout(ctx, time.Minute)
	defer cancelDrain()
	if err := reopened.Flush(drain); err != nil {
		t.Fatalf("Flush on the reopened writer: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the reopened writer drained, want 0", got)
	}

	// Records inserted after the reopen ride the same path, so the recovery is
	// a working writer rather than a one-shot drain.
	for i := records; i < records+2; i++ {
		if err := reopened.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d after the reopen: %v", i, err)
		}
	}
	if err := reopened.Close(drain); err != nil {
		t.Fatalf("Close: %v", err)
	}

	duck := openDuckDB(t, bucket)
	var total, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total, &distinct)
	if total != records+2 || distinct != records+2 {
		t.Errorf("committed %d rows with %d distinct ids, want %d of each exactly once", total, distinct, records+2)
	}
}

// TestOpenWriterOnAClosedStoreIsRefused proves the API contract holds at the
// boundary rather than at whichever resource happens to be touched first.
//
// Opening a writer against a closed store used to reach the catalog before the
// closed check, so the caller got a driver's own "database is closed" wrapped
// inside an error about schema reconciliation: the wrong error, and one no
// caller could match on.
func TestOpenWriterOnAClosedStoreIsRefused(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	// Opened and closed once, so the store is a used one rather than an empty
	// one, and its databases are genuinely shut.
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := writer.Insert(ctx, makeFill(1)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	if _, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"}); !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("OpenWriter on a closed store returned %v, want ErrClosed", err)
	}
	if _, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"}); !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("OpenWriter for an unopened table on a closed store returned %v, want ErrClosed", err)
	}
	if err := store.Close(ctx); !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("second Store.Close returned %v, want ErrClosed", err)
	}
}

// TestReopenDuringADrainIsRefusedUntilCloseReturns is the regression test for a
// bug two individually-correct changes made between them.
//
// Releasing a table when Close was entered is what lets a caller reopen it
// after a failed drain; reading staging fresh at every open is what lets that
// reopened writer find the records. Together they let a writer be opened while
// the previous one's worker was still draining — and that writer's own scan
// handed it the batch already in flight. Both then owned it: the first
// committed and pruned, and the second was left holding row ids that no longer
// existed, failing every attempt for the rest of the process's life. The
// records were never at risk, because the upload is idempotent and the catalog
// refuses the second commit; the writer was.
//
// The overlap is now unrepresentable: the table stays held until the worker has
// exited for certain, so an open during the drain is refused and an open after
// Close returns is not.
func TestReopenDuringADrainIsRefusedUntilCloseReturns(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	const records = 9
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	// The drain cannot finish inside its budget, which is what keeps the window
	// below wide open rather than a race to hit.
	const drainBudget = 2 * time.Second
	proxy.SetDelay(time.Minute)

	closed := make(chan error, 1)
	go func() {
		budget, cancel := context.WithTimeout(ctx, drainBudget)
		defer cancel()
		closed <- writer.Close(budget)
	}()

	// Every open attempted well inside the drain's own budget is attempted
	// during the drain, and every one of them must be refused.
	attempts := 0
	for deadline := time.Now().Add(drainBudget / 4); time.Now().Before(deadline); {
		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
		if !errors.Is(err, icelake.ErrTableInUse) {
			t.Fatalf("OpenWriter during the previous writer's drain returned %v, want ErrTableInUse", err)
		}
		attempts++
		time.Sleep(time.Millisecond)
	}
	if attempts == 0 {
		t.Fatal("no open was attempted during the drain window")
	}
	t.Logf("%d opens refused during the drain", attempts)

	if err := <-closed; err == nil {
		t.Fatal("Close whose context expired mid-drain returned no error, want one")
	}
	if got := stagedRows(t, cfg.StagingPath); got != records {
		t.Fatalf("staging holds %d rows after the failed drain, want %d", got, records)
	}

	// Close has returned, so the table is free again — and the reopened writer
	// inherits exactly the records the first one could not deliver.
	proxy.SetDelay(0)
	reopened, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after Close returned: %v", err)
	}
	if pending, _ := reopened.Pending(); pending != records {
		t.Errorf("Pending() = %d on the reopened writer, want %d", pending, records)
	}

	drain, cancelDrain := context.WithTimeout(ctx, time.Minute)
	defer cancelDrain()
	if err := reopened.Close(drain); err != nil {
		t.Fatalf("Close on the reopened writer: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the reopened writer drained, want 0", got)
	}

	duck := openDuckDB(t, bucket)
	var total, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total, &distinct)
	if total != records || distinct != records {
		t.Errorf("committed %d rows with %d distinct ids, want %d of each exactly once", total, distinct, records)
	}
	if files := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(files) != 1 {
		t.Errorf("the table has %d data files, want 1: %s", len(files), describe(files))
	}
}
