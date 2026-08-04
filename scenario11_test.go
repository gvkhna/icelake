package icelake_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/flush"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TestSpoolFileIsTheUploadedObject is the first two clauses of TESTING.md's
// scenario 11: the cache holds the object, at the mirrored path, byte for byte —
// and DuckDB reading the directory sees exactly what DuckDB reading the table
// sees.
//
// The two claims share one container and one written state deliberately: they
// are two questions about the same files, and writing them twice would double
// the substrate cost of the scenario for nothing.
//
// The byte comparison is whole-file rather than by length. "Byte-identical" is
// the claim the spool's whole framing rests on — the local file is the uploaded
// buffer rather than a re-encoding — and a length check would pass for two files
// that differ everywhere but their size.
func TestSpoolFileIsTheUploadedObject(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := cacheConfig(t, bucket)
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// Three batches at the configured record threshold, so the claim is about a
	// directory of files rather than about one file that could have been
	// special.
	const records = 150
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(objects) < 3 {
		t.Fatalf("the table has %d data files, want at least 3 so the claim is about a directory", len(objects))
	}

	t.Run("the spool file is the object", func(t *testing.T) {
		cached := cachedFiles(t, cfg.CacheDir)
		if len(cached) != len(objects) {
			t.Fatalf("the cache holds %d files and the bucket %d objects; the spool must hold one file per uploaded batch\ncache: %v",
				len(cached), len(objects), cached)
		}

		for _, o := range objects {
			// The mirrored path: the same two segments, the same data
			// directory, the same name. An operator comparing the two needs no
			// lookup table, which is only true if this is literally the key with
			// the warehouse prefix swapped for the cache directory.
			rel := cachePathFor(cfg.WarehousePrefix, o.key)
			if _, err := os.Stat(filepath.Join(cfg.CacheDir, rel)); err != nil {
				t.Fatalf("no cache file at the mirrored path for %s: %v", o.key, err)
			}
			if got, want := readCached(t, cfg.CacheDir, rel), objectBody(t, bucket, o.key); !bytes.Equal(got, want) {
				t.Errorf("the cached file %s is %d bytes and its object is %d bytes, and they differ; the spool writes the buffer the upload sends",
					rel, len(got), len(want))
			}
		}

		// Every file is marked committed, which is what makes it evictable and
		// what tells an operator it no longer has to be backed up.
		rows := spoolRows(t, cfg.StagingPath)
		if len(rows) != len(objects) {
			t.Fatalf("spool_files holds %d rows for %d objects", len(rows), len(objects))
		}
		for _, r := range rows {
			if !r.committed {
				t.Errorf("spool file %s is not marked committed after its batch committed", r.batchKey)
			}
			if r.namespace != "market" || r.table != "fills" {
				t.Errorf("spool file %s is recorded under %s.%s", r.batchKey, r.namespace, r.table)
			}
		}
	})

	t.Run("duckdb reads the cache and the table alike", func(t *testing.T) {
		duck := openDuckDB(t, bucket)

		columns := "symbol, side, price, quantity, sequence_id, order_id, source, venue_timestamp_ms"
		cache := fmt.Sprintf("SELECT %s FROM read_parquet('%s')", columns,
			filepath.Join(cfg.CacheDir, "market", "fills", "data", "*.parquet"))
		table := fmt.Sprintf("SELECT %s FROM %s", columns,
			duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")))

		var inCacheOnly, inTableOnly, cacheRows int
		duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM (%s EXCEPT %s)", cache, table), &inCacheOnly)
		duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM (%s EXCEPT %s)", table, cache), &inTableOnly)
		duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM (%s)", cache), &cacheRows)

		if inCacheOnly != 0 || inTableOnly != 0 {
			t.Errorf("read_parquet over the cache and iceberg_scan over the table disagree: %d rows only in the cache, %d only in the table",
				inCacheOnly, inTableOnly)
		}
		if cacheRows != records {
			t.Errorf("read_parquet over the cache returned %d rows, want the %d written", cacheRows, records)
		}
	})
}

// TestRetentionEvictsTheOldestCommittedFile is scenario 11's retention clause:
// with a bound set and time advanced past it, the oldest committed file is gone
// and the newest is not, the bucket is untouched, and the table still reads back
// complete.
//
// Time is driven through the injected clock, which is the one seam TESTING.md
// allows. Retention is the one behaviour here whose entire subject is elapsed
// time — how old a file is, and how often the sweeper looks — so against a real
// clock this test would wait a minute per sweep and then assert on how long the
// machine happened to take. Everything else is real: the same container, real
// files on a real disk, and the sweeper's own goroutine doing its own work.
func TestRetentionEvictsTheOldestCommittedFile(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	clock := newSweepClock()
	cfg := cacheConfig(t, bucket)
	cfg.Clock = clock
	cfg.CacheMaxBytes = 0
	cfg.CacheMaxAge = time.Hour

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// Two batches two hours apart on the injected clock, so exactly one of them
	// is older than the bound when the sweep runs.
	writeBatch := func(from, to int) {
		for i := from; i < to; i++ {
			if err := writer.Insert(ctx, makeFill(i)); err != nil {
				t.Fatalf("Insert %d: %v", i, err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	writeBatch(0, 10)
	clock.Advance(2 * time.Hour)
	writeBatch(10, 20)

	rows := spoolRows(t, cfg.StagingPath)
	if len(rows) != 2 {
		t.Fatalf("spool_files holds %d rows, want the 2 batches this test flushed", len(rows))
	}
	oldest, newest := rows[0], rows[1]
	oldestPath := filepath.Join("market", "fills", "data", oldest.batchKey+".parquet")
	newestPath := filepath.Join("market", "fills", "data", newest.batchKey+".parquet")

	// Two files icelake did not write: one beside the cache's own tree and one
	// inside it. Eviction deletes exactly the paths its own table names and
	// never lists the directory to decide, so neither may be touched — including
	// by the sweep that is about to delete something.
	strays := map[string]string{
		filepath.Join(cfg.CacheDir, "operator-notes.txt"):                     "not icelake's",
		filepath.Join(cfg.CacheDir, "market", "fills", "data", "stray.bytes"): "also not icelake's",
	}
	for p, body := range strays {
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("planting %s: %v", p, err)
		}
	}

	before := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(before) != 2 {
		t.Fatalf("the bucket holds %d data files, want 2", len(before))
	}

	// The clock already stands two hours past the first batch, so one sweep is
	// enough to make the age bound bite.
	clock.Tick(t)
	waitForSpoolState(t, cfg.StagingPath, "the sweeper to evict the file that is past its age bound",
		func(rows []spoolRow, _ int) bool { return len(rows) == 1 })

	if _, err := os.Stat(filepath.Join(cfg.CacheDir, oldestPath)); !os.IsNotExist(err) {
		t.Errorf("the file past its age bound is still on disk (stat error %v)", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.CacheDir, newestPath)); err != nil {
		t.Errorf("the newest committed file was evicted too (%v); retention evicts oldest first, and this one is inside its bound", err)
	}
	if remaining := uncommittedSpoolFiles(t, cfg.StagingPath); remaining != 0 {
		t.Errorf("%d spool files are uncommitted; this test committed both", remaining)
	}
	if rows := spoolRows(t, cfg.StagingPath); len(rows) != 1 || rows[0].batchKey != newest.batchKey {
		t.Errorf("spool_files holds %+v after the eviction, want only the newest batch %s", rows, newest.batchKey)
	}

	for p, want := range strays {
		body, err := os.ReadFile(p)
		if err != nil {
			t.Errorf("a file icelake did not write was removed by a sweep: %s (%v)", p, err)

			continue
		}
		if string(body) != want {
			t.Errorf("a file icelake did not write was rewritten by a sweep: %s", p)
		}
	}

	// Eviction is free precisely because nothing was lost: the bucket is
	// untouched and the table still reads back everything written.
	after := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(after) != len(before) {
		t.Errorf("the bucket holds %d data files after a sweep, want the %d it held before; retention touches local disk only",
			len(after), len(before))
	}

	// A committed file an operator deleted by hand is not an error. It is
	// disposable — byte-identical to an object in a committed snapshot — so the
	// right response to finding it gone is to forget its row, not to report a
	// failure nobody is listening for. The surviving file is deleted here and
	// then aged past the bound, so the next sweep meets a tracked file that is
	// not there.
	if err := os.Remove(filepath.Join(cfg.CacheDir, newestPath)); err != nil {
		t.Fatalf("deleting a committed cache file by hand: %v", err)
	}
	clock.Advance(2 * time.Hour)
	clock.Tick(t)
	waitForSpoolState(t, cfg.StagingPath, "the sweeper to forget the row of a file that was deleted by hand",
		func(rows []spoolRow, descriptors int) bool { return len(rows) == 0 && descriptors == 0 })

	// Eviction is free precisely because nothing was lost: the bucket is
	// untouched and the table still reads back everything written.
	after = dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(after) != len(before) {
		t.Errorf("the bucket holds %d data files after every eviction, want the %d it held before", len(after), len(before))
	}

	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s",
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))), &total)
	if total != 20 {
		t.Errorf("the table reads back %d rows after an eviction, want 20", total)
	}
}

// TestRetentionEvictsBySizeUntilTheCacheFits is the size half of scenario 11's
// retention clause, and it is deliberately a *partial* eviction.
//
// The age test beside it can only ever say "evict what is too old", which a
// sweeper that deleted the whole committed set would satisfy just as well. The
// bound here sits between one file and two, so the only correct answer is to
// delete exactly one — the older one — and stop. A sweeper that evicted greedily
// until the cache was empty, or that stopped before the bound was met, fails
// here and nowhere else in this scenario.
func TestRetentionEvictsBySizeUntilTheCacheFits(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	clock := newSweepClock()
	cfg := cacheConfig(t, bucket)
	cfg.Clock = clock
	cfg.CacheMaxAge = 0

	// The bound is decided after the files exist, so it can be placed between
	// them rather than guessed at: a threshold picked in advance would be a
	// threshold this test was tuned to rather than one it proves anything about.
	// Until then the cache is bounded by a size nothing here approaches.
	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	for _, batch := range [][2]int{{0, 10}, {10, 20}} {
		for i := batch[0]; i < batch[1]; i++ {
			if err := writer.Insert(ctx, makeFill(i)); err != nil {
				t.Fatalf("Insert %d: %v", i, err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		// Distinct creation times, so "oldest first" has an order to be first in.
		clock.Advance(time.Minute)
	}

	rows := spoolRows(t, cfg.StagingPath)
	if len(rows) != 2 {
		t.Fatalf("spool_files holds %d rows, want the 2 batches this test flushed", len(rows))
	}
	oldest, newest := rows[0], rows[1]

	// Between one file and two: the newest alone fits, the pair does not.
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}
	clock = newSweepClock()
	cfg.Clock = clock
	cfg.CacheMaxBytes = newest.byteLen + oldest.byteLen - 1
	if cfg.CacheMaxBytes < newest.byteLen {
		t.Fatalf("the two batches are %d and %d bytes; a bound between them does not exist", oldest.byteLen, newest.byteLen)
	}
	reopened := openStore(t, cfg)
	if _, err := icelake.OpenWriter(ctx, reopened, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"}); err != nil {
		t.Fatalf("OpenWriter after reopening under the tighter bound: %v", err)
	}

	clock.Tick(t)
	waitForSpoolState(t, cfg.StagingPath, "the sweeper to evict down to the size bound",
		func(rows []spoolRow, _ int) bool { return len(rows) == 1 })

	// A second sweep, so that "and stop" is a checked claim rather than a
	// snapshot taken before the sweeper had finished having ideas.
	clock.Sweeps(t, 2)

	remaining := spoolRows(t, cfg.StagingPath)
	if len(remaining) != 1 || remaining[0].batchKey != newest.batchKey {
		t.Fatalf("spool_files holds %+v, want only the newest batch %s: eviction is oldest first and stops at the bound",
			remaining, newest.batchKey)
	}
	if _, err := os.Stat(filepath.Join(cfg.CacheDir, "market", "fills", "data", oldest.batchKey+".parquet")); !os.IsNotExist(err) {
		t.Errorf("the oldest committed file is still on disk (stat error %v)", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.CacheDir, "market", "fills", "data", newest.batchKey+".parquet")); err != nil {
		t.Errorf("the newest committed file was evicted too (%v); the cache was inside its bound without it", err)
	}
}

// TestUncommittedSpoolFilesSurviveEverySweep is the assertion that matters most
// in scenario 11, in that document's own words.
//
// A file the bucket has never confirmed is not protected by policy code that
// could be edited wrong — it is invisible to the statement that does the
// deleting. So this drives the cache far over a deliberately tiny bound with an
// endpoint that refuses everything, sweeps repeatedly, and requires that nothing
// at all is deleted; then heals the endpoint and requires that the same files,
// now committed, are evicted. A sweeper that filtered uncommitted files in Go
// rather than in the statement would pass every other assertion in this scenario
// and fail this one.
func TestUncommittedSpoolFilesSurviveEverySweep(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	clock := newSweepClock()

	cfg := cacheConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	cfg.Clock = clock
	// One byte: everything this test writes is far over it, so a sweep that can
	// see a file will delete it.
	cfg.CacheMaxBytes = 1

	store := openStore(t, cfg)
	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter fills: %v", err)
	}
	events, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		t.Fatalf("OpenWriter events: %v", err)
	}

	// From here nothing reaches the store. Records keep flowing and spool files
	// keep landing, because the spool write is before the upload.
	proxy.SetReject(true)

	for i := range 10 {
		if err := fills.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert fill %d: %v", i, err)
		}
		if err := events.Insert(ctx, makeEvent(i)); err != nil {
			t.Fatalf("Insert event %d: %v", i, err)
		}
	}
	if err := fills.Flush(ctx); err == nil {
		t.Fatal("Flush succeeded against an endpoint that refuses every connection")
	}
	if err := events.Flush(ctx); err == nil {
		t.Fatal("Flush succeeded against an endpoint that refuses every connection")
	}

	stranded := cachedFiles(t, cfg.CacheDir)
	if len(stranded) != 2 {
		t.Fatalf("the cache holds %v, want one file per table written while the endpoint was down", stranded)
	}
	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 2 {
		t.Fatalf("%d spool files are uncommitted, want 2; nothing was uploaded", n)
	}

	// Sweep after sweep, far over the bound, with nothing evictable in sight.
	// Four ticks are delivered and each one is taken by the sweeper's own
	// goroutine before Tick returns, so three sweeps have certainly run to
	// completion by the time this returns and the fourth has certainly started.
	// That is the whole reason the tick is a blocking hand-off: this is the one
	// claim in the scenario whose correct behaviour is that nothing happens, so
	// "the sweeps ran" cannot be inferred from any effect they had.
	clock.Advance(time.Hour)
	clock.Sweeps(t, 4)

	if got := cachedFiles(t, cfg.CacheDir); len(got) != 2 {
		t.Fatalf("the cache holds %v after sweep upon sweep far over its bound, want the 2 files no bucket has confirmed", got)
	}
	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 2 {
		t.Fatalf("%d spool files are uncommitted after the sweeps, want the 2 that were before them", n)
	}

	// Healed. The backlog commits through the ordinary retry, and only then do
	// the files become evictable.
	proxy.SetReject(false)
	if err := fills.Flush(ctx); err != nil {
		t.Fatalf("Flush after the endpoint healed: %v", err)
	}
	if err := events.Flush(ctx); err != nil {
		t.Fatalf("Flush after the endpoint healed: %v", err)
	}
	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 0 {
		t.Fatalf("%d spool files are still uncommitted after the backlog committed", n)
	}

	clock.Advance(time.Hour)
	clock.Tick(t)
	// The sweep's last act is its own descriptor prune, so waiting for that is
	// waiting for the whole sweep rather than for the first thing it did.
	waitForSpoolState(t, cfg.StagingPath, "the sweeper to evict the files that just became committed",
		func(rows []spoolRow, descriptors int) bool { return len(rows) == 0 && descriptors == 0 })

	if files := cachedFiles(t, cfg.CacheDir); len(files) != 0 {
		t.Errorf("the cache still holds %v after every file became evictable", files)
	}

	// Nothing was lost by any of it: both tables read back complete.
	duck := openDuckDB(t, bucket)
	for _, c := range []struct {
		namespace, table string
	}{{"market", "fills"}, {"archive", "events"}} {
		var total int
		duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s",
			duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, c.namespace, c.table))), &total)
		if total != 10 {
			t.Errorf("%s.%s reads back %d rows, want 10", c.namespace, c.table, total)
		}
	}
}

// TestCrashAfterSpoolBeforeUploadRewritesTheIdenticalFile is scenario 11's first
// crash variant, at the seam the spool opens.
//
// At that instant a Parquet file exists on local disk that no bucket has ever
// seen. The restarted writer must rebuild the identical batch, rewrite the
// identical file, upload it under the stored batch key and commit it once — one
// data file, one snapshot, no duplicate rows.
func TestCrashAfterSpoolBeforeUploadRewritesTheIdenticalFile(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := cacheConfig(t, bucket)

	const records = 20
	plan := crashPlan(cfg)
	plan.Script = scriptFlushCrash
	plan.Fills = records
	plan.CrashPoint = flush.PointAfterSpoolBeforeUpload
	plan.CrashMode = crashExit

	child := startCrashChild(t, plan)
	child.await(t, lineStaged)
	child.awaitCrashPoint(t, plan.CrashPoint)

	// What the dead process left: a file on disk, a row that knows nothing about
	// a bucket, and an empty bucket.
	cached := cachedFiles(t, cfg.CacheDir)
	if len(cached) != 1 {
		t.Fatalf("the cache holds %v after a crash at the spool seam, want exactly the one file the batch wrote", cached)
	}
	before := readCached(t, cfg.CacheDir, cached[0])
	if rows := spoolRows(t, cfg.StagingPath); len(rows) != 1 || rows[0].committed {
		t.Fatalf("spool_files holds %+v, want one row that has never reached a bucket", rows)
	}
	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(objects) != 0 {
		t.Fatalf("the bucket holds %d data files after a crash before the upload", len(objects))
	}

	// The restart. Replay owns this batch — its rows are still staged — so the
	// transition drain must leave it alone and the ordinary flush path must
	// finish it.
	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after the crash: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close after the crash: %v", err)
	}

	objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(objects) != 1 {
		t.Fatalf("the table has %d data files after the restart, want 1: the retry rewrites the same key rather than adding a second",
			len(objects))
	}
	if n := snapshotCount(t, bucket, cfg.WarehousePrefix, "market", "fills"); n != 1 {
		t.Errorf("the table has %d snapshots after the restart, want 1", n)
	}
	if got, want := cachedFiles(t, cfg.CacheDir), cached; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("the cache holds %v after the restart, want the same one file %v", got, want)
	}
	if after := readCached(t, cfg.CacheDir, cached[0]); !bytes.Equal(after, before) {
		t.Errorf("the rewritten spool file differs from the one the dead process wrote (%d bytes against %d); the same batch encodes to the same bytes",
			len(after), len(before))
	}
	if got, want := objectBody(t, bucket, objects[0].key), before; !bytes.Equal(got, want) {
		t.Errorf("the uploaded object differs from the file on disk")
	}
	if rows := spoolRows(t, cfg.StagingPath); len(rows) != 1 || !rows[0].committed {
		t.Errorf("spool_files holds %+v after the restart, want one row marked committed", rows)
	}
	if n := stagedRows(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d rows are still staged after the batch committed", n)
	}

	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s",
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))), &total)
	if total != records {
		t.Errorf("the table reads back %d rows, want the %d the dead process had accepted", total, records)
	}
}

// TestCrashAfterCommitBeforeMarkMarksTheSpoolFileOnTheNextRun is scenario 11's
// second crash variant, and it is about the one window the mark's ordering
// leaves open.
//
// Marking the file committed happens *after* the commit lands, so a crash
// between them leaves a file the table already holds whose row still says it has
// never reached a bucket. That is the harmless direction — a file kept too long
// costs disk — and the next run must close it: replay finds the batch already
// committed, marks the file rather than committing a second time, and the file
// becomes evictable from then on.
func TestCrashAfterCommitBeforeMarkMarksTheSpoolFileOnTheNextRun(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := cacheConfig(t, bucket)

	const records = 20
	plan := crashPlan(cfg)
	plan.Script = scriptFlushCrash
	plan.Fills = records
	plan.CrashPoint = flush.PointAfterCommitBeforePrune
	plan.CrashMode = crashExit

	child := startCrashChild(t, plan)
	child.await(t, lineStaged)
	child.awaitCrashPoint(t, plan.CrashPoint)

	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(objects) != 1 {
		t.Fatalf("the bucket holds %d data files after a crash between the commit and the prune, want 1", len(objects))
	}
	if rows := spoolRows(t, cfg.StagingPath); len(rows) != 1 || rows[0].committed {
		t.Fatalf("spool_files holds %+v; a crash before the mark must leave the file unmarked, which is the safe direction", rows)
	}
	if n := sealedRows(t, cfg.StagingPath); n != records {
		t.Fatalf("%d sealed rows survive the crash, want the %d that were never pruned", n, records)
	}

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after the crash: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close after the crash: %v", err)
	}

	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(objects) != 1 {
		t.Errorf("the table has %d data files after the restart, want the 1 the dead process committed", len(objects))
	}
	if n := snapshotCount(t, bucket, cfg.WarehousePrefix, "market", "fills"); n != 1 {
		t.Errorf("the table has %d snapshots after the restart, want 1: the already-committed batch must not commit twice", n)
	}
	if rows := spoolRows(t, cfg.StagingPath); len(rows) != 1 || !rows[0].committed {
		t.Errorf("spool_files holds %+v after the restart, want the file marked committed and therefore evictable", rows)
	}
	if n := stagedRows(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d rows are still staged after the restart pruned an already-committed batch", n)
	}

	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s",
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))), &total)
	if total != records {
		t.Errorf("the table reads back %d rows, want %d exactly once", total, records)
	}
}
