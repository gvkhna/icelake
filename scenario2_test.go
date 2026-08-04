package icelake_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/flush"
	"github.com/gvkhna/icelake/internal/schemamap"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TestCrashMidStreamRecoversEveryAcceptedRecord is TESTING.md's scenario 2 in
// its base form: a process killed while it is accepting records, before any
// batch reached a flush threshold, so nothing was ever encoded, uploaded or
// committed.
//
// The claim is the one the whole write-ahead layer exists for. A record that
// Insert returned success for is durable at that instant, and the next run
// finds it, flushes it and commits it exactly once — from the staging file
// alone, because there is nothing else left.
//
// It also covers scenario 2's third bullet directly: two tables staging into
// the one shared staging.db are both recovered and routed to the right table,
// with neither table's rows appearing in the other's. The single ordered replay
// scan is what makes that worth checking rather than assuming.
func TestCrashMidStreamRecoversEveryAcceptedRecord(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	// Both thresholds are put out of reach, because the scenario is explicitly
	// a crash *before* the batch's size or time threshold trips: nothing may
	// have been uploaded or committed at the moment of the kill.
	cfg.FlushMaxRecords = 10_000
	cfg.FlushMaxBytes = 64 << 20
	cfg.FlushInterval = time.Hour

	plan := crashPlan(cfg)
	plan.Script = scriptStream
	plan.Fills, plan.Events = unbounded, unbounded

	// The child inserts into both tables in lockstep and never stops. The kill
	// therefore lands while it is genuinely mid-stream — there is no last
	// insert for it to have finished — and it lands only once the child has
	// said, on a pipe, that a particular record of each table is already
	// durable. Neither half of that is a timing hope.
	const acknowledged = 20
	child := startCrashChild(t, plan)
	child.await(t, acceptedLine("event", acknowledged-1))
	child.sigkill(t)

	fills := stagedRowsFor(t, cfg.StagingPath, plan.FillNamespace, plan.FillTable)
	events := stagedRowsFor(t, cfg.StagingPath, plan.EventNamespace, plan.EventTable)
	t.Logf("the kill left %d staged fills and %d staged events", len(fills), len(events))

	// Insert's contract, checked against the file rather than against icelake's
	// own opinion of it: every record it acknowledged is on disk. It may have
	// accepted more that the parent never read an acknowledgement for, which is
	// why this is a floor and the exact figure below comes from the file.
	if len(fills) < acknowledged || len(events) < acknowledged {
		t.Fatalf("staging holds %d fills and %d events after the kill, want at least the %d of each the child acknowledged",
			len(fills), len(events), acknowledged)
	}
	for _, r := range slices.Concat(fills, events) {
		if r.batchKey != "" {
			t.Fatalf("staged row %d was already sealed; the kill was meant to land before any threshold tripped", r.id)
		}
	}
	// Nothing reached the bucket, which is what makes this a test of recovery
	// from staging rather than of a partly-committed table. Both halves are
	// checked: no Parquet file was written, and no Iceberg commit happened.
	if got := len(dataFiles(t, bucket, cfg.WarehousePrefix, plan.FillNamespace, plan.FillTable)); got != 0 {
		t.Fatalf("the crashed run left %d data files under the fills table, want 0", got)
	}
	if got := len(dataFiles(t, bucket, cfg.WarehousePrefix, plan.EventNamespace, plan.EventTable)); got != 0 {
		t.Fatalf("the crashed run left %d data files under the events table, want 0", got)
	}
	if got := snapshotCount(t, bucket, cfg.WarehousePrefix, plan.FillNamespace, plan.FillTable); got != 0 {
		t.Fatalf("the crashed run left %d snapshots on the fills table, want 0", got)
	}
	if got := snapshotCount(t, bucket, cfg.WarehousePrefix, plan.EventNamespace, plan.EventTable); got != 0 {
		t.Fatalf("the crashed run left %d snapshots on the events table, want 0", got)
	}

	// The next run: same staging file, same bucket, a fresh process.
	store := openStore(t, cfg)
	recoveredFills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: plan.FillNamespace, Table: plan.FillTable,
	})
	if err != nil {
		t.Fatalf("OpenWriter for the fills table: %v", err)
	}
	recoveredEvents, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{
		Namespace: plan.EventNamespace, Table: plan.EventTable,
	})
	if err != nil {
		t.Fatalf("OpenWriter for the events table: %v", err)
	}

	// Each writer claimed its own table's rows and nothing else's.
	if pending, _ := recoveredFills.Pending(); pending != len(fills) {
		t.Errorf("the fills writer picked up %d records, want the %d staged for it", pending, len(fills))
	}
	if pending, _ := recoveredEvents.Pending(); pending != len(events) {
		t.Errorf("the events writer picked up %d records, want the %d staged for it", pending, len(events))
	}

	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := recoveredFills.Close(drain); err != nil {
		t.Fatalf("Close on the recovered fills writer: %v", err)
	}
	if err := recoveredEvents.Close(drain); err != nil {
		t.Fatalf("Close on the recovered events writer: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the recovery run drained it, want 0", got)
	}

	duck := openDuckDB(t, bucket)

	// The recovered records are contiguous from zero, so the count, the
	// distinct count and the bounds together say every accepted record arrived
	// exactly once — not merely that the right number of rows exist.
	fillsTable := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, plan.FillNamespace, plan.FillTable))
	var total, distinct, lowest, highest int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id), min(sequence_id), max(sequence_id) FROM "+fillsTable,
		&total, &distinct, &lowest, &highest)
	if total != len(fills) || distinct != len(fills) || lowest != 0 || highest != len(fills)-1 {
		t.Errorf("the fills table reads back %d rows, %d distinct, ids %d..%d; want %d of each over ids 0..%d",
			total, distinct, lowest, highest, len(fills), len(fills)-1)
	}

	eventsTable := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, plan.EventNamespace, plan.EventTable))
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_ordinal), min(sequence_ordinal), max(sequence_ordinal) FROM "+eventsTable,
		&total, &distinct, &lowest, &highest)
	if total != len(events) || distinct != len(events) || lowest != 0 || highest != len(events)-1 {
		t.Errorf("the events table reads back %d rows, %d distinct, ids %d..%d; want %d of each over ids 0..%d",
			total, distinct, lowest, highest, len(events), len(events)-1)
	}

	// Routed, not merely counted: a spot record from each table is the record
	// that table's own generator makes, so a row that crossed between the two
	// during the shared replay scan would be visible as content and not only as
	// an arithmetic discrepancy.
	var orderID string
	duck.queryRow(t, fmt.Sprintf("SELECT order_id FROM %s WHERE sequence_id = %d", fillsTable, acknowledged-1), &orderID)
	if want := makeFill(acknowledged - 1).OrderID; orderID != want {
		t.Errorf("fill %d read back as order %q, want %q", acknowledged-1, orderID, want)
	}
	var sum string
	duck.queryRow(t, fmt.Sprintf("SELECT payload_sha256 FROM %s WHERE sequence_ordinal = %d", eventsTable, acknowledged-1), &sum)
	if want := makeEvent(acknowledged - 1).PayloadSHA256; sum != want {
		t.Errorf("event %d read back with payload hash %s, want %s", acknowledged-1, sum, want)
	}
}

// crashed is what a child that died at a named crash point left behind, read
// out of the staging file rather than out of icelake.
type crashed struct {
	cfg  icelake.Config
	plan childPlan
	rows []stagedRow
	key  string
}

// crashDuringFlush runs a child that stages one batch, flushes it, and dies at
// the named point inside that flush; then it checks what every variant-2 case
// must be able to assume before it asserts anything of its own.
//
// The two universal checks are the ones the whole variant rests on. The batch
// was sealed *durably* before it was encoded — every staged row carries the
// same batch key — and that key is what the batch's own contents hash to, which
// the test recomputes for itself from the payloads and the recorded descriptor
// rather than taking icelake's word for it.
func crashDuringFlush(t *testing.T, bucket testsubstrate.Bucket, table, point string, records int) crashed {
	t.Helper()

	cfg := testConfig(t, bucket)
	// Only the explicit Flush may seal, so the crash point fires once, against
	// one batch of a known size.
	cfg.FlushMaxRecords = records + 10
	cfg.FlushInterval = time.Hour

	plan := crashPlan(cfg)
	plan.Script = scriptFlushCrash
	plan.FillTable = table
	plan.Fills = records
	plan.Events = 0
	plan.CrashPoint = point
	plan.CrashMode = crashExit

	child := startCrashChild(t, plan)
	child.await(t, lineStaged)
	child.awaitCrashPoint(t, point)

	rows := stagedRowsFor(t, cfg.StagingPath, plan.FillNamespace, plan.FillTable)
	if len(rows) != records {
		t.Fatalf("staging holds %d rows after the crash, want the %d that were accepted", len(rows), records)
	}
	key, payloads := sealedBatch(t, rows)
	if len(payloads) != records {
		t.Fatalf("%d of the %d staged rows carry the batch key, want all of them: a batch is sealed durably, whole, before it is encoded",
			len(payloads), records)
	}

	// The stored key is the batch's own content hash, recomputed here from the
	// payloads the crashed process left and the shape it recorded them under.
	// Without the durable seal this is exactly where content-hash keying would
	// be weakest: a replay that re-batched at a different record would hash to a
	// different, non-colliding name and orphan whatever had been uploaded.
	recorded := stagedDescriptor(t, cfg.StagingPath, rows[0].schemaFP)
	recomputed, err := canon.BatchHash(plan.FillNamespace, plan.FillTable, recorded, payloads)
	if err != nil {
		t.Fatalf("recomputing the batch hash: %v", err)
	}
	if recomputed != key {
		t.Errorf("the batch's rows hash to %s, but staging recorded the key %s", recomputed, key)
	}

	return crashed{cfg: cfg, plan: plan, rows: rows, key: key}
}

// recoverFills opens the table again in this process, drains it, and returns
// once every recovered batch has committed.
//
// The compression level is deliberately not the one the crashed run used. The
// batch key is a hash of the canonical in-memory batch and never of the encoded
// bytes, so the same logical batch must land on the same object key even though
// the bytes it is made of are different — which is the property that would
// break the moment the key became a hash of the compressed Parquet output.
func recoverFills(t *testing.T, c crashed, zstdLevel int) {
	t.Helper()

	cfg := c.cfg
	cfg.ZSTDLevel = zstdLevel

	ctx := context.Background()
	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: c.plan.FillNamespace, Table: c.plan.FillTable,
	})
	if err != nil {
		t.Fatalf("OpenWriter on the recovery run: %v", err)
	}
	if pending, _ := writer.Pending(); pending != len(c.rows) {
		t.Errorf("the recovered writer picked up %d records, want the %d left staged", pending, len(c.rows))
	}

	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := writer.Close(drain); err != nil {
		t.Fatalf("Close on the recovery run: %v", err)
	}
}

// TestCrashAtNamedFlushPoints is scenario 2's variant 2: a process killed at
// each of the exact moments inside a flush where recovery means something
// different.
//
// Every case ends in the same place — one data file under the batch's stored
// key, one snapshot referencing it, every record queryable exactly once — and
// gets there from a different starting state: nothing uploaded, uploaded but
// not committed, and committed but not yet pruned. The last of those is the
// only window in which replay can meet a batch that is *already* in the table,
// and it is where "exactly once" stops being a property of the happy path.
func TestCrashAtNamedFlushPoints(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	const records = 24

	// The recovery run compresses at a different level from the crashed one in
	// every case below, so an object key that survived by accident — because
	// the encoder happened to be deterministic — cannot pass.
	const crashedLevel = 1 // testConfig's level, which the child inherits
	const recoveryLevel = 9

	t.Run("after the seal, before the encode", func(t *testing.T) {
		c := crashDuringFlush(t, bucket, "fills_sealed", flush.PointAfterSealBeforeEncode, records)

		// Nothing was encoded, so nothing can have been uploaded or committed.
		if got := dataFiles(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable); len(got) != 0 {
			t.Fatalf("the crash left %d data files, want 0: %s", len(got), describe(got))
		}
		if got := snapshotCount(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable); got != 0 {
			t.Fatalf("the crash left %d snapshots, want 0", got)
		}

		recoverFills(t, c, recoveryLevel)
		assertRecovered(t, bucket, c, 1)
	})

	t.Run("after the upload, before the commit", func(t *testing.T) {
		c := crashDuringFlush(t, bucket, "fills_uploaded", flush.PointAfterUploadBeforeCommit, records)

		// The data file is in the bucket and the table does not know about it:
		// the exact window the idempotent-overwrite claim is about.
		uploaded := dataFiles(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable)
		if len(uploaded) != 1 {
			t.Fatalf("the crash left %d data files, want the 1 it uploaded: %s", len(uploaded), describe(uploaded))
		}
		if got, want := baseNames(uploaded)[0], c.key+".parquet"; got != want {
			t.Fatalf("the crashed run uploaded %s, want %s", got, want)
		}
		if got := snapshotCount(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable); got != 0 {
			t.Fatalf("the crash left %d snapshots, want 0 — it died before the commit", got)
		}

		recoverFills(t, c, recoveryLevel)
		current := assertRecovered(t, bucket, c, 1)

		// The same key, different bytes. The retry overwrote the object rather
		// than adding a second one, and it did so under a name that did not
		// move even though the compressed contents did — which is what proves
		// the key names the logical batch and not the encoder's output.
		if current[0].etag == uploaded[0].etag {
			t.Errorf("the object was not rewritten: it still has the entity tag %s written at ZSTD level %d, so the recovery run at level %d cannot have re-encoded it",
				uploaded[0].etag, crashedLevel, recoveryLevel)
		}
	})

	t.Run("after the commit, before the prune", func(t *testing.T) {
		c := crashDuringFlush(t, bucket, "fills_committed", flush.PointAfterCommitBeforePrune, records)

		// Committed, and still fully staged: the batch's rows are only ever
		// pruned after the commit is confirmed, so this window is where a
		// careless replay would commit the same data a second time.
		committed := dataFiles(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable)
		if len(committed) != 1 {
			t.Fatalf("the crash left %d data files, want 1: %s", len(committed), describe(committed))
		}
		if got := snapshotCount(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable); got != 1 {
			t.Fatalf("the crash left %d snapshots, want the 1 it committed", got)
		}

		recoverFills(t, c, recoveryLevel)
		current := assertRecovered(t, bucket, c, 1)

		// Nothing was written a second time. Replay found the batch already in
		// the table's current snapshot, skipped the commit and pruned staging —
		// so the object still carries the bytes the crashed run uploaded, at
		// its compression level rather than the recovery run's.
		if current[0].etag != committed[0].etag {
			t.Errorf("the committed object was rewritten (%s, was %s); a batch already confirmed committed must not be re-uploaded",
				current[0].etag, committed[0].etag)
		}
	})
}

// assertRecovered is the end state every variant-2 case must reach: one data
// file under the batch's stored key, the expected number of snapshots, an empty
// staging table, and every record queryable exactly once.
func assertRecovered(t *testing.T, bucket testsubstrate.Bucket, c crashed, wantSnapshots int) []object {
	t.Helper()

	files := dataFiles(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable)
	if len(files) != 1 {
		t.Fatalf("the table has %d data files, want 1: %s", len(files), describe(files))
	}
	// The stored key, not a recomputed one. A sealed batch's key is read back
	// from staging and never derived again, which is what keeps a retry an
	// overwrite of the same object.
	if got, want := baseNames(files)[0], c.key+".parquet"; got != want {
		t.Errorf("the replayed batch was uploaded as %s, want the key %s stamped in staging before the crash", got, want)
	}
	if got := snapshotCount(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable); got != wantSnapshots {
		t.Errorf("the table has %d snapshots, want %d", got, wantSnapshots)
	}
	if got := stagedRows(t, c.cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the recovery run, want 0", got)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, c.cfg.WarehousePrefix, c.plan.FillNamespace, c.plan.FillTable))

	want := len(c.rows)
	var total, distinct, lowest, highest int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id), min(sequence_id), max(sequence_id) FROM "+table,
		&total, &distinct, &lowest, &highest)
	if total != want || distinct != want || lowest != 0 || highest != want-1 {
		t.Errorf("the table reads back %d rows, %d distinct, ids %d..%d; want %d of each over ids 0..%d",
			total, distinct, lowest, highest, want, want-1)
	}

	// One record from the middle of the recovered batch, by value. Counts and
	// bounds prove no record was lost or duplicated; they say nothing about
	// whether the record that came back is the record that went in, and a
	// recovery path that decoded a staged payload wrongly would satisfy every
	// assertion above. The decimal is compared against the exact string the
	// unscaled value must render as, per scenario 1's standard.
	spot := want / 2
	expected := makeFill(spot)

	var symbol, side, orderID, price string
	var epochMS int64
	duck.queryRow(t, fmt.Sprintf(`
		SELECT symbol, side, order_id, CAST(price AS VARCHAR), epoch_ms(venue_timestamp_ms)
		FROM %s WHERE sequence_id = %d`, table, spot),
		&symbol, &side, &orderID, &price, &epochMS)

	if symbol != expected.Symbol || side != expected.Side || orderID != expected.OrderID || epochMS != expected.VenueTimeMS {
		t.Errorf("recovered record %d reads back as (%q, %q, %q, %d), want (%q, %q, %q, %d)",
			spot, symbol, side, orderID, epochMS, expected.Symbol, expected.Side, expected.OrderID, expected.VenueTimeMS)
	}
	if got := decimalString(expected.Price, 9); price != got {
		t.Errorf("recovered record %d has price %s, want exactly %s", spot, price, got)
	}

	return files
}

// TestCrashRecoveryUnderAChangedSchema is scenario 2's variant 3: the case
// where replay meets rows encoded under a shape the current binary no longer
// declares.
//
// The crashed run leaves both kinds of staged row at once — one batch sealed
// and never encoded, and further rows that were never sealed at all — and the
// next run declares one extra optional column. The two kinds must be treated
// differently, and the difference is the whole point of the fingerprint
// decision: the sealed batch keeps the key it was stamped with, because
// recomputing it under the new shape would name a different object; the
// unsealed rows are re-encoded under the new shape when they are sealed, and
// their key is therefore a hash of the new bytes.
func TestCrashRecoveryUnderAChangedSchema(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()

	const sealed = 50
	const unsealed = 7
	const witnesses = 3
	const written = sealed + unsealed
	const added = 5

	cfg := testConfig(t, bucket)
	cfg.FlushMaxRecords = sealed
	cfg.FlushInterval = time.Hour

	plan := crashPlan(cfg)
	plan.Script = scriptHoldSeal
	plan.Fills = sealed
	plan.Extra = unsealed
	// A handful of rows for a second table, staged and left alone. They are the
	// witnesses for the descriptor claim below: after the fills table drains,
	// the shapes it was written under must be gone from staging_schemas while
	// this table's own shape, which a live row still names, must not.
	plan.Events = witnesses
	plan.CrashPoint = flush.PointAfterSealBeforeEncode
	plan.CrashMode = crashHold

	child := startCrashChild(t, plan)
	child.await(t, lineHeld)
	child.await(t, lineReady)
	child.sigkill(t)

	// What the crash left, read out of the file: one sealed batch of fifty and
	// seven rows behind it carrying no batch key.
	rows := stagedRowsFor(t, cfg.StagingPath, plan.FillNamespace, plan.FillTable)
	if len(rows) != written {
		t.Fatalf("staging holds %d fills after the crash, want %d", len(rows), written)
	}
	storedKey, sealedPayloads := sealedBatch(t, rows)
	if len(sealedPayloads) != sealed {
		t.Fatalf("%d rows carry a batch key, want exactly the %d that were sealed", len(sealedPayloads), sealed)
	}
	tailPayloads := unsealedPayloads(rows)
	if len(tailPayloads) != unsealed {
		t.Fatalf("%d rows carry no batch key, want the %d staged behind the sealed batch", len(tailPayloads), unsealed)
	}
	if got := len(dataFiles(t, bucket, cfg.WarehousePrefix, plan.FillNamespace, plan.FillTable)); got != 0 {
		t.Fatalf("the crash left %d data files, want 0: it died before anything was encoded", got)
	}

	// The two shapes involved: the one the crashed run wrote under, recorded in
	// staging, and the one the next run declares.
	oldShape := stagedDescriptor(t, cfg.StagingPath, rows[0].schemaFP)
	newShape := declaredDescriptor[FillV2](t, plan.FillNamespace, plan.FillTable)
	if oldShape.Fingerprint() == newShape.Fingerprint() {
		t.Fatal("the two declarations describe the same shape; this variant needs them to differ")
	}

	// Recomputing the sealed batch's key under the new shape gives a different
	// answer — which is precisely why the stored key has to be authoritative
	// rather than derived again on replay.
	underNewShape, err := canon.BatchHash(plan.FillNamespace, plan.FillTable, newShape, sealedPayloads)
	if err != nil {
		t.Fatalf("recomputing the batch hash under the new shape: %v", err)
	}
	if underNewShape == storedKey {
		t.Fatal("the batch hashes to the same key under both shapes, so this variant proves nothing about the stored key")
	}

	// What the unsealed rows must hash to once they are sealed: their payloads
	// re-encoded under the new shape, in staged order. Computed here, by the
	// test, from the bytes the crashed process left behind.
	transcoded := make([][]byte, len(tailPayloads))
	for i, payload := range tailPayloads {
		if transcoded[i], err = canon.Transcode(plan.FillTable, oldShape, newShape, payload); err != nil {
			t.Fatalf("re-encoding staged payload %d under the new shape: %v", i, err)
		}
	}
	tailKey, err := canon.BatchHash(plan.FillNamespace, plan.FillTable, newShape, transcoded)
	if err != nil {
		t.Fatalf("hashing the re-encoded rows: %v", err)
	}

	// The next run, declaring one extra optional field.
	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillV2]{
		Namespace: plan.FillNamespace, Table: plan.FillTable,
	})
	if err != nil {
		t.Fatalf("OpenWriter under the changed declaration: %v", err)
	}
	if pending, _ := writer.Pending(); pending != written {
		t.Errorf("the recovered writer picked up %d records, want the %d left staged", pending, written)
	}

	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := writer.Flush(drain); err != nil {
		t.Fatalf("Flush on the recovery run: %v", err)
	}

	// Both recovered groups are committed by now, and each landed on the key it
	// had to land on: the sealed batch on the key stamped before the crash, the
	// re-batched tail on the hash of its own re-encoded bytes.
	recovered := baseNames(dataFiles(t, bucket, cfg.WarehousePrefix, plan.FillNamespace, plan.FillTable))
	if len(recovered) != 2 {
		t.Fatalf("the recovery run wrote %d data files, want 2 (the sealed batch and the rows behind it): %v", len(recovered), recovered)
	}
	if !slices.Contains(recovered, storedKey+".parquet") {
		t.Errorf("the sealed batch is not under its stored key %s; the files are %v", storedKey+".parquet", recovered)
	}
	if !slices.Contains(recovered, tailKey+".parquet") {
		t.Errorf("the rows staged behind it are not under %s, the hash of their re-encoding under the new shape; the files are %v",
			tailKey+".parquet", recovered)
	}

	// The fills table's staged rows are gone, and with them both of the shapes
	// they named — while the untouched second table's shape, which its own live
	// rows still name, is untouched. Descriptor cleanup is by reference, not by
	// emptiness.
	if got := len(stagedRowsFor(t, cfg.StagingPath, plan.FillNamespace, plan.FillTable)); got != 0 {
		t.Errorf("staging still holds %d rows for the recovered table, want 0", got)
	}
	fingerprints := stagedFingerprints(t, cfg.StagingPath)
	if slices.Contains(fingerprints, oldShape.Fingerprint()) {
		t.Errorf("staging_schemas still carries the shape the crashed run wrote under, which no live row names any more")
	}
	if got := len(stagedRowsFor(t, cfg.StagingPath, plan.EventNamespace, plan.EventTable)); got != witnesses {
		t.Errorf("the second table has %d staged rows, want the %d the crashed run left", got, witnesses)
	}
	if len(fingerprints) != 1 {
		t.Errorf("staging_schemas carries %d shapes, want only the one the second table's live rows name", len(fingerprints))
	}

	// The writer keeps working afterwards, under the new declaration, with the
	// new column actually carrying values.
	for i := written; i < written+added; i++ {
		if err := writer.Insert(ctx, makeFillV2(i)); err != nil {
			t.Fatalf("Insert %d after the recovery: %v", i, err)
		}
	}
	if err := writer.Close(drain); err != nil {
		t.Fatalf("Close on the recovery run: %v", err)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, plan.FillNamespace, plan.FillTable))

	var total, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+table, &total, &distinct)
	if total != written+added || distinct != written+added {
		t.Errorf("the table reads back %d rows with %d distinct ids, want %d of each exactly once",
			total, distinct, written+added)
	}

	// Which rows are null, not merely how many. The recovered records occupy
	// the id range the crashed run wrote and must all read back with no value
	// in the new column; the records written afterwards occupy the range beyond
	// it and must all carry one. A count-only assertion would be satisfied by a
	// table where one recovered row had somehow acquired a value and one new
	// row had lost it.
	var nulls, lowestNull, highestNull int
	duck.queryRow(t, "SELECT count(*), min(sequence_id), max(sequence_id) FROM "+table+" WHERE venue_order_type IS NULL",
		&nulls, &lowestNull, &highestNull)
	if nulls != written || lowestNull != 0 || highestNull != written-1 {
		t.Errorf("%d rows have no venue_order_type, over ids %d..%d; want the %d recovered from before the column existed, over ids 0..%d",
			nulls, lowestNull, highestNull, written, written-1)
	}

	var valued, lowestValued, highestValued int
	duck.queryRow(t, "SELECT count(*), min(sequence_id), max(sequence_id) FROM "+table+" WHERE venue_order_type IS NOT NULL",
		&valued, &lowestValued, &highestValued)
	if valued != added || lowestValued != written || highestValued != written+added-1 {
		t.Errorf("%d rows carry the new column's value, over ids %d..%d; want the %d written after the change, over ids %d..%d",
			valued, lowestValued, highestValued, added, written, written+added-1)
	}

	// And by value, one record from each of the three groups: the sealed batch
	// that was re-uploaded under its stored key, the rows that were re-encoded
	// under the new shape at seal time, and a row written after the change.
	// Every assertion above is about how many rows exist and where their ids
	// sit; none of them would notice a payload decoded wrongly on the way back
	// through replay, which is the one thing a schema change makes plausible.
	for _, spot := range []int{sealed / 2, sealed + unsealed/2} {
		expected := makeFill(spot)

		var orderID, price string
		var isNull bool
		duck.queryRow(t, fmt.Sprintf(`
			SELECT order_id, CAST(price AS VARCHAR), venue_order_type IS NULL
			FROM %s WHERE sequence_id = %d`, table, spot), &orderID, &price, &isNull)

		if orderID != expected.OrderID || price != decimalString(expected.Price, 9) || !isNull {
			t.Errorf("recovered record %d reads back as (%q, %s, null=%t), want (%q, %s, null=true)",
				spot, orderID, price, isNull, expected.OrderID, decimalString(expected.Price, 9))
		}
	}

	newest := written + added - 1
	expected := makeFillV2(newest)

	var orderID, kind string
	duck.queryRow(t, fmt.Sprintf("SELECT order_id, venue_order_type FROM %s WHERE sequence_id = %d", table, newest),
		&orderID, &kind)
	if orderID != expected.OrderID || kind != *expected.VenueOrderType {
		t.Errorf("record %d written after the change reads back as (%q, %q), want (%q, %q)",
			newest, orderID, kind, expected.OrderID, *expected.VenueOrderType)
	}

	// And the second table's rows, still staged through all of that, are
	// delivered to their own table when it is finally opened — the shared
	// staging file routed them nowhere else in the meantime.
	events, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{
		Namespace: plan.EventNamespace, Table: plan.EventTable,
	})
	if err != nil {
		t.Fatalf("OpenWriter for the second table: %v", err)
	}
	if err := events.Close(drain); err != nil {
		t.Fatalf("Close on the second table: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows once both tables have drained, want 0", got)
	}
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_ordinal) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, plan.EventNamespace, plan.EventTable)), &total, &distinct)
	if total != witnesses || distinct != witnesses {
		t.Errorf("the second table reads back %d rows with %d distinct ids, want %d of each", total, distinct, witnesses)
	}
}

// TestStagedRowsUnderAnUnknownFieldRefuseTheOpen is variant 3's fail-loudly
// half: staged rows whose recorded shape carries a field id the current
// declaration does not have at all.
//
// There is no honest way to replay those. The values in that column cannot be
// written to a table the declaration does not describe, and dropping them
// silently would mean losing data icelake had already told its caller was
// accepted. So the open is refused, with an error naming the field, and the
// rows stay exactly where they are — recoverable the moment a binary that
// declares the column runs again.
func TestStagedRowsUnderAnUnknownFieldRefuseTheOpen(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	cfg.FlushInterval = time.Hour

	const records = 5

	plan := crashPlan(cfg)
	plan.Script = scriptStream
	plan.Decl = declFillV2
	plan.FillTable = "fills_evolved"
	plan.Fills = records
	plan.Events = 0

	child := startCrashChild(t, plan)
	child.await(t, lineReady)
	child.sigkill(t)

	staged := stagedRowsFor(t, cfg.StagingPath, plan.FillNamespace, plan.FillTable)
	if len(staged) != records {
		t.Fatalf("staging holds %d rows after the crash, want %d", len(staged), records)
	}

	store := openStore(t, cfg)
	_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: plan.FillNamespace, Table: plan.FillTable,
	})

	var schemaErr icelake.StagingSchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("OpenWriter against rows staged under an unknown field returned %v (%T), want a StagingSchemaError", err, err)
	}
	if schemaErr.Kind != icelake.StagingSchemaKindFieldRemoved {
		t.Errorf("StagingSchemaError.Kind = %q, want %q", schemaErr.Kind, icelake.StagingSchemaKindFieldRemoved)
	}
	if schemaErr.FieldID != 9 {
		t.Errorf("StagingSchemaError.FieldID = %d, want the 9 the declaration no longer has", schemaErr.FieldID)
	}

	// Refused, not consumed: the records are still staged, so the values in the
	// unknown column were never at risk.
	if got := len(stagedRowsFor(t, cfg.StagingPath, plan.FillNamespace, plan.FillTable)); got != records {
		t.Fatalf("staging holds %d rows after the refusal, want the %d it was refused over", got, records)
	}

	// A binary that does declare the column picks them up and delivers them,
	// values intact.
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillV2]{
		Namespace: plan.FillNamespace, Table: plan.FillTable,
	})
	if err != nil {
		t.Fatalf("OpenWriter under the declaration that does have the field: %v", err)
	}
	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := writer.Close(drain); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after the corrected run drained, want 0", got)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, plan.FillNamespace, plan.FillTable))

	var total, valued int
	duck.queryRow(t, fmt.Sprintf(
		"SELECT count(*), count(*) FILTER (venue_order_type IS NOT NULL) FROM %s", table), &total, &valued)
	if total != records || valued != records {
		t.Errorf("the table reads back %d rows of which %d carry the column's value, want %d of each",
			total, valued, records)
	}
}

// declaredDescriptor derives the canonical shape a declaration describes, the
// same way the write path does, so a test can compute a batch key for itself
// instead of asking the code under test what it thinks the answer is.
func declaredDescriptor[T any](tb testing.TB, namespace, table string) *canon.Descriptor {
	tb.Helper()

	decl, err := schemamap.Declare[T](namespace, table)
	if err != nil {
		tb.Fatalf("deriving the declaration: %v", err)
	}
	d, err := canon.Describe(table, decl.IcebergSchema())
	if err != nil {
		tb.Fatalf("describing the declared shape: %v", err)
	}

	return d
}
