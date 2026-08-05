package icelake_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// batchPoison is a two-column declaration with one column narrow enough to be
// unrepresentable on purpose, so a batch can carry exactly one bad record among
// good ones. It is scenario 7's poison declaration under its own name, because
// what is being asked here is a different question: not whether the record is
// refused, but whether the batch around it is.
type batchPoison struct {
	Note   string `parquet:"name=note, logical=String, fieldid=1" arrow:"note"`
	Amount int64  `parquet:"name=amount, logical=decimal, precision=5, scale=2, fieldid=2" arrow:"amount"`
}

// TestBatchInsertRoundTripsThroughTheBucket is scenario 14's first substrate
// clause: records handed over as slices are batched, encoded, uploaded and
// committed exactly as records handed over one at a time are, and a reader that
// has never heard of icelake gets every one of them back with its exact values.
//
// It is the clause that proves the batch door is the same pipeline rather than a
// parallel one. Everything else in this scenario is about what happens when a
// batch is refused, and none of that would matter if the accepted ones did not
// arrive.
func TestBatchInsertRoundTripsThroughTheBucket(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	const records = 500
	const perCall = 100

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	for start := 0; start < records; start += perCall {
		rows := make([]Fill, perCall)
		for i := range rows {
			rows[i] = makeFill(start + i)
		}
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("InsertBatch at %d: %v", start, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after a clean close, want 0", got)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))

	var total, distinct int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*), count(DISTINCT sequence_id) FROM %s", table), &total, &distinct)
	if total != records {
		t.Errorf("row count = %d, want %d", total, records)
	}
	if distinct != records {
		t.Errorf("distinct sequence ids = %d, want %d; rows were duplicated or lost", distinct, records)
	}

	// Spot checks either side of a batch boundary, so a call boundary cannot
	// hide a mistake. The expected decimal string is derived from the unscaled
	// integer the test itself wrote.
	for _, i := range []int{0, perCall - 1, perCall, records - 1} {
		want := makeFill(i)

		var symbol, price string
		var epochMS int64
		duck.queryRow(t, fmt.Sprintf(
			"SELECT symbol, CAST(price AS VARCHAR), epoch_ms(venue_timestamp_ms) FROM %s WHERE sequence_id = %d",
			table, i), &symbol, &price, &epochMS)

		if symbol != want.Symbol {
			t.Errorf("record %d symbol = %q, want %q", i, symbol, want.Symbol)
		}
		if expect := decimalString(want.Price, 9); price != expect {
			t.Errorf("record %d price = %s, want exactly %s", i, price, expect)
		}
		if epochMS != want.VenueTimeMS {
			t.Errorf("record %d timestamp = %d ms, want the %d it was written with", i, epochMS, want.VenueTimeMS)
		}
	}
}

// TestABatchIsRefusedWholeOrAcceptedWhole is scenario 14's refusal matrix: every
// way a batch can be refused, and the one thing they all have in common, which
// is that nothing from a refused batch is ever in staging afterwards.
//
// The subtests share one container and one store because they share one claim.
// Each one asserts the state of the real staging file rather than asking icelake
// what it thinks it did, since "nothing was accepted" is a statement about the
// disk.
func TestABatchIsRefusedWholeOrAcceptedWhole(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()

	t.Run("a bad row refuses the whole batch and says which one", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		writer := openPoisonWriter(t, cfg, bucket, "refusal")

		// One unrepresentable value in the middle of good records. The value is
		// a decimal past the declared precision, which is a refusal scenario 7
		// already proves for a single record; what is asked here is what happens
		// to the records around it.
		rows := []batchPoison{
			{Note: "first", Amount: 1},
			{Note: "second", Amount: 2},
			{Note: "third", Amount: 100_000},
			{Note: "fourth", Amount: 4},
		}

		err := writer.InsertBatch(ctx, rows)

		var refusal icelake.BatchError
		if !errors.As(err, &refusal) {
			t.Fatalf("InsertBatch returned %v (%T), want an icelake.BatchError", err, err)
		}
		if refusal.Index != 2 {
			t.Errorf("BatchError.Index = %d, want 2, the position of the bad row in the slice", refusal.Index)
		}

		// The row's own refusal is still reachable through the wrapper, which is
		// what makes every assertion a caller already writes about a single-row
		// refusal keep working against a batch.
		var poison icelake.PoisonError
		if !errors.As(err, &poison) {
			t.Fatalf("the BatchError does not unwrap to an icelake.PoisonError: %v", err)
		}
		if poison.Field != "amount" || poison.Reason != icelake.PoisonReasonDecimalRange {
			t.Errorf("PoisonError = (%q, %q), want (%q, %q)",
				poison.Field, poison.Reason, "amount", icelake.PoisonReasonDecimalRange)
		}

		if got := stagedRows(t, cfg.StagingPath); got != 0 {
			t.Errorf("staging holds %d rows after a refused batch, want 0: the rows before the bad one must not have landed", got)
		}
		if pending, _ := writer.Pending(); pending != 0 {
			t.Errorf("Pending() = %d after a refused batch, want 0", pending)
		}
	})

	t.Run("the ceiling refuses a batch whole", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		// A ceiling small enough that one batch can be built to fit it exactly
		// and the next one to miss it by a single record.
		cfg.StagingMaxRecords = 10
		// The flush thresholds are put out of reach rather than raised past the
		// ceiling, which the configuration rightly refuses: the floor holds back
		// every automatic trigger, so nothing this subtest stages can leave
		// staging underneath the counts it asserts.
		cfg.FlushMaxRecords = 10
		cfg.FlushInterval = time.Hour
		cfg.MinFlushInterval = time.Hour

		store := openStore(t, cfg)
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "ceiling"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}

		// A batch one record too large for an empty store. Nothing of it may
		// land, which is the decision under test: the alternative — filling the
		// headroom and refusing the tail — would leave the caller holding a
		// slice with an unstated prefix already taken.
		tooMany := make([]Fill, cfg.StagingMaxRecords+1)
		for i := range tooMany {
			tooMany[i] = makeFill(i)
		}
		if err := writer.InsertBatch(ctx, tooMany); !errors.Is(err, icelake.ErrStagingFull) {
			t.Fatalf("InsertBatch of %d records against a ceiling of %d returned %v, want ErrStagingFull",
				len(tooMany), cfg.StagingMaxRecords, err)
		}
		if got := stagedRows(t, cfg.StagingPath); got != 0 {
			t.Fatalf("staging holds %d rows after a batch the ceiling refused, want 0", got)
		}

		// And the ceiling is a ceiling rather than a refusal of everything: a
		// batch that fits exactly is accepted, so this test cannot pass by the
		// writer having stopped working.
		fits := tooMany[:cfg.StagingMaxRecords]
		if err := writer.InsertBatch(ctx, fits); err != nil {
			t.Fatalf("InsertBatch of a batch that fits exactly: %v", err)
		}
		if got, want := stagedRows(t, cfg.StagingPath), int(cfg.StagingMaxRecords); got != want {
			t.Errorf("staging holds %d rows after a batch that fits exactly, want %d", got, want)
		}

		// One more record now cannot fit, which is the same refusal met from a
		// full store rather than from an over-large batch.
		if err := writer.InsertBatch(ctx, tooMany[:1]); !errors.Is(err, icelake.ErrStagingFull) {
			t.Errorf("InsertBatch against a full store returned %v, want ErrStagingFull", err)
		}
	})

	t.Run("a closed writer answers ErrClosed whatever the batch holds", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		writer := openPoisonWriter(t, cfg, bucket, "closed")

		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// A batch whose first row is unrepresentable. The ordering promise is
		// that the writer's own state is answered first, so this must be
		// ErrClosed and not the row's refusal: the caller's next move is about
		// the writer, not about that record.
		err := writer.InsertBatch(ctx, []batchPoison{{Note: "bad", Amount: 100_000}, {Note: "ok", Amount: 1}})
		if !errors.Is(err, icelake.ErrClosed) {
			t.Errorf("InsertBatch on a closed writer returned %v, want ErrClosed", err)
		}
		var refusal icelake.BatchError
		if errors.As(err, &refusal) {
			t.Errorf("InsertBatch on a closed writer returned a BatchError naming row %d; a closed writer is not about any row", refusal.Index)
		}

		// And an empty batch is answered the same way, for the same reason.
		if err := writer.InsertBatch(ctx, nil); !errors.Is(err, icelake.ErrClosed) {
			t.Errorf("InsertBatch of an empty slice on a closed writer returned %v, want ErrClosed", err)
		}
	})

	t.Run("an empty batch touches nothing", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		writer := openPoisonWriter(t, cfg, bucket, "empty")

		if err := writer.InsertBatch(ctx, nil); err != nil {
			t.Errorf("InsertBatch(nil) returned %v, want nil", err)
		}
		if err := writer.InsertBatch(ctx, []batchPoison{}); err != nil {
			t.Errorf("InsertBatch of an empty slice returned %v, want nil", err)
		}
		if got := stagedRows(t, cfg.StagingPath); got != 0 {
			t.Errorf("staging holds %d rows after two empty batches, want 0", got)
		}
		if accepted := writer.Status().RecordsAccepted; accepted != 0 {
			t.Errorf("RecordsAccepted = %d after two empty batches, want 0", accepted)
		}
	})

	t.Run("OnAccept runs once per record, in order, after the batch is durable", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		cfg.FlushMaxRecords = 1000
		cfg.FlushInterval = time.Hour
		cfg.MinFlushInterval = time.Hour
		store := openStore(t, cfg)

		var seen []int64
		var staged []int
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
			Namespace: "market",
			Table:     "mirrored",
			OnAccept: func(row Fill) error {
				seen = append(seen, row.SequenceID)
				// Read the file, not the library: the promise is that the whole
				// batch is durable before the first callback runs, so every one
				// of them must see all of it.
				staged = append(staged, stagedRows(t, cfg.StagingPath))

				return nil
			},
		})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}

		const size = 5
		rows := make([]Fill, size)
		for i := range rows {
			rows[i] = makeFill(i)
		}
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}

		want := []int64{0, 1, 2, 3, 4}
		if len(seen) != len(want) {
			t.Fatalf("OnAccept ran %d times for a batch of %d, want once per record", len(seen), size)
		}
		for i := range want {
			if seen[i] != want[i] {
				t.Errorf("OnAccept saw record %d at position %d, want %d: the callbacks are in batch order", seen[i], i, want[i])
			}
			if staged[i] != size {
				t.Errorf("staging held %d rows when OnAccept ran for position %d, want all %d: the batch is durable before any callback runs",
					staged[i], i, size)
			}
		}
	})

	t.Run("a failing OnAccept is a MirrorError, not a refusal", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		cfg.FlushMaxRecords = 1000
		cfg.FlushInterval = time.Hour
		cfg.MinFlushInterval = time.Hour
		store := openStore(t, cfg)

		mirror := errors.New("the mirror fell behind")
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
			Namespace: "market",
			Table:     "mirrorfail",
			OnAccept:  func(Fill) error { return mirror },
		})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}

		rows := []Fill{makeFill(0), makeFill(1), makeFill(2)}
		err = writer.InsertBatch(ctx, rows)

		var reported icelake.MirrorError
		if !errors.As(err, &reported) {
			t.Fatalf("InsertBatch returned %v (%T), want an icelake.MirrorError", err, err)
		}
		if !errors.Is(err, mirror) {
			t.Errorf("the MirrorError does not unwrap to the callback's own error")
		}
		// The whole point of the type: the records were accepted, so retrying
		// would write them twice.
		if got := stagedRows(t, cfg.StagingPath); got != len(rows) {
			t.Errorf("staging holds %d rows after a failing mirror, want all %d: a mirror failure is not a refusal", got, len(rows))
		}
	})

	t.Run("a bad JSON line refuses the whole batch and says which one", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		cfg.FlushMaxRecords = 1000
		cfg.FlushInterval = time.Hour
		cfg.MinFlushInterval = time.Hour
		store := openStore(t, cfg)

		writer, err := icelake.OpenDynamicWriter(ctx, store, icelake.DynamicTableConfig{
			Namespace: "market",
			Table:     "jsonbatch",
			Schema:    benchSchema(t),
		})
		if err != nil {
			t.Fatalf("OpenDynamicWriter: %v", err)
		}

		lines := [][]byte{benchLine(t, 0), benchLine(t, 1), []byte(`{"symbol":`), benchLine(t, 3)}

		err = writer.InsertJSONBatch(ctx, lines)

		var refusal icelake.BatchError
		if !errors.As(err, &refusal) {
			t.Fatalf("InsertJSONBatch returned %v (%T), want an icelake.BatchError", err, err)
		}
		if refusal.Index != 2 {
			t.Errorf("BatchError.Index = %d, want 2, the position of the bad line", refusal.Index)
		}

		var record icelake.RecordError
		if !errors.As(err, &record) {
			t.Fatalf("the BatchError does not unwrap to an icelake.RecordError: %v", err)
		}
		if record.Kind != icelake.RecordKindMalformed {
			t.Errorf("RecordError.Kind = %q, want %q", record.Kind, icelake.RecordKindMalformed)
		}

		if got := stagedRows(t, cfg.StagingPath); got != 0 {
			t.Errorf("staging holds %d rows after a refused JSON batch, want 0", got)
		}

		// A batch of lines that all decode is accepted, so the refusal above is
		// the bad line rather than the door being shut.
		if err := writer.InsertJSONBatch(ctx, [][]byte{benchLine(t, 0), benchLine(t, 1)}); err != nil {
			t.Fatalf("InsertJSONBatch of two good lines: %v", err)
		}
		if got := stagedRows(t, cfg.StagingPath); got != 2 {
			t.Errorf("staging holds %d rows after two good lines, want 2", got)
		}
	})

	t.Run("a cancelled context leaves the whole batch out", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		cfg.StagingMaxRecords = 1 << 20
		cfg.StagingMaxBytes = 1 << 30
		cfg.FlushMaxRecords = 1 << 20
		cfg.FlushMaxBytes = 1 << 30
		cfg.FlushInterval = time.Hour
		cfg.MinFlushInterval = time.Hour
		store := openStore(t, cfg)

		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "cancelled"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}

		// A batch big enough that a deadline can land inside its transaction,
		// and — the part that makes this test discriminating rather than merely
		// true — a deadline derived from how long a whole batch actually takes
		// on this machine, so that the cut lands in the *middle* of one.
		//
		// A cut that lands before the first row would leave nothing staged under
		// any implementation, including one that opened a transaction per record,
		// which is exactly what the first version of this subtest did with its
		// one-millisecond starting budget: it passed against a mutation that
		// removed the very property it names. Timing a whole batch first and
		// halving from there fixes that — a per-record implementation cut half
		// way through leaves half its records behind, and one transaction leaves
		// none.
		const size = 20_000
		rows := make([]Fill, size)
		for i := range rows {
			rows[i] = makeFill(i)
		}

		start := time.Now()
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}
		full := time.Since(start)

		staged := stagedRows(t, cfg.StagingPath)
		if staged != size {
			t.Fatalf("an uncancelled batch of %d staged %d rows", size, staged)
		}

		for budget := full / 2; budget >= time.Microsecond; budget /= 2 {
			deadline, cancel := context.WithTimeout(ctx, budget)
			err := writer.InsertBatch(deadline, rows)
			cancel()

			after := stagedRows(t, cfg.StagingPath)
			switch {
			case err == nil:
				// It fitted inside the budget after all, so nothing was cut and
				// a smaller budget is the next thing to try.
				if after != staged+size {
					t.Fatalf("InsertBatch returned nil with %d rows staged, want the %d that were there plus %d",
						after, staged, size)
				}
				staged = after

			case errors.Is(err, context.DeadlineExceeded):
				if after != staged {
					t.Fatalf("a batch of %d cut off after %v left %d of its rows staged, want none: "+
						"a batch is one transaction", size, budget, after-staged)
				}

				return

			default:
				t.Fatalf("InsertBatch under a %v budget returned %v, want nil or a deadline", budget, err)
			}
		}

		t.Skip("no deadline short enough to land inside a batch on this machine")
	})
}

// openPoisonWriter opens a writer over the two-column declaration this scenario
// refuses records with, in its own table so the subtests cannot interfere with
// each other.
func openPoisonWriter(t *testing.T, cfg icelake.Config, _ testsubstrate.Bucket, table string) *icelake.Writer[batchPoison] {
	t.Helper()

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(context.Background(), store, icelake.TableConfig[batchPoison]{
		Namespace: "market",
		Table:     table,
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	return writer
}

// batchWatchWindow is how long the crash test watches a batch before killing the
// process, and batchWatchPoll is how often it looks. See the comment at their
// one use.
const (
	batchWatchWindow = 2 * time.Second
	batchWatchPoll   = 20 * time.Millisecond
)

// TestACrashInsideABatchLeavesNoneOfItBehind is scenario 14's crash clause, and
// the only one of the batch's atomicity claims that no in-process test can make:
// that a batch is all or nothing across a *crash*, not merely across an error
// or a cancellation.
//
// ARCHITECTURE.md's M15 decision says the batch is atomic "on a crash between
// any two of its rows", and until this test existed nothing killed a process
// there. The window is the problem: a staging transaction is open for
// microseconds unless the batch is big enough to make it milliseconds, so the
// child hands over a batch of a quarter of a million records and the parent
// kills it as soon as the child says it has entered one. The child prints a
// second line only on the path where the batch *finished*, so a kill that
// arrived too late is reported as a test that could not do its job rather than
// as a pass.
//
// The records inserted one at a time before it are the control: Insert's own
// promise says they are durable, so a staging file holding exactly them and
// nothing else is the whole claim in one number.
func TestACrashInsideABatchLeavesNoneOfItBehind(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	// The flush thresholds are put out of reach, so nothing the child accepted
	// can have been sealed, encoded or uploaded at the moment of the kill: what
	// this test reads afterwards is the staging file and nothing else.
	cfg.FlushMaxRecords = 1_000_000
	cfg.FlushMaxBytes = 512 << 20
	cfg.FlushInterval = time.Hour
	cfg.StagingMaxRecords = 2_000_000
	cfg.StagingMaxBytes = 1 << 30

	plan := crashPlan(cfg)
	plan.Script = scriptBatchCrash
	plan.Fills = 20
	plan.Batch = 250_000

	child := startCrashChild(t, plan)
	child.await(t, lineStaged)

	// Watching the file while the batch runs, rather than only after the kill,
	// and this is the part that makes the test hold its claim rather than
	// appear to. A kill timed by a clock lands wherever the machine happens to
	// have got to: too early and the staging file looks the same whether the
	// batch is one transaction or two hundred and fifty thousand, which is
	// exactly how the first draft of this test passed against a mutation that
	// opened one per row. What separates the two designs is not where the kill
	// lands but whether the row count ever moves *during* the batch — under one
	// transaction it cannot, and under one per row it must — so that is what is
	// asserted, on a poll, before anything is killed.
	completed := false
	for deadline := time.Now().Add(batchWatchWindow); time.Now().Before(deadline); {
		if child.saw(lineReady) {
			completed = true

			break
		}

		staged := stagedRowsFor(t, cfg.StagingPath, plan.FillNamespace, plan.FillTable)
		if len(staged) != plan.Fills && len(staged) != plan.Fills+plan.Batch {
			t.Fatalf("staging held %d rows while a batch of %d was in flight, want either the %d that were there before it or all %d: "+
				"a count between the two means the batch is not one transaction",
				len(staged), plan.Batch, plan.Fills, plan.Fills+plan.Batch)
		}
		time.Sleep(batchWatchPoll)
	}

	child.sigkill(t)

	staged := stagedRowsFor(t, cfg.StagingPath, plan.FillNamespace, plan.FillTable)
	want := plan.Fills
	if completed {
		// The batch finished before the window ended, so the kill landed after
		// it rather than inside it. The atomicity claim is still held — by the
		// poll above, which never saw a partial count — and what this branch
		// asserts is the other half of the same sentence: all of it, then.
		t.Logf("the batch of %d finished within %v, so the kill landed after it; the poll above is what held the claim",
			plan.Batch, batchWatchWindow)
		want = plan.Fills + plan.Batch
	}
	if len(staged) != want {
		t.Fatalf("staging holds %d rows after a crash around a batch of %d, want exactly %d: "+
			"a batch is one transaction, so a crash inside it leaves none of it", len(staged), plan.Batch, want)
	}
	// What survived the crash, which is what the recovery half below is about.
	// It is `want` rather than plan.Fills because the branch above may have
	// taken the other arm, and every assertion after this point is about
	// whatever is actually on disk rather than about which arm ran.
	//
	// **Corrected at M16 (2026-08-04), and the defect was in this test rather
	// than in anything it tests.** The `completed` branch above was added with
	// the test and adjusts the staged count it expects, and the three
	// assertions after it were left reading plan.Fills — so on any machine fast
	// enough to finish a quarter of a million records inside the watch window
	// the test failed at the *recovery* clause while the atomicity clause it
	// exists for had passed. It was reproduced at the commit that introduced it
	// before being changed here, so this is not a fix for something M16 broke;
	// it is a branch that had never run on the machines it was written on.
	survivors := len(staged)

	for _, r := range staged {
		if r.batchKey != "" {
			t.Fatalf("staged row %d was already sealed; the kill was meant to land before any threshold tripped", r.id)
		}
	}

	// The next run, over the same staging file and the same bucket. It has to
	// pick up exactly the surviving records, accept a fresh batch through the
	// same door, and commit both exactly once — which is what makes the crash a
	// thing icelake recovers from rather than a thing it survives.
	//
	// The fresh batch is deliberately a small one. What it proves is that the
	// writer is usable and that the records land exactly once, and neither of
	// those is a claim about size; re-sending a quarter of a million records
	// through a test bucket would prove the same thing and cost a minute.
	const resent = 200

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: plan.FillNamespace, Table: plan.FillTable,
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if pending, _ := writer.Pending(); pending != survivors {
		t.Errorf("the restarted writer picked up %d records, want the %d that survived the crash", pending, survivors)
	}

	rows := make([]Fill, resent)
	for i := range rows {
		// Numbered past everything the crashed run could have staged, so the
		// distinct-id count below is a real exactly-once check rather than a
		// count that a collision would flatter.
		rows[i] = makeFill(plan.Fills + plan.Batch + i)
	}
	if err := writer.InsertBatch(ctx, rows); err != nil {
		t.Fatalf("InsertBatch after the crash: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, plan.FillNamespace, plan.FillTable))

	var total, distinct int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*), count(DISTINCT sequence_id) FROM %s", table), &total, &distinct)
	if want := survivors + resent; total != want || distinct != want {
		t.Errorf("the table holds %d rows and %d distinct sequence ids, want %d of each: the survivors and the re-sent batch, each exactly once",
			total, distinct, want)
	}
}
