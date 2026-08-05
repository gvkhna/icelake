package e2e

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/gvkhna/icelake"
)

// The end-to-end half of the benchmarks TESTING.md's scenario 14 describes,
// plus the one figure in that scenario that is asserted rather than printed.
//
// The benchmarks assert nothing and are in no gate: a testing.B does not run
// under `go test` without `-bench`, so `mise run check` is exactly as fast as it
// was. `mise run bench` runs these together with the staging package's, and the
// section of TESTING.md named "How the benchmarks are run and read" says what
// the five figures mean beside each other.
//
// They run in local-only mode, which is not a stand-in for anything here: what
// they measure is the cost of accepting a record, which ends at the staging
// transaction, and the flush thresholds below are set so far out of reach that
// no flush happens inside a measured run at all. Nothing in them touches a
// bucket, and nothing in them claims anything about one.

// benchBatch is the batch size the end-to-end benchmarks insert with. It matches
// the staging package's, so the two sets of figures can be read against each
// other, and it is the size ARCHITECTURE.md's measured table calls the point at
// which the curve has stopped being steep.
const benchBatch = 1000

// minBatchSpeedup is what TestBatchedIngestStaysManyRecordsPerTransaction
// requires: how many times cheaper one record must be through the batch door
// than through the single-record door, measured on the same machine, with the
// same records, in the same run.
//
// A ratio rather than a rate, because a rate is a property of the disk and would
// have to be recalibrated for every machine it ever ran on. Reinstating a
// transaction per record makes the two doors the same code doing the same work,
// so the ratio falls to about one wherever it is run — measured at 0.9 to 1.0 on
// an ordinary disk and 0.4 to 1.3 on a tmpfs, by mutating the staging store to
// open a transaction per row and running it — which is what makes this
// detectable without calibration.
//
// Two is chosen against the least favourable machine rather than the comfortable
// one. On an ordinary disk the measured figure is in the tens with the race
// detector on and the hundreds without it, because a sync dominates everything.
// On a filesystem whose sync costs nothing at all — a tmpfs, or a disk that does
// not honour barriers — the advantage shrinks to what a transaction costs apart
// from its sync, and there the measured floor over thirty consecutive runs was
// just above four. So the bound sits below that floor and above everything the
// mutation produced, which is the widest gap available in both directions at
// once.
const minBatchSpeedup = 2

// TestBatchedIngestStaysManyRecordsPerTransaction is scenario 14's regression
// tripwire: the assertion that the batch door still amortizes the staging fsync
// across a batch instead of paying one per record.
//
// It is a test rather than a benchmark on purpose. What it guards is not a speed
// but a shape — ARCHITECTURE.md's M15 decision that the batching layer lives
// inside the library — and the shape has a consequence that can be checked
// without tuning anything: one record is many times cheaper through the batch
// door than through the single-record door.
func TestBatchedIngestStaysManyRecordsPerTransaction(t *testing.T) {
	dir := t.TempDir()

	syncPeriod := measureSyncPeriod(t, dir)
	perRecord, perBatched, speedup := measureBothDoors(t, dir)

	// The sync figure is reported and not asserted on. It is what makes the
	// other two readable — on a disk that syncs in milliseconds the speedup is
	// the fsync being amortized, and on one that syncs in microseconds it is
	// what a transaction costs apart from its sync — and neither reading changes
	// what the ratio has to be for the library to be doing what it says.
	t.Logf("one sync %v, one record %v, one record batched %v, %.1f times cheaper batched",
		syncPeriod, perRecord, perBatched, speedup)

	if speedup < minBatchSpeedup {
		t.Errorf("the batch door is only %.1f times cheaper per record than the single-record door "+
			"(%v against %v, on a machine where a sync costs %v), want at least %d: "+
			"a figure near one means every record is paying for its own transaction again",
			speedup, perBatched, perRecord, syncPeriod, minBatchSpeedup)
	}
}

// measureSyncPeriod measures what one disk sync costs in the directory the
// staging file will live in, by doing the one thing a staging commit ultimately
// costs: a small write and a sync.
//
// It is bounded from both ends. A fast disk stops at a fixed number of syncs so
// the test stays quick; a slow one stops at a time budget so it stays bounded,
// with a floor on the count so the figure is never taken from two or three
// samples.
func measureSyncPeriod(t *testing.T, dir string) time.Duration {
	t.Helper()

	const (
		maxSyncs = 200
		minSyncs = 20
		budget   = 250 * time.Millisecond
	)

	f, err := os.Create(filepath.Join(dir, "sync-probe.bin"))
	if err != nil {
		t.Fatalf("creating the probe file: %v", err)
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, 256)
	start := time.Now()
	count := 0
	for count < maxSyncs {
		if _, err := f.Write(buf); err != nil {
			t.Fatalf("writing the probe file: %v", err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("syncing the probe file: %v", err)
		}
		count++
		if count >= minSyncs && time.Since(start) >= budget {
			break
		}
	}

	return time.Since(start) / time.Duration(count)
}

// measureBothDoors measures what one record costs through [icelake.Writer.Insert]
// and through [icelake.Writer.InsertBatch], and returns the median of several
// rounds of each.
//
// Three things about how it measures are the difference between a figure worth
// asserting on and one that is not, and all three were forced by watching the
// first version of this test wobble.
//
// The rounds are *interleaved*: one round of each door, then the next. That is
// what lets the ratio be taken per round and the median taken of the ratios,
// rather than the two medians being divided — a machine that becomes busy for
// one round inflates both of that round's figures, so their ratio survives it
// where either figure alone does not.
//
// A median rather than a mean, because what this has to survive is the
// occasional round that took three times as long for reasons outside the
// process, and a mean carries that outlier where a median discards it. And every
// record is built before any clock starts, because timing fmt.Sprintf alongside
// the library would make the ratio move with the cost of the fixture.
//
// The single-record door is given many records per round for a reason worth
// naming, since it is what made the first version of this test wobble: on a
// filesystem whose sync is nearly free, one Insert costs tens of microseconds
// and SQLite's automatic WAL checkpoint lands inside one round out of several,
// tripling that round's figure. Many records per round and a median across
// rounds are what turn that into an outlier instead of the answer.
//
// The two durations come back alongside the ratio only so the failure message
// and the log line can report what was actually measured; the ratio is not their
// quotient.
func measureBothDoors(t *testing.T, dir string) (perRecord, perBatched time.Duration, speedup float64) {
	t.Helper()

	const (
		rounds        = 5
		singleRecords = 60
		batchSize     = 2048
	)

	ctx := context.Background()
	single := ingestWriter(t, filepath.Join(dir, "single"))
	batched := ingestWriter(t, filepath.Join(dir, "batched"))

	singleRows := make([][]Fill, rounds)
	batchRows := make([][]Fill, rounds)
	for r := range rounds {
		singleRows[r] = make([]Fill, singleRecords)
		for i := range singleRows[r] {
			singleRows[r][i] = makeFill(r*singleRecords + i)
		}
		batchRows[r] = make([]Fill, batchSize)
		for i := range batchRows[r] {
			batchRows[r][i] = makeFill(r*batchSize + i)
		}
	}
	warmup := make([]Fill, 64)
	for i := range warmup {
		warmup[i] = makeFill(i)
	}

	// A little of each door before any clock starts, so neither figure is
	// dominated by whatever the first transaction against a brand-new file has
	// to do.
	if err := single.Insert(ctx, warmup[0]); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := batched.InsertBatch(ctx, warmup); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	singles := make([]time.Duration, rounds)
	batches := make([]time.Duration, rounds)
	for r := range rounds {
		start := time.Now()
		for _, row := range singleRows[r] {
			if err := single.Insert(ctx, row); err != nil {
				t.Fatalf("Insert: %v", err)
			}
		}
		singles[r] = time.Since(start) / singleRecords

		start = time.Now()
		if err := batched.InsertBatch(ctx, batchRows[r]); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}
		batches[r] = time.Since(start) / batchSize
	}

	ratios := make([]float64, rounds)
	for r := range ratios {
		ratios[r] = float64(singles[r]) / float64(batches[r])
	}

	slices.Sort(singles)
	slices.Sort(batches)
	slices.Sort(ratios)

	return singles[rounds/2], batches[rounds/2], ratios[rounds/2]
}

// ingestWriter opens a writer over its own staging file, with the thresholds set
// so that nothing flushes while a measurement is running: what is being measured
// is acceptance, and a Parquet encode underneath it would be measuring something
// else.
func ingestWriter(t *testing.T, dir string) *icelake.Writer[Fill] {
	t.Helper()

	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("creating the store directory: %v", err)
	}
	store := openStore(t, benchConfig(dir))
	w, err := icelake.OpenWriter(context.Background(), store, icelake.TableConfig[Fill]{Namespace: "bench", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	return w
}

// BenchmarkWriterInsertBatch is the batch door end to end through the public
// API: bind, poison check, canonical encoding and the staging transaction, for a
// typed declaration struct. The ns/op it reports is per record.
func BenchmarkWriterInsertBatch(b *testing.B) {
	ctx := context.Background()
	store := openStore(b, benchConfig(b.TempDir()))
	w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "bench", Table: "fills"})
	if err != nil {
		b.Fatalf("OpenWriter: %v", err)
	}
	b.Cleanup(func() { _ = w.Close(context.Background()) })

	rows := make([]Fill, benchBatch)
	for i := range rows {
		rows[i] = makeFill(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += benchBatch {
		n := min(benchBatch, b.N-i)
		if err := w.InsertBatch(ctx, rows[:n]); err != nil {
			b.Fatalf("InsertBatch: %v", err)
		}
	}
}

// BenchmarkDynamicInsertJSONBatch is the same measurement through the door the
// icelake command uses: the per-line JSON decode and the runtime-schema codec on
// top of everything the benchmark above measures. The gap between the two is
// what a caller pays for declaring its table at run time and handing over bytes
// instead of values.
func BenchmarkDynamicInsertJSONBatch(b *testing.B) {
	ctx := context.Background()
	store := openStore(b, benchConfig(b.TempDir()))
	w, err := icelake.OpenDynamicWriter(ctx, store, icelake.DynamicTableConfig{
		Namespace: "bench",
		Table:     "fills",
		Schema:    benchSchema(b),
	})
	if err != nil {
		b.Fatalf("OpenDynamicWriter: %v", err)
	}
	b.Cleanup(func() { _ = w.Close(context.Background()) })

	lines := make([][]byte, benchBatch)
	for i := range lines {
		lines[i] = benchLine(b, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += benchBatch {
		n := min(benchBatch, b.N-i)
		if err := w.InsertJSONBatch(ctx, lines[:n]); err != nil {
			b.Fatalf("InsertJSONBatch: %v", err)
		}
	}
}

// BenchmarkDynamicInsertCBORBatch is BenchmarkDynamicInsertJSONBatch's twin in
// the other encoding: the same eight columns, the same records, the same batch
// size, through the door a record written as CBOR goes in by.
//
// The two exist beside each other because the gap between them is the whole of
// what a second encoding buys, and it is a decode cost and nothing else: every
// layer under the decode — bind, poison check, canonical encoding, the staging
// transaction — is the same objects doing the same work, which is the property
// TestTheTwoFormatsMeanExactlyOneThing asserts on the bytes. So the difference
// between these two ns/op figures is the saving, and it is worth measuring
// rather than assuming, because the record is only a decode away from staging
// and a disk sync is a large thing to be measuring around.
func BenchmarkDynamicInsertCBORBatch(b *testing.B) {
	ctx := context.Background()
	store := openStore(b, benchConfig(b.TempDir()))
	w, err := icelake.OpenDynamicWriter(ctx, store, icelake.DynamicTableConfig{
		Namespace: "bench",
		Table:     "fills",
		Schema:    benchSchema(b),
	})
	if err != nil {
		b.Fatalf("OpenDynamicWriter: %v", err)
	}
	b.Cleanup(func() { _ = w.Close(context.Background()) })

	items := make([][]byte, benchBatch)
	for i := range items {
		items[i] = benchCBORItem(b, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += benchBatch {
		n := min(benchBatch, b.N-i)
		if err := w.InsertCBORBatch(ctx, items[:n]); err != nil {
			b.Fatalf("InsertCBORBatch: %v", err)
		}
	}
}

// BenchmarkDynamicDecodeJSONBatch and BenchmarkDynamicDecodeCBORBatch measure
// the two doors' decode and nothing else, which is the only granularity at
// which the difference between the formats is visible at all: the two
// benchmarks above put the decode next to a staging transaction, and a staging
// transaction is between one and two orders of magnitude larger than either
// decode, so their figures overlap and say nothing.
//
// The technique is the two doors' own documented ordering, used rather than
// worked around: both decode every item *before* the writer's lifecycle check,
// so a closed writer runs the whole decode and then answers [icelake.ErrClosed]
// without touching staging. That makes a public-API benchmark of a private step
// possible with no seam added for it, and it fails loudly if that ordering ever
// changes, because the error would stop being ErrClosed.
func BenchmarkDynamicDecodeJSONBatch(b *testing.B) {
	w := closedBenchWriter(b)
	lines := make([][]byte, benchBatch)
	for i := range lines {
		lines[i] = benchLine(b, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += benchBatch {
		n := min(benchBatch, b.N-i)
		if err := w.InsertJSONBatch(context.Background(), lines[:n]); !isClosed(err) {
			b.Fatalf("InsertJSONBatch on a closed writer returned %v, want ErrClosed", err)
		}
	}
}

func BenchmarkDynamicDecodeCBORBatch(b *testing.B) {
	w := closedBenchWriter(b)
	items := make([][]byte, benchBatch)
	for i := range items {
		items[i] = benchCBORItem(b, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += benchBatch {
		n := min(benchBatch, b.N-i)
		if err := w.InsertCBORBatch(context.Background(), items[:n]); !isClosed(err) {
			b.Fatalf("InsertCBORBatch on a closed writer returned %v, want ErrClosed", err)
		}
	}
}

// closedBenchWriter opens the eight-column table the benchmarks share and closes
// it, so that what the two decode benchmarks measure is the decode.
func closedBenchWriter(b *testing.B) *icelake.DynamicWriter {
	b.Helper()

	ctx := context.Background()
	store := openStore(b, benchConfig(b.TempDir()))
	w, err := icelake.OpenDynamicWriter(ctx, store, icelake.DynamicTableConfig{
		Namespace: "bench",
		Table:     "fills",
		Schema:    benchSchema(b),
	})
	if err != nil {
		b.Fatalf("OpenDynamicWriter: %v", err)
	}
	if err := w.Close(ctx); err != nil {
		b.Fatalf("Close: %v", err)
	}

	return w
}

// benchCBORItem renders the same [makeFill] record as one CBOR data item, in
// the spellings the format's own column of SCHEMA.md's coercion table gives: a
// decimal as its digits in a text string, a timestamp as an integer, and every
// other column as the CBOR type its column takes directly.
//
// It is deliberately the same record benchLine renders and not a
// CBOR-flattering one. The point of the pair is a difference that is only the
// decode, so anything that made one door's fixture cheaper than the other's
// would turn the figure into a fact about this file.
func benchCBORItem(tb testing.TB, i int) []byte {
	tb.Helper()

	row := makeFill(i)
	item, err := cbor.Marshal(map[string]any{
		"symbol":             row.Symbol,
		"side":               row.Side,
		"price":              decimalString(row.Price, 9),
		"quantity":           decimalString(row.Quantity, 9),
		"sequence_id":        row.SequenceID,
		"order_id":           row.OrderID,
		"source":             row.Source,
		"venue_timestamp_ms": row.VenueTimeMS,
	})
	if err != nil {
		tb.Fatalf("marshalling a CBOR item: %v", err)
	}

	return item
}

// benchConfig is a local-only store whose flush thresholds are far out of reach,
// so that nothing but acceptance happens while a measurement is running, and
// whose staging ceiling is high enough that a long benchmark is not stopped by
// it.
func benchConfig(dir string) icelake.Config {
	return icelake.Config{
		StagingPath:       filepath.Join(dir, "staging.db"),
		CacheDir:          filepath.Join(dir, "cache"),
		LocalOnly:         true,
		FlushMaxRecords:   1 << 30,
		FlushMaxBytes:     1 << 40,
		FlushInterval:     time.Hour,
		MinFlushInterval:  time.Hour,
		ZSTDLevel:         1,
		StagingMaxRecords: 1 << 30,
		StagingMaxBytes:   1 << 40,
	}
}

// benchSchema declares exactly the columns [Fill] declares, with the same field
// ids and the same types, as a runtime schema document would.
//
// Being the *same shape* is what makes BenchmarkWriterInsertBatch and
// BenchmarkDynamicInsertJSONBatch comparable, and comparability is the only
// reason the second one exists: the gap between them is meant to be the cost of
// declaring a table at run time and handing over bytes instead of values, and it
// is only that if everything else about the two tables is identical. An earlier
// version of this declared five columns against the struct's eight, and the door
// doing strictly more work measured faster — a figure that read as a finding and
// was an artefact of the fixture.
func benchSchema(tb testing.TB) *icelake.TableSchema {
	tb.Helper()

	schema, err := icelake.NewTableSchema("fills", []icelake.FieldSpec{
		{Name: "symbol", Type: "string", FieldID: 1},
		{Name: "side", Type: "string", FieldID: 2},
		{Name: "price", Type: "decimal(18,9)", FieldID: 3},
		{Name: "quantity", Type: "decimal(18,9)", FieldID: 4},
		{Name: "sequence_id", Type: "long", FieldID: 5},
		{Name: "order_id", Type: "string", FieldID: 6},
		{Name: "source", Type: "string", FieldID: 7},
		{Name: "venue_timestamp_ms", Type: "timestamptz", FieldID: 8},
	})
	if err != nil {
		tb.Fatalf("NewTableSchema: %v", err)
	}

	return schema
}

// benchLine renders one [makeFill] record as a line of JSON for the schema above
// — the same record the struct benchmark inserts, in the spelling an operator
// would actually pipe: a decimal as its digits, a timestamp as epoch
// milliseconds.
func benchLine(tb testing.TB, i int) []byte {
	tb.Helper()

	row := makeFill(i)
	line, err := json.Marshal(map[string]any{
		"symbol":             row.Symbol,
		"side":               row.Side,
		"price":              json.RawMessage(decimalString(row.Price, 9)),
		"quantity":           json.RawMessage(decimalString(row.Quantity, 9)),
		"sequence_id":        row.SequenceID,
		"order_id":           row.OrderID,
		"source":             row.Source,
		"venue_timestamp_ms": row.VenueTimeMS,
	})
	if err != nil {
		tb.Fatalf("marshalling a line: %v", err)
	}

	return line
}
