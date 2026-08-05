package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TestVolumeRoundTripCaseA is TESTING.md's scenario 1 for Case A: pure
// structured facts written as many batches, then read back by DuckDB as one
// table.
//
// The claims it proves are the ones the whole design rests on. Every record
// written comes back exactly once, across many data files and many snapshots,
// through a reader that has never heard of icelake. A DECIMAL column round-trips
// *exactly*, asserted against the decimal string the value must be rather than
// against a tolerance — a tolerance would pass even if the column had silently
// become a float, which is the precise failure the type decision exists to
// prevent. And a TIMESTAMP column comes back as a real timestamp type at the
// right instant, rather than as an integer the reader has to reinterpret.
func TestVolumeRoundTripCaseA(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	const records = 500
	const perBatch = 50 // cfg.FlushMaxRecords
	const wantFiles = records / perBatch

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Many small batches, not one big one: the story under test is "collect
	// over time in many batches, query it all later as one table", and a
	// single-file table would not exercise it at all.
	if got := len(dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")); got != wantFiles {
		t.Errorf("data files = %d, want %d (one per %d-record batch)", got, wantFiles, perBatch)
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

	if got := duck.columnType(t, table, "price"); got != "DECIMAL(18,9)" {
		t.Errorf("price reads back as %s, want DECIMAL(18,9)", got)
	}
	if got := duck.columnType(t, table, "venue_timestamp_ms"); got != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("venue_timestamp_ms reads back as %s, want a real timestamp type", got)
	}

	// Spot checks: the first record, one from the middle of a later batch, and
	// the last one, so a batch boundary cannot hide a mistake.
	for _, i := range []int{0, 237, records - 1} {
		want := makeFill(i)

		var symbol, side, orderID, source, price, quantity string
		var epochMS int64
		duck.queryRow(t, fmt.Sprintf(`
			SELECT symbol, side, order_id, source,
			       CAST(price AS VARCHAR), CAST(quantity AS VARCHAR),
			       epoch_ms(venue_timestamp_ms)
			FROM %s WHERE sequence_id = %d`, table, i),
			&symbol, &side, &orderID, &source, &price, &quantity, &epochMS)

		if symbol != want.Symbol || side != want.Side || orderID != want.OrderID || source != want.Source {
			t.Errorf("record %d read back as (%q, %q, %q, %q), want (%q, %q, %q, %q)",
				i, symbol, side, orderID, source, want.Symbol, want.Side, want.OrderID, want.Source)
		}
		// The expected decimal strings are derived from the unscaled integers
		// the test itself wrote, not copied from what the code produced.
		if got, expect := price, decimalString(want.Price, 9); got != expect {
			t.Errorf("record %d price = %s, want exactly %s", i, got, expect)
		}
		if got, expect := quantity, decimalString(want.Quantity, 9); got != expect {
			t.Errorf("record %d quantity = %s, want exactly %s", i, got, expect)
		}
		if epochMS != want.VenueTimeMS {
			t.Errorf("record %d timestamp = %d ms, want the %d it was written with", i, epochMS, want.VenueTimeMS)
		}
	}
}

// Scenario 1's Case A also carries a schema-evolution clause — several batches
// committed under the original schema, the writer restarted against a
// declaration with one extra optional field, several more batches written, and
// the table read back as one table across the boundary with none of the earlier
// data files rewritten. It is proven by TestReconcileAddsAnOptionalColumn in
// reconcile_test.go, where it sits beside the other arms of the reconciliation
// loop it exercises, rather than being asserted a second time here: a second
// copy of a passing test is not a second claim.

// TestVolumeRoundTripCaseB is scenario 1 for Case B: the raw-payload archive.
//
// The claim is byte fidelity for an untyped blob column — the case that decides
// whether one Iceberg table can replace both halves of a "typed rows here,
// hand-rolled compressed archive file there" split. The payload is msgpack
// rather than JSON on purpose, so what is proven is format-agnostic byte
// fidelity rather than "JSON survived a JSON column".
func TestVolumeRoundTripCaseB(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)

	// The credential provider form, covered here rather than nowhere: it is a
	// supported input shape, and an untested one would rot.
	cfg.AccessKeyID, cfg.SecretAccessKey = "", ""
	cfg.Credentials = credentials.NewStaticCredentialsProvider(bucket.AccessKeyID, bucket.SecretAccessKey, "")

	store := openStore(t, cfg)

	const records = 300
	const perBatch = 50
	const wantFiles = records / perBatch

	// A per-table override, which is what FlushPolicy exists for: this table's
	// rows carry a payload each and are an order of magnitude larger than the
	// scalar table's, so a single byte threshold across both would be wrong for
	// one of them.
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{
		Namespace: "archive",
		Table:     "events",
		Flush: &icelake.FlushPolicy{
			FlushMaxRecords:  perBatch,
			FlushMaxBytes:    8 << 20,
			FlushInterval:    time.Minute,
			ZSTDLevel:        1,
			MinFlushInterval: time.Nanosecond,
		},
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	var rawBytes int
	for i := range records {
		event := makeEvent(i)
		rawBytes += len(event.Payload)
		if err := writer.Insert(ctx, event); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files := dataFiles(t, bucket, cfg.WarehousePrefix, "archive", "events")
	if len(files) != wantFiles {
		t.Errorf("data files = %d, want %d", len(files), wantFiles)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "archive", "events"))

	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", table), &total)
	if total != records {
		t.Errorf("row count = %d, want %d", total, records)
	}
	if got := duck.columnType(t, table, "observed_at_ms"); got != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("observed_at_ms reads back as %s, want a real timestamp type", got)
	}
	if got := duck.columnType(t, table, "payload"); got != "BLOB" {
		t.Errorf("payload reads back as %s, want BLOB", got)
	}

	for _, i := range []int{0, 149, records - 1} {
		want := makeEvent(i)

		var sourceID, kind, storedSum string
		var payload []byte
		var epochMS int64
		duck.queryRow(t, fmt.Sprintf(`
			SELECT source_id, event_kind, payload, payload_sha256, epoch_ms(observed_at_ms)
			FROM %s WHERE sequence_ordinal = %d`, table, i),
			&sourceID, &kind, &payload, &storedSum, &epochMS)

		if sourceID != want.SourceID || kind != want.EventKind || epochMS != want.ObservedAtMS {
			t.Errorf("record %d metadata read back as (%q, %q, %d), want (%q, %q, %d)",
				i, sourceID, kind, epochMS, want.SourceID, want.EventKind, want.ObservedAtMS)
		}
		// Two independent statements: the bytes are the bytes that went in, and
		// the hash column written alongside them still describes them. The
		// second is what would catch a payload silently swapped with another
		// row's.
		if !bytes.Equal(payload, want.Payload) {
			t.Errorf("record %d payload came back as % x, want % x", i, payload, want.Payload)
		}
		sum := sha256.Sum256(payload)
		if got := hex.EncodeToString(sum[:]); got != storedSum || got != want.PayloadSHA256 {
			t.Errorf("record %d payload hashes to %s; the stored column says %s and the record said %s",
				i, got, storedSum, want.PayloadSHA256)
		}
	}

	// A rough size comparison, not a benchmark gate: the point of absorbing the
	// archive into Parquet+ZSTD is that it stays competitive with the
	// hand-rolled compressed file it replaces, and a table several times larger
	// than its own raw payloads would say the opposite.
	var stored int64
	for _, f := range files {
		stored += f.size
	}
	// The comparison is deliberately generous: the Parquet side also carries
	// the five metadata columns and a footer per file, and these files are far
	// smaller than any production batch would be.
	t.Logf("case B: %d records, %d raw payload bytes, %d bytes of Parquet across %d files (%.2fx, metadata columns included)",
		records, rawBytes, stored, len(files), float64(stored)/float64(rawBytes))
	if stored > int64(rawBytes)*3 {
		t.Errorf("the archive table is %d bytes for %d bytes of payload, which is not competitive with the compressed file it replaces",
			stored, rawBytes)
	}
}

// TestTimeThresholdCommitsWithoutAnExplicitFlush proves the other half of
// scenario 1's "many batches over time": a batch that never reaches its size
// threshold is still committed, on its own, once the time threshold passes.
//
// This is the trigger the failure-handling design leans on — it is what
// guarantees a flush attempt arrives even if inserts stop entirely — so it is
// worth proving directly rather than inferring from a test that also calls
// Flush.
func TestTimeThresholdCommitsWithoutAnExplicitFlush(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	cfg.FlushInterval = 250 * time.Millisecond
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

	// Nothing else is called: no Flush, no Close. The only thing that can
	// commit these three records is the interval trigger.
	deadline := time.Now().Add(30 * time.Second)
	for len(dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the time threshold never committed the batch")
		}
		time.Sleep(25 * time.Millisecond)
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, "SELECT count(*) FROM "+duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total)
	if total != 3 {
		t.Errorf("row count = %d, want 3", total)
	}
}

// decimalString renders an unscaled integer at a scale, which is what a DECIMAL
// column declared on an int64 holds. It is written out here so the expected
// value in an assertion is derived independently rather than being whatever the
// code under test produced.
func decimalString(unscaled int64, scale int) string {
	sign := ""
	if unscaled < 0 {
		sign, unscaled = "-", -unscaled
	}
	digits := fmt.Sprintf("%0*d", scale+1, unscaled)
	split := len(digits) - scale

	return sign + digits[:split] + "." + digits[split:]
}
