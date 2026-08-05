package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// Fill is Case A: pure structured facts, fully typed and scalar, immutable once
// written. It is the declaration struct SCHEMA.md carries as its worked
// example, tags included, rather than a second copy that could drift from it.
type Fill struct {
	Symbol      string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side        string `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price       int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity    int64  `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID  int64  `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID     string `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source      string `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
	VenueTimeMS int64  `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
}

// EventArchive is Case B: structured metadata columns plus a raw payload column
// carrying the full original event body. It is the case that proves one Iceberg
// table can absorb what otherwise needs a hand-rolled compressed archive file
// next to a database.
type EventArchive struct {
	SourceID        string `parquet:"name=source_id, logical=String, fieldid=1" arrow:"source_id"`
	EventKind       string `parquet:"name=event_kind, logical=String, fieldid=2" arrow:"event_kind"`
	SequenceOrdinal int64  `parquet:"name=sequence_ordinal, fieldid=3" arrow:"sequence_ordinal"`
	ObservedAtMS    int64  `parquet:"name=observed_at_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=4" arrow:"observed_at_ms"`
	Payload         []byte `parquet:"name=payload, type=BYTE_ARRAY, fieldid=5" arrow:"payload"`
	PayloadSHA256   string `parquet:"name=payload_sha256, logical=String, fieldid=6" arrow:"payload_sha256"`
}

// testConfig builds the configuration every storage-facing test runs with.
//
// The thresholds are deliberately small, per TESTING.md's configuration
// section: a test writing a modest number of records must produce several
// Parquet files and several snapshots, because the claim under test is "collect
// over time in many small batches, query it all later as one table" and a
// single-file table would not exercise it. The compression level is the low,
// fast one for the same reason — tests should be quick, not representative of
// production ratios.
//
// The flush floor is configured out of the way, at a nanosecond. Every scenario
// but the one that exists to prove the floor wants the configured thresholds to
// be the binding constraint, and the one-second default would coalesce the very
// batches these tests are counting into an unpredictable number of larger ones.
// The interval is long for the same reason: a test that asserts an exact file
// count must not race a timer, so the tests that want the time threshold set it
// themselves.
//
// The retry policy is the short one TESTING.md's configuration section
// prescribes — two attempts, a base delay in the tens of milliseconds — so that
// a test which forces a flush failure spends milliseconds backing off rather
// than the half-minute the production defaults would take. It is the one place
// the suite overrides a default rather than a production-shaped value, and
// being able to do so at all is only possible because the retry policy is
// configuration rather than a constant.
func testConfig(tb testing.TB, bucket testsubstrate.Bucket) icelake.Config {
	tb.Helper()

	dir := tb.TempDir()

	return icelake.Config{
		StagingPath:       filepath.Join(dir, "staging.db"),
		CatalogPath:       filepath.Join(dir, "catalog.db"),
		Endpoint:          bucket.Endpoint,
		Bucket:            bucket.Bucket,
		WarehousePrefix:   "warehouse",
		Region:            bucket.Region,
		AccessKeyID:       bucket.AccessKeyID,
		SecretAccessKey:   bucket.SecretAccessKey,
		FlushMaxRecords:   50,
		FlushMaxBytes:     1 << 20,
		FlushInterval:     time.Minute,
		MinFlushInterval:  time.Nanosecond,
		ZSTDLevel:         1,
		StagingMaxRecords: 100_000,
		StagingMaxBytes:   256 << 20,
		FlushMaxAttempts:  2,
		RetryBaseDelay:    20 * time.Millisecond,
		RetryMaxDelay:     100 * time.Millisecond,
	}
}

// openStore opens a store against a fresh temporary directory and closes it
// when the test ends, so no test has to remember either.
func openStore(tb testing.TB, cfg icelake.Config) *icelake.Store {
	tb.Helper()

	store, err := icelake.Open(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := store.Close(ctx); err != nil && !isClosed(err) {
			tb.Errorf("Store.Close: %v", err)
		}
	})

	return store
}

// isClosed reports whether an error is the already-closed sentinel, which a
// cleanup that follows an explicit Close in the test body must tolerate.
func isClosed(err error) bool { return errors.Is(err, icelake.ErrClosed) }

// makeFill builds one deterministic record, so a test can assert on an exact
// value it can also compute for itself.
//
// Price and quantity are unscaled integers at scale 9, which is what a DECIMAL
// column declared on an int64 holds: the price below is 1.234567890 plus the
// row's own index, and the decimal string a reader must give back is derived
// from the same number rather than copied from what the code produced.
func makeFill(i int) Fill {
	return Fill{
		Symbol:      fmt.Sprintf("SYM%d", i%7),
		Side:        []string{"buy", "sell"}[i%2],
		Price:       1_234_567_890 + int64(i),
		Quantity:    2_000_000_000 + int64(i),
		SequenceID:  int64(i),
		OrderID:     fmt.Sprintf("order-%06d", i),
		Source:      fmt.Sprintf("venue-%d", i%3),
		VenueTimeMS: 1_700_000_000_000 + int64(i)*1000,
	}
}

// makeEvent builds one deterministic archive record with a msgpack-shaped
// payload.
//
// The payload is deliberately not JSON: the claim under test is byte fidelity
// for an untyped blob column, and filling it with JSON would risk proving only
// that JSON survives. The bytes below are a real msgpack map — 0x83 is
// "fixmap of 3" — carrying a per-record body so no two rows share a payload.
func makeEvent(i int) EventArchive {
	payload := msgpackEvent(i)
	sum := sha256.Sum256(payload)

	return EventArchive{
		SourceID:        fmt.Sprintf("stream-%d", i%5),
		EventKind:       []string{"created", "edited", "deleted"}[i%3],
		SequenceOrdinal: int64(i),
		ObservedAtMS:    1_700_000_000_000 + int64(i)*1000,
		Payload:         payload,
		PayloadSHA256:   hex.EncodeToString(sum[:]),
	}
}

// msgpackEvent hand-builds a small msgpack map: {"i": <i>, "k": <kind>,
// "b": <blob>}. It is written by hand rather than with an encoder so the test
// depends on no serialization library of its own.
func msgpackEvent(i int) []byte {
	out := []byte{0x83} // fixmap, 3 entries

	out = append(out, 0xa1, 'i') // fixstr "i"
	out = append(out, 0xd2,      // int32
		byte(i>>24), byte(i>>16), byte(i>>8), byte(i))

	out = append(out, 0xa1, 'k') // fixstr "k"
	kind := []string{"created", "edited", "deleted"}[i%3]
	out = append(out, byte(0xa0|len(kind)))
	out = append(out, kind...)

	out = append(out, 0xa1, 'b') // fixstr "b"
	body := make([]byte, 64)
	for j := range body {
		body[j] = byte((i * 31) + j)
	}
	out = append(out, 0xc4, byte(len(body))) // bin8
	out = append(out, body...)

	return out
}
