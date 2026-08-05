package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TestQuickstart runs the README quickstart — the same sequence [Example]
// carries, in the same order — against the real substrate, and asserts the
// record comes back.
//
// The two exist as a pair on purpose. The example is documentation and must
// compile everywhere, including on a machine with no container runtime, so it
// carries no expected output and is never run. This is what proves the sequence
// in it actually works: the same calls, with the endpoint and credentials
// pointed at a container instead of at R2, and a reader that has never heard of
// icelake checking the result.
func TestQuickstart(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()

	// The table's shape: one tagged struct. Field ids are permanent.
	type Fill struct {
		Symbol      string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
		Price       int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=2" arrow:"price"`
		VenueTimeMS int64  `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=3" arrow:"venue_timestamp_ms"`
	}

	dir := t.TempDir()
	store, err := icelake.Open(ctx, icelake.Config{
		StagingPath:       filepath.Join(dir, "staging.db"),
		CatalogPath:       filepath.Join(dir, "catalog.db"),
		Endpoint:          bucket.Endpoint,
		Bucket:            bucket.Bucket,
		WarehousePrefix:   "warehouse",
		Region:            bucket.Region,
		AccessKeyID:       bucket.AccessKeyID,
		SecretAccessKey:   bucket.SecretAccessKey,
		FlushMaxRecords:   50_000,
		FlushMaxBytes:     128 << 20,
		FlushInterval:     15 * time.Minute,
		ZSTDLevel:         1,
		StagingMaxRecords: 5_000_000,
		StagingMaxBytes:   4 << 30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := store.Close(ctx); err != nil {
			t.Errorf("Store.Close: %v", err)
		}
	}()

	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: "myservice",
		Table:     "fills",
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	venueTime := time.Now().UnixMilli()
	if err := fills.Insert(ctx, Fill{Symbol: "ABC", Price: 1_234_567_890, VenueTimeMS: venueTime}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if records, _ := fills.Pending(); records != 1 {
		t.Errorf("Pending() = %d records after one insert, want 1", records)
	}
	if err := fills.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if records, bytes := fills.Pending(); records != 0 || bytes != 0 {
		t.Errorf("Pending() = (%d, %d) after Flush, want (0, 0)", records, bytes)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, "warehouse", "myservice", "fills"))

	var symbol, price string
	var epochMS int64
	duck.queryRow(t, "SELECT symbol, CAST(price AS VARCHAR), epoch_ms(venue_timestamp_ms) FROM "+table,
		&symbol, &price, &epochMS)
	if symbol != "ABC" || price != "1.234567890" || epochMS != venueTime {
		t.Errorf("the quickstart's record read back as (%q, %s, %d), want (%q, %s, %d)",
			symbol, price, epochMS, "ABC", "1.234567890", venueTime)
	}
}
