package icelake_test

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gvkhna/icelake"
)

// Example is the README quickstart, kept honest two ways.
//
// It carries no "// Output:" comment, so the testing package compiles it and
// does not run it — which is the right trade for the one piece of documentation
// that has to be correct on every machine, including one with no container
// runtime to run it against. What proves it actually works is
// TestQuickstart in quickstart_test.go, which runs this same sequence against
// the real substrate and asserts the records come back.
func Example() {
	ctx := context.Background()

	// The table's shape: one tagged struct. Field ids are permanent.
	type Fill struct {
		Symbol      string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
		Price       int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=2" arrow:"price"`
		VenueTimeMS int64  `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=3" arrow:"venue_timestamp_ms"`
	}

	store, err := icelake.Open(ctx, icelake.Config{
		StagingPath:       "/var/lib/myservice/staging.db",
		CatalogPath:       "/var/lib/myservice/catalog.db",
		Endpoint:          "https://ACCOUNT.r2.cloudflarestorage.com",
		Bucket:            "my-bucket",
		WarehousePrefix:   "warehouse",
		AccessKeyID:       os.Getenv("R2_ACCESS_KEY_ID"),
		SecretAccessKey:   os.Getenv("R2_SECRET_ACCESS_KEY"),
		FlushMaxRecords:   50_000,
		FlushMaxBytes:     128 << 20, // 128 MiB
		FlushInterval:     15 * time.Minute,
		ZSTDLevel:         19, // use 1 in dev and tests
		StagingMaxRecords: 5_000_000,
		StagingMaxBytes:   4 << 30, // 4 GiB — also bounds memory

		// Optional: where a flush that used up its retry budget is reported.
		// It runs on a background goroutine — log it, count it, page someone,
		// and return promptly. Nothing is lost when it fires: the batch stays
		// staged and the next flush trigger tries again.
		OnFlushError: func(e icelake.FlushError) { log.Printf("icelake: %v", e) },
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := store.Close(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: "myservice",
		Table:     "fills",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Insert returns once the record is durably staged locally.
	// Batching, upload, and Iceberg commit happen in the background.
	err = fills.Insert(ctx, Fill{
		Symbol:      "ABC",
		Price:       1_234_567_890, // 1.234567890 at scale 9
		VenueTimeMS: time.Now().UnixMilli(),
	})
	if err != nil {
		log.Fatal(err)
	}

	// Optional: a checkpoint. Flush returns once everything accepted so far is
	// committed and queryable. A long-running service lets the thresholds do
	// this and only calls Close on shutdown.
	if err := fills.Flush(ctx); err != nil {
		log.Fatal(err)
	}
}
