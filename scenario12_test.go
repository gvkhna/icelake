package icelake_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/flush"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// FillV3 is FillV2 with one more optional column, and it exists so that the
// transition scenario can tell "reconciled to the newest backlog file's shape"
// apart from "reconciled to the declaration the process is holding".
//
// With only two shapes those two are the same table, so a drain that ran after
// the reconciliation would look identical to one that ran before it. A third
// shape makes the difference visible in the table's own metadata: the newest
// file's shape has nine columns and the current declaration has ten.
type FillV3 struct {
	Symbol         string  `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side           string  `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price          int64   `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity       int64   `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID     int64   `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID        string  `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source         string  `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
	VenueTimeMS    int64   `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
	VenueOrderType *string `parquet:"name=venue_order_type, logical=String, fieldid=9" arrow:"venue_order_type"`
	Desk           *string `parquet:"name=desk, logical=String, fieldid=10" arrow:"desk"`
}

// TestLocalOnlyConfigurationMatrix is scenario 12's configuration clause: every
// rule of the local-only matrix, one case each, plus the case that proves the
// violations are collected rather than returned first-one-wins.
//
// It needs no container, and neither does the mode it describes.
func TestLocalOnlyConfigurationMatrix(t *testing.T) {
	ctx := context.Background()

	// A valid local-only configuration, which every case below then breaks in
	// exactly one way. It is built here rather than through offlineConfig
	// because a local-only store's whole shape is the absence of the fields that
	// one supplies.
	local := func(t *testing.T) icelake.Config {
		t.Helper()

		return localOnlyConfig(t, t.TempDir())
	}

	cases := []struct {
		name   string
		field  string
		config func(*testing.T) icelake.Config
	}{
		{"local-only with no cache directory", "CacheDir", func(t *testing.T) icelake.Config {
			c := local(t)
			c.CacheDir = ""

			return c
		}},
		{"local-only with a catalog path", "CatalogPath", func(t *testing.T) icelake.Config {
			c := local(t)
			c.CatalogPath = filepath.Join(t.TempDir(), "catalog.db")

			return c
		}},
		{"local-only with an endpoint", "Endpoint", func(t *testing.T) icelake.Config {
			c := local(t)
			c.Endpoint = "http://127.0.0.1:9"

			return c
		}},
		{"local-only with a bucket", "Bucket", func(t *testing.T) icelake.Config {
			c := local(t)
			c.Bucket = "somewhere"

			return c
		}},
		{"local-only with a warehouse prefix", "WarehousePrefix", func(t *testing.T) icelake.Config {
			c := local(t)
			c.WarehousePrefix = "warehouse"

			return c
		}},
		{"local-only with an access key", "AccessKeyID", func(t *testing.T) icelake.Config {
			c := local(t)
			c.AccessKeyID = "unused"

			return c
		}},
		{"local-only with a secret key", "SecretAccessKey", func(t *testing.T) icelake.Config {
			c := local(t)
			c.SecretAccessKey = "unused"

			return c
		}},
		{"local-only with a credential provider", "Credentials", func(t *testing.T) icelake.Config {
			c := local(t)
			c.Credentials = staticCredentials("unused", "unused")

			return c
		}},
		// Retention only ever evicts a file that has been uploaded and
		// committed, and nothing is uploaded in this mode, so a bound here is a
		// knob that cannot turn.
		{"local-only with a size bound", "CacheMaxBytes", func(t *testing.T) icelake.Config {
			c := local(t)
			c.CacheMaxBytes = 1 << 20

			return c
		}},
		{"local-only with an age bound", "CacheMaxAge", func(t *testing.T) icelake.Config {
			c := local(t)
			c.CacheMaxAge = time.Hour

			return c
		}},
		// Independent of the mode: a bound with nothing to bound, and a cache
		// with no bound at all, which has to be asked for by name through
		// LocalOnly rather than fallen into by leaving two fields blank.
		{"a size bound with no cache directory", "CacheMaxBytes", func(t *testing.T) icelake.Config {
			c := offlineConfig(t)
			c.CacheMaxBytes = 1 << 20

			return c
		}},
		{"an age bound with no cache directory", "CacheMaxAge", func(t *testing.T) icelake.Config {
			c := offlineConfig(t)
			c.CacheMaxAge = time.Hour

			return c
		}},
		{"a cache directory in bucket mode with neither bound", "CacheDir", func(t *testing.T) icelake.Config {
			c := offlineConfig(t)
			c.CacheDir = filepath.Join(t.TempDir(), "cache")

			return c
		}},
		{"a negative size bound", "CacheMaxBytes", func(t *testing.T) icelake.Config {
			c := offlineConfig(t)
			c.CacheDir = filepath.Join(t.TempDir(), "cache")
			c.CacheMaxBytes = -1

			return c
		}},
		{"a negative age bound", "CacheMaxAge", func(t *testing.T) icelake.Config {
			c := offlineConfig(t)
			c.CacheDir = filepath.Join(t.TempDir(), "cache")
			c.CacheMaxAge = -time.Second

			return c
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := c.config(t)

			store, err := icelake.Open(ctx, cfg)
			if store != nil {
				t.Error("a refused Open returned a usable store")
			}

			var cfgErr icelake.ConfigError
			if !errors.As(err, &cfgErr) {
				t.Fatalf("Open returned %v (%T), want an icelake.ConfigError", err, err)
			}
			if !hasFieldViolation(err, c.field) {
				t.Errorf("no violation reported for %s; got %v", c.field, err)
			}
		})
	}

	// Every forbidden field named separately, in one run. A store told to reach
	// no bucket while holding a bucket's credentials is a caller who thinks they
	// configured something they did not, and being told that one of seven fields
	// should be empty would not tell them which.
	t.Run("every violation at once", func(t *testing.T) {
		cfg := localOnlyConfig(t, t.TempDir())
		cfg.CacheDir = ""
		cfg.CatalogPath = "catalog.db"
		cfg.Endpoint = "http://127.0.0.1:9"
		cfg.Bucket = "somewhere"
		cfg.WarehousePrefix = "warehouse"
		cfg.AccessKeyID = "unused"
		cfg.SecretAccessKey = "unused"
		cfg.CacheMaxBytes = 1
		cfg.CacheMaxAge = time.Hour

		store, err := icelake.Open(ctx, cfg)
		if store != nil {
			t.Error("a refused Open returned a usable store")
		}
		for _, field := range []string{
			"CacheDir", "CatalogPath", "Endpoint", "Bucket", "WarehousePrefix",
			"AccessKeyID", "SecretAccessKey", "CacheMaxBytes", "CacheMaxAge",
		} {
			if !hasFieldViolation(err, field) {
				t.Errorf("no violation reported for %s; every problem must be reported in one run", field)
			}
		}

		// Nothing was opened or created on the way to that error.
		if _, err := os.Stat(cfg.StagingPath); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the staging database exists after a refused Open (stat error %v)", err)
		}
	})
}

// TestLocalOnlyStoreWritesPrunesAndIsReadable is scenario 12's local-only
// clause, and it runs with no container at all.
//
// That is deliberate and it is asserted rather than relied on: this test does
// not call the container-runtime helper, because local-only mode has no
// substrate in it. Nothing is being stood in for — the storage substrate is
// absent from the *feature*, not from the test — which is the distinction
// TESTING.md draws beside its ban on mocks. What is real here is everything the
// mode actually has: real files on a real disk, a real staging database, and a
// real DuckDB reading the result back.
func TestLocalOnlyStoreWritesPrunesAndIsReadable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := localOnlyConfig(t, dir)

	store := openStore(t, cfg)

	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter fills: %v", err)
	}
	events, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		t.Fatalf("OpenWriter events: %v", err)
	}

	const records = 30
	for i := range records {
		if err := fills.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert fill %d: %v", i, err)
		}
		if err := events.Insert(ctx, makeEvent(i)); err != nil {
			t.Fatalf("Insert event %d: %v", i, err)
		}
	}
	if err := fills.Flush(ctx); err != nil {
		t.Fatalf("Flush fills: %v", err)
	}
	if err := events.Flush(ctx); err != nil {
		t.Fatalf("Flush events: %v", err)
	}

	// The rows were pruned against the spool file, which is this mode's whole
	// durability story: the file on disk is what a confirmed flush means where
	// there is no bucket.
	if n := stagedRows(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d rows are still staged after a local-only flush; the rows are pruned against the spool file", n)
	}

	cached := cachedFiles(t, cfg.CacheDir)
	want := []string{
		filepath.Join("archive", "events", "data"),
		filepath.Join("market", "fills", "data"),
	}
	if len(cached) != 2 {
		t.Fatalf("the cache holds %v, want one file per table", cached)
	}
	for i, f := range cached {
		if filepath.Dir(f) != want[i] {
			t.Errorf("cached file %s is not under %s; the layout mirrors the warehouse layout", f, want[i])
		}
	}
	rows := spoolRows(t, cfg.StagingPath)
	if len(rows) != 2 {
		t.Fatalf("spool_files holds %d rows, want 2", len(rows))
	}
	for _, r := range rows {
		if r.committed {
			t.Errorf("spool file %s is marked committed in a mode that never commits anything", r.batchKey)
		}
	}

	// No catalog anywhere on disk. Asserted rather than merely not used: a
	// local-only store opens no catalog, and a file appearing here would mean
	// the mode had quietly kept half of the bucket path.
	err = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Base(p) == "catalog.db" {
			t.Errorf("a catalog database exists at %s; local-only mode opens none", p)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walking the local-only store's directory: %v", err)
	}

	// The one Store method that needs a bucket says so with the sentinel rather
	// than failing from a client that was never built.
	if err := store.DrainSpool(ctx); !errors.Is(err, icelake.ErrLocalOnly) {
		t.Errorf("DrainSpool returned %v, want ErrLocalOnly", err)
	}

	// DuckDB reads the cache with scenario 1's exactness: the decimal against
	// its exact decimal string rather than a tolerance, and the timestamp as a
	// real timestamp type rather than an integer a reader has to reinterpret.
	duck := openLocalDuckDB(t)
	fillsGlob := filepath.Join(cfg.CacheDir, "market", "fills", "data", "*.parquet")
	table := fmt.Sprintf("read_parquet('%s')", fillsGlob)

	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", table), &total)
	if total != records {
		t.Errorf("the cache reads back %d rows, want %d", total, records)
	}

	var price string
	duck.queryRow(t, fmt.Sprintf("SELECT CAST(price AS VARCHAR) FROM %s WHERE sequence_id = 7", table), &price)
	if want := "1.234567897"; price != want {
		t.Errorf("price reads back %s, want exactly %s", price, want)
	}
	if got := duck.columnType(t, table, "venue_timestamp_ms"); got != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("venue_timestamp_ms reads back as %s, want a real timestamp type", got)
	}

	var sum int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')",
		filepath.Join(cfg.CacheDir, "archive", "events", "data", "*.parquet")), &sum)
	if sum != records {
		t.Errorf("the event cache reads back %d rows, want %d", sum, records)
	}
}

// TestTransitionDrainsTheBacklogInOrder is scenario 12's transition clause, and
// it is the one that needs the container.
//
// A local-only run writes four batches under two shapes; the same StagingPath
// and the same CacheDir are then opened in bucket mode under a *third*
// declaration. What the table's own metadata must then say, read back from the
// bucket:
//
//   - the table was created from the OLDEST file's descriptor — eight columns,
//     not the nine of the newest file and not the ten the process declares;
//   - the files were committed in creation order, which the snapshots' own
//     added-records say, because each batch was given a distinct size;
//   - the table was walked forward one file at a time, so the ninth column
//     appears exactly at the first file that was written under it;
//   - the reconciliation to the current declaration happened after the last
//     file rather than before the first, which is the tenth column arriving
//     last.
//
// A drain that ran after the table had been reconciled forward would satisfy the
// row counts and the read-back whenever the shapes happen to be compatible,
// which is exactly why the ordering is asserted directly rather than inferred
// from the data.
func TestTransitionDrainsTheBacklogInOrder(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	dir := t.TempDir()
	local := localOnlyConfig(t, dir)

	// Batch sizes are all different so that the order they were committed in is
	// legible from the snapshots themselves.
	sizes := []int{7, 8, 9, 10}
	writeLocalOnly(t, local, sizes)

	cfg := bucketConfigOver(t, local, bucket)
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillV3]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter in bucket mode: %v", err)
	}

	// One data file per batch, one snapshot per batch, in creation order.
	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(objects) != len(sizes) {
		t.Fatalf("the bucket holds %d data files after the drain, want one per backlog file (%d)", len(objects), len(sizes))
	}
	added := readMetadata(t, bucket, latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")).addedRecords(t)
	if !slices.Equal(added, sizes) {
		t.Errorf("the snapshots added %v records in order, want %v; the backlog is committed in file creation order", added, sizes)
	}

	// The table's shape after every commit it has ever taken. Creation is the
	// first entry; then one per drained file; then the reconciliation to the
	// declaration this process is holding.
	widths := schemaWidthPerCommit(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(widths) == 0 {
		t.Fatal("the table has no metadata files")
	}
	if widths[0] != 8 {
		t.Errorf("the table was created with %d columns, want the 8 of the OLDEST backlog file's descriptor; "+
			"a table created from the current declaration would have 10", widths[0])
	}
	if got := widths[len(widths)-1]; got != 10 {
		t.Errorf("the table ends with %d columns, want the 10 the current declaration has", got)
	}
	if !slices.IsSorted(widths) {
		t.Errorf("the table's column count over its commits is %v, which is not monotonic; the drain walks the shape forward one file at a time", widths)
	}
	if !slices.Contains(widths, 9) {
		t.Errorf("the table's column count over its commits is %v and never passes through the 9 of the second shape; "+
			"each file must be committed against its own recorded shape", widths)
	}
	// The ten-column shape is the current declaration's and must appear only
	// after every backlog file has been committed. There are four files, so at
	// most the very last metadata file may be ten columns wide.
	for i, w := range widths[:len(widths)-1] {
		if w == 10 {
			t.Errorf("the table already had the current declaration's 10 columns at commit %d of %d; "+
				"the reconciliation must happen after the last backlog file, not before the first", i, len(widths))

			break
		}
	}

	// Every file marked committed, nothing left staged, and every record
	// queryable through the table rather than only through the cache.
	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d spool files are still uncommitted after the drain", n)
	}
	if n := stagedRows(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d rows are still staged after the drain", n)
	}

	total := 0
	for _, n := range sizes {
		total += n
	}
	duck := openDuckDB(t, bucket)
	metadata := latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")

	var got int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", duck.scan(metadata)), &got)
	if got != total {
		t.Errorf("the table reads back %d rows, want the %d written during the local-only period", got, total)
	}

	// The rows written before the added column read NULL in it, which is what
	// makes this one table across the boundary rather than two.
	var nulls int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE venue_order_type IS NULL", duck.scan(metadata)), &nulls)
	if want := sizes[0] + sizes[1]; nulls != want {
		t.Errorf("%d rows read NULL in the added column, want the %d written before it existed", nulls, want)
	}
	var desks int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE desk IS NULL", duck.scan(metadata)), &desks)
	if desks != total {
		t.Errorf("%d rows read NULL in the column this process added, want all %d: nothing was written under it", desks, total)
	}

	// Re-running is a no-op: a second bucket-mode open commits nothing further
	// and adds no snapshot.
	snapshotsBefore := snapshotCount(t, bucket, cfg.WarehousePrefix, "market", "fills")
	metadataBefore := len(metadataVersions(t, bucket, cfg.WarehousePrefix, "market", "fills"))
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	again := openStore(t, cfg)
	reopened, err := icelake.OpenWriter(ctx, again, icelake.TableConfig[FillV3]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter a second time: %v", err)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatalf("Close a second time: %v", err)
	}
	if err := again.DrainSpool(ctx); err != nil {
		t.Fatalf("DrainSpool with nothing left to drain: %v", err)
	}

	if n := snapshotCount(t, bucket, cfg.WarehousePrefix, "market", "fills"); n != snapshotsBefore {
		t.Errorf("the table has %d snapshots after a second open, want the %d it already had", n, snapshotsBefore)
	}
	if n := len(metadataVersions(t, bucket, cfg.WarehousePrefix, "market", "fills")); n != metadataBefore {
		t.Errorf("the table has %d metadata files after a second open, want the %d it already had", n, metadataBefore)
	}
	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(objects) != len(sizes) {
		t.Errorf("the bucket holds %d data files after a second open, want %d", len(objects), len(sizes))
	}
}

// TestDrainSpoolDrainsATableNoWriterReopens is scenario 12's last clause.
//
// It is the only path that covers a table whose declaration is no longer in the
// process: nothing opens a writer for it, so nothing would ever look at its
// backlog, and the files would sit on one local disk forever.
func TestDrainSpoolDrainsATableNoWriterReopens(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	dir := t.TempDir()
	local := localOnlyConfig(t, dir)

	// Two tables written locally; only one of them is ever opened again.
	store := openStore(t, local)
	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter fills: %v", err)
	}
	events, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		t.Fatalf("OpenWriter events: %v", err)
	}
	for i := range 12 {
		if err := fills.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert fill %d: %v", i, err)
		}
		if err := events.Insert(ctx, makeEvent(i)); err != nil {
			t.Fatalf("Insert event %d: %v", i, err)
		}
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	cfg := bucketConfigOver(t, local, bucket)
	reopened := openStore(t, cfg)
	if err := reopened.DrainSpool(ctx); err != nil {
		t.Fatalf("DrainSpool: %v", err)
	}

	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d spool files are still uncommitted after DrainSpool", n)
	}

	duck := openDuckDB(t, bucket)
	for _, c := range []struct{ namespace, table string }{{"market", "fills"}, {"archive", "events"}} {
		if objects := dataFiles(t, bucket, cfg.WarehousePrefix, c.namespace, c.table); len(objects) != 1 {
			t.Errorf("%s.%s has %d data files after DrainSpool, want 1", c.namespace, c.table, len(objects))

			continue
		}
		var total int
		duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s",
			duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, c.namespace, c.table))), &total)
		if total != 12 {
			t.Errorf("%s.%s reads back %d rows, want 12", c.namespace, c.table, total)
		}
	}
}

// TestADrainKilledPartWayThroughResumes is scenario 12's crash clause: a run
// killed part-way through a drain and restarted produces the same final set of
// snapshots and one data file per batch.
//
// It is the idempotency claim scenario 2 makes about a single flush, made about
// a backlog, and it rests on the same three things: the file's name is its batch
// key, the object key is what that batch would always have had, and the two
// orderings that could double-count are closed by the snapshot check before the
// commit and by marking the file committed after the commit lands.
//
// The kill is deterministic rather than timed. The child is pointed at a proxy
// that holds every request back, so the drain takes seconds; the parent watches
// the *bucket* — directly, not through the proxy — and kills the moment the
// first data file appears, which is necessarily before the last one has.
func TestADrainKilledPartWayThroughResumes(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	dir := t.TempDir()
	local := localOnlyConfig(t, dir)

	sizes := []int{3, 4, 5, 6, 7, 8}
	writeLocalOnly(t, local, sizes)

	cfg := bucketConfigOver(t, local, bucket)
	proxy := startSlowProxy(t, bucket.Endpoint, 50*time.Millisecond)

	killed := cfg
	killed.Endpoint = proxy.Endpoint
	plan := crashPlan(killed)
	plan.Script = scriptDrain

	child := startCrashChild(t, plan)
	waitFor(t, "the drain to commit its first backlog file", func() bool {
		return len(dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")) > 0
	})
	child.sigkill(t)

	// The kill really did land inside the drain: some of the backlog is still
	// waiting. Without this the rest of the test would pass just as happily
	// against a drain that had finished before the parent looked.
	interrupted := uncommittedSpoolFiles(t, cfg.StagingPath)
	if interrupted == 0 {
		t.Fatal("the drain had finished before the child was killed; this test needs a drain interrupted part-way")
	}

	// The restart, against the real endpoint, with the ordinary declaration.
	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillV2]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after the kill: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close after the kill: %v", err)
	}

	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(objects) != len(sizes) {
		t.Errorf("the bucket holds %d data files after the resumed drain, want one per batch (%d); "+
			"a re-uploaded file overwrites its own key rather than adding a second", len(objects), len(sizes))
	}
	if n := snapshotCount(t, bucket, cfg.WarehousePrefix, "market", "fills"); n != len(sizes) {
		t.Errorf("the table has %d snapshots after the resumed drain, want one per batch (%d)", n, len(sizes))
	}
	added := readMetadata(t, bucket, latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")).addedRecords(t)
	if !slices.Equal(added, sizes) {
		t.Errorf("the snapshots added %v records in order, want %v; a resumed drain commits what is left, in the same order, exactly once", added, sizes)
	}
	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d spool files are still uncommitted after the resumed drain", n)
	}

	total := 0
	for _, n := range sizes {
		total += n
	}
	duck := openDuckDB(t, bucket)
	var got int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s",
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))), &got)
	if got != total {
		t.Errorf("the table reads back %d rows, want the %d written locally, each committed exactly once", got, total)
	}
}

// TestASpoolFileRewrittenUnderANewShapeDrainsUnderThatShape is a regression
// test, and the chain it walks is narrow enough to be worth spelling out because
// every step of it is ordinary and the end of it is data loss.
//
// A local-only run seals a batch and dies at the spool seam, leaving a file
// written under the declaration of the day. The service is then redeployed with
// one more column, and the batch is *replayed* — under its stored batch key,
// because a sealed batch's key is never recomputed, but re-encoded onto the new
// shape, because replay always encodes to the current declaration. So the same
// file name comes back carrying a different schema. If the row recording that
// file kept the old fingerprint, the transition drain would derive an
// eight-column declaration for a nine-column file, create or reconcile the table
// to the wrong shape, and commit today's bytes against yesterday's columns.
//
// The assertion that catches it is the recorded fingerprint changing, and then
// the table the drain builds having the columns the file really has.
func TestASpoolFileRewrittenUnderANewShapeDrainsUnderThatShape(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	dir := t.TempDir()
	local := localOnlyConfig(t, dir)

	const records = 20
	plan := crashPlan(local)
	plan.Script = scriptFlushCrash
	plan.Decl = declFill
	plan.Fills = records
	plan.CrashPoint = flush.PointAfterSpoolBeforeUpload
	plan.CrashMode = crashExit

	child := startCrashChild(t, plan)
	child.await(t, lineStaged)
	child.awaitCrashPoint(t, plan.CrashPoint)

	// What the dead process left: a file under the old shape, and the batch's
	// rows still staged and still sealed under the key that names it.
	before := spoolRows(t, local.StagingPath)
	if len(before) != 1 || before[0].committed {
		t.Fatalf("spool_files holds %+v, want one row that has never reached a bucket", before)
	}
	if n := sealedRows(t, local.StagingPath); n != records {
		t.Fatalf("%d sealed rows survive the crash, want %d", n, records)
	}

	// The redeploy: the same staging file and the same cache, one more column.
	store := openStore(t, local)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillV2]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter under the evolved declaration: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	after := spoolRows(t, local.StagingPath)
	if len(after) != 1 {
		t.Fatalf("spool_files holds %d rows after the replay, want the same one file rewritten", len(after))
	}
	if after[0].batchKey != before[0].batchKey {
		t.Fatalf("the replayed batch was written under key %s, want its stored key %s; a sealed batch's key is never recomputed",
			after[0].batchKey, before[0].batchKey)
	}
	if after[0].schemaFP == before[0].schemaFP {
		t.Fatalf("the spool row still records schema %s after the file was rewritten under a new declaration; "+
			"the drain reads that fingerprint to decide what shape to commit this file against", after[0].schemaFP)
	}
	if n := stagedRows(t, local.StagingPath); n != 0 {
		t.Errorf("%d rows are still staged after the local-only replay flushed the batch", n)
	}

	// The transition. The drain must build the table from the shape the file
	// actually has, which is the new one.
	cfg := bucketConfigOver(t, local, bucket)
	bucketStore := openStore(t, cfg)
	drained, err := icelake.OpenWriter(ctx, bucketStore, icelake.TableConfig[FillV2]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter in bucket mode over the rewritten backlog: %v", err)
	}
	if err := drained.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	widths := schemaWidthPerCommit(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(widths) == 0 {
		t.Fatal("the table has no metadata files")
	}
	if widths[0] != 9 {
		t.Errorf("the table was created with %d columns, want the 9 the rewritten file actually has", widths[0])
	}
	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d spool files are still uncommitted after the drain", n)
	}

	duck := openDuckDB(t, bucket)
	metadata := latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")
	var total, nulls int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", duck.scan(metadata)), &total)
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE venue_order_type IS NULL", duck.scan(metadata)), &nulls)
	if total != records {
		t.Errorf("the table reads back %d rows, want the %d the dead process had accepted", total, records)
	}
	// The column the redeploy added is queryable and null for every row, which
	// is exactly right: these records were accepted before it existed, so replay
	// re-encoded them into the new shape with nothing to put in it. What matters
	// is that the column is *there* to be null — a table built from the stale
	// fingerprint would not have it at all, and this query would fail rather
	// than answer.
	if nulls != records {
		t.Errorf("%d rows read NULL in the column the redeploy added, want all %d", nulls, records)
	}
}

// TestDrainSpoolSerializesAgainstWriters is finding 4 of the M11 review, and the
// claim is about what two callers may do at once.
//
// A drain and a writer's own open-time drain would otherwise walk the same
// backlog concurrently: the object keys are identical and the snapshot check
// stops most double commits, but the two would still race over marking files and
// pruning rows the other is reading. So a drain claims each table's slot in the
// store exactly as [icelake.OpenWriter] does, and skips a table that already has
// a live writer, whose own open already drained it.
func TestDrainSpoolSerializesAgainstWriters(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	dir := t.TempDir()
	local := localOnlyConfig(t, dir)

	store := openStore(t, local)
	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter fills: %v", err)
	}
	events, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		t.Fatalf("OpenWriter events: %v", err)
	}
	for i := range 10 {
		if err := fills.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert fill %d: %v", i, err)
		}
		if err := events.Insert(ctx, makeEvent(i)); err != nil {
			t.Fatalf("Insert event %d: %v", i, err)
		}
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	cfg := bucketConfigOver(t, local, bucket)
	reopened := openStore(t, cfg)

	// A live writer on one of the two tables. Its own open drained that table's
	// backlog; the drain below must leave it alone rather than walk it again.
	held, err := icelake.OpenWriter(ctx, reopened, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter in bucket mode: %v", err)
	}

	if err := reopened.DrainSpool(ctx); err != nil {
		t.Fatalf("DrainSpool with a live writer on one table: %v", err)
	}

	// Both tables ended up drained — one by the writer's own open, one by the
	// call above — and neither was committed twice.
	for _, c := range []struct{ namespace, table string }{{"market", "fills"}, {"archive", "events"}} {
		if objects := dataFiles(t, bucket, cfg.WarehousePrefix, c.namespace, c.table); len(objects) != 1 {
			t.Errorf("%s.%s has %d data files, want 1", c.namespace, c.table, len(objects))
		}
		if n := snapshotCount(t, bucket, cfg.WarehousePrefix, c.namespace, c.table); n != 1 {
			t.Errorf("%s.%s has %d snapshots, want 1", c.namespace, c.table, n)
		}
	}
	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d spool files are still uncommitted", n)
	}

	if err := held.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The exclusion has to hold when the two really do arrive at once, and that
	// needs a backlog for them to arrive at. Everything above is drained, so the
	// race is run against a second local-only store, written under its own
	// namespace so it has a backlog of its own and a table nothing has touched.
	//
	// This is the shape the reviewer's reproduction used, and the reason it
	// matters: with no backlog, both sides return at the empty guard before they
	// reach anything worth serializing, and the test proves nothing at all.
	raceLocal := localOnlyConfig(t, t.TempDir())
	writeLocalOnlyTable(t, raceLocal, "race", "fills", []int{4, 5, 6})

	raceCfg := bucketConfigOver(t, raceLocal, bucket)
	racing := openStore(t, raceCfg)

	if n := uncommittedSpoolFiles(t, raceCfg.StagingPath); n != 3 {
		t.Fatalf("%d spool files are waiting for the race, want the 3 the local-only run left", n)
	}

	var openErr, drainErr error
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		w, err := icelake.OpenWriter(ctx, racing, icelake.TableConfig[Fill]{Namespace: "race", Table: "fills"})
		openErr = err
		if err == nil {
			openErr = w.Close(ctx)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		drainErr = racing.DrainSpool(ctx)
	}()
	close(start)
	wg.Wait()

	// The documented outcomes, and nothing else. An OpenWriter that lost the
	// slot is refused with the sentinel that says so; a DrainSpool that lost it
	// skips the table and says nothing. What neither may be is a second
	// committer, and what neither may return is a raw duplicate-key failure from
	// the staging database, which is what two unclaimed walks produce.
	if openErr != nil && !errors.Is(openErr, icelake.ErrTableInUse) {
		t.Errorf("the concurrent OpenWriter returned %v (%T), want nil or ErrTableInUse", openErr, openErr)
	}
	if drainErr != nil {
		t.Errorf("the concurrent DrainSpool returned %v (%T), want nil: a table whose slot is held is skipped, not refused",
			drainErr, drainErr)
	}

	// Whoever did the work, the backlog landed exactly once.
	if objects := dataFiles(t, bucket, raceCfg.WarehousePrefix, "race", "fills"); len(objects) != 3 {
		t.Errorf("race.fills has %d data files after the concurrent open and drain, want one per backlog file (3)", len(objects))
	}
	if n := snapshotCount(t, bucket, raceCfg.WarehousePrefix, "race", "fills"); n != 3 {
		t.Errorf("race.fills has %d snapshots, want one per backlog file (3): every file commits exactly once", n)
	}
	if n := uncommittedSpoolFiles(t, raceCfg.StagingPath); n != 0 {
		t.Errorf("%d spool files are still uncommitted after the concurrent open and drain", n)
	}

	// And the losing side left nothing half-done behind it: a retry finds an
	// empty backlog and opens normally.
	retry, err := icelake.OpenWriter(ctx, racing, icelake.TableConfig[Fill]{Namespace: "race", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after the race: %v", err)
	}
	if err := retry.Close(ctx); err != nil {
		t.Fatalf("Close after the race: %v", err)
	}
	if n := snapshotCount(t, bucket, raceCfg.WarehousePrefix, "race", "fills"); n != 3 {
		t.Errorf("race.fills has %d snapshots after a retry, want the 3 it already had", n)
	}
}

// TestDrainSpoolRefusesAClosedStore is the last of the entry-point refusals: a
// closed store answers with the sentinel that says so, before it touches a
// database that is no longer open.
//
// It needs no container and no cache, which is the point — the refusal happens
// before either would be reached.
func TestDrainSpoolRefusesAClosedStore(t *testing.T) {
	ctx := context.Background()

	store := openStore(t, offlineConfig(t))
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	if err := store.DrainSpool(ctx); !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("DrainSpool on a closed store returned %v, want ErrClosed", err)
	}

	// The same answer must come from a closed local-only store: closed wins
	// over local-only, or the doc's "before it touches anything, exactly as
	// every other entry point does" is false for the one mode DrainSpool
	// exists to rescue.
	localStore := openStore(t, localOnlyConfig(t, t.TempDir()))
	if err := localStore.Close(ctx); err != nil {
		t.Fatalf("Store.Close (local-only): %v", err)
	}
	if err := localStore.DrainSpool(ctx); !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("DrainSpool on a closed local-only store returned %v, want ErrClosed", err)
	}
}

// TestDrainSpoolReportsOneTableAndDrainsTheRest is finding 5 of the M11 review.
//
// This is a process-wide recovery call, so one table's failure must not decide
// what happens to every other table's backlog. It keeps the first error and
// returns it once every table has been attempted, which is the shape
// [icelake.Store.Close] already uses when it drains many writers.
//
// The failure is produced by deleting one table's spool file from under it,
// which is a real state — an operator can do exactly that — and reaches the
// drain as a [icelake.SpoolError] of kind drain.
func TestDrainSpoolReportsOneTableAndDrainsTheRest(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	dir := t.TempDir()
	local := localOnlyConfig(t, dir)

	store := openStore(t, local)
	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter fills: %v", err)
	}
	events, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		t.Fatalf("OpenWriter events: %v", err)
	}
	for i := range 10 {
		if err := fills.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert fill %d: %v", i, err)
		}
		if err := events.Insert(ctx, makeEvent(i)); err != nil {
			t.Fatalf("Insert event %d: %v", i, err)
		}
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	// archive.events sorts before market.fills, so the table that fails is the
	// one the walk meets first: what is being proven is that the walk carried on
	// afterwards, which a test whose failing table came last could not show.
	for _, r := range spoolRows(t, local.StagingPath) {
		if r.namespace != "archive" {
			continue
		}
		if err := os.Remove(filepath.Join(local.CacheDir, r.namespace, r.table, "data", r.batchKey+".parquet")); err != nil {
			t.Fatalf("removing a spool file to make its table undrainable: %v", err)
		}
	}

	cfg := bucketConfigOver(t, local, bucket)
	reopened := openStore(t, cfg)

	err = reopened.DrainSpool(ctx)
	var spoolErr icelake.SpoolError
	if !errors.As(err, &spoolErr) {
		t.Fatalf("DrainSpool returned %v (%T), want an icelake.SpoolError for the table whose file is gone", err, err)
	}
	if spoolErr.Kind != icelake.SpoolKindDrain {
		t.Errorf("the failure is kind %q, want %q", spoolErr.Kind, icelake.SpoolKindDrain)
	}

	// The other table was drained anyway, which is the whole claim.
	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills"); len(objects) != 1 {
		t.Errorf("market.fills has %d data files after a failure on another table, want 1", len(objects))
	}
	if n := uncommittedSpoolFiles(t, cfg.StagingPath); n != 1 {
		t.Errorf("%d spool files are uncommitted, want the 1 whose file an operator removed", n)
	}

	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s",
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))), &total)
	if total != 10 {
		t.Errorf("market.fills reads back %d rows, want 10", total)
	}
}

// writeLocalOnlyTable runs a local-only store over cfg and flushes one batch per
// entry in sizes into a single table under Case A's declaration, then closes it.
//
// It leaves exactly len(sizes) spool files that no bucket has ever seen, which
// is the state a transition starts from and the only way to get one: in bucket
// mode a batch whose upload failed keeps its rows in staging, and the drain
// deliberately leaves those alone because replay owns them.
func writeLocalOnlyTable(tb testing.TB, cfg icelake.Config, namespace, table string, sizes []int) {
	tb.Helper()

	ctx := context.Background()
	store, err := icelake.Open(ctx, cfg)
	if err != nil {
		tb.Fatalf("Open local-only: %v", err)
	}
	defer func() {
		if err := store.Close(ctx); err != nil {
			tb.Fatalf("Store.Close: %v", err)
		}
	}()

	w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: namespace, Table: table})
	if err != nil {
		tb.Fatalf("OpenWriter %s.%s: %v", namespace, table, err)
	}

	next := 0
	for _, n := range sizes {
		for range n {
			if err := w.Insert(ctx, makeFill(next)); err != nil {
				tb.Fatalf("Insert: %v", err)
			}
			next++
		}
		if err := w.Flush(ctx); err != nil {
			tb.Fatalf("Flush: %v", err)
		}
	}
}

// writeLocalOnly runs a local-only store over cfg and flushes one batch per
// entry in sizes, the first half under Case A's declaration and the second half
// under the evolved one.
//
// Reopening between the two shapes is what a caller actually does: a second run
// of the same program with one more field in its struct. The batches are
// deliberately different sizes so that a later assertion can tell which is which
// from the table's own snapshots.
func writeLocalOnly(tb testing.TB, cfg icelake.Config, sizes []int) {
	tb.Helper()

	ctx := context.Background()
	next := 0

	run := func(shape int, counts []int) {
		store, err := icelake.Open(ctx, cfg)
		if err != nil {
			tb.Fatalf("Open local-only: %v", err)
		}
		defer func() {
			if err := store.Close(ctx); err != nil {
				tb.Fatalf("Store.Close: %v", err)
			}
		}()

		if shape == 1 {
			w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
			if err != nil {
				tb.Fatalf("OpenWriter: %v", err)
			}
			for _, n := range counts {
				for range n {
					if err := w.Insert(ctx, makeFill(next)); err != nil {
						tb.Fatalf("Insert: %v", err)
					}
					next++
				}
				if err := w.Flush(ctx); err != nil {
					tb.Fatalf("Flush: %v", err)
				}
			}

			return
		}

		w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillV2]{Namespace: "market", Table: "fills"})
		if err != nil {
			tb.Fatalf("OpenWriter: %v", err)
		}
		for _, n := range counts {
			for range n {
				if err := w.Insert(ctx, makeFillV2(next)); err != nil {
					tb.Fatalf("Insert: %v", err)
				}
				next++
			}
			if err := w.Flush(ctx); err != nil {
				tb.Fatalf("Flush: %v", err)
			}
		}
	}

	half := len(sizes) / 2
	run(1, sizes[:half])
	run(2, sizes[half:])
}
