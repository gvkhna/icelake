package icelake_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/flush"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TESTING.md scenario 16: the ClickHouse mirror, end to end.
//
// Every claim here is about the mirror ARCHITECTURE.md's "The ClickHouse
// mirror" section decides — the encode-seam tap, the doorbell-not-queue failure
// shape, the _batch_key column, the add-only reconciliation and its refusal —
// and every one of them is asserted against a real server rather than against
// what icelake believes it did.

// mirrorDatabase is the database the mirror uses when nothing configures one,
// written out here rather than read from the library so that a test asserting
// where a table landed is not asking the library where it put it.
const mirrorDatabase = "icelake"

// unreachableAddr is an address nothing is listening on. It is used where the
// claim is about a mirror that cannot be reached and no server is wanted at
// all; port 1 refuses immediately, so a test that expects an outage does not
// wait for one.
const unreachableAddr = "127.0.0.1:1"

// TestTheMirrorHoldsEveryBatchWithItsBatchKey is the scenario's first two
// clauses: rows arrive with the right values in the right column types, and
// every row carries the batch key that also names its object in the warehouse.
func TestTheMirrorHoldsEveryBatchWithItsBatchKey(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)
	ch := testsubstrate.StartClickHouse(t)

	ctx := context.Background()
	cfg := cacheConfig(t, bucket)
	cfg.ClickHouse = mirrorFor(ch)
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// Three batches at the configured record threshold, so the claim is about a
	// directory of files and a set of batch keys rather than about one batch
	// that could have been special.
	const records = 150
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	conn := chConn(t, ch)
	target := mirrorDatabase + ".market__fills"

	t.Run("the table was created with the mapped types", func(t *testing.T) {
		// Read from the server's own catalog rather than inferred from what the
		// inserts accepted: a column created at the wrong type still accepts
		// every value this scenario writes, so the type is the claim.
		want := map[string]string{
			"symbol":             "String",
			"side":               "String",
			"price":              "Decimal(18, 9)",
			"quantity":           "Decimal(18, 9)",
			"sequence_id":        "Int64",
			"order_id":           "String",
			"source":             "String",
			"venue_timestamp_ms": "DateTime64(6, 'UTC')",
			"_batch_key":         "String",
		}
		got := chColumnTypes(t, conn, mirrorDatabase, "market__fills")
		for name, typ := range want {
			if got[name] != typ {
				t.Errorf("column %s is %q, want %q per SCHEMA.md's mapping", name, got[name], typ)
			}
		}
		if len(got) != len(want) {
			t.Errorf("the mirror table has %d columns, want %d", len(got), len(want))
		}
	})

	t.Run("every record is there with its own values", func(t *testing.T) {
		if n := chCount(t, conn, "SELECT count() FROM "+target); n != records {
			t.Fatalf("the mirror holds %d rows, want the %d that were written", n, records)
		}

		// One record, checked against numbers this test derives rather than
		// copies: the decimal at its exact digits, and the timestamp as an
		// integer count of microseconds so that no formatting can hide a scale
		// error. Row 7's price is the unscaled 1_234_567_897 at scale 9.
		const row = 7
		got := chStrings(t, conn, fmt.Sprintf(
			"SELECT concat(symbol, '|', side, '|', toString(price), '|', toString(quantity), '|', toString(toUnixTimestamp64Micro(venue_timestamp_ms)), '|', order_id) FROM %s WHERE sequence_id = %d",
			target, row))
		want := fmt.Sprintf("SYM%d|%s|1.234567897|2.000000007|%d|order-%06d",
			row%7, "sell", (1_700_000_000_000+int64(row)*1000)*1000, row)
		if len(got) != 1 || got[0] != want {
			t.Errorf("row %d reads back as %v, want [%s]", row, got, want)
		}
	})

	t.Run("the batch keys are the warehouse's own object names", func(t *testing.T) {
		keys := chStrings(t, conn, "SELECT DISTINCT _batch_key FROM "+target+" ORDER BY _batch_key")
		objects := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
		if len(objects) < 3 {
			t.Fatalf("the table has %d data files, want at least 3", len(objects))
		}

		var names []string
		for _, o := range objects {
			names = append(names, strings.TrimSuffix(filepath.Base(o.key), ".parquet"))
		}
		if got, want := sortedCopy(keys), sortedCopy(names); !equalStrings(got, want) {
			t.Errorf("the mirror's batch keys are %v and the warehouse's objects are %v; they are the same strings or the join between the two is a fiction",
				got, want)
		}
	})
}

// TestAMirrorOutageNeverCostsTheLake is the doorbell decision's sharp end: a
// ClickHouse that goes away mid-run costs a notification and nothing else.
//
// The server is stopped rather than misconfigured, because a wrong address
// would prove something weaker — it never worked — and the claim is about a
// mirror that was working and stopped.
func TestAMirrorOutageNeverCostsTheLake(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)
	ch := testsubstrate.StartClickHouse(t)

	ctx := context.Background()

	var mu sync.Mutex
	var mirrorErrs []icelake.ClickHouseError
	var flushErrs []icelake.FlushError

	cfg := cacheConfig(t, bucket)
	cfg.ClickHouse = mirrorFor(ch)
	cfg.OnClickHouseError = func(err icelake.ClickHouseError) {
		mu.Lock()
		defer mu.Unlock()
		mirrorErrs = append(mirrorErrs, err)
	}
	cfg.OnFlushError = func(err icelake.FlushError) {
		mu.Lock()
		defer mu.Unlock()
		flushErrs = append(flushErrs, err)
	}
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// One batch while the mirror is healthy, so the outage below is a change
	// rather than the only state this table has ever known.
	for i := range 50 {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush before the outage: %v", err)
	}

	ch.Stop(t)

	const after = 100
	for i := 50; i < 50+after; i++ {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d after the outage: %v", i, err)
		}
	}
	// Neither of these may report the mirror. A Close that failed because
	// ClickHouse was down would be ClickHouse blocking the lake, which is the
	// one thing the mirror may never do.
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush during the outage: %v; a mirror failure must never reach Flush", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close during the outage: %v; a mirror failure must never reach Close", err)
	}

	mu.Lock()
	seen, flushed := mirrorErrs, flushErrs
	mu.Unlock()

	if len(seen) == 0 {
		t.Fatal("the mirror was down for two batches and OnClickHouseError never fired; the doorbell is the only channel there is")
	}
	for _, e := range seen {
		if e.Kind != icelake.ClickHouseKindConnect && e.Kind != icelake.ClickHouseKindInsert && e.Kind != icelake.ClickHouseKindSchema {
			t.Errorf("a mirror outage reported Kind %q, want connect, schema or insert", e.Kind)
		}
	}
	if len(flushed) != 0 {
		t.Errorf("OnFlushError fired %d times during a ClickHouse outage; a mirror failure is not a flush failure", len(flushed))
	}

	// The lake is untouched: every record is committed and queryable by a
	// reader that has never heard of the mirror.
	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s",
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))), &total)
	if total != 150 {
		t.Errorf("the table reads back %d rows, want 150; a ClickHouse outage may not lose lake data", total)
	}
}

// TestAWriterOpensAgainstAnUnreachableMirror is the reachability half of the
// open-time split: a server that is down when the writer opens is reported and
// the writer opens anyway, because refusing would mean a ClickHouse outage
// stopping the lake from accepting records.
//
// It runs local-only and needs no bucket: what it is about is whether a writer
// opens and commits, which is the same objects doing the same work either way.
func TestAWriterOpensAgainstAnUnreachableMirror(t *testing.T) {
	ctx := context.Background()

	var mu sync.Mutex
	var seen []icelake.ClickHouseError

	cfg := localOnlyConfig(t, t.TempDir())
	cfg.ClickHouse = &icelake.ClickHouseConfig{Addr: unreachableAddr, Username: "nobody"}
	cfg.OnClickHouseError = func(err icelake.ClickHouseError) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, err)
	}
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter against an unreachable mirror: %v; only a conflict may refuse an open", err)
	}
	for i := range 50 {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if n := len(cachedFiles(t, cfg.CacheDir)); n != 1 {
		t.Errorf("the cache holds %d files, want the 1 the batch wrote; the lake is unaffected by an unreachable mirror", n)
	}
	if n := stagedRows(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d rows are still staged after a flush that only the mirror failed", n)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("an unreachable mirror produced no notification at all")
	}
}

// TestAnUnmappableTableNameIsRefused covers the naming half of the conflict
// rule. It needs no server: the refusal happens before anything is created,
// which is the point of it being a conflict rather than a schema failure.
func TestAnUnmappableTableNameIsRefused(t *testing.T) {
	ctx := context.Background()

	cfg := localOnlyConfig(t, t.TempDir())
	cfg.ClickHouse = &icelake.ClickHouseConfig{Addr: unreachableAddr, Username: "nobody"}
	store := openStore(t, cfg)

	_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market__data", Table: "fills"})
	var typed icelake.ClickHouseError
	if !errors.As(err, &typed) || typed.Kind != icelake.ClickHouseKindConflict {
		t.Fatalf("OpenWriter for a namespace containing a double underscore returned %v, want a ClickHouseError of Kind conflict", err)
	}
}

// TestTheMirrorReconcilesAddOnlyAndRefusesTheRest is the reconciliation clause:
// a live table gains a new optional column, an operator's own arrangement is
// left alone, and a column at the wrong type is refused with that column named.
func TestTheMirrorReconcilesAddOnlyAndRefusesTheRest(t *testing.T) {
	testsubstrate.RequireDocker(t)
	ch := testsubstrate.StartClickHouse(t)

	ctx := context.Background()
	conn := chConn(t, ch)

	t.Run("a new optional column is added", func(t *testing.T) {
		cfg := localOnlyConfig(t, t.TempDir())
		cfg.ClickHouse = mirrorFor(ch)

		store := openStore(t, cfg)
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "recon", Table: "fills"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if got := chColumnTypes(t, conn, mirrorDatabase, "recon__fills"); got["venue_order_type"] != "" {
			t.Fatalf("the table already has venue_order_type before the declaration grew it")
		}

		// The same table, reopened under a declaration with one more optional
		// column — which is the only shape icelake's own schema evolution ever
		// produces.
		store2 := openStore(t, cfg)
		writer2, err := icelake.OpenWriter(ctx, store2, icelake.TableConfig[FillV2]{Namespace: "recon", Table: "fills"})
		if err != nil {
			t.Fatalf("OpenWriter after the declaration grew: %v", err)
		}
		if err := writer2.Insert(ctx, makeFillV2(1)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		if err := writer2.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if got := chColumnTypes(t, conn, mirrorDatabase, "recon__fills")["venue_order_type"]; got != "Nullable(String)" {
			t.Errorf("venue_order_type is %q after the declaration grew, want Nullable(String) added by the add-only reconciliation", got)
		}
		if n := chCount(t, conn, "SELECT count() FROM "+mirrorDatabase+".recon__fills"); n != 1 {
			t.Errorf("the mirror holds %d rows after the column was added, want 1", n)
		}
	})

	t.Run("a column at the wrong type is refused", func(t *testing.T) {
		chExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+mirrorDatabase)
		chExec(t, conn, "DROP TABLE IF EXISTS "+mirrorDatabase+".wrong__fills")
		// sequence_id as a String rather than an Int64: a table icelake did not
		// create, disagreeing in a way the add-only rule will not repair.
		chExec(t, conn, "CREATE TABLE "+mirrorDatabase+".wrong__fills ("+
			"`symbol` String, `side` String, `price` Decimal(18, 9), `quantity` Decimal(18, 9), "+
			"`sequence_id` String, `order_id` String, `source` String, "+
			"`venue_timestamp_ms` DateTime64(6, 'UTC'), `_batch_key` String) ENGINE = MergeTree ORDER BY tuple()")

		cfg := localOnlyConfig(t, t.TempDir())
		cfg.ClickHouse = mirrorFor(ch)
		store := openStore(t, cfg)

		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "wrong", Table: "fills"})
		var typed icelake.ClickHouseError
		if !errors.As(err, &typed) || typed.Kind != icelake.ClickHouseKindConflict {
			t.Fatalf("OpenWriter against a table with a retyped column returned %v, want a ClickHouseError of Kind conflict", err)
		}
		if typed.Column != "sequence_id" {
			t.Errorf("the conflict names column %q, want sequence_id", typed.Column)
		}
	})

	t.Run("an operator's own table is kept and written into", func(t *testing.T) {
		chExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+mirrorDatabase)
		chExec(t, conn, "DROP TABLE IF EXISTS "+mirrorDatabase+".mine__fills")
		// Their sorting key, their partitioning, their extra column. icelake
		// verifies the columns it writes and nothing else.
		chExec(t, conn, "CREATE TABLE "+mirrorDatabase+".mine__fills ("+
			"`symbol` String, `side` String, `price` Decimal(18, 9), `quantity` Decimal(18, 9), "+
			"`sequence_id` Int64, `order_id` String, `source` String, "+
			"`venue_timestamp_ms` DateTime64(6, 'UTC'), `_batch_key` String, "+
			"`ingested_at` DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY (symbol, sequence_id) "+
			"SETTINGS non_replicated_deduplication_window = 1000")

		cfg := localOnlyConfig(t, t.TempDir())
		cfg.ClickHouse = mirrorFor(ch)
		store := openStore(t, cfg)

		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "mine", Table: "fills"})
		if err != nil {
			t.Fatalf("OpenWriter against an operator's own table: %v", err)
		}
		for i := range 5 {
			if err := writer.Insert(ctx, makeFill(i)); err != nil {
				t.Fatalf("Insert %d: %v", i, err)
			}
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if n := chCount(t, conn, "SELECT count() FROM "+mirrorDatabase+".mine__fills"); n != 5 {
			t.Errorf("the mirror holds %d rows in the operator's own table, want 5", n)
		}
		if got := chStrings(t, conn, "SELECT sorting_key FROM system.tables WHERE database = '"+mirrorDatabase+"' AND name = 'mine__fills'"); len(got) != 1 || !strings.Contains(got[0], "symbol") {
			t.Errorf("the operator's sorting key reads back as %v, want theirs kept", got)
		}
	})
}

// TestLocalOnlyModeMirrorsWithNoBucketAtAll is the clause that proves the tap
// sits at the encode seam rather than after the catalog commit: in this mode
// there is no commit at all, and the rows still reach the mirror.
func TestLocalOnlyModeMirrorsWithNoBucketAtAll(t *testing.T) {
	testsubstrate.RequireDocker(t)
	ch := testsubstrate.StartClickHouse(t)

	ctx := context.Background()
	cfg := localOnlyConfig(t, t.TempDir())
	cfg.ClickHouse = mirrorFor(ch)
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "local", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	const records = 50
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	conn := chConn(t, ch)
	if n := chCount(t, conn, "SELECT count() FROM "+mirrorDatabase+".local__fills"); n != records {
		t.Fatalf("the mirror holds %d rows in local-only mode, want %d", n, records)
	}

	// The same join, against the spool rather than a bucket: the file names and
	// the batch keys are the same strings in both modes.
	keys := chStrings(t, conn, "SELECT DISTINCT _batch_key FROM "+mirrorDatabase+".local__fills ORDER BY _batch_key")
	var names []string
	for _, f := range cachedFiles(t, cfg.CacheDir) {
		names = append(names, strings.TrimSuffix(filepath.Base(f), ".parquet"))
	}
	if got, want := sortedCopy(keys), sortedCopy(names); !equalStrings(got, want) {
		t.Errorf("the mirror's batch keys are %v and the cache holds %v", got, want)
	}
}

// TestReplayDoesNotDuplicateInTheMirror is the deduplication clause, and it is
// the one that needs a real crash.
//
// The process is killed at the spool seam — after the mirror insert, which
// fires inside the encode step, and before anything reached the bucket — so the
// restart replays a batch the mirror has already seen. The assertion is the row
// count and nothing else: a token that was accepted and ignored looks exactly
// like a token that worked, right up until the count doubles.
func TestReplayDoesNotDuplicateInTheMirror(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)
	ch := testsubstrate.StartClickHouse(t)

	ctx := context.Background()
	cfg := cacheConfig(t, bucket)
	cfg.ClickHouse = mirrorFor(ch)

	const records = 20
	plan := crashPlan(cfg)
	plan.Script = scriptFlushCrash
	plan.Fills = records
	plan.FillNamespace = "replay"
	plan.CrashPoint = flush.PointAfterSpoolBeforeUpload
	plan.CrashMode = crashExit

	child := startCrashChild(t, plan)
	child.await(t, lineStaged)
	child.awaitCrashPoint(t, plan.CrashPoint)

	conn := chConn(t, ch)
	target := mirrorDatabase + ".replay__fills"

	before := chCount(t, conn, "SELECT count() FROM "+target)
	if before != records {
		t.Fatalf("the mirror holds %d rows after the crash, want the %d the dead process inserted at the encode seam", before, records)
	}
	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "replay", "fills"); len(objects) != 0 {
		t.Fatalf("the bucket holds %d data files after a crash before the upload", len(objects))
	}

	// The restart. The batch is still staged under its stored key, so the tap
	// fires again with the identical rows under the identical token.
	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "replay", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after the crash: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close after the crash: %v", err)
	}

	if objects := dataFiles(t, bucket, cfg.WarehousePrefix, "replay", "fills"); len(objects) != 1 {
		t.Fatalf("the table has %d data files after the restart, want 1", len(objects))
	}
	if after := chCount(t, conn, "SELECT count() FROM "+target); after != before {
		t.Errorf("the mirror holds %d rows after the replay and held %d before it; the deduplication token collapses the identical batch",
			after, before)
	}
	if keys := chStrings(t, conn, "SELECT DISTINCT _batch_key FROM "+target); len(keys) != 1 {
		t.Errorf("the mirror holds %d distinct batch keys after one batch was written twice, want 1", len(keys))
	}
}

// TestMirrorConfigurationIsRefusedByField is scenario 7's matrix extended to
// the mirror: a ClickHouse config that is half-written is refused per field, in
// both modes, before anything is opened.
func TestMirrorConfigurationIsRefusedByField(t *testing.T) {
	base := func() icelake.Config {
		return icelake.Config{
			StagingPath:       filepath.Join(t.TempDir(), "staging.db"),
			CacheDir:          filepath.Join(t.TempDir(), "cache"),
			LocalOnly:         true,
			FlushMaxRecords:   50,
			FlushMaxBytes:     1 << 20,
			FlushInterval:     time.Minute,
			ZSTDLevel:         1,
			StagingMaxRecords: 1000,
			StagingMaxBytes:   1 << 20,
		}
	}

	cases := []struct {
		name   string
		mirror *icelake.ClickHouseConfig
		fields []string
	}{
		{"no address", &icelake.ClickHouseConfig{Username: "icelake"}, []string{"ClickHouse.Addr"}},
		{"no username", &icelake.ClickHouseConfig{Addr: unreachableAddr}, []string{"ClickHouse.Username"}},
		{"neither", &icelake.ClickHouseConfig{}, []string{"ClickHouse.Addr", "ClickHouse.Username"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			cfg.ClickHouse = tc.mirror

			_, err := icelake.Open(context.Background(), cfg)
			var cfgErr icelake.ConfigError
			if !errors.As(err, &cfgErr) {
				t.Fatalf("Open returned %v, want a ConfigError", err)
			}
			for _, field := range tc.fields {
				if !hasConfigField(err, field) {
					t.Errorf("the refusal does not name %s; every violation is its own ConfigFieldError", field)
				}
			}
		})
	}
}

// hasConfigField reports whether a configuration refusal names one field.
func hasConfigField(err error, field string) bool {
	var joined interface{ Unwrap() []error }
	var one icelake.ConfigFieldError
	if errors.As(err, &one) && one.Field == field {
		return true
	}

	var cfgErr icelake.ConfigError
	if !errors.As(err, &cfgErr) {
		return false
	}
	if !errors.As(cfgErr.Err, &joined) {
		return false
	}
	for _, e := range joined.Unwrap() {
		if errors.As(e, &one) && one.Field == field {
			return true
		}
	}

	return false
}

// equalStrings reports whether two string slices are element-wise equal.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
