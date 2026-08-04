package icelake_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "modernc.org/sqlite"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
	"github.com/gvkhna/icelake/rebuild"
)

// TestCatalogRebuild is TESTING.md's scenario 3: delete catalog.db entirely and
// prove the catalog is a disposable, rebuildable cache rather than
// irreplaceable state.
//
// The shape is scenario 1's, then a deletion, then scenario 1's validation
// again. Both Case A and Case B are written through the real write path, the
// writers and the store are closed, the catalog database is removed from disk,
// and Rebuild is called with no table list and no hint of any kind — only an
// endpoint, a bucket, a prefix and a place to write. What comes back must name
// exactly the two tables that were written, at exactly the metadata files the
// original catalog pointed at, and the same DuckDB queries run through the
// rebuilt catalog must return identical results.
//
// This is valid *because the writer is stopped*, and that is not a convenience:
// rebuild takes "current" to be the highest-numbered metadata file it can read,
// which is a correct answer only while nothing is committing. The test proves
// nothing about safety under concurrent writers, and is not meant to — nor
// anything about R2's list-consistency under concurrent writes, which is a
// separate, still-open manual verification per ARCHITECTURE.md.
func TestCatalogRebuild(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)

	const fills = 500
	const events = 300
	seedBothCases(t, cfg, fills, events)

	// The state to reproduce, read out of the original catalog file rather than
	// out of icelake's opinion of it: which metadata file each table is
	// currently at.
	before := catalogRows(t, cfg.CatalogPath)
	if len(before) != 2 {
		t.Fatalf("the original catalog holds %d tables, want 2: %v", len(before), before)
	}

	duck := openDuckDB(t, bucket)
	fillsBefore := fillFacts(t, duck, before["market.fills"], fills)
	eventsBefore := eventFacts(t, duck, before["archive.events"], events)

	// Total loss of local catalog state. staging.db is deliberately untouched:
	// the two files have opposite disposability rules and this test is about
	// only one of them.
	stagingBefore := fileDigest(t, cfg.StagingPath)
	removeCatalogFiles(t, cfg.CatalogPath)

	report, err := rebuild.Rebuild(ctx, rebuild.RebuildOptions{
		Endpoint:        cfg.Endpoint,
		Bucket:          cfg.Bucket,
		WarehousePrefix: cfg.WarehousePrefix,
		Region:          cfg.Region,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		CatalogPath:     cfg.CatalogPath,
	})
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// Rediscovered, not told about. Nothing above named a namespace or a table
	// to the rebuild call, so the only way these can be here is that listing the
	// prefix found them.
	if got := report.Namespaces; !slices.Equal(got, []string{"archive", "market"}) {
		t.Errorf("discovered namespaces = %v, want exactly [archive market]", got)
	}
	if got := reportedTables(report); !slices.Equal(got, []string{"archive.events", "market.fills"}) {
		t.Errorf("discovered tables = %v, want exactly [archive.events market.fills]", got)
	}
	for _, tbl := range report.Tables {
		if !tbl.Registered {
			t.Errorf("table %s.%s was discovered but not registered", tbl.Namespace, tbl.Table)
		}
		if tbl.Snapshots == 0 {
			t.Errorf("table %s.%s was registered at a metadata file with no snapshots", tbl.Namespace, tbl.Table)
		}
	}

	// The rebuilt rows must name the same metadata files the deleted ones did.
	after := catalogRows(t, cfg.CatalogPath)
	for name, location := range before {
		if after[name] != location {
			t.Errorf("table %s rebuilt at %q, want the %q the deleted catalog named", name, after[name], location)
		}
	}
	if len(after) != len(before) {
		t.Errorf("the rebuilt catalog holds %d tables, want the %d it replaced: %v", len(after), len(before), after)
	}

	// Scenario 1's validation queries again, reached through the rebuilt
	// catalog, asserted identical rather than merely successful.
	if got := fillFacts(t, duck, after["market.fills"], fills); !slices.Equal(got, fillsBefore) {
		t.Errorf("Case A reads back differently after the rebuild:\n got %v\nwant %v", got, fillsBefore)
	}
	if got := eventFacts(t, duck, after["archive.events"], events); !slices.Equal(got, eventsBefore) {
		t.Errorf("Case B reads back differently after the rebuild:\n got %v\nwant %v", got, eventsBefore)
	}

	if got := fileDigest(t, cfg.StagingPath); got != stagingBefore {
		t.Errorf("staging.db changed during a catalog rebuild; it must not be touched at all")
	}
}

// TestCatalogRebuildLeavesTheTableWritable proves the rebuilt catalog is
// equivalent to the one it replaced for writing as well as for reading.
//
// Reading back is the headline claim, but it is only half of "disposable": a
// catalog an operator can read from and never commit through again would still
// have cost them the table. The row rebuild writes has to be the row the next
// commit's compare-and-swap reads, which is a real property of how registration
// is done and not something read-back can show.
func TestCatalogRebuildLeavesTheTableWritable(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)

	const first = 100
	seedFills(t, cfg, 0, first)

	removeCatalogFiles(t, cfg.CatalogPath)
	if _, err := rebuild.Rebuild(ctx, rebuildOptions(cfg)); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// A second run of the ordinary write path, against the rebuilt catalog and
	// the same bucket: it must reconcile the existing table, append to it, and
	// commit.
	const second = 150
	seedFills(t, cfg, first, second)

	rows := catalogRows(t, cfg.CatalogPath)
	duck := openDuckDB(t, bucket)

	var total, distinct int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*), count(DISTINCT sequence_id) FROM %s",
		duck.scan(rows["market.fills"])), &total, &distinct)
	if total != first+second || distinct != first+second {
		t.Errorf("after appending through the rebuilt catalog: %d rows, %d distinct; want %d of each",
			total, distinct, first+second)
	}

	// And the bucket agrees, reached without the catalog at all — the commits
	// after the rebuild landed on the same table rather than starting a new
	// history beside it.
	if got := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")); got != duck.scan(rows["market.fills"]) {
		t.Errorf("the catalog's current metadata file is %q, the bucket's latest is %q", rows["market.fills"], got)
	}
}

// TestRebuildRefusesAnExistingCatalog covers the first of the two safety
// behaviours: the tool never overwrites a catalog database it was not told it
// could replace.
func TestRebuildRefusesAnExistingCatalog(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	seedFills(t, cfg, 0, 50)

	original := fileDigest(t, cfg.CatalogPath)

	t.Run("refused without Replace", func(t *testing.T) {
		_, err := rebuild.Rebuild(ctx, rebuildOptions(cfg))
		if !errors.Is(err, rebuild.ErrCatalogExists) {
			t.Fatalf("Rebuild over an existing catalog returned %v, want ErrCatalogExists", err)
		}
		if got := fileDigest(t, cfg.CatalogPath); got != original {
			t.Error("the refused rebuild changed the catalog database it refused to overwrite")
		}
	})

	t.Run("a dry run reports without touching it", func(t *testing.T) {
		// A dry run writes nothing, so it has nothing to protect and must not
		// refuse: looking before deciding whether to replace is the sequence
		// this makes possible.
		opts := rebuildOptions(cfg)
		opts.DryRun = true

		report, err := rebuild.Rebuild(ctx, opts)
		if err != nil {
			t.Fatalf("dry-run Rebuild: %v", err)
		}
		if got := reportedTables(report); !slices.Equal(got, []string{"market.fills"}) {
			t.Errorf("dry run discovered %v, want [market.fills]", got)
		}
		for _, tbl := range report.Tables {
			if tbl.Registered {
				t.Errorf("dry run reported %s.%s as registered", tbl.Namespace, tbl.Table)
			}
		}
		if got := fileDigest(t, cfg.CatalogPath); got != original {
			t.Error("the dry run changed the catalog database")
		}
	})

	t.Run("Replace rebuilds it", func(t *testing.T) {
		opts := rebuildOptions(cfg)
		opts.Replace = true

		report, err := rebuild.Rebuild(ctx, opts)
		if err != nil {
			t.Fatalf("Rebuild with Replace: %v", err)
		}
		if got := reportedTables(report); !slices.Equal(got, []string{"market.fills"}) {
			t.Errorf("discovered %v, want [market.fills]", got)
		}

		rows := catalogRows(t, cfg.CatalogPath)
		if got := rows["market.fills"]; got != latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills") {
			t.Errorf("the replaced catalog names %q, want the bucket's current metadata file", got)
		}
	})
}

// TestRebuildRefusesAPrefixItCannotExplain covers the second safety behaviour.
//
// Every case here is one where the tool must fail rather than carry on with a
// smaller answer: a table discovery cannot see is the one failure a recovery
// tool must never have, and "succeeded, found nothing" is exactly how it would
// have it.
//
// This is the far end of the name rule TestOpenWriterRefusesUnusableNames
// enforces at the writing end, and the pair is the whole argument for that
// rule's strictness: a name carrying a slash or a dot would put a table where
// the two-level discovery below cannot parse it back out, and the two tests
// together say that icelake will not create such a prefix and will not quietly
// pass over one that exists.
func TestRebuildRefusesAPrefixItCannotExplain(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)

	newOptions := func(t *testing.T, prefix string) rebuild.RebuildOptions {
		t.Helper()
		opts := rebuildOptions(cfg)
		opts.WarehousePrefix = prefix
		opts.CatalogPath = t.TempDir() + "/catalog.db"

		return opts
	}

	assertPrefixError := func(t *testing.T, err error, wantPrefix string, wantKind rebuild.RebuildPrefixKind) {
		t.Helper()

		var prefixErr rebuild.RebuildPrefixError
		if !errors.As(err, &prefixErr) {
			t.Fatalf("Rebuild returned %v, want a RebuildPrefixError", err)
		}
		if prefixErr.Prefix != wantPrefix || prefixErr.Kind != wantKind {
			t.Errorf("refused %q as %q, want %q as %q", prefixErr.Prefix, prefixErr.Kind, wantPrefix, wantKind)
		}
	}

	t.Run("a prefix with nothing under it", func(t *testing.T) {
		_, err := rebuild.Rebuild(ctx, newOptions(t, "no_such_warehouse"))
		assertPrefixError(t, err, "no_such_warehouse", rebuild.RebuildPrefixKindEmpty)
	})

	t.Run("a namespace name the layout forbids", func(t *testing.T) {
		const prefix = "garbage_ns"
		putObject(t, bucket, prefix+"/Bad-Namespace/fills/metadata/00000-abc.metadata.json", []byte("{}"))

		_, err := rebuild.Rebuild(ctx, newOptions(t, prefix))
		assertPrefixError(t, err, "Bad-Namespace", rebuild.RebuildPrefixKindUnparseable)
	})

	t.Run("a table name the layout forbids", func(t *testing.T) {
		const prefix = "garbage_table"
		putObject(t, bucket, prefix+"/market/fills.v2/metadata/00000-abc.metadata.json", []byte("{}"))

		_, err := rebuild.Rebuild(ctx, newOptions(t, prefix))
		assertPrefixError(t, err, "market/fills.v2", rebuild.RebuildPrefixKindUnparseable)
	})

	t.Run("a table with no readable metadata", func(t *testing.T) {
		const prefix = "no_metadata"
		putObject(t, bucket, prefix+"/market/fills/data/deadbeef.parquet", []byte("not parquet either"))
		putObject(t, bucket, prefix+"/market/fills/metadata/00000-abc.metadata.json", []byte("{not json"))

		_, err := rebuild.Rebuild(ctx, newOptions(t, prefix))
		assertPrefixError(t, err, "market/fills", rebuild.RebuildPrefixKindNoMetadata)
	})
}

// TestRebuildReportsABucketItCannotRead covers the third failure a recovery
// tool has to be honest about: the bucket itself did not answer.
//
// Rebuild reads object storage directly — it never opens a table, commits or
// uploads — so a wrong endpoint, a rejected credential or a missing bucket has
// no other path in the taxonomy to arrive on. Reported as anything less
// specific, "I could not read the warehouse" would be indistinguishable from "I
// read the warehouse and it was empty", which is the one confusion this tool
// must never create.
//
// It needs no substrate on purpose: the endpoint below is one nothing answers
// on, which is exactly the condition under test.
func TestRebuildReportsABucketItCannotRead(t *testing.T) {
	opts := rebuild.RebuildOptions{
		Endpoint:        "http://127.0.0.1:9",
		Bucket:          "unreachable",
		WarehousePrefix: "warehouse",
		Region:          "us-east-1",
		AccessKeyID:     "unused",
		SecretAccessKey: "unused",
		CatalogPath:     t.TempDir() + "/catalog.db",
	}

	report, err := rebuild.Rebuild(context.Background(), opts)
	if report != nil {
		t.Error("Rebuild returned a report for a bucket it could not read")
	}

	var storageErr rebuild.RebuildStorageError
	if !errors.As(err, &storageErr) {
		t.Fatalf("Rebuild returned %v (%T), want a rebuild.RebuildStorageError", err, err)
	}
	if storageErr.Bucket != opts.Bucket {
		t.Errorf("RebuildStorageError.Bucket = %q, want %q", storageErr.Bucket, opts.Bucket)
	}
	if storageErr.Err == nil {
		t.Error("RebuildStorageError.Err is nil, want the SDK's own error wrapped and reachable")
	}

	// A failed read leaves no catalog behind: there is nothing to half-build
	// from a warehouse that was never listed.
	if _, err := os.Stat(opts.CatalogPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a catalog exists after a failed rebuild (stat error %v)", err)
	}
}

// TestRebuildTakesTheLatestValidMetadataFile proves the "valid" in "latest
// valid": a metadata file that cannot be read is stepped over, in favour of the
// highest-numbered one that can, and what was stepped over is reported rather
// than swallowed.
func TestRebuildTakesTheLatestValidMetadataFile(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	seedFills(t, cfg, 0, 50)

	real := latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")

	// A file numbered above every real one, whose bytes are not table metadata.
	// Taking "latest" literally would pick this and produce a catalog that
	// cannot load the table at all.
	corrupt := cfg.WarehousePrefix + "/market/fills/metadata/99999-corrupt.metadata.json"
	putObject(t, bucket, corrupt, []byte(`{"this-is": "not table metadata"}`))

	removeCatalogFiles(t, cfg.CatalogPath)
	report, err := rebuild.Rebuild(ctx, rebuildOptions(cfg))
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	if got := report.Tables[0].MetadataLocation; got != real {
		t.Errorf("registered %q, want the latest file that actually parses, %q", got, real)
	}
	if !slices.Contains(report.Skipped, rebuild.RebuildSkip{Key: corrupt, Reason: rebuild.RebuildSkipUnreadableMetadata}) {
		t.Errorf("the unreadable metadata file was passed over without being reported: %v", report.Skipped)
	}
}

// TestRebuildStepsOverWhatProvablyIsNotATable holds the boundary between this
// tool's two ways of reacting to something it did not expect in the warehouse.
//
// Everything it cannot explain fails the whole run, loudly, as a
// RebuildPrefixError — because a prefix that might have been a table and was
// passed over is a table left out of the rebuilt catalog, and the next writer
// would create it again and fork its metadata chain. Two shapes are exempt, and
// they are exempt because they provably are not tables: an object sitting
// directly at the warehouse or namespace level, since a table is always a
// prefix with more beneath it, and a namespace prefix with no child prefixes at
// all, since a table is always one level deeper again. Both are stepped over,
// both are reported so an operator sees them, and neither costs the run.
//
// Nothing here was reported anywhere before. The skip enum's third value is
// held by TestRebuildTakesTheLatestValidMetadataFile above; these two were not,
// so the line between "step over" and "refuse the warehouse" rested entirely on
// the refusal side of it being tested.
func TestRebuildStepsOverWhatProvablyIsNotATable(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	seedFills(t, cfg, 0, 50)

	// One object at the warehouse level, one at a namespace level, and one
	// namespace prefix whose only content is an object rather than a table
	// prefix. All three are what an operator's own tooling leaves behind — a
	// README, a marker file, an export — beside a real warehouse.
	warehouseStray := cfg.WarehousePrefix + "/NOTES.txt"
	namespaceStray := cfg.WarehousePrefix + "/market/NOTES.txt"
	emptyNamespace := cfg.WarehousePrefix + "/leftovers/NOTES.txt"
	for _, key := range []string{warehouseStray, namespaceStray, emptyNamespace} {
		putObject(t, bucket, key, []byte("not a table"))
	}

	removeCatalogFiles(t, cfg.CatalogPath)
	report, err := rebuild.Rebuild(ctx, rebuildOptions(cfg))
	if err != nil {
		t.Fatalf("Rebuild over a warehouse with strays beside it: %v", err)
	}

	// The real table is still found and still registered: stepping over these
	// must not cost the run anything.
	if got := reportedTables(report); !slices.Equal(got, []string{"market.fills"}) {
		t.Errorf("discovered tables = %v, want exactly [market.fills]", got)
	}

	for _, want := range []rebuild.RebuildSkip{
		{Key: warehouseStray, Reason: rebuild.RebuildSkipStrayObject},
		{Key: namespaceStray, Reason: rebuild.RebuildSkipStrayObject},
		{Key: emptyNamespace, Reason: rebuild.RebuildSkipStrayObject},
		{Key: cfg.WarehousePrefix + "/leftovers/", Reason: rebuild.RebuildSkipEmptyNamespace},
	} {
		if !slices.Contains(report.Skipped, want) {
			t.Errorf("%s was passed over without being reported as %s: %v", want.Key, want.Reason, report.Skipped)
		}
	}
}

// TestRebuildLeavesNoHalfBuiltCatalog is the direct test of the interruption
// hazard the temporary-file-and-rename structure exists to remove.
//
// A catalog holding some of its tables and not the rest is worse than no catalog
// at all, and worse in a way that raises no error: the next writer would find a
// missing table absent, create it, and start a second metadata chain for it
// numbered from the beginning — below the real one's sequence, so a later
// rebuild would pick the old chain and everything written since the fork would
// be unreachable. Each subtest here fails a rebuild at a different point and
// asserts the same thing: the catalog path holds the complete old file, or
// nothing, and never a partial one.
func TestRebuildLeavesNoHalfBuiltCatalog(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	cfg := testConfig(t, bucket)
	seedBothCases(t, cfg, 60, 60)

	t.Run("a cancelled run leaves nothing behind", func(t *testing.T) {
		// The bucket scan is the first thing to notice the cancellation, which
		// is exactly the shape of an operator pressing Ctrl-C: the run stops
		// somewhere in the middle and must leave the path as it found it.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		opts := rebuildOptions(cfg)
		opts.CatalogPath = filepath.Join(t.TempDir(), "catalog.db")

		if _, err := rebuild.Rebuild(ctx, opts); err == nil {
			t.Fatal("Rebuild with a cancelled context returned no error")
		}
		assertNoCatalogFiles(t, opts.CatalogPath)
	})

	t.Run("a run that cannot land leaves the previous catalog untouched", func(t *testing.T) {
		// The last step is made to fail, after every row has been written:
		// renaming onto a non-empty directory cannot succeed. What matters is
		// what is left behind — a complete previous catalog, and no leftovers.
		dir := t.TempDir()
		path := filepath.Join(dir, "catalog.db")

		if _, err := rebuild.Rebuild(context.Background(), withCatalogPath(cfg, path)); err != nil {
			t.Fatalf("first Rebuild: %v", err)
		}
		complete := fileDigest(t, path)

		blocked := filepath.Join(dir, "blocked.db")
		if err := os.MkdirAll(filepath.Join(blocked, "occupied"), 0o755); err != nil {
			t.Fatalf("preparing the blocked path: %v", err)
		}
		opts := withCatalogPath(cfg, blocked)
		opts.Replace = true

		_, err := rebuild.Rebuild(context.Background(), opts)
		if err == nil {
			t.Fatal("Rebuild onto a path it cannot replace returned no error")
		}
		// Typed, and typed as a write. This subtest used to assert only that
		// some error came back, which is satisfied by every failure a rebuild
		// has — including the ones that mean the bucket is unreadable or the
		// options are wrong. An operator running this tool has usually already
		// lost the catalog, so "the scan worked and the file would not land" is
		// the one verdict that says try again at another path rather than go
		// and fix the warehouse.
		var catErr rebuild.CatalogError
		if !errors.As(err, &catErr) {
			t.Fatalf("Rebuild returned %v (%T), want a rebuild.CatalogError", err, err)
		}
		// The Kind constant is spelled with the root package's name because
		// rebuild re-exports the error type and not the kind constants; they
		// are the same values either way, which is what the alias buys.
		if catErr.Kind != icelake.CatalogKindWrite {
			t.Errorf("CatalogError.Kind = %q, want %q", catErr.Kind, icelake.CatalogKindWrite)
		}
		if catErr.Path != blocked {
			t.Errorf("CatalogError.Path = %q, want the path it could not write %q", catErr.Path, blocked)
		}
		if _, err := os.Stat(blocked + ".tmp"); !os.IsNotExist(err) {
			t.Errorf("the failed rebuild left its half-built catalog behind (%v)", err)
		}
		if got := fileDigest(t, path); got != complete {
			t.Error("the failed rebuild changed a catalog it was not even writing to")
		}
	})

	// The other end of the same structure. The rename above is the last step;
	// this is the first — clearing whatever a previous run that died before its
	// rename left at the temporary path. SQLite would reopen such a file
	// happily and add today's rows to yesterday's, so a temporary path that
	// cannot be cleared has to stop the run rather than be built on. A
	// non-empty directory there is an ordinary filesystem state, not an
	// injected fault: it is what an operator gets from a mistyped path.
	t.Run("a run whose temporary path cannot be cleared", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "catalog.db")
		if err := os.MkdirAll(filepath.Join(path+".tmp", "occupied"), 0o755); err != nil {
			t.Fatalf("preparing the obstructed temporary path: %v", err)
		}

		_, err := rebuild.Rebuild(context.Background(), withCatalogPath(cfg, path))
		if err == nil {
			t.Fatal("Rebuild over a temporary path it cannot clear returned no error")
		}

		var catErr rebuild.CatalogError
		if !errors.As(err, &catErr) {
			t.Fatalf("Rebuild returned %v (%T), want a rebuild.CatalogError", err, err)
		}
		if catErr.Kind != icelake.CatalogKindWrite {
			t.Errorf("CatalogError.Kind = %q, want %q", catErr.Kind, icelake.CatalogKindWrite)
		}

		// And nothing was written at the destination, which is the claim the
		// refusal exists for: a run that could not start cleanly must not leave
		// a catalog an operator would trust. The obstruction this test built at
		// the temporary path is still there and is deliberately not asserted
		// about — it is the test's own fixture, not a leftover.
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("%s exists after a rebuild that never started (%v)", path, err)
		}
	})

	t.Run("a completed run leaves no temporary file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "catalog.db")

		report, err := rebuild.Rebuild(context.Background(), withCatalogPath(cfg, path))
		if err != nil {
			t.Fatalf("Rebuild: %v", err)
		}
		if len(report.Tables) != 2 {
			t.Fatalf("rebuilt %d tables, want 2", len(report.Tables))
		}
		if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
			t.Errorf("a completed rebuild left a temporary catalog behind (%v)", err)
		}

		// Complete means complete: both tables, not one.
		if rows := catalogRows(t, path); len(rows) != 2 {
			t.Errorf("the rebuilt catalog holds %v, want both tables", rows)
		}
	})
}

// TestRebuildOptionsAreValidatedLoudly asserts the option rules mirror Config's,
// including that every violation is reported at once rather than one per run.
//
// It needs no substrate: validation happens before the bucket or the catalog
// file is touched, which is itself part of the claim.
func TestRebuildOptionsAreValidatedLoudly(t *testing.T) {
	t.Run("everything missing at once", func(t *testing.T) {
		_, err := rebuild.Rebuild(context.Background(), rebuild.RebuildOptions{})

		var cfgErr rebuild.ConfigError
		if !errors.As(err, &cfgErr) {
			t.Fatalf("Rebuild with empty options returned %v, want a ConfigError", err)
		}
		for _, field := range []string{"Endpoint", "Bucket", "WarehousePrefix", "CatalogPath", "Credentials"} {
			if !hasFieldViolation(err, field) {
				t.Errorf("no violation reported for %s; every problem must be reported in one run", field)
			}
		}
	})

	t.Run("both credential forms", func(t *testing.T) {
		_, err := rebuild.Rebuild(context.Background(), rebuild.RebuildOptions{
			Endpoint:        "http://127.0.0.1:1",
			Bucket:          "b",
			WarehousePrefix: "w",
			CatalogPath:     t.TempDir() + "/catalog.db",
			AccessKeyID:     "id",
			SecretAccessKey: "secret",
			Credentials:     staticCredentials("id", "secret"),
		})
		if !hasFieldViolation(err, "Credentials") {
			t.Fatalf("supplying both credential forms returned %v, want a Credentials violation", err)
		}
	})

	// The existence check that runs immediately after validation, and its one
	// arm that is neither "it is there" nor "it is not". A catalog path whose
	// parent is a regular file cannot be statted at all, and the difference
	// between that and "not there" is the difference between refusing and
	// scanning a whole warehouse before failing to write. It is also the only
	// arm of this check with no test: the other two are the refusal and the
	// Replace override in TestRebuildRefusesAnExistingCatalog.
	//
	// No substrate, and that is part of the claim: this runs before a client
	// exists, so the endpoint below is never reached.
	t.Run("a catalog path that cannot be examined", func(t *testing.T) {
		dir := t.TempDir()
		blocking := filepath.Join(dir, "a-file")
		if err := os.WriteFile(blocking, []byte("not a directory"), 0o600); err != nil {
			t.Fatalf("preparing the unexaminable path: %v", err)
		}
		path := filepath.Join(blocking, "catalog.db")

		_, err := rebuild.Rebuild(context.Background(), rebuild.RebuildOptions{
			Endpoint:        "http://127.0.0.1:9",
			Bucket:          "unreachable",
			WarehousePrefix: "warehouse",
			Region:          "us-east-1",
			AccessKeyID:     "id",
			SecretAccessKey: "secret",
			CatalogPath:     path,
		})
		if err == nil {
			t.Fatal("Rebuild with a catalog path it cannot examine returned no error")
		}

		var catErr rebuild.CatalogError
		if !errors.As(err, &catErr) {
			t.Fatalf("Rebuild returned %v (%T), want a rebuild.CatalogError", err, err)
		}
		if catErr.Kind != icelake.CatalogKindOpen {
			t.Errorf("CatalogError.Kind = %q, want %q", catErr.Kind, icelake.CatalogKindOpen)
		}
		if catErr.Path != path {
			t.Errorf("CatalogError.Path = %q, want %q", catErr.Path, path)
		}
		if errors.Is(err, rebuild.ErrCatalogExists) {
			t.Error("a path that could not be examined was reported as one that already holds a catalog")
		}
	})
}

// hasFieldViolation reports whether a joined configuration error carries a
// violation naming one field.
//
// It walks the join rather than reading a message, which is the only form of
// error assertion this suite allows: the "every violation at once" promise is
// per field, so it is asserted per field.
//
// It serves both the rebuild options above and scenario 7's Config matrix, and
// it can: rebuild re-exports the root package's own ConfigError and
// ConfigFieldError as aliases rather than declaring a parallel pair, so these
// are the same two types either way. The tests that assert that re-export is
// real name rebuild's own spelling in their own bodies.
func hasFieldViolation(err error, field string) bool {
	var joined interface{ Unwrap() []error }

	var cfgErr icelake.ConfigError
	if !errors.As(err, &cfgErr) {
		return false
	}
	if !errors.As(cfgErr.Err, &joined) {
		return false
	}
	for _, one := range joined.Unwrap() {
		var fieldErr icelake.ConfigFieldError
		if errors.As(one, &fieldErr) && fieldErr.Field == field {
			return true
		}
	}

	return false
}

// staticCredentials builds the provider form of credentials, which exists here
// only so a test can supply both forms at once and be refused.
func staticCredentials(id, secret string) aws.CredentialsProvider {
	return credentials.NewStaticCredentialsProvider(id, secret, "")
}

// seedBothCases writes Case A and Case B through the real write path and stops
// the writer, which is the state scenario 3 starts from.
func seedBothCases(tb testing.TB, cfg icelake.Config, fills, events int) {
	tb.Helper()

	ctx := context.Background()
	store, err := icelake.Open(ctx, cfg)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}

	fillWriter, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		tb.Fatalf("OpenWriter (fills): %v", err)
	}
	for i := range fills {
		if err := fillWriter.Insert(ctx, makeFill(i)); err != nil {
			tb.Fatalf("Insert fill %d: %v", i, err)
		}
	}

	eventWriter, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		tb.Fatalf("OpenWriter (events): %v", err)
	}
	for i := range events {
		if err := eventWriter.Insert(ctx, makeEvent(i)); err != nil {
			tb.Fatalf("Insert event %d: %v", i, err)
		}
	}

	// Stopped, not merely idle: everything is drained and both databases are
	// closed before the catalog is deleted.
	if err := store.Close(ctx); err != nil {
		tb.Fatalf("Store.Close: %v", err)
	}
}

// seedFills writes one run of Case A records and stops the writer, so a test can
// build up a table across several runs.
func seedFills(tb testing.TB, cfg icelake.Config, from, count int) {
	tb.Helper()

	ctx := context.Background()
	store, err := icelake.Open(ctx, cfg)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		tb.Fatalf("OpenWriter: %v", err)
	}
	for i := from; i < from+count; i++ {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			tb.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := store.Close(ctx); err != nil {
		tb.Fatalf("Store.Close: %v", err)
	}
}

// rebuildOptions is the options struct every test here starts from: the same
// endpoint, bucket, prefix and catalog path the writer used, and nothing else.
// There is deliberately no table list, and no field in which one could be
// supplied.
func rebuildOptions(cfg icelake.Config) rebuild.RebuildOptions {
	return rebuild.RebuildOptions{
		Endpoint:        cfg.Endpoint,
		Bucket:          cfg.Bucket,
		WarehousePrefix: cfg.WarehousePrefix,
		Region:          cfg.Region,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		CatalogPath:     cfg.CatalogPath,
	}
}

// withCatalogPath is rebuildOptions pointed at a different catalog file, for the
// tests that need several rebuilds of one bucket side by side.
func withCatalogPath(cfg icelake.Config, path string) rebuild.RebuildOptions {
	opts := rebuildOptions(cfg)
	opts.CatalogPath = path

	return opts
}

// assertNoCatalogFiles states that a rebuild left nothing at all at a path:
// no catalog, no write-ahead siblings, and no half-built temporary file.
func assertNoCatalogFiles(tb testing.TB, path string) {
	tb.Helper()

	for _, name := range []string{path, path + "-wal", path + "-shm", path + ".tmp", path + ".tmp-wal", path + ".tmp-shm"} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			tb.Errorf("%s exists after a failed rebuild (%v); the path must be left as it was found", name, err)
		}
	}
}

// reportedTables flattens a report's tables into sorted "namespace.table"
// names, which is the form a discovery assertion compares.
func reportedTables(report *rebuild.RebuildReport) []string {
	out := make([]string, 0, len(report.Tables))
	for _, t := range report.Tables {
		out = append(out, t.Namespace+"."+t.Table)
	}
	slices.Sort(out)

	return out
}

// catalogRows reads which metadata file the catalog database currently names for
// each table, by opening the file directly.
//
// Reading the file rather than asking icelake is the point, exactly as it is for
// the staging database elsewhere in this suite: the claim under test is that a
// rebuilt catalog holds the same rows the deleted one did, and asking the
// library would be asking the thing under test to grade itself.
func catalogRows(tb testing.TB, catalogPath string) map[string]string {
	tb.Helper()

	db, err := sql.Open("sqlite", catalogPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		tb.Fatalf("opening the catalog database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			tb.Errorf("closing the catalog database: %v", err)
		}
	}()

	rows, err := db.Query(
		`SELECT table_namespace, table_name, metadata_location FROM iceberg_tables ORDER BY table_namespace, table_name`)
	if err != nil {
		tb.Fatalf("reading catalog rows: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			tb.Errorf("closing the catalog-row query: %v", err)
		}
	}()

	out := map[string]string{}
	for rows.Next() {
		var namespace, name, location string
		if err := rows.Scan(&namespace, &name, &location); err != nil {
			tb.Fatalf("scanning a catalog row: %v", err)
		}
		out[namespace+"."+name] = location
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("reading catalog rows: %v", err)
	}

	return out
}

// removeCatalogFiles deletes the catalog database and the two files SQLite keeps
// beside it, which together are the whole of the local catalog state.
func removeCatalogFiles(tb testing.TB, catalogPath string) {
	tb.Helper()

	for _, name := range []string{catalogPath, catalogPath + "-wal", catalogPath + "-shm"} {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			tb.Fatalf("removing %s: %v", name, err)
		}
	}
	if _, err := os.Stat(catalogPath); !os.IsNotExist(err) {
		tb.Fatalf("the catalog database is still there after being deleted (%v)", err)
	}
}

// fileDigest hashes a file, which is how a test states "this was not touched".
func fileDigest(tb testing.TB, path string) string {
	tb.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("reading %s: %v", path, err)
	}
	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:])
}

// putObject writes one object into the bucket, for the tests that need
// something under the warehouse prefix that icelake itself would never write.
func putObject(tb testing.TB, bucket testsubstrate.Bucket, key string, body []byte) {
	tb.Helper()

	_, err := testsubstrate.NewS3Client(bucket).PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		tb.Fatalf("putting %s: %v", key, err)
	}
}

// fillFacts runs scenario 1's Case A validation queries against one metadata
// location and returns their answers as comparable text.
//
// It asserts what scenario 1 asserts — the counts, the exact DECIMAL strings,
// the real timestamp type — so the "identical results" comparison is between two
// sets of answers that were each independently correct, not merely between two
// runs of the same query.
func fillFacts(tb testing.TB, duck *duck, metadataLocation string, records int) []string {
	tb.Helper()

	table := duck.scan(metadataLocation)
	facts := []string{}

	var total, distinct int
	duck.queryRow(tb, fmt.Sprintf("SELECT count(*), count(DISTINCT sequence_id) FROM %s", table), &total, &distinct)
	if total != records || distinct != records {
		tb.Errorf("Case A: %d rows, %d distinct sequence ids; want %d of each", total, distinct, records)
	}
	facts = append(facts, fmt.Sprintf("rows=%d distinct=%d", total, distinct))
	facts = append(facts, "price="+duck.columnType(tb, table, "price"))
	facts = append(facts, "ts="+duck.columnType(tb, table, "venue_timestamp_ms"))

	for _, i := range []int{0, 237, records - 1} {
		want := makeFill(i)

		var symbol, side, orderID, source, price, quantity string
		var epochMS int64
		duck.queryRow(tb, fmt.Sprintf(`
			SELECT symbol, side, order_id, source,
			       CAST(price AS VARCHAR), CAST(quantity AS VARCHAR),
			       epoch_ms(venue_timestamp_ms)
			FROM %s WHERE sequence_id = %d`, table, i),
			&symbol, &side, &orderID, &source, &price, &quantity, &epochMS)

		if price != decimalString(want.Price, 9) || quantity != decimalString(want.Quantity, 9) {
			tb.Errorf("Case A record %d: price %s quantity %s, want %s and %s exactly",
				i, price, quantity, decimalString(want.Price, 9), decimalString(want.Quantity, 9))
		}
		if epochMS != want.VenueTimeMS {
			tb.Errorf("Case A record %d: timestamp %d ms, want %d", i, epochMS, want.VenueTimeMS)
		}
		facts = append(facts, fmt.Sprintf("%d=%s/%s/%s/%s/%s/%s/%d",
			i, symbol, side, orderID, source, price, quantity, epochMS))
	}

	return facts
}

// eventFacts runs scenario 1's Case B validation queries, including the
// byte-fidelity check on the raw payload column.
func eventFacts(tb testing.TB, duck *duck, metadataLocation string, records int) []string {
	tb.Helper()

	table := duck.scan(metadataLocation)
	facts := []string{}

	var total int
	duck.queryRow(tb, fmt.Sprintf("SELECT count(*) FROM %s", table), &total)
	if total != records {
		tb.Errorf("Case B: %d rows, want %d", total, records)
	}
	facts = append(facts, fmt.Sprintf("rows=%d", total))
	facts = append(facts, "payload="+duck.columnType(tb, table, "payload"))
	facts = append(facts, "ts="+duck.columnType(tb, table, "observed_at_ms"))

	for _, i := range []int{0, 149, records - 1} {
		want := makeEvent(i)

		var sourceID, kind, storedSum string
		var payload []byte
		var epochMS int64
		duck.queryRow(tb, fmt.Sprintf(`
			SELECT source_id, event_kind, payload, payload_sha256, epoch_ms(observed_at_ms)
			FROM %s WHERE sequence_ordinal = %d`, table, i),
			&sourceID, &kind, &payload, &storedSum, &epochMS)

		sum := sha256.Sum256(payload)
		if got := hex.EncodeToString(sum[:]); got != storedSum || got != want.PayloadSHA256 {
			tb.Errorf("Case B record %d: payload hashes to %s, stored column says %s, record said %s",
				i, got, storedSum, want.PayloadSHA256)
		}
		facts = append(facts, fmt.Sprintf("%d=%s/%s/%s/%d", i, sourceID, kind, storedSum, epochMS))
	}

	return facts
}
