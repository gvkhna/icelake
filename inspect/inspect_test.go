package inspect_test

// The inspect package's own suite. Fixtures are written through icelake's
// public API — a real store, a real writer, a real bucket in a MinIO container
// — because what the report claims about a deployment is only worth asserting
// against a deployment something actually wrote.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/inspect"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// fillsDocument declares the two tables the bucket tests ask about: one that
// gets written, one that never does.
const fillsDocument = `{"tables": [
  {"namespace": "market", "table": "fills", "fields": [
    {"name": "symbol", "type": "string", "fieldid": 1},
    {"name": "qty",    "type": "long",   "fieldid": 2}
  ]},
  {"namespace": "market", "table": "quotes", "fields": [
    {"name": "symbol", "type": "string", "fieldid": 1},
    {"name": "px",     "type": "double", "fieldid": 2}
  ]}
]}`

// parseTables parses the document and hands back its declarations.
func parseTables(t *testing.T) []icelake.DynamicTableConfig {
	t.Helper()

	tables, err := icelake.ParseSchemaDocument([]byte(fillsDocument))
	if err != nil {
		t.Fatalf("ParseSchemaDocument: %v", err)
	}

	return tables
}

// refs reduces declarations to the identities Inspect takes.
func refs(tables []icelake.DynamicTableConfig) []inspect.TableRef {
	out := make([]inspect.TableRef, len(tables))
	for i, tc := range tables {
		out[i] = inspect.TableRef{Namespace: tc.Namespace, Table: tc.Table}
	}

	return out
}

// TestInspectRefusesImpossibleOptions is the options matrix: every refusal is
// about the options alone, decided before anything is touched, and names the
// field.
func TestInspectRefusesImpossibleOptions(t *testing.T) {
	cases := []struct {
		name     string
		opts     inspect.Options
		mentions []string
	}{
		{"an empty staging path", inspect.Options{LocalOnly: true}, []string{"StagingPath"}},
		{"local-only alongside a bucket", inspect.Options{
			StagingPath: "staging.db", LocalOnly: true, Endpoint: "http://127.0.0.1:9", Bucket: "b",
		}, []string{"Endpoint", "Bucket"}},
		{"bucket mode with nothing configured", inspect.Options{
			StagingPath: "staging.db",
		}, []string{"Endpoint", "Bucket", "WarehousePrefix"}},
		{"a prefix that is only slashes", inspect.Options{
			StagingPath: "staging.db", Endpoint: "http://127.0.0.1:9", Bucket: "b", WarehousePrefix: "/",
			AccessKeyID: "k", SecretAccessKey: "s",
		}, []string{"WarehousePrefix"}},
		{"a mirror with no address", inspect.Options{
			StagingPath: "staging.db", LocalOnly: true, ClickHouse: &inspect.ClickHouseOptions{},
		}, []string{"ClickHouse.Addr"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			report, err := inspect.Inspect(t.Context(), c.opts)
			if err == nil {
				t.Fatalf("Inspect accepted the options, report %+v", report)
			}
			for _, want := range c.mentions {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not mention %s:\n%v", want, err)
				}
			}
		})
	}
}

// TestInspectLocalOnlyReadsTheStagingTruthAndCreatesNothing walks a local-only
// deployment through its first three states — never started, mid-write with
// records staged, cleanly drained — and asserts the report tells each one
// straight. The first state doubles as the writes-nothing proof: asking about
// a staging database that does not exist must not be the thing that creates
// it.
func TestInspectLocalOnlyReadsTheStagingTruthAndCreatesNothing(t *testing.T) {
	dir := t.TempDir()
	stagingPath := filepath.Join(dir, "staging.db")
	opts := inspect.Options{StagingPath: stagingPath, LocalOnly: true}

	report, err := inspect.Inspect(t.Context(), opts)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if report.Staging.Exists || report.Staging.Err != nil {
		t.Fatalf("a fresh deployment reported %+v, want not-exists with no error", report.Staging)
	}
	if _, err := os.Stat(stagingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspecting a fresh deployment created the staging database: %v", err)
	}
	if report.Bucket != nil || len(report.Tables) != 0 || report.ClickHouse != nil {
		t.Errorf("local-only report probed things it has no configuration for: %+v", report)
	}

	// A store with thresholds no three records can trip, so the records stay
	// staged and waiting while it is open.
	store, err := icelake.Open(t.Context(), icelake.Config{
		StagingPath:       stagingPath,
		CacheDir:          filepath.Join(dir, "cache"),
		LocalOnly:         true,
		FlushInterval:     time.Hour,
		FlushMaxRecords:   1000,
		FlushMaxBytes:     1 << 20,
		StagingMaxRecords: 100_000,
		StagingMaxBytes:   1 << 30,
		ZSTDLevel:         1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	writer, err := icelake.OpenDynamicWriter(t.Context(), store, parseTables(t)[0])
	if err != nil {
		t.Fatalf("OpenDynamicWriter: %v", err)
	}
	for _, line := range []string{
		`{"symbol":"A","qty":1}`,
		`{"symbol":"B","qty":2}`,
		`{"symbol":"C","qty":3}`,
	} {
		if err := writer.InsertJSON(t.Context(), []byte(line)); err != nil {
			t.Fatalf("InsertJSON: %v", err)
		}
	}

	report, err = inspect.Inspect(t.Context(), opts)
	if err != nil {
		t.Fatalf("Inspect beside the open store: %v", err)
	}
	if got := report.Staging; !got.Exists || got.Err != nil || got.WaitingRecords != 3 || got.WaitingBytes <= 0 {
		t.Fatalf("mid-write staging = %+v, want 3 waiting records with positive bytes", got)
	}
	if report.Staging.QuarantinedRecords != 0 {
		t.Errorf("QuarantinedRecords = %d, want 0", report.Staging.QuarantinedRecords)
	}

	// Close drains: in local-only the spool file on disk is the whole of what
	// a confirmed flush means, so afterwards nothing waits and the files are,
	// by definition, backlog a bucket has never confirmed.
	if err := store.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	report, err = inspect.Inspect(t.Context(), opts)
	if err != nil {
		t.Fatalf("Inspect after the drain: %v", err)
	}
	if got := report.Staging; got.WaitingRecords != 0 || got.SpoolBacklogFiles == 0 {
		t.Fatalf("drained staging = %+v, want 0 waiting and a non-empty spool backlog", got)
	}
}

// TestInspectAgainstABucketTellsAWrittenTableFromAnUnwrittenOne is the bucket
// half against real object storage: the reachability probe passes, a table
// that committed reports its history's tip, and a declared table that never
// committed is "not yet", not a failure.
func TestInspectAgainstABucketTellsAWrittenTableFromAnUnwrittenOne(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	dir := t.TempDir()
	stagingPath := filepath.Join(dir, "staging.db")
	tables := parseTables(t)

	store, err := icelake.Open(t.Context(), icelake.Config{
		StagingPath:       stagingPath,
		CatalogPath:       filepath.Join(dir, "catalog.db"),
		Endpoint:          bucket.Endpoint,
		Bucket:            bucket.Bucket,
		WarehousePrefix:   "warehouse",
		Region:            bucket.Region,
		AccessKeyID:       bucket.AccessKeyID,
		SecretAccessKey:   bucket.SecretAccessKey,
		FlushInterval:     time.Hour,
		MinFlushInterval:  time.Nanosecond,
		FlushMaxRecords:   1000,
		FlushMaxBytes:     1 << 20,
		StagingMaxRecords: 100_000,
		StagingMaxBytes:   1 << 30,
		ZSTDLevel:         1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Only fills is written; quotes stays declared-and-absent.
	writer, err := icelake.OpenDynamicWriter(t.Context(), store, tables[0])
	if err != nil {
		t.Fatalf("OpenDynamicWriter: %v", err)
	}
	if err := writer.InsertJSON(t.Context(), []byte(`{"symbol":"A","qty":1}`)); err != nil {
		t.Fatalf("InsertJSON: %v", err)
	}
	if err := store.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	opts := inspect.Options{
		StagingPath:     stagingPath,
		Endpoint:        bucket.Endpoint,
		Bucket:          bucket.Bucket,
		WarehousePrefix: "warehouse",
		Region:          bucket.Region,
		AccessKeyID:     bucket.AccessKeyID,
		SecretAccessKey: bucket.SecretAccessKey,
		Tables:          refs(tables),
	}
	report, err := inspect.Inspect(t.Context(), opts)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}

	if report.Bucket == nil || report.Bucket.Err != nil {
		t.Fatalf("bucket probe = %+v, want a pass", report.Bucket)
	}
	if report.Bucket.RoundTrip <= 0 {
		t.Errorf("bucket round trip = %v, want positive", report.Bucket.RoundTrip)
	}

	if len(report.Tables) != 2 {
		t.Fatalf("got %d table reports, want 2", len(report.Tables))
	}
	fills, quotes := report.Tables[0], report.Tables[1]

	if fills.Err != nil || !fills.Exists {
		t.Fatalf("the written table reported %+v, want exists with no error", fills)
	}
	if fills.Snapshots < 1 {
		t.Errorf("the written table reports %d snapshots, want at least 1", fills.Snapshots)
	}
	if fills.MetadataSequence < 1 {
		t.Errorf("the written table reports metadata sequence %d, want at least 1: creation is 0 and the commit moved it", fills.MetadataSequence)
	}
	if age := time.Since(fills.LastCommitAt); age < 0 || age > time.Hour {
		t.Errorf("the written table's last commit is %v old (%v), want recent", age, fills.LastCommitAt)
	}

	if quotes.Err != nil || quotes.Exists {
		t.Errorf("the never-written table reported %+v, want not-exists with no error", quotes)
	}

	// After a clean close, nothing waits.
	if got := report.Staging; !got.Exists || got.WaitingRecords != 0 {
		t.Errorf("staging after a clean close = %+v, want present with 0 waiting", got)
	}
}

// TestInspectReportsARefusedBucketWithoutEchoingItPerTable pins the dependency
// rule: when the bucket probe fails, the failure is reported once, and every
// table says "not checked" rather than restating the same unreachable
// endpoint as its own finding.
func TestInspectReportsARefusedBucketWithoutEchoingItPerTable(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	dir := t.TempDir()
	report, err := inspect.Inspect(t.Context(), inspect.Options{
		StagingPath:     filepath.Join(dir, "staging.db"),
		Endpoint:        bucket.Endpoint,
		Bucket:          bucket.Bucket,
		WarehousePrefix: "warehouse",
		Region:          bucket.Region,
		AccessKeyID:     bucket.AccessKeyID,
		SecretAccessKey: "not-the-secret",
		Tables:          []inspect.TableRef{{Namespace: "market", Table: "fills"}},
	})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}

	if report.Bucket == nil || report.Bucket.Err == nil {
		t.Fatalf("bucket probe with the wrong secret = %+v, want a failure", report.Bucket)
	}
	if len(report.Tables) != 1 || !errors.Is(report.Tables[0].Err, inspect.ErrNotChecked) {
		t.Fatalf("table reports = %+v, want exactly one, marked not-checked", report.Tables)
	}
}
