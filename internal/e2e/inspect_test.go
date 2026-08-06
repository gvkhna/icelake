package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "modernc.org/sqlite"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// object is one thing in the bucket, as an outside observer sees it.
type object struct {
	key  string
	size int64
	etag string
}

// dataFiles lists a table's data files, in key order.
//
// Counting them is how a test asserts that many small batches really became
// many files rather than one, and their sizes and entity tags are what a later
// assertion compares to prove a schema change rewrote none of them.
func dataFiles(tb testing.TB, bucket testsubstrate.Bucket, prefix, namespace, table string) []object {
	tb.Helper()

	return listObjects(tb, bucket, path.Join(prefix, namespace, table, "data")+"/")
}

// listObjects lists everything under one prefix.
func listObjects(tb testing.TB, bucket testsubstrate.Bucket, prefix string) []object {
	tb.Helper()

	out, err := testsubstrate.NewS3Client(bucket).ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket.Bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		tb.Fatalf("listing %s: %v", prefix, err)
	}

	objects := make([]object, 0, len(out.Contents))
	for _, o := range out.Contents {
		objects = append(objects, object{key: aws.ToString(o.Key), size: aws.ToInt64(o.Size), etag: aws.ToString(o.ETag)})
	}

	return objects
}

// stagedRows counts the rows still in the staging database.
//
// It opens the real file rather than asking icelake, because the claims it
// serves are about what is left on disk after a call returned: nothing after a
// clean drain, and everything after a shutdown that ran out of time. Both are
// statements about the file, so the file is what the test reads.
func stagedRows(tb testing.TB, stagingPath string) int {
	tb.Helper()

	db, err := sql.Open("sqlite", stagingPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		tb.Fatalf("opening the staging database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			tb.Errorf("closing the staging database: %v", err)
		}
	}()

	var n int
	if err := db.QueryRow("SELECT count(*) FROM staging").Scan(&n); err != nil {
		tb.Fatalf("counting staged rows: %v", err)
	}

	return n
}

// sealedRows counts the staged rows that carry a batch key: records that were
// sealed into a batch but whose commit never landed.
func sealedRows(tb testing.TB, stagingPath string) int {
	tb.Helper()

	db, err := sql.Open("sqlite", stagingPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		tb.Fatalf("opening the staging database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			tb.Errorf("closing the staging database: %v", err)
		}
	}()

	var n int
	if err := db.QueryRow("SELECT count(*) FROM staging WHERE batch_key IS NOT NULL").Scan(&n); err != nil {
		tb.Fatalf("counting sealed rows: %v", err)
	}

	return n
}

// stagedRow is one row of the staging database as an outside reader sees it,
// with its payload.
//
// Reading the file directly is the point. A crash test's assertions are about
// what a killed process left on disk, and asking icelake what it thinks is
// there would be asking the thing under test to grade itself.
type stagedRow struct {
	id       int64
	batchKey string
	schemaFP string
	payload  []byte
}

// stagedRowsFor reads one table's staged rows in id order, which is both insert
// order and batch order.
func stagedRowsFor(tb testing.TB, stagingPath, namespace, table string) []stagedRow {
	tb.Helper()

	db := openStagingFile(tb, stagingPath)

	rows, err := db.Query(
		`SELECT id, coalesce(batch_key, ''), schema_fp, payload
		 FROM staging WHERE namespace = ? AND table_name = ? ORDER BY id`, namespace, table)
	if err != nil {
		tb.Fatalf("reading staged rows: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			tb.Errorf("closing the staged-row query: %v", err)
		}
	}()

	var out []stagedRow
	for rows.Next() {
		var r stagedRow
		if err := rows.Scan(&r.id, &r.batchKey, &r.schemaFP, &r.payload); err != nil {
			tb.Fatalf("scanning a staged row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("reading staged rows: %v", err)
	}

	return out
}

// sealedBatch reduces a table's staged rows to the one sealed batch among them:
// its stored key and its payloads in batch order.
//
// It fails if the rows do not form exactly one sealed batch, because every
// assertion built on top of this needs to be talking about one batch and would
// otherwise quietly average two.
func sealedBatch(tb testing.TB, rows []stagedRow) (key string, payloads [][]byte) {
	tb.Helper()

	for _, r := range rows {
		if r.batchKey == "" {
			continue
		}
		if key != "" && r.batchKey != key {
			tb.Fatalf("the staged rows carry more than one batch key (%s and %s)", key, r.batchKey)
		}
		key = r.batchKey
		payloads = append(payloads, r.payload)
	}
	if key == "" {
		tb.Fatal("no staged row carries a batch key; nothing was sealed")
	}

	return key, payloads
}

// unsealedPayloads returns the payloads of the staged rows that carry no batch
// key, in id order.
func unsealedPayloads(rows []stagedRow) [][]byte {
	var out [][]byte
	for _, r := range rows {
		if r.batchKey == "" {
			out = append(out, r.payload)
		}
	}

	return out
}

// stagedDescriptor reads back the schema descriptor a fingerprint names, which
// is what lets a test decode or re-encode a staged payload the way the writer
// that staged it would have.
func stagedDescriptor(tb testing.TB, stagingPath, fp string) *canon.Descriptor {
	tb.Helper()

	db := openStagingFile(tb, stagingPath)

	var raw []byte
	if err := db.QueryRow("SELECT descriptor FROM staging_schemas WHERE fp = ?", fp).Scan(&raw); err != nil {
		tb.Fatalf("reading the descriptor for %s: %v", fp, err)
	}
	d, err := canon.ParseDescriptor(raw)
	if err != nil {
		tb.Fatalf("parsing the descriptor for %s: %v", fp, err)
	}

	return d
}

// stagedFingerprints returns every schema fingerprint the staging database
// still carries a descriptor for, in order.
//
// The claim it serves is that a descriptor is dropped once no live row names it
// — and, just as importantly, that one still named by a live row is kept.
func stagedFingerprints(tb testing.TB, stagingPath string) []string {
	tb.Helper()

	db := openStagingFile(tb, stagingPath)

	rows, err := db.Query("SELECT fp FROM staging_schemas ORDER BY fp")
	if err != nil {
		tb.Fatalf("reading schema fingerprints: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			tb.Errorf("closing the fingerprint query: %v", err)
		}
	}()

	var out []string
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			tb.Fatalf("scanning a fingerprint: %v", err)
		}
		out = append(out, fp)
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("reading schema fingerprints: %v", err)
	}

	return out
}

// openStagingFile opens the staging database read-only-ish, for a test that
// wants to see the file rather than icelake's opinion of it, and closes it when
// the test ends.
func openStagingFile(tb testing.TB, stagingPath string) *sql.DB {
	tb.Helper()

	db, err := sql.Open("sqlite", stagingPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		tb.Fatalf("opening the staging database: %v", err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Errorf("closing the staging database: %v", err)
		}
	})

	return db
}

// metadataDoc is the part of an Iceberg metadata file these tests read.
type metadataDoc struct {
	// CurrentSnapshotID is nil, or -1, for a metadata file written before any
	// data was committed — a freshly created table, or one whose schema changed
	// before its first flush.
	CurrentSnapshotID *int64 `json:"current-snapshot-id"`
	Snapshots         []struct {
		SnapshotID int64 `json:"snapshot-id"`
		// Summary is the snapshot's own record of what the commit did. Added at
		// M11 for the transition scenario, which asserts the *order* a backlog
		// was committed in directly: give each batch a distinct size and the
		// added-records of the snapshots, in order, is that order stated by the
		// table itself rather than inferred from a row count that cannot see it.
		Summary map[string]string `json:"summary"`
	} `json:"snapshots"`

	// The schema each metadata file was written under. A commit writes a new
	// metadata file, so walking these in version order is walking the table's
	// shape forward one commit at a time — which is how the transition scenario
	// checks that each backlog file was committed against its own recorded shape
	// and that the reconciliation to the current declaration came after the last
	// of them rather than before the first.
	CurrentSchemaID int `json:"current-schema-id"`
	Schemas         []struct {
		SchemaID int               `json:"schema-id"`
		Fields   []json.RawMessage `json:"fields"`
	} `json:"schemas"`

	// The three creation properties SCHEMA.md's Branch A and ARCHITECTURE.md's
	// 2026-07-30 decision both record as what CreateTable produces when no
	// partition-spec and no sort-order option is passed.
	//
	// The two default-id fields are pointers on purpose. Both are legitimately
	// zero, so a plain int could not tell "the table says 0" from "the key is
	// not in the file at all" — and an assertion that passes because a field
	// vanished is exactly the kind of quiet pass this pair exists to catch.
	FormatVersion      int               `json:"format-version"`
	Properties         map[string]string `json:"properties"`
	DefaultSpecID      *int              `json:"default-spec-id"`
	DefaultSortOrderID *int              `json:"default-sort-order-id"`
	PartitionSpecs     []struct {
		SpecID int `json:"spec-id"`
		// A partition field's shape does not matter here; the claim is that
		// there are none, so the elements are left unparsed.
		Fields []json.RawMessage `json:"fields"`
	} `json:"partition-specs"`
	SortOrders []struct {
		OrderID int               `json:"order-id"`
		Fields  []json.RawMessage `json:"fields"`
	} `json:"sort-orders"`
}

// hasSnapshot reports whether the metadata file names a current snapshot, which
// is what decides whether a reader can scan it as a table at all.
func (m metadataDoc) hasSnapshot() bool {
	return m.CurrentSnapshotID != nil && *m.CurrentSnapshotID >= 0 && len(m.Snapshots) > 0
}

// readMetadata fetches and parses one metadata file out of the bucket.
//
// It reads object storage rather than asking the catalog, for the same reason
// the DuckDB validation does: the question is always what is actually in the
// bucket, and a local catalog file is a cache that may have died mid-sentence.
func readMetadata(tb testing.TB, bucket testsubstrate.Bucket, location string) metadataDoc {
	tb.Helper()

	key := strings.TrimPrefix(location, "s3://"+bucket.Bucket+"/")

	out, err := testsubstrate.NewS3Client(bucket).GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		tb.Fatalf("reading %s: %v", key, err)
	}
	defer func() {
		if err := out.Body.Close(); err != nil {
			tb.Errorf("closing %s: %v", key, err)
		}
	}()

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		tb.Fatalf("reading %s: %v", key, err)
	}

	var doc metadataDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		tb.Fatalf("parsing %s: %v", key, err)
	}

	return doc
}

// snapshotCount reports how many snapshots the table's current metadata file
// lists.
func snapshotCount(tb testing.TB, bucket testsubstrate.Bucket, prefix, namespace, table string) int {
	tb.Helper()

	return len(readMetadata(tb, bucket, latestMetadata(tb, bucket, prefix, namespace, table)).Snapshots)
}

// metadataVersions lists every one of a table's metadata files, oldest first.
//
// Each commit writes a new one, so reading the table at each in turn is reading
// it as it was after each commit — which is how a test compares the order
// snapshots landed in against the order records were written in, without
// needing to parse a manifest list.
func metadataVersions(tb testing.TB, bucket testsubstrate.Bucket, prefix, namespace, table string) []string {
	tb.Helper()

	type versioned struct {
		version int
		key     string
	}

	var files []versioned
	for _, o := range listObjects(tb, bucket, path.Join(prefix, namespace, table, "metadata")+"/") {
		name := path.Base(o.key)
		if !strings.HasSuffix(name, ".metadata.json") {
			continue
		}
		version, err := strconv.Atoi(name[:strings.IndexByte(name, '-')])
		if err != nil {
			continue
		}
		files = append(files, versioned{version: version, key: o.key})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })

	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, "s3://"+path.Join(bucket.Bucket, f.key))
	}

	return out
}

// baseNames reduces a listing to the object names alone, which is what a
// stored-key assertion compares against.
func baseNames(objects []object) []string {
	out := make([]string, 0, len(objects))
	for _, o := range objects {
		out = append(out, path.Base(o.key))
	}

	return out
}

// describe renders a listing compactly for a failure message.
func describe(objects []object) string {
	parts := make([]string, 0, len(objects))
	for _, o := range objects {
		parts = append(parts, path.Base(o.key)+" "+o.etag)
	}

	return strings.Join(parts, ", ")
}

// stagedBatchKeys returns the distinct batch keys the staging database holds,
// which is how a test names the object a replayed batch must upload to without
// asking icelake to tell it.
func stagedBatchKeys(tb testing.TB, stagingPath string) []string {
	tb.Helper()

	db, err := sql.Open("sqlite", stagingPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		tb.Fatalf("opening the staging database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			tb.Errorf("closing the staging database: %v", err)
		}
	}()

	rows, err := db.Query("SELECT DISTINCT batch_key FROM staging WHERE batch_key IS NOT NULL ORDER BY batch_key")
	if err != nil {
		tb.Fatalf("reading batch keys: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			tb.Errorf("closing the batch-key query: %v", err)
		}
	}()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			tb.Fatalf("scanning a batch key: %v", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("reading batch keys: %v", err)
	}

	return keys
}
