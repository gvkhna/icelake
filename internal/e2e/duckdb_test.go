package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// duck is a DuckDB connection pointed at the same object store icelake wrote
// to, used purely as an independent reader.
//
// Querying is explicitly outside icelake's scope, which is exactly why it is
// the right validation tool: nothing icelake owns is involved in reading back
// what it wrote, so a passing assertion is evidence about the table rather than
// about icelake agreeing with itself.
type duck struct {
	db *sql.DB
}

// openDuckDB opens an in-process DuckDB with the iceberg and httpfs extensions
// loaded and the object store configured.
//
// The extensions are installed from DuckDB's own repository the first time a
// machine runs the suite and are cached afterwards. A machine that cannot reach
// it skips rather than fails: the validation is real and required in CI, and a
// developer offline should get a clear skip instead of a mystery failure.
func openDuckDB(tb testing.TB, bucket testsubstrate.Bucket) *duck {
	tb.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		tb.Fatalf("opening duckdb: %v", err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Errorf("closing duckdb: %v", err)
		}
	})

	endpoint, err := url.Parse(bucket.Endpoint)
	if err != nil {
		tb.Fatalf("parsing the endpoint: %v", err)
	}

	setup := []string{
		"INSTALL iceberg",
		"LOAD iceberg",
		"INSTALL httpfs",
		"LOAD httpfs",
		fmt.Sprintf("SET s3_endpoint = '%s'", endpoint.Host),
		fmt.Sprintf("SET s3_region = '%s'", bucket.Region),
		fmt.Sprintf("SET s3_access_key_id = '%s'", bucket.AccessKeyID),
		fmt.Sprintf("SET s3_secret_access_key = '%s'", bucket.SecretAccessKey),
		"SET s3_url_style = 'path'",
		fmt.Sprintf("SET s3_use_ssl = %t", endpoint.Scheme == "https"),
	}
	for _, stmt := range setup {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			if strings.HasPrefix(stmt, "INSTALL") {
				tb.Skipf("cannot install the DuckDB extensions this validation needs (%q): %v", stmt, err)
			}
			tb.Fatalf("duckdb %q: %v", stmt, err)
		}
	}

	return &duck{db: db}
}

// openLocalDuckDB opens an in-process DuckDB with nothing configured beyond
// DuckDB itself.
//
// It exists for scenario 12's local-only half, which reads Parquet files off a
// local disk and has no endpoint, no credentials and no table to point an
// extension at. read_parquet is built in, so nothing is installed and nothing
// can be skipped for want of a network — which suits a test whose whole subject
// is a mode that reaches no network.
func openLocalDuckDB(tb testing.TB) *duck {
	tb.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		tb.Fatalf("opening duckdb: %v", err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Errorf("closing duckdb: %v", err)
		}
	})

	return &duck{db: db}
}

// scan is the table expression every query below reads through: the Iceberg
// table at its current metadata file.
//
// The exact metadata file is named rather than letting DuckDB guess the latest,
// because the writer already knows which one its last commit produced and
// guessing is an extension setting that has changed across versions.
func (d *duck) scan(metadataLocation string) string {
	return fmt.Sprintf("iceberg_scan('%s')", metadataLocation)
}

// queryRow runs a single-row query against the table and scans it into dest.
func (d *duck) queryRow(tb testing.TB, query string, dest ...any) {
	tb.Helper()

	if err := d.db.QueryRowContext(context.Background(), query).Scan(dest...); err != nil {
		tb.Fatalf("duckdb query %q: %v", query, err)
	}
}

// columnType returns the DuckDB type name of one column of a table expression,
// which is how a test asserts that a timestamp really is a timestamp rather
// than an integer a reader has to reinterpret.
func (d *duck) columnType(tb testing.TB, table, column string) string {
	tb.Helper()

	var name string
	d.queryRow(tb, fmt.Sprintf("SELECT typeof(%s) FROM %s LIMIT 1", column, table), &name)

	return name
}

// latestMetadata finds a table's current metadata file by listing the bucket:
// the highest-numbered NNNNN-<uuid>.metadata.json under the table's metadata/
// prefix.
//
// It deliberately does not ask icelake, or icelake's catalog, where the table
// is. The point of the read-back validation is to reach the table the way an
// outside consumer would, from the bucket alone, so the assertion is about what
// was actually written rather than about what the writer believes it wrote.
func latestMetadata(tb testing.TB, bucket testsubstrate.Bucket, prefix, namespace, table string) string {
	tb.Helper()

	dir := path.Join(prefix, namespace, table, "metadata") + "/"
	out, err := testsubstrate.NewS3Client(bucket).ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket.Bucket),
		Prefix: aws.String(dir),
	})
	if err != nil {
		tb.Fatalf("listing %s: %v", dir, err)
	}

	best, bestVersion := "", -1
	for _, o := range out.Contents {
		name := path.Base(*o.Key)
		if !strings.HasSuffix(name, ".metadata.json") {
			continue
		}
		version, err := strconv.Atoi(name[:strings.IndexByte(name, '-')])
		if err != nil {
			continue
		}
		if version > bestVersion {
			best, bestVersion = *o.Key, version
		}
	}
	if best == "" {
		tb.Fatalf("no metadata file under %s", dir)
	}

	return "s3://" + path.Join(bucket.Bucket, best)
}
