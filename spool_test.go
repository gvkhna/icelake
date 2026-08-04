package icelake_test

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// cacheConfig is [testConfig] with the local Parquet spool turned on.
//
// The size bound is deliberately far larger than anything these tests write:
// bucket mode refuses a cache with neither bound set, and the clauses that are
// not about eviction want a sweeper that never evicts anything. The tests that
// are about eviction set their own tiny bounds, per TESTING.md's configuration
// section — a bound that never trips proves nothing, and a bound that trips
// under a test about something else proves the wrong thing.
func cacheConfig(tb testing.TB, bucket testsubstrate.Bucket) icelake.Config {
	tb.Helper()

	cfg := testConfig(tb, bucket)
	cfg.CacheDir = filepath.Join(tb.TempDir(), "cache")
	cfg.CacheMaxBytes = 1 << 30

	return cfg
}

// localOnlyConfig is a configuration for a store that reaches no bucket: no
// endpoint, no credentials, no catalog, and a cache directory that is where the
// data goes rather than where a copy of it is kept.
//
// It takes no [testsubstrate.Bucket] because there is nothing to take one from.
// That is the point of the mode and the reason scenario 12's first half needs no
// container: the storage substrate is absent from the feature, not stood in for.
func localOnlyConfig(tb testing.TB, dir string) icelake.Config {
	tb.Helper()

	return icelake.Config{
		StagingPath:       filepath.Join(dir, "staging.db"),
		CacheDir:          filepath.Join(dir, "cache"),
		LocalOnly:         true,
		FlushMaxRecords:   50,
		FlushMaxBytes:     1 << 20,
		FlushInterval:     time.Hour,
		MinFlushInterval:  time.Nanosecond,
		ZSTDLevel:         1,
		StagingMaxRecords: 100_000,
		StagingMaxBytes:   256 << 20,
	}
}

// bucketConfigOver is the bucket-mode configuration a transition uses: the same
// staging file and the same cache directory the local-only run wrote through,
// with an endpoint, a bucket, a prefix and credentials added.
//
// Nothing else changes, which is the whole shape of the feature under test: a
// person puts four values into the configuration they already had and restarts.
func bucketConfigOver(tb testing.TB, local icelake.Config, bucket testsubstrate.Bucket) icelake.Config {
	tb.Helper()

	cfg := local
	cfg.LocalOnly = false
	cfg.CatalogPath = filepath.Join(filepath.Dir(local.StagingPath), "catalog.db")
	cfg.Endpoint = bucket.Endpoint
	cfg.Bucket = bucket.Bucket
	cfg.WarehousePrefix = "warehouse"
	cfg.Region = bucket.Region
	cfg.AccessKeyID = bucket.AccessKeyID
	cfg.SecretAccessKey = bucket.SecretAccessKey
	cfg.FlushMaxAttempts = 2
	cfg.RetryBaseDelay = 20 * time.Millisecond
	cfg.RetryMaxDelay = 100 * time.Millisecond
	cfg.CacheMaxBytes = 1 << 30

	return cfg
}

// cachedFiles lists the spool's Parquet files under one cache directory, as
// paths relative to it, in sorted order.
//
// It walks the real directory rather than asking icelake what it thinks is
// there: every claim these tests make is about what an operator would find on
// the disk, and asking the thing under test to describe its own output would be
// asking it to grade itself.
func cachedFiles(tb testing.TB, cacheDir string) []string {
	tb.Helper()

	var out []string
	err := filepath.WalkDir(cacheDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		rel, err := filepath.Rel(cacheDir, p)
		if err != nil {
			return err
		}
		out = append(out, rel)

		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		tb.Fatalf("walking the cache directory: %v", err)
	}
	slices.Sort(out)

	return out
}

// spoolRow is one row of spool_files as an outside reader sees it.
type spoolRow struct {
	batchKey  string
	namespace string
	table     string
	schemaFP  string
	byteLen   int64
	createdAt int64
	committed bool
}

// spoolRows reads every spool_files row in creation order, which is the order
// the transition drain walks them in.
func spoolRows(tb testing.TB, stagingPath string) []spoolRow {
	tb.Helper()

	return readSpoolRows(tb, openStagingFile(tb, stagingPath))
}

// readSpoolRows is [spoolRows] against a handle the caller already has.
//
// The split exists for the polling helper below: opening the staging file
// registers a cleanup per call, and a poll that opened it every five
// milliseconds would leave thousands of handles for the test to close at the
// end.
func readSpoolRows(tb testing.TB, db *sql.DB) []spoolRow {
	tb.Helper()

	rows, err := db.Query(
		`SELECT batch_key, namespace, table_name, schema_fp, byte_len, created_at, committed_at
		 FROM spool_files ORDER BY created_at, batch_key`)
	if err != nil {
		tb.Fatalf("reading spool rows: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			tb.Errorf("closing the spool-row query: %v", err)
		}
	}()

	var out []spoolRow
	for rows.Next() {
		var r spoolRow
		var committedAt sql.NullInt64
		if err := rows.Scan(&r.batchKey, &r.namespace, &r.table, &r.schemaFP, &r.byteLen, &r.createdAt, &committedAt); err != nil {
			tb.Fatalf("scanning a spool row: %v", err)
		}
		r.committed = committedAt.Valid
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("reading spool rows: %v", err)
	}

	return out
}

// readSpoolSchemaCount counts the descriptors spool_schemas still holds, which
// is what proves the spool's own prune is separate from staging's.
func readSpoolSchemaCount(tb testing.TB, db *sql.DB) int {
	tb.Helper()

	var n int
	if err := db.QueryRow("SELECT count(*) FROM spool_schemas").Scan(&n); err != nil {
		tb.Fatalf("counting spool descriptors: %v", err)
	}

	return n
}

// waitForSpoolState polls the spool's own tables until cond holds.
//
// A sweep is a goroutine's work, so a test that fires the sweeper's timer and
// then reads has to wait for the sweep rather than assume it. It polls through
// one database handle for the reason [readSpoolRows] gives, and it fails the
// test rather than hanging if the state never arrives.
func waitForSpoolState(tb testing.TB, stagingPath, what string, cond func(rows []spoolRow, descriptors int) bool) {
	tb.Helper()

	db := openStagingFile(tb, stagingPath)
	waitFor(tb, what, func() bool {
		return cond(readSpoolRows(tb, db), readSpoolSchemaCount(tb, db))
	})
}

// uncommittedSpoolFiles counts the spool files that have never reached a bucket
// — the ones an operator has to back up and the sweeper must never touch.
func uncommittedSpoolFiles(tb testing.TB, stagingPath string) int {
	tb.Helper()

	n := 0
	for _, r := range spoolRows(tb, stagingPath) {
		if !r.committed {
			n++
		}
	}

	return n
}

// readCached reads one spool file whole.
func readCached(tb testing.TB, cacheDir, rel string) []byte {
	tb.Helper()

	body, err := os.ReadFile(filepath.Join(cacheDir, rel))
	if err != nil {
		tb.Fatalf("reading the cached file %s: %v", rel, err)
	}

	return body
}

// objectBody fetches one object whole, which is what "byte-identical" has to be
// asserted against: a length check would pass for two files that differ.
func objectBody(tb testing.TB, bucket testsubstrate.Bucket, key string) []byte {
	tb.Helper()

	out, err := testsubstrate.NewS3Client(bucket).GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		tb.Fatalf("fetching %s: %v", key, err)
	}
	defer func() {
		if err := out.Body.Close(); err != nil {
			tb.Errorf("closing %s: %v", key, err)
		}
	}()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		tb.Fatalf("reading %s: %v", key, err)
	}

	return body
}

// cachePathFor is where a batch's file must be, given the object it was
// uploaded as: the same two segments, the same directory, the same name.
func cachePathFor(prefix, key string) string {
	return strings.TrimPrefix(strings.TrimPrefix(key, prefix), "/")
}

// columns returns how many columns a metadata file's current schema has.
func (m metadataDoc) columns(tb testing.TB) int {
	tb.Helper()

	for _, s := range m.Schemas {
		if s.SchemaID == m.CurrentSchemaID {
			return len(s.Fields)
		}
	}
	tb.Fatalf("metadata names current schema %d, which is not among its %d schemas", m.CurrentSchemaID, len(m.Schemas))

	return 0
}

// addedRecords is what each of a metadata file's snapshots says it added, in
// snapshot order.
//
// It is how the transition scenario asserts *ordering* directly rather than
// inferring it from a row count. Give each batch a distinct size and this is the
// order the batches were committed in, stated by the table itself — where a
// total row count would be equally happy with any order at all, which is exactly
// why TESTING.md asks for the direct assertion.
func (m metadataDoc) addedRecords(tb testing.TB) []int {
	tb.Helper()

	out := make([]int, 0, len(m.Snapshots))
	for _, s := range m.Snapshots {
		n, err := strconv.Atoi(s.Summary["added-records"])
		if err != nil {
			tb.Fatalf("snapshot %d has added-records %q, which is not a number", s.SnapshotID, s.Summary["added-records"])
		}
		out = append(out, n)
	}

	return out
}

// schemaWidthPerCommit is the column count of each of a table's metadata files,
// in version order: the table's shape after every commit it has ever taken.
func schemaWidthPerCommit(tb testing.TB, bucket testsubstrate.Bucket, prefix, namespace, table string) []int {
	tb.Helper()

	locations := metadataVersions(tb, bucket, prefix, namespace, table)
	out := make([]int, 0, len(locations))
	for _, loc := range locations {
		out = append(out, readMetadata(tb, bucket, loc).columns(tb))
	}

	return out
}

// sweepClock is icelake's clock with both of the things a retention test needs
// under the test's own control: what time it is, and when the retention sweeper
// runs.
//
// TESTING.md allows exactly one seam and this is it. Retention is the one
// behaviour in this library whose whole subject is elapsed time — how old a file
// is, and how often the sweeper looks — so against a real clock this test would
// either wait a minute per sweep or assert on how long the machine happened to
// take. Everything else is real: the same MinIO container, the same staging
// file, real files on a real disk, and the sweeper's own goroutine doing its own
// work when the timer it asked for fires.
//
// Only the sweeper's timer is ever fired, and knowing which one it is costs
// nothing: [icelake.Open] takes the sweeper's timer on its own goroutine before
// it returns, and a caller cannot open a writer without a store, so the first
// timer this clock ever hands out is necessarily the sweeper's. That is an
// ordering the store guarantees rather than a race this test usually wins —
// [icelake.Open] creates the timer and then starts the goroutine, in that order,
// for exactly this reason. Leaving every writer's own interval timer alone is
// what keeps these tests' batch boundaries exactly where the test put them.
type sweepClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*sweepTimer
}

// newSweepClock starts a stopped clock at a fixed instant.
func newSweepClock() *sweepClock {
	return &sweepClock{now: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)}
}

// Now returns the current time, which only [sweepClock.Advance] changes.
func (c *sweepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

// Advance moves the clock forward by d.
func (c *sweepClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// NewTimer hands back a timer this clock controls and remembers.
func (c *sweepClock) NewTimer(time.Duration) icelake.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &sweepTimer{c: make(chan time.Time)}
	c.timers = append(c.timers, t)

	return t
}

// Sleep returns immediately unless ctx is already done, so a retry backoff costs
// these tests nothing.
func (c *sweepClock) Sleep(ctx context.Context, _ time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Tick delivers one tick to the retention sweeper and returns once the sweeper
// has taken it.
//
// The channel is unbuffered, so "taken it" is a fact rather than a hope: the
// send completes only when the sweeper's own goroutine is sitting in its select,
// which it only reaches after the sweep in front of it has returned. Two
// consequences are what the tests lean on, and they are worth stating because a
// buffered nudge would give neither. A Tick that returns means a sweep has
// *started*. A second Tick that returns means the first sweep has *finished* —
// which is how a test asserts that several sweeps really happened over a cache
// where the correct behaviour is to delete nothing, and therefore where there is
// no other evidence a sweep ran at all.
func (c *sweepClock) Tick(tb testing.TB) {
	tb.Helper()

	c.mu.Lock()
	if len(c.timers) == 0 {
		c.mu.Unlock()
		tb.Fatal("no timer has been created; the store's retention sweeper is the first thing to ask this clock for one")
	}
	sweeper, now := c.timers[0], c.now
	c.mu.Unlock()

	select {
	case sweeper.c <- now:
	case <-time.After(30 * time.Second):
		tb.Fatal("the retention sweeper did not take its tick; it is not running, or a sweep has not returned")
	}
}

// Sweeps delivers n ticks and returns once n-1 sweeps have certainly completed
// and the n-th has certainly started, for the reason [sweepClock.Tick] gives.
func (c *sweepClock) Sweeps(tb testing.TB, n int) {
	tb.Helper()

	for range n {
		c.Tick(tb)
	}
}

// sweepTimer is a timer whose only source of ticks is the test.
type sweepTimer struct{ c chan time.Time }

// C returns the tick channel.
func (t *sweepTimer) C() <-chan time.Time { return t.c }

// Reset re-arms the timer, which for a timer the test delivers to by hand means
// nothing has to happen.
func (t *sweepTimer) Reset(time.Duration) bool { return true }

// Stop halts the timer, which for the same reason means nothing has to happen.
func (t *sweepTimer) Stop() bool { return true }
