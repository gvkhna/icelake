package icelake_test

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TestCacheHTTPFSDemonstration is TESTING.md's scenario 5: DuckDB's
// cache_httpfs community extension, pointed at the same bucket icelake wrote
// to, serving a repeated query from local disk instead of re-fetching from the
// bucket.
//
// It demonstrates a downstream consumption pattern rather than icelake's own
// correctness — the pattern ARCHITECTURE.md's query-side section recommends for
// a long exploratory session, where re-fetching every object on every query is
// the thing worth avoiding. icelake's own scope stops at the bucket; what is
// being shown here is that the tables it writes are ordinary enough that a
// stock caching layer in front of a stock reader just works on them.
//
// The measurement is deliberately taken outside DuckDB. Every request the
// reader makes passes through a counting proxy in front of the same real MinIO
// every other test writes to, so "was it served from cache" is answered by
// requests that did not reach the bucket, not by a claim the extension makes
// about itself. And the same pair of runs is measured through a reader with no
// cache extension at all, which is what says the drop came from the cache
// rather than from anything DuckDB does on its own.
//
// It skips rather than fails when the community extension cannot be installed —
// that needs DuckDB's community repository, and a machine without it should get
// a clear skip rather than a mystery failure in a test that is a demonstration
// by design.
func TestCacheHTTPFSDemonstration(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	// Enough records to make several data files, so the repeated query has real
	// objects to fetch rather than one tiny one.
	const records = 200

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	metadata := latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")

	// Every read from here on goes through the counter. The store behind it is
	// the same MinIO, unchanged.
	counter := startCountingProxy(t, bucket.Endpoint)
	proxied := bucket
	proxied.Endpoint = counter.Endpoint

	query := fmt.Sprintf("SELECT count(*), sum(sequence_id) FROM iceberg_scan('%s')", metadata)

	// Each run gets its own reader, which is the comparison that means
	// something. DuckDB's own caching already absorbs most of a repeat inside
	// one connection, but it is memory-only and dies with the process — the
	// property this extension adds, and the one the exploratory-session pattern
	// actually needs, is a cache on local disk that a later session still has.
	// So "twice" here is twice across two readers, not twice down one.
	plainFirst := countRequests(t, counter, openDuckDB(t, proxied), query, records)
	plainSecond := countRequests(t, counter, openDuckDB(t, proxied), query, records)

	// The same two runs, through readers with cache_httpfs on, caching into this test's own
	// directory rather than the extension's default under /tmp — a stale cache
	// from another run would be indistinguishable from the property under test.
	cacheDir := t.TempDir()
	cachedFirst := countRequests(t, counter, openCachedDuckDB(t, proxied, cacheDir), query, records)
	onDisk := cacheBytes(t, cacheDir)
	cachedSecond := countRequests(t, counter, openCachedDuckDB(t, proxied, cacheDir), query, records)

	t.Logf("requests to the bucket for the same query, each run in a fresh reader: without the cache extension %d then %d; with it %d then %d, having left %d bytes in the local cache directory",
		plainFirst, plainSecond, cachedFirst, cachedSecond, onDisk)

	// The claim, in the only form that is actually about the bucket: the second
	// reader issued fewer requests than the first one did, and fewer than an
	// uncached second reader — so the difference is the cache on local disk and
	// not something DuckDB would have done anyway.
	if cachedSecond >= cachedFirst {
		t.Errorf("the second cached reader issued %d requests and the first issued %d; the repeat was not served from cache",
			cachedSecond, cachedFirst)
	}
	if cachedSecond >= plainSecond {
		t.Errorf("the second cached reader issued %d requests and an uncached one issued %d; the cache made no difference",
			cachedSecond, plainSecond)
	}
	if onDisk == 0 {
		t.Error("the cache directory is empty after a query ran through the extension; nothing was cached on local disk")
	}
}

// openCachedDuckDB opens a reader with the cache_httpfs community extension
// enabled and its on-disk cache pointed at dir.
//
// It skips the test when the extension cannot be installed. That needs DuckDB's
// community repository over the network, and this is a demonstration of a
// downstream pattern rather than a test of icelake's own correctness — a
// machine without that repository should say so plainly rather than fail.
func openCachedDuckDB(tb testing.TB, bucket testsubstrate.Bucket, dir string) *duck {
	tb.Helper()

	d := openDuckDB(tb, bucket)
	for _, stmt := range []string{
		"INSTALL cache_httpfs FROM community",
		"LOAD cache_httpfs",
		fmt.Sprintf("SET cache_httpfs_cache_directory = '%s'", dir),
	} {
		if _, err := d.db.ExecContext(context.Background(), stmt); err != nil {
			tb.Skipf("cannot install the cache_httpfs community extension this demonstration needs (%q): %v", stmt, err)
		}
	}

	return d
}

// cacheBytes reports how many bytes the extension has written under its cache
// directory — the "local disk" half of "served from local disk cache", read
// from the filesystem rather than from the extension's own accounting.
func cacheBytes(tb testing.TB, dir string) int64 {
	tb.Helper()

	var total int64
	err := filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()

		return nil
	})
	if err != nil {
		tb.Fatalf("reading the cache directory: %v", err)
	}

	return total
}

// countRequests runs one query, checks it returned what the table holds, and
// reports how many requests reached the bucket while it ran.
func countRequests(tb testing.TB, counter *countingProxy, d *duck, query string, records int) int {
	tb.Helper()

	before := counter.Count()

	var total, sum int64
	d.queryRow(tb, query, &total, &sum)

	// The same answer every time, whichever reader and whichever run: a cache
	// that changed the result would be a far more interesting failure than one
	// that saved no requests.
	want := int64(records) * int64(records-1) / 2
	if total != int64(records) || sum != want {
		tb.Fatalf("the query returned (%d rows, sequence sum %d), want (%d, %d)", total, sum, records, want)
	}

	return counter.Count() - before
}

// countingProxy forwards every request to the object store and counts them.
//
// It is not a mock and stands in for nothing: the store on the other side is
// the same real MinIO, and every byte is passed through untouched. What it adds
// is the one observation this demonstration needs and neither DuckDB nor the
// bucket offers — how many requests actually left the reader — measured outside
// the thing whose caching is being demonstrated.
//
// Signed requests survive it for the same reason they survive the slow proxy
// the failure tests use: the signature covers the Host header the client sent,
// and this proxy leaves that header alone, so the store verifies the same host
// the client signed.
type countingProxy struct {
	// Endpoint is the base URL to point a reader at.
	Endpoint string

	server *httptest.Server

	mu       sync.Mutex
	requests int
}

// startCountingProxy starts a counting proxy in front of an endpoint and stops
// it when the test ends.
func startCountingProxy(tb testing.TB, endpoint string) *countingProxy {
	tb.Helper()

	target, err := url.Parse(endpoint)
	if err != nil {
		tb.Fatalf("parsing the endpoint: %v", err)
	}

	p := &countingProxy{}
	reverse := httputil.NewSingleHostReverseProxy(target)
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.requests++
		p.mu.Unlock()

		reverse.ServeHTTP(w, r)
	}))
	p.Endpoint = p.server.URL
	tb.Cleanup(p.server.Close)

	return p
}

// Count returns how many requests have reached the store through this proxy.
func (p *countingProxy) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.requests
}
