package icelake

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gvkhna/icelake/internal/catalogdb"
)

// This is the sanctioned package-internal behaviour tier: it holds the
// generator's defence that product SQL never serializes credentials. The public
// command path is covered separately; losing this invariant would leak a secret
// to every saved init file.
func TestDuckDBInitLocalOnlyUsesQualifiedViewsAndDurableCache(t *testing.T) {
	cacheDir := t.TempDir()
	dataDir := filepath.Join(cacheDir, "reddit", "edge_occurrence", "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "part.parquet"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	script, err := DuckDBInit(context.Background(), DuckDBInitOptions{
		CacheDir:  cacheDir,
		LocalOnly: true,
		Tables:    []DuckDBInitTable{{Namespace: "reddit", Name: "edge_occurrence"}},
	})
	if err != nil {
		t.Fatalf("DuckDBInit: %v", err)
	}
	for _, want := range []string{
		"INSTALL cache_httpfs FROM community;",
		"LOAD cache_httpfs;",
		"SET cache_httpfs_cache_directory='" + cacheDir + "/duckdb-cache';",
		"SET cache_httpfs_enable_cache_validation=true;",
		`CREATE SCHEMA IF NOT EXISTS "reddit";`,
		`CREATE OR REPLACE VIEW "reddit"."edge_occurrence"`,
		"read_parquet('" + filepath.Join(cacheDir, "reddit", "edge_occurrence", "data", "*.parquet") + "', union_by_name = true)",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q:\n%s", want, script)
		}
	}
}

// This is the sanctioned package-internal behaviour tier: it pins the
// bucket-script boundary where credentials must remain references rather than
// values, while a real catalog file supplies the metadata location.
func TestDuckDBInitLocalOnlyRefusesAnEmptyTable(t *testing.T) {
	_, err := DuckDBInit(context.Background(), DuckDBInitOptions{
		CacheDir: t.TempDir(), LocalOnly: true,
		Tables: []DuckDBInitTable{{Namespace: "reddit", Name: "empty"}},
	})
	if err == nil {
		t.Fatal("DuckDBInit succeeded for an empty local table")
	}
}

func TestDuckDBInitBucketUsesCatalogMetadataAndCredentialReferences(t *testing.T) {
	catalogPath := filepath.Join(t.TempDir(), "catalog.db")
	db, err := catalogdb.Open(catalogPath)
	if err != nil {
		t.Fatalf("opening catalog: %v", err)
	}
	if _, err := catalogdb.New(db, catalogPath, nil); err != nil {
		t.Fatalf("creating catalog schema: %v", err)
	}
	if err := catalogdb.RegisterTable(context.Background(), db, catalogPath, "reddit", "edge", "s3://bucket/prefix/reddit/edge/metadata/00001-a.metadata.json"); err != nil {
		t.Fatalf("registering table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("closing catalog: %v", err)
	}

	script, err := DuckDBInit(context.Background(), DuckDBInitOptions{
		CacheDir: "/state/cache", CatalogPath: catalogPath, Endpoint: "https://s3.example.test:8443", Bucket: "bucket", Prefix: "prefix", Region: "auto",
		Tables: []DuckDBInitTable{{Namespace: "reddit", Name: "edge"}},
	})
	if err != nil {
		t.Fatalf("DuckDBInit: %v", err)
	}
	for _, want := range []string{
		"SET s3_endpoint='s3.example.test:8443';",
		"SET s3_use_ssl=true;",
		"SET s3_access_key_id=getenv('ICELAKE_ACCESS_KEY_ID');",
		"SET s3_secret_access_key=getenv('ICELAKE_SECRET_ACCESS_KEY');",
		"iceberg_scan('s3://bucket/prefix/reddit/edge/metadata/00001-a.metadata.json')",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "unsafe_enable_version_guessing") {
		t.Error("script enables unsafe metadata guessing")
	}
}
