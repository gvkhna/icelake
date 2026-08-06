package catalogdb

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// This is the sanctioned package-internal behaviour tier: catalogdb owns this
// local file boundary, and the public duckdb-init path depends on a missing
// catalog remaining missing. Losing it would turn inspection into mutation.
func TestMetadataLocationDoesNotCreateAMissingCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	_, err := MetadataLocation(context.Background(), path, "reddit", "edge")
	if err == nil {
		t.Fatal("MetadataLocation succeeded for a missing catalog")
	}
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if _, statErr := os.Stat(candidate); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("read-only catalog lookup created %s; stat error = %v", candidate, statErr)
		}
	}
}
