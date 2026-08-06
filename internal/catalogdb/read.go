package catalogdb

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/gvkhna/icelake/internal/errdef"
)

// MetadataLocation reads the current metadata file recorded for one table.
//
// It opens an existing catalog database with SQLite query_only enabled, so it
// cannot create or change a catalog while a daemon owns it. The catalog schema
// belongs to iceberg-go; this narrow parameterized SELECT is the read-side
// counterpart to RegisterTable's documented INSERT exception.
func MetadataLocation(ctx context.Context, path, namespace, table string) (string, error) {
	if path == "" {
		return "", errdef.NewCatalogError(errdef.CatalogKindOpen, path, "catalog path is empty", nil)
	}
	if strings.ContainsRune(path, '?') {
		return "", errdef.NewCatalogError(errdef.CatalogKindOpen, path, "catalog path contains '?', which the SQLite driver reads as the start of DSN parameters", nil)
	}
	db, err := sql.Open("sqlite", readOnlyDSN(path))
	if err != nil {
		return "", errdef.NewCatalogError(errdef.CatalogKindOpen, path, "opening the catalog database read-only", err)
	}
	defer func() { _ = db.Close() }()

	const query = `SELECT metadata_location FROM iceberg_tables
		WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	var location string
	if err := db.QueryRowContext(ctx, query, Name, namespace, table).Scan(&location); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("catalog has no current metadata for %s.%s", namespace, table)
		}
		return "", errdef.NewCatalogError(errdef.CatalogKindOpen, path, "reading current table metadata", err)
	}
	return location, nil
}

func readOnlyDSN(path string) string {
	q := url.Values{}
	q.Set("mode", "ro")
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", BusyTimeout.Milliseconds()))
	q.Add("_pragma", "query_only(1)")
	return "file:" + path + "?" + q.Encode()
}
