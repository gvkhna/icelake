// Package catalogdb opens the local Iceberg SQL catalog database and writes the
// rows that say which metadata file is current for a table.
//
// It exists because two entry points must agree about this file exactly: the
// write path, which commits through iceberg-go's SQL catalog, and the
// catalog-rebuild tool, which reconstructs the same file from object storage
// alone. The catalog name below is part of every row's primary key, so a rebuild
// that wrote a different one would produce a database the writer could open and
// find empty — the failure mode most likely to look like success. One constant,
// one open path, one insert.
//
// The catalog's own schema is managed entirely by iceberg-go: it creates its two
// tables itself, and nothing icelake owns ever migrates this file.
package catalogdb

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"

	// The pure-Go SQLite driver, registered under the name "sqlite" — the same
	// driver the staging store opens, so the two databases share one driver and
	// icelake's runtime still needs no cgo.
	_ "modernc.org/sqlite"

	"github.com/gvkhna/icelake/internal/errdef"
)

// Name is the catalog name icelake's rows are written under. It is internal to
// the catalog database and never appears in a table identifier a caller
// supplies, but it is part of every row's primary key, so every writer of this
// file must use this one value.
const Name = "icelake"

// BusyTimeout is how long a statement waits for the catalog database's write
// lock before failing. Two tables' flush workers can commit at the same moment,
// and a commit is a short write transaction, so the contention this absorbs is
// real but always brief.
const BusyTimeout = 5 * time.Second

// ForceVirtualAddressingKey is the one storage property icelake records on a
// table it creates.
//
// It says "address this bucket path-style", which is true of every endpoint
// icelake writes to, and it has to be a property rather than something icelake
// passes at call time because iceberg-go decides addressing from properties
// alone. Everything else about reaching the bucket — the endpoint, the region,
// the credentials, the disabled checksum headers — travels on the context
// instead, deliberately: an endpoint is environment configuration, and baking it
// into a table's durable metadata would mean a table remembering where it was
// first written and trying to go back there.
const ForceVirtualAddressingKey = "s3.force-virtual-addressing"

// registerTableSQL writes one row of iceberg-go's own iceberg_tables model.
//
// The columns are spelled out here because the model type itself is unexported,
// so the only way to write a row is to write the SQL. That is a real coupling to
// a library's internal storage shape, and it is confined to this one statement
// so that an upgrade which changed the shape breaks in one obvious place rather
// than in several subtle ones.
const registerTableSQL = `INSERT INTO iceberg_tables
	(catalog_name, table_namespace, table_name, iceberg_type, metadata_location, previous_metadata_location)
	VALUES (?, ?, ?, ?, ?, NULL)`

// Open opens (creating it if absent) the catalog database.
//
// The pragmas travel as DSN parameters for the same reason the staging store's
// do: database/sql hands out whichever pooled connection is free, so a pragma
// issued once after opening would apply to one connection and no others. Every
// transaction takes its write lock at BEGIN, because two tables committing at
// the same moment is an ordinary event here and a deferred transaction that
// upgrades mid-way is the one contention case a busy timeout cannot absorb.
func Open(path string) (*sql.DB, error) {
	if strings.ContainsRune(path, '?') {
		return nil, errdef.NewCatalogError(errdef.CatalogKindOpen, path,
			"catalog path contains '?', which the SQLite driver reads as the start of DSN parameters", nil)
	}

	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", BusyTimeout.Milliseconds()))
	q.Add("_pragma", "journal_mode(WAL)")
	q.Set("_txlock", "immediate")

	db, err := sql.Open("sqlite", path+"?"+q.Encode())
	if err != nil {
		return nil, errdef.NewCatalogError(errdef.CatalogKindOpen, path, "opening the catalog database", err)
	}

	return db, nil
}

// New builds iceberg-go's SQL catalog over an already-open database.
//
// The direct constructor is used rather than the registry route because it
// leaves icelake owning the *sql.DB and therefore its lifecycle; the registry
// route opens the database itself. The catalog manages its own schema — it
// creates its own tables on this call — so nothing icelake owns ever migrates
// this file.
func New(db *sql.DB, path string, props iceberg.Properties) (*sqlcat.Catalog, error) {
	cat, err := sqlcat.NewCatalog(Name, db, sqlcat.SQLite, props)
	if err != nil {
		return nil, errdef.NewCatalogError(errdef.CatalogKindOpen, path, "preparing the SQL catalog", err)
	}

	return cat, nil
}

// RegisterTable records an already-existing table's current metadata file in the
// catalog, without reading or writing anything in the bucket.
//
// This is the one thing the catalog-rebuild path needs and iceberg-go's SQL
// catalog does not offer. Three of its four public routes to a catalog row are
// wrong here for the same reason: CreateTable and CommitTable both *write* a new
// metadata file, which would discard the table's history and add a commit to a
// table nobody is writing to, and the RegisterTable method other backends expose
// (Glue, Hive, REST) simply does not exist on the SQL backend in the pinned
// version. What is left is what the design document already describes — "then
// repopulates the SQL catalog's rows from that scan" — so the row is written
// directly, in the shape the catalog reads it back in.
//
// previous_metadata_location is deliberately left null. It is the catalog's
// record of what the last commit replaced, and a rebuild did not replace
// anything; inventing a value would claim a history this database never saw. The
// compare-and-swap the next commit performs reads metadata_location only, so a
// null previous location costs the writer nothing.
func RegisterTable(ctx context.Context, db *sql.DB, path, namespace, name, metadataLocation string) error {
	_, err := db.ExecContext(ctx, registerTableSQL,
		Name, namespace, name, sqlcat.TableType, metadataLocation)
	if err != nil {
		return errdef.NewCatalogError(errdef.CatalogKindWrite, path,
			fmt.Sprintf("recording table %s.%s at %s", namespace, name, metadataLocation), err)
	}

	return nil
}
