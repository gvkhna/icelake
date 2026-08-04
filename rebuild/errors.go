package rebuild

import "github.com/gvkhna/icelake/internal/errdef"

// The errors this package returns are declared in the same internal leaf
// package as the rest of icelake's taxonomy and re-exported here as aliases. An
// alias is the same type, not a copy, so errors.As against the name below
// matches an error constructed anywhere inside the library — and a caller who
// imports only this package never needs the root package to match one.
//
// Configuration violations reuse the root package's ConfigError and
// ConfigFieldError, deliberately: the credential rules here are identical to
// Config's, and a second error type saying the same thing about the same fields
// would be a second thing to keep in step.

// ErrCatalogExists reports that [Rebuild] was pointed at a catalog database
// that already exists without [RebuildOptions.Replace] being set.
//
// Rebuild reconstructs the whole file, so running it over a live catalog would
// throw away rows nothing asked it to throw away. A dry run never trips this,
// because a dry run writes nothing and so has nothing to protect.
var ErrCatalogExists = errdef.ErrCatalogExists

// RebuildPrefixKind says which way a prefix defeated rebuild, because the
// operator's next action differs for each.
type RebuildPrefixKind = errdef.RebuildPrefixKind

const (
	// RebuildPrefixKindUnparseable marks a prefix discovered under the
	// warehouse that is not a valid namespace or table name.
	RebuildPrefixKindUnparseable = errdef.RebuildPrefixKindUnparseable
	// RebuildPrefixKindNoMetadata marks a discovered table prefix with no
	// metadata file that could be read as Iceberg table metadata.
	RebuildPrefixKindNoMetadata = errdef.RebuildPrefixKindNoMetadata
	// RebuildPrefixKindEmpty marks a warehouse prefix that yielded no tables at
	// all — almost always a mistyped prefix or the wrong bucket.
	RebuildPrefixKindEmpty = errdef.RebuildPrefixKindEmpty
)

// RebuildPrefixError reports a prefix rebuild refuses: the configured warehouse
// prefix itself, or one discovered beneath it. Prefix is the offending prefix
// and Kind says which refusal it was.
type RebuildPrefixError = errdef.RebuildPrefixError

// RebuildStorageError reports a read against the warehouse bucket that rebuild
// could not complete — a listing or a metadata fetch that failed. Bucket and Key
// name what was being read, and Err carries the wrapped SDK error.
type RebuildStorageError = errdef.RebuildStorageError

// CatalogError reports a failure of the catalog database itself: it would not
// open, a row would not go in, or the rebuilt file would not move into place.
// Path names the file and Err carries the wrapped driver or filesystem error.
// Rebuild returns it for everything that goes wrong on the local side of the
// job, as opposed to the bucket side.
type CatalogError = errdef.CatalogError

// ConfigError reports invalid [RebuildOptions], wrapping the join of one
// [ConfigFieldError] per violation so that an operator fixing a command line
// learns everything wrong with it in one run. It is returned before the bucket
// or the catalog file is touched.
type ConfigError = errdef.ConfigError

// ConfigFieldError reports one option field that fails one rule. Field is the
// field name as a caller spells it in Go.
type ConfigFieldError = errdef.ConfigFieldError
