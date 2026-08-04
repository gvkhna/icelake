package rebuild

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gvkhna/icelake/internal/catalogdb"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/s3cfg"
)

// RebuildOptions says which bucket to read and which catalog database to write.
//
// The object-storage fields mirror the write path's own configuration exactly,
// including the credential rule: supply either the two key strings or one
// pre-built provider, never both and never neither, and never nothing at all —
// a recovery tool reaching for whatever credentials happen to be in the
// environment is a worse version of the problem that rule exists to prevent.
type RebuildOptions struct {
	// Endpoint is the S3-compatible endpoint's base URL, including scheme.
	// Required.
	Endpoint string

	// Bucket is the bucket the warehouse lives in. Required.
	Bucket string

	// WarehousePrefix is the key prefix discovery starts from. Required.
	//
	// The layout beneath it is <WarehousePrefix>/<namespace>/<table>/, exactly
	// two path segments before each table's own metadata/ and data/
	// directories. Discovery reads that and nothing else, so a prefix that is
	// off by one level finds nothing and says so.
	WarehousePrefix string

	// Region is the region requests are signed for. Optional; empty means
	// "auto", which is what Cloudflare R2 wants.
	Region string

	// AccessKeyID and SecretAccessKey are static credentials. Supply these or
	// Credentials, never both and never neither.
	AccessKeyID     string
	SecretAccessKey string

	// Credentials is a pre-built credential provider, the alternative to the
	// two key strings.
	Credentials aws.CredentialsProvider

	// CatalogPath is the catalog database to write. Required.
	CatalogPath string

	// Replace allows an existing file at CatalogPath to be thrown away and
	// rebuilt. Without it, an existing file is refused with [ErrCatalogExists].
	//
	// Replacing means replacing: a whole new catalog is built from the scan
	// alone and moved onto the path in one rename. Nothing is merged, because a
	// merge would keep rows for tables that are no longer in the bucket, and the
	// bucket is the only authority here. The existing file is untouched until
	// the replacement is complete, so a run that fails leaves the old catalog
	// exactly as it was.
	//
	// Setting it alongside DryRun is accepted and does nothing: a dry run writes
	// nothing, so there is nothing for permission-to-overwrite to permit.
	Replace bool

	// DryRun discovers and reports without writing anything at all.
	//
	// It does not touch CatalogPath, not even to look at it, and therefore never
	// returns [ErrCatalogExists]: the refusal exists to protect a file from
	// being overwritten, and a run that writes nothing has nothing to protect.
	// This is what makes "look first, then decide whether to replace" a usable
	// sequence rather than a refusal an operator has to work around.
	DryRun bool
}

// RebuildTable is one table discovery found and the metadata file it chose for
// it.
type RebuildTable struct {
	// Namespace and Table are the two path segments the table was discovered
	// under, which are also its identifier in the rebuilt catalog.
	Namespace string
	Table     string

	// MetadataLocation is the full s3:// URI of the metadata file recorded as
	// this table's current state.
	MetadataLocation string

	// MetadataSequence is the number parsed out of that file's name — the
	// commit count the table had reached.
	MetadataSequence int

	// Snapshots is how many snapshots that metadata file lists, which is the
	// cheapest way for an operator to sanity-check that a rebuilt table carries
	// the history they expected.
	Snapshots int

	// Registered says whether the row was actually written. It is false for
	// every table of a dry run.
	Registered bool
}

// RebuildSkipReason says why discovery stepped over something instead of
// treating it as a table.
//
// Every value here is a case that provably is not a table. Anything that might
// have been one fails the run instead, as a [RebuildPrefixError].
type RebuildSkipReason string

const (
	// RebuildSkipStrayObject marks an object sitting directly at the warehouse
	// or namespace level. A table is always a prefix with more beneath it, so an
	// object at these levels cannot be one.
	RebuildSkipStrayObject RebuildSkipReason = "stray_object"
	// RebuildSkipEmptyNamespace marks a namespace prefix with no table prefixes
	// under it. A table is always a prefix one level deeper, so there is nothing
	// hiding here.
	RebuildSkipEmptyNamespace RebuildSkipReason = "empty_namespace"
	// RebuildSkipUnreadableMetadata marks a metadata file that was fetched and
	// did not parse as Iceberg table metadata. The scan steps down to the
	// next-highest candidate, which is what "latest valid" means.
	RebuildSkipUnreadableMetadata RebuildSkipReason = "unreadable_metadata"
)

// RebuildSkip is one thing discovery passed over, and why.
type RebuildSkip struct {
	// Key is the object key or prefix that was skipped.
	Key string
	// Reason says which case it was.
	Reason RebuildSkipReason
}

// RebuildReport is what a rebuild found and what it did about it, so an operator
// can eyeball the result before trusting it — and so a dry run has something to
// return.
type RebuildReport struct {
	// Bucket, WarehousePrefix and CatalogPath echo back what was scanned and
	// what was written, so a report is self-describing when it is pasted
	// somewhere else.
	Bucket          string
	WarehousePrefix string
	CatalogPath     string

	// DryRun says whether anything was actually written.
	DryRun bool

	// Namespaces is every namespace that yielded at least one table, sorted.
	Namespaces []string

	// Tables is every table discovered, sorted by namespace then table.
	Tables []RebuildTable

	// Skipped is everything discovery stepped over, in the order it met it.
	Skipped []RebuildSkip
}

// noteStrays records objects found where only prefixes can be tables.
func (r *RebuildReport) noteStrays(keys []string) {
	for _, key := range keys {
		r.Skipped = append(r.Skipped, RebuildSkip{Key: key, Reason: RebuildSkipStrayObject})
	}
}

// String renders the report as the plain block of text the CLI prints, so that
// the command stays a flag parser with no formatting decisions of its own.
func (r *RebuildReport) String() string {
	var b strings.Builder

	headline := "catalog rebuild"
	if r.DryRun {
		headline = "catalog rebuild (dry run — nothing was written)"
	}
	fmt.Fprintf(&b, "%s\n", headline)
	fmt.Fprintf(&b, "  bucket:    %s\n", r.Bucket)
	fmt.Fprintf(&b, "  prefix:    %s\n", r.WarehousePrefix)
	fmt.Fprintf(&b, "  catalog:   %s\n", r.CatalogPath)
	fmt.Fprintf(&b, "  found:     %d namespace(s), %d table(s)\n", len(r.Namespaces), len(r.Tables))

	for _, t := range r.Tables {
		state := "discovered"
		if t.Registered {
			state = "registered"
		}
		fmt.Fprintf(&b, "  %s  %s.%s  seq %d  %d snapshot(s)  %s\n",
			state, t.Namespace, t.Table, t.MetadataSequence, t.Snapshots, t.MetadataLocation)
	}

	for _, s := range r.Skipped {
		fmt.Fprintf(&b, "  skipped    %s  (%s)\n", s.Key, s.Reason)
	}

	return b.String()
}

// Rebuild reconstructs a local Iceberg SQL catalog from the warehouse bucket
// alone.
//
// It lists the configured prefix to discover namespaces and tables from the
// two-segment key layout, picks each table's latest readable metadata file, and
// writes one catalog row per table — from that scan and nothing else. No table
// list, no hint, and no state outside the bucket is consulted or accepted,
// because a side channel saying "these are the tables" is the thing that would
// make the catalog irreplaceable again.
//
// Nothing in the bucket is written, created or rewritten. Every table keeps
// exactly the history it had, and a second run over the same bucket produces the
// same catalog.
//
// Run it with the writer stopped. Rebuild takes "current" to be the
// highest-numbered metadata file it can read, which is only true while nothing
// else is committing.
//
// It refuses, before reading the bucket at all, to overwrite an existing catalog
// database unless [RebuildOptions.Replace] is set, and it fails on any prefix
// under the warehouse that the layout cannot explain rather than passing it over
// — a table discovery cannot see is the one failure this must never have.
//
// A run that fails, or that is interrupted, never leaves a half-built catalog
// behind: the whole thing is assembled in a temporary file beside the
// destination and moved into place by one rename, so the path holds the
// complete new catalog, the old one, or nothing. (A run that dies in the narrow
// window between clearing the old database's write-ahead log and completing the
// rename leaves the old one without that log, which costs nothing: a catalog
// this package writes is always closed cleanly, so the log has already been
// checkpointed into the database and removed.)
func Rebuild(ctx context.Context, opts RebuildOptions) (*RebuildReport, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	// The cheapest refusal first: an operator who meant to keep their catalog
	// should learn that before a scan of the whole warehouse, not after.
	if !opts.DryRun {
		if err := opts.checkCatalogAbsent(); err != nil {
			return nil, err
		}
	}

	settings := s3cfg.Settings{
		Endpoint:        opts.Endpoint,
		Region:          opts.Region,
		AccessKeyID:     opts.AccessKeyID,
		SecretAccessKey: opts.SecretAccessKey,
		Credentials:     opts.Credentials,
	}
	awsCfg, transport := settings.Build()
	defer transport.CloseIdleConnections()

	prefix := strings.Trim(opts.WarehousePrefix, "/")
	d := &discovery{
		client: s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			// Path-style addressing, matching how the writer reaches the same
			// bucket: the endpoint is a host this tool was told to use, with no
			// wildcard DNS behind it.
			o.UsePathStyle = true
		}),
		bucket: opts.Bucket,
		prefix: prefix,
	}

	report := &RebuildReport{
		Bucket:          opts.Bucket,
		WarehousePrefix: prefix,
		CatalogPath:     opts.CatalogPath,
		DryRun:          opts.DryRun,
	}
	if err := d.run(ctx, report); err != nil {
		return nil, err
	}

	if opts.DryRun {
		return report, nil
	}
	if err := register(ctx, opts, report); err != nil {
		return nil, err
	}

	return report, nil
}

// validate collects every problem with the options and returns them as one
// [ConfigError] wrapping the join of one [ConfigFieldError] per violation, so an
// operator fixing a command line learns everything wrong with it in one run.
//
// The credential half is the write path's own check, called rather than copied:
// the rule is deliberately identical, and two implementations of one rule are
// two chances to disagree.
func (o RebuildOptions) validate() error {
	var violations []error

	for _, req := range []struct{ field, value string }{
		{"Endpoint", o.Endpoint},
		{"Bucket", o.Bucket},
		// The trimmed value, so that a prefix of "/" is refused here rather
		// than becoming an empty prefix that lists the whole bucket.
		{"WarehousePrefix", strings.Trim(o.WarehousePrefix, "/")},
		{"CatalogPath", o.CatalogPath},
	} {
		if req.value == "" {
			violations = append(violations, errdef.ConfigFieldError{Field: req.field, Rule: "must not be empty"})
		}
	}

	violations = append(violations, s3cfg.Settings{
		AccessKeyID:     o.AccessKeyID,
		SecretAccessKey: o.SecretAccessKey,
		Credentials:     o.Credentials,
	}.ValidateCredentials()...)

	if len(violations) == 0 {
		return nil
	}

	return errdef.ConfigError{Err: errors.Join(violations...)}
}

// checkCatalogAbsent refuses a catalog database that already exists, unless the
// caller said it may be replaced.
func (o RebuildOptions) checkCatalogAbsent() error {
	_, err := os.Stat(o.CatalogPath)
	switch {
	case err == nil:
		if o.Replace {
			return nil
		}

		return fmt.Errorf("icelake: catalog %q: %w", o.CatalogPath, errdef.ErrCatalogExists)
	case errors.Is(err, fs.ErrNotExist):
		return nil
	default:
		return errdef.NewCatalogError(errdef.CatalogKindOpen, o.CatalogPath,
			"checking whether the catalog database already exists", err)
	}
}

// tempSuffix names the file a rebuild is assembled in before it becomes the
// catalog. It sits beside the destination so that the rename below is a rename
// within one filesystem, which is what makes it atomic.
const tempSuffix = ".tmp"

// register writes the scan into a catalog database.
//
// It creates each namespace through the catalog's own public call and each table
// by recording its current metadata location directly, which is the one thing
// iceberg-go's SQL backend has no method for. Writing the row is all that is
// needed and all that is wanted: the metadata file it names already exists and
// already describes the table completely, so registration is a statement about
// where to look, never a new commit.
//
// The whole catalog is built in a temporary file and moved into place by a
// single rename, and that is a correctness requirement rather than tidiness.
// Each row insert is its own transaction, so a rebuild that died partway — a
// signal, a full disk, a lost SSH session — would otherwise leave a catalog
// holding some of its tables and not the rest. That state is worse than having
// no catalog at all, and worse in a way that is not obvious: the next writer
// would find the missing table absent, create it, and start a *second* metadata
// chain for it, numbered from the beginning. The bucket would then hold two
// chains under one prefix, the new one below the old one's sequence, so a later
// rebuild would pick the *old* chain and every record written since the fork
// would be unreachable — data lost with no error raised anywhere. A rename is
// the one filesystem operation that cannot half-happen, so the partial state is
// made unrepresentable rather than detected. Wrapping the inserts in one
// transaction would close the same hole for a crash but not for a write that
// fails on a full disk after the catalog is already in place; the temporary file
// covers both, so it is the one taken.
func register(ctx context.Context, opts RebuildOptions, report *RebuildReport) error {
	temp := opts.CatalogPath + tempSuffix

	// A previous run that died before its rename may have left one behind, and
	// SQLite would happily reopen it and add today's rows to yesterday's.
	if err := removeDatabase(temp); err != nil {
		return err
	}
	moved := false
	defer func() {
		if !moved {
			_ = removeDatabase(temp)
		}
	}()

	if err := build(ctx, temp, opts, report); err != nil {
		return err
	}

	// The old catalog's write-ahead siblings go first: a rename replaces the
	// database file and nothing else, and a stale log left beside a fresh
	// database is exactly the recovery hazard this whole function exists to
	// avoid. The database file itself is never removed — the rename replaces it
	// in one step, so there is no moment at which the path holds nothing.
	//
	// This does open a narrow window in which a failure leaves the old database
	// without its log. That is stated rather than glossed, and it is harmless
	// here: every catalog this package writes is closed cleanly, which
	// checkpoints the log into the database and deletes it, so in any state this
	// build can produce the sidecars hold nothing the database does not.
	for _, sidecar := range []string{opts.CatalogPath + "-wal", opts.CatalogPath + "-shm"} {
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return errdef.NewCatalogError(errdef.CatalogKindWrite, opts.CatalogPath,
				"removing the previous catalog database's write-ahead log", err)
		}
	}

	if err := os.Rename(temp, opts.CatalogPath); err != nil {
		return errdef.NewCatalogError(errdef.CatalogKindWrite, opts.CatalogPath,
			"moving the rebuilt catalog database into place", err)
	}
	moved = true

	for i := range report.Tables {
		report.Tables[i].Registered = true
	}

	return nil
}

// build assembles a complete catalog database at path.
//
// Closing the database is part of building it, not cleanup: a clean close is
// what checkpoints the write-ahead log into the file and removes it, which is
// what leaves one self-contained file for the caller to rename.
func build(ctx context.Context, path string, opts RebuildOptions, report *RebuildReport) error {
	db, err := catalogdb.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Constructing the catalog is also what creates its two tables in the fresh
	// file: iceberg-go manages that schema entirely itself, and nothing icelake
	// owns ever migrates this database.
	cat, err := catalogdb.New(db, path, iceberg.Properties{
		"warehouse":                         "s3://" + opts.Bucket + "/" + report.WarehousePrefix,
		catalogdb.ForceVirtualAddressingKey: "false",
	})
	if err != nil {
		return err
	}

	for _, ns := range report.Namespaces {
		if err := cat.CreateNamespace(ctx, table.Identifier{ns}, nil); err != nil &&
			!errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			return errdef.NewCatalogError(errdef.CatalogKindWrite, path, "creating namespace "+ns, err)
		}
	}

	for _, t := range report.Tables {
		if err := catalogdb.RegisterTable(ctx, db, path, t.Namespace, t.Table, t.MetadataLocation); err != nil {
			return err
		}
	}

	if err := db.Close(); err != nil {
		return errdef.NewCatalogError(errdef.CatalogKindClose, path,
			"closing the rebuilt catalog database", err)
	}

	return nil
}

// removeDatabase deletes a SQLite database and the two files it keeps beside it
// in WAL mode, treating "it was not there" as success.
func removeDatabase(path string) error {
	for _, name := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return errdef.NewCatalogError(errdef.CatalogKindWrite, path, "removing "+name, err)
		}
	}

	return nil
}
