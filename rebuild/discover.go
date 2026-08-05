package rebuild

import (
	"context"
	"errors"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/metascan"
	"github.com/gvkhna/icelake/internal/schemamap"
)

// discovery holds what one rebuild scan needs: a client, a bucket, and the
// warehouse prefix everything hangs under.
type discovery struct {
	client *s3.Client
	bucket string
	// prefix is the warehouse prefix with any leading or trailing slash
	// removed, so that every key this type composes has exactly one separator
	// between segments.
	prefix string
}

// run performs the whole two-level scan and returns what it found.
//
// The shape of the walk is the layout convention itself, which is why the layout
// is load-bearing correctness rather than a naming nicety: listing the warehouse
// prefix with a delimiter yields namespaces, listing one level deeper yields
// that namespace's tables, and nothing else anywhere says what should exist.
// Nothing is read from a caller-supplied list, because a side channel saying
// "these are the tables" is exactly the state this design refuses to keep
// outside the bucket.
func (d *discovery) run(ctx context.Context, report *RebuildReport) error {
	namespaces, strays, err := d.children(ctx, d.prefix+"/")
	if err != nil {
		return err
	}
	report.noteStrays(strays)

	for _, ns := range namespaces {
		if err := schemamap.ValidateName(errdef.NameKindNamespace, ns); err != nil {
			return errdef.RebuildPrefixError{Prefix: ns, Kind: errdef.RebuildPrefixKindUnparseable}
		}

		tables, strays, err := d.children(ctx, d.prefix+"/"+ns+"/")
		if err != nil {
			return err
		}
		report.noteStrays(strays)

		if len(tables) == 0 {
			// Provably not a table: a table is always a prefix one level
			// deeper, so a namespace with no child prefixes cannot be hiding
			// one. Reported rather than silently passed over, because an
			// operator should still get to see it.
			report.Skipped = append(report.Skipped, RebuildSkip{Key: d.prefix + "/" + ns + "/", Reason: RebuildSkipEmptyNamespace})

			continue
		}

		for _, name := range tables {
			if err := schemamap.ValidateName(errdef.NameKindTable, name); err != nil {
				return errdef.RebuildPrefixError{Prefix: ns + "/" + name, Kind: errdef.RebuildPrefixKindUnparseable}
			}

			t, err := d.table(ctx, report, ns, name)
			if err != nil {
				return err
			}
			report.Tables = append(report.Tables, t)
		}

		// Reached only once every table under it registered, because anything
		// less than that returned above: a namespace is in the report when all
		// of it is.
		report.Namespaces = append(report.Namespaces, ns)
	}

	if len(report.Tables) == 0 {
		return errdef.RebuildPrefixError{Prefix: d.prefix, Kind: errdef.RebuildPrefixKindEmpty}
	}

	sort.Strings(report.Namespaces)
	sort.Slice(report.Tables, func(i, j int) bool {
		if report.Tables[i].Namespace != report.Tables[j].Namespace {
			return report.Tables[i].Namespace < report.Tables[j].Namespace
		}

		return report.Tables[i].Table < report.Tables[j].Table
	})

	return nil
}

// table finds one table's current metadata file: the highest-numbered one under
// its metadata/ directory whose bytes actually parse as Iceberg table metadata.
//
// "Latest valid" rather than "latest" is the whole point. A metadata file that
// cannot be read is not a current state anything could be resumed from, so the
// scan steps down to the next-highest and records what it passed over — which is
// what makes a half-written file at the top of the sequence recoverable rather
// than fatal. Running out of candidates is fatal: registering nothing would
// silently leave a table out of the rebuilt catalog.
func (d *discovery) table(ctx context.Context, report *RebuildReport, namespace, name string) (RebuildTable, error) {
	dir := d.prefix + "/" + namespace + "/" + name + "/metadata/"

	keys, err := d.objects(ctx, dir)
	if err != nil {
		return RebuildTable{}, err
	}

	for _, c := range metascan.Candidates(keys) {
		metadata, err := d.metadata(ctx, c.Key)
		if err != nil {
			var unreadable parseFailure
			if errors.As(err, &unreadable) {
				report.Skipped = append(report.Skipped, RebuildSkip{Key: c.Key, Reason: RebuildSkipUnreadableMetadata})

				continue
			}

			return RebuildTable{}, err
		}

		return RebuildTable{
			Namespace:        namespace,
			Table:            name,
			MetadataLocation: "s3://" + path.Join(d.bucket, c.Key),
			MetadataSequence: c.Sequence,
			Snapshots:        len(metadata.Snapshots()),
		}, nil
	}

	return RebuildTable{}, errdef.RebuildPrefixError{
		Prefix: namespace + "/" + name,
		Kind:   errdef.RebuildPrefixKindNoMetadata,
	}
}

// metadata fetches one object and parses it as Iceberg table metadata.
//
// A fetch that fails is a real storage failure and is reported as one. A fetch
// that succeeds and does not parse is a [parseFailure], which the caller treats
// as "not this one, try the next".
func (d *discovery) metadata(ctx context.Context, key string) (table.Metadata, error) {
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, errdef.RebuildStorageError{Bucket: d.bucket, Key: key, Err: err}
	}
	defer func() { _ = out.Body.Close() }()

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, errdef.RebuildStorageError{Bucket: d.bucket, Key: key, Err: err}
	}

	metadata, err := table.ParseMetadataBytes(raw)
	if err != nil {
		return nil, parseFailure{err: err}
	}

	return metadata, nil
}

// children lists one level below a prefix, returning the names of the child
// prefixes and the keys of any objects sitting directly at that level.
//
// The split matters. A child prefix might be a namespace or a table and is
// therefore held to the naming rules; an object at this level provably is not
// either, because both are directories with more beneath them, so it is recorded
// and stepped over rather than being allowed to fail a recovery.
func (d *discovery) children(ctx context.Context, prefix string) (names, strays []string, err error) {
	pager := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(d.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, errdef.RebuildStorageError{Bucket: d.bucket, Key: prefix, Err: err}
		}
		for _, p := range page.CommonPrefixes {
			names = append(names, path.Base(strings.TrimSuffix(aws.ToString(p.Prefix), "/")))
		}
		for _, o := range page.Contents {
			strays = append(strays, aws.ToString(o.Key))
		}
	}

	sort.Strings(names)
	sort.Strings(strays)

	return names, strays, nil
}

// objects lists every key under a prefix, following pagination.
func (d *discovery) objects(ctx context.Context, prefix string) ([]string, error) {
	pager := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucket),
		Prefix: aws.String(prefix),
	})

	var keys []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, errdef.RebuildStorageError{Bucket: d.bucket, Key: prefix, Err: err}
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}

	return keys, nil
}

// parseFailure marks a metadata file that was fetched successfully and did not
// parse. It never crosses the package boundary: it is the internal signal that
// tells the scan to step down to the next candidate instead of giving up.
type parseFailure struct {
	err error
}

// Error implements the error interface.
func (e parseFailure) Error() string { return e.err.Error() }

// Unwrap exposes the parse error underneath.
func (e parseFailure) Unwrap() error { return e.err }
