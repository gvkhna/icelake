package inspect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gvkhna/icelake/internal/chmirror"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/metascan"
	"github.com/gvkhna/icelake/internal/s3cfg"
	"github.com/gvkhna/icelake/internal/staging"
)

// ErrNotChecked marks a probe that was never attempted because an earlier
// probe it depends on failed. It appears only on [Table.Err]: when the bucket
// probe fails, every table would fail with the same unreachable endpoint, and
// one real failure plus n echoes of it reads as n+1 problems when there is
// one.
var ErrNotChecked = errors.New("not checked: the bucket probe failed")

// Options says what to inspect. The object-storage fields mirror the write
// path's own configuration exactly, including the credential rule: supply
// either the two key strings or one pre-built provider, never both — and in
// bucket mode never neither.
type Options struct {
	// StagingPath is the staging database to summarize. Required. The file not
	// existing is not an error: it is reported as [Staging.Exists] false,
	// because a deployment that has never accepted a record has no staging
	// database yet, and inspection must not be the thing that creates one.
	StagingPath string

	// LocalOnly says the deployment writes to its local cache with no bucket
	// at all. When set, every object-storage field must be empty — the same
	// required-and-forbidden matrix the write path enforces — and the bucket,
	// table and nothing-else probes are skipped rather than failed.
	LocalOnly bool

	// Endpoint is the S3-compatible endpoint's base URL, including scheme.
	// Required in bucket mode.
	Endpoint string

	// Bucket is the bucket the warehouse lives in. Required in bucket mode.
	Bucket string

	// WarehousePrefix is the key prefix each table hangs under. Required in
	// bucket mode.
	WarehousePrefix string

	// Region is the region requests are signed for. Optional; empty means
	// "auto", which is what Cloudflare R2 wants.
	Region string

	// AccessKeyID and SecretAccessKey are static credentials. Supply these or
	// Credentials, never both and, in bucket mode, never neither.
	AccessKeyID     string
	SecretAccessKey string

	// Credentials is a pre-built credential provider, the alternative to the
	// two key strings.
	Credentials aws.CredentialsProvider

	// Tables is which tables to look for under the warehouse prefix — the
	// deployment's declared tables, not a discovery request. Inspection asks
	// "is what was declared actually there", and discovery of what else exists
	// is the rebuild package's job.
	Tables []TableRef

	// ClickHouse turns the mirror probe on. Nil means no mirror is configured
	// and none is probed.
	ClickHouse *ClickHouseOptions
}

// TableRef names one declared table.
type TableRef struct {
	Namespace string
	Table     string
}

// ClickHouseOptions is how to reach the mirror, mirroring the write path's own
// ClickHouse configuration field for field.
type ClickHouseOptions struct {
	// Addr is the host:port of the server's native protocol port.
	Addr string
	// Database is the database the mirror's tables live in. Empty means the
	// mirror's default.
	Database string
	// Username is the user to connect as.
	Username string
	// Password is that user's password, or empty when the user has none.
	Password string
	// TLS says whether to connect over TLS.
	TLS bool
}

// Report is everything one inspection found. Each probe carries its own Err
// rather than failing the whole report, because the failures are the finding:
// a bucket that refuses its credentials is exactly what an operator ran the
// inspection to learn, and learning it must not cost them the staging numbers.
type Report struct {
	// Staging is the staging database's summary. Always present.
	Staging Staging

	// Bucket is the reachability probe's result, and nil in local-only mode.
	Bucket *Bucket

	// Tables holds one entry per requested table, in the order they were
	// requested, and is empty in local-only mode.
	Tables []Table

	// ClickHouse is the mirror probe's result, and nil when none is
	// configured.
	ClickHouse *ClickHouse
}

// Staging summarizes the staging database.
type Staging struct {
	// Path is the file that was summarized.
	Path string

	// Exists says whether the file is there at all. False is a fresh
	// deployment, not a failure: nothing has ever been accepted.
	Exists bool

	// WaitingRecords and WaitingBytes are the accepted records not yet
	// committed to a table — durable on disk, committed by a drain or by the
	// next run's replay.
	WaitingRecords int64
	WaitingBytes   int64

	// QuarantinedRecords are rows that will never be replayed and count
	// against the staging ceiling until an operator removes them.
	QuarantinedRecords int64

	// SpoolBacklogFiles and SpoolBacklogBytes are local Parquet files that
	// have never been uploaded and catalog-committed. In local-only mode that
	// is every file, by definition.
	SpoolBacklogFiles int64
	SpoolBacklogBytes int64

	// Err is why the summary could not be read, and nil when it could.
	Err error
}

// Bucket is the reachability probe: one bounded listing under the warehouse
// prefix, which succeeds only when the endpoint, the credentials and the
// bucket all agree.
type Bucket struct {
	// Bucket and WarehousePrefix echo what was probed.
	Bucket          string
	WarehousePrefix string

	// RoundTrip is how long the listing took.
	RoundTrip time.Duration

	// Err is why the probe failed, and nil when it did not.
	Err error
}

// Table is one declared table's state in the bucket.
type Table struct {
	// Namespace and Name identify the table as it was requested.
	Namespace string
	Name      string

	// Exists says whether the table has any metadata in the bucket. False is
	// not a failure: a declared table that has never committed does not exist
	// yet, and its first commit creates it.
	Exists bool

	// MetadataSequence is the commit count parsed from the current metadata
	// file's name.
	MetadataSequence int

	// Snapshots is how many snapshots that metadata file lists.
	Snapshots int

	// LastCommitAt is the current metadata file's own last-updated stamp.
	LastCommitAt time.Time

	// Err is why the table could not be read — [ErrNotChecked] when the bucket
	// probe already failed — and nil otherwise.
	Err error
}

// ClickHouse is the mirror probe: one ping.
type ClickHouse struct {
	// Addr echoes what was probed.
	Addr string

	// RoundTrip is how long the ping took.
	RoundTrip time.Duration

	// Err is why the probe failed, and nil when it did not.
	Err error
}

// Inspect runs every configured probe and reports what it found.
//
// The returned error is only ever about the options: an impossible
// configuration is refused before anything is touched, exactly as the write
// path refuses it. Every failure past that point — a staging database that
// cannot be read, a bucket that refuses its credentials, a mirror that does
// not answer — lands in the corresponding [Report] field's Err, because those
// failures are what the caller is here to learn about.
//
// Nothing is written anywhere, whatever happens. See the package comment for
// what that rests on, probe by probe.
func Inspect(ctx context.Context, opts Options) (*Report, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	report := &Report{Staging: summarizeStaging(ctx, opts.StagingPath)}
	if opts.LocalOnly {
		return report, nil
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

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		// Path-style addressing, matching how the writer reaches the same
		// bucket: the endpoint is a host this tool was told to use, with no
		// wildcard DNS behind it.
		o.UsePathStyle = true
	})
	prefix := strings.Trim(opts.WarehousePrefix, "/")

	report.Bucket = probeBucket(ctx, client, opts.Bucket, prefix)
	for _, ref := range opts.Tables {
		report.Tables = append(report.Tables, probeTable(ctx, client, opts.Bucket, prefix, ref, report.Bucket.Err))
	}

	if opts.ClickHouse != nil {
		report.ClickHouse = probeClickHouse(ctx, *opts.ClickHouse)
	}

	return report, nil
}

// validate refuses an impossible Options with one [errdef.ConfigError] joining
// every violation, matching the write path's own rule that an operator learns
// everything wrong in one run.
func (o Options) validate() error {
	var violations []error
	refuse := func(field, rule string) {
		violations = append(violations, errdef.ConfigFieldError{Field: field, Rule: rule})
	}

	if o.StagingPath == "" {
		refuse("StagingPath", "must not be empty")
	}

	if o.LocalOnly {
		for _, f := range []struct{ field, value string }{
			{"Endpoint", o.Endpoint},
			{"Bucket", o.Bucket},
			{"WarehousePrefix", o.WarehousePrefix},
			{"AccessKeyID", o.AccessKeyID},
			{"SecretAccessKey", o.SecretAccessKey},
		} {
			if f.value != "" {
				refuse(f.field, "must be empty when LocalOnly is set")
			}
		}
		if o.Credentials != nil {
			refuse("Credentials", "must be nil when LocalOnly is set")
		}
	} else {
		for _, f := range []struct{ field, value string }{
			{"Endpoint", o.Endpoint},
			{"Bucket", o.Bucket},
			// The trimmed value, so that a prefix of "/" is refused here
			// rather than becoming an empty prefix that lists the whole
			// bucket.
			{"WarehousePrefix", strings.Trim(o.WarehousePrefix, "/")},
		} {
			if f.value == "" {
				refuse(f.field, "must not be empty")
			}
		}
		violations = append(violations, s3cfg.Settings{
			AccessKeyID:     o.AccessKeyID,
			SecretAccessKey: o.SecretAccessKey,
			Credentials:     o.Credentials,
		}.ValidateCredentials()...)
	}

	if o.ClickHouse != nil && o.ClickHouse.Addr == "" {
		refuse("ClickHouse.Addr", "must not be empty when ClickHouse is set")
	}

	if len(violations) == 0 {
		return nil
	}

	return errdef.ConfigError{Err: errors.Join(violations...)}
}

// summarizeStaging stats the file first, because a deployment that has never
// accepted a record has no staging database, and asking SQLite would create
// exactly the file whose absence is the answer.
func summarizeStaging(ctx context.Context, path string) Staging {
	st := Staging{Path: path}

	_, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return st
	case err != nil:
		st.Err = err

		return st
	}
	st.Exists = true

	sum, err := staging.Summarize(ctx, path)
	if err != nil {
		st.Err = err

		return st
	}
	st.WaitingRecords = sum.WaitingRecords
	st.WaitingBytes = sum.WaitingBytes
	st.QuarantinedRecords = sum.QuarantinedRecords
	st.SpoolBacklogFiles = sum.SpoolBacklogFiles
	st.SpoolBacklogBytes = sum.SpoolBacklogBytes

	return st
}

// probeBucket asks for at most one key under the warehouse prefix. Success
// proves the endpoint answers, the credentials sign, and the bucket exists;
// what the listing contains is deliberately not judged, because an empty
// warehouse is a deployment that has not committed yet, not a broken one.
func probeBucket(ctx context.Context, client *s3.Client, bucket, prefix string) *Bucket {
	b := &Bucket{Bucket: bucket, WarehousePrefix: prefix}

	start := time.Now()
	_, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix + "/"),
		MaxKeys: aws.Int32(1),
	})
	b.RoundTrip = time.Since(start)
	if err != nil {
		b.Err = fmt.Errorf("listing %q in bucket %q: %w", prefix, bucket, err)
	}

	return b
}

// probeTable reads one declared table's current state: its metadata directory
// listed, the current file picked by the shared metascan rule, and that file's
// tip read back.
func probeTable(ctx context.Context, client *s3.Client, bucket, prefix string, ref TableRef, bucketErr error) Table {
	t := Table{Namespace: ref.Namespace, Name: ref.Table}
	if bucketErr != nil {
		t.Err = ErrNotChecked

		return t
	}

	dir := prefix + "/" + ref.Namespace + "/" + ref.Table + "/metadata/"
	keys, err := listAll(ctx, client, bucket, dir)
	if err != nil {
		t.Err = err

		return t
	}

	candidates := metascan.Candidates(keys)
	if len(candidates) == 0 {
		// No metadata files means the table has never committed. That holds
		// even if other keys sit under the directory: a table without metadata
		// is not a table yet, and the first commit will write file 00000.
		return t
	}

	// Latest valid rather than latest, exactly as rebuild takes it: a file
	// that does not parse is not a state anything could be resumed from, so
	// the scan steps down to the next-highest.
	unreadable := 0
	for _, c := range candidates {
		metadata, err := fetchMetadata(ctx, client, bucket, c.Key)
		if err != nil {
			var storage storageFailure
			if errors.As(err, &storage) {
				t.Err = err

				return t
			}
			unreadable++

			continue
		}

		t.Exists = true
		t.MetadataSequence = c.Sequence
		t.Snapshots = len(metadata.Snapshots())
		t.LastCommitAt = time.UnixMilli(metadata.LastUpdatedMillis()).UTC()

		return t
	}

	t.Err = fmt.Errorf("%d metadata file(s) under %s, none of which parse as Iceberg table metadata", unreadable, dir)

	return t
}

// probeClickHouse opens the mirror's own client construction and pings once.
func probeClickHouse(ctx context.Context, opts ClickHouseOptions) *ClickHouse {
	ch := &ClickHouse{Addr: opts.Addr}

	conn, err := chmirror.Open(chmirror.Settings{
		Addr:     opts.Addr,
		Database: opts.Database,
		Username: opts.Username,
		Password: opts.Password,
		TLS:      opts.TLS,
	})
	if err != nil {
		ch.Err = err

		return ch
	}
	defer func() { _ = conn.Close() }()

	start := time.Now()
	ch.Err = conn.Ping(ctx)
	ch.RoundTrip = time.Since(start)

	return ch
}

// storageFailure marks a fetch that failed before there were bytes to parse,
// which is a real storage problem rather than an unreadable candidate.
type storageFailure struct {
	err error
}

// Error implements the error interface.
func (e storageFailure) Error() string { return e.err.Error() }

// Unwrap exposes the fetch error underneath.
func (e storageFailure) Unwrap() error { return e.err }

// fetchMetadata reads one object and parses it as Iceberg table metadata. A
// failure to fetch comes back as a [storageFailure]; a failure to parse comes
// back bare, which the caller treats as "not this one, try the next".
func fetchMetadata(ctx context.Context, client *s3.Client, bucket, key string) (table.Metadata, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, storageFailure{err: fmt.Errorf("reading %q: %w", key, err)}
	}
	defer func() { _ = out.Body.Close() }()

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, storageFailure{err: fmt.Errorf("reading %q: %w", key, err)}
	}

	return table.ParseMetadataBytes(raw)
}

// listAll lists every key under a prefix, following pagination.
func listAll(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	var keys []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing %q: %w", prefix, err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}

	return keys, nil
}
