package staging

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
)

// SpoolEntry is one row of spool_files: a Parquet file that exists in the cache
// directory, and what is known about it.
//
// It deliberately carries no path. Where a spool file lives is the cache
// directory's layout, which internal/spool owns; what this package owns is the
// durable record that the file exists, what shape it was written under, and
// whether it has reached a bucket.
type SpoolEntry struct {
	// BatchKey is the batch's content hash, which is also the file's name and
	// the object's.
	BatchKey string
	// Namespace and Table are the file's table identity.
	Namespace string
	Table     string
	// SchemaFP fingerprints the shape the file's rows were written under. It is
	// not necessarily any shape the current process declares.
	SchemaFP string
	// ByteLen is the file's size, recorded so the retention size bound needs no
	// walk of the directory.
	ByteLen int64
	// CreatedAt is when the file was written. The age bound and the drain order
	// both read it.
	CreatedAt time.Time
	// CommittedAt is when the file was confirmed uploaded and committed, and is
	// the zero time until both have happened. A file with a zero CommittedAt is
	// the one thing in the cache that is not disposable.
	//
	// There is deliberately no Committed() helper beside it. Nothing in this
	// library ever asks an entry whether it is committed: the two callers are
	// the drain, which is handed only uncommitted files by [Store.SpoolBacklog],
	// and retention, which is handed only committed ones by
	// [Store.SpoolEvictable]. Both distinctions are made by the statement rather
	// than by the caller, which is the whole of the protection for a file no
	// bucket has confirmed, and a convenience that let a caller filter in Go
	// would be the first step back toward doing it there.
	CommittedAt time.Time
}

// SpoolRecord is one spool file being recorded, after its bytes are already on
// disk.
//
// It carries the descriptor rather than a bare fingerprint for the same reason
// [AppendRow] does: the fingerprint is derived from the descriptor here, so a
// caller cannot record a file under a shape that is not the shape its rows were
// written with, and the two rows land in one transaction, so no recorded file
// can exist whose shape is missing. The transition drain reads that shape years
// later, with no struct and no document in the process that describes it.
type SpoolRecord struct {
	// Namespace and Table are the file's table identity, already validated one
	// layer up.
	Namespace string
	Table     string
	// BatchKey is the batch's content hash.
	BatchKey string
	// Descriptor is the shape the file's rows were written under.
	Descriptor *canon.Descriptor
	// ByteLen is the file's size on disk.
	ByteLen int64
	// CreatedAt comes from the caller, and therefore from icelake's injected
	// clock, so that no read of time hides inside this package.
	CreatedAt time.Time
}

// RecordSpoolFile records a file that is already on disk, together with the
// shape it was written under, in one transaction.
//
// Both rows tolerate a conflict, because a batch can be written more than once
// under one name: the name is the batch key, and a sealed batch keeps its stored
// key rather than recomputing it. Usually that rewrite is byte-identical, which
// is what makes a crashed flush safe to retry. It is not always: a batch sealed
// before a schema-changing deploy is re-encoded onto the current shape on
// replay, under the key it was sealed with, so the same file name can come back
// carrying a different shape and a different size. Those two columns are
// therefore updated on conflict and the file's own descriptor is re-recorded,
// because the transition drain reads exactly them to decide what to create the
// table as and what to reconcile it to.
//
// What the conflict never touches is committed_at: a rewrite must not un-commit
// a file a previous run already got into a table. The statement does not mention
// that column at all, which makes it unreachable from here rather than merely
// left alone.
func (s *Store) RecordSpoolFile(ctx context.Context, rec SpoolRecord) error {
	if rec.Namespace == "" || rec.Table == "" {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "spool record has no table identity", nil)
	}
	if rec.BatchKey == "" {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "spool record has no batch key", nil)
	}
	if rec.Descriptor == nil {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "spool record has no descriptor for the shape its file was written under", nil)
	}

	err := s.tx(ctx, func(q *Queries) error {
		if err := q.UpsertSpoolSchema(ctx, UpsertSpoolSchemaParams{
			Fp:         rec.Descriptor.Fingerprint(),
			Descriptor: rec.Descriptor.Bytes(),
		}); err != nil {
			return err
		}
		return q.UpsertSpoolFile(ctx, UpsertSpoolFileParams{
			BatchKey:  rec.BatchKey,
			Namespace: rec.Namespace,
			TableName: rec.Table,
			SchemaFp:  rec.Descriptor.Fingerprint(),
			ByteLen:   rec.ByteLen,
			CreatedAt: rec.CreatedAt.UnixMilli(),
		})
	})
	if err != nil {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path,
			fmt.Sprintf("recording spool file %s", rec.BatchKey), err)
	}

	return nil
}

// MarkSpoolCommitted records that a file has been uploaded and catalog-committed
// and is therefore evictable from now on.
//
// Marking a file that is already marked, or one no longer recorded at all,
// matches no row and is not an error. Both are states a crash between the commit
// and this call leaves behind, and re-running the mark is how they are recovered.
func (s *Store) MarkSpoolCommitted(ctx context.Context, batchKey string, at time.Time) error {
	if batchKey == "" {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "marking a spool file committed needs a batch key", nil)
	}
	if _, err := s.q.MarkSpoolFileCommitted(ctx, MarkSpoolFileCommittedParams{
		CommittedAt: sql.NullInt64{Int64: at.UnixMilli(), Valid: true},
		BatchKey:    batchKey,
	}); err != nil {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path,
			fmt.Sprintf("marking spool file %s committed", batchKey), err)
	}

	return nil
}

// SpoolSchema returns the shape a spool file was written under.
//
// A fingerprint with no recorded descriptor is a corrupt store rather than a
// normal miss — the descriptor is written in the same transaction as the file's
// own row — so it fails loudly rather than handing back a nil shape for a caller
// to trip over. Without it a backlog file could not be committed at all: its
// descriptor is the only statement of its shape that outlives the process that
// wrote it.
func (s *Store) SpoolSchema(ctx context.Context, fp string) (*canon.Descriptor, error) {
	raw, err := s.q.GetSpoolSchema(ctx, fp)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errdef.NewStagingError(errdef.StagingKindState, s.path,
			fmt.Sprintf("no recorded descriptor for spool schema fingerprint %s", fp), err)
	}
	if err != nil {
		return nil, errdef.NewStagingError(errdef.StagingKindRead, s.path,
			fmt.Sprintf("reading the descriptor for spool schema fingerprint %s", fp), err)
	}

	return canon.ParseDescriptor(raw)
}

// SpoolBacklog lists one table's files that have never reached a bucket, in the
// order the transition drain must walk them: creation order, oldest first.
func (s *Store) SpoolBacklog(ctx context.Context, namespace, table string) ([]SpoolEntry, error) {
	rows, err := s.q.SpoolBacklogForTable(ctx, SpoolBacklogForTableParams{Namespace: namespace, TableName: table})
	if err != nil {
		return nil, errdef.NewStagingError(errdef.StagingKindRead, s.path,
			fmt.Sprintf("reading the spool backlog for %s.%s", namespace, table), err)
	}

	out := make([]SpoolEntry, 0, len(rows))
	for _, r := range rows {
		out = append(out, SpoolEntry{
			BatchKey:  r.BatchKey,
			Namespace: namespace,
			Table:     table,
			SchemaFP:  r.SchemaFp,
			ByteLen:   r.ByteLen,
			CreatedAt: time.UnixMilli(r.CreatedAt).UTC(),
		})
	}

	return out, nil
}

// SpoolBacklogTables names every table holding files that have never reached a
// bucket.
//
// It exists for the drain of a table no writer reopens: such a table has no
// declaration anywhere in the process, so nothing else would ever ask about it.
func (s *Store) SpoolBacklogTables(ctx context.Context) ([]TableID, error) {
	rows, err := s.q.SpoolTablesWithBacklog(ctx)
	if err != nil {
		return nil, errdef.NewStagingError(errdef.StagingKindRead, s.path, "reading the tables with a spool backlog", err)
	}

	out := make([]TableID, 0, len(rows))
	for _, r := range rows {
		out = append(out, TableID{Namespace: r.Namespace, Table: r.TableName})
	}

	return out, nil
}

// SpoolCommittedBytes totals the files retention may evict.
//
// The bound is over committed files only, which is why this counts only them: a
// backlog the bucket has never confirmed is not merely protected from eviction,
// it is not counted toward a bound that evicting it is the only way to satisfy.
func (s *Store) SpoolCommittedBytes(ctx context.Context) (int64, error) {
	raw, err := s.q.SpoolCommittedBytes(ctx)
	if err != nil {
		return 0, errdef.NewStagingError(errdef.StagingKindRead, s.path, "totalling the committed spool files", err)
	}
	bytes, err := asInt64(raw)
	if err != nil {
		return 0, errdef.NewStagingError(errdef.StagingKindRead, s.path, "totalling the committed spool files", err)
	}

	return bytes, nil
}

// SpoolEvictable lists the files retention may delete, oldest first.
//
// Everything else in the cache is protected by this statement's own shape rather
// than by a rule the caller applies afterwards: a file that has not been
// uploaded and committed is not visible here at all. That is the design and not
// an implementation detail — a size cap enforced by a loop that skips
// uncommitted files is one refactor away from being a size cap that deletes the
// only copy of a day's records.
func (s *Store) SpoolEvictable(ctx context.Context) ([]SpoolEntry, error) {
	rows, err := s.q.SpoolEvictable(ctx)
	if err != nil {
		return nil, errdef.NewStagingError(errdef.StagingKindRead, s.path, "reading the evictable spool files", err)
	}

	out := make([]SpoolEntry, 0, len(rows))
	for _, r := range rows {
		out = append(out, SpoolEntry{
			BatchKey:    r.BatchKey,
			Namespace:   r.Namespace,
			Table:       r.TableName,
			ByteLen:     r.ByteLen,
			CreatedAt:   time.UnixMilli(r.CreatedAt).UTC(),
			CommittedAt: time.UnixMilli(r.CommittedAt.Int64).UTC(),
		})
	}

	return out, nil
}

// DeleteSpoolFile forgets an evicted file. Deleting a row that is already gone is
// not an error, for the same reason pruning an already-pruned staged row is not.
func (s *Store) DeleteSpoolFile(ctx context.Context, batchKey string) error {
	if _, err := s.q.DeleteSpoolFile(ctx, batchKey); err != nil {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path,
			fmt.Sprintf("forgetting evicted spool file %s", batchKey), err)
	}

	return nil
}

// PruneSpoolSchemas drops descriptors no spool file names any more, returning how
// many were removed.
//
// It is spool_schemas' own prune and is deliberately not shared with
// staging_schemas': the two tables exist separately precisely because their rows
// have different lifetimes, and a prune that answered to both would delete a
// spool file's shape the moment its staged rows were pruned — which is seconds
// after the file is written, and years before the drain needs to read it.
func (s *Store) PruneSpoolSchemas(ctx context.Context) (int64, error) {
	n, err := s.q.DeleteUnreferencedSpoolSchemas(ctx)
	if err != nil {
		return 0, errdef.NewStagingError(errdef.StagingKindWrite, s.path, "pruning unreferenced spool descriptors", err)
	}

	return n, nil
}
