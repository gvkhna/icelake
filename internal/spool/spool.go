package spool

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/staging"
)

// The permissions the spool creates directories and files with.
//
// They are the ordinary ones for content a person is meant to be able to read
// with another tool — the whole point of a directory of Parquet files is that
// DuckDB, or rclone, or an operator's own eyes can reach it — and the bytes are
// in any case byte-identical to an object the same operator's credentials can
// fetch from the bucket.
const (
	dirPerm  fs.FileMode = 0o755
	filePerm fs.FileMode = 0o644
)

// Spool is one store's local Parquet cache: a directory on disk, and the rows in
// the staging database that say what is in it.
//
// It is created only when a cache directory is configured. A nil *Spool is not a
// usable value; callers hold a nil pointer to mean "no spool" and check it, which
// is what makes the whole feature one branch at each seam rather than a second
// write path.
type Spool struct {
	dir string
	db  *staging.Store
}

// New returns a spool over dir, recording what it writes in db.
func New(dir string, db *staging.Store) *Spool { return &Spool{dir: dir, db: db} }

// Dir returns the configured cache directory, so an error raised by a layer
// above can name the same directory this spool was built for.
func (s *Spool) Dir() string { return s.dir }

// Path returns where one batch's data file lives:
// <dir>/<namespace>/<table>/data/<batch-key>.parquet.
//
// The layout mirrors the warehouse key layout segment for segment, deliberately,
// so that comparing a local file against its object needs no lookup table.
func (s *Spool) Path(namespace, table, batchKey string) string {
	return filepath.Join(s.dir, namespace, table, "data", batchKey+".parquet")
}

// Write puts one batch's encoded Parquet bytes on disk and records the file,
// file first and database second.
//
// The bytes go to a temporary file beside their destination, are fsynced,
// renamed into place, and the containing directory is fsynced — so the file
// either exists whole under its final name or does not exist at all, since a
// rename is the one filesystem operation that cannot half-happen. Only then does
// one transaction record the batch's schema descriptor and its file row.
//
// A crash between the two leaves a file no row names, and nothing repairs it
// because nothing has to: the batch's rows are still staged, the file's name is
// the batch's key, and the retry writes that same name again and re-records it.
// Both halves are idempotent for the same reason the upload is.
//
// "The same name" rather than "the same bytes", and the difference is small but
// real. A batch this process seals is a content hash over its rows, so a rewrite
// of it is byte-identical. A batch sealed by a *previous* process keeps its
// stored key — never recomputed, or the object already uploaded under the old
// name would be orphaned — so if the declaration moved between the seal and the
// replay, the rows are re-encoded onto the current shape and the file that comes
// back under that name carries a different shape and a different size. That is
// why the row is re-recorded rather than left alone, and why the descriptor
// travels with it: what the drain must read years later is the shape of the
// bytes that are actually there.
//
// The two failures are separate kinds because they mean different things about
// what is at risk. A write failure means the bytes are not on disk — which in
// bucket mode fails the flush and leaves the batch staged, and in local-only mode
// is the failure of the only durable act there is. A record failure means a file
// exists that no sweep will evict and no drain will find until the batch's own
// retry rewrites both.
func (s *Spool) Write(ctx context.Context, namespace, table, batchKey string, desc *canon.Descriptor, body []byte, now time.Time) error {
	path := s.Path(namespace, table, batchKey)

	if err := s.writeFile(path, batchKey, body); err != nil {
		return err
	}

	if err := s.db.RecordSpoolFile(ctx, staging.SpoolRecord{
		Namespace:  namespace,
		Table:      table,
		BatchKey:   batchKey,
		Descriptor: desc,
		ByteLen:    int64(len(body)),
		CreatedAt:  now,
	}); err != nil {
		return errdef.NewSpoolError(errdef.SpoolKindRecord, s.dir, path, batchKey,
			"the file is on disk and its row is not; the batch's own retry rewrites both", err)
	}

	return nil
}

// writeFile is the durable half of [Spool.Write]: temporary file, fsync, rename,
// directory fsync.
//
// The temporary name is derived from the batch key rather than randomised, which
// matters after a crash: a leftover temporary from a dead process is truncated
// and rewritten by the retry of the same batch instead of accumulating one file
// per attempt in a directory nothing ever tidies. It cannot collide with a live
// writer's, because one table has one writer and one flush worker. And it
// deliberately does not end in .parquet, so that a reader pointed at the
// directory never picks up a half-written file.
func (s *Spool) writeFile(path, batchKey string, body []byte) (err error) {
	fail := func(detail string, cause error) error {
		return errdef.NewSpoolError(errdef.SpoolKindWrite, s.dir, path, batchKey, detail, cause)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fail("creating the cache directory", err)
	}

	tmp := filepath.Join(dir, batchKey+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, filePerm)
	if err != nil {
		return fail("creating the temporary file", err)
	}
	defer func() {
		// Only reached when something below failed: the success path has already
		// closed and renamed the file, and removing a name that is no longer
		// there is not a failure worth reporting over the one that got here.
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	if _, err := f.Write(body); err != nil {
		return fail("writing the temporary file", err)
	}
	// Before the rename, not after: a rename that beat its own data to the disk
	// would leave a file under its final name whose contents are whatever the
	// filesystem had got round to.
	if err := f.Sync(); err != nil {
		return fail("flushing the temporary file", err)
	}
	if err := f.Close(); err != nil {
		return fail("closing the temporary file", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fail("renaming the temporary file into place", err)
	}
	if err := syncDir(dir); err != nil {
		return fail("flushing the cache directory", err)
	}

	return nil
}

// syncDir fsyncs a directory, which is what makes a rename into it durable
// rather than merely visible to this process.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()

		return err
	}

	return d.Close()
}

// MarkCommitted records that a batch's file has been uploaded and
// catalog-committed, which is the moment retention may evict it.
//
// It runs after the commit lands, never before, and the asymmetry is deliberate:
// a crash in that window leaves a committed file the sweeper will never evict,
// which is the harmless direction. A file kept too long costs disk; a file
// evicted too early costs the drain a batch it would then have to re-upload from
// nothing.
func (s *Spool) MarkCommitted(ctx context.Context, namespace, table, batchKey string, now time.Time) error {
	if err := s.db.MarkSpoolCommitted(ctx, batchKey, now); err != nil {
		return errdef.NewSpoolError(errdef.SpoolKindMark, s.dir, s.Path(namespace, table, batchKey), batchKey,
			"the batch is committed and its file could not be marked committed; the file is merely unevictable", err)
	}

	return nil
}

// Backlog lists one table's files that have never reached a bucket, oldest
// first — what the transition drain walks.
func (s *Spool) Backlog(ctx context.Context, namespace, table string) ([]staging.SpoolEntry, error) {
	return s.db.SpoolBacklog(ctx, namespace, table)
}

// BacklogTables names every table holding files that have never reached a
// bucket, which is what a drain with no declaration in hand walks.
func (s *Spool) BacklogTables(ctx context.Context) ([]staging.TableID, error) {
	return s.db.SpoolBacklogTables(ctx)
}

// Schema returns the shape a spool file was written under, which is the only
// statement of that shape that outlives the process that wrote it.
func (s *Spool) Schema(ctx context.Context, fp string) (*canon.Descriptor, error) {
	return s.db.SpoolSchema(ctx, fp)
}

// Sweep evicts committed files that are over the configured bounds: by age
// first, then by size, oldest first.
//
// Only committed files are ever visible to it, and that protection is the shape
// of the query rather than a rule applied here — see this package's doc comment,
// which is where the reasoning lives, because it is the one property of this
// function that must survive being refactored by someone who has not read it.
//
// It is best-effort by design and returns the first failure it met after trying
// everything else. A file that could not be deleted leaves the cache larger than
// it was asked to be and nothing else: the file is committed, so its records are
// in the table and in the bucket, nothing is stuck, no batch is waiting, and the
// next tick tries again. The caller — the store's sweeper goroutine — discards
// what this returns for exactly that reason.
//
// A bound of zero is no bound. Both being zero cannot happen in bucket mode,
// where configuration refuses a cache with neither, and a sweeper does not run at
// all in the one mode where nothing is ever evictable.
func (s *Spool) Sweep(ctx context.Context, now time.Time, maxAge time.Duration, maxBytes int64) error {
	files, err := s.db.SpoolEvictable(ctx)
	if err != nil {
		return err
	}

	// The size bound's own total, from the statement that owns it rather than
	// re-derived by summing the rows above. The two agree by construction, which
	// is the point of asking: the number the bound is enforced against is the
	// number the database reports for the set the bound is defined over, so a
	// future change to what "evictable" means moves both together.
	total, err := s.db.SpoolCommittedBytes(ctx)
	if err != nil {
		return err
	}

	var first error
	evicted := 0
	remaining := make([]staging.SpoolEntry, 0, len(files))

	// Age first. The list is already oldest-first, so this is one pass.
	for _, f := range files {
		if maxAge > 0 && now.Sub(f.CreatedAt) > maxAge {
			if err := s.evict(ctx, f); err != nil {
				if first == nil {
					first = err
				}

				continue
			}
			total -= f.ByteLen
			evicted++

			continue
		}
		remaining = append(remaining, f)
	}

	// Then size, still oldest-first, until the committed set is inside its bound.
	for _, f := range remaining {
		if maxBytes <= 0 || total <= maxBytes {
			break
		}
		if err := s.evict(ctx, f); err != nil {
			if first == nil {
				first = err
			}

			continue
		}
		total -= f.ByteLen
		evicted++
	}

	if evicted > 0 {
		if _, err := s.db.PruneSpoolSchemas(ctx); err != nil && first == nil {
			first = err
		}
	}

	return first
}

// evict deletes one committed file and then forgets its row.
//
// The file goes first so that the row is never dropped for a file that is still
// there: a row without its file is recovered by the next sweep, which tolerates
// the missing file and deletes the row, while a file without its row is
// invisible to every statement this package has and would sit in the cache
// forever.
//
// A file that is already gone is not an error. A committed spool file is
// disposable — it is byte-identical to an object in a committed snapshot — so an
// operator who deleted one by hand has done nothing wrong, and the right response
// is to forget the row rather than to report a failure nobody is listening for.
//
// A failure that is *not* tolerated is returned as a plain wrapped error rather
// than as a [errdef.SpoolError], and that is deliberate rather than an
// oversight. The taxonomy has four kinds and no eviction kind, because an error
// from here reaches no caller: it goes to [Spool.Sweep], which hands it to a
// goroutine that discards it. Building a typed error with one of the other four
// kinds would be worse than untyped — the write kind means the bytes of a flush
// could not be put on disk, which is a statement about a batch at risk, and
// eviction has no batch at risk. So the taxonomy stays four kinds wide, the
// no-eviction-kind decision stays structurally true, and this failure is exactly
// what it is: a file the sweeper could not delete.
func (s *Spool) evict(ctx context.Context, f staging.SpoolEntry) error {
	path := s.Path(f.Namespace, f.Table, f.BatchKey)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("icelake: spool %q: evicting the committed file for %s.%s: %w",
			path, f.Namespace, f.Table, err)
	}

	return s.db.DeleteSpoolFile(ctx, f.BatchKey)
}
