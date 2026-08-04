package icelake

import (
	"context"
	"errors"
	"os"

	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/flush"
	"github.com/gvkhna/icelake/internal/schemamap"
	"github.com/gvkhna/icelake/internal/staging"
)

// DrainSpool uploads and commits every spool file the cache still holds that has
// never reached a bucket, for every table that has one.
//
// It is the half of local-only mode that makes it more than a demo. A person
// tries icelake locally for a week, then puts an endpoint, a bucket, a prefix
// and credentials into the same configuration and restarts against the same
// StagingPath and the same CacheDir. Every spool file whose commit was never
// confirmed is a batch that exists on disk and in no table; this is what moves
// them.
//
// A writer drains its own table when it opens, so this exists for the table no
// writer reopens: such a table has no declaration anywhere in the process, and
// nothing else would ever ask about it. It stops at the drain for the same
// reason — there is no declaration to reconcile the table to afterwards, so the
// table is left at the shape of the newest file it committed.
//
// **It serializes against writers through the one slot a table has, and the
// outcome for the second arrival is stated here because it is a promise rather
// than an accident.** Both paths that walk a backlog claim the table's slot in
// the store's own registry first — this call, and [OpenWriter], whose open-time
// drain is the first thing it does under its claim — so exactly one of them can
// be touching a given table at any moment. Without that, both would walk the
// same files: the object keys would be identical and the snapshot check would
// stop most double commits, but the two would still race over marking files and
// pruning rows the other is reading, and, because the catalog is one local
// SQLite file, the loser would surface a raw duplicate-key failure from it
// rather than anything a caller could match on.
//
// Which of the two arrives second decides what it is told, and neither answer is
// a raced commit:
//
//   - This call arriving second **skips that table and returns nil for it**. A
//     table whose slot is held is a table someone else is already draining or
//     has already drained — a writer drains its own backlog as it opens — so
//     there is nothing here to do, and reporting a failure would turn a no-op
//     into one.
//   - [OpenWriter] arriving second is **refused with [ErrTableInUse]**, and
//     returns no writer. It cannot skip its drain the way this call skips a
//     table: its whole reason for draining before it loads the table is that the
//     backlog must be committed against its own recorded shapes *before*
//     anything reconciles the table to the current declaration. Proceeding while
//     another drain is still walking would do exactly what the ordering exists
//     to prevent. Refusing is the honest answer, and the caller's retry finds an
//     empty backlog a moment later.
//
// **There is no window between the two, and it took a second attempt to get
// that right, so it is written down as the property rather than as an
// intention.** A table has an owner from the first moment either path touches it
// until either the drain ends or the writer itself exists. The first version of
// this claim released the slot when the drain finished and let [OpenWriter] load
// its table unclaimed, on the reasoning that a drain slipping into that gap
// would find an empty backlog — which is true, and beside the point: the drain
// it would find nothing to do for still creates the namespace and loads the
// table on its way to finding that out, against the same local catalog the
// writer is loading from at that instant. It failed about one run in fifteen
// with a raw SQLite constraint error. The slot is now handed from the claim
// straight to the writer under one hold of the registry's lock
// ([Store.swapClaim]), never released and re-taken, so there is no instant at
// which the table is unowned for anything to slip into.
//
// The refusals are ordered, and the order is a promise rather than an accident
// of where each check sits. A closed store answers [ErrClosed] before anything
// else is judged, exactly as every other entry point does and including a store
// that is also local-only: the caller shut this store down, and that is the more
// fundamental of the two things wrong with the call. Otherwise a store opened
// with [Config.LocalOnly] answers [ErrLocalOnly], which is the one place that
// sentinel comes from — a store with no bucket cannot drain into one, and
// answering with whatever a client that was never built would have said would be
// a worse way of saying the same thing. A store with no cache configured has
// nothing to drain and returns nil.
//
// One table's failure does not stop the others. The first error is kept and
// returned once every table has been attempted — the same shape [Store.Close]
// uses when it drains many writers, and for the same reason: this is a
// process-wide recovery call, and a bucket that refused one table's first file
// is no reason to leave every other table's backlog on one local disk.
//
// The drain is idempotent for the same reason a crash-recovered upload is: a
// spool file's name is its batch key, the batch key is a content hash, and the
// object key it uploads to is the same key that batch would have had if the
// bucket had been configured from the first day. A drain interrupted anywhere
// re-uploads byte-identical bytes over the same key on the next run, and the two
// orderings that could double-count are closed the same way the flush path
// closes them: the snapshot check before the commit, and marking the file
// committed after the commit lands.
func (s *Store) DrainSpool(ctx context.Context) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if s.cfg.LocalOnly {
		return ErrLocalOnly
	}
	if s.spool == nil {
		return nil
	}

	tables, err := s.spool.BacklogTables(ctx)
	if err != nil {
		return err
	}

	var first error
	for _, t := range tables {
		if err := s.drainIfFree(ctx, t.Namespace, t.Table); err != nil && first == nil {
			first = err
		}
	}

	return first
}

// drainIfFree drains one table with that table's slot held for the whole walk,
// and skips a table whose slot somebody else already holds.
//
// Skipping is right for this caller and only for this caller: a table whose slot
// is held is one a writer is opening — and a writer drains its own backlog
// before it does anything else — so there is nothing here to do and reporting a
// failure would turn a no-op into one. [OpenWriter] gets the opposite answer for
// the opposite reason, which [Store.DrainSpool] sets out in full.
//
// Everything the drain does happens under the claim, including the openTable
// inside [Store.drainFile] that creates the table or reconciles it to a file's
// recorded shape. That is the part worth checking rather than assuming: the
// catalog is one local SQLite file shared by every table in the process, so two
// unclaimed walkers of one table race each other's CreateNamespace and one of
// them meets a raw duplicate-key failure. The claim is taken before the walk
// starts and released after it ends, so no step of it is ever outside.
func (s *Store) drainIfFree(ctx context.Context, namespace, name string) error {
	if err := s.claimTable(namespace, name); err != nil {
		if errors.Is(err, ErrTableInUse) {
			return nil
		}

		return err
	}
	defer s.releaseTable(namespace, name)

	return s.drainTable(ctx, namespace, name)
}

// tableClaim is the placeholder that holds a table's slot while it is being
// drained, or while the writer that will hold the slot is still being opened.
//
// Closing it is a no-op, and that is the honest answer rather than a shortcut: a
// writer is closed because it holds a sealed batch that has to be drained before
// the databases go, and a claim holds nothing — every file its drain has
// committed is committed and every file it has not is exactly where it was. A
// [Store.Close] that lands while a claim is held therefore leaves that caller to
// fail on its next database call, or at [Store.swapClaim], which answers
// [ErrClosed].
type tableClaim struct{}

// close satisfies the interface the store drains writers through.
func (tableClaim) close(context.Context) error { return nil }

// drainTable walks one table's backlog of spool files, oldest first, bringing
// the live table to each file's own recorded shape and committing it there.
//
// It is what [OpenWriter] runs before it loads the table, and what
// [Store.DrainSpool] runs for a table no writer reopens. It is a no-op with no
// cache configured, in local-only mode, and for a table with no backlog — which
// is every table on every ordinary run, so the cost of the feature on the common
// path is one query against a bounded table.
func (s *Store) drainTable(ctx context.Context, namespace, name string) error {
	if s.spool == nil || s.cfg.LocalOnly {
		return nil
	}

	backlog, err := s.spool.Backlog(ctx, namespace, name)
	if err != nil {
		return err
	}
	if len(backlog) == 0 {
		return nil
	}

	staged, err := s.stagedBatchKeys(ctx, namespace, name)
	if err != nil {
		return err
	}

	ictx := s.icebergCtx(ctx)
	for _, file := range backlog {
		// A batch whose rows are still staged is owned by replay, which will
		// seal, encode, upload and commit it through the ordinary flush path.
		// Draining it here as well would upload the same batch from both ends.
		if staged[file.BatchKey] {
			continue
		}
		if err := s.drainFile(ictx, file); err != nil {
			return err
		}
	}

	return nil
}

// drainFile brings the live table to one backlog file's own recorded shape and
// commits that file against it.
//
// The order inside is the whole design, and the reason is stated at the call
// site in [OpenWriter]: the file is committed against the shape it was written
// under, never against a shape the current process happens to declare. The
// table is created from that shape if it does not exist yet — the ordinary case,
// since local-only mode never created one, and since the backlog is walked
// oldest first it is the *oldest* file's descriptor a table is created from —
// and is otherwise reconciled forward to it, one file at a time.
//
// Everything that can go wrong on the way to the commit is a [SpoolError] of
// kind drain wrapping what actually failed, so a caller can match either the
// spool failure or the reconciliation error inside it, and so that one table's
// undrainable backlog blocks exactly that table's open. The one exception is the
// last step, which keeps its own kind: a file that is committed and could not be
// marked is not a drain that failed, it is a file that is merely unevictable.
func (s *Store) drainFile(ctx context.Context, file staging.SpoolEntry) error {
	fail := func(detail string, cause error) error {
		return errdef.NewSpoolError(errdef.SpoolKindDrain, s.spool.Dir(),
			s.spool.Path(file.Namespace, file.Table, file.BatchKey), file.BatchKey, detail, cause)
	}

	// The shape the file was written under, read from the descriptor the spool
	// stored beside it rather than from any struct or document this process
	// happens to hold. It is the only statement of that shape that outlived the
	// run that wrote it.
	desc, err := s.spool.Schema(ctx, file.SchemaFP)
	if err != nil {
		return fail("reading the shape this file was written under", err)
	}
	decl, err := schemamap.DeclareFromDescriptor(file.Namespace, file.Table, desc.IcebergFields())
	if err != nil {
		return fail("deriving a declaration from the shape this file was written under", err)
	}
	tbl, err := s.openTable(ctx, decl)
	if err != nil {
		return fail("bringing the table to the shape this file was written under", err)
	}

	body, err := os.ReadFile(s.spool.Path(file.Namespace, file.Table, file.BatchKey))
	if err != nil {
		return fail("reading the spool file", err)
	}

	key := flush.DataKey(s.cfg.WarehousePrefix, file.Namespace, file.Table, file.BatchKey)
	uri := flush.DataURI(s.cfg.Bucket, s.cfg.WarehousePrefix, file.Namespace, file.Table, file.BatchKey)

	// The upload is an overwrite whenever a previous run got this far: the same
	// bytes, read from the same file, under the same key.
	if err := flush.PutObject(ctx, s.s3, s.cfg.Bucket, key, body); err != nil {
		return fail("uploading the spool file", err)
	}

	// The same check the crash path makes, and for the same reason: a previous
	// run may have committed this exact file and died before it could mark it.
	done, err := flush.AlreadyCommitted(ctx, tbl, uri)
	if err != nil {
		return fail("checking whether this file is already committed", err)
	}
	if !done {
		if _, err := flush.AddFile(ctx, tbl, uri); err != nil {
			return fail("committing the spool file", err)
		}
	}

	// After the commit lands, never before. A crash in this window leaves a
	// committed file the sweeper will never evict, which is the harmless
	// direction: a file kept too long costs disk, and a file evicted too early
	// would cost this drain a batch it could then never recover.
	return s.spool.MarkCommitted(ctx, file.Namespace, file.Table, file.BatchKey, s.clock.Now())
}

// stagedBatchKeys returns the batch keys one table still has rows staged under.
//
// The drain has to skip those files, and the reason is ownership rather than
// efficiency: a batch whose rows are still in staging belongs to the writer's
// replay, which seals it, encodes it and commits it through the ordinary flush
// path. A drain that uploaded it too would put the same batch on two paths at
// once — and while the object key would be identical and the snapshot check
// would stop the double commit, the two would then race over pruning the rows
// the other is reading.
func (s *Store) stagedBatchKeys(ctx context.Context, namespace, name string) (map[string]bool, error) {
	index, err := s.staging.Scan(ctx)
	if err != nil {
		return nil, err
	}

	keys := make(map[string]bool)
	for _, b := range index.Claim(namespace, name) {
		if b.Key != "" {
			keys[b.Key] = true
		}
	}

	return keys, nil
}
