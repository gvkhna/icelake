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

// AppendRow is one record being durably accepted into staging.
//
// It carries the descriptor rather than a bare fingerprint on purpose. The
// fingerprint is derived from the descriptor here, so a caller cannot record a
// payload under a shape that is not the shape it was encoded with, and the
// descriptor is written in the same transaction as the row, so no staged row can
// ever exist — not even across a crash between two statements — whose recorded
// shape is missing. Replay depends on that: a payload whose descriptor is absent
// is undecodable by any binary, including the one that wrote it.
type AppendRow struct {
	// Namespace and Table are the record's table identity. Both are validated
	// one layer up, at OpenWriter, before any row reaches here.
	Namespace string
	Table     string
	// Descriptor is the shape Payload was encoded under.
	Descriptor *canon.Descriptor
	// Payload is the canonical record encoding from [canon.Encode].
	Payload []byte
	// CreatedAt is diagnostic only and is never read by the write path. It
	// comes from the caller — and therefore from icelake's injected clock —
	// rather than being taken here, so that no read of time hides inside this
	// package.
	CreatedAt time.Time
}

// StagedRow is a staged record read back with its payload.
type StagedRow struct {
	// ID is the row's primary key, and the monotonic order replay reads rows in.
	ID int64
	// Namespace and Table are the record's table identity.
	Namespace string
	Table     string
	// BatchKey is the batch this row was sealed into, or empty if it is not
	// sealed yet. Once set it is authoritative and is never recomputed: the
	// declared schema may have changed since, and recomputing would produce a
	// different key, stranding an already-uploaded object as an orphan.
	BatchKey string
	// SchemaFP fingerprints the shape Payload is encoded under. It is not
	// necessarily the current declaration's fingerprint.
	SchemaFP string
	// Payload is the canonical record encoding.
	Payload []byte
	// CreatedAt is diagnostic only.
	CreatedAt time.Time
	// Quarantined is true for a row whose batch could not be encoded. Such rows
	// are excluded from every future replay and are left for an operator to
	// inspect, export or delete by hand.
	Quarantined bool
}

// SealRow is one row's part of a seal.
type SealRow struct {
	// ID is the row to seal.
	ID int64
	// Payload, when non-nil, is the row re-encoded under the sealing
	// descriptor, replacing what is stored. It is nil for the overwhelmingly
	// normal case of a row already encoded under that shape.
	Payload []byte
}

// SealRequest describes one batch being sealed durably before it is encoded.
type SealRequest struct {
	// Namespace and Table are the one table this batch belongs to. A batch is
	// always exactly one table's rows: its key is computed with the namespace
	// and table name as hash inputs, and the object it names lives under that
	// table's own prefix, so a batch spanning two tables would have a key that
	// is wrong for at least one of them. Every row named below must belong to
	// this table, and one that does not fails the seal.
	Namespace string
	Table     string
	// BatchKey is the batch's content hash, computed by the caller over exactly
	// the payloads this seal leaves in the database.
	BatchKey string
	// Descriptor is the shape every row in the sealed batch is encoded under
	// once this seal commits. A sealed batch is always fingerprint-homogeneous,
	// which is what lets the batch key be computed over payload bytes that agree
	// with the schema component of the key's own input.
	Descriptor *canon.Descriptor
	// Rows are the batch's rows, in batch order.
	Rows []SealRow
}

// Counters are the staged-but-uncommitted totals the staging ceiling is checked
// against. Quarantined rows are included deliberately: a quarantine that grows
// should eventually stop the writer loudly rather than silently eat headroom
// forever.
type Counters struct {
	// Records is the number of staged rows.
	Records int64
	// Bytes is the sum of their payload lengths.
	Bytes int64
}

// Append durably accepts one record and returns the id it was given.
//
// The record is committed before this returns — that is the whole point of the
// layer, and the reason the database runs at synchronous=FULL. The descriptor
// the payload was encoded under is recorded in the same transaction.
//
// A failure here is returned, not absorbed. There is no in-memory fallback
// buffer for "the durability layer is broken", because such a buffer would be a
// silent downgrade of the exact guarantee this layer exists to provide: the
// record is refused, and the caller keeps it.
func (s *Store) Append(ctx context.Context, row AppendRow) (int64, error) {
	if row.Descriptor == nil {
		return 0, errdef.NewStagingError(errdef.StagingKindWrite, s.path, "append has no descriptor to record the payload's shape", nil)
	}
	if len(row.Payload) == 0 {
		return 0, errdef.NewStagingError(errdef.StagingKindWrite, s.path, "append has an empty payload", nil)
	}

	var id int64
	err := s.tx(ctx, func(q *Queries) error {
		if err := q.UpsertSchema(ctx, UpsertSchemaParams{
			Fp:         row.Descriptor.Fingerprint(),
			Descriptor: row.Descriptor.Bytes(),
		}); err != nil {
			return err
		}
		var err error
		id, err = q.InsertStagedRow(ctx, InsertStagedRowParams{
			Namespace: row.Namespace,
			TableName: row.Table,
			SchemaFp:  row.Descriptor.Fingerprint(),
			Payload:   row.Payload,
			ByteLen:   int64(len(row.Payload)),
			CreatedAt: row.CreatedAt.UnixMilli(),
		})
		return err
	})
	if err != nil {
		return 0, errdef.NewStagingError(errdef.StagingKindWrite, s.path, "staging a record", err)
	}
	return id, nil
}

// Rows reads the given rows back by primary key, in the order asked for.
//
// This is the second half of replay: [Store.Scan] builds the index of which
// rows are pending for which table without reading a single payload, the caller
// opening a table claims that table's group out of it, and then the payloads for
// exactly those ids are read back here. It is also how the flush path reads a
// batch back to encode it.
//
// A row the caller asked for that is not there fails loudly. The caller learned
// the id from this same store, so a missing row means something outside this
// process deleted it — the violated one-writer rule — and quietly returning a
// shorter batch would mean uploading a batch under a key that names a different
// set of records.
func (s *Store) Rows(ctx context.Context, ids []int64) ([]StagedRow, error) {
	out := make([]StagedRow, 0, len(ids))
	for _, id := range ids {
		r, err := s.q.GetStagedPayload(ctx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errdef.NewStagingError(errdef.StagingKindState, s.path,
				fmt.Sprintf("staged row %d is gone", id), err)
		}
		if err != nil {
			return nil, errdef.NewStagingError(errdef.StagingKindRead, s.path,
				fmt.Sprintf("reading staged row %d", id), err)
		}
		out = append(out, StagedRow{
			ID:          r.ID,
			Namespace:   r.Namespace,
			Table:       r.TableName,
			BatchKey:    r.BatchKey.String,
			SchemaFP:    r.SchemaFp,
			Payload:     r.Payload,
			CreatedAt:   time.UnixMilli(r.CreatedAt).UTC(),
			Quarantined: r.QuarantinedAt.Valid,
		})
	}
	return out, nil
}

// Seal stamps a batch key on exactly the given rows, in one transaction, and
// rewrites the payload of any row the caller re-encoded under the sealing
// descriptor.
//
// Sealing durably, before the batch is encoded or uploaded, is what makes crash
// recovery reproduce the *same* batch rather than a differently-bounded one —
// which is what makes the content-hash file key idempotent in practice and not
// only in theory. A re-batching that split at a different record would produce a
// different, non-colliding hash and orphan whatever had already been uploaded.
//
// Re-encoding happens here rather than in a separate step for the same reason:
// a sealed batch must be fingerprint-homogeneous, so the transaction that stamps
// the key is the one that makes it so. The sealing descriptor is recorded too,
// so a batch sealed under a shape no staged row was originally written under is
// still decodable after a crash.
//
// A row that is already sealed, already quarantined, or belongs to a different
// table is not moved: the update matches no row and Seal fails with
// [errdef.StagingKindState] rather than silently taking a record out of the
// batch that already owns it, or building a batch whose key is wrong for half
// its contents.
func (s *Store) Seal(ctx context.Context, req SealRequest) error {
	if req.Namespace == "" || req.Table == "" {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "seal has no table identity; a batch belongs to exactly one table", nil)
	}
	if req.BatchKey == "" {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "seal has no batch key", nil)
	}
	if req.Descriptor == nil {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "seal has no descriptor for the shape it seals under", nil)
	}
	if len(req.Rows) == 0 {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "seal has no rows; an empty batch has no content to name", nil)
	}

	err := s.tx(ctx, func(q *Queries) error {
		if err := q.UpsertSchema(ctx, UpsertSchemaParams{
			Fp:         req.Descriptor.Fingerprint(),
			Descriptor: req.Descriptor.Bytes(),
		}); err != nil {
			return err
		}
		for _, row := range req.Rows {
			var n int64
			var err error
			if row.Payload == nil {
				n, err = q.SealStagedRow(ctx, SealStagedRowParams{
					BatchKey:  sql.NullString{String: req.BatchKey, Valid: true},
					ID:        row.ID,
					Namespace: req.Namespace,
					TableName: req.Table,
				})
			} else {
				n, err = q.SealStagedRowRecoded(ctx, SealStagedRowRecodedParams{
					BatchKey:  sql.NullString{String: req.BatchKey, Valid: true},
					SchemaFp:  req.Descriptor.Fingerprint(),
					Payload:   row.Payload,
					ByteLen:   int64(len(row.Payload)),
					ID:        row.ID,
					Namespace: req.Namespace,
					TableName: req.Table,
				})
			}
			if err != nil {
				return err
			}
			if n != 1 {
				return errdef.NewStagingError(errdef.StagingKindState, s.path,
					fmt.Sprintf("staged row %d is gone, belongs to a different table, is already sealed into another batch, or is quarantined", row.ID), nil)
			}
		}
		return nil
	})
	if err != nil {
		// A refusal raised inside the transaction is already the right typed
		// error and already says which row; wrapping it again in a write
		// failure would misreport a rejected seal as a broken database.
		var already errdef.StagingError
		if errors.As(err, &already) {
			return err
		}
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "sealing a batch", err)
	}
	return nil
}

// Delete prunes rows whose batch has been confirmed committed. Nothing is ever
// pruned before that confirmation, which is what makes the whole staging layer
// safe to lose a process at any point.
//
// Deleting a row that is already gone is not an error: a crash between the
// catalog commit and the prune is recovered by re-running the prune, and that
// re-run must be able to succeed.
//
// It does not remove now-unreferenced descriptors — see [Store.PruneSchemas],
// which is separate so that this path stays purely primary-key.
func (s *Store) Delete(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	err := s.tx(ctx, func(q *Queries) error {
		for _, id := range ids {
			if _, err := q.DeleteStagedRow(ctx, id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "pruning committed rows", err)
	}
	return nil
}

// Quarantine marks rows whose batch could not be encoded, recording the encoder
// error for an operator to read.
//
// A batch that cannot be encoded is marked in place rather than deleted or
// retried forever: it is excluded from every future replay, reported through the
// flush-error callback, and left as plain SQLite rows an operator can inspect,
// export or delete by hand. Quarantined rows deliberately keep counting toward
// the staging ceiling, so a quarantine that grows stops the writer loudly rather
// than silently eating headroom.
//
// Marking a row that is already quarantined is not an error; the first mark and
// its recorded cause stand.
//
// A row that carries a batch key may only be quarantined as part of its whole
// batch. Marking some of a sealed batch's rows and not others would leave a
// batch whose stored key no longer names its own bytes — replay would offer a
// shorter batch under a key computed over the longer one, which is the same way
// content-hash keying can lie that the seal guard exists to close, reached
// through the quarantine door instead. Quarantine is a batch-level decision in
// the design ("a batch that cannot be encoded is quarantined in place"), so
// requiring the caller to name the whole batch is the rule, not a restriction.
func (s *Store) Quarantine(ctx context.Context, ids []int64, at time.Time, cause error) error {
	if len(ids) == 0 {
		return nil
	}
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	err := s.tx(ctx, func(q *Queries) error {
		if err := s.checkWholeBatches(ctx, q, ids); err != nil {
			return err
		}
		for _, id := range ids {
			if _, err := q.QuarantineStagedRow(ctx, QuarantineStagedRowParams{
				QuarantinedAt:   sql.NullInt64{Int64: at.UnixMilli(), Valid: true},
				QuarantineError: sql.NullString{String: reason, Valid: true},
				ID:              id,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// As in Seal: a refusal raised inside the transaction is already the
		// right typed error and already says which batch. Re-wrapping it as a
		// write failure would misreport a rejected quarantine as a broken
		// database.
		var already errdef.StagingError
		if errors.As(err, &already) {
			return err
		}
		return errdef.NewStagingError(errdef.StagingKindWrite, s.path, "quarantining a batch", err)
	}
	return nil
}

// checkWholeBatches refuses a quarantine that would take only part of a sealed
// batch. It runs inside the caller's transaction, so the counts it compares
// cannot move underneath the marks that follow them.
func (s *Store) checkWholeBatches(ctx context.Context, q *Queries, ids []int64) error {
	// Deduplicated per batch, so a caller passing an id twice cannot make a
	// partial batch look complete by counting one row as two. Batches are
	// identified by table as well as key, exactly as sealing identifies them.
	named := make(map[batchID]map[int64]bool)
	for _, id := range ids {
		row, err := q.GetStagedRowIdentity(ctx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return errdef.NewStagingError(errdef.StagingKindState, s.path,
				fmt.Sprintf("staged row %d is gone", id), err)
		}
		if err != nil {
			return err
		}
		if !row.BatchKey.Valid || row.BatchKey.String == "" {
			continue
		}
		b := batchID{
			table: TableID{Namespace: row.Namespace, Table: row.TableName},
			key:   row.BatchKey.String,
		}
		if named[b] == nil {
			named[b] = make(map[int64]bool)
		}
		named[b][id] = true
	}

	for b, members := range named {
		total, err := q.CountStagedRowsWithBatchKey(ctx, CountStagedRowsWithBatchKeyParams{
			Namespace: b.table.Namespace,
			TableName: b.table.Table,
			BatchKey:  sql.NullString{String: b.key, Valid: true},
		})
		if err != nil {
			return err
		}
		if total != int64(len(members)) {
			return errdef.NewStagingError(errdef.StagingKindState, s.path, fmt.Sprintf(
				"quarantining %d of the %d rows sealed into batch %s of table %s.%s would leave a batch whose stored key no longer names its bytes; quarantine the whole batch or none of it",
				len(members), total, b.key, b.table.Namespace, b.table.Table), nil)
		}
	}
	return nil
}

// Schema returns the recorded shape a fingerprint names, so replay can decode a
// row by the shape that wrote it rather than by whatever the current binary
// declares.
//
// A fingerprint with no recorded descriptor is a corrupt store, not a normal
// miss: every path that writes a staged row records its descriptor in the same
// transaction, so it fails with [errdef.StagingKindState] rather than returning
// a nil descriptor for a caller to trip over.
func (s *Store) Schema(ctx context.Context, fp string) (*canon.Descriptor, error) {
	raw, err := s.q.GetSchema(ctx, fp)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errdef.NewStagingError(errdef.StagingKindState, s.path,
			fmt.Sprintf("no recorded descriptor for schema fingerprint %s", fp), err)
	}
	if err != nil {
		return nil, errdef.NewStagingError(errdef.StagingKindRead, s.path,
			fmt.Sprintf("reading the descriptor for schema fingerprint %s", fp), err)
	}
	d, err := canon.ParseDescriptor(raw)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// PruneSchemas drops descriptors no live staged row names any more, returning
// how many were removed.
//
// It is deliberately not part of [Store.Delete]. Every other statement in this
// package touches rows by primary key, and this one cannot: it has to look
// across the whole staging table to know a fingerprint has no rows left. Keeping
// it a separate call leaves the commit path purely primary-key and lets the
// caller decide when to pay for the scan — after a batch commits, not after
// every row.
//
// A stale descriptor is harmless while it lingers: it costs a few bytes and is
// never read, because nothing references it.
func (s *Store) PruneSchemas(ctx context.Context) (int64, error) {
	n, err := s.q.DeleteUnreferencedSchemas(ctx)
	if err != nil {
		return 0, errdef.NewStagingError(errdef.StagingKindWrite, s.path, "pruning unreferenced descriptors", err)
	}
	return n, nil
}

// Counters returns the staged record count and total payload bytes as a direct
// SQL aggregate: the second way of obtaining the numbers the in-memory ceiling
// counters carry, and therefore the way to check them against the database.
//
// It reads no payloads, and the table it aggregates is itself bounded by the
// ceiling those numbers enforce — which is what makes this cheap by
// construction rather than by hope. Nothing in the write path calls it, and
// nothing should: the counters are seeded from the totals [Store.Scan] measures
// on the ordered pass startup makes anyway, and from there they move with each
// accepted record and each committed batch.
//
// Corrected at the completeness audit's fifth round (2026-08-01): this comment
// used to open by calling it "the aggregate that seeds the in-memory ceiling
// counters", which its own next sentence then denied — that sentence said
// startup does not need to call it because [Store.Scan] returns the same
// numbers, and startup indeed never does. Two readings were available to
// anyone who met the pair, and both cost something: believe the first and this
// looks like a hot-path query on the open path, which is where a reader goes
// hunting for a cost that is not there; believe the second and the function
// looks like dead code, which is where the next person deletes it. It is
// neither. Its one caller is TestCountersAgreeWithTheReplayScan, which asserts
// that the aggregate and the ordered pass return the same totals, and that is
// exactly what a second query path is for: the ceiling is only as trustworthy
// as the agreement between the two ways of computing it, and a check written
// against the same path it checks would assert nothing.
func (s *Store) Counters(ctx context.Context) (Counters, error) {
	row, err := s.q.StagedTotals(ctx)
	if err != nil {
		return Counters{}, errdef.NewStagingError(errdef.StagingKindRead, s.path, "counting staged rows", err)
	}
	bytes, err := asInt64(row.Bytes)
	if err != nil {
		return Counters{}, errdef.NewStagingError(errdef.StagingKindRead, s.path, "counting staged bytes", err)
	}
	return Counters{Records: row.Records, Bytes: bytes}, nil
}

// asInt64 normalizes what the driver returns for SUM(). SQLite's aggregate is
// dynamically typed and the generated signature is interface{}, so the concrete
// type depends on whether the sum is an integer or the COALESCE default.
func asInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("unexpected sum type %T", v)
	}
}

// tx runs fn inside one transaction, rolling back on any error. Every write in
// this package goes through it: a staged row and the descriptor it names must
// land together or not at all, and a seal must stamp every one of its rows or
// none of them.
func (s *Store) tx(ctx context.Context, fn func(q *Queries) error) (err error) {
	t, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// The rollback's own error is deliberately dropped: the caller's
			// error is the one that explains what happened, and a failed
			// rollback on a transaction that is already failing adds nothing an
			// operator can act on.
			_ = t.Rollback()
		}
	}()
	if err = fn(s.q.WithTx(t)); err != nil {
		return err
	}
	return t.Commit()
}
