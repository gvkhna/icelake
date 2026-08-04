package staging

import (
	"context"
	"fmt"

	"github.com/gvkhna/icelake/internal/errdef"
)

// TableID is a table's identity inside the shared staging database.
type TableID struct {
	// Namespace is the table's namespace.
	Namespace string
	// Table is the table name.
	Table string
}

// batchID identifies one sealed batch: its table and its stored key. The table
// is part of the identity because a batch belongs to exactly one table, so two
// tables carrying the same key string are two batches rather than one — which is
// the same rule the seal statements enforce, applied to the checks that read
// batches back.
type batchID struct {
	table TableID
	key   string
}

// PendingRow is one staged row the replay index knows about, without its
// payload. The payload is read back afterwards by primary key, through
// [Store.Rows].
type PendingRow struct {
	// ID is the row's primary key, and its position in insert order.
	ID int64
	// SchemaFP fingerprints the shape the payload is encoded under, which is
	// not necessarily the current declaration's.
	SchemaFP string
	// ByteLen is the payload's length.
	ByteLen int64
}

// PendingBatch is a group of staged rows replay must reassemble together.
//
// A batch with a non-empty Key was sealed before the process died: it must be
// rebuilt with exactly this membership, in exactly this order, because its key
// was computed over precisely these records and re-uploading under any other
// boundary would orphan whatever had already been written under that name.
// Rows sealed under the same key are the same batch even if newer unsealed rows
// were staged between them.
//
// The one batch with an empty Key is the tail of rows that were accepted but
// never sealed. They are free to be re-batched however the current
// configuration's thresholds see fit.
type PendingBatch struct {
	// Key is the stored batch key, or empty for rows that were never sealed.
	Key string
	// Rows are the batch's rows in ascending id order, which is insert order.
	Rows []PendingRow
}

// Index is the in-memory result of one ordered pass over staging.
//
// One pass does both jobs: it measures the totals the ceiling counters are
// seeded from, and it groups every pending row by table and, within a table, by
// batch key. That is why the staging table needs no index on (namespace,
// table_name) — no pass is ever filtered to one table, every pass scans all of
// them in primary-key order and the payloads are then read back by primary key.
// No index should be added for this until a real, measured slow query proves
// one is needed.
//
// An Index is a snapshot of the moment its own [Store.Scan] ran, and is not
// safe for concurrent use. Two callers make one, and neither keeps it. Process
// startup makes a pass before any writer exists, to seed the counters and to
// refuse a staging database this build will not replay, and keeps the counters
// and nothing else. Opening a writer makes another and claims one table's rows
// out of it. Nothing holds an Index across those calls, and nothing should:
// rescanning is what makes the answer come from the durable file rather than
// from a memory of it.
//
// Corrected at the completeness audit's fifth round (2026-08-01), and what it
// replaces was false rather than merely dated. The sentence said "An Index is a
// snapshot taken at startup, before any writer exists", and one Index is built
// per OpenWriter, by the root package's Store.pendingFor, at whatever moment a
// caller opens a table. A reader who believed it would conclude that a writer
// opened at minute ten cannot see a row staged at minute five by a writer that
// closed at minute nine — which is not a small misreading, because that is
// precisely the bug the design was changed to fix. ARCHITECTURE.md's "Decision
// (2026-07-31, M4)" under "Local write-ahead durability" records the change and
// the reasoning: a snapshot taken at process start cannot contain rows staged
// during a writer's own life, so the natural response to a Close that returned
// with records undrained — reopen the table and try again — was handed nothing,
// and those records sat in staging, accepted and undelivered, for the rest of
// the process's life.
//
// What is actually being corrected here is a survey rather than a sentence,
// and that is the part worth keeping. The pass that fixed Open's and
// OpenWriter's own doc comments and ARCHITECTURE.md's API sketch closed by
// naming what it had left behind — "internal/staging's package and
// generated-query documentation", which is doc.go and queries.sql. Those two
// categories reach four of the eight sentences that were actually left: the
// three in doc.go and the one in queries.sql. They do not reach the two in
// this file, the one in open.go or the one in store.go, and among the four
// they miss is the only copy of the wording that was false rather than stale.
// Enumerated rather than characterised, the pre-M4 wording stood in eight
// places across five sources in this package, and this pass corrects all
// eight: in this file, this type's doc and [Store.Scan]'s; in doc.go, the
// package summary's "startup replay index", the shared-file argument's "one
// replay pass at startup", and the whole "What the layers above do with it"
// paragraph; in open.go, the same shared-file argument on [Store]; in
// store.go, [Store.Rows]'s "second half of startup replay"; and in
// queries.sql, ScanStagedRows's comment, from which the copy in
// queries.sql.go is generated. No comment left in this package describes the
// replay pass as something only startup makes; where startup's own pass is
// still named, it is named as one of the two. The word "startup" also
// survives twice more in open.go, and both of those are about the migration
// run and the schema state, which really do happen once at process start and
// are untouched by any of this. The arithmetic those eight comments were
// making is untouched too — one file to back up and one migration run at
// startup are exactly as true as they were, and the replay pass is still one
// ordered scan of a table the ceiling bounds, paid once per writer opened
// instead of once per process.
//
// Two nearby sentences are deliberately not counted among the eight, because
// neither carried the superseded wording and lumping them in would inflate a
// number this comment is asking to be checked. [Index.Counters] below said
// these totals "are what the in-memory ceiling counters are seeded from",
// which is true of the startup pass and says nothing either way about the
// others; it has been made specific rather than corrected. And [Store.Counters]
// in store.go opened by calling itself the aggregate that seeds those counters,
// which its own next sentence already denied — a different defect, in a
// function no production code calls at all, corrected in its own place with its
// own note.
type Index struct {
	counters Counters
	order    []TableID
	groups   map[TableID][]PendingBatch
}

// Scan makes one ordered pass over the staging table and returns the replay
// index it builds. Process startup makes one such pass, and opening a writer
// makes another; see [Index] for which caller keeps what.
//
// It reads no payloads — only ids, table identity, batch key, fingerprint,
// length and quarantine state — so its cost is a scan of a table the staging
// ceiling already bounds, not a read of everything staged.
//
// Quarantined rows are counted toward the ceiling but left out of the index:
// they are excluded from every future replay, and a quarantine that grows is
// meant to eventually stop the writer rather than silently free up headroom. A
// sealed batch that is only *partly* quarantined is refused outright instead —
// dropping the marked rows there would hand replay a shorter batch under a key
// computed over the longer one.
func (s *Store) Scan(ctx context.Context) (*Index, error) {
	rows, err := s.q.ScanStagedRows(ctx)
	if err != nil {
		return nil, errdef.NewStagingError(errdef.StagingKindRead, s.path, "scanning staged rows", err)
	}

	ix := &Index{groups: make(map[TableID][]PendingBatch)}
	// Tracks, per table, which batch each key is currently accumulating into,
	// so rows of one sealed batch reassemble together even when rows of another
	// batch were staged between them.
	position := make(map[TableID]map[string]int)
	// Tracks, per sealed batch, how many of its rows are quarantined and how
	// many are live. A batch must be all one or all the other; see the split
	// check below.
	quarantined := make(map[batchID]int)
	live := make(map[batchID]int)

	for _, r := range rows {
		ix.counters.Records++
		ix.counters.Bytes += r.ByteLen
		if key := r.BatchKey.String; r.BatchKey.Valid && key != "" {
			b := batchID{table: TableID{Namespace: r.Namespace, Table: r.TableName}, key: key}
			if r.QuarantinedAt.Valid {
				quarantined[b]++
			} else {
				live[b]++
			}
		}
		if r.QuarantinedAt.Valid {
			continue
		}

		id := TableID{Namespace: r.Namespace, Table: r.TableName}
		if _, seen := position[id]; !seen {
			position[id] = make(map[string]int)
			ix.order = append(ix.order, id)
		}

		key := r.BatchKey.String
		at, seen := position[id][key]
		if !seen {
			at = len(ix.groups[id])
			position[id][key] = at
			ix.groups[id] = append(ix.groups[id], PendingBatch{Key: key})
		}
		ix.groups[id][at].Rows = append(ix.groups[id][at].Rows, PendingRow{
			ID:       r.ID,
			SchemaFP: r.SchemaFp,
			ByteLen:  r.ByteLen,
		})
	}

	// Defence in depth against a database an older or buggier binary wrote.
	// [Store.Quarantine] refuses to mark part of a sealed batch, so this cannot
	// arise from this build — but a batch that lost some members to quarantine
	// would be replayed here as a *shorter* batch under the key computed over
	// the longer one, and the retry would then overwrite a correctly uploaded
	// object with a truncated one. That is silent data loss under a key that
	// still looks right, so it is refused rather than skipped.
	for b, n := range quarantined {
		if live[b] > 0 {
			return nil, errdef.NewStagingError(errdef.StagingKindState, s.path, fmt.Sprintf(
				"batch %s of table %s.%s has %d quarantined rows and %d live ones; a partly-quarantined batch would replay under a key computed over rows it no longer contains",
				b.key, b.table.Namespace, b.table.Table, n, live[b]), nil)
		}
	}

	return ix, nil
}

// Counters returns the staged totals the pass measured, including quarantined
// rows. The pass made at process startup is the one whose totals seed the
// in-memory ceiling counters. A pass made to open a writer measures the same
// thing and its caller ignores the result, because by then the counters are
// live and are moved by each accepted record and each committed batch.
func (ix *Index) Counters() Counters { return ix.counters }

// Tables returns the identities of every table with unclaimed pending rows, in
// the order their first pending row was staged.
//
// Rows belonging to a table no writer claims in a given run stay in staging
// untouched and keep counting toward the ceiling. That is deliberate: they are
// accepted, undelivered data, and discarding them because this run's code no
// longer declares that table would be silent data loss. A service retiring a
// table drains it — opens it once more and lets it flush — or deletes its rows
// knowingly.
func (ix *Index) Tables() []TableID {
	out := make([]TableID, 0, len(ix.order))
	for _, id := range ix.order {
		if _, ok := ix.groups[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// pendingBatches returns one table's pending batches in replay order, without
// consuming them.
//
// Batches come back in the order they were sealed — first row id order —
// followed by the unsealed tail, whose Key is empty. That ordering is what
// keeps a table's snapshots in insert order: an already-sealed batch commits
// before anything staged after it.
func (ix *Index) pendingBatches(namespace, table string) []PendingBatch {
	batches, ok := ix.groups[TableID{Namespace: namespace, Table: table}]
	if !ok {
		return nil
	}

	// The unsealed tail is moved to the end. Everything before it is already in
	// seal order, because a batch first appears in the scan at its lowest row
	// id and the scan runs in ascending id order.
	out := make([]PendingBatch, 0, len(batches))
	var unsealed []PendingBatch
	for _, b := range batches {
		if b.Key == "" {
			unsealed = append(unsealed, b)

			continue
		}
		out = append(out, b)
	}

	return append(out, unsealed...)
}

// Claim takes one table's pending batches out of the index, so nothing can hand
// the same rows to two writers.
//
// Claiming a table with nothing pending, or claiming one twice, returns no
// batches and is not an error.
func (ix *Index) Claim(namespace, table string) []PendingBatch {
	out := ix.pendingBatches(namespace, table)
	delete(ix.groups, TableID{Namespace: namespace, Table: table})

	return out
}
