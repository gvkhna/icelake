package staging

import (
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestSummarizeCountsWaitingQuarantinedAndBacklog is the read-only summary's
// arithmetic, asserted against a database whose contents this test placed row
// by row: waiting and quarantined split exactly at quarantined_at, the bytes
// are the waiting rows' bytes alone, and the spool backlog counts only files
// no bucket has confirmed.
//
// It runs against the store while the store is still open, deliberately: the
// summary exists to be read while a daemon owns the database, so reading it
// beside a live writer connection is the case, not an edge.
func TestSummarizeCountsWaitingQuarantinedAndBacklog(t *testing.T) {
	s, path := openStore(t)
	ctx := t.Context()

	var ids []int64
	for seq := int64(1); seq <= 3; seq++ {
		id, err := s.Append(ctx, fillRow(t, seq))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		ids = append(ids, id)
	}
	if err := s.Quarantine(ctx, ids[:1], stagedAt, errors.New("an encoder failure")); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}

	d := describe[fill](t, fillNamespace, fillTable)
	for _, rec := range []SpoolRecord{
		{Namespace: fillNamespace, Table: fillTable, BatchKey: "aa00", Descriptor: d, ByteLen: 100, CreatedAt: stagedAt},
		{Namespace: fillNamespace, Table: fillTable, BatchKey: "bb11", Descriptor: d, ByteLen: 40, CreatedAt: stagedAt},
	} {
		if err := s.RecordSpoolFile(ctx, rec); err != nil {
			t.Fatalf("RecordSpoolFile: %v", err)
		}
	}
	if err := s.MarkSpoolCommitted(ctx, "aa00", stagedAt.Add(time.Second)); err != nil {
		t.Fatalf("MarkSpoolCommitted: %v", err)
	}

	sum, err := Summarize(ctx, path)
	if err != nil {
		t.Fatalf("Summarize: %v", err)
	}

	if sum.WaitingRecords != 2 {
		t.Errorf("WaitingRecords = %d, want 2", sum.WaitingRecords)
	}
	if sum.QuarantinedRecords != 1 {
		t.Errorf("QuarantinedRecords = %d, want 1", sum.QuarantinedRecords)
	}
	if sum.WaitingBytes <= 0 {
		t.Errorf("WaitingBytes = %d, want positive", sum.WaitingBytes)
	}
	// The quarantined row's bytes are excluded: waiting bytes are the bytes a
	// drain will move, and quarantine is exactly the promise that these never
	// will be.
	all, err := s.Counters(ctx)
	if err != nil {
		t.Fatalf("Counters: %v", err)
	}
	if sum.WaitingBytes >= all.Bytes {
		t.Errorf("WaitingBytes = %d, want less than the all-rows total %d once a row is quarantined",
			sum.WaitingBytes, all.Bytes)
	}

	if sum.SpoolBacklogFiles != 1 {
		t.Errorf("SpoolBacklogFiles = %d, want 1: the committed file is not backlog", sum.SpoolBacklogFiles)
	}
	if sum.SpoolBacklogBytes != 40 {
		t.Errorf("SpoolBacklogBytes = %d, want 40", sum.SpoolBacklogBytes)
	}
}

// TestSummarizeIsRefusedAWritingStatement pins what "read-only" rests on: the
// connection the summary opens carries SQLite's query_only pragma, so a write
// through it is refused by the engine rather than avoided by discipline. The
// test drives a write through the same DSN the summary uses, because the
// summary itself offers no way to try one — which is the point, and this is
// the proof one level below it.
func TestSummarizeIsRefusedAWritingStatement(t *testing.T) {
	s, path := openStore(t)
	if _, err := s.Append(t.Context(), fillRow(t, 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	db, err := sql.Open(driverName, readOnlyDSN(path))
	if err != nil {
		t.Fatalf("opening the read-only DSN: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(t.Context(), "DELETE FROM staging"); err == nil {
		t.Fatal("a DELETE through the summary's connection succeeded, want a query_only refusal")
	}
}

// TestSummarizeRefusesTheDSNCutPath mirrors Open's own guard: a '?' in the
// path would silently open a different file, and a summary of the wrong file
// is a report that lies.
func TestSummarizeRefusesTheDSNCutPath(t *testing.T) {
	if _, err := Summarize(t.Context(), "staging?.db"); err == nil {
		t.Fatal("a path containing '?' was accepted")
	}
}
