package staging

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/schemamap"
)

// These are TESTING.md's second sanctioned category — a package-internal
// behaviour test of a layer that owns one local file — and this file holds both
// of the two claim shapes that category allows. There are fifteen tests, and
// every one of them is placed below by name.
//
// Eight durability tests hold the first shape: the caller-facing half is proven
// end to end in scenarios 1, 6 and 8, where records survive a process, a sealed
// batch commits after a restart and a drained writer leaves nothing staged, and
// these pin the same seam one call at a time, against the file itself rather
// than through a bucket and a catalog. They are
// TestStagedRecordsSurviveAReopen…,
// TestSealStampsTheBatchKeyAndItSurvivesAReopen, TestReplayReassemblesEach…,
// TestDeletePrunesCommittedRows…,
// TestSealReencodesRowsUnderTheCurrentShape, TestAppendRecordsTheShape…,
// TestTwoTablesSharingAKeyStringAreTwoBatches and
// TestQuarantinedRowsLeaveReplayButKeepCountingTowardTheCeiling. Each of those
// eight closes the store and opens the file again rather than asserting against
// in-process state, because a claim about what survives a call can only honestly
// be made against the file.
//
// TestCountersAgreeWithTheReplayScan holds the first shape too and is listed
// apart from them, because it is not a durability claim and deliberately does
// not reopen: what it asserts is that two query paths over the same open
// database agree — the cheap aggregate the ceiling is enforced from, and the
// ordered replay pass — and a reopen would add nothing to that. Its
// caller-facing half is scenario 7's staging ceiling, which is only as
// trustworthy as the agreement, since a bound enforced from a counter that had
// drifted from the scan would refuse the wrong writes. (Corrected at the
// completeness audit's fifth round, 2026-08-01. This test used to sit inside the
// durability list, in a paragraph that then said every one of those tests closed
// the store and opened the file again. Eleven of this file's fifteen tests do;
// this is one of the four that do not, the other three being
// TestSealRefusesARowAnotherBatchAlreadyOwns,
// TestSealRefusesRowsFromAnotherTable and
// TestRowsFailsLoudlyWhenAStagedRowIsGone — so the sentence was true of the
// group it named only if this test was not in it. The defect was found by the
// round's review of its own findings rather than by any of the passes that read
// this file, and it sat one level below the header the round before had just
// corrected — which is the pattern this audit keeps finding, and the reason it
// is written down here rather than tidied away.)
//
// (Corrected again at the fifth round's repair pass, 2026-08-01, and what is
// being corrected this time is the correction directly above. It first read
// "Ten of this file's fifteen tests do; this is one of the five that do not",
// and both figures were wrong by one test. The cause matters more than the
// numbers: the count was taken by matching `reopen(t,` inside each test
// function, which counts calls to the helper defined below rather than the
// behaviour the sentence actually claims, which is close-then-open.
// TestScanRefusesAPartlyQuarantinedBatch does close the store and open the same
// file again, but by hand — closeStore, a direct write into the file, then
// Open of the same path — because it has to edit the database between the close
// and the open, and a check looking for the helper's name could not see it. So
// the paragraph whose whole subject is a miscount was itself miscounted, in
// the same shape it describes: right at the level it looked, wrong one level
// below.)
//
// Five refusals hold the second shape: they are defences this layer keeps
// against its own in-process callers, and the public path is built so nothing
// reaches them. Sealing a row another batch already owns, sealing across two
// tables, quarantining part of a sealed batch, scanning a partly quarantined
// batch and asking for a staged row that is gone are all impossible through
// OpenWriter, which allows one live writer per table and drives the
// seal/scan/commit order itself — so an end-to-end test could only ever show
// that today's public path does not reach them, while the store would still be
// wrong on the day a second caller inside this process does. What each defends
// is the same thing: a staged row belongs to exactly one batch, and a batch's
// key names exactly its own bytes. Lose either and a committed table gains rows
// twice or loses them silently, which is the failure this whole layer exists to
// make impossible. Three of the five assert against the store they opened,
// since the claim is the refusal itself:
// TestSealRefusesARowAnotherBatchAlreadyOwns,
// TestSealRefusesRowsFromAnotherTable and
// TestRowsFailsLoudlyWhenAStagedRowIsGone.
// The other two close the store and open the file again, for two different
// reasons. TestQuarantineRefusesPartOfASealedBatch reopens because it does not
// stop at the refusal: it goes on to assert what the refused call left behind,
// which is a claim about the file. TestScanRefusesAPartlyQuarantinedBatch
// closes the store, writes the split state into the file directly and opens it
// again, because the state it refuses cannot be produced through this package's
// own API at all — the quarantine guard makes it unreachable — so the only way
// to present that state to Scan is to put it in the file while nothing holds it
// open.
//
// TestSealRollsBackEveryRowWhenOneIsRefused is the fifteenth and also holds the
// second shape, and it is named separately because it defends a different
// invariant from those five (named at the completeness audit's fifth round,
// 2026-08-01, when this header was found to account for fourteen of the file's
// fifteen tests — the test itself is sound and has not changed). The five prove
// that a bad row is refused. This one proves the refusal is all-or-nothing: a
// seal over several rows, one of which another batch already owns, must leave
// every row exactly as it was rather than keeping the stamps it had already
// written before it reached the bad one. What that costs to lose is worse than
// any single refusal, because the damage is silent — rows would carry a key
// whose hash was computed over a set that includes rows still sitting unsealed,
// so the next run would batch those separately and upload two objects for one
// batch, neither of them named by its own bytes. It reopens the file to check,
// since "left exactly as it was" is a claim about what is on disk.
//
// They are scenario-style tests against a real staging.db on disk, driven
// through this package's own surface.
//
// Payloads are produced by the real canonical encoder from real declarations,
// never by hand-written bytes: staging's whole job is to hand back exactly what
// a later flush will hash and upload, and a fixture of invented bytes would not
// be that.

// fill is TESTING.md's Case A declaration: pure structured facts.
type fill struct {
	Symbol      string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side        string `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price       int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity    int64  `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID  int64  `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID     string `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source      string `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
	VenueTimeMS int64  `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
}

// fillEvolved is Case A after the schema-evolution step: one extra optional
// column carrying a fresh field id.
type fillEvolved struct {
	Symbol         string  `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side           string  `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price          int64   `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity       int64   `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID     int64   `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID        string  `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source         string  `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
	VenueTimeMS    int64   `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
	VenueOrderType *string `parquet:"name=venue_order_type, logical=String, fieldid=9" arrow:"venue_order_type"`
}

// eventArchive is TESTING.md's Case B declaration: metadata columns plus a raw
// binary payload column. It shares the one staging.db with the fills table,
// which is the shared-file decision this package exists under.
type eventArchive struct {
	SourceID        string `parquet:"name=source_id, logical=String, fieldid=1" arrow:"source_id"`
	EventKind       string `parquet:"name=event_kind, logical=String, fieldid=2" arrow:"event_kind"`
	SequenceOrdinal int64  `parquet:"name=sequence_ordinal, fieldid=3" arrow:"sequence_ordinal"`
	ObservedAtMS    int64  `parquet:"name=observed_at_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=4" arrow:"observed_at_ms"`
	Payload         []byte `parquet:"name=payload, type=BYTE_ARRAY, fieldid=5" arrow:"payload"`
	PayloadSHA256   string `parquet:"name=payload_sha256, logical=String, fieldid=6" arrow:"payload_sha256"`
}

const (
	fillNamespace    = "market"
	fillTable        = "fills"
	archiveNamespace = "platform"
	archiveTable     = "event_archive"
)

// stagedAt is a fixed time for created_at. The column is diagnostic only and is
// never read by the write path, so a constant keeps the fixtures deterministic
// without pretending the value matters.
var stagedAt = time.UnixMilli(1_760_000_000_000).UTC()

// describe derives the recorded shape of a declaration through the real
// derivation chain, exactly as the layer above will.
func describe[T any](t *testing.T, namespace, table string) *canon.Descriptor {
	t.Helper()

	decl, err := schemamap.Declare[T](namespace, table)
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}
	d, err := canon.Describe(decl.Table(), decl.IcebergSchema())
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	return d
}

// fillValues is one Case A record's values in ascending field-id order, which
// is the order the canonical encoding writes them in.
func fillValues(seq int64) canon.Row {
	return canon.Row{
		"BTC-USD",
		"buy",
		int64(64_250_000_000_000),
		int64(1_500_000_000),
		seq,
		fmt.Sprintf("order-%04d", seq),
		"venue_one",
		int64(1_760_000_000_000) + seq,
	}
}

// archiveValues is one Case B record's values, including a raw byte payload.
func archiveValues(seq int64) canon.Row {
	return canon.Row{
		"stream_one",
		"created",
		seq,
		int64(1_760_000_000_000) + seq,
		[]byte{0x82, 0xa1, 0x61, byte(seq)},
		"0000000000000000000000000000000000000000000000000000000000000000",
	}
}

// encode runs the real canonical encoder over one row.
func encode(t *testing.T, table string, d *canon.Descriptor, row canon.Row) []byte {
	t.Helper()

	payload, err := canon.Encode(table, d, row)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return payload
}

// fillRow builds one staged Case A record.
func fillRow(t *testing.T, seq int64) AppendRow {
	t.Helper()

	d := describe[fill](t, fillNamespace, fillTable)
	return AppendRow{
		Namespace:  fillNamespace,
		Table:      fillTable,
		Descriptor: d,
		Payload:    encode(t, fillTable, d, fillValues(seq)),
		CreatedAt:  stagedAt,
	}
}

// archiveRow builds one staged Case B record.
func archiveRow(t *testing.T, seq int64) AppendRow {
	t.Helper()

	d := describe[eventArchive](t, archiveNamespace, archiveTable)
	return AppendRow{
		Namespace:  archiveNamespace,
		Table:      archiveTable,
		Descriptor: d,
		Payload:    encode(t, archiveTable, d, archiveValues(seq)),
		CreatedAt:  stagedAt,
	}
}

// openStore opens a staging database in the test's own temp directory.
func openStore(t *testing.T) (*Store, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "staging.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

// reopen closes a store and opens the same file again, which is the only honest
// way to assert that something is durable rather than merely remembered.
func reopen(t *testing.T, s *Store, path string) *Store {
	t.Helper()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	next, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = next.Close() })
	return next
}

func closeStore(t *testing.T, s *Store) {
	t.Helper()

	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// appendAll stages a sequence of records and returns the ids they were given.
func appendAll(t *testing.T, s *Store, rows ...AppendRow) []int64 {
	t.Helper()

	ids := make([]int64, 0, len(rows))
	for _, r := range rows {
		id, err := s.Append(t.Context(), r)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

// pendingIDs flattens a claimed batch's row ids.
func pendingIDs(b PendingBatch) []int64 {
	out := make([]int64, 0, len(b.Rows))
	for _, r := range b.Rows {
		out = append(out, r.ID)
	}
	return out
}

// TestStagedRecordsSurviveAReopenAndStayInTheirOwnTable is the durability claim
// this whole package exists for, plus the shared-staging-file decision: two
// tables' records go into one file, and the single ordered replay scan routes
// each table's rows back to it with neither table's rows appearing in the
// other's.
func TestStagedRecordsSurviveAReopenAndStayInTheirOwnTable(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	fills := appendAll(t, s, fillRow(t, 1), fillRow(t, 2), fillRow(t, 3))
	archives := appendAll(t, s, archiveRow(t, 1), archiveRow(t, 2))

	s = reopen(t, s, path)

	ix, err := s.Scan(t.Context())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got, want := ix.Counters().Records, int64(len(fills)+len(archives)); got != want {
		t.Errorf("staged records = %d, want %d", got, want)
	}
	if ix.Counters().Bytes <= 0 {
		t.Errorf("staged bytes = %d, want a positive total", ix.Counters().Bytes)
	}

	wantTables := []TableID{
		{Namespace: fillNamespace, Table: fillTable},
		{Namespace: archiveNamespace, Table: archiveTable},
	}
	if got := ix.Tables(); !slices.Equal(got, wantTables) {
		t.Errorf("Tables() = %v, want %v", got, wantTables)
	}

	claimed := ix.Claim(fillNamespace, fillTable)
	if len(claimed) != 1 {
		t.Fatalf("fills claimed %d batches, want 1", len(claimed))
	}
	if claimed[0].Key != "" {
		t.Errorf("unsealed batch has key %q, want empty", claimed[0].Key)
	}
	if got := pendingIDs(claimed[0]); !slices.Equal(got, fills) {
		t.Errorf("fills row ids = %v, want %v", got, fills)
	}

	// Claiming is a claim: the second writer for the same table gets nothing,
	// which is what stops two writers replaying the same rows.
	if got := ix.Claim(fillNamespace, fillTable); len(got) != 0 {
		t.Errorf("second Claim returned %d batches, want 0", len(got))
	}
	if got, want := ix.Tables(), []TableID{{Namespace: archiveNamespace, Table: archiveTable}}; !slices.Equal(got, want) {
		t.Errorf("Tables() after claiming fills = %v, want %v", got, want)
	}

	// And the payloads come back byte-identical, decodable by the shape recorded
	// alongside them rather than by anything the current binary declares.
	rows, err := s.Rows(t.Context(), fills)
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	for i, r := range rows {
		d, err := s.Schema(t.Context(), r.SchemaFP)
		if err != nil {
			t.Fatalf("Schema: %v", err)
		}
		got, err := canon.Decode(fillTable, d, r.Payload)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		want := fillValues(int64(i + 1))
		if !slices.Equal(got, want) {
			t.Errorf("row %d decoded to %v, want %v", r.ID, got, want)
		}
	}
}

// TestCountersAgreeWithTheReplayScan checks the two ways the ceiling's numbers
// can be obtained against each other. They are separate code paths — an
// aggregate and an ordered pass — and the ceiling is only as trustworthy as
// their agreement.
func TestCountersAgreeWithTheReplayScan(t *testing.T) {
	t.Parallel()

	s, _ := openStore(t)
	appendAll(t, s, fillRow(t, 1), fillRow(t, 2), archiveRow(t, 1))

	ix, err := s.Scan(t.Context())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	counters, err := s.Counters(t.Context())
	if err != nil {
		t.Fatalf("Counters: %v", err)
	}
	if counters != ix.Counters() {
		t.Errorf("Counters() = %+v, Scan counters = %+v", counters, ix.Counters())
	}
	if counters.Records != 3 {
		t.Errorf("records = %d, want 3", counters.Records)
	}

	// The byte total is the sum of exactly the payloads that were stored, which
	// is what makes it a bound on disk rather than an estimate of one.
	rows, err := s.Rows(t.Context(), []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	var want int64
	for _, r := range rows {
		want += int64(len(r.Payload))
	}
	if counters.Bytes != want {
		t.Errorf("bytes = %d, want %d", counters.Bytes, want)
	}
}

// TestSealStampsTheBatchKeyAndItSurvivesAReopen is the sealing claim: a batch is
// sealed durably before it is encoded, so a crash recovers the same batch with
// the same membership and the same key — which is what makes the content-hash
// object key idempotent in practice rather than only in theory.
func TestSealStampsTheBatchKeyAndItSurvivesAReopen(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	d := describe[fill](t, fillNamespace, fillTable)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2), fillRow(t, 3))

	rows, err := s.Rows(t.Context(), ids)
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	payloads := make([][]byte, 0, len(rows))
	for _, r := range rows {
		payloads = append(payloads, r.Payload)
	}
	key, err := canon.BatchHash(fillNamespace, fillTable, d, payloads)
	if err != nil {
		t.Fatalf("BatchHash: %v", err)
	}

	seal := SealRequest{Namespace: fillNamespace, Table: fillTable, BatchKey: key, Descriptor: d}
	for _, id := range ids {
		seal.Rows = append(seal.Rows, SealRow{ID: id})
	}
	if err := s.Seal(t.Context(), seal); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	s = reopen(t, s, path)

	ix, err := s.Scan(t.Context())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	claimed := ix.Claim(fillNamespace, fillTable)
	if len(claimed) != 1 {
		t.Fatalf("claimed %d batches, want 1", len(claimed))
	}
	if claimed[0].Key != key {
		t.Errorf("recovered batch key = %q, want %q", claimed[0].Key, key)
	}
	if got := pendingIDs(claimed[0]); !slices.Equal(got, ids) {
		t.Errorf("recovered batch membership = %v, want %v", got, ids)
	}

	// The stored key is authoritative, but with the schema unchanged across the
	// restart it must also be reproducible: recomputing the hash from the
	// replayed payloads has to land on the same name.
	replayed, err := s.Rows(t.Context(), pendingIDs(claimed[0]))
	if err != nil {
		t.Fatalf("Rows after reopen: %v", err)
	}
	recomputed := make([][]byte, 0, len(replayed))
	for _, r := range replayed {
		recomputed = append(recomputed, r.Payload)
		if r.BatchKey != key {
			t.Errorf("row %d carries batch key %q, want %q", r.ID, r.BatchKey, key)
		}
	}
	again, err := canon.BatchHash(fillNamespace, fillTable, d, recomputed)
	if err != nil {
		t.Fatalf("BatchHash after reopen: %v", err)
	}
	if again != key {
		t.Errorf("recomputed key = %q, want the stored %q", again, key)
	}
}

// TestSealRefusesARowAnotherBatchAlreadyOwns proves the seal-exactly-once
// guard is structural. A record silently moving from one sealed batch to
// another would change both batches' contents without changing either's stored
// key, which is the one way content-hash keying can lie.
func TestSealRefusesARowAnotherBatchAlreadyOwns(t *testing.T) {
	t.Parallel()

	s, _ := openStore(t)
	d := describe[fill](t, fillNamespace, fillTable)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2))

	first := SealRequest{Namespace: fillNamespace, Table: fillTable, BatchKey: "aaaa", Descriptor: d, Rows: []SealRow{{ID: ids[0]}, {ID: ids[1]}}}
	if err := s.Seal(t.Context(), first); err != nil {
		t.Fatalf("first Seal: %v", err)
	}

	second := SealRequest{Namespace: fillNamespace, Table: fillTable, BatchKey: "bbbb", Descriptor: d, Rows: []SealRow{{ID: ids[1]}}}
	err := s.Seal(t.Context(), second)
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("second Seal error is not a StagingError: %#v", err)
	}
	if se.Kind != errdef.StagingKindState {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindState)
	}

	// And the refusal changed nothing: the row still belongs to the first batch.
	rows, err := s.Rows(t.Context(), ids)
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	for _, r := range rows {
		if r.BatchKey != "aaaa" {
			t.Errorf("row %d has batch key %q, want %q", r.ID, r.BatchKey, "aaaa")
		}
	}
}

// TestSealRefusesRowsFromAnotherTable proves a batch is exactly one table's
// rows. The batch key is computed with the namespace and table name as hash
// inputs and the object it names lives under that table's own prefix, so a
// batch spanning two tables would carry a key that is wrong for at least one of
// them and would file half its records under the wrong table.
func TestSealRefusesRowsFromAnotherTable(t *testing.T) {
	t.Parallel()

	s, _ := openStore(t)
	d := describe[fill](t, fillNamespace, fillTable)
	mine := appendAll(t, s, fillRow(t, 1))
	theirs := appendAll(t, s, archiveRow(t, 1))

	err := s.Seal(t.Context(), SealRequest{
		Namespace:  fillNamespace,
		Table:      fillTable,
		BatchKey:   "mixed",
		Descriptor: d,
		Rows:       []SealRow{{ID: mine[0]}, {ID: theirs[0]}},
	})
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("sealing across two tables did not fail loudly: %#v", err)
	}
	if se.Kind != errdef.StagingKindState {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindState)
	}

	// Nothing was stamped: the refusal took the whole transaction with it.
	rows, err := s.Rows(t.Context(), append(mine, theirs...))
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	for _, r := range rows {
		if r.BatchKey != "" {
			t.Errorf("row %d was sealed into %q despite the refusal", r.ID, r.BatchKey)
		}
	}
}

// TestSealRollsBackEveryRowWhenOneIsRefused proves the seal is atomic. A partial
// seal would be the worst of both worlds: some rows stamped with a key whose
// hash was computed over a set that includes rows still sitting unsealed, so the
// next run would batch those separately and upload two objects for one batch.
func TestSealRollsBackEveryRowWhenOneIsRefused(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	d := describe[fill](t, fillNamespace, fillTable)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2), fillRow(t, 3))

	// The third row is taken by another batch first, so the three-row seal
	// below fails on its last row after the first two have already been updated.
	if err := s.Seal(t.Context(), SealRequest{
		Namespace: fillNamespace, Table: fillTable, BatchKey: "owner", Descriptor: d,
		Rows: []SealRow{{ID: ids[2]}},
	}); err != nil {
		t.Fatalf("Seal of the owning batch: %v", err)
	}

	err := s.Seal(t.Context(), SealRequest{
		Namespace: fillNamespace, Table: fillTable, BatchKey: "doomed", Descriptor: d,
		Rows: []SealRow{{ID: ids[0]}, {ID: ids[1]}, {ID: ids[2]}},
	})
	var se errdef.StagingError
	if !errors.As(err, &se) || se.Kind != errdef.StagingKindState {
		t.Fatalf("Seal error = %#v, want a StagingError of kind %q", err, errdef.StagingKindState)
	}

	s = reopen(t, s, path)

	rows, err := s.Rows(t.Context(), ids)
	if err != nil {
		t.Fatalf("Rows after reopen: %v", err)
	}
	for i, r := range rows {
		want := ""
		if i == 2 {
			want = "owner"
		}
		if r.BatchKey != want {
			t.Errorf("row %d batch key = %q, want %q", r.ID, r.BatchKey, want)
		}
	}
}

// TestQuarantineRefusesPartOfASealedBatch closes the same hole the seal guard
// closes, through the quarantine door. Marking some of a sealed batch's rows
// would leave a batch whose stored key no longer names its bytes: replay would
// offer the survivors under the key computed over all of them, and the retry
// would overwrite a correct object with a truncated one.
func TestQuarantineRefusesPartOfASealedBatch(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	d := describe[fill](t, fillNamespace, fillTable)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2), fillRow(t, 3))

	if err := s.Seal(t.Context(), SealRequest{
		Namespace: fillNamespace, Table: fillTable, BatchKey: "sealed", Descriptor: d,
		Rows: []SealRow{{ID: ids[0]}, {ID: ids[1]}, {ID: ids[2]}},
	}); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	err := s.Quarantine(t.Context(), ids[:2], stagedAt, errors.New("encode failed"))
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("quarantining part of a sealed batch did not fail loudly: %#v", err)
	}
	if se.Kind != errdef.StagingKindState {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindState)
	}
	// Naming the same row twice must not make a partial batch look complete.
	if err := s.Quarantine(t.Context(), []int64{ids[0], ids[0], ids[1]}, stagedAt, errors.New("encode failed")); err == nil {
		t.Error("a duplicated id passed the whole-batch guard")
	}

	// The whole batch is accepted, and then replay is empty rather than short.
	if err := s.Quarantine(t.Context(), ids, stagedAt, errors.New("encode failed")); err != nil {
		t.Fatalf("quarantining the whole batch: %v", err)
	}

	s = reopen(t, s, path)

	ix, err := s.Scan(t.Context())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got := ix.Claim(fillNamespace, fillTable); len(got) != 0 {
		t.Errorf("replay offered %d batches, want none — the whole batch is quarantined", len(got))
	}
	if got, want := ix.Counters().Records, int64(3); got != want {
		t.Errorf("records = %d, want %d — quarantined rows still count", got, want)
	}
}

// TestTwoTablesSharingAKeyStringAreTwoBatches closes the last asymmetry between
// sealing and the two checks that read batches back. Sealing has always
// identified a batch by table as well as key; the whole-batch quarantine guard
// and the split check keyed on the key alone, so a key string appearing under
// two tables would have made each look like a fragment of the other's batch.
// Real keys are SHA-256 digests, so this cannot happen by accident — the point
// is that the invariant is now exact rather than merely improbable.
func TestTwoTablesSharingAKeyStringAreTwoBatches(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	const shared = "same_key_string"

	fills := appendAll(t, s, fillRow(t, 1), fillRow(t, 2))
	archives := appendAll(t, s, archiveRow(t, 1), archiveRow(t, 2))

	if err := s.Seal(t.Context(), SealRequest{
		Namespace: fillNamespace, Table: fillTable, BatchKey: shared,
		Descriptor: describe[fill](t, fillNamespace, fillTable),
		Rows:       []SealRow{{ID: fills[0]}, {ID: fills[1]}},
	}); err != nil {
		t.Fatalf("sealing the fills batch: %v", err)
	}
	if err := s.Seal(t.Context(), SealRequest{
		Namespace: archiveNamespace, Table: archiveTable, BatchKey: shared,
		Descriptor: describe[eventArchive](t, archiveNamespace, archiveTable),
		Rows:       []SealRow{{ID: archives[0]}, {ID: archives[1]}},
	}); err != nil {
		t.Fatalf("sealing the archive batch: %v", err)
	}

	// The guard must count only the fills table's rows. Scoped to the key alone
	// it would see four members against two named and refuse a complete batch.
	if err := s.Quarantine(t.Context(), fills, stagedAt, errors.New("encode failed")); err != nil {
		t.Fatalf("quarantining a whole batch whose key another table also uses: %v", err)
	}

	s = reopen(t, s, path)

	// And the split check must not see one table's quarantined batch and the
	// other's live batch as one half-quarantined batch.
	ix, err := s.Scan(t.Context())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got := ix.Claim(fillNamespace, fillTable); len(got) != 0 {
		t.Errorf("fills offered %d batches, want none — its batch is quarantined", len(got))
	}
	claimed := ix.Claim(archiveNamespace, archiveTable)
	if len(claimed) != 1 || claimed[0].Key != shared {
		t.Fatalf("archive claimed %+v, want one batch keyed %q", claimed, shared)
	}
	if got := pendingIDs(claimed[0]); !slices.Equal(got, archives) {
		t.Errorf("archive batch = %v, want %v", got, archives)
	}
}

// TestScanRefusesAPartlyQuarantinedBatch is the second line of the same defence,
// for a database this build could not have written but an older or buggier one
// could have. The split state is produced by writing to the file directly,
// because the guard above makes it unreachable through the package's own API —
// which is the point: the check has to hold for files, not just for callers.
func TestScanRefusesAPartlyQuarantinedBatch(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	d := describe[fill](t, fillNamespace, fillTable)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2), fillRow(t, 3))
	if err := s.Seal(t.Context(), SealRequest{
		Namespace: fillNamespace, Table: fillTable, BatchKey: "sealed", Descriptor: d,
		Rows: []SealRow{{ID: ids[0]}, {ID: ids[1]}, {ID: ids[2]}},
	}); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	closeStore(t, s)

	inspect(t, path, func(db *sql.DB) {
		if _, err := db.Exec("UPDATE staging SET quarantined_at = 1 WHERE id = ?", ids[1]); err != nil {
			t.Fatalf("writing the split state: %v", err)
		}
	})

	next, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeStore(t, next)

	ix, err := next.Scan(t.Context())
	if ix != nil {
		t.Error("Scan returned an index for a partly-quarantined batch")
	}
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("Scan error is not a StagingError: %#v", err)
	}
	if se.Kind != errdef.StagingKindState {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindState)
	}
}

// TestSealReencodesRowsUnderTheCurrentShape covers the seal-time transcode: a
// row staged before a schema change is re-encoded in the same transaction that
// stamps its batch key, so the sealed batch is fingerprint-homogeneous and the
// batch hash is computed over payload bytes that agree with the schema
// component of its own input.
func TestSealReencodesRowsUnderTheCurrentShape(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	old := describe[fill](t, fillNamespace, fillTable)
	current := describe[fillEvolved](t, fillNamespace, fillTable)
	if old.Fingerprint() == current.Fingerprint() {
		t.Fatal("test setup: the two shapes must have different fingerprints")
	}

	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2))

	staged, err := s.Rows(t.Context(), ids)
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	seal := SealRequest{Namespace: fillNamespace, Table: fillTable, Descriptor: current}
	payloads := make([][]byte, 0, len(staged))
	for _, r := range staged {
		recoded, err := canon.Transcode(fillTable, old, current, r.Payload)
		if err != nil {
			t.Fatalf("Transcode: %v", err)
		}
		payloads = append(payloads, recoded)
		seal.Rows = append(seal.Rows, SealRow{ID: r.ID, Payload: recoded})
	}
	key, err := canon.BatchHash(fillNamespace, fillTable, current, payloads)
	if err != nil {
		t.Fatalf("BatchHash: %v", err)
	}
	seal.BatchKey = key
	if err := s.Seal(t.Context(), seal); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	s = reopen(t, s, path)

	sealed, err := s.Rows(t.Context(), ids)
	if err != nil {
		t.Fatalf("Rows after reopen: %v", err)
	}
	for i, r := range sealed {
		if r.SchemaFP != current.Fingerprint() {
			t.Errorf("row %d fingerprint = %s, want the current %s", r.ID, r.SchemaFP, current.Fingerprint())
		}
		if !slices.Equal(r.Payload, payloads[i]) {
			t.Errorf("row %d payload was not replaced by its re-encoded form", r.ID)
		}
		// The added column decodes as null, which is what a field added after a
		// row was staged has to mean.
		values, err := canon.Decode(fillTable, current, r.Payload)
		if err != nil {
			t.Fatalf("Decode under the current shape: %v", err)
		}
		if values[len(values)-1] != nil {
			t.Errorf("row %d added column = %v, want nil", r.ID, values[len(values)-1])
		}
	}

	// The sealing descriptor was recorded, so a crash right here still leaves
	// every sealed row decodable.
	if _, err := s.Schema(t.Context(), current.Fingerprint()); err != nil {
		t.Errorf("sealing descriptor was not recorded: %v", err)
	}

	// And once no live row names the old shape any more, its descriptor goes.
	removed, err := s.PruneSchemas(t.Context())
	if err != nil {
		t.Fatalf("PruneSchemas: %v", err)
	}
	if removed != 1 {
		t.Errorf("PruneSchemas removed %d descriptors, want 1", removed)
	}
	// Typed, not merely non-nil. A fingerprint with no recorded descriptor is
	// a corrupt store rather than a normal miss — every path that stages a row
	// records its descriptor in the same transaction — so what a caller is
	// handed has to say so. An assertion that some error came back would be
	// satisfied by a read failure, which means the opposite thing about the
	// database and about what to do next.
	desc, err := s.Schema(t.Context(), old.Fingerprint())
	if err == nil {
		t.Error("the old descriptor is still recorded after pruning")
	}
	if desc != nil {
		t.Error("Schema returned a descriptor alongside its error")
	}
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("error is %T, want errdef.StagingError", err)
	}
	if se.Kind != errdef.StagingKindState {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindState)
	}
	if _, err := s.Schema(t.Context(), current.Fingerprint()); err != nil {
		t.Errorf("pruning removed a descriptor that is still referenced: %v", err)
	}
}

// TestReplayReassemblesEachSealedBatchSeparately proves the index groups by
// stored batch key rather than by adjacency. Batches are contiguous in ordinary
// operation, so this stages them interleaved on purpose: grouping that happened
// to work only because rows arrive in batch order would be a grouping that
// breaks the first time it matters.
func TestReplayReassemblesEachSealedBatchSeparately(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	d := describe[fill](t, fillNamespace, fillTable)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2), fillRow(t, 3), fillRow(t, 4), fillRow(t, 5))

	seal := func(key string, rows ...int64) {
		t.Helper()
		req := SealRequest{Namespace: fillNamespace, Table: fillTable, BatchKey: key, Descriptor: d}
		for _, id := range rows {
			req.Rows = append(req.Rows, SealRow{ID: id})
		}
		if err := s.Seal(t.Context(), req); err != nil {
			t.Fatalf("Seal %s: %v", key, err)
		}
	}
	seal("batch_one", ids[0], ids[2])
	seal("batch_two", ids[1], ids[3])
	// ids[4] stays unsealed.

	s = reopen(t, s, path)

	ix, err := s.Scan(t.Context())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	claimed := ix.Claim(fillNamespace, fillTable)
	if len(claimed) != 3 {
		t.Fatalf("claimed %d batches, want 3", len(claimed))
	}
	if claimed[0].Key != "batch_one" || claimed[1].Key != "batch_two" {
		t.Errorf("sealed batches came back as %q then %q, want batch_one then batch_two", claimed[0].Key, claimed[1].Key)
	}
	if claimed[2].Key != "" {
		t.Errorf("last batch key = %q, want the unsealed tail", claimed[2].Key)
	}
	if got, want := pendingIDs(claimed[0]), []int64{ids[0], ids[2]}; !slices.Equal(got, want) {
		t.Errorf("batch_one = %v, want %v", got, want)
	}
	if got, want := pendingIDs(claimed[1]), []int64{ids[1], ids[3]}; !slices.Equal(got, want) {
		t.Errorf("batch_two = %v, want %v", got, want)
	}
	if got, want := pendingIDs(claimed[2]), []int64{ids[4]}; !slices.Equal(got, want) {
		t.Errorf("unsealed tail = %v, want %v", got, want)
	}
}

// TestDeletePrunesCommittedRowsAndIsSafeToRepeat covers the prune-on-commit
// path, including the re-run a crash between the catalog commit and the prune
// forces. Ids are never reused after a delete, so replay order stays insert
// order for whatever is still staged.
func TestDeletePrunesCommittedRowsAndIsSafeToRepeat(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2), fillRow(t, 3))

	// The prune deletes the *highest* ids, which is the real shape of a
	// post-commit prune and the only shape that can tell AUTOINCREMENT apart
	// from a plain INTEGER PRIMARY KEY. A plain key reuses the largest id once
	// its row is gone; AUTOINCREMENT never does. Deleting from the middle would
	// pass either way, and would therefore prove nothing about the column type
	// replay order depends on.
	highest := ids[1:]
	if err := s.Delete(t.Context(), highest); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// The re-run after a crash mid-prune must succeed, not fail on rows that
	// are already gone.
	if err := s.Delete(t.Context(), highest); err != nil {
		t.Fatalf("repeated Delete: %v", err)
	}

	// Reopened first, because the counter AUTOINCREMENT keeps is a row in
	// sqlite_sequence rather than in-memory state, and the claim being made is
	// that it survives a restart.
	s = reopen(t, s, path)

	next, err := s.Append(t.Context(), fillRow(t, 4))
	if err != nil {
		t.Fatalf("Append after delete: %v", err)
	}
	if slices.Contains(ids, next) {
		t.Errorf("new row reused id %d, which a deleted row already had", next)
	}
	if want := ids[len(ids)-1]; next <= want {
		t.Errorf("new row got id %d, want one above the highest ever used (%d)", next, want)
	}

	ix, err := s.Scan(t.Context())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got, want := ix.Counters().Records, int64(2); got != want {
		t.Errorf("records after pruning = %d, want %d", got, want)
	}
	claimed := ix.Claim(fillNamespace, fillTable)
	if len(claimed) != 1 {
		t.Fatalf("claimed %d batches, want 1", len(claimed))
	}
	if got, want := pendingIDs(claimed[0]), []int64{ids[0], next}; !slices.Equal(got, want) {
		t.Errorf("surviving rows = %v, want %v in insert order", got, want)
	}
}

// TestRowsFailsLoudlyWhenAStagedRowIsGone covers the read-back half of the same
// invariant. The caller learned these ids from this store, so a row that is not
// there means something outside this process removed it — and quietly returning
// a shorter batch would upload a batch under a key naming a different set of
// records.
func TestRowsFailsLoudlyWhenAStagedRowIsGone(t *testing.T) {
	t.Parallel()

	s, _ := openStore(t)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2))
	if err := s.Delete(t.Context(), ids[:1]); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Rows(t.Context(), ids)
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("Rows error is not a StagingError: %#v", err)
	}
	if se.Kind != errdef.StagingKindState {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindState)
	}
}

// TestQuarantinedRowsLeaveReplayButKeepCountingTowardTheCeiling proves both
// halves of the quarantine decision at once. Excluding them from replay is what
// stops a doomed batch becoming an infinite retry loop; keeping them in the
// counters is what stops a growing quarantine silently eating the headroom the
// ceiling is supposed to bound.
func TestQuarantinedRowsLeaveReplayButKeepCountingTowardTheCeiling(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	ids := appendAll(t, s, fillRow(t, 1), fillRow(t, 2), fillRow(t, 3))

	cause := errdef.NewEncodingError(errdef.EncodingKindValue, fillTable, "symbol", 1, "unencodable")
	if err := s.Quarantine(t.Context(), ids[:2], stagedAt, cause); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}

	s = reopen(t, s, path)

	ix, err := s.Scan(t.Context())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got, want := ix.Counters().Records, int64(3); got != want {
		t.Errorf("records = %d, want %d — quarantined rows still count", got, want)
	}
	claimed := ix.Claim(fillNamespace, fillTable)
	if len(claimed) != 1 {
		t.Fatalf("claimed %d batches, want 1", len(claimed))
	}
	if got, want := pendingIDs(claimed[0]), []int64{ids[2]}; !slices.Equal(got, want) {
		t.Errorf("replayable rows = %v, want %v", got, want)
	}

	// The rows are still there for an operator to read, marked.
	rows, err := s.Rows(t.Context(), ids)
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	for i, r := range rows {
		if want := i < 2; r.Quarantined != want {
			t.Errorf("row %d quarantined = %v, want %v", r.ID, r.Quarantined, want)
		}
	}

	// A quarantined row is not sealable: it belongs to no future batch.
	err = s.Seal(t.Context(), SealRequest{
		Namespace:  fillNamespace,
		Table:      fillTable,
		BatchKey:   "cccc",
		Descriptor: describe[fill](t, fillNamespace, fillTable),
		Rows:       []SealRow{{ID: ids[0]}},
	})
	var se errdef.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("sealing a quarantined row did not fail loudly: %#v", err)
	}
	if se.Kind != errdef.StagingKindState {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, errdef.StagingKindState)
	}
}

// TestAppendRecordsTheShapeInTheSameTransaction is the invariant replay depends
// on: there is no window, not even a crash-sized one, in which a staged payload
// exists whose descriptor does not. A payload whose shape is unrecorded is
// undecodable by every binary, including the one that wrote it.
func TestAppendRecordsTheShapeInTheSameTransaction(t *testing.T) {
	t.Parallel()

	s, path := openStore(t)
	d := describe[fill](t, fillNamespace, fillTable)
	appendAll(t, s, fillRow(t, 1))

	s = reopen(t, s, path)

	got, err := s.Schema(t.Context(), d.Fingerprint())
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !slices.Equal(got.Bytes(), d.Bytes()) {
		t.Errorf("recorded descriptor bytes differ from the shape that was staged")
	}
	if got.Fingerprint() != d.Fingerprint() {
		t.Errorf("recorded fingerprint = %s, want %s", got.Fingerprint(), d.Fingerprint())
	}
}
