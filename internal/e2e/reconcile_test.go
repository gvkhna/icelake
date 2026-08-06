package e2e

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// FillV2 is Fill with one field appended: an optional venue order type,
// carrying the next field id after the table's current maximum.
//
// That discipline is the whole of the field-id rule for evolution. AddColumn
// takes no field-id argument — it assigns the id itself, from the table's own
// monotonic counter — so a new field's tag is only ever right if it names the
// number the table is about to hand out anyway.
type FillV2 struct {
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

// FillWrongNewID is FillV2 with the new field's id guessed rather than derived.
type FillWrongNewID struct {
	Symbol         string  `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side           string  `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price          int64   `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity       int64   `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID     int64   `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID        string  `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source         string  `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
	VenueTimeMS    int64   `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
	VenueOrderType *string `parquet:"name=venue_order_type, logical=String, fieldid=20" arrow:"venue_order_type"`
}

// FillRequiredNewField adds a field that is not a pointer, and therefore
// required.
type FillRequiredNewField struct {
	Symbol         string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side           string `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price          int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity       int64  `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID     int64  `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID        string `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source         string `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
	VenueTimeMS    int64  `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
	VenueOrderType string `parquet:"name=venue_order_type, logical=String, fieldid=9" arrow:"venue_order_type"`
}

// FillRenamed is Fill with column 7 renamed. Only the name changes; the field
// id is what says it is the same column, which is the reason a rename is
// expressible at all.
type FillRenamed struct {
	Symbol      string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side        string `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price       int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity    int64  `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID  int64  `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID     string `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Venue       string `parquet:"name=venue, logical=String, fieldid=7" arrow:"venue"`
	VenueTimeMS int64  `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
}

// narrowCounter and wideCounter are the same table with column 2 declared at
// two different integer widths, which is the safe type change Iceberg allows
// and the unsafe one it refuses, depending on which direction it is applied in.
type narrowCounter struct {
	Name  string `parquet:"name=name, logical=String, fieldid=1" arrow:"name"`
	Count int32  `parquet:"name=count, fieldid=2" arrow:"count"`
}

type wideCounter struct {
	Name  string `parquet:"name=name, logical=String, fieldid=1" arrow:"name"`
	Count int64  `parquet:"name=count, fieldid=2" arrow:"count"`
}

// TestReconcileAddsAnOptionalColumn proves the evolution path SCHEMA.md's
// reconciliation loop exists for, and is also TESTING.md's scenario 1 Case A
// schema-evolution clause in full: a table with real committed data behind it
// gains a column, and reading it back gives one table across the boundary.
//
// The counts are what that clause asks for and are load-bearing rather than
// arbitrary: several batches are written and committed under the original
// schema, and several more after the change, so the boundary has real data
// files and real snapshots on both sides of it rather than one of each. A
// single file either side would still pass every assertion below while proving
// nothing about a table that had been collecting for a while, which is the only
// kind of table anyone evolves.
//
// The last assertion is the one that matters most and is easy to skip: not one
// of the data files written before the change was touched. Schema evolution
// here is metadata-only, and a table format that rewrote history to add a
// column would be no better than the rebuild it is meant to replace.
func TestReconcileAddsAnOptionalColumn(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)

	// Three batches before the change and two after, at the shared test
	// configuration's fifty-record threshold.
	const perBatch = 50 // cfg.FlushMaxRecords
	const before = 3 * perBatch
	const after = 2 * perBatch

	first := openStore(t, cfg)
	old, err := icelake.OpenWriter(ctx, first, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	for i := range before {
		if err := old.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := old.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}

	original := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(original) != before/perBatch {
		t.Fatalf("%d data files were written before the schema change, want %d; the boundary must have several batches behind it",
			len(original), before/perBatch)
	}

	// A new run of the same service, with one more field in the declaration.
	second := openStore(t, cfg)
	evolved, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[FillV2]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter after adding a column: %v", err)
	}
	limit := "limit"
	for i := before; i < before+after; i++ {
		row := makeFill(i)
		if err := evolved.Insert(ctx, FillV2{
			Symbol: row.Symbol, Side: row.Side, Price: row.Price, Quantity: row.Quantity,
			SequenceID: row.SequenceID, OrderID: row.OrderID, Source: row.Source,
			VenueTimeMS: row.VenueTimeMS, VenueOrderType: &limit,
		}); err != nil {
			t.Fatalf("Insert %d after adding a column: %v", i, err)
		}
	}
	if err := evolved.Close(ctx); err != nil {
		t.Fatalf("Close after adding a column: %v", err)
	}

	duck := openDuckDB(t, bucket)
	table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))

	var total, nulls, valued int
	duck.queryRow(t, fmt.Sprintf(`
		SELECT count(*),
		       count(*) FILTER (venue_order_type IS NULL),
		       count(*) FILTER (venue_order_type = 'limit')
		FROM %s`, table), &total, &nulls, &valued)

	if total != before+after {
		t.Errorf("row count = %d, want %d; the table must read back as one table across the change", total, before+after)
	}
	if nulls != before {
		t.Errorf("%d rows have no venue_order_type, want the %d written before the column existed", nulls, before)
	}
	if valued != after {
		t.Errorf("%d rows carry the new column's value, want the %d written after it existed", valued, after)
	}

	// One row from each side of the boundary, read back whole: the old rows did
	// not merely survive being counted, they still carry the exact values they
	// were written with.
	for _, i := range []int{before / 2, before + after/2} {
		want := makeFill(i)

		var symbol, price string
		var epochMS int64
		duck.queryRow(t, fmt.Sprintf(`
			SELECT symbol, CAST(price AS VARCHAR), epoch_ms(venue_timestamp_ms)
			FROM %s WHERE sequence_id = %d`, table, i), &symbol, &price, &epochMS)
		if symbol != want.Symbol || price != decimalString(want.Price, 9) || epochMS != want.VenueTimeMS {
			t.Errorf("record %d reads back across the schema change as (%q, %s, %d), want (%q, %s, %d)",
				i, symbol, price, epochMS, want.Symbol, decimalString(want.Price, 9), want.VenueTimeMS)
		}
	}

	// Metadata-only, proven: every data file that existed before the change is
	// still there, same size, same entity tag.
	current := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if want := (before + after) / perBatch; len(current) != want {
		t.Errorf("the table has %d data files after the change, want %d: the batches written after it must be new files, not rewrites",
			len(current), want)
	}
	for _, was := range original {
		i := slices.IndexFunc(current, func(is object) bool { return is.key == was.key })
		if i < 0 {
			t.Errorf("data file %s written before the schema change is gone; current files: %s", was.key, describe(current))

			continue
		}
		if current[i].size != was.size || current[i].etag != was.etag {
			t.Errorf("data file %s was rewritten by the schema change (%d bytes %s, was %d bytes %s)",
				was.key, current[i].size, current[i].etag, was.size, was.etag)
		}
	}
}

// TestCreatedTableHasTheDecidedCreationProperties is the Branch A half of
// SCHEMA.md's table lifecycle: what a table icelake creates actually *is*,
// before any of the evolution machinery above ever runs.
//
// SCHEMA.md and ARCHITECTURE.md's creation decisions specify an unpartitioned
// spec, the unsorted sort order, format version 2, and the manifest-merge
// properties. The first three are library defaults; the properties are passed
// explicitly because icelake commits one Parquet file at a time. That is a
// claim about a library's defaults and explicit creation settings, and the reason
// it is worth a test rather than a comment is stated in ARCHITECTURE.md itself:
// the decision is recorded so that it stays a decision if a future release
// changes its default. A decision nothing checks is not a decision, it is a
// hope. Every one of these three would change the shape of every table icelake
// creates, and not one of them would fail any other test in this suite: a
// partitioned table still reads back correctly, a sorted one still reads back
// correctly, and a format-version-3 table would read back correctly too until
// the day it met a reader that only speaks 2.
//
// The properties are asserted at two points in the table's life, because they
// are two different claims. At the first metadata file, they are what creation
// produced. At the latest, they are what survived a real commit — a commit
// writes a whole new metadata file, and a library that reset a default while
// rebuilding it would leave the creation-time assertion perfectly happy.
func TestCreatedTableHasTheDecidedCreationProperties(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	// OpenWriter against a table that does not exist is Branch A in full: this
	// call is what creates it, through the one CreateTable in the code base.
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// Asserted here, before a single record is inserted, deliberately. The
	// write path has its own opinions about a partitioned table and would fail
	// a flush against one — which is a fine second line of defence but a poor
	// test, because the failure would arrive as a flush error naming a
	// partition transform rather than as the plain statement that the table
	// icelake created was not the table it decided to create.
	created := metadataVersions(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(created) != 1 {
		t.Fatalf("the table has %d metadata files immediately after creation, want exactly 1", len(created))
	}
	t.Run("as created", func(t *testing.T) {
		assertCreationProperties(t, readMetadata(t, bucket, created[0]))
	})

	for i := range 50 { // cfg.FlushMaxRecords, so one batch really commits
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// And again against the metadata file the commit wrote, which is a whole
	// new document rather than an edit of the one above.
	committed := metadataVersions(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(committed) < 2 {
		t.Fatalf("the table has %d metadata files after a commit, want at least 2", len(committed))
	}
	t.Run("after a commit", func(t *testing.T) {
		doc := readMetadata(t, bucket, committed[len(committed)-1])
		if !doc.hasSnapshot() {
			t.Fatal("the latest metadata file names no snapshot; this half of the claim needs a real commit behind it")
		}
		assertCreationProperties(t, doc)
	})
}

// assertCreationProperties holds the three properties SCHEMA.md's Branch A
// records, read out of a real metadata file in the bucket rather than out of
// the catalog — the same discipline the rest of this suite reads by, since the
// question is always what a downstream reader would find.
func assertCreationProperties(t *testing.T, doc metadataDoc) {
	t.Helper()

	// Format version 2. Version 1 has no row-level deletes and a different
	// manifest layout; a version above 2 is not what anything downstream of
	// this project has been verified against.
	if doc.FormatVersion != 2 {
		t.Errorf("format-version = %d, want 2", doc.FormatVersion)
	}

	// Unpartitioned: the default spec must exist, must be the one the table
	// points at, and must have no partition fields. A spec with fields would
	// change every data file's path and every writer's obligations.
	if doc.DefaultSpecID == nil {
		t.Error("the metadata file names no default-spec-id")
	} else {
		found := false
		for _, spec := range doc.PartitionSpecs {
			if spec.SpecID != *doc.DefaultSpecID {
				continue
			}
			found = true
			if len(spec.Fields) != 0 {
				t.Errorf("the default partition spec has %d fields, want an unpartitioned spec with none", len(spec.Fields))
			}
		}
		if !found {
			t.Errorf("the metadata file has no partition spec with id %d, which it names as the default", *doc.DefaultSpecID)
		}
	}

	// Every append begins as a small manifest. Merging them after the threshold
	// bounds reader metadata work without rewriting a data file.
	for key, want := range map[string]string{
		"commit.manifest-merge.enabled":      "true",
		"commit.manifest.target-size-bytes":  "8388608",
		"commit.manifest.min-count-to-merge": "100",
	} {
		if got := doc.Properties[key]; got != want {
			t.Errorf("table property %q = %q, want %q", key, got, want)
		}
	}

	// Unsorted: same shape of check against the sort orders. A sort order with
	// fields is a promise about the contents of every data file, which icelake
	// never makes and could not keep.
	if doc.DefaultSortOrderID == nil {
		t.Error("the metadata file names no default-sort-order-id")
	} else {
		found := false
		for _, order := range doc.SortOrders {
			if order.OrderID != *doc.DefaultSortOrderID {
				continue
			}
			found = true
			if len(order.Fields) != 0 {
				t.Errorf("the default sort order has %d fields, want the unsorted order with none", len(order.Fields))
			}
		}
		if !found {
			t.Errorf("the metadata file has no sort order with id %d, which it names as the default", *doc.DefaultSortOrderID)
		}
	}
}

// TestReconcileRenamesAndWidensColumns covers the other two arms of the
// reconciliation loop, each against a table that already holds committed data.
func TestReconcileRenamesAndWidensColumns(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()

	t.Run("rename", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		store := openStore(t, cfg)

		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "renamed"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		if err := writer.Insert(ctx, makeFill(1)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// Same field id, different name: a rename, and recognizable as one only
		// because identity is the id.
		renamed, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillRenamed]{Namespace: "market", Table: "renamed"})
		if err != nil {
			t.Fatalf("OpenWriter after renaming a column: %v", err)
		}
		if err := renamed.Insert(ctx, FillRenamed{Symbol: "SYM", Venue: "venue-9", SequenceID: 2}); err != nil {
			t.Fatalf("Insert after renaming a column: %v", err)
		}
		if err := renamed.Close(ctx); err != nil {
			t.Fatalf("Close after renaming a column: %v", err)
		}

		duck := openDuckDB(t, bucket)
		table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "renamed"))

		// The column reads back under its new name for both rows, including the
		// one written before the rename: the rename moved the name, not the
		// data.
		var total int
		var oldRow, newRow string
		duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", table), &total)
		duck.queryRow(t, fmt.Sprintf("SELECT venue FROM %s WHERE sequence_id = 1", table), &oldRow)
		duck.queryRow(t, fmt.Sprintf("SELECT venue FROM %s WHERE sequence_id = 2", table), &newRow)
		if total != 2 {
			t.Errorf("row count = %d, want 2", total)
		}
		if want := makeFill(1).Source; oldRow != want {
			t.Errorf("the row written before the rename reads back as %q under the new name, want %q", oldRow, want)
		}
		if newRow != "venue-9" {
			t.Errorf("the row written after the rename reads back as %q, want %q", newRow, "venue-9")
		}
	})

	t.Run("widen", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		store := openStore(t, cfg)

		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[narrowCounter]{Namespace: "market", Table: "counters"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		if err := writer.Insert(ctx, narrowCounter{Name: "narrow", Count: 7}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		widened, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[wideCounter]{Namespace: "market", Table: "counters"})
		if err != nil {
			t.Fatalf("OpenWriter after widening a column: %v", err)
		}
		if err := widened.Insert(ctx, wideCounter{Name: "wide", Count: 1 << 40}); err != nil {
			t.Fatalf("Insert after widening a column: %v", err)
		}
		if err := widened.Close(ctx); err != nil {
			t.Fatalf("Close after widening a column: %v", err)
		}

		duck := openDuckDB(t, bucket)
		table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "counters"))

		if got := duck.columnType(t, table, "count"); got != "BIGINT" {
			t.Errorf("count reads back as %s after widening, want BIGINT", got)
		}
		var narrow, wide int64
		duck.queryRow(t, fmt.Sprintf("SELECT count FROM %s WHERE name = 'narrow'", table), &narrow)
		duck.queryRow(t, fmt.Sprintf("SELECT count FROM %s WHERE name = 'wide'", table), &wide)
		if narrow != 7 || wide != 1<<40 {
			t.Errorf("counts read back as (%d, %d), want (7, %d)", narrow, wide, int64(1)<<40)
		}
	})
}

// TestReconcileRefusals proves the loop's three refusals, each of which exists
// because the alternative is a table that quietly stops matching what the
// program believes it holds.
func TestReconcileRefusals(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	// A table with data behind it, which is what makes the refusals meaningful.
	seed := func(t *testing.T, table string) {
		t.Helper()

		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: table})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		if err := writer.Insert(ctx, makeFill(1)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	t.Run("a new required field", func(t *testing.T) {
		seed(t, "required_field")

		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillRequiredNewField]{Namespace: "market", Table: "required_field"})

		var reconcile icelake.ReconcileError
		if !errors.As(err, &reconcile) {
			t.Fatalf("OpenWriter returned %v (%T), want a ReconcileError: a field added to an existing table has no value for the rows already written", err, err)
		}
		if reconcile.Table != "required_field" {
			t.Errorf("ReconcileError.Table = %q, want %q", reconcile.Table, "required_field")
		}
	})

	t.Run("an unsafe type change", func(t *testing.T) {
		// The safe direction first, so the narrowing below is applied to a
		// table that really is wider.
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[wideCounter]{Namespace: "market", Table: "narrowing"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		if err := writer.Insert(ctx, wideCounter{Name: "wide", Count: 1 << 40}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// long to int loses values. icelake reimplements none of this check: it
		// only has to keep asking the library to refuse incompatible changes.
		_, err = icelake.OpenWriter(ctx, store, icelake.TableConfig[narrowCounter]{Namespace: "market", Table: "narrowing"})

		var reconcile icelake.ReconcileError
		if !errors.As(err, &reconcile) {
			t.Fatalf("OpenWriter returned %v (%T), want a ReconcileError wrapping the library's own refusal", err, err)
		}
		if reconcile.Err == nil {
			t.Error("ReconcileError.Err is nil, want the underlying refusal wrapped and reachable")
		}
	})

	// This is the narrative SCHEMA.md carried as an expectation to confirm at
	// this milestone: a declaration that guesses a new field's id rather than
	// deriving it from the table's current maximum.
	//
	// It is refused, and it is refused earlier than that narrative supposed —
	// at declaration, before the catalog is touched at all, by the pre-order
	// field-id rule. The reasoning that went with it is what made the earlier
	// refusal possible: because every id icelake ever declares is the pre-order
	// numbering, and because AddColumn assigns exactly the next id after the
	// table's maximum, a declaration that reaches the reconciliation loop can
	// only ever carry ids the table agrees with. The confusing failure the
	// narrative predicted — an AddColumn for a column name the table already
	// has — is therefore unreachable through icelake's own path.
	t.Run("a guessed field id for a new column", func(t *testing.T) {
		seed(t, "guessed_id")

		before := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "guessed_id")

		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillWrongNewID]{Namespace: "market", Table: "guessed_id"})

		var fieldID icelake.FieldIDError
		if !errors.As(err, &fieldID) {
			t.Fatalf("OpenWriter returned %v (%T), want a FieldIDError naming the offending field", err, err)
		}
		if fieldID.Field != "venue_order_type" {
			t.Errorf("FieldIDError.Field = %q, want %q", fieldID.Field, "venue_order_type")
		}
		if fieldID.FieldID != 20 || fieldID.WantID != 9 {
			t.Errorf("FieldIDError says the declaration asked for %d and the numbering requires %d, want 20 and 9",
				fieldID.FieldID, fieldID.WantID)
		}

		// Nothing was touched: the refusal happened before the catalog was
		// consulted, so the table is exactly as the previous run left it.
		if after := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "guessed_id"); len(after) != len(before) {
			t.Errorf("the refused declaration changed the table's data files (%d, was %d)", len(after), len(before))
		}
	})

	// The refusals above blocked one table each. The store is still perfectly
	// usable, and another table opens and commits normally — which is the
	// concrete form of "a reconciliation failure blocks that one table".
	t.Run("other tables are unaffected", func(t *testing.T) {
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "unaffected"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		if err := writer.Insert(ctx, makeFill(1)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		duck := openDuckDB(t, bucket)
		var total int
		duck.queryRow(t, "SELECT count(*) FROM "+
			duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "unaffected")), &total)
		if total != 1 {
			t.Errorf("row count = %d, want 1", total)
		}
	})
}

// TestOpenWriterReportsAnUnreachableTableAsAReconcileError covers the other
// half of this error's surface: the two places on the table-open path that
// touch the bucket rather than the local catalog.
//
// Every ReconcileError asserted above is raised by a purely local refusal — a
// required new field, a narrowing the library itself rejects, a field-id rule —
// so nothing anywhere proved that a table icelake cannot reach at all comes
// back as this type. It matters because it decides what a caller sees on the
// most ordinary bad day there is: the endpoint is down, or the credentials no
// longer let this process read the warehouse. Without the wrap, an operator
// gets the SDK's own error with no table name in it, from a call whose whole
// job was to open one named table.
//
// The two subtests are the two branches of that path — a table that exists,
// whose metadata file has to be read, and one that does not, whose first
// metadata file has to be written — and they are separate because a store that
// can neither read nor write is not the same as one that can do neither yet.
// The connections are rejected at the proxy already sitting in front of the
// same real MinIO, exactly as scenarios 7 and 8 do it: what reaches icelake is
// a connection that fails immediately, and the substrate behind it is
// unchanged.
func TestOpenWriterReportsAnUnreachableTableAsAReconcileError(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint

	assertReconcileError := func(t *testing.T, err error, table string) {
		t.Helper()

		var reconcile icelake.ReconcileError
		if !errors.As(err, &reconcile) {
			t.Fatalf("OpenWriter returned %v (%T), want a ReconcileError naming the table", err, err)
		}
		if reconcile.Table != table {
			t.Errorf("ReconcileError.Table = %q, want %q", reconcile.Table, table)
		}
		if reconcile.Err == nil {
			t.Error("ReconcileError.Err is nil, want the storage failure wrapped and reachable underneath")
		}
	}

	t.Run("a table whose metadata cannot be read", func(t *testing.T) {
		store := openStore(t, cfg)

		// Create the table for real first, so that the failing open below is
		// one that finds a catalog row and then cannot fetch what it points at.
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "unreadable"})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("Writer.Close: %v", err)
		}

		proxy.SetReject(true)
		defer proxy.SetReject(false)

		reopened, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "unreadable"})
		if reopened != nil {
			t.Error("OpenWriter returned a usable writer for a table it could not load")
		}
		assertReconcileError(t, err, "unreadable")
	})

	t.Run("a table that cannot be created", func(t *testing.T) {
		store := openStore(t, cfg)

		proxy.SetReject(true)
		defer proxy.SetReject(false)

		// The namespace check and the namespace insert are both local
		// catalog.db work, so the first thing on this path that touches the
		// bucket is the write of the table's first metadata file.
		created, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "uncreatable"})
		if created != nil {
			t.Error("OpenWriter returned a usable writer for a table it could not create")
		}
		assertReconcileError(t, err, "uncreatable")
	})
}

// FillWithoutTimestamp is Fill with its last column dropped from the
// declaration. The ids that remain are still the pre-order numbering, so the
// declaration itself is perfectly legal — which is the point: nothing about the
// struct says anything is wrong, and the problem only exists because records
// were already staged under the shape that had the column.
type FillWithoutTimestamp struct {
	Symbol     string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side       string `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price      int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity   int64  `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID int64  `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID    string `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source     string `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
}

// TestOpenWriterRefusesStagedRowsItCannotRepresent proves the fail-loudly half
// of the schema-fingerprint decision: staged records survive a shutdown, and
// the next binary declares a struct that has no column for one of the things
// they hold.
//
// The refusal is the whole point. Opening anyway would mean either dropping
// that column's accepted values silently or writing them somewhere they do not
// belong, and the operator's actual next step — drain the table with the
// previous binary — is only available if nothing has been half-done in the
// meantime.
func TestOpenWriterRefusesStagedRowsItCannotRepresent(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint

	first := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, first, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	const records = 4
	for i := range records {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	// Leave the records staged: the drain cannot reach the store.
	proxy.SetDelay(time.Minute)
	budget, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := writer.Close(budget); err == nil {
		t.Fatal("Close whose context expired mid-drain returned no error, want one")
	}
	shutdown, cancelShutdown := context.WithTimeout(ctx, time.Minute)
	defer cancelShutdown()
	if err := first.Close(shutdown); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != records {
		t.Fatalf("staging holds %d rows, want %d", got, records)
	}
	proxy.SetDelay(0)

	// The next run's declaration has no field id 8, which the staged rows have
	// a value for.
	second := openStore(t, cfg)
	dropped, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[FillWithoutTimestamp]{Namespace: "market", Table: "fills"})

	var staged icelake.StagingSchemaError
	if !errors.As(err, &staged) {
		t.Fatalf("OpenWriter returned %v (%T), want a StagingSchemaError", err, err)
	}
	if dropped != nil {
		t.Error("OpenWriter returned a usable writer alongside its error; a table it could not verify must yield no writer at all")
	}
	if staged.Kind != icelake.StagingSchemaKindFieldRemoved {
		t.Errorf("StagingSchemaError.Kind = %q, want %q", staged.Kind, icelake.StagingSchemaKindFieldRemoved)
	}
	if staged.FieldID != 8 {
		t.Errorf("StagingSchemaError.FieldID = %d, want 8 (the column the declaration dropped)", staged.FieldID)
	}

	// Nothing was consumed: the records are still there for the previous binary
	// to drain.
	if got := stagedRows(t, cfg.StagingPath); got != records {
		t.Errorf("staging holds %d rows after the refusal, want the same %d", got, records)
	}
}

// strandStagedRows inserts records through one declaration and returns with
// every one of them still in the staging database, recorded under that
// declaration's shape.
//
// The mechanism is not a new one. It is exactly what
// TestOpenWriterRefusesStagedRowsItCannotRepresent above does, and for the same
// reason: the slow proxy already sitting in front of the same real MinIO is set
// to a delay no drain budget can outlast, so Close runs out of time with the
// batch sealed and unsent and the records stay on disk for the next open to
// find. Nothing is stood in for — the records are written to the real staging
// file by the real write path, and the store behind the proxy is the same
// container every other test in this file writes to.
//
// The caller closes the store and lowers the delay afterwards, in that order,
// which is the order the test above established: the writer is already gone by
// then, so the store's own shutdown has nothing left to send.
func strandStagedRows[T any](t *testing.T, store *icelake.Store, proxy *slowProxy, namespace, table string, rows []T) {
	t.Helper()

	ctx := context.Background()

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[T]{Namespace: namespace, Table: table})
	if err != nil {
		t.Fatalf("OpenWriter for %s.%s: %v", namespace, table, err)
	}
	for i, row := range rows {
		if err := writer.Insert(ctx, row); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	proxy.SetDelay(time.Minute)
	budget, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := writer.Close(budget); err == nil {
		t.Fatal("Close whose context expired mid-drain returned no error, want one")
	}
}

// TestOpenWriterRefusesStagedRowsUnderAChangedColumn holds the two
// StagingSchemaError kinds that the removed-column test above does not reach: a
// staged column the current declaration gives a different type, and a column
// the current declaration requires that the staged rows have no value for.
//
// Added at the completeness audit's fifth round (2026-08-01), and the gap it
// closes is worth stating because it was invisible from either side on its own.
// internal/canon/remap_test.go enumerates five shape pairs that cannot be mapped
// honestly, as pure computation, and its header claimed that the caller-facing
// half of every one of them was already proven end to end. Only the removed
// column was: TestOpenWriterRefusesStagedRowsItCannotRepresent above and
// scenario 2's TestStagedRowsUnderAnUnknownFieldRefuseTheOpen both assert
// StagingSchemaKindFieldRemoved and nothing else, so two of the three kinds a
// caller can be handed had no assertion anywhere that a caller is ever handed
// them. Both subtests below reach a real caller through OpenWriter, against the
// real substrate, with real records really left in a real staging file.
//
// Neither kind is reachable by simply changing a declaration between two runs,
// which is why they were missed and why the arrangements below are as long as
// they are. Reconciliation runs before the staged-shape check — the table has to
// be brought to the declared shape before anything can ask whether the staged
// rows fit it — so any declaration change reconciliation itself refuses fails as
// a ReconcileError first and never reaches this error at all. What is left is
// the changes reconciliation accepts, and each kind below is reached through one
// of them: a widened column type for the retyped kind, and a column the live
// table already carries that the staged rows' shape does not for the required
// kind. The removed kind, tested above, comes through a third — reconciliation
// never infers a drop from an omission, so a declaration that simply leaves a
// live column out is accepted, and the staged rows that hold a value for it are
// what refuse.
func TestOpenWriterRefusesStagedRowsUnderAChangedColumn(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()

	// A safe widening — the change TestReconcileRenamesAndWidensColumns proves
	// reconciliation applies happily — met by rows staged under the narrow
	// shape. The widening is safe for the table's committed data, where every
	// int32 already written is a perfectly good int64. It says nothing about
	// staged payloads: those are canonical bytes encoded under a descriptor that
	// said int32, and reading them back as int64 would misread them rather than
	// widen them, so the open is refused instead.
	//
	// One limit on what this proves is recorded rather than asserted, because it
	// was found while building the test (completeness audit's fifth round,
	// 2026-08-01) and it is a question for the documents rather than for this
	// file. StagingSchemaError's own text tells the operator to drain the table
	// with the previous binary. Reached this way, that advice does not work: the
	// widening is committed by reconciliation before the staged-shape check runs,
	// so by the time the caller holds the refusal the live column is already an
	// int64 and the previous binary's int32 declaration is refused as a
	// narrowing. The refusal below is still the right answer — the alternative is
	// misreading staged bytes as a wider type — but what an operator does next is
	// not something this test can settle, and nothing is asserted about it here.
	//
	// The last sentence of that paragraph used to read "nothing is asserted
	// about it here because no document claims anything about it today", and the
	// second half was fixed the same day, at the round's second repair pass: a
	// limit on operator advice recorded only in a test comment is recorded where
	// nobody who needs it will look. It now sits on the doc comments of
	// `StagingSchemaError` in `errors.go` and in `internal/errdef`, which is
	// where the caller meets the advice it qualifies. It is still not asserted,
	// and that part stays: the claim is about what an operator should be told.
	t.Run("a column the new declaration widened", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		proxy := startSlowProxy(t, bucket.Endpoint, 0)
		cfg.Endpoint = proxy.Endpoint

		staged := []narrowCounter{
			{Name: "alpha", Count: 1},
			{Name: "bravo", Count: 2},
			{Name: "charlie", Count: 3},
			{Name: "delta", Count: 4},
		}

		first := openStore(t, cfg)
		strandStagedRows(t, first, proxy, "market", "retyped", staged)

		shutdown, cancelShutdown := context.WithTimeout(ctx, time.Minute)
		defer cancelShutdown()
		if err := first.Close(shutdown); err != nil {
			t.Fatalf("Store.Close: %v", err)
		}
		if got := stagedRows(t, cfg.StagingPath); got != len(staged) {
			t.Fatalf("staging holds %d rows, want the %d that could not be flushed", got, len(staged))
		}
		proxy.SetDelay(0)

		// The next run declares the same column one width wider.
		second := openStore(t, cfg)
		widened, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[wideCounter]{Namespace: "market", Table: "retyped"})

		var schemaErr icelake.StagingSchemaError
		if !errors.As(err, &schemaErr) {
			t.Fatalf("OpenWriter returned %v (%T), want a StagingSchemaError", err, err)
		}
		if widened != nil {
			t.Error("OpenWriter returned a usable writer alongside its error; a table it could not verify must yield no writer at all")
		}
		if schemaErr.Kind != icelake.StagingSchemaKindFieldRetyped {
			t.Errorf("StagingSchemaError.Kind = %q, want %q", schemaErr.Kind, icelake.StagingSchemaKindFieldRetyped)
		}
		if schemaErr.FieldID != 2 {
			t.Errorf("StagingSchemaError.FieldID = %d, want 2 (the column whose type the declaration changed)", schemaErr.FieldID)
		}
		if schemaErr.Table != "retyped" {
			t.Errorf("StagingSchemaError.Table = %q, want %q", schemaErr.Table, "retyped")
		}

		// Nothing was consumed. The staged bytes are still exactly what the
		// narrow declaration wrote, which is what makes them readable at all.
		if got := stagedRows(t, cfg.StagingPath); got != len(staged) {
			t.Errorf("staging holds %d rows after the refusal, want the same %d", got, len(staged))
		}
	})

	// The other kind, and the arrangement below is forced rather than chosen. A
	// declaration carrying a required field the live table does not have is
	// refused by reconciliation itself, with a ReconcileError, because the rows
	// already committed have no value for it — so the column has to be one the
	// table already carries while being absent from the shape the staged rows
	// were written under, and every route to this error has that shape. In a
	// single service the story that produces it is a rollback: an optional column
	// is added, the service rolls back to the build from before it while records
	// are still being accepted, and then deploys forward to a build that has
	// since made the same column required.
	t.Run("a column the new declaration requires", func(t *testing.T) {
		cfg := testConfig(t, bucket)
		proxy := startSlowProxy(t, bucket.Endpoint, 0)
		cfg.Endpoint = proxy.Endpoint

		first := openStore(t, cfg)

		// The build that added the column, which is what puts field id 9 on the
		// live table as an optional one.
		added, err := icelake.OpenWriter(ctx, first, icelake.TableConfig[FillV2]{Namespace: "market", Table: "required_later"})
		if err != nil {
			t.Fatalf("OpenWriter under the declaration that adds the column: %v", err)
		}
		if err := added.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// The rollback: a build with no field id 9 at all, whose records are
		// therefore staged under a shape that has no value for it. A declaration
		// that omits a live column is deliberately legal — reconciliation never
		// infers a drop from an omission — so this run is unremarkable until its
		// records fail to flush.
		staged := make([]Fill, 4)
		for i := range staged {
			staged[i] = makeFill(i)
		}
		strandStagedRows(t, first, proxy, "market", "required_later", staged)

		shutdown, cancelShutdown := context.WithTimeout(ctx, time.Minute)
		defer cancelShutdown()
		if err := first.Close(shutdown); err != nil {
			t.Fatalf("Store.Close: %v", err)
		}
		if got := stagedRows(t, cfg.StagingPath); got != len(staged) {
			t.Fatalf("staging holds %d rows, want the %d that could not be flushed", got, len(staged))
		}
		proxy.SetDelay(0)

		// The deploy forward, with the column now required.
		second := openStore(t, cfg)
		required, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[FillRequiredNewField]{Namespace: "market", Table: "required_later"})

		var schemaErr icelake.StagingSchemaError
		if !errors.As(err, &schemaErr) {
			t.Fatalf("OpenWriter returned %v (%T), want a StagingSchemaError", err, err)
		}
		if required != nil {
			t.Error("OpenWriter returned a usable writer alongside its error; a table it could not verify must yield no writer at all")
		}
		if schemaErr.Kind != icelake.StagingSchemaKindFieldRequired {
			t.Errorf("StagingSchemaError.Kind = %q, want %q", schemaErr.Kind, icelake.StagingSchemaKindFieldRequired)
		}
		if schemaErr.FieldID != 9 {
			t.Errorf("StagingSchemaError.FieldID = %d, want 9 (the column the declaration requires and the staged rows never had)", schemaErr.FieldID)
		}
		if schemaErr.Table != "required_later" {
			t.Errorf("StagingSchemaError.Table = %q, want %q", schemaErr.Table, "required_later")
		}

		if got := stagedRows(t, cfg.StagingPath); got != len(staged) {
			t.Errorf("staging holds %d rows after the refusal, want the same %d", got, len(staged))
		}

		// And the operator's documented next step really is available here: the
		// error tells them to drain with the previous binary, so the rollback
		// declaration is opened again and takes the same records to the table,
		// values intact, with the now-optional column null.
		drained, err := icelake.OpenWriter(ctx, second, icelake.TableConfig[Fill]{Namespace: "market", Table: "required_later"})
		if err != nil {
			t.Fatalf("OpenWriter under the previous binary's declaration: %v", err)
		}
		if err := drained.Close(ctx); err != nil {
			t.Fatalf("Close after draining: %v", err)
		}
		if got := stagedRows(t, cfg.StagingPath); got != 0 {
			t.Errorf("staging holds %d rows after the drain, want 0", got)
		}

		duck := openDuckDB(t, bucket)
		table := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "required_later"))

		var total, nulls int
		var orderID string
		duck.queryRow(t, fmt.Sprintf("SELECT count(*), count(*) FILTER (venue_order_type IS NULL) FROM %s", table), &total, &nulls)
		duck.queryRow(t, fmt.Sprintf("SELECT order_id FROM %s WHERE sequence_id = 2", table), &orderID)
		if total != len(staged) || nulls != len(staged) {
			t.Errorf("the drained table holds %d rows of which %d are null in the column that was made required, want %d of each",
				total, nulls, len(staged))
		}
		if want := makeFill(2).OrderID; orderID != want {
			t.Errorf("a drained record reads back as %q, want %q", orderID, want)
		}
	})
}
