package canon

import (
	"strings"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/gvkhna/icelake/internal/schemamap"
)

// This file holds one test, and it is the second sanctioned unit-test category
// from TESTING.md: a package-internal behaviour test of a layer that is pure
// in-memory computation. It holds the first claim shape — the caller-facing half
// is already proven end to end through the public API, by
// TestStructAndDocumentAgreeOnOneShape in the root package, which opens a real
// store through both front doors and compares the schema fingerprints the two
// writers actually recorded on disk.
//
// What this pins more precisely than the substrate can is the two artifacts that
// exist only between the derivation and the write path: the Arrow schema, whose
// per-field PARQUET:field_id metadata is what carries permanent identity into
// every data file, and the Iceberg schema, which is what a table is created
// from. Neither is observable from outside — a data file read back through
// DuckDB shows the columns but not the metadata that named them — so a drift in
// either would be invisible to every end-to-end test in this repo while quietly
// making the two declaration sources two different tables.
//
// What it would cost to lose: the fingerprint the root-package test compares is
// derived *from* the Iceberg schema, so an Arrow-metadata drift that happened
// not to change the Iceberg schema would pass there and still write data files
// whose columns are identified by different ids than the table's.
//
// It lives in this package rather than in internal/schemamap because it needs
// both sides: schemamap derives the two schemas and canon takes the descriptor
// over them, and canon's tests may import schemamap while the reverse is a cycle
// — the arrangement SCHEMA.md's M11 correction records.

// twinRecord declares every one of the nine canonical column kinds in both
// optionalities, which is the widest shape this design can express and therefore
// the one with the most places for a twin to drift.
type twinRecord struct {
	Flag       bool     `parquet:"name=flag, fieldid=1" arrow:"flag"`
	Count      int32    `parquet:"name=count, fieldid=2" arrow:"count"`
	Seq        int64    `parquet:"name=seq, fieldid=3" arrow:"seq"`
	Ratio      float32  `parquet:"name=ratio, fieldid=4" arrow:"ratio"`
	Weight     float64  `parquet:"name=weight, fieldid=5" arrow:"weight"`
	Price      int64    `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=6" arrow:"price"`
	At         int64    `parquet:"name=at, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=7" arrow:"at"`
	Symbol     string   `parquet:"name=symbol, logical=String, fieldid=8" arrow:"symbol"`
	Payload    []byte   `parquet:"name=payload, type=BYTE_ARRAY, fieldid=9" arrow:"payload"`
	OptFlag    *bool    `parquet:"name=opt_flag, fieldid=10" arrow:"opt_flag"`
	OptCount   *int32   `parquet:"name=opt_count, fieldid=11" arrow:"opt_count"`
	OptSeq     *int64   `parquet:"name=opt_seq, fieldid=12" arrow:"opt_seq"`
	OptRatio   *float32 `parquet:"name=opt_ratio, fieldid=13" arrow:"opt_ratio"`
	OptWeight  *float64 `parquet:"name=opt_weight, fieldid=14" arrow:"opt_weight"`
	OptPrice   *int64   `parquet:"name=opt_price, logical=decimal, precision=18, scale=9, fieldid=15" arrow:"opt_price"`
	OptAt      *int64   `parquet:"name=opt_at, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=16" arrow:"opt_at"`
	OptSymbol  *string  `parquet:"name=opt_symbol, logical=String, fieldid=17" arrow:"opt_symbol"`
	OptPayload *[]byte  `parquet:"name=opt_payload, type=BYTE_ARRAY, fieldid=18" arrow:"opt_payload"`
}

// twinFields is the same table stated the way a schema document states it: a
// list of columns, written out by hand rather than derived from the struct,
// because a list generated from its own twin would agree by construction and
// would prove nothing.
func twinFields() []iceberg.NestedField {
	return []iceberg.NestedField{
		{ID: 1, Name: "flag", Type: iceberg.PrimitiveTypes.Bool, Required: true},
		{ID: 2, Name: "count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		{ID: 3, Name: "seq", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 4, Name: "ratio", Type: iceberg.PrimitiveTypes.Float32, Required: true},
		{ID: 5, Name: "weight", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		{ID: 6, Name: "price", Type: iceberg.DecimalTypeOf(18, 9), Required: true},
		{ID: 7, Name: "at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		{ID: 8, Name: "symbol", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: 9, Name: "payload", Type: iceberg.PrimitiveTypes.Binary, Required: true},
		{ID: 10, Name: "opt_flag", Type: iceberg.PrimitiveTypes.Bool},
		{ID: 11, Name: "opt_count", Type: iceberg.PrimitiveTypes.Int32},
		{ID: 12, Name: "opt_seq", Type: iceberg.PrimitiveTypes.Int64},
		{ID: 13, Name: "opt_ratio", Type: iceberg.PrimitiveTypes.Float32},
		{ID: 14, Name: "opt_weight", Type: iceberg.PrimitiveTypes.Float64},
		{ID: 15, Name: "opt_price", Type: iceberg.DecimalTypeOf(18, 9)},
		{ID: 16, Name: "opt_at", Type: iceberg.PrimitiveTypes.TimestampTz},
		{ID: 17, Name: "opt_symbol", Type: iceberg.PrimitiveTypes.String},
		{ID: 18, Name: "opt_payload", Type: iceberg.PrimitiveTypes.Binary},
	}
}

// TestStructAndFieldsDeriveOneSchema is TESTING.md scenario 10's twin-equivalence
// clause at the seam the artifacts live on.
//
// The document path replaces exactly one of the struct path's three library
// calls: where the tag parser reads a Go type to build a Parquet schema tree,
// DeclareFields builds the same tree node by node. Two details of that twin are
// load-bearing and invisible from anywhere else — a decimal must be built on
// physical INT64, and a timestamp at millisecond unit with isAdjustedToUTC — and
// a twin that got either wrong would derive a perfectly valid table that is
// simply not the table the equivalent struct declares.
func TestStructAndFieldsDeriveOneSchema(t *testing.T) {
	fromStruct, err := schemamap.Declare[twinRecord]("market", "everything")
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}
	fromFields, err := schemamap.DeclareFields("market", "everything", twinFields())
	if err != nil {
		t.Fatalf("DeclareFields: %v", err)
	}

	// The Parquet schema first, because it is the one artifact that can catch a
	// physical-node drift: a decimal built on a fixed-length byte array and a
	// timestamp built at microseconds both derive perfectly ordinary Arrow and
	// Iceberg columns, and the first of the two derives an *identical* one, so
	// the fingerprint cross-check OpenDynamicWriter runs cannot see it. Both
	// mutations were made deliberately at M12 to find out which check noticed;
	// this one notices both, and SCHEMA.md's M12 correction records why the
	// paragraph that used to claim otherwise was wrong.
	// The one difference between the two renderings is the root group's own
	// name, and it is a difference that cannot reach anything: the tag parser
	// names the root after the Go type and this path names it after the table,
	// while the Parquet-to-Arrow call builds its fields from the root's children
	// and gives the schema no metadata of its own. It is asserted as a known
	// difference rather than papered over, and the columns beneath it are then
	// compared in full.
	if got, want := parquetRoot(fromFields), "everything"; !strings.Contains(got, want) {
		t.Errorf("the field-derived root group is %q, want it named for the table (%q)", got, want)
	}
	if got, want := parquetRoot(fromStruct), "twinRecord"; !strings.Contains(got, want) {
		t.Errorf("the struct-derived root group is %q, want it named for the Go type (%q)", got, want)
	}
	if got, want := parquetColumns(fromFields), parquetColumns(fromStruct); got != want {
		t.Errorf("parquet columns:\n got %s\nwant %s", got, want)
	}

	// The Arrow schema's own rendering covers each column's name, type and
	// nullability and its metadata, which is where the permanent field id
	// travels.
	if got, want := fromFields.ArrowSchema().String(), fromStruct.ArrowSchema().String(); got != want {
		t.Errorf("arrow schema:\n got %s\nwant %s", got, want)
	}

	// And the ids themselves, read out by key rather than trusted to the
	// rendering, because they are the whole of column identity everywhere else
	// in this design.
	declared := fromStruct.ArrowSchema().Fields()
	derived := fromFields.ArrowSchema().Fields()
	if len(declared) != len(derived) {
		t.Fatalf("the two declarations have %d and %d columns", len(declared), len(derived))
	}
	for i := range declared {
		want, wantOK := declared[i].Metadata.GetValue("PARQUET:field_id")
		got, gotOK := derived[i].Metadata.GetValue("PARQUET:field_id")
		if !wantOK || !gotOK || got != want {
			t.Errorf("column %q carries field id %q (present %t), want %q (present %t)",
				declared[i].Name, got, gotOK, want, wantOK)
		}
	}

	if got, want := fromFields.IcebergSchema().String(), fromStruct.IcebergSchema().String(); got != want {
		t.Errorf("iceberg schema:\n got %s\nwant %s", got, want)
	}

	// The descriptor the two derive to, and the descriptor the field list reads
	// straight into: three routes, one fingerprint. The third is the cross-check
	// OpenDynamicWriter runs at every open, checked here against a shape wide
	// enough for every kind to be wrong in its own way.
	declaredDesc, err := Describe("everything", fromStruct.IcebergSchema())
	if err != nil {
		t.Fatalf("Describe (struct): %v", err)
	}
	derivedDesc, err := Describe("everything", fromFields.IcebergSchema())
	if err != nil {
		t.Fatalf("Describe (fields): %v", err)
	}
	directDesc, err := FromFields("everything", twinFields())
	if err != nil {
		t.Fatalf("FromFields: %v", err)
	}

	if got, want := derivedDesc.Fingerprint(), declaredDesc.Fingerprint(); got != want {
		t.Errorf("fingerprint through the field-list derivation = %s, want the struct's %s", got, want)
	}
	if got, want := directDesc.Fingerprint(), declaredDesc.Fingerprint(); got != want {
		t.Errorf("fingerprint read straight from the field list = %s, want the struct's %s", got, want)
	}
}

// parquetRoot and parquetColumns split a Parquet schema's rendering into the
// root group's own line and the columns beneath it, so the twin comparison can
// state the one known difference between the two derivations and then require
// everything else to be identical.
func parquetRoot(decl *schemamap.Declaration) string {
	root, _, _ := strings.Cut(decl.ParquetSchema().String(), "\n")

	return root
}

func parquetColumns(decl *schemamap.Declaration) string {
	_, columns, _ := strings.Cut(decl.ParquetSchema().String(), "\n")

	return columns
}
