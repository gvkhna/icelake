package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// Everything is a declaration struct covering all nine canonical column kinds in
// both optionalities, which is what the twin-equivalence test needs: a shape
// wide enough that a drift in any one of the eighteen node constructions the
// document path performs has somewhere to show up.
type Everything struct {
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

// everythingDocument is the schema document that declares exactly the table
// [Everything] declares, written out by hand rather than generated from the
// struct — a generated twin would agree with its source by construction and
// would prove nothing about the two derivations.
const everythingDocument = `{"tables": [
  {"namespace": "market", "table": "everything", "fields": [
    {"name": "flag",        "type": "boolean",       "fieldid": 1},
    {"name": "count",       "type": "int",           "fieldid": 2},
    {"name": "seq",         "type": "long",          "fieldid": 3},
    {"name": "ratio",       "type": "float",         "fieldid": 4},
    {"name": "weight",      "type": "double",        "fieldid": 5},
    {"name": "price",       "type": "decimal(18,9)", "fieldid": 6},
    {"name": "at",          "type": "timestamptz",   "fieldid": 7},
    {"name": "symbol",      "type": "string",        "fieldid": 8},
    {"name": "payload",     "type": "binary",        "fieldid": 9},
    {"name": "opt_flag",    "type": "boolean",       "fieldid": 10, "optional": true},
    {"name": "opt_count",   "type": "int",           "fieldid": 11, "optional": true},
    {"name": "opt_seq",     "type": "long",          "fieldid": 12, "optional": true},
    {"name": "opt_ratio",   "type": "float",         "fieldid": 13, "optional": true},
    {"name": "opt_weight",  "type": "double",        "fieldid": 14, "optional": true},
    {"name": "opt_price",   "type": "decimal(18,9)", "fieldid": 15, "optional": true},
    {"name": "opt_at",      "type": "timestamptz",   "fieldid": 16, "optional": true},
    {"name": "opt_symbol",  "type": "string",        "fieldid": 17, "optional": true},
    {"name": "opt_payload", "type": "binary",        "fieldid": 18, "optional": true}
  ]}
]}`

// TestStructAndDocumentAgreeOnOneShape is scenario 10's twin-equivalence
// clause, held through the public API and with no container: a table declared by
// a Go struct and the same table declared by a schema document must produce the
// same canonical descriptor fingerprint.
//
// That fingerprint is the thing the equality is actually *for*. It keys
// staging_schemas, it is hashed into every batch key, and a batch key is an
// object name — so two writers sharing one table through two front doors is a
// fact only while the two derivations agree on it byte for byte. A descriptor
// carries no table name, deliberately, which is what lets this compare two
// differently-named tables and be comparing only their shapes.
//
// It reads the fingerprint the writers actually recorded rather than deriving
// one itself, which is why it runs a real local-only store: the number under
// test is the one that reached the disk. The other two thirds of the twin claim
// — equal Arrow schemas including their field-id metadata, and equal Iceberg
// schemas — are pinned at the seam where those artifacts exist, by
// TestStructAndFieldsDeriveOneSchema in internal/canon, for the reason that
// header gives.
func TestStructAndDocumentAgreeOnOneShape(t *testing.T) {
	ctx := context.Background()
	cfg := localOnlyConfig(t, t.TempDir())
	store := openStore(t, cfg)

	tables, err := icelake.ParseSchemaDocument([]byte(everythingDocument))
	if err != nil {
		t.Fatalf("ParseSchemaDocument: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("the document declares %d tables, want 1", len(tables))
	}

	// The same shape under two names, so that comparing the two fingerprints
	// compares shapes and nothing else.
	byStruct, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Everything]{Namespace: "market", Table: "by_struct"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	fromDoc := tables[0]
	fromDoc.Table = "by_document"
	byDocument, err := icelake.OpenDynamicWriter(ctx, store, fromDoc)
	if err != nil {
		t.Fatalf("OpenDynamicWriter: %v", err)
	}

	if err := byStruct.Insert(ctx, everythingValue()); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := byDocument.InsertJSON(ctx, everythingLine(t)); err != nil {
		t.Fatalf("InsertJSON: %v", err)
	}
	if err := byStruct.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := byDocument.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	shapes := make(map[string]string)
	for _, r := range spoolRows(t, cfg.StagingPath) {
		shapes[r.table] = r.schemaFP
	}
	if len(shapes) != 2 {
		t.Fatalf("the spool recorded %d tables, want 2", len(shapes))
	}
	if shapes["by_struct"] != shapes["by_document"] {
		t.Errorf("the struct declared shape %s and the document declared shape %s; the two sources must be one shape",
			shapes["by_struct"], shapes["by_document"])
	}
}

// everythingValue is one [Everything] record with every column populated,
// including the optional ones, so the record exercises both halves of the
// declaration.
func everythingValue() Everything {
	flag, count, seq := true, int32(7), int64(9)
	ratio, weight := float32(1.5), 2.5
	price, at := int64(1_234_567_890), int64(1_700_000_000_000)
	symbol := "SYM"
	payload := []byte{0x01, 0x02, 0x03}

	return Everything{
		Flag: flag, Count: count, Seq: seq, Ratio: ratio, Weight: weight,
		Price: price, At: at, Symbol: symbol, Payload: payload,
		OptFlag: &flag, OptCount: &count, OptSeq: &seq, OptRatio: &ratio, OptWeight: &weight,
		OptPrice: &price, OptAt: &at, OptSymbol: &symbol, OptPayload: &payload,
	}
}

// everythingLine is [everythingValue] as a JSON line, in the spellings the
// coercion table accepts.
func everythingLine(tb testing.TB) []byte {
	tb.Helper()

	v := everythingValue()
	line, err := json.Marshal(map[string]any{
		"flag":        v.Flag,
		"count":       v.Count,
		"seq":         v.Seq,
		"ratio":       v.Ratio,
		"weight":      v.Weight,
		"price":       string(decimalText(v.Price)),
		"at":          v.At,
		"symbol":      v.Symbol,
		"payload":     base64.StdEncoding.EncodeToString(v.Payload),
		"opt_flag":    v.Flag,
		"opt_count":   v.Count,
		"opt_seq":     v.Seq,
		"opt_ratio":   v.Ratio,
		"opt_weight":  v.Weight,
		"opt_price":   string(decimalText(v.Price)),
		"opt_at":      rfc3339Millis(v.At),
		"opt_symbol":  v.Symbol,
		"opt_payload": base64.StdEncoding.EncodeToString(v.Payload),
	})
	if err != nil {
		tb.Fatalf("rendering the record: %v", err)
	}

	return line
}

// TestSchemaDocumentRefusesEveryBadDocument is the parse half of scenario 10's
// refusals: every rule SCHEMA.md states about the document format, one case
// each, asserted on the typed error rather than on its message.
//
// Two of the cases assert an error type from elsewhere on purpose. A bad name is
// a NameError and a bad field id is a FieldIDError, because the rule being
// broken is the same rule a struct declaration breaks, and a caller matching on
// it should not have to know which of the two sources produced the table.
func TestSchemaDocumentRefusesEveryBadDocument(t *testing.T) {
	valid := func(fields string) string {
		return `{"tables":[{"namespace":"market","table":"fills","fields":[` + fields + `]}]}`
	}

	cases := []struct {
		name string
		doc  string
		kind icelake.SchemaDocKind
	}{
		{"not JSON at all", `{"tables":`, icelake.SchemaDocKindSyntax},
		{"an unknown key on a table", `{"tables":[{"namespace":"market","table":"fills","colums":[]}]}`, icelake.SchemaDocKindSyntax},
		{"an unknown key on a field", valid(`{"name":"a","type":"string","feildid":1}`), icelake.SchemaDocKindSyntax},
		{"a second JSON value after the document", valid(`{"name":"a","type":"string","fieldid":1}`) + `{}`, icelake.SchemaDocKindSyntax},
		{"a table with no fields", `{"tables":[{"namespace":"market","table":"fills","fields":[]}]}`, icelake.SchemaDocKindNoFields},
		{"two fields with one name", valid(`{"name":"a","type":"string","fieldid":1},{"name":"a","type":"long","fieldid":2}`), icelake.SchemaDocKindDuplicateField},
		{"a field with no name", valid(`{"type":"string","fieldid":1}`), icelake.SchemaDocKindDuplicateField},
		{"a type outside the nine", valid(`{"name":"a","type":"uuid","fieldid":1}`), icelake.SchemaDocKindUnknownType},
		{"a decimal with no scale", valid(`{"name":"a","type":"decimal(18)","fieldid":1}`), icelake.SchemaDocKindDecimal},
		{"a decimal that is not a spelling", valid(`{"name":"a","type":"decimal","fieldid":1}`), icelake.SchemaDocKindDecimal},
		{"a decimal past the int64 ceiling", valid(`{"name":"a","type":"decimal(19,2)","fieldid":1}`), icelake.SchemaDocKindDecimal},
		{"a decimal whose scale exceeds its precision", valid(`{"name":"a","type":"decimal(4,9)","fieldid":1}`), icelake.SchemaDocKindDecimal},
		{"a decimal with a non-numeric precision", valid(`{"name":"a","type":"decimal(x,2)","fieldid":1}`), icelake.SchemaDocKindDecimal},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tables, err := icelake.ParseSchemaDocument([]byte(c.doc))
			if tables != nil {
				t.Error("a refused document returned table configurations")
			}

			var docErr icelake.SchemaDocError
			if !errors.As(err, &docErr) {
				t.Fatalf("ParseSchemaDocument returned %v (%T), want an icelake.SchemaDocError", err, err)
			}
			if docErr.Kind != c.kind {
				t.Errorf("kind = %q, want %q", docErr.Kind, c.kind)
			}
		})
	}

	t.Run("a duplicate namespace and table pair", func(t *testing.T) {
		doc := `{"tables":[
			{"namespace":"market","table":"fills","fields":[{"name":"a","type":"string","fieldid":1}]},
			{"namespace":"market","table":"fills","fields":[{"name":"a","type":"string","fieldid":1}]}]}`

		var docErr icelake.SchemaDocError
		_, err := icelake.ParseSchemaDocument([]byte(doc))
		if !errors.As(err, &docErr) || docErr.Kind != icelake.SchemaDocKindDuplicateTable {
			t.Fatalf("ParseSchemaDocument returned %v, want a duplicate-table SchemaDocError", err)
		}
	})

	// The two refusals that deliberately are not this type.
	t.Run("a bad table name is a NameError", func(t *testing.T) {
		doc := `{"tables":[{"namespace":"market","table":"Fills","fields":[{"name":"a","type":"string","fieldid":1}]}]}`

		var nameErr icelake.NameError
		_, err := icelake.ParseSchemaDocument([]byte(doc))
		if !errors.As(err, &nameErr) {
			t.Fatalf("ParseSchemaDocument returned %v (%T), want an icelake.NameError", err, err)
		}
		if nameErr.Kind != icelake.NameKindTable {
			t.Errorf("kind = %q, want %q", nameErr.Kind, icelake.NameKindTable)
		}
	})

	t.Run("a field id violation is a FieldIDError", func(t *testing.T) {
		for _, c := range []struct {
			name   string
			doc    string
			gotID  int
			wantID int
		}{
			{"missing", valid(`{"name":"a","type":"string"}`), -1, 1},
			{"not the pre-order position", valid(`{"name":"a","type":"string","fieldid":1},{"name":"b","type":"long","fieldid":7}`), 7, 2},
		} {
			t.Run(c.name, func(t *testing.T) {
				var idErr icelake.FieldIDError
				_, err := icelake.ParseSchemaDocument([]byte(c.doc))
				if !errors.As(err, &idErr) {
					t.Fatalf("ParseSchemaDocument returned %v (%T), want an icelake.FieldIDError", err, err)
				}
				if idErr.FieldID != c.gotID || idErr.WantID != c.wantID {
					t.Errorf("FieldID/WantID = %d/%d, want %d/%d", idErr.FieldID, idErr.WantID, c.gotID, c.wantID)
				}
			})
		}
	})

	t.Run("a document declaring nothing is not an error", func(t *testing.T) {
		tables, err := icelake.ParseSchemaDocument([]byte(`{"tables":[]}`))
		if err != nil {
			t.Fatalf("ParseSchemaDocument on an empty document: %v", err)
		}
		if len(tables) != 0 {
			t.Errorf("an empty document parsed to %d tables", len(tables))
		}
	})

	t.Run("Flush and OnAccept are left for the caller", func(t *testing.T) {
		tables, err := icelake.ParseSchemaDocument([]byte(everythingDocument))
		if err != nil {
			t.Fatalf("ParseSchemaDocument: %v", err)
		}
		if tables[0].Flush != nil || tables[0].OnAccept != nil {
			t.Error("ParseSchemaDocument filled in Flush or OnAccept; a document states a shape, not a policy")
		}
		if tables[0].Namespace != "market" || tables[0].Table != "everything" {
			t.Errorf("table identity = %s.%s", tables[0].Namespace, tables[0].Table)
		}
		if tables[0].Schema.Len() != 18 {
			t.Errorf("the schema carries %d columns, want 18", tables[0].Schema.Len())
		}
	})
}

// fillDocument declares Case A's fill shape and Case B's payload-bearing shape,
// which is what scenario 10 asks a document to open two writers over.
const fillDocument = `{"tables": [
  {"namespace": "market", "table": "fills", "fields": [
    {"name": "symbol",             "type": "string",        "fieldid": 1},
    {"name": "side",               "type": "string",        "fieldid": 2},
    {"name": "price",              "type": "decimal(18,9)", "fieldid": 3},
    {"name": "quantity",           "type": "decimal(18,9)", "fieldid": 4},
    {"name": "sequence_id",        "type": "long",          "fieldid": 5},
    {"name": "order_id",           "type": "string",        "fieldid": 6},
    {"name": "source",             "type": "string",        "fieldid": 7},
    {"name": "venue_timestamp_ms", "type": "timestamptz",   "fieldid": 8},
    {"name": "venue_order_type",   "type": "string",        "fieldid": 9, "optional": true}
  ]},
  {"namespace": "archive", "table": "events", "fields": [
    {"name": "source_id",        "type": "string",      "fieldid": 1},
    {"name": "event_kind",       "type": "string",      "fieldid": 2},
    {"name": "sequence_ordinal", "type": "long",        "fieldid": 3},
    {"name": "observed_at_ms",   "type": "timestamptz", "fieldid": 4},
    {"name": "payload",          "type": "binary",      "fieldid": 5},
    {"name": "payload_sha256",   "type": "string",      "fieldid": 6}
  ]}
]}`

// dynamicFillLine renders one fill as a JSON line, cycling through every awkward
// form the coercion table admits.
//
// That cycling is the point of the clause: decimals arrive as strings and as
// exact JSON numbers, timestamps arrive as epoch millis and as RFC3339 at
// whole-millisecond precision, and the optional column is present, absent and
// explicitly null in turn. All of them have to land as the same values a struct
// writer would have produced.
func dynamicFillLine(tb testing.TB, i int) []byte {
	tb.Helper()

	fill := makeFill(i)
	row := map[string]any{
		"symbol":      fill.Symbol,
		"side":        fill.Side,
		"quantity":    json.RawMessage(decimalText(fill.Quantity)),
		"sequence_id": fill.SequenceID,
		"order_id":    fill.OrderID,
		"source":      fill.Source,
	}

	// A decimal as a string, then as an exact JSON number.
	if i%2 == 0 {
		row["price"] = string(decimalText(fill.Price))
	} else {
		row["price"] = json.RawMessage(decimalText(fill.Price))
	}

	// Epoch milliseconds, then RFC3339 at whole-millisecond precision.
	if i%2 == 0 {
		row["venue_timestamp_ms"] = fill.VenueTimeMS
	} else {
		row["venue_timestamp_ms"] = rfc3339Millis(fill.VenueTimeMS)
	}

	// Present, explicitly null, and absent in turn.
	switch i % 3 {
	case 0:
		row["venue_order_type"] = []string{"limit", "market"}[i%2]
	case 1:
		row["venue_order_type"] = nil
	}

	line, err := json.Marshal(row)
	if err != nil {
		tb.Fatalf("rendering fill %d: %v", i, err)
	}

	return line
}

// dynamicEventLine renders one archive record as a JSON line, with its payload
// in the one base64 spelling a binary column accepts.
func dynamicEventLine(tb testing.TB, i int) []byte {
	tb.Helper()

	event := makeEvent(i)
	line, err := json.Marshal(map[string]any{
		"source_id":        event.SourceID,
		"event_kind":       event.EventKind,
		"sequence_ordinal": event.SequenceOrdinal,
		"observed_at_ms":   event.ObservedAtMS,
		"payload":          base64.StdEncoding.EncodeToString(event.Payload),
		"payload_sha256":   event.PayloadSHA256,
	})
	if err != nil {
		tb.Fatalf("rendering event %d: %v", i, err)
	}

	return line
}

// rfc3339Millis renders epoch milliseconds as the one timestamp string spelling
// a timestamptz column accepts: RFC3339 at whole-millisecond precision, which is
// exactly the precision the column stores.
func rfc3339Millis(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// decimalText renders an unscaled decimal(18,9) value as the digits a caller
// would actually write: the exact decimal string, never a float.
func decimalText(unscaled int64) []byte {
	sign := ""
	if unscaled < 0 {
		sign, unscaled = "-", -unscaled
	}

	return fmt.Appendf(nil, "%s%d.%09d", sign, unscaled/1_000_000_000, unscaled%1_000_000_000)
}

// TestDynamicWriterRoundTripsThroughADocument is scenario 10's first clause: a
// document declaring two tables opens two writers, records go in as JSON lines
// carrying every awkward form the coercion table admits, and DuckDB reads back
// what scenario 1 reads back, to the same exactness.
func TestDynamicWriterRoundTripsThroughADocument(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	tables, err := icelake.ParseSchemaDocument([]byte(fillDocument))
	if err != nil {
		t.Fatalf("ParseSchemaDocument: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("the document declares %d tables, want 2", len(tables))
	}

	writers := make(map[string]*icelake.DynamicWriter, len(tables))
	for _, tc := range tables {
		w, err := icelake.OpenDynamicWriter(ctx, store, tc)
		if err != nil {
			t.Fatalf("OpenDynamicWriter %s.%s: %v", tc.Namespace, tc.Table, err)
		}
		writers[tc.Table] = w
	}

	const records = 120
	for i := range records {
		if err := writers["fills"].InsertJSON(ctx, dynamicFillLine(t, i)); err != nil {
			t.Fatalf("InsertJSON fill %d: %v", i, err)
		}
		if err := writers["events"].InsertJSON(ctx, dynamicEventLine(t, i)); err != nil {
			t.Fatalf("InsertJSON event %d: %v", i, err)
		}
	}
	for name, w := range writers {
		if err := w.Close(ctx); err != nil {
			t.Fatalf("Close %s: %v", name, err)
		}
	}

	duck := openDuckDB(t, bucket)

	fills := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))
	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", fills), &total)
	if total != records {
		t.Errorf("market.fills reads back %d rows, want %d", total, records)
	}

	// Scenario 1's exactness, held from the other front door: the decimal
	// against its exact decimal string rather than a tolerance, and the
	// timestamp as a real timestamp type rather than an integer to reinterpret.
	if got := duck.columnType(t, fills, "price"); got != "DECIMAL(18,9)" {
		t.Errorf("price reads back as %s, want DECIMAL(18,9)", got)
	}
	if got := duck.columnType(t, fills, "venue_timestamp_ms"); got != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("venue_timestamp_ms reads back as %s, want a real timestamp type", got)
	}
	for _, i := range []int{0, 1, 7, 42} {
		want := string(decimalText(makeFill(i).Price))
		var price string
		duck.queryRow(t, fmt.Sprintf("SELECT CAST(price AS VARCHAR) FROM %s WHERE sequence_id = %d", fills, i), &price)
		if price != want {
			t.Errorf("row %d reads price %s, want exactly %s", i, price, want)
		}

		var at int64
		duck.queryRow(t, fmt.Sprintf("SELECT epoch_ms(venue_timestamp_ms) FROM %s WHERE sequence_id = %d", fills, i), &at)
		if at != makeFill(i).VenueTimeMS {
			t.Errorf("row %d reads timestamp %d, want %d; both accepted spellings mean the same instant",
				i, at, makeFill(i).VenueTimeMS)
		}
	}

	// The optional column: valued, null and absent all landed as themselves.
	var valued, nulls int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE venue_order_type IS NOT NULL", fills), &valued)
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE venue_order_type IS NULL", fills), &nulls)
	if valued+nulls != records || valued == 0 || nulls == 0 {
		t.Errorf("the optional column reads %d valued and %d null over %d rows", valued, nulls, records)
	}

	// The payload column, byte for byte, checked the way Case B checks it.
	events := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "archive", "events"))
	var mismatched int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE lower(sha256(payload)) != payload_sha256", events), &mismatched)
	if mismatched != 0 {
		t.Errorf("%d payloads do not match their recorded sha256 after a base64 round trip", mismatched)
	}
	if got := duck.columnType(t, events, "payload"); got != "BLOB" {
		t.Errorf("payload reads back as %s, want BLOB", got)
	}
}

// fillDocumentV1 is the fills table before the optional column was added: the
// same eight columns, the same ids, and nothing else.
const fillDocumentV1 = `{"tables": [
  {"namespace": "market", "table": "fills", "fields": [
    {"name": "symbol",             "type": "string",        "fieldid": 1},
    {"name": "side",               "type": "string",        "fieldid": 2},
    {"name": "price",              "type": "decimal(18,9)", "fieldid": 3},
    {"name": "quantity",           "type": "decimal(18,9)", "fieldid": 4},
    {"name": "sequence_id",        "type": "long",          "fieldid": 5},
    {"name": "order_id",           "type": "string",        "fieldid": 6},
    {"name": "source",             "type": "string",        "fieldid": 7},
    {"name": "venue_timestamp_ms", "type": "timestamptz",   "fieldid": 8}
  ]}
]}`

// tableFromDocument parses a document and returns the one entry for a table,
// failing the test if the document does not declare it.
func tableFromDocument(tb testing.TB, doc, namespace, table string) icelake.DynamicTableConfig {
	tb.Helper()

	tables, err := icelake.ParseSchemaDocument([]byte(doc))
	if err != nil {
		tb.Fatalf("ParseSchemaDocument: %v", err)
	}
	for _, tc := range tables {
		if tc.Namespace == namespace && tc.Table == table {
			return tc
		}
	}
	tb.Fatalf("the document declares no table %s.%s", namespace, table)

	return icelake.DynamicTableConfig{}
}

// TestDocumentEvolutionIsTheSameEvolution is scenario 10's evolution clause.
//
// Adding a field with a fresh field id and "optional": true to a document, and
// reopening, must produce a real AddColumn against a table with committed data
// behind it, and must rewrite not one existing data file. That is the same claim
// Case A's evolution clause makes from the struct path, made here from the
// runtime path — and the reason it is worth making twice is that the
// reconciliation loop cannot tell the two sources apart, which is a property
// only a test through the other door can demonstrate.
func TestDocumentEvolutionIsTheSameEvolution(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	before, err := icelake.OpenDynamicWriter(ctx, store, tableFromDocument(t, fillDocumentV1, "market", "fills"))
	if err != nil {
		t.Fatalf("OpenDynamicWriter (before): %v", err)
	}
	for i := range 60 {
		if err := before.InsertJSON(ctx, dynamicFillLineV1(t, i)); err != nil {
			t.Fatalf("InsertJSON %d: %v", i, err)
		}
	}
	if err := before.Close(ctx); err != nil {
		t.Fatalf("Close (before): %v", err)
	}

	existing := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	if len(existing) == 0 {
		t.Fatal("the table has no data files before the schema change")
	}

	// The same document with one more optional column, reopened against the same
	// table.
	after, err := icelake.OpenDynamicWriter(ctx, store, tableFromDocument(t, fillDocument, "market", "fills"))
	if err != nil {
		t.Fatalf("OpenDynamicWriter (after): %v", err)
	}
	for i := 60; i < 100; i++ {
		if err := after.InsertJSON(ctx, dynamicFillLine(t, i)); err != nil {
			t.Fatalf("InsertJSON %d: %v", i, err)
		}
	}
	if err := after.Close(ctx); err != nil {
		t.Fatalf("Close (after): %v", err)
	}

	// Metadata-only: every file that existed before the change is still there,
	// same size and same entity tag.
	post := dataFiles(t, bucket, cfg.WarehousePrefix, "market", "fills")
	kept := make(map[string]object, len(post))
	for _, o := range post {
		kept[o.key] = o
	}
	for _, o := range existing {
		got, ok := kept[o.key]
		if !ok {
			t.Errorf("data file %s is gone after a schema change", o.key)

			continue
		}
		if got.size != o.size || got.etag != o.etag {
			t.Errorf("data file %s was rewritten by a schema change (%d bytes/%s, was %d/%s)",
				o.key, got.size, got.etag, o.size, o.etag)
		}
	}

	// And it reads back as one table across the boundary.
	duck := openDuckDB(t, bucket)
	fills := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))

	var total, nulls int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", fills), &total)
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE venue_order_type IS NULL", fills), &nulls)
	if total != 100 {
		t.Errorf("the table reads back %d rows, want 100", total)
	}
	if nulls < 60 {
		t.Errorf("%d rows read NULL in the added column, want at least the 60 written before it existed", nulls)
	}
}

// dynamicFillLineV1 is [dynamicFillLine] without the column the evolution adds.
func dynamicFillLineV1(tb testing.TB, i int) []byte {
	tb.Helper()

	var row map[string]any
	if err := json.Unmarshal(dynamicFillLine(tb, i), &row); err != nil {
		tb.Fatalf("re-reading fill %d: %v", i, err)
	}
	delete(row, "venue_order_type")

	line, err := json.Marshal(row)
	if err != nil {
		tb.Fatalf("rendering fill %d: %v", i, err)
	}

	return line
}

// TestTheTwoAPIsCrossOnOneTable is scenario 10's cross-API clause, and it is the
// caller-facing consequence of the two declaration sources producing one
// fingerprint.
//
// A struct writer accepts records and cannot deliver them; a dynamic writer
// declared from the equivalent document opens the same table, replays those
// rows, and commits them exactly once. Replay decodes by the shape recorded
// beside each row, so if the two paths disagreed by a single byte this is where
// it would surface — as a refusal at open, or as a batch that would not encode.
// It also puts the two codecs on opposite ends of one batch: rows bound by the
// struct binder are built into an Arrow record by the dynamic codec.
func TestTheTwoAPIsCrossOnOneTable(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	store := openStore(t, cfg)

	// The struct half: records accepted, and an endpoint that refuses every
	// connection, so they are left sealed in staging with nowhere to go.
	structWriter, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillV2]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	const records = 30
	for i := range records {
		if err := structWriter.Insert(ctx, makeFillV2(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	proxy.SetReject(true)
	if err := structWriter.Flush(ctx); err == nil {
		t.Fatal("Flush succeeded against an endpoint that refuses every connection")
	}
	if err := structWriter.Close(ctx); err == nil {
		t.Fatal("Close succeeded against an endpoint that refuses every connection")
	}
	if n := stagedRows(t, cfg.StagingPath); n != records {
		t.Fatalf("%d rows are staged, want the %d the struct writer accepted", n, records)
	}

	// The dynamic half: the same table, declared by the document, picking up
	// what the struct writer left.
	proxy.SetReject(false)
	dynamicWriter, err := icelake.OpenDynamicWriter(ctx, store, tableFromDocument(t, fillDocument, "market", "fills"))
	if err != nil {
		t.Fatalf("OpenDynamicWriter over a struct writer's table: %v", err)
	}
	if err := dynamicWriter.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// It writes normally afterwards, too.
	for i := records; i < records+10; i++ {
		if err := dynamicWriter.InsertJSON(ctx, dynamicFillLine(t, i)); err != nil {
			t.Fatalf("InsertJSON %d: %v", i, err)
		}
	}
	if err := dynamicWriter.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if n := stagedRows(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d rows are still staged after the dynamic writer drained the table", n)
	}

	// Exactly once: the replayed batch is one snapshot, not two, and the table
	// holds every record either writer accepted.
	duck := openDuckDB(t, bucket)
	fills := duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))

	var total, distinct int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", fills), &total)
	duck.queryRow(t, fmt.Sprintf("SELECT count(DISTINCT order_id) FROM %s", fills), &distinct)
	if total != records+10 || distinct != total {
		t.Errorf("the table reads back %d rows and %d distinct order ids, want %d of each: every record commits exactly once",
			total, distinct, records+10)
	}

	// The schema never moved, which is what equal declarations mean: one schema
	// in the table's metadata, not a second one added when the other front door
	// opened it.
	metadata := readMetadata(t, bucket, latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills"))
	if len(metadata.Schemas) != 1 {
		t.Errorf("the table carries %d schemas after being opened by both APIs, want 1", len(metadata.Schemas))
	}
}

// coercionDocument declares a table wide enough to hold one column of every kind
// the coercion table has a rule about, which is what the matrix below needs: a
// boolean to refuse 0, 1 and "true" against, an int to overflow, a long, a
// decimal, a timestamptz, a string, a binary, and an optional column to leave
// out.
const coercionDocument = `{"tables": [
  {"namespace": "market", "table": "coerce", "fields": [
    {"name": "flag",    "type": "boolean",       "fieldid": 1},
    {"name": "count",   "type": "int",           "fieldid": 2},
    {"name": "seq",     "type": "long",          "fieldid": 3},
    {"name": "price",   "type": "decimal(18,9)", "fieldid": 4},
    {"name": "at",      "type": "timestamptz",   "fieldid": 5},
    {"name": "symbol",  "type": "string",        "fieldid": 6},
    {"name": "payload", "type": "binary",        "fieldid": 7},
    {"name": "note",    "type": "string",        "fieldid": 8, "optional": true}
  ]}
]}`

// TestDynamicWriterRefusesEveryRowTheCoercionTableRefuses is scenario 10's
// coercion matrix: one case per refusal row in SCHEMA.md's table, each asserted
// on the typed error's Kind and Field rather than on its message, and each
// leaving staging untouched.
//
// It asserts the acceptances too, in the same table, because half the rows in
// that document are about what *is* taken — an integral json.Number that only
// parses through a double, both timestamp spellings — and a matrix that only
// listed refusals would be satisfied by a writer that refused everything.
//
// The two rows worth naming because they are the ones a reviewer will want to
// delete as pedantic are here on purpose: a float64 above 2^53 into a long, and
// a float64 into a decimal at all. Both arrive through Insert rather than
// InsertJSON, because InsertJSON's whole reason for existing is that it never
// produces one.
func TestDynamicWriterRefusesEveryRowTheCoercionTableRefuses(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	store := openStore(t, cfg)

	writer, err := icelake.OpenDynamicWriter(ctx, store, tableFromDocument(t, coercionDocument, "market", "coerce"))
	if err != nil {
		t.Fatalf("OpenDynamicWriter: %v", err)
	}

	// A row that is accepted, so every case below differs from a good one in
	// exactly one way. Its values are JSON-shaped — json.Number rather than Go
	// int — so that the Insert cases at the end are testing the one value they
	// changed rather than tripping over another.
	good := map[string]any{
		"flag":    true,
		"count":   json.Number("7"),
		"seq":     json.Number("9"),
		"price":   "1.5",
		"at":      json.Number("1700000000000"),
		"symbol":  "SYM",
		"payload": base64.StdEncoding.EncodeToString([]byte{0x01, 0x02}),
	}
	rowWith := func(mutate func(row map[string]any)) map[string]any {
		row := make(map[string]any, len(good))
		for k, v := range good {
			row[k] = v
		}
		mutate(row)

		return row
	}
	with := func(mutate func(row map[string]any)) []byte {
		line, err := json.Marshal(rowWith(mutate))
		if err != nil {
			t.Fatalf("rendering a row: %v", err)
		}

		return line
	}

	refusals := []struct {
		name  string
		line  []byte
		field string
		kind  icelake.RecordKind
	}{
		{"a key the declaration does not have", with(func(r map[string]any) { r["sybmol"] = "SYM" }), "sybmol", icelake.RecordKindUnknownField},
		{"a required column with no key", with(func(r map[string]any) { delete(r, "symbol") }), "symbol", icelake.RecordKindMissing},
		{"a null in a required column", with(func(r map[string]any) { r["symbol"] = nil }), "symbol", icelake.RecordKindNull},

		// boolean: exactly true and false, and nothing that merely means them.
		{"a boolean spelled as 0", with(func(r map[string]any) { r["flag"] = json.Number("0") }), "flag", icelake.RecordKindType},
		{"a boolean spelled as 1", with(func(r map[string]any) { r["flag"] = json.Number("1") }), "flag", icelake.RecordKindType},
		{`a boolean spelled as "true"`, with(func(r map[string]any) { r["flag"] = "true" }), "flag", icelake.RecordKindType},

		// int and long.
		{"a number into a string column", with(func(r map[string]any) { r["symbol"] = json.Number("1") }), "symbol", icelake.RecordKindType},
		{"a string where a number belongs", with(func(r map[string]any) { r["seq"] = "12" }), "seq", icelake.RecordKindType},
		{"a non-integral number in a long", with(func(r map[string]any) { r["seq"] = json.Number("1.5") }), "seq", icelake.RecordKindInexact},
		{"digits past the range of a long", with(func(r map[string]any) { r["seq"] = json.Number("99999999999999999999") }), "seq", icelake.RecordKindRange},
		{"an integral number past 2^53", with(func(r map[string]any) { r["seq"] = json.Number("1e300") }), "seq", icelake.RecordKindInexact},
		{"a number past the range of an int", with(func(r map[string]any) { r["count"] = json.Number("2147483648") }), "count", icelake.RecordKindRange},
		{"a number below the range of an int", with(func(r map[string]any) { r["count"] = json.Number("-2147483649") }), "count", icelake.RecordKindRange},

		// decimal: one grammar, no exponent, no more fraction digits than scale.
		{"a decimal with more fraction digits than its scale", with(func(r map[string]any) { r["price"] = "1.0123456789" }), "price", icelake.RecordKindInexact},
		{"a decimal written with an exponent", with(func(r map[string]any) { r["price"] = "1e3" }), "price", icelake.RecordKindInexact},
		{"a decimal with no digit before its point", with(func(r map[string]any) { r["price"] = ".5" }), "price", icelake.RecordKindMalformed},
		{"a decimal with no digit after its point", with(func(r map[string]any) { r["price"] = "5." }), "price", icelake.RecordKindMalformed},
		{"a decimal with a leading plus", with(func(r map[string]any) { r["price"] = "+1.5" }), "price", icelake.RecordKindMalformed},

		// timestamptz.
		{"a timestamp finer than a millisecond", with(func(r map[string]any) { r["at"] = "2026-08-01T00:00:00.0001Z" }), "at", icelake.RecordKindInexact},
		{"a timestamp that is not RFC3339", with(func(r map[string]any) { r["at"] = "yesterday" }), "at", icelake.RecordKindMalformed},

		// binary, and the row as a whole.
		{"a binary column given something that is not base64", with(func(r map[string]any) { r["payload"] = "not base64!" }), "payload", icelake.RecordKindMalformed},
		{"an object where a scalar belongs", with(func(r map[string]any) { r["symbol"] = map[string]any{"a": 1} }), "symbol", icelake.RecordKindType},
		{"a line that is not JSON at all", []byte(`{"symbol":`), "", icelake.RecordKindMalformed},
		{"a line that is a JSON array", []byte(`[1,2,3]`), "", icelake.RecordKindMalformed},
	}

	for _, c := range refusals {
		t.Run(c.name, func(t *testing.T) {
			err := writer.InsertJSON(ctx, c.line)

			var recErr icelake.RecordError
			if !errors.As(err, &recErr) {
				t.Fatalf("InsertJSON returned %v (%T), want an icelake.RecordError", err, err)
			}
			if recErr.Kind != c.kind {
				t.Errorf("kind = %q, want %q", recErr.Kind, c.kind)
			}
			if recErr.Field != c.field {
				t.Errorf("field = %q, want %q", recErr.Field, c.field)
			}
			if recErr.Table != "coerce" || recErr.Namespace != "market" {
				t.Errorf("table = %s.%s, want market.coerce", recErr.Namespace, recErr.Table)
			}
		})
	}

	// The acceptances. A json.Number that does not parse as an integer on its
	// own is still an integral value a caller may reasonably write, and the
	// table says so: it is read through a float64 to find out, and taken when it
	// is whole and inside 2^53.
	accepted := []struct {
		name string
		line []byte
	}{
		{"an integer written with a fraction that is zero", with(func(r map[string]any) { r["seq"] = json.Number("1.0") })},
		{"an integer written with trailing zeros", with(func(r map[string]any) { r["seq"] = json.Number("10.00") })},
		{"an integer written with an exponent", with(func(r map[string]any) { r["seq"] = json.Number("1e3") })},
		{"a timestamp written with a fraction that is zero", with(func(r map[string]any) { r["at"] = json.Number("1700000000000.0") })},
		{"a timestamp written as RFC3339 at whole milliseconds", with(func(r map[string]any) { r["at"] = "2026-08-01T00:00:00.001Z" })},
		{"an optional column left out", with(func(r map[string]any) { delete(r, "note") })},
		{"an optional column explicitly null", with(func(r map[string]any) { r["note"] = nil })},
	}

	for _, c := range accepted {
		t.Run(c.name, func(t *testing.T) {
			if err := writer.InsertJSON(ctx, c.line); err != nil {
				t.Fatalf("InsertJSON refused a row the coercion table accepts: %v", err)
			}
		})
	}

	// The two the front door makes unreachable, reached through the other one: a
	// caller who built the map itself, with an ordinary float64 in it.
	t.Run("a float64 above 2^53 into a long", func(t *testing.T) {
		var recErr icelake.RecordError
		row := rowWith(func(r map[string]any) { r["seq"] = float64(1 << 60) })
		if err := writer.Insert(ctx, row); !errors.As(err, &recErr) {
			t.Fatalf("Insert returned %v (%T), want an icelake.RecordError", err, err)
		} else if recErr.Kind != icelake.RecordKindInexact || recErr.Field != "seq" {
			t.Errorf("kind/field = %q/%q, want inexact/seq", recErr.Kind, recErr.Field)
		}
	})

	t.Run("a float64 into a decimal at all", func(t *testing.T) {
		var recErr icelake.RecordError
		row := rowWith(func(r map[string]any) { r["price"] = 1.5 })
		if err := writer.Insert(ctx, row); !errors.As(err, &recErr) {
			t.Fatalf("Insert returned %v (%T), want an icelake.RecordError", err, err)
		} else if recErr.Kind != icelake.RecordKindType || recErr.Field != "price" {
			t.Errorf("kind/field = %q/%q, want type/price", recErr.Kind, recErr.Field)
		}
	})

	// A hand-built map has to carry JSON-shaped values, and a plain Go int is
	// refused loudly rather than converted: the coercion table is closed, and a
	// library that widened it for one Go type would owe an answer for every
	// other one.
	t.Run("a plain Go int in a hand-built record", func(t *testing.T) {
		var recErr icelake.RecordError
		row := rowWith(func(r map[string]any) { r["seq"] = 9 })
		if err := writer.Insert(ctx, row); !errors.As(err, &recErr) {
			t.Fatalf("Insert returned %v (%T), want an icelake.RecordError", err, err)
		} else if recErr.Kind != icelake.RecordKindType || recErr.Field != "seq" {
			t.Errorf("kind/field = %q/%q, want type/seq", recErr.Kind, recErr.Field)
		}
	})

	// A refused row is not accepted: nothing any of those cases sent reached the
	// staging database, which is the same standard scenario 7 holds the poison
	// check to. Only the accepted rows are there.
	if n := stagedRows(t, cfg.StagingPath); n != len(accepted) {
		t.Errorf("%d rows are staged after %d refusals and %d acceptances; a refused row is never accepted",
			n, len(refusals)+3, len(accepted))
	}

	// And the poison check still runs underneath the coercion table: a value the
	// table admits but the column cannot hold is refused by the same check the
	// struct path runs.
	t.Run("a decimal past the column's precision is still a poison", func(t *testing.T) {
		var poison icelake.PoisonError
		line := with(func(r map[string]any) { r["price"] = "1000000000.5" })
		if err := writer.InsertJSON(ctx, line); !errors.As(err, &poison) {
			t.Fatalf("InsertJSON returned %v (%T), want an icelake.PoisonError", err, err)
		} else if poison.Reason != icelake.PoisonReasonDecimalRange {
			t.Errorf("reason = %q, want %q", poison.Reason, icelake.PoisonReasonDecimalRange)
		}
	})

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestDynamicWriterWritesInLocalOnlyMode is the one clause that puts the two M11
// and M12 features together, because that combination is the whole of the
// five-minute start the CLI is built for: a schema document, records on a pipe,
// no bucket and no credentials anywhere.
//
// It needs no container for the reason scenario 12's local-only half needs none:
// the mode has no substrate in it.
func TestDynamicWriterWritesInLocalOnlyMode(t *testing.T) {
	ctx := context.Background()
	cfg := localOnlyConfig(t, t.TempDir())
	store := openStore(t, cfg)

	writer, err := icelake.OpenDynamicWriter(ctx, store, tableFromDocument(t, fillDocument, "market", "fills"))
	if err != nil {
		t.Fatalf("OpenDynamicWriter: %v", err)
	}

	const records = 40
	for i := range records {
		if err := writer.InsertJSON(ctx, dynamicFillLine(t, i)); err != nil {
			t.Fatalf("InsertJSON %d: %v", i, err)
		}
	}

	// The four number spellings the coercion matrix proves are accepted, sent
	// here so the values they land as are proven too. The matrix's acceptance
	// rows assert only that staging gained a row; a regression that coerced
	// "1e3" to the wrong integer would pass it. These read back below, by
	// value, through DuckDB.
	edges := []string{
		`{"symbol":"EDGE","side":"buy","price":"1.5","quantity":"1","sequence_id":1000001.0,"order_id":"edge-frac-zero","source":"t","venue_timestamp_ms":1700000000001}`,
		`{"symbol":"EDGE","side":"buy","price":"1.5","quantity":"1","sequence_id":1000002.00,"order_id":"edge-two-zeros","source":"t","venue_timestamp_ms":1700000000002}`,
		`{"symbol":"EDGE","side":"buy","price":"1.5","quantity":"1","sequence_id":1.000003e6,"order_id":"edge-exponent","source":"t","venue_timestamp_ms":1700000000003}`,
		`{"symbol":"EDGE","side":"buy","price":"1.5","quantity":"1","sequence_id":1000004,"order_id":"edge-ts-frac","source":"t","venue_timestamp_ms":1700000000004.0}`,
	}
	for _, line := range edges {
		if err := writer.InsertJSON(ctx, []byte(line)); err != nil {
			t.Fatalf("InsertJSON edge %s: %v", line, err)
		}
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if n := stagedRows(t, cfg.StagingPath); n != 0 {
		t.Errorf("%d rows are still staged; local-only prunes against the spool file", n)
	}
	if files := cachedFiles(t, cfg.CacheDir); len(files) == 0 {
		t.Fatal("the cache holds no files after a local-only run")
	}

	duck := openLocalDuckDB(t)
	table := fmt.Sprintf("read_parquet('%s')", filepath.Join(cfg.CacheDir, "market", "fills", "data", "*.parquet"))

	var total int
	duck.queryRow(t, fmt.Sprintf("SELECT count(*) FROM %s", table), &total)
	if total != records+4 {
		t.Errorf("the cache reads back %d rows, want %d", total, records+4)
	}

	// The landed values of the four accepted edge spellings, exactly.
	for order, want := range map[string]int64{
		"edge-frac-zero": 1000001,
		"edge-two-zeros": 1000002,
		"edge-exponent":  1000003,
	} {
		var got int64
		duck.queryRow(t, fmt.Sprintf("SELECT sequence_id FROM %s WHERE order_id = '%s'", table, order), &got)
		if got != want {
			t.Errorf("%s landed sequence_id %d, want %d", order, got, want)
		}
	}
	var tsMillis int64
	duck.queryRow(t, fmt.Sprintf("SELECT epoch_ms(venue_timestamp_ms) FROM %s WHERE order_id = 'edge-ts-frac'", table), &tsMillis)
	if tsMillis != 1700000000004 {
		t.Errorf("edge-ts-frac landed at %d ms, want 1700000000004", tsMillis)
	}

	var price string
	duck.queryRow(t, fmt.Sprintf("SELECT CAST(price AS VARCHAR) FROM %s WHERE sequence_id = 7", table), &price)
	if want := string(decimalText(makeFill(7).Price)); price != want {
		t.Errorf("price reads back %s, want exactly %s", price, want)
	}
	if got := duck.columnType(t, table, "venue_timestamp_ms"); got != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("venue_timestamp_ms reads back as %s, want a real timestamp type", got)
	}
}
