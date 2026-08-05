package icelake

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/schemamap"
)

// Record is one row of a table declared at runtime: column names to values, the
// shape parsed JSON already has.
//
// It is an alias rather than a defined type on purpose. A caller that already
// holds a map[string]any — from its own decoder, from a transformation, from a
// test fixture — passes it straight in, and a caller reading a [WriterStatus]
// gets something every JSON-shaped tool in Go already understands.
//
// A record built by hand rather than decoded must carry decoder-shaped values,
// and that is a real constraint rather than a formality: the accepted Go types
// are json.Number, float64, string, bool, nil, and — for a binary column —
// either []byte or the base64 string a JSON decoder would have produced. A
// plain Go int in a long column
// is refused, loudly, with a [RecordError] of kind type naming the field, rather
// than being converted, because the coercion table is closed and a library that
// quietly widened it for one Go type would owe an answer for every other.
// [DynamicWriter.InsertJSON] never meets this: it decodes the line itself, so
// every value in the map it builds is one the table already accepts.
//
// One further consequence of it being a map is worth stating rather than leaving
// to be found: a record handed to Insert is read during that call and never
// again, so icelake never sees a later change to it — but
// [WriterStatus.RecentRecords] keeps the last few records by value, and the value
// of a map is a reference, so a caller that mutates a map it has already inserted
// changes what a later Status displays. That is the shallow-copy caveat
// WriterStatus already documents for any declaration type with a reference
// field, met here by every record rather than by some of them. InsertJSON has no
// such exposure either: the map it builds is freshly allocated per line and no
// caller holds it.
type Record = map[string]any

// DynamicWriter accepts records for one table declared by a runtime schema
// document instead of by a Go type.
//
// It is a defined struct embedding *[Writer][Record], deliberately not an alias
// for it: Go does not allow a method to be declared on an instantiated generic
// type, so [DynamicWriter.InsertJSON] could not hang off Writer[Record] under
// any spelling. Embedding promotes Insert, InsertBatch, Flush, Close, Pending
// and Status unchanged, so there is exactly one writer implementation in this
// library and this wrapper adds the two JSON doors to it — one line and a slice
// of lines — rather than duplicating any of them. Every
// guarantee the struct path makes — durable accept before Insert returns, the
// staging ceiling, sealing, replay, quarantine, the flush floor, drain on Close
// — is the same objects doing the same work.
type DynamicWriter struct {
	*Writer[Record]
}

// OpenDynamicWriter opens one table whose shape came from a schema document.
//
// It is [OpenWriter] with the declaration derived from field specs rather than
// from a Go type, and everything else is not merely equivalent but literally the
// same code: the same claim on the table's slot, the same spool drain before the
// table is loaded, the same startup lifecycle, the same pass over staging, the
// same worker and the same two goroutines. From the moment a declaration exists,
// nothing below this function can tell which source stated it.
//
// The derivation runs one check the struct path has no analogue for, and it runs
// it here, before a row is accepted. The struct path compares two independent
// reflections of one struct against each other; a document is read once and has
// no second reader to disagree with, so the exposure is different in kind — the
// hand-built Parquet nodes could stop matching what the tag parser would have
// built for the equivalent struct, silently, on some future arrow-go bump. So
// the check runs against the thing that actually matters downstream: the
// fingerprint of the descriptor derived *through* the three-call chain must
// equal the fingerprint of one built directly from the field specs, which never
// touches Parquet or Arrow at all. That fingerprint keys staging_schemas and is
// hashed into every batch key and therefore into every object name, so equality
// of the two is exactly the property the write path depends on. A disagreement
// is a [CrossCheckError] naming the first column the two shapes differ on.
func OpenDynamicWriter(ctx context.Context, s *Store, tc DynamicTableConfig) (*DynamicWriter, error) {
	if tc.Schema == nil || tc.Schema.Len() == 0 {
		return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindNoFields, tc.Table, "",
			"the table configuration carries no schema; a dynamic writer is opened against a stated shape", nil)
	}

	w, err := openWriter(ctx, s, writerPlan[Record]{
		namespace: tc.Namespace,
		table:     tc.Table,
		flush:     tc.Flush,
		onAccept:  tc.OnAccept,
		derive: func() (*schemamap.Declaration, *canon.Descriptor, rowCodec[Record], error) {
			decl, err := schemamap.DeclareFields(tc.Namespace, tc.Table, tc.Schema.fields)
			if err != nil {
				return nil, nil, nil, err
			}
			desc, err := canon.Describe(tc.Table, decl.IcebergSchema())
			if err != nil {
				return nil, nil, nil, err
			}
			if err := crossCheckSpecs(tc.Table, tc.Schema.fields, desc); err != nil {
				return nil, nil, nil, err
			}

			return decl, desc, newDynamicCodec(tc.Namespace, tc.Table, desc, schemamap.FileSchema(decl.ArrowSchema())), nil
		},
	})
	if err != nil {
		return nil, err
	}

	return &DynamicWriter{Writer: w}, nil
}

// crossCheckSpecs is the document path's twin of the struct path's
// two-derivation cross-check: one shape, two independent routes to a canonical
// descriptor, and their fingerprints must agree.
//
// The route being checked is the long one — field specs, Parquet nodes, Arrow,
// Iceberg, descriptor — against the short one, which is the specs read straight
// into a descriptor. What that pins, stated as what it is rather than as what it
// would be nice for it to be, is that the round trip through Parquet and Arrow
// gives every column back with its id, name, type, precision, scale and
// requiredness intact. That is the property the write path depends on, because
// the descriptor is what the canonical encoding, the schema fingerprint and
// every batch key are taken over — so a table declared by a document and the
// same table declared by a struct produce the same object names for the same
// records.
//
// What it deliberately does not pin, because it cannot, is the *physical* form
// of the Parquet nodes this path builds by hand. A decimal built on a
// fixed-length byte array derives the same Arrow and Iceberg column as one built
// on INT64, and therefore the same fingerprint; only the file's own encoding
// differs. That fidelity is held by TestStructAndFieldsDeriveOneSchema in
// internal/canon, which compares the two declarations' Parquet schemas as well —
// SCHEMA.md's M12 correction records how the division of labour was found, which
// was by making both mutations and seeing which check noticed.
func crossCheckSpecs(tableName string, fields []iceberg.NestedField, derived *canon.Descriptor) error {
	direct, err := canon.FromFields(tableName, fields)
	if err != nil {
		return err
	}
	if direct.Fingerprint() == derived.Fingerprint() {
		return nil
	}

	return errdef.NewCrossCheckError(tableName, firstShapeDifference(direct, derived), fmt.Sprintf(
		"the shape derived through the parquet and arrow chain fingerprints %s, and the shape read straight from the field specs fingerprints %s",
		derived.Fingerprint(), direct.Fingerprint()))
}

// firstShapeDifference names the first column two descriptors disagree about, so
// a cross-check failure carries the Table/Column pair the error type promises
// rather than only a pair of hashes.
func firstShapeDifference(a, b *canon.Descriptor) string {
	left, right := a.Fields(), b.Fields()
	for i, f := range left {
		if i >= len(right) {
			return f.Name
		}
		if left[i] != right[i] {
			return f.Name
		}
	}
	if len(right) > len(left) {
		return right[len(left)].Name
	}

	return ""
}

// InsertJSON accepts one record as a line of JSON.
//
// It is the documented front door for this writer, and the reason is the number
// path: it decodes with json.Decoder.UseNumber, so a JSON number arrives as its
// own digits rather than as a float64 that has already lost them. A decimal
// column never sees a binary float through this door, and an integer column
// never sees a value past 2^53 that only looks whole — which is exactly what
// [DynamicWriter.Insert] cannot promise about a map a caller built itself, and
// why the coercion table refuses those cases when it meets them.
//
// Everything after decoding is [Writer.Insert]: the same refusals in the same
// order, the same durable accept, the same mirror hook. The map this builds is
// freshly allocated per line and is not read again after the record is staged,
// so nothing a caller passes in can be changed underneath it and nothing it
// keeps is shared with a caller.
//
// A line that is not a JSON object is refused with a [RecordError] of kind
// malformed, naming no field, because the row as a whole is what is wrong.
func (w *DynamicWriter) InsertJSON(ctx context.Context, line []byte) error {
	row, err := w.decodeLine(line)
	if err != nil {
		return err
	}

	return w.Insert(ctx, row)
}

// InsertJSONBatch accepts a whole slice of records as lines of JSON, in one
// unit.
//
// It is [DynamicWriter.InsertJSON]'s decode applied to every line — the same
// decoder with the same UseNumber, the same refusal of a line carrying a second
// JSON value, the same refusal of a JSON null — followed by one
// [Writer.InsertBatch] over what they decoded to. So it inherits both halves
// unchanged: the exact-digits number path that is the reason this front door
// exists at all, and the batch's atomicity, which is that every line is durable
// when this returns nil and none of them is when it returns an error.
//
// A line that will not decode refuses the whole slice, with the [RecordError]
// that line earns on its own wrapped in a [BatchError] carrying its index. So
// does a record the table then refuses, with its own error and its own index.
// Nothing is staged on either path, and the caller still owns every line it
// passed in.
//
// The decode runs before the writer's own lifecycle check, exactly as it does in
// InsertJSON: a malformed line on a closed writer is answered as a malformed
// line. That is the single-line door's behaviour and this one matches it rather
// than inventing a second ordering for the same work.
//
// An empty slice touches nothing and returns whatever [Writer.InsertBatch]
// returns for one — nil on an open writer, and [ErrClosed] on a closed one,
// because a closed writer's answer is about the writer rather than about what it
// was handed. The lines are read during this call and never again, and the maps
// built from them are freshly allocated per line, so nothing a caller holds is
// shared with the writer afterwards.
func (w *DynamicWriter) InsertJSONBatch(ctx context.Context, lines [][]byte) error {
	if len(lines) == 0 {
		// Deliberately a call into the batch door rather than an early nil: the
		// lifecycle answer is that door's to give, and returning nil here would
		// make a closed writer answer one way for an empty slice of lines and
		// another for an empty slice of records.
		return w.InsertBatch(ctx, nil)
	}

	rows := make([]Record, len(lines))
	for i, line := range lines {
		row, err := w.decodeLine(line)
		if err != nil {
			return errdef.BatchError{Index: i, Err: err}
		}
		rows[i] = row
	}

	return w.InsertBatch(ctx, rows)
}

// decodeLine is the strict per-line decode both JSON doors run, in one place so
// that the batch door cannot come to differ from the single-line one it is
// documented as repeating.
func (w *DynamicWriter) decodeLine(line []byte) (Record, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()

	var row Record
	if err := dec.Decode(&row); err != nil {
		return nil, errdef.NewRecordError(errdef.RecordKindMalformed, w.decl.Namespace(), w.decl.Table(), "",
			fmt.Sprintf("the line is not a JSON object: %v", err))
	}
	if dec.More() {
		return nil, errdef.NewRecordError(errdef.RecordKindMalformed, w.decl.Namespace(), w.decl.Table(), "",
			"the line carries more than one JSON value")
	}
	if row == nil {
		return nil, errdef.NewRecordError(errdef.RecordKindMalformed, w.decl.Namespace(), w.decl.Table(), "",
			"the line is a JSON null rather than an object")
	}

	return row, nil
}

// dynamicCodec is the [rowCodec] for a table declared at runtime: parsed JSON in
// one direction, Arrow columns in the other.
//
// It holds the descriptor rather than any Go type, which is the whole point —
// the columns it validates against are the ones the table actually has, read
// from the same descriptor the canonical encoding and the batch hash are taken
// over.
type dynamicCodec struct {
	namespace string
	table     string
	desc      *canon.Descriptor
	// columns is the descriptor's fields, in ascending field-id order, which is
	// the order a canonical row's values are in.
	columns []canon.Field
	// byName resolves a JSON key to its column's position, so an unknown key is
	// a map miss rather than a scan.
	byName map[string]int
	// file is the Arrow schema data files are written with.
	file *arrow.Schema
}

// newDynamicCodec builds the codec for one table's recorded shape.
func newDynamicCodec(namespace, table string, desc *canon.Descriptor, file *arrow.Schema) *dynamicCodec {
	columns := desc.Fields()
	byName := make(map[string]int, len(columns))
	for i, c := range columns {
		byName[c.Name] = i
	}

	return &dynamicCodec{namespace: namespace, table: table, desc: desc, columns: columns, byName: byName, file: file}
}

// Bind converts one parsed record into its canonical row form.
//
// It is the closed coercion table SCHEMA.md sets out, and every value it does
// not cover is a loud typed refusal rather than a best-effort conversion. This
// is the same discipline as the column adapter's whitelist and the Go type
// whitelist, applied at the one seam where data enters without a compiler having
// looked at it: a refused row is not accepted, so the caller still owns it.
//
// Two passes rather than one, and the order is deliberate. Every key the record
// carries is resolved against the declaration first, so a typo is reported as
// the unknown key it is rather than as a missing column somewhere else; then
// every column is filled, so a required column with nothing in it is reported
// against the column rather than against whatever key happened to be last.
func (c *dynamicCodec) Bind(rec Record) (canon.Row, error) {
	for key := range rec {
		if _, ok := c.byName[key]; !ok {
			return nil, c.refuse(errdef.RecordKindUnknownField, key,
				"the declaration has no such column; an unknown key is either a typo or a schema that has not caught up")
		}
	}

	row := make(canon.Row, len(c.columns))
	for i, column := range c.columns {
		raw, present := rec[column.Name]
		if !present || raw == nil {
			if column.Required {
				kind := errdef.RecordKindMissing
				detail := "the column is required and the record has no such key"
				if present {
					kind, detail = errdef.RecordKindNull, "the column is required and cannot hold a null"
				}

				return nil, c.refuse(kind, column.Name, detail)
			}

			continue
		}

		v, err := c.coerce(column, raw)
		if err != nil {
			return nil, err
		}
		row[i] = v
	}

	return row, nil
}

// coerce turns one JSON value into the Go value its column's canonical encoding
// carries, or refuses it.
func (c *dynamicCodec) coerce(column canon.Field, raw any) (any, error) {
	switch column.Kind {
	case canon.KindBoolean:
		b, ok := raw.(bool)
		if !ok {
			return nil, c.refuse(errdef.RecordKindType, column.Name, fmt.Sprintf(
				"a boolean column holds JSON true or false, not %s; 0, 1 and \"true\" are a caller's bug rather than a dialect", jsonTypeOf(raw)))
		}

		return b, nil

	case canon.KindInt:
		n, err := c.integer(column, raw)
		if err != nil {
			return nil, err
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, c.refuse(errdef.RecordKindRange, column.Name, fmt.Sprintf(
				"value %d is outside the range of an int column", n))
		}

		return int32(n), nil

	case canon.KindLong:
		return c.integer(column, raw)

	case canon.KindFloat:
		f, err := c.float(column, raw)
		if err != nil {
			return nil, err
		}

		return float32(f), nil

	case canon.KindDouble:
		return c.float(column, raw)

	case canon.KindDecimal:
		return c.decimal(column, raw)

	case canon.KindTimestampTz:
		return c.timestamp(column, raw)

	case canon.KindString:
		s, ok := raw.(string)
		if !ok {
			return nil, c.refuse(errdef.RecordKindType, column.Name, fmt.Sprintf(
				"a string column holds a JSON string, not %s", jsonTypeOf(raw)))
		}

		return s, nil

	case canon.KindBinary:
		return c.binary(column, raw)

	default:
		return nil, c.refuse(errdef.RecordKindType, column.Name, fmt.Sprintf("column has %s", column.Kind))
	}
}

// integer reads an int or long column's value: a json.Number with an integral
// value, or a float64 that is integral and exact.
//
// The two arms meet at the same rule and it is worth saying why, because the
// obvious reading of "a json.Number is exact" is not true of every json.Number.
// A number whose digits parse straight into an int64 is exact and is taken as
// written — that is the whole reason InsertJSON decodes with UseNumber. A number
// that does *not* parse as an integer may still be an integer: "1.0", "10.00"
// and "1e3" are all integral values a caller may reasonably write, and reading
// them means going through a float64. So they are accepted on exactly the terms
// a float64 is accepted on, the 2^53 rule included, because at that point the
// value really has passed through a double and the digits are no longer
// necessarily the digits that were written.
//
// The float64 arm exists for [DynamicWriter.Insert], where a caller built the
// map itself and may well have decoded with the ordinary number path. It refuses
// any magnitude past 2^53 even when the value looks whole, for the reason just
// given, which is precisely why InsertJSON is the documented front door.
func (c *dynamicCodec) integer(column canon.Field, raw any) (int64, error) {
	switch v := raw.(type) {
	case json.Number:
		n, err := strconv.ParseInt(v.String(), 10, 64)
		if err == nil {
			return n, nil
		}
		// Digits that are an integer and simply too big for one. Reading it
		// through a double would only lose which integer it was.
		if errors.Is(err, strconv.ErrRange) {
			return 0, c.refuse(errdef.RecordKindRange, column.Name, fmt.Sprintf(
				"value %s is outside the range of a 64-bit integer", v.String()))
		}

		f, ferr := v.Float64()
		if ferr != nil {
			if errors.Is(ferr, strconv.ErrRange) {
				return 0, c.refuse(errdef.RecordKindRange, column.Name, fmt.Sprintf(
					"value %s is outside the range of any number this column can hold", v.String()))
			}

			return 0, c.refuse(errdef.RecordKindMalformed, column.Name, fmt.Sprintf(
				"value %s is not a number", v.String()))
		}

		return c.integralFloat(column, f, v.String())

	case float64:
		return c.integralFloat(column, v, strconv.FormatFloat(v, 'g', -1, 64))

	default:
		return 0, c.refuse(errdef.RecordKindType, column.Name, fmt.Sprintf(
			"an integer column holds a JSON number, not %s", jsonTypeOf(raw)))
	}
}

// integralFloat is the one rule both of [dynamicCodec.integer]'s arms end at: a
// value that reached an integer column through a binary float is accepted only
// if it is whole and small enough that the float still knows which whole number
// it is.
//
// The written form is passed in rather than re-derived, so the message quotes
// what the caller actually sent.
func (c *dynamicCodec) integralFloat(column canon.Field, f float64, written string) (int64, error) {
	if f != math.Trunc(f) {
		return 0, c.refuse(errdef.RecordKindInexact, column.Name, fmt.Sprintf(
			"value %s is not an integer, and an integer column stores no fraction", written))
	}
	if math.Abs(f) > 1<<53 {
		return 0, c.refuse(errdef.RecordKindInexact, column.Name, fmt.Sprintf(
			"value %s reached this column through a binary float above 2^53, where it is already not necessarily the value that was written", written))
	}

	return int64(f), nil
}

// float reads a float or double column's value. Any JSON number is accepted:
// these columns are binary floats by declaration, so a caller asking for one has
// already accepted what that means.
func (c *dynamicCodec) float(column canon.Field, raw any) (float64, error) {
	switch v := raw.(type) {
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, c.refuse(errdef.RecordKindRange, column.Name, fmt.Sprintf(
				"value %s is outside the range of a floating-point column", v.String()))
		}

		return f, nil

	case float64:
		return v, nil

	default:
		return 0, c.refuse(errdef.RecordKindType, column.Name, fmt.Sprintf(
			"a floating-point column holds a JSON number, not %s", jsonTypeOf(raw)))
	}
}

// decimal reads a decimal column's value and scales it into the column's
// declared scale.
//
// A float64 is refused always, whatever its value: the whole of this design's
// money decision is that a decimal never travels through a binary float, and an
// exception here would be the one place it did. What is accepted is digits — a
// json.Number or a string — with no exponent and at most as many fraction digits
// as the column's scale, because a value with more of them cannot be stored
// without dropping one, and dropping one silently is what this column type
// exists to prevent.
func (c *dynamicCodec) decimal(column canon.Field, raw any) (int64, error) {
	var text string
	switch v := raw.(type) {
	case json.Number:
		text = v.String()
	case string:
		text = v
	case float64:
		return 0, c.refuse(errdef.RecordKindType, column.Name,
			"a decimal column never accepts a binary float; send the digits, as a JSON number through InsertJSON or as a string")
	default:
		return 0, c.refuse(errdef.RecordKindType, column.Name, fmt.Sprintf(
			"a decimal column holds an exact number or a string of digits, not %s", jsonTypeOf(raw)))
	}

	unscaled, err := scaleDecimal(text, column.Scale)
	if err != nil {
		return 0, c.refuse(err.kind, column.Name, err.detail)
	}

	return unscaled, nil
}

// timestamp reads a timestamptz column's value: epoch milliseconds as an
// integral number, or an RFC3339 string at whole-millisecond precision.
//
// A string carrying microseconds or finer is refused rather than rounded. A
// silently truncated instant is the failure this column type exists to prevent,
// and a caller who really has microseconds needs to decide what to do about them
// rather than have icelake decide quietly.
func (c *dynamicCodec) timestamp(column canon.Field, raw any) (int64, error) {
	switch v := raw.(type) {
	case json.Number, float64:
		return c.integer(column, raw)

	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return 0, c.refuse(errdef.RecordKindMalformed, column.Name, fmt.Sprintf(
				"value %q is not an RFC3339 timestamp", v))
		}
		if t.Nanosecond()%int(time.Millisecond) != 0 {
			return 0, c.refuse(errdef.RecordKindInexact, column.Name, fmt.Sprintf(
				"value %q carries precision finer than a millisecond, which this column stores none of", v))
		}

		return t.UnixMilli(), nil

	default:
		return 0, c.refuse(errdef.RecordKindType, column.Name, fmt.Sprintf(
			"a timestamptz column holds epoch milliseconds as a number or an RFC3339 string, not %s", jsonTypeOf(raw)))
	}
}

// binary reads a binary column's value: raw bytes as they stand, or standard
// base64 with padding.
//
// The two arms are one rule seen from two formats, and neither is a second
// spelling of the other. A format that can carry bytes carries them — a CBOR
// byte string is bytes, and base64-ing it would be encoding a value the format
// already had — and a format that cannot spells them in exactly one encoding,
// so that a round trip is a fact rather than a guess: a value that decodes
// under two spellings is a value two producers can disagree about while both
// believing they are right. Both arms end at the same []byte, which is the
// whole of what the canonical encoding stores.
//
// **Decision (2026-08-04, M16): []byte joins the closed set of Go values a
// [Record] may carry, rather than CBOR byte strings being base64-encoded on
// their way in.** The rejected alternative was the cheaper one — normalise a
// CBOR byte string to the base64 string the JSON path already accepts, and
// change nothing here — and it is rejected because it makes a byte string
// indistinguishable from text: a CBOR byte string arriving at a *string* column
// would then be accepted as though the producer had typed base64, which is
// precisely the silent misreading every other row of this table is written to
// prevent. The cost accepted is that the Go type whitelist for a hand-built
// record is now six types rather than five, and `SCHEMA.md` states it.
func (c *dynamicCodec) binary(column canon.Field, raw any) ([]byte, error) {
	switch v := raw.(type) {
	case []byte:
		return v, nil

	case string:
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, c.refuse(errdef.RecordKindMalformed, column.Name,
				"value is not standard base64 with padding, which is the one encoding a binary column accepts from a text value")
		}

		return decoded, nil

	default:
		return nil, c.refuse(errdef.RecordKindType, column.Name, fmt.Sprintf(
			"a binary column holds raw bytes or a base64 string, not %s", jsonTypeOf(raw)))
	}
}

// refuse builds the typed refusal, naming the table and the column.
func (c *dynamicCodec) refuse(kind errdef.RecordKind, field, detail string) error {
	return errdef.NewRecordError(kind, c.namespace, c.table, field, detail)
}

// jsonTypeOf names what a decoded value is, for a message a human reads.
//
// It covers the byte-string case too, which no JSON decoder produces: a record
// decoded from CBOR travels the same coercion table, so a byte string offered
// to a column that cannot hold one has to be named in the refusal by what it
// is rather than by its Go type.
func jsonTypeOf(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case json.Number, float64:
		return "a number"
	case string:
		return "a string"
	case []byte:
		return "a byte string"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	default:
		return fmt.Sprintf("a %T", v)
	}
}

// decimalRefusal is why a decimal's digits could not be scaled, carried out of
// [scaleDecimal] so that the caller can attach the column name it knows and this
// function can stay a pure calculation over text.
type decimalRefusal struct {
	kind   errdef.RecordKind
	detail string
}

// Error implements the error interface. The message is the detail; the kind is
// what a caller acts on.
func (e *decimalRefusal) Error() string { return e.detail }

// scaleDecimal turns a decimal's digits into the unscaled int64 the canonical
// encoding stores, at the column's declared scale.
//
// It is deliberately a text calculation rather than a parse into a float and a
// multiply: the input is exact digits and the output is an exact integer, and
// putting a binary float between them is the one thing a decimal column must
// never do. An exponent is refused rather than expanded, because "1e3" is a
// spelling that invites a reader to disagree with a writer about how many
// significant digits were meant.
//
// The grammar it accepts is JSON's number grammar with the exponent removed: an
// optional minus, at least one digit, and optionally a point followed by at
// least one digit. Exactly one spelling per value, for the same reason exactly
// one base64 spelling is accepted for a binary column — ".5", "5." and "+1.5"
// are refused not because they are unreadable but because a value two producers
// can spell two ways is a value they can disagree about while both believing
// they are right.
func scaleDecimal(text string, scale int) (int64, *decimalRefusal) {
	if strings.ContainsAny(text, "eE") {
		return 0, &decimalRefusal{errdef.RecordKindInexact, fmt.Sprintf(
			"value %q carries an exponent; a decimal column takes plain digits so that its significant digits are not a matter of opinion", text)}
	}

	digits, negative := strings.CutPrefix(text, "-")
	whole, fraction, hasPoint := strings.Cut(digits, ".")

	malformed := &decimalRefusal{errdef.RecordKindMalformed, fmt.Sprintf(
		"value %q is not a decimal; a decimal is an optional minus, digits, and optionally a point and more digits", text)}
	if !allDigits(whole) || (hasPoint && !allDigits(fraction)) {
		return 0, malformed
	}

	if len(fraction) > scale {
		return 0, &decimalRefusal{errdef.RecordKindInexact, fmt.Sprintf(
			"value %q has %d fraction digits and the column stores %d; storing it would drop one", text, len(fraction), scale)}
	}

	// Padded rather than multiplied, for the same reason the exponent is
	// refused: the unscaled value is the digits themselves with the point moved,
	// and moving it by appending zeros cannot round anything.
	combined := whole + fraction + strings.Repeat("0", scale-len(fraction))

	unscaled, err := strconv.ParseInt(combined, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, &decimalRefusal{errdef.RecordKindRange, fmt.Sprintf(
				"value %q does not fit the int64 a decimal column's unscaled value is stored as", text)}
		}

		return 0, malformed
	}
	if negative {
		unscaled = -unscaled
	}

	return unscaled, nil
}

// allDigits reports whether s is one or more ASCII digits and nothing else,
// which is what both halves of a decimal's grammar are.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

// FileSchema returns the Arrow schema this codec builds records at: the declared
// schema with millisecond timestamps widened to the microseconds Iceberg
// defines.
//
// The writer takes its own copy of that schema from here rather than deriving a
// second one, so there is exactly one file schema per table and the record this
// codec builds is validated against the schema it was built with.
func (c *dynamicCodec) FileSchema() *arrow.Schema { return c.file }

// Build turns a batch's canonical rows into the Arrow record its data file is
// written from.
//
// It is the dynamic twin of the struct codec's Build, and it arrives at the same
// place by a shorter road. The struct path hands a slice of Go values to the
// row-data reflection layer, which infers Arrow columns from the Go types; there
// are no Go values here, so this builds each column directly from the canonical
// values — at exactly the types that reflection would have produced, which is
// what lets the identical [schemamap.AdaptColumn] tail run afterwards. The
// decimal sign-extension and the millisecond-to-microsecond rescale are that
// tail's, unchanged and already tested.
func (c *dynamicCodec) Build(rows []canon.Row) (record arrow.RecordBatch, err error) {
	mem := memory.DefaultAllocator
	fields := c.file.Fields()
	if len(fields) != len(c.columns) {
		return nil, errdef.NewCrossCheckError(c.table, "", fmt.Sprintf(
			"the file schema has %d columns and the table's shape has %d", len(fields), len(c.columns)))
	}

	adapted := make([]arrow.Array, 0, len(fields))
	defer func() {
		for _, a := range adapted {
			a.Release()
		}
	}()

	for i, column := range c.columns {
		raw, err := c.column(column, i, rows, mem)
		if err != nil {
			return nil, err
		}

		converted, err := schemamap.AdaptColumn(c.table, fields[i].Name, raw, fields[i].Type, mem)
		raw.Release()
		if err != nil {
			return nil, err
		}
		adapted = append(adapted, converted)
	}

	// The record constructor validates column count and type and reports a
	// disagreement by panicking. Nothing here can reach that — the adapter has
	// just produced each column at the schema's own type — but a background
	// flush must not be able to take a caller's process down, so it is turned
	// back into an error.
	defer func() {
		if r := recover(); r != nil {
			record, err = nil, errdef.NewCrossCheckError(c.table, "",
				fmt.Sprintf("the adapted columns do not match the declared schema: %v", r))
		}
	}()

	return array.NewRecordBatch(c.file, adapted, int64(len(rows))), nil
}

// column builds one column of a batch, at the Go type the canonical encoding
// carries for it — which is the type the row-data reflection produces for the
// equivalent struct field, so that what comes out of here is what
// [schemamap.AdaptColumn] already knows how to adapt.
//
// A decimal and a timestamp are both int64 here for that reason: the adapter
// sign-extends the first and rescales the second, exactly as it does on the
// struct path.
func (c *dynamicCodec) column(field canon.Field, at int, rows []canon.Row, mem memory.Allocator) (arrow.Array, error) {
	wrongType := func(v any) error {
		return errdef.NewEncodingError(errdef.EncodingKindValue, c.table, field.Name, field.ID, fmt.Sprintf(
			"decoded value has Go type %T, which is not what a %s column holds", v, field.Kind))
	}

	switch field.Kind {
	case canon.KindBoolean:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for _, row := range rows {
			v, ok := valueAt[bool](row, at)
			if !ok {
				if !isNullAt(row, at) {
					return nil, wrongType(row[at])
				}
				b.AppendNull()

				continue
			}
			b.Append(v)
		}

		return b.NewArray(), nil

	case canon.KindInt:
		b := array.NewInt32Builder(mem)
		defer b.Release()
		for _, row := range rows {
			v, ok := valueAt[int32](row, at)
			if !ok {
				if !isNullAt(row, at) {
					return nil, wrongType(row[at])
				}
				b.AppendNull()

				continue
			}
			b.Append(v)
		}

		return b.NewArray(), nil

	case canon.KindLong, canon.KindDecimal, canon.KindTimestampTz:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, row := range rows {
			v, ok := valueAt[int64](row, at)
			if !ok {
				if !isNullAt(row, at) {
					return nil, wrongType(row[at])
				}
				b.AppendNull()

				continue
			}
			b.Append(v)
		}

		return b.NewArray(), nil

	case canon.KindFloat:
		b := array.NewFloat32Builder(mem)
		defer b.Release()
		for _, row := range rows {
			v, ok := valueAt[float32](row, at)
			if !ok {
				if !isNullAt(row, at) {
					return nil, wrongType(row[at])
				}
				b.AppendNull()

				continue
			}
			b.Append(v)
		}

		return b.NewArray(), nil

	case canon.KindDouble:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, row := range rows {
			v, ok := valueAt[float64](row, at)
			if !ok {
				if !isNullAt(row, at) {
					return nil, wrongType(row[at])
				}
				b.AppendNull()

				continue
			}
			b.Append(v)
		}

		return b.NewArray(), nil

	case canon.KindString:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for _, row := range rows {
			v, ok := valueAt[string](row, at)
			if !ok {
				if !isNullAt(row, at) {
					return nil, wrongType(row[at])
				}
				b.AppendNull()

				continue
			}
			b.Append(v)
		}

		return b.NewArray(), nil

	case canon.KindBinary:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer b.Release()
		for _, row := range rows {
			v, ok := valueAt[[]byte](row, at)
			if !ok {
				if !isNullAt(row, at) {
					return nil, wrongType(row[at])
				}
				b.AppendNull()

				continue
			}
			b.Append(v)
		}

		return b.NewArray(), nil

	default:
		return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, c.table, field.Name, field.ID,
			fmt.Sprintf("column has %s", field.Kind))
	}
}

// valueAt reads one row's value for one column at the Go type that column's
// canonical encoding produces, reporting false for a null and for anything else.
func valueAt[V any](row canon.Row, at int) (V, bool) {
	var zero V
	if isNullAt(row, at) {
		return zero, false
	}
	v, ok := row[at].(V)

	return v, ok
}

// isNullAt reports whether a row has no value for a column.
//
// A row shorter than the shape counts as null rather than as a panic: the
// decoder produced these rows from the recorded descriptor, so a length
// disagreement is a corrupt payload the encoder refuses with a better error than
// an index out of range.
func isNullAt(row canon.Row, at int) bool { return at >= len(row) || row[at] == nil }
