package icelake

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/gvkhna/icelake/internal/errdef"
)

// cborMode is the one decoder configuration this library reads CBOR with.
//
// Every option on it is a refusal, and they are collected here rather than
// spread over the call sites so that "what icelake accepts as CBOR" is one
// object a reader can look at:
//
//   - DupMapKey enforced. A map with a key twice is refused rather than
//     resolved by first-wins or last-wins. Two producers can disagree about
//     which rule a decoder uses while both believing they are right, which is
//     the same argument the binary column's single base64 spelling rests on.
//   - UTF8RejectInvalid. A text string that is not valid UTF-8 is refused at the
//     decoder, before the poison check would refuse it one layer down. Both
//     refusals stay: this one is about CBOR's own rules and that one is about
//     what a column may hold.
//   - Unknown struct fields are an error, which is what makes the envelope's two
//     keys the only two an envelope may carry.
//   - DefaultMapType is map[string]any, so a map with a key that is not text is
//     refused rather than decoded into a shape no column could be named by.
//   - IndefLengthForbidden. An indefinite-length map, array, text string or byte
//     string is refused wherever it appears. RFC 8949's rules for deterministic
//     encoding exclude them, and the input grammar in ingest.go relies on the
//     same rule one level up, where a record's first byte has to say how long
//     the record is.
//
// It is built once at package initialisation. DecMode refuses only an option
// combination it cannot honour, and these options are constants in this file,
// so a failure here would be a defect in this file rather than anything a
// caller can cause — which is why it is not an error some call has to carry.
var cborMode = newCBORMode()

// cborStrictMode is cborMode with the last two refusals — every tag, and the
// undefined simple value — moved into the decoder itself, which is what lets a
// valid record be read in one pass.
//
// **Decision (2026-08-04, after M16 shipped, from a profile rather than a
// guess): the happy path decodes once, through this mode.** M16's decode read
// every record twice — the row into raw per-field bytes, then each field on its
// own — because a plain decode interprets tag 1 into a time value and reads
// undefined as the same nil as null, and the refusals have to be sharper than
// that. The profiler priced that choice at about two fifths of the whole
// decode: the second pass alone was a fifth, and it is why a binary format
// measured slower than JSON here. The decoder already knows how to make both
// refusals itself — TagsForbidden refuses every tag number in its
// well-formedness pass, before interpretation exists to happen, and a simple-
// value registry rejecting number 23 refuses undefined while leaving null
// alone — so the two-pass shape was paying for sharpness the options give
// away for free. Measured after the change, the CBOR decode is faster than
// the JSON decode, which is the way round a binary format should come out.
// What this mode cannot do is say *which field* carried the tag it refused,
// so the two-pass decode below is kept as the error path: when this mode
// refuses a record, the slow decode re-reads it to name the field and the
// reason, at a cost only a refused record pays.
var cborStrictMode = newCBORStrictMode()

// cborBaseOptions is the option set both modes share; the doc comment on
// [cborMode] is its specification.
func cborBaseOptions() cbor.DecOptions {
	return cbor.DecOptions{
		DupMapKey:         cbor.DupMapKeyEnforcedAPF,
		UTF8:              cbor.UTF8RejectInvalid,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
		DefaultMapType:    reflect.TypeOf(map[string]any(nil)),
		IndefLength:       cbor.IndefLengthForbidden,
	}
}

// newCBORMode builds [cborMode].
func newCBORMode() cbor.DecMode {
	mode, err := cborBaseOptions().DecMode()
	if err != nil {
		panic("icelake: the CBOR decode options in cbor.go do not form a decode mode: " + err.Error())
	}

	return mode
}

// newCBORStrictMode builds [cborStrictMode].
func newCBORStrictMode() cbor.DecMode {
	simple, err := cbor.NewSimpleValueRegistryFromDefaults(cbor.WithRejectedSimpleValue(cborUndefinedSimpleValue))
	if err != nil {
		panic("icelake: the CBOR simple-value registry in cbor.go cannot be built: " + err.Error())
	}

	opts := cborBaseOptions()
	opts.TagsMd = cbor.TagsForbidden
	opts.SimpleValues = simple

	mode, err := opts.DecMode()
	if err != nil {
		panic("icelake: the strict CBOR decode options in cbor.go do not form a decode mode: " + err.Error())
	}

	return mode
}

// The two CBOR encodings this library refuses on sight, recognised from the
// first byte of an item rather than from whatever Go value a decoder would have
// produced for it.
//
// Reading the major type out of the head is not a second CBOR parser: an item's
// first three bits are its major type, always, in every version of the format,
// and that is the whole of what these two constants read. Doing it here rather
// than after decoding is what makes the refusal general — a tag icelake has
// never heard of is refused exactly like tag 0, where a check on the decoded Go
// type would only catch the handful the decoder knows how to turn into
// something.
const (
	// cborMajorTag is major type 6, a tagged value.
	cborMajorTag = 6
	// cborUndefined is the whole encoding of the undefined simple value.
	cborUndefined = 0xf7
	// cborUndefinedSimpleValue is undefined's simple-value number, which is what
	// the strict mode's registry rejects it by. The two constants describe the
	// same value from two sides: 0xf7 is its whole encoding on the wire, and 23
	// is its number in the simple-value space (0xe0 + 23).
	cborUndefinedSimpleValue = 23
)

// InsertCBOR accepts one record as a single CBOR data item.
//
// It is [DynamicWriter.InsertJSON]'s twin in the other format and it exists for
// the same reason that one does: a producer that is not Go hands over bytes,
// and the door that takes bytes is the door that can promise what they mean.
// Everything after decoding is [Writer.Insert] — the same coercion table, the
// same refusals in the same order, the same durable accept, the same mirror
// hook — so a record sent as CBOR and the same record sent as JSON reach
// staging as identical bytes. `SCHEMA.md` owns the table that makes that true.
//
// The item must be a CBOR map with text keys. An item that is not is refused
// with a [RecordError] of kind malformed, naming no field, because the row as a
// whole is what is wrong.
func (w *DynamicWriter) InsertCBOR(ctx context.Context, item []byte) error {
	row, err := w.decodeCBORItem(item)
	if err != nil {
		return err
	}

	return w.Insert(ctx, row)
}

// InsertCBORBatch accepts a whole slice of records as CBOR data items, in one
// unit.
//
// It is [DynamicWriter.InsertJSONBatch] in the other format, and it inherits
// both halves of that door unchanged: the strict per-item decode, and the
// batch's atomicity, which is that every item is durable when this returns nil
// and none of them is when it returns an error.
//
// An item that will not decode refuses the whole slice, with the [RecordError]
// that item earns on its own wrapped in a [BatchError] carrying its index. So
// does a record the table then refuses, with its own error and its own index.
// Nothing is staged on either path, and the caller still owns every item it
// passed in.
//
// The decode runs before the writer's own lifecycle check, exactly as it does
// in InsertCBOR and exactly as the JSON doors order the same two things. An
// empty slice touches nothing and returns whatever [Writer.InsertBatch] returns
// for one — nil on an open writer, [ErrClosed] on a closed one.
func (w *DynamicWriter) InsertCBORBatch(ctx context.Context, items [][]byte) error {
	if len(items) == 0 {
		// Deliberately a call into the batch door rather than an early nil: the
		// lifecycle answer is that door's to give, and returning nil here would
		// make a closed writer answer one way for an empty slice of items and
		// another for an empty slice of records.
		return w.InsertBatch(ctx, nil)
	}

	rows := make([]Record, len(items))
	for i, item := range items {
		row, err := w.decodeCBORItem(item)
		if err != nil {
			return errdef.BatchError{Index: i, Err: err}
		}
		rows[i] = row
	}

	return w.InsertBatch(ctx, rows)
}

// decodeCBORItem is the strict per-item decode both CBOR doors run, in one
// place so that the batch door cannot come to differ from the single-item one
// it is documented as repeating. It is [DynamicWriter.decodeLine]'s twin.
//
// A valid record is read in one pass through [cborStrictMode], whose decoder
// makes every refusal itself — the reasoning, the profile that forced it and
// the cost it replaced are on that mode's declaration. A record that pass
// refuses is read again by [DynamicWriter.decodeCBORItemSlow], the original
// two-step decode, whose whole remaining job is to say precisely what was
// wrong: which field, which tag, undefined rather than null. Only a refused
// record pays for the second read, and every refusal a caller sees is worded
// by the slow path, so the two paths cannot disagree about what is accepted —
// the strict mode is the base options plus two refusals the slow path also
// makes, only later and with better words.
func (w *DynamicWriter) decodeCBORItem(item []byte) (Record, error) {
	var fields map[string]any
	rest, err := cborStrictMode.UnmarshalFirst(item, &fields)
	if err != nil {
		return w.decodeCBORItemSlow(item)
	}

	malformed := func(detail string) error {
		return errdef.NewRecordError(errdef.RecordKindMalformed, w.decl.Namespace(), w.decl.Table(), "", detail)
	}
	if len(rest) > 0 {
		return nil, malformed("the item carries more than one CBOR data item")
	}
	if fields == nil {
		return nil, malformed("the item is CBOR null rather than a map")
	}

	row := make(Record, len(fields))
	for name, value := range fields {
		v, err := w.cborValue(name, value)
		if err != nil {
			return nil, err
		}
		row[name] = v
	}

	return row, nil
}

// decodeCBORItemSlow is the two-step decode, kept as the error path.
//
// It reads the row into its fields' raw bytes and then each field on its own,
// which is what lets a tagged value be refused as a tagged value naming its
// column, and undefined be told from null — the sharpness the one-pass decode
// above cannot word, bought at the price of reading the record twice, which
// only a record the one-pass decode refused ever pays.
func (w *DynamicWriter) decodeCBORItemSlow(item []byte) (Record, error) {
	malformed := func(detail string) error {
		return errdef.NewRecordError(errdef.RecordKindMalformed, w.decl.Namespace(), w.decl.Table(), "", detail)
	}

	var fields map[string]cbor.RawMessage
	rest, err := cborMode.UnmarshalFirst(item, &fields)
	if err != nil {
		return nil, malformed(fmt.Sprintf("the item is not a CBOR map with text keys: %v", err))
	}
	if len(rest) > 0 {
		return nil, malformed("the item carries more than one CBOR data item")
	}
	if fields == nil {
		return nil, malformed("the item is CBOR null rather than a map")
	}

	row := make(Record, len(fields))
	for name, raw := range fields {
		value, err := w.decodeCBORField(name, raw)
		if err != nil {
			return nil, err
		}
		row[name] = value
	}

	return row, nil
}

// decodeCBORField turns one field's CBOR bytes into the Go value the coercion
// table reads, or refuses it.
//
// The mapping it applies is closed and is stated in `SCHEMA.md`'s CBOR column:
// an integer becomes its own decimal digits as a [json.Number], which is the
// same exact-digits path `InsertJSON`'s UseNumber decode produces and is why an
// integer past 2^53 is exact here rather than inexact; a byte string becomes
// []byte, which a binary column takes as it stands and every other column
// refuses; text becomes a string; a float becomes a float64, half, single and
// double alike, and meets exactly the refusals a JSON float meets; a boolean
// becomes a bool; null becomes nil. Everything else — a tag, undefined, an
// array, a map — is refused, because no column holds it and guessing which one
// a caller meant is how a value ends up in a column that will not say what
// happened to it.
func (w *DynamicWriter) decodeCBORField(field string, raw cbor.RawMessage) (any, error) {
	refuse := func(kind errdef.RecordKind, detail string) (any, error) {
		return nil, errdef.NewRecordError(kind, w.decl.Namespace(), w.decl.Table(), field, detail)
	}

	switch {
	case len(raw) == 0:
		return refuse(errdef.RecordKindMalformed, "the field carries no CBOR item at all")
	case raw[0]>>5 == cborMajorTag:
		return refuse(errdef.RecordKindType, fmt.Sprintf(
			"the value carries %s, and a tag is refused rather than interpreted: a tagged value is a second spelling of "+
				"something a column already accepts exactly one spelling of", cborTagName(raw)))
	case raw[0] == cborUndefined:
		return refuse(errdef.RecordKindType,
			"the value is CBOR undefined; null means a column has no value and undefined means something else, "+
				"so reading one as the other would decide, quietly, that they are the same")
	}

	var value any
	if err := cborMode.Unmarshal(raw, &value); err != nil {
		return refuse(errdef.RecordKindMalformed, fmt.Sprintf("the value is not a CBOR value this library reads: %v", err))
	}

	return w.cborValue(field, value)
}

// cborValue turns one field's decoded CBOR value into the Go value the coercion
// table reads, or refuses it. It is the half of the field decode both paths
// share — the one-pass decode hands it values straight out of the decoder, the
// two-step error path hands it values decoded from a field's raw bytes, and
// having exactly one copy is what keeps the two paths' answers identical for
// every value both can reach.
func (w *DynamicWriter) cborValue(field string, value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return v, nil
	case uint64:
		// A CBOR integer is an integer by construction, so it travels as its
		// own digits and never through a float. That is what makes the
		// coercion table's 2^53 rule inapplicable to it: there is no binary
		// float anywhere on this path to have lost anything.
		return json.Number(strconv.FormatUint(v, 10)), nil
	case int64:
		return json.Number(strconv.FormatInt(v, 10)), nil
	case float64:
		return v, nil
	case string:
		return v, nil
	case []byte:
		return v, nil
	default:
		return nil, errdef.NewRecordError(errdef.RecordKindType, w.decl.Namespace(), w.decl.Table(), field, fmt.Sprintf(
			"a %s is not a value any column holds", cborTypeOf(value)))
	}
}

// cborTagName names the tag on an item, with its number when the head carries
// the number in its own byte, which is every tag below 24 and therefore every
// tag a caller is likely to have reached for by accident: 0 and 1 are the two
// date/time tags and 2 and 3 are the bignums.
func cborTagName(raw []byte) string {
	if info := raw[0] & 0x1f; info < 24 {
		return fmt.Sprintf("CBOR tag %d", info)
	}

	return "a CBOR tag"
}

// cborTypeOf names what a decoded CBOR value is, for a message a human reads.
// The tagged types appear in it even though a tag is refused before any of them
// can be produced, because a decoder configured differently in some future
// version of this file would reach them and a message reading "a time.Time"
// helps nobody.
func cborTypeOf(v any) string {
	switch v.(type) {
	case []any:
		return "CBOR array"
	case map[string]any, map[any]any:
		return "CBOR map"
	case time.Time:
		return "CBOR date/time"
	case big.Int, *big.Int:
		return "CBOR bignum"
	case cbor.Tag:
		return "tagged CBOR value"
	default:
		return fmt.Sprintf("CBOR value of Go type %T", v)
	}
}
