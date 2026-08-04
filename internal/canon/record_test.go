package canon

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/gvkhna/icelake/internal/errdef"
)

// This file is both of TESTING.md's sanctioned tiers at once, and which test is
// which is worth stating rather than leaving to be inferred.
//
// The golden encode and decode tests, and the round trip beside them, are the
// first category: the canonical record encoding is named on that document's
// closed list of calculations whose exact output matters, and every expected
// byte string here is assembled from the format specification in this package's
// doc comment rather than captured from the encoder. That is the whole reason
// the exception exists — a payload staged today is decoded by a build that does
// not exist yet.
//
// The refusals and the aliasing tests are the second category, and they hold its
// second claim shape: defences this layer keeps against its own in-process
// callers, which the public path is built to make unreachable.
// TestEncodeRefusesValuesItCannotWrite covers Go values Declare has already
// refused one layer up, so no caller reaches it; TestDecodeRefusesMalformed
// Payloads reads bytes icelake never writes, which arrive only from a damaged
// staging file or from another build's encoding; TestEncodeTreatsNilAndEmpty
// BinaryAlike and TestDecodedBinaryDoesNotAliasThePayload pin the two places
// where an ambiguity or a shared backing array would silently change a record
// after it was hashed. The invariant all four defend is the same one: a batch's
// stored key names its own bytes, and a payload decoded on any future build is
// the row that was staged. Lose that and a retried flush writes a different
// object under a key that claims otherwise.

// goldenRow1 is the first golden record: every column present except the
// optional string, which is null, and the optional long, which carries a value.
// It exists to pin one payload of every type at once, including the two
// optional shapes.
var goldenRow1 = Row{
	true,                     // 1  flag
	int32(-2),                // 2  count
	int64(1_000_000),         // 3  seq
	float32(1.5),             // 4  ratio
	float64(-2.5),            // 5  weight
	int64(1234567890),        // 6  price, unscaled, i.e. 1.234567890 at scale 9
	int64(1_700_000_000_000), // 7  at, epoch milliseconds
	"btc_usd",                // 8  symbol
	[]byte{0x82, 0xa1, 0x61}, // 9  payload
	nil,                      // 10 note, null
	int64(42),                // 11 extra, present
}

// goldenPayload1 is what goldenRow1 encodes to under the golden descriptor,
// written out from the layout in this package's doc comment: a format-version
// byte, then each column's value in ascending field-id order, with a presence
// byte before an optional column's value and none before a required one.
var goldenPayload1 = []byte{
	0x01, // encoding format version 1

	0x01,                   // id 1  flag = true (required, so no presence byte)
	0xff, 0xff, 0xff, 0xfe, // id 2  count = -2, two's complement int32
	0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x42, 0x40, // id 3  seq = 1000000 = 0xF4240
	0x3f, 0xc0, 0x00, 0x00, // id 4  ratio = 1.5: sign 0, exponent 127, fraction 0.5
	0xc0, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id 5  weight = -2.5 as binary64
	0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xd2, // id 6  price = 1234567890 = 0x499602D2
	0x00, 0x00, 0x01, 0x8b, 0xcf, 0xe5, 0x68, 0x00, // id 7  at = 1700000000000

	0x00, 0x00, 0x00, 0x07, // id 8  symbol, 7 bytes
	0x62, 0x74, 0x63, 0x5f, 0x75, 0x73, 0x64, //        "btc_usd"

	0x00, 0x00, 0x00, 0x03, // id 9  payload, 3 bytes
	0x82, 0xa1, 0x61, //                                the payload's own bytes

	0x00, // id 10 note is optional and null: presence 0x00, no value follows

	0x01,                                           // id 11 extra is optional and present: presence 0x01
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, //  extra = 42
}

// goldenRow2 is the second golden record, chosen to pin the cases that are easy
// to get wrong and impossible to notice: an empty string, an empty binary, a
// false boolean, positive zero in both float widths, a negative decimal, and
// the opposite null pattern from goldenRow1.
var goldenRow2 = Row{
	false,              // 1  flag
	int32(0),           // 2  count
	int64(-1),          // 3  seq
	float32(0),         // 4  ratio
	float64(0),         // 5  weight
	int64(-1234567890), // 6  price
	int64(0),           // 7  at
	"",                 // 8  symbol, empty
	[]byte{},           // 9  payload, empty
	"hi",               // 10 note, present
	nil,                // 11 extra, null
}

// goldenPayload2 is goldenRow2's encoding. The two length-prefixed columns show
// the empty case explicitly: a four-byte zero length and no value bytes at all.
var goldenPayload2 = []byte{
	0x01, // encoding format version 1

	0x00,                   // id 1  flag = false
	0x00, 0x00, 0x00, 0x00, // id 2  count = 0
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // id 3  seq = -1, all ones
	0x00, 0x00, 0x00, 0x00, // id 4  ratio = +0.0
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id 5  weight = +0.0
	0xff, 0xff, 0xff, 0xff, 0xb6, 0x69, 0xfd, 0x2e, // id 6  price = -1234567890 = 2^64 - 0x499602D2
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id 7  at = 0

	0x00, 0x00, 0x00, 0x00, // id 8  symbol = "", length 0, no bytes follow
	0x00, 0x00, 0x00, 0x00, // id 9  payload = empty, length 0, no bytes follow

	0x01,                   // id 10 note is present
	0x00, 0x00, 0x00, 0x02, //  length 2
	0x68, 0x69, //              "hi"

	0x00, // id 11 extra is null
}

// TestEncodeProducesGoldenPayloadBytes pins the canonical encoding of a record
// carrying every column type at once. These bytes are stored in
// staging.payload and are the exact input a batch's object key is hashed over,
// so a change to any one of them changes where a retried upload lands.
func TestEncodeProducesGoldenPayloadBytes(t *testing.T) {
	d := goldenDescriptor(t)

	got, err := Encode("fills", d, goldenRow1)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !slices.Equal(got, goldenPayload1) {
		t.Errorf("payload bytes:\n got %x\nwant %x", got, goldenPayload1)
	}
	if want := 70; len(got) != want {
		t.Errorf("payload length = %d bytes, want %d", len(got), want)
	}
}

// TestEncodeProducesGoldenPayloadBytesForEmptyAndNullValues pins the same thing
// for the empty and null cases, which have no visible bytes of their own and so
// are exactly the ones a wrong encoding would hide.
func TestEncodeProducesGoldenPayloadBytesForEmptyAndNullValues(t *testing.T) {
	d := goldenDescriptor(t)

	got, err := Encode("fills", d, goldenRow2)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !slices.Equal(got, goldenPayload2) {
		t.Errorf("payload bytes:\n got %x\nwant %x", got, goldenPayload2)
	}
	if want := 58; len(got) != want {
		t.Errorf("payload length = %d bytes, want %d", len(got), want)
	}
}

// TestEncodeTreatsNilAndEmptyBinaryAlike pins a documented normalization rather
// than leaving it to be discovered. A required binary column has no null to
// round-trip to, so a nil []byte is a present, empty value — which is also what
// the Arrow and Parquet sides of the write path do with it. Stating it as a
// test keeps it a decision instead of an accident.
func TestEncodeTreatsNilAndEmptyBinaryAlike(t *testing.T) {
	d := goldenDescriptor(t)

	withNil := slices.Clone(goldenRow2)
	withNil[8] = []byte(nil)

	got, err := Encode("fills", d, withNil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !slices.Equal(got, goldenPayload2) {
		t.Errorf("a nil []byte encoded differently from an empty one:\n got %x\nwant %x", got, goldenPayload2)
	}
}

// TestEncodeWritesValuesInAscendingFieldIDOrder proves the payload's order
// comes from the permanent ids and not from the order the columns were
// declared in. Reordering a declaration's fields is legal — identity is the id
// — and must not change a single byte of any record.
func TestEncodeWritesValuesInAscendingFieldIDOrder(t *testing.T) {
	// Declared c, a, b; ids 9, 2, 5. The payload must come out a, b, c.
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 9, Name: "c", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 5, Name: "b", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	d, err := Describe("fills", sc)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}

	// The Row is in descriptor order, which is id order: a=1, b=2, c=3.
	got, err := Encode("fills", d, Row{int32(1), int32(2), int32(3)})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	want := []byte{
		0x01,                   // format version
		0x00, 0x00, 0x00, 0x01, // id 2 ("a") = 1
		0x00, 0x00, 0x00, 0x02, // id 5 ("b") = 2
		0x00, 0x00, 0x00, 0x03, // id 9 ("c") = 3
	}
	if !slices.Equal(got, want) {
		t.Errorf("payload bytes:\n got %x\nwant %x", got, want)
	}
}

// TestEncodeRefusesValuesItCannotWrite covers every way a caller of this
// package can hand it something it must not silently coerce. Each one is a
// programming error inside the library rather than a caller's bad data, and
// each must be a typed refusal so the write path stops instead of staging bytes
// no decoder agrees with.
func TestEncodeRefusesValuesItCannotWrite(t *testing.T) {
	d := goldenDescriptor(t)

	rowWith := func(i int, v any) Row {
		out := slices.Clone(goldenRow1)
		out[i] = v
		return out
	}

	cases := []struct {
		name        string
		row         Row
		wantFieldID int
	}{
		{"too few values", goldenRow1[:len(goldenRow1)-1], 0},
		{"too many values", append(slices.Clone(goldenRow1), int64(0)), 0},
		{"null in a required column", rowWith(0, nil), 1},
		{"int64 in a boolean column", rowWith(0, int64(1)), 1},
		{"int64 in an int column", rowWith(1, int64(-2)), 2},
		{"int32 in a long column", rowWith(2, int32(7)), 3},
		{"float64 in a float column", rowWith(3, float64(1.5)), 4},
		{"float32 in a double column", rowWith(4, float32(1.5)), 5},
		{"string in a decimal column", rowWith(5, "1.234567890"), 6},
		{"time-shaped string in a timestamp column", rowWith(6, "2023-11-14T22:13:20Z"), 7},
		{"[]byte in a string column", rowWith(7, []byte("btc_usd")), 8},
		{"string in a binary column", rowWith(8, "raw"), 9},
		{"int64 in an optional string column", rowWith(9, int64(1)), 10},
		{"string in an optional long column", rowWith(10, "42"), 11},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := Encode("fills", d, c.row)
			if err == nil {
				t.Fatalf("Encode succeeded, producing %x; want a refusal", out)
			}

			var ee errdef.EncodingError
			if !errors.As(err, &ee) {
				t.Fatalf("error is %T, want errdef.EncodingError", err)
			}
			if ee.Kind != errdef.EncodingKindValue {
				t.Errorf("EncodingError.Kind = %q, want %q", ee.Kind, errdef.EncodingKindValue)
			}
			if ee.Table != "fills" {
				t.Errorf("EncodingError.Table = %q, want %q", ee.Table, "fills")
			}
			if ee.FieldID != c.wantFieldID {
				t.Errorf("EncodingError.FieldID = %d, want %d", ee.FieldID, c.wantFieldID)
			}
		})
	}
}

// TestDecodeRefusesMalformedPayloads covers the ways a stored payload can be
// wrong. Every one of these arrives from a SQLite file that may have been
// written by a different build, so each must be a typed refusal rather than a
// half-read row that silently reports the wrong values.
func TestDecodeRefusesMalformedPayloads(t *testing.T) {
	d := goldenDescriptor(t)

	withByte := func(i int, b byte) []byte {
		out := slices.Clone(goldenPayload1)
		out[i] = b
		return out
	}

	cases := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"unknown format version", withByte(0, 0x02)},
		{"truncated before the last column", goldenPayload1[:len(goldenPayload1)-1]},
		{"truncated mid-string", goldenPayload1[:46]},
		// Two stopping points the cases around them step over, and both are
		// stopping points where a missing refusal is silent rather than loud. A
		// payload that ends exactly where an optional column's presence byte
		// would be reads, to a decoder that did not check, as that column being
		// absent — so the row decodes, with a null nobody wrote. Byte 60 is the
		// first such boundary: the "note" column's presence byte, at the offset
		// derived in the case below.
		{"truncated exactly at an optional column's presence byte", goldenPayload1[:60]},
		// Bytes 42 to 45 are the "symbol" column's four-byte length prefix (1
		// version + 1 boolean + 4 + 8 + 4 + 8 + 8 + 8 fixed-width). A payload
		// that stops inside it reads, to a decoder that did not check, as a
		// length of zero — an empty string in place of a value, and every
		// column after it shifted.
		{"truncated inside a string column's length prefix", goldenPayload1[:44]},
		{"trailing bytes", append(slices.Clone(goldenPayload1), 0x00)},
		// Byte 60 is the optional "note" column's presence byte: 1 version + 1
		// boolean + 4 + 8 + 4 + 8 + 8 + 8 fixed-width + (4+7) symbol + (4+3)
		// payload.
		{"presence byte that is neither present nor absent", withByte(60, 0x02)},
		// Byte 1 is the required boolean column's value.
		{"boolean that is neither true nor false", withByte(1, 0x02)},
		// Bytes 53 to 56 are the "payload" column's four-byte length prefix;
		// corrupting one of them asks for far more bytes than remain, which
		// must be refused rather than allocated for.
		{"length prefix longer than the payload", withByte(55, 0xff)},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row, err := Decode("fills", d, c.in)
			if err == nil {
				t.Fatalf("Decode succeeded, producing %v; want a refusal", row)
			}

			var ee errdef.EncodingError
			if !errors.As(err, &ee) {
				t.Fatalf("error is %T, want errdef.EncodingError", err)
			}
			if ee.Kind != errdef.EncodingKindFormat {
				t.Errorf("EncodingError.Kind = %q, want %q", ee.Kind, errdef.EncodingKindFormat)
			}
		})
	}
}

// TestDecodeReadsGoldenPayloadsBackIntoTheirValues is the decoder's half of the
// golden fixtures: it starts from the hand-written bytes rather than from
// something the encoder just produced, so it proves the two agree on the format
// as specified and not merely with each other.
func TestDecodeReadsGoldenPayloadsBackIntoTheirValues(t *testing.T) {
	d := goldenDescriptor(t)

	cases := []struct {
		name    string
		payload []byte
		want    Row
	}{
		{"record 1", goldenPayload1, goldenRow1},
		{"record 2", goldenPayload2, goldenRow2},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Decode("fills", d, c.payload)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			assertRowsEqual(t, got, c.want)
		})
	}
}

// TestEncodeDecodeRoundTrip is the encoder and decoder's joint correctness
// contract: decode of encode returns the identical values. The value set is
// deliberately made of the boundaries — both int extremes, both float
// infinities, negative zero, the empty and the null of each nullable column —
// because a round trip over comfortable values proves very little.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	d := goldenDescriptor(t)

	note := "a note"
	extra := int64(-1)

	cases := []struct {
		name string
		row  Row
	}{
		{"the first golden record", goldenRow1},
		{"the second golden record", goldenRow2},
		{"integer extremes", Row{
			true, int32(math.MaxInt32), int64(math.MaxInt64), float32(1), float64(1),
			int64(999999999999999999), int64(math.MaxInt64), "x", []byte{0xff}, note, extra,
		}},
		{"integer extremes, negative", Row{
			false, int32(math.MinInt32), int64(math.MinInt64), float32(-1), float64(-1),
			int64(-999999999999999999), int64(math.MinInt64), "y", []byte{0x00}, note, extra,
		}},
		{"float boundaries", Row{
			true, int32(0), int64(0),
			float32(math.Inf(-1)), math.Inf(1),
			int64(0), int64(0), "z", []byte{0x01}, note, extra,
		}},
		{"negative zero", Row{
			true, int32(0), int64(0),
			float32(math.Float32frombits(0x80000000)), math.Float64frombits(0x8000000000000000),
			int64(0), int64(0), "", []byte{}, note, extra,
		}},
		// The smallest positive subnormal of each width: one in the mantissa
		// and nothing else. A conversion through any wider or narrower type on
		// the way to the wire would flush these to zero.
		{"the smallest subnormals", Row{
			true, int32(0), int64(0),
			float32(math.Float32frombits(0x00000001)), math.Float64frombits(0x0000000000000001),
			int64(0), int64(0), "", []byte{}, note, extra,
		}},
		// A NaN with a payload, not the canonical quiet NaN. The encoding
		// copies float bits rather than comparing values, so the specific
		// payload bits must survive; asserting on == would pass for any NaN at
		// all, including one whose payload had been rewritten.
		{"a NaN carrying specific payload bits", Row{
			true, int32(0), int64(0),
			float32(math.Float32frombits(0x7fc00001)), math.Float64frombits(0x7ff8000000000001),
			int64(0), int64(0), "", []byte{}, note, extra,
		}},
		{"every nullable column null", Row{
			true, int32(0), int64(0), float32(0), float64(0),
			int64(0), int64(0), "", []byte{}, nil, nil,
		}},
		{"multi-byte UTF-8 and high bytes", Row{
			true, int32(1), int64(1), float32(1), float64(1),
			int64(1), int64(1), "é中\U0001f9ca", []byte{0x00, 0xff, 0x7f, 0x80}, "é", extra,
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload, err := Encode("fills", d, c.row)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			got, err := Decode("fills", d, payload)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			assertRowsEqual(t, got, c.row)

			// Re-encoding what came back must reproduce the identical bytes.
			// The batch key is a hash of these bytes, and replay re-derives it
			// from rows that have been through exactly this cycle.
			again, err := Encode("fills", d, got)
			if err != nil {
				t.Fatalf("re-Encode: %v", err)
			}
			if !slices.Equal(again, payload) {
				t.Errorf("re-encoding a decoded row changed the bytes:\n got %x\nwant %x", again, payload)
			}
		})
	}
}

// TestDecodedBinaryDoesNotAliasThePayload proves a decoded []byte is a copy.
// Replay decodes out of a buffer the staging layer is free to reuse, so a
// decoded row that pointed back into it would change under its owner.
func TestDecodedBinaryDoesNotAliasThePayload(t *testing.T) {
	d := goldenDescriptor(t)

	payload := slices.Clone(goldenPayload1)
	row, err := Decode("fills", d, payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	decoded, ok := row[8].([]byte)
	if !ok {
		t.Fatalf("binary column decoded to %T, want []byte", row[8])
	}
	before := slices.Clone(decoded)

	for i := range payload {
		payload[i] = 0xee
	}
	if !slices.Equal(decoded, before) {
		t.Errorf("decoded binary value changed with the payload buffer: %x, want %x", decoded, before)
	}
}

// assertRowsEqual compares two rows value by value, including dynamic type. A
// float comparison here is deliberately on bits rather than on == so that NaN,
// negative zero and infinity are all compared as the exact values they are.
func assertRowsEqual(t *testing.T, got, want Row) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("row has %d values, want %d", len(got), len(want))
	}

	for i := range want {
		g, w := got[i], want[i]

		switch wv := w.(type) {
		case nil:
			if g != nil {
				t.Errorf("value %d = %#v, want null", i, g)
			}
		case float32:
			gv, ok := g.(float32)
			if !ok {
				t.Errorf("value %d has type %T, want float32", i, g)
				continue
			}
			if math.Float32bits(gv) != math.Float32bits(wv) {
				t.Errorf("value %d = %v (bits %#08x), want %v (bits %#08x)", i, gv, math.Float32bits(gv), wv, math.Float32bits(wv))
			}
		case float64:
			gv, ok := g.(float64)
			if !ok {
				t.Errorf("value %d has type %T, want float64", i, g)
				continue
			}
			if math.Float64bits(gv) != math.Float64bits(wv) {
				t.Errorf("value %d = %v (bits %#016x), want %v (bits %#016x)", i, gv, math.Float64bits(gv), wv, math.Float64bits(wv))
			}
		case []byte:
			gv, ok := g.([]byte)
			if !ok {
				t.Errorf("value %d has type %T, want []byte", i, g)
				continue
			}
			if gv == nil {
				t.Errorf("value %d decoded to a nil []byte; an empty binary value must come back non-nil", i)
			}
			if !slices.Equal(gv, wv) {
				t.Errorf("value %d = %x, want %x", i, gv, wv)
			}
		default:
			if g != w {
				t.Errorf("value %d = %#v (%T), want %#v (%T)", i, g, g, w, w)
			}
		}
	}
}
