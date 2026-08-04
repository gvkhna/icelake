package canon

import (
	"errors"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/schemamap"
)

// The golden tests in this package are the first sanctioned unit-test category
// from TESTING.md — a calculation whose exact output matters — and this
// milestone is the reason that exception exists. Every expected byte string
// below is written out from the format specification in this package's doc
// comment — region by region, with the derivation in the comment beside it —
// and never captured from running the encoder. Every expected hash was computed
// outside Go, by feeding the same hand-assembled byte string to sha256sum. An
// expectation captured from the code under test would assert nothing at all,
// and these particular bytes are durable: a payload staged today is decoded by
// a build that does not exist yet, and an object key computed today must be
// recomputed identically after a crash.
//
// Two more tests here are that same first category even though they carry no
// byte string, and saying so is the point of this paragraph: an earlier version
// of this header swept every non-golden test into the second category with the
// words "every one of them", which was wrong about three of them and is
// corrected here at the audit's fourth round (2026-08-01).
// TestDescribeSortsColumnsIntoFieldIDOrder asserts an exact field list, and
// ascending field-id order is part of the format specification rather than a
// property of this implementation — the expected slice is written from that
// specification, not read back from Describe. TestFingerprintSurvivesSerialize
// AndParse compares the golden descriptor's fingerprint against itself across a
// round trip, so the value it holds is the golden one the fixture above already
// pins.
//
// TestFingerprintDistinguishesEveryMaterialDifference sits in the second
// category and holds its *first* claim shape — a seam whose caller-facing half
// is proven end to end and is only enumerated more cheaply here. It is the same
// shape, and the same argument, as batch_test.go's TestBatchHashDependsOn
// EveryComponentOfItsInput, and the two headers said opposite things about it
// until this round. The caller-facing half is real: a table whose declared
// shape has moved refuses its staged rows with a typed StagingSchemaError
// through the public API, and the reconciliation tests drive a live table
// across an added column, a rename and a widening and read every pre-existing
// row back. What the substrate cannot arrange is the enumeration — a field
// added, removed, renamed, retyped, its precision or scale or nullability
// changed, one at a time, with no container to start — because end to end a
// fingerprint that ignored one of those surfaces only as old rows silently
// decoded under a new shape, which is a wrong answer rather than an error.
//
// The remaining four are the second category's *second* claim shape: a defence
// this layer keeps against its own in-process callers, which the public path is
// built to make unreachable. TestParseDescriptorRefusesMalformedBytes
// reads stored bytes that icelake never writes — they arrive only from a
// damaged staging file or from a build that recorded a shape this one cannot
// read — and the invariant is that a shape which cannot be read whole is
// refused rather than half-read, because a half-read descriptor silently
// misreads every payload recorded under it. TestDescribeRefusesUnsupported
// ColumnTypes covers types Declare has already rejected one layer up, so no
// caller reaches it; it is here because this package must not start describing
// a column it cannot encode on the day another caller does. TestBytesIsACopy
// and TestIndexOfFieldID defend the two properties everything downstream leans
// on — that a descriptor cannot be mutated through what it hands out, since its
// fingerprint was computed on the assumption that it cannot, and that columns
// are found by permanent field id rather than by position. Lose either and a
// batch's stored key stops naming its own bytes.

// goldenRecord is the declaration struct the golden fixtures are built from. It
// carries every column type SCHEMA.md's declaration path can produce — verified
// by enumeration, not assumed — in both a required and (for two of them) an
// optional form, so the fixtures below cover the whole encodable surface rather
// than a convenient corner of it.
type goldenRecord struct {
	Flag    bool    `parquet:"name=flag, fieldid=1" arrow:"flag"`
	Count   int32   `parquet:"name=count, fieldid=2" arrow:"count"`
	Seq     int64   `parquet:"name=seq, fieldid=3" arrow:"seq"`
	Ratio   float32 `parquet:"name=ratio, fieldid=4" arrow:"ratio"`
	Weight  float64 `parquet:"name=weight, fieldid=5" arrow:"weight"`
	Price   int64   `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=6" arrow:"price"`
	At      int64   `parquet:"name=at, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=7" arrow:"at"`
	Symbol  string  `parquet:"name=symbol, logical=String, fieldid=8" arrow:"symbol"`
	Payload []byte  `parquet:"name=payload, type=BYTE_ARRAY, fieldid=9" arrow:"payload"`
	Note    *string `parquet:"name=note, logical=String, fieldid=10" arrow:"note"`
	Extra   *int64  `parquet:"name=extra, fieldid=11" arrow:"extra"`
}

// goldenDescriptorBytes is the serialized descriptor for goldenRecord, written
// out from the layout in this package's doc comment.
//
//	1 byte   format version
//	4 bytes  big-endian column count
//	per column, ascending field id:
//	  4 bytes  big-endian field id
//	  1 byte   type tag
//	  1 byte   flags (bit 0 = required)
//	  2 bytes  decimal precision and scale, decimal columns only
//	  4 bytes  big-endian name length, then the name
//
// The name bytes are plain ASCII and are spelled out in hex rather than as Go
// string literals, so that reading this fixture never requires trusting a
// second layer to have produced the bytes the comment claims.
var goldenDescriptorBytes = []byte{
	0x01,                   // descriptor format version 1
	0x00, 0x00, 0x00, 0x0b, // 11 columns

	// id 1, boolean (0x01), required (0x01), name "flag" (4 bytes)
	0x00, 0x00, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x04, 0x66, 0x6c, 0x61, 0x67,
	// id 2, int (0x02), required, name "count" (5 bytes)
	0x00, 0x00, 0x00, 0x02, 0x02, 0x01, 0x00, 0x00, 0x00, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74,
	// id 3, long (0x03), required, name "seq" (3 bytes)
	0x00, 0x00, 0x00, 0x03, 0x03, 0x01, 0x00, 0x00, 0x00, 0x03, 0x73, 0x65, 0x71,
	// id 4, float (0x04), required, name "ratio" (5 bytes)
	0x00, 0x00, 0x00, 0x04, 0x04, 0x01, 0x00, 0x00, 0x00, 0x05, 0x72, 0x61, 0x74, 0x69, 0x6f,
	// id 5, double (0x05), required, name "weight" (6 bytes)
	0x00, 0x00, 0x00, 0x05, 0x05, 0x01, 0x00, 0x00, 0x00, 0x06, 0x77, 0x65, 0x69, 0x67, 0x68, 0x74,
	// id 6, decimal (0x06), required, precision 18 (0x12), scale 9 (0x09), name "price" (5 bytes)
	0x00, 0x00, 0x00, 0x06, 0x06, 0x01, 0x12, 0x09, 0x00, 0x00, 0x00, 0x05, 0x70, 0x72, 0x69, 0x63, 0x65,
	// id 7, timestamptz (0x07), required, name "at" (2 bytes)
	0x00, 0x00, 0x00, 0x07, 0x07, 0x01, 0x00, 0x00, 0x00, 0x02, 0x61, 0x74,
	// id 8, string (0x08), required, name "symbol" (6 bytes)
	0x00, 0x00, 0x00, 0x08, 0x08, 0x01, 0x00, 0x00, 0x00, 0x06, 0x73, 0x79, 0x6d, 0x62, 0x6f, 0x6c,
	// id 9, binary (0x09), required, name "payload" (7 bytes)
	0x00, 0x00, 0x00, 0x09, 0x09, 0x01, 0x00, 0x00, 0x00, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64,
	// id 10, string (0x08), optional (0x00), name "note" (4 bytes)
	0x00, 0x00, 0x00, 0x0a, 0x08, 0x00, 0x00, 0x00, 0x00, 0x04, 0x6e, 0x6f, 0x74, 0x65,
	// id 11, long (0x03), optional (0x00), name "extra" (5 bytes)
	0x00, 0x00, 0x00, 0x0b, 0x03, 0x00, 0x00, 0x00, 0x00, 0x05, 0x65, 0x78, 0x74, 0x72, 0x61,
}

// goldenFingerprint is sha256("icelake/schema-fp/v1\n" || goldenDescriptorBytes),
// computed by feeding that exact 190-byte string (21 bytes of domain tag plus
// the 169-byte descriptor above) to sha256sum outside this program.
const goldenFingerprint = "762caa7ad438925ed3182596015bd2cfe68ec569c74a71606a9f24752888f9c3"

// goldenFields is the column list the descriptor above describes, stated
// independently of it so a change to either has to be a deliberate change to
// both.
var goldenFields = []Field{
	{ID: 1, Name: "flag", Kind: KindBoolean, Required: true},
	{ID: 2, Name: "count", Kind: KindInt, Required: true},
	{ID: 3, Name: "seq", Kind: KindLong, Required: true},
	{ID: 4, Name: "ratio", Kind: KindFloat, Required: true},
	{ID: 5, Name: "weight", Kind: KindDouble, Required: true},
	{ID: 6, Name: "price", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
	{ID: 7, Name: "at", Kind: KindTimestampTz, Required: true},
	{ID: 8, Name: "symbol", Kind: KindString, Required: true},
	{ID: 9, Name: "payload", Kind: KindBinary, Required: true},
	{ID: 10, Name: "note", Kind: KindString},
	{ID: 11, Name: "extra", Kind: KindLong},
}

// goldenDescriptor derives the descriptor the way the write path does — through
// the real declaration chain in schemamap, from the real struct — rather than
// by hand-building the field list. That is what makes the golden bytes a test
// of the seam between the two packages and not only of this one's serializer.
func goldenDescriptor(t *testing.T) *Descriptor {
	t.Helper()

	decl, err := schemamap.Declare[goldenRecord]("market", "fills")
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}
	d, err := Describe(decl.Table(), decl.IcebergSchema())
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	return d
}

// TestDescribeProducesGoldenDescriptorBytes pins the descriptor a real
// declaration derives to, byte for byte. These bytes live in
// staging_schemas.descriptor and are read back by builds that no longer contain
// the struct that produced them, so a silent change to them is a silent change
// to how every previously staged row is interpreted.
func TestDescribeProducesGoldenDescriptorBytes(t *testing.T) {
	d := goldenDescriptor(t)

	if got := d.Bytes(); !slices.Equal(got, goldenDescriptorBytes) {
		t.Errorf("descriptor bytes:\n got %x\nwant %x", got, goldenDescriptorBytes)
	}
	if got := d.Fields(); !slices.Equal(got, goldenFields) {
		t.Errorf("descriptor fields = %+v, want %+v", got, goldenFields)
	}
	if got, want := len(d.Bytes()), 169; got != want {
		t.Errorf("descriptor length = %d bytes, want %d", got, want)
	}
}

// TestDescriptorFingerprintIsGolden pins schema_fp for the golden shape. The
// fingerprint is the primary key of staging_schemas and the value every staged
// row records, so a drift here orphans every descriptor already on disk.
func TestDescriptorFingerprintIsGolden(t *testing.T) {
	if got := goldenDescriptor(t).Fingerprint(); got != goldenFingerprint {
		t.Errorf("Fingerprint() = %s, want %s", got, goldenFingerprint)
	}
}

// TestBytesIsACopy proves a Descriptor cannot be mutated through the bytes it
// hands out. It is immutable by construction, and the fingerprint it already
// computed depends on that staying true.
func TestBytesIsACopy(t *testing.T) {
	d := goldenDescriptor(t)

	b := d.Bytes()
	b[0] = 0xff
	if got := d.Bytes(); got[0] != 0x01 {
		t.Errorf("mutating the result of Bytes() changed the descriptor: version byte is now %#02x", got[0])
	}

	fields := d.Fields()
	fields[0].Name = "clobbered"
	if got := d.Fields(); got[0].Name != "flag" {
		t.Errorf("mutating the result of Fields() changed the descriptor: column 0 is now %q", got[0].Name)
	}
}

// TestParseDescriptorRoundTripsGoldenBytes proves the stored form is enough on
// its own: parsing the golden bytes with no struct, no declaration and no
// schema anywhere in sight rebuilds the identical shape, the identical bytes
// and the identical fingerprint. That is the whole promise staging_schemas
// makes to a replay after a schema-changing deploy.
func TestParseDescriptorRoundTripsGoldenBytes(t *testing.T) {
	d, err := ParseDescriptor(goldenDescriptorBytes)
	if err != nil {
		t.Fatalf("ParseDescriptor: %v", err)
	}

	if got := d.Fields(); !slices.Equal(got, goldenFields) {
		t.Errorf("parsed fields = %+v, want %+v", got, goldenFields)
	}
	if got := d.Bytes(); !slices.Equal(got, goldenDescriptorBytes) {
		t.Errorf("re-serialized bytes:\n got %x\nwant %x", got, goldenDescriptorBytes)
	}
	if got := d.Fingerprint(); got != goldenFingerprint {
		t.Errorf("fingerprint after parse = %s, want %s", got, goldenFingerprint)
	}
}

// TestParseDescriptorRefusesMalformedBytes covers every way the stored form can
// be wrong. All of these arrive from a file on disk written by some other
// build, so each one must be a typed refusal rather than a half-read shape that
// then silently misreads every payload recorded under it.
func TestParseDescriptorRefusesMalformedBytes(t *testing.T) {
	// A minimal valid descriptor: one required long column named "a".
	valid := []byte{
		0x01,                   // version
		0x00, 0x00, 0x00, 0x01, // 1 column
		0x00, 0x00, 0x00, 0x01, // id 1
		0x03,                   // long
		0x01,                   // required
		0x00, 0x00, 0x00, 0x01, // name length 1
		0x61, // "a"
	}
	if _, err := ParseDescriptor(valid); err != nil {
		t.Fatalf("the control case must parse, or the cases below prove nothing: %v", err)
	}

	withByte := func(i int, b byte) []byte {
		out := slices.Clone(valid)
		out[i] = b
		return out
	}

	cases := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		// A well-formed descriptor of nothing. Left accepted it would describe
		// a shape whose payloads are a lone version byte and whose batches
		// would still mint a real object key, and Describe refuses the same
		// shape on the way in — so accepting it back would be an asymmetry
		// between the two constructors, not a tolerance.
		{"no columns", []byte{0x01, 0x00, 0x00, 0x00, 0x00}},
		{"unknown format version", withByte(0, 0x02)},
		{"unknown type tag", withByte(9, 0x7f)},
		{"reserved flag bit set", withByte(10, 0x03)},
		{"field id zero", []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x01, 0x61}},
		{"truncated mid-column", valid[:10]},
		{"truncated mid-name", valid[:len(valid)-1]},
		{"trailing bytes", append(slices.Clone(valid), 0x00)},
		{"column count larger than the bytes that follow", withByte(4, 0xff)},
		{"empty column name", []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00}},
		{"decimal precision above the int64 ceiling", []byte{
			0x01, 0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x27, 0x09, 0x00, 0x00, 0x00, 0x01, 0x61,
		}},
		{"decimal scale above its precision", []byte{
			0x01, 0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x02, 0x09, 0x00, 0x00, 0x00, 0x01, 0x61,
		}},
		// Non-canonical: the same shape, but with the columns written in
		// descending field-id order. Exactly one byte string may describe a
		// shape, or the fingerprint would not be a function of the shape.
		{"columns not in ascending field-id order", []byte{
			0x01, 0x00, 0x00, 0x00, 0x02,
			0x00, 0x00, 0x00, 0x02, 0x03, 0x01, 0x00, 0x00, 0x00, 0x01, 0x62,
			0x00, 0x00, 0x00, 0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x01, 0x61,
		}},
		{"the same field id twice", []byte{
			0x01, 0x00, 0x00, 0x00, 0x02,
			0x00, 0x00, 0x00, 0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x01, 0x61,
			0x00, 0x00, 0x00, 0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x01, 0x62,
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := ParseDescriptor(c.in)
			if err == nil {
				t.Fatalf("ParseDescriptor(%x) succeeded with %d columns, want a refusal", c.in, d.Len())
			}
			var ee errdef.EncodingError
			if !errors.As(err, &ee) {
				t.Fatalf("error is %T, want errdef.EncodingError", err)
			}
		})
	}
}

// TestDescribeRefusesUnsupportedColumnTypes proves the type whitelist is closed
// and loud. Every type below is a real Iceberg type that no declaration can
// currently produce; giving one a speculative encoding would mean shipping a
// durable format for a shape nothing can write, and silently accepting one
// would mean staging bytes no decoder agrees on.
func TestDescribeRefusesUnsupportedColumnTypes(t *testing.T) {
	cases := []struct {
		name string
		typ  iceberg.Type
	}{
		{"date", iceberg.PrimitiveTypes.Date},
		{"time", iceberg.PrimitiveTypes.Time},
		{"timestamp without a zone", iceberg.PrimitiveTypes.Timestamp},
		{"uuid", iceberg.PrimitiveTypes.UUID},
		{"fixed", iceberg.FixedTypeOf(16)},
		{"list", &iceberg.ListType{ElementID: 2, Element: iceberg.PrimitiveTypes.String, ElementRequired: true}},
		{"struct", &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "inner", Type: iceberg.PrimitiveTypes.String, Required: true},
		}}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "col", Type: c.typ, Required: true})

			d, err := Describe("fills", sc)
			if err == nil {
				t.Fatalf("Describe accepted a %s column, producing %d columns; want a refusal", c.name, d.Len())
			}

			var ee errdef.EncodingError
			if !errors.As(err, &ee) {
				t.Fatalf("error is %T, want errdef.EncodingError", err)
			}
			if ee.Kind != errdef.EncodingKindUnsupportedType {
				t.Errorf("EncodingError.Kind = %q, want %q", ee.Kind, errdef.EncodingKindUnsupportedType)
			}
			if ee.Table != "fills" || ee.Field != "col" || ee.FieldID != 1 {
				t.Errorf("error identifies table %q field %q id %d, want %q/%q/1", ee.Table, ee.Field, ee.FieldID, "fills", "col")
			}
		})
	}
}

// TestDescribeSortsColumnsIntoFieldIDOrder proves ascending field-id order is
// imposed rather than inherited from declaration order. It matters because a
// declaration whose struct fields have been reordered — which is legal, since
// identity is the id — must still produce the same fingerprint and the same
// payload layout as before the reordering.
func TestDescribeSortsColumnsIntoFieldIDOrder(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 9, Name: "c", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 5, Name: "b", Type: iceberg.PrimitiveTypes.Binary},
	)

	d, err := Describe("fills", sc)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}

	want := []Field{
		{ID: 2, Name: "a", Kind: KindLong, Required: true},
		{ID: 5, Name: "b", Kind: KindBinary},
		{ID: 9, Name: "c", Kind: KindString, Required: true},
	}
	if got := d.Fields(); !slices.Equal(got, want) {
		t.Errorf("Fields() = %+v, want %+v", got, want)
	}
}

// A duplicate field id is deliberately not covered here, and the omission is
// the finding rather than a gap: iceberg-go's own schema constructor panics on
// a schema whose fields share an id, so the input this package would have to be
// handed cannot be built. Describe still refuses it as a second line; there is
// simply no way to reach that branch to assert on it, and a test that
// constructed the descriptor by hand to reach it would be testing this
// package's own internals rather than a claim anyone makes. The stored-bytes
// route into the same mistake is covered above, under "the same field id
// twice".

// TestFingerprintDistinguishesEveryMaterialDifference is the fingerprint's
// actual contract: equal shapes give equal fingerprints, and every difference
// that changes what a payload means gives a different one. Each variant below
// differs from the base in exactly one way, so a fingerprint that ignored any
// one of them would be caught here and nowhere else — and ignoring one means
// decoding an old row under a new shape without ever noticing.
func TestFingerprintDistinguishesEveryMaterialDifference(t *testing.T) {
	base := []Field{
		{ID: 1, Name: "a", Kind: KindLong, Required: true},
		{ID: 2, Name: "b", Kind: KindString},
		{ID: 3, Name: "c", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
	}

	variants := map[string][]Field{
		"field added": append(slices.Clone(base), Field{ID: 4, Name: "d", Kind: KindBinary}),
		"field removed": {
			{ID: 1, Name: "a", Kind: KindLong, Required: true},
			{ID: 2, Name: "b", Kind: KindString},
		},
		"field renamed": {
			{ID: 1, Name: "a", Kind: KindLong, Required: true},
			{ID: 2, Name: "renamed", Kind: KindString},
			{ID: 3, Name: "c", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
		},
		"field retyped": {
			{ID: 1, Name: "a", Kind: KindInt, Required: true},
			{ID: 2, Name: "b", Kind: KindString},
			{ID: 3, Name: "c", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
		},
		"nullability changed": {
			{ID: 1, Name: "a", Kind: KindLong},
			{ID: 2, Name: "b", Kind: KindString},
			{ID: 3, Name: "c", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
		},
		"field id changed": {
			{ID: 1, Name: "a", Kind: KindLong, Required: true},
			{ID: 2, Name: "b", Kind: KindString},
			{ID: 4, Name: "c", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
		},
		"decimal precision changed": {
			{ID: 1, Name: "a", Kind: KindLong, Required: true},
			{ID: 2, Name: "b", Kind: KindString},
			{ID: 3, Name: "c", Kind: KindDecimal, Precision: 17, Scale: 9, Required: true},
		},
		"decimal scale changed": {
			{ID: 1, Name: "a", Kind: KindLong, Required: true},
			{ID: 2, Name: "b", Kind: KindString},
			{ID: 3, Name: "c", Kind: KindDecimal, Precision: 18, Scale: 8, Required: true},
		},
	}

	fp := func(fields []Field) string {
		t.Helper()
		d, err := newDescriptor("fills", fields)
		if err != nil {
			t.Fatalf("newDescriptor: %v", err)
		}
		return d.Fingerprint()
	}

	baseFP := fp(base)

	// The same shape, built a second time from an independent field list, must
	// fingerprint identically — otherwise nothing could ever match.
	if got := fp(slices.Clone(base)); got != baseFP {
		t.Errorf("the same shape fingerprinted twice gave %s then %s", baseFP, got)
	}

	seen := map[string]string{baseFP: "base"}
	for name, fields := range variants {
		got := fp(fields)
		if other, dup := seen[got]; dup {
			t.Errorf("%q fingerprints identically to %q (%s); a material difference must change schema_fp", name, other, got)
			continue
		}
		seen[got] = name
	}
}

// TestFingerprintSurvivesSerializeAndParse proves the fingerprint is a property
// of the recorded bytes, not of the in-memory object that happened to produce
// them. Replay computes it from a descriptor read out of SQLite, which is the
// only place it is ever recomputed.
func TestFingerprintSurvivesSerializeAndParse(t *testing.T) {
	original := goldenDescriptor(t)

	reparsed, err := ParseDescriptor(original.Bytes())
	if err != nil {
		t.Fatalf("ParseDescriptor: %v", err)
	}
	if got, want := reparsed.Fingerprint(), original.Fingerprint(); got != want {
		t.Errorf("fingerprint after a serialize/parse round trip = %s, want %s", got, want)
	}
}

// TestIndexOfFieldID covers the lookup the replay path maps by. It is a
// one-line function, but "which position holds field id 10" is the question
// every cross-shape decision in this package asks, and answering it by position
// instead of by id is precisely the bug the whole fingerprint design exists to
// prevent.
func TestIndexOfFieldID(t *testing.T) {
	d := goldenDescriptor(t)

	if got, ok := d.IndexOfFieldID(10); !ok || got != 9 {
		t.Errorf("IndexOfFieldID(10) = %d, %t; want 9, true", got, ok)
	}
	if _, ok := d.IndexOfFieldID(12); ok {
		t.Error("IndexOfFieldID(12) reported a column, want none")
	}
}
