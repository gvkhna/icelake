package canon

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"slices"

	"github.com/apache/iceberg-go"

	"github.com/gvkhna/icelake/internal/errdef"
)

// Kind is the canonical type tag of one column: the identity the encoding, the
// descriptor and the fingerprint all agree on.
//
// The numbers are part of a durable on-disk format and are permanent. A new
// type takes the next unused number; a number is never reused for a different
// type, and never renumbered, for exactly the reason a permanent field ID is
// never reused — old bytes outlive the code that wrote them.
//
// The set is closed on purpose, and it is not an arbitrary subset: it is
// exactly the Iceberg primitive types a caller's declaration struct can
// produce, given the closed Go field-type whitelist schemamap's Declare
// enforces. Anything outside it — date, time, uuid, fixed, a timestamp with no
// zone, or any nested type — cannot be declared today, and is refused loudly
// here rather than being given a speculative encoding no declaration can reach.
//
// The Go types named below are the ones that reach each tag through that
// whitelist. They are the *only* ones: a plain int would derive to long on a
// 64-bit build and int on a 32-bit one, which would put the build's word size
// inside a durable descriptor, so Declare refuses it rather than letting either
// tag be reachable two ways.
type Kind uint8

const (
	// KindBoolean is Iceberg boolean, declared as a Go bool.
	KindBoolean Kind = 0x01
	// KindInt is Iceberg int, declared as a Go int32 and only as an int32.
	KindInt Kind = 0x02
	// KindLong is Iceberg long, declared as a Go int64 and only as an int64.
	KindLong Kind = 0x03
	// KindFloat is Iceberg float, declared as a Go float32.
	KindFloat Kind = 0x04
	// KindDouble is Iceberg double, declared as a Go float64.
	KindDouble Kind = 0x05
	// KindDecimal is Iceberg decimal(P, S), declared as a Go int64 holding the
	// unscaled value. Precision is capped at 18 because that is what an int64
	// unscaled value can hold, which is the same ceiling the declaration path
	// already imposes.
	KindDecimal Kind = 0x06
	// KindTimestampTz is Iceberg timestamptz, declared as a Go int64 holding
	// epoch milliseconds. The stored Iceberg column is microseconds and the
	// write path upcasts on the way out; the canonical encoding keeps the
	// caller's own int64 untouched.
	KindTimestampTz Kind = 0x07
	// KindString is Iceberg string, declared as a Go string.
	KindString Kind = 0x08
	// KindBinary is Iceberg binary, declared as a Go []byte.
	KindBinary Kind = 0x09
)

// String returns the Iceberg spelling of the type, for error messages.
func (k Kind) String() string {
	switch k {
	case KindBoolean:
		return "boolean"
	case KindInt:
		return "int"
	case KindLong:
		return "long"
	case KindFloat:
		return "float"
	case KindDouble:
		return "double"
	case KindDecimal:
		return "decimal"
	case KindTimestampTz:
		return "timestamptz"
	case KindString:
		return "string"
	case KindBinary:
		return "binary"
	default:
		return fmt.Sprintf("unknown type tag %#02x", uint8(k))
	}
}

// fixedWidth returns the number of value bytes a fixed-width kind occupies, and
// false for the two length-prefixed kinds.
func (k Kind) fixedWidth() (int, bool) {
	switch k {
	case KindBoolean:
		return 1, true
	case KindInt, KindFloat:
		return 4, true
	case KindLong, KindDouble, KindDecimal, KindTimestampTz:
		return 8, true
	default:
		return 0, false
	}
}

// Field is one column of a recorded schema shape: its permanent id, its name,
// its canonical type, and whether it is required. These four things are exactly
// what is needed to decode a payload written under that shape without the Go
// struct that wrote it.
type Field struct {
	// ID is the permanent field ID. Identity everywhere in icelake is this
	// number, never a name and never a position.
	ID int
	// Name is the column name.
	Name string
	// Kind is the canonical type.
	Kind Kind
	// Precision and Scale are set only for [KindDecimal] and are zero
	// otherwise.
	Precision int
	Scale     int
	// Required is true for a column that can never be null. An optional column
	// carries a presence byte in the payload; a required one does not, which
	// makes "a null in a required column" unrepresentable rather than merely
	// forbidden.
	Required bool
}

// maximum decimal precision an int64 unscaled value can hold, which is the same
// ceiling arrow-go's own applicability check imposes on an INT64-backed decimal.
const maxDecimalPrecision = 18

// The two durable format versions. They are separate numbers because the two
// formats can change independently: a new column type changes the descriptor
// format without touching how an existing value is written, and a change to how
// a value is written does not change how a shape is described.
const (
	descriptorVersion byte = 0x01
	encodingVersion   byte = 0x01
)

// flagRequired is bit 0 of a descriptor field's flags byte. Every other bit is
// reserved and must be zero, so a future flag cannot be silently ignored by an
// older reader — it makes the descriptor fail to parse instead.
const flagRequired byte = 0x01

// fingerprintDomain prefixes the fingerprint's hash input so that a schema
// fingerprint and a batch hash can never be the same 32 bytes even if their
// remaining inputs somehow coincided.
const fingerprintDomain = "icelake/schema-fp/v1\n"

// MaxValueBytes is the longest a single string or binary value may be in the
// canonical encoding: the largest length its four-byte length prefix can
// express. This is the concrete meaning of ARCHITECTURE.md's "longer than the
// encoder's addressable limits" — the poison check at Insert refuses such a
// value at the boundary, and this package refuses it as the second line.
const MaxValueBytes uint32 = math.MaxUint32

// Descriptor is a recorded schema shape: the ordered list of columns a payload
// was encoded under, its canonical serialized form, and the fingerprint that
// names it.
//
// It is the value stored in staging_schemas(fp, descriptor), and it is what
// makes replay independent of the current binary: a row staged before a
// schema-changing deploy is decoded by the shape recorded alongside it, never
// by whatever the current declaration happens to say. A Descriptor is immutable
// once constructed — both constructors validate, canonicalize, serialize and
// fingerprint up front, so [Descriptor.Bytes] and [Descriptor.Fingerprint] are
// pure accessors that cannot disagree with the fields.
//
// It deliberately carries no namespace or table name. The fingerprint is a
// function of shape alone, so two tables that happen to have identical shapes
// share one staging_schemas row — which is correct, because a staged row's
// table identity is already recorded in its own columns.
type Descriptor struct {
	fields  []Field
	encoded []byte
	fp      string
}

// Describe derives a descriptor from a declared Iceberg schema — in practice
// the one schemamap already computed for the table, reached through
// Declaration.IcebergSchema.
//
// The Iceberg schema is the right seam of the three the declaration derives.
// The Parquet and Arrow schemas are transport representations, and reading
// field IDs back out of Arrow means re-parsing a metadata string; the Iceberg
// schema is the table's own contract, carries id, name, type and requiredness
// as first-class fields, and is the last link of the chain — so a descriptor
// derived from it cannot describe a shape different from the one the table will
// actually hold. Nothing is re-derived from struct tags here.
//
// Fields are sorted into ascending field-ID order, which is the order
// everything downstream depends on: the payload's value order, the descriptor's
// own serialized order, and therefore the fingerprint and the batch hash.
func Describe(table string, sc *iceberg.Schema) (*Descriptor, error) {
	return FromFields(table, sc.Fields())
}

// FromFields derives a descriptor from a bare list of Iceberg columns, which is
// what [Describe] does once it has taken the fields off a schema.
//
// It is a separate entry point for one caller, and the caller is what makes it
// worth having: the runtime schema document states a table's shape as a list of
// columns, and the cross-check that pins the document path against drift needs a
// descriptor built *directly* from that list — never touching Parquet or Arrow —
// to compare against the one derived through the three-call chain. Two
// independent routes from one input to one canonical form, and equality of their
// fingerprints is exactly the property the write path depends on, since that
// fingerprint keys staging_schemas and is hashed into every batch key.
//
// The two entry points share this implementation rather than resembling each
// other, which is the point: a check between two derivations is only worth
// running if the thing being compared is computed the same way on both sides.
func FromFields(table string, src []iceberg.NestedField) (*Descriptor, error) {
	if len(src) == 0 {
		return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, "", 0,
			"schema has no columns; there is nothing to encode")
	}

	fields := make([]Field, 0, len(src))
	for _, nf := range src {
		f, err := describeField(table, nf)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	// An Iceberg schema lists its fields in declaration order, which need not be
	// id order — a struct's fields can be reordered freely, since identity is
	// the permanent id and never the position. Ascending id order is therefore
	// imposed here rather than inherited.
	slices.SortFunc(fields, func(a, b Field) int { return cmp.Compare(a.ID, b.ID) })

	// Unreachable through the declaration path as it stands: iceberg-go's own
	// schema constructor panics on a schema with two fields sharing an id, so
	// no such *iceberg.Schema can be built to hand to this function. Kept as a
	// second line anyway, because an id naming exactly one column is the
	// assumption every cross-shape decision in this package rests on, and a
	// silent violation of it would remap staged values onto the wrong column.
	for i := 1; i < len(fields); i++ {
		if fields[i].ID == fields[i-1].ID {
			return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, fields[i].Name, fields[i].ID,
				fmt.Sprintf("two columns share field id %d; field ids are permanent identities and must be unique", fields[i].ID))
		}
	}

	return newDescriptor(table, fields)
}

// describeField maps one Iceberg column onto its canonical type tag, refusing
// anything the encoding has no representation for.
func describeField(table string, nf iceberg.NestedField) (Field, error) {
	unsupported := func(detail string) (Field, error) {
		return Field{}, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, nf.Name, nf.ID, detail)
	}

	f := Field{ID: nf.ID, Name: nf.Name, Required: nf.Required}

	switch t := nf.Type.(type) {
	case iceberg.BooleanType:
		f.Kind = KindBoolean
	case iceberg.Int32Type:
		f.Kind = KindInt
	case iceberg.Int64Type:
		f.Kind = KindLong
	case iceberg.Float32Type:
		f.Kind = KindFloat
	case iceberg.Float64Type:
		f.Kind = KindDouble
	case iceberg.StringType:
		f.Kind = KindString
	case iceberg.BinaryType:
		f.Kind = KindBinary
	case iceberg.TimestampTzType:
		f.Kind = KindTimestampTz
	case iceberg.DecimalType:
		f.Kind = KindDecimal
		f.Precision, f.Scale = t.Precision(), t.Scale()
	default:
		return unsupported(fmt.Sprintf(
			"column type %s has no canonical encoding; the declaration path supports boolean, int, long, float, double, decimal, timestamptz, string and binary only", nf.Type))
	}

	return f, nil
}

// IcebergFields returns the descriptor's columns as Iceberg fields, in
// ascending field-id order: the exact inverse of what [Describe] read.
//
// It exists for one caller, and the caller is what makes it worth having rather
// than being a convenience. The transition drain has to bring a live table to
// the shape of a spool file written weeks earlier, with no declaration struct
// and no schema document anywhere in the process describing it — all it holds is
// the descriptor the spool stored beside the file. Turning that back into a
// declaration starts here, because the mapping from a canonical kind to an
// Iceberg type is exactly the mapping [describeField] performs in the other
// direction, and the two belong beside each other so a new type cannot be added
// to one and forgotten in the other.
//
// It cannot fail. Both constructors refuse a field whose kind is not one of the
// nine, and refuse a decimal whose precision or scale is out of range, so every
// field of an existing Descriptor has a type on the other side of this switch.
func (d *Descriptor) IcebergFields() []iceberg.NestedField {
	out := make([]iceberg.NestedField, 0, len(d.fields))
	for _, f := range d.fields {
		var typ iceberg.Type
		switch f.Kind {
		case KindBoolean:
			typ = iceberg.PrimitiveTypes.Bool
		case KindInt:
			typ = iceberg.PrimitiveTypes.Int32
		case KindLong:
			typ = iceberg.PrimitiveTypes.Int64
		case KindFloat:
			typ = iceberg.PrimitiveTypes.Float32
		case KindDouble:
			typ = iceberg.PrimitiveTypes.Float64
		case KindDecimal:
			typ = iceberg.DecimalTypeOf(f.Precision, f.Scale)
		case KindTimestampTz:
			typ = iceberg.PrimitiveTypes.TimestampTz
		case KindString:
			typ = iceberg.PrimitiveTypes.String
		case KindBinary:
			typ = iceberg.PrimitiveTypes.Binary
		}
		out = append(out, iceberg.NestedField{ID: f.ID, Name: f.Name, Type: typ, Required: f.Required})
	}

	return out
}

// ParseDescriptor reads a descriptor back from its stored bytes.
//
// It is strict on purpose: unknown version, unknown type tag, reserved flag
// bit, out-of-range decimal, truncation, trailing bytes and any departure from
// canonical form are all refused rather than tolerated. The canonicality check
// is structural rather than a list of rules — the parsed fields are
// re-serialized and the result must be byte-identical to the input — so exactly
// one byte string can ever describe a given shape, which is what lets the
// fingerprint be a hash of these bytes.
//
// It takes no table name, and the errors it returns carry none, because
// staging_schemas is keyed by fingerprint alone and is shared by every table: a
// descriptor read out of it belongs to whichever tables happen to have that
// shape, so naming one of them in an error would be a guess.
func ParseDescriptor(b []byte) (*Descriptor, error) {
	fields, err := decodeDescriptor(b)
	if err != nil {
		return nil, err
	}

	d, err := newDescriptor("", fields)
	if err != nil {
		return nil, err
	}
	if string(d.encoded) != string(b) {
		return nil, errdef.NewEncodingError(errdef.EncodingKindFormat, "", "", 0,
			"descriptor is not in canonical form; columns must appear exactly once, in ascending field-id order")
	}
	return d, nil
}

// newDescriptor validates every field, serializes the canonical form once, and
// fingerprints it. Both constructors funnel through here so a Descriptor can
// never exist in a state its own bytes disagree with.
func newDescriptor(table string, fields []Field) (*Descriptor, error) {
	// The column count goes into the serialized form as a uint32, and those
	// bytes are part of a hash preimage. A count that truncated on the way in
	// would let two different shapes serialize identically, which is exactly
	// what the length fields exist to prevent. Unreachable with any real
	// schema; checked because "unreachable" is not a property of a durable
	// format, it is a property of today's callers.
	if uint64(len(fields)) > uint64(MaxValueBytes) {
		return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, "", 0,
			fmt.Sprintf("schema has %d columns, more than the %d a descriptor can address", len(fields), MaxValueBytes))
	}

	for _, f := range fields {
		if f.ID < 1 || f.ID > math.MaxInt32 {
			return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, f.Name, f.ID,
				fmt.Sprintf("field id %d is out of range; ids run from 1 to %d", f.ID, math.MaxInt32))
		}
		if f.Name == "" {
			return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, f.Name, f.ID,
				fmt.Sprintf("field id %d has an empty column name", f.ID))
		}
		if uint64(len(f.Name)) > uint64(MaxValueBytes) {
			return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, f.Name, f.ID,
				fmt.Sprintf("field id %d has a column name longer than %d bytes", f.ID, MaxValueBytes))
		}
		if _, ok := knownKind(f.Kind); !ok {
			return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, f.Name, f.ID,
				fmt.Sprintf("column has %s", f.Kind))
		}
		if f.Kind == KindDecimal {
			if f.Precision < 1 || f.Precision > maxDecimalPrecision {
				return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, f.Name, f.ID,
					fmt.Sprintf("decimal precision %d is out of range; an int64 unscaled value holds 1 to %d digits", f.Precision, maxDecimalPrecision))
			}
			if f.Scale < 0 || f.Scale > f.Precision {
				return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, f.Name, f.ID,
					fmt.Sprintf("decimal scale %d is out of range for precision %d", f.Scale, f.Precision))
			}
		} else if f.Precision != 0 || f.Scale != 0 {
			return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, f.Name, f.ID,
				fmt.Sprintf("column of type %s carries a decimal precision or scale", f.Kind))
		}
	}

	enc := encodeDescriptor(fields)

	h := sha256.New()
	h.Write([]byte(fingerprintDomain))
	h.Write(enc)

	return &Descriptor{
		fields:  slices.Clone(fields),
		encoded: enc,
		fp:      hex.EncodeToString(h.Sum(nil)),
	}, nil
}

// knownKind reports whether a type tag is one this version of the format
// defines.
func knownKind(k Kind) (Kind, bool) {
	switch k {
	case KindBoolean, KindInt, KindLong, KindFloat, KindDouble, KindDecimal, KindTimestampTz, KindString, KindBinary:
		return k, true
	default:
		return 0, false
	}
}

// encodeDescriptor writes the canonical serialized form. The layout is
// documented in full in the package doc comment and in ARCHITECTURE.md; it is
// fixed and versioned because these bytes are stored in staging_schemas and are
// read back by binaries that no longer contain the struct they came from.
func encodeDescriptor(fields []Field) []byte {
	out := make([]byte, 0, 5+len(fields)*24)
	out = append(out, descriptorVersion)
	out = binary.BigEndian.AppendUint32(out, uint32(len(fields)))

	for _, f := range fields {
		out = binary.BigEndian.AppendUint32(out, uint32(f.ID))
		out = append(out, byte(f.Kind))

		var flags byte
		if f.Required {
			flags |= flagRequired
		}
		out = append(out, flags)

		if f.Kind == KindDecimal {
			out = append(out, byte(f.Precision), byte(f.Scale))
		}

		out = binary.BigEndian.AppendUint32(out, uint32(len(f.Name)))
		out = append(out, f.Name...)
	}

	return out
}

// decodeDescriptor is the exact inverse of encodeDescriptor, and refuses
// anything it does not recognize instead of guessing.
func decodeDescriptor(b []byte) ([]Field, error) {
	malformed := func(detail string) ([]Field, error) {
		return nil, errdef.NewEncodingError(errdef.EncodingKindFormat, "", "", 0, detail)
	}

	c := cursor{buf: b}

	version, ok := c.byteAt()
	if !ok {
		return malformed("descriptor is empty")
	}
	if version != descriptorVersion {
		return malformed(fmt.Sprintf("descriptor format version %#02x is not %#02x, the only version this build writes", version, descriptorVersion))
	}

	count, ok := c.uint32()
	if !ok {
		return malformed("descriptor ends before its column count")
	}
	// A zero-column descriptor is refused for the same reason Describe refuses
	// a zero-column schema: it would describe a shape with nothing in it, whose
	// payloads are a lone version byte and whose batches would still mint a
	// perfectly real object key. Symmetry between the two constructors is the
	// point — a shape this package will not produce is a shape it will not
	// accept back either.
	if count == 0 {
		return malformed("descriptor has no columns; there is nothing to encode")
	}
	// Every column costs at least eleven bytes, so a count larger than the
	// bytes that remain is corruption, not an allocation request. Checking it
	// before the make below is what keeps a damaged length field from asking
	// for gigabytes.
	if uint64(count) > uint64(c.remaining()) {
		return malformed(fmt.Sprintf("descriptor claims %d columns but has only %d bytes left", count, c.remaining()))
	}

	fields := make([]Field, 0, count)
	prevID := 0
	for i := range int(count) {
		var f Field

		id, ok := c.uint32()
		if !ok {
			return malformed(fmt.Sprintf("descriptor ends inside column %d's field id", i))
		}
		if id < 1 || id > math.MaxInt32 {
			return malformed(fmt.Sprintf("descriptor column %d has field id %d, outside the valid range 1 to %d", i, id, math.MaxInt32))
		}
		// Strictly ascending, which rules out both a repeated id and a
		// descriptor written in any order but the canonical one. Exactly one
		// byte string may describe a shape, or the fingerprint would not be a
		// function of the shape.
		if int(id) <= prevID {
			return malformed(fmt.Sprintf("descriptor column %d has field id %d, which does not follow %d; columns must appear once each, in ascending field-id order", i, id, prevID))
		}
		prevID = int(id)
		f.ID = int(id)

		tag, ok := c.byteAt()
		if !ok {
			return malformed(fmt.Sprintf("descriptor ends inside column %d's type tag", i))
		}
		kind, known := knownKind(Kind(tag))
		if !known {
			return malformed(fmt.Sprintf("descriptor column %d has %s", i, Kind(tag)))
		}
		f.Kind = kind

		flags, ok := c.byteAt()
		if !ok {
			return malformed(fmt.Sprintf("descriptor ends inside column %d's flags", i))
		}
		if flags&^flagRequired != 0 {
			return malformed(fmt.Sprintf("descriptor column %d sets reserved flag bits %#02x", i, flags&^flagRequired))
		}
		f.Required = flags&flagRequired != 0

		if f.Kind == KindDecimal {
			precision, ok := c.byteAt()
			if !ok {
				return malformed(fmt.Sprintf("descriptor ends inside column %d's decimal precision", i))
			}
			scale, ok := c.byteAt()
			if !ok {
				return malformed(fmt.Sprintf("descriptor ends inside column %d's decimal scale", i))
			}
			f.Precision, f.Scale = int(precision), int(scale)
		}

		nameLen, ok := c.uint32()
		if !ok {
			return malformed(fmt.Sprintf("descriptor ends inside column %d's name length", i))
		}
		name, ok := c.bytes(nameLen)
		if !ok {
			return malformed(fmt.Sprintf("descriptor ends inside column %d's name", i))
		}
		f.Name = string(name)

		fields = append(fields, f)
	}

	if c.remaining() != 0 {
		return malformed(fmt.Sprintf("descriptor has %d trailing bytes after its last column", c.remaining()))
	}

	return fields, nil
}

// Bytes returns the canonical serialized descriptor — the blob stored in
// staging_schemas.descriptor. The result is a copy; a Descriptor's own bytes
// can never be modified through it.
func (d *Descriptor) Bytes() []byte { return slices.Clone(d.encoded) }

// Fingerprint returns schema_fp: the lowercase hex SHA-256 over a fixed domain
// tag followed by the canonical descriptor bytes.
//
// Equal shapes give equal fingerprints and different shapes give different
// ones, for every difference that matters — a field added, removed, renamed,
// retyped, made optional or required, or given a different id — because every
// one of those changes the descriptor bytes the hash is taken over.
//
// The name is included deliberately, and this is the one place the fingerprint
// covers more than decoding strictly needs: decoding is by field id, so a
// rename cannot change how a payload is read. It is included because
// staging_schemas is content-addressed — fp is that table's primary key and the
// descriptor is its value — so a hash that did not cover the whole stored value
// would let two genuinely different descriptors collide onto one row, and the
// descriptor that came back would be whichever was written first.
func (d *Descriptor) Fingerprint() string { return d.fp }

// Len returns the number of columns.
func (d *Descriptor) Len() int { return len(d.fields) }

// Fields returns the columns in ascending field-ID order. The result is a copy.
func (d *Descriptor) Fields() []Field { return slices.Clone(d.fields) }

// IndexOfFieldID returns the position of the column with the given permanent
// field id, and whether there is one. It is what a replay path uses to line a
// decoded [Row] up against a different shape by id rather than by position.
func (d *Descriptor) IndexOfFieldID(id int) (int, bool) {
	for i, f := range d.fields {
		if f.ID == id {
			return i, true
		}
	}
	return 0, false
}

// cursor is a bounds-checked reader over a byte slice. It reports failure
// rather than panicking, because every input it reads has been sitting in a
// database file and may have been written by a different build.
type cursor struct {
	buf []byte
	pos int
}

func (c *cursor) remaining() int { return len(c.buf) - c.pos }

func (c *cursor) byteAt() (byte, bool) {
	if c.remaining() < 1 {
		return 0, false
	}
	b := c.buf[c.pos]
	c.pos++
	return b, true
}

func (c *cursor) bytes(n uint32) ([]byte, bool) {
	if uint64(c.remaining()) < uint64(n) {
		return nil, false
	}
	b := c.buf[c.pos : c.pos+int(n)]
	c.pos += int(n)
	return b, true
}

func (c *cursor) uint32() (uint32, bool) {
	b, ok := c.bytes(4)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint32(b), true
}
