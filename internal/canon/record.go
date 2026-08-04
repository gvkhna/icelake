package canon

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/gvkhna/icelake/internal/errdef"
)

// Row is one record's field values, in the same order as the [Descriptor] they
// belong to — ascending field-ID order. A Row is always exactly as long as its
// descriptor has columns; a value is never omitted, only null.
//
// Each value's dynamic type is fixed by its column's [Kind], and nothing else
// is accepted:
//
//	KindBoolean                              bool
//	KindInt                                  int32
//	KindLong, KindDecimal, KindTimestampTz   int64
//	KindFloat                                float32
//	KindDouble                               float64
//	KindString                               string
//	KindBinary                               []byte
//
// A null is the untyped nil interface value. A []byte column is a special case
// worth stating: a nil []byte is a present, empty value, not a null, because
// the interface holding it is not nil. That distinction is deliberate and
// matches what the column can actually represent — a required binary column has
// no null to round-trip to — so nil and empty []byte both encode to a
// zero-length value and both decode back to a non-nil, zero-length slice.
//
// Decimal values are the unscaled int64 the caller already scaled, and
// timestamp values are the caller's own epoch milliseconds. This package moves
// bits and never rescales, reinterprets or validates a value's domain — the
// same contract the column adapter states, and for the same reason: a value
// problem must be refused at the boundary where the caller supplied it and the
// context to fix it exists, which is Insert's poison check, not inside an
// encoder with no caller to return to.
type Row []any

// Encode writes one record's canonical byte form: the payload stored in
// staging.payload, and the exact bytes a batch's content hash — and therefore
// its object key — is computed over.
//
// The layout is one format-version byte followed by each column's value in
// ascending field-ID order, with no per-field framing: which shape wrote a
// payload is recorded alongside it as a schema fingerprint, not inside it. An
// optional column's value is preceded by a single presence byte; a required
// column's is not, which makes a null in a required column unrepresentable
// rather than merely forbidden. Fixed-width values are big-endian; string and
// binary values carry a four-byte big-endian length prefix.
//
// The encoding is total and deterministic: the same values under the same shape
// produce the same bytes on every platform and every build, which is the
// property upload idempotency rests on. Nothing here depends on Go map
// iteration, struct field order, pointer identity, or a library's own notion of
// stable output.
func Encode(table string, d *Descriptor, row Row) ([]byte, error) {
	if len(row) != len(d.fields) {
		return nil, errdef.NewEncodingError(errdef.EncodingKindValue, table, "", 0, fmt.Sprintf(
			"row has %d values but the schema shape has %d columns", len(row), len(d.fields)))
	}

	out := make([]byte, 0, encodedSizeHint(d))
	out = append(out, encodingVersion)

	for i, f := range d.fields {
		v := row[i]

		if v == nil {
			if f.Required {
				return nil, errdef.NewEncodingError(errdef.EncodingKindValue, table, f.Name, f.ID,
					"column is required and cannot hold a null")
			}
			out = append(out, 0x00)
			continue
		}
		if !f.Required {
			out = append(out, 0x01)
		}

		var err error
		if out, err = appendValue(out, table, f, v); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// encodedSizeHint guesses a payload's size closely enough to avoid regrowing
// the buffer for the common case. It is only a capacity hint; being wrong costs
// an allocation, never a wrong byte.
func encodedSizeHint(d *Descriptor) int {
	n := 1
	for _, f := range d.fields {
		if !f.Required {
			n++
		}
		if w, fixed := f.Kind.fixedWidth(); fixed {
			n += w
			continue
		}
		n += 4 + 16
	}
	return n
}

// appendValue writes one non-null value in its column's declared form, refusing
// any Go type other than the single one that column accepts.
func appendValue(out []byte, table string, f Field, v any) ([]byte, error) {
	wrongType := func() ([]byte, error) {
		return nil, errdef.NewEncodingError(errdef.EncodingKindValue, table, f.Name, f.ID, fmt.Sprintf(
			"value has Go type %T, which is not what a %s column holds", v, f.Kind))
	}

	switch f.Kind {
	case KindBoolean:
		b, ok := v.(bool)
		if !ok {
			return wrongType()
		}
		if b {
			return append(out, 0x01), nil
		}
		return append(out, 0x00), nil

	case KindInt:
		n, ok := v.(int32)
		if !ok {
			return wrongType()
		}
		return binary.BigEndian.AppendUint32(out, uint32(n)), nil

	case KindLong, KindDecimal, KindTimestampTz:
		n, ok := v.(int64)
		if !ok {
			return wrongType()
		}
		return binary.BigEndian.AppendUint64(out, uint64(n)), nil

	case KindFloat:
		x, ok := v.(float32)
		if !ok {
			return wrongType()
		}
		return binary.BigEndian.AppendUint32(out, math.Float32bits(x)), nil

	case KindDouble:
		x, ok := v.(float64)
		if !ok {
			return wrongType()
		}
		return binary.BigEndian.AppendUint64(out, math.Float64bits(x)), nil

	case KindString:
		s, ok := v.(string)
		if !ok {
			return wrongType()
		}
		if err := checkValueLen(table, f, len(s)); err != nil {
			return nil, err
		}
		out = binary.BigEndian.AppendUint32(out, uint32(len(s)))
		return append(out, s...), nil

	case KindBinary:
		b, ok := v.([]byte)
		if !ok {
			return wrongType()
		}
		if err := checkValueLen(table, f, len(b)); err != nil {
			return nil, err
		}
		out = binary.BigEndian.AppendUint32(out, uint32(len(b)))
		return append(out, b...), nil

	default:
		// Unreachable: no Descriptor can hold an unknown kind, because both
		// constructors validate every tag. Kept loud rather than silent so a
		// future kind added to the tag list and forgotten here fails on its
		// first record instead of writing something wrong.
		return nil, errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, table, f.Name, f.ID,
			fmt.Sprintf("column has %s", f.Kind))
	}
}

// checkValueLen enforces the four-byte length prefix's ceiling, which is what
// ARCHITECTURE.md's "longer than the encoder's addressable limits" means
// concretely.
func checkValueLen(table string, f Field, n int) error {
	if uint64(n) > uint64(MaxValueBytes) {
		return errdef.NewEncodingError(errdef.EncodingKindValue, table, f.Name, f.ID, fmt.Sprintf(
			"value is %d bytes, longer than the %d the canonical encoding can address", n, MaxValueBytes))
	}
	return nil
}

// Decode reads a payload back into field values, driven entirely by the
// descriptor recorded alongside it rather than by any current declaration.
//
// This is what makes replay after a schema-changing deploy correct: the shape
// that wrote a row is the shape that reads it, so a row staged by a previous
// build decodes into the values that build meant, not into whatever the current
// struct happens to line up positionally with. Mapping those values onto the
// current shape is a separate, explicit step — see [Remap] and [Transcode].
//
// Decoding is strict. A payload written under a different format version, one
// that ends early, one with a presence byte that is neither present nor absent,
// and one with bytes left over after its last column are all refused with a
// typed error rather than half-read.
func Decode(table string, d *Descriptor, payload []byte) (Row, error) {
	malformed := func(detail string) (Row, error) {
		return nil, errdef.NewEncodingError(errdef.EncodingKindFormat, table, "", 0, detail)
	}

	c := cursor{buf: payload}

	version, ok := c.byteAt()
	if !ok {
		return malformed("payload is empty")
	}
	if version != encodingVersion {
		return malformed(fmt.Sprintf("payload format version %#02x is not %#02x, the only version this build writes", version, encodingVersion))
	}

	row := make(Row, len(d.fields))
	for i, f := range d.fields {
		if !f.Required {
			present, ok := c.byteAt()
			if !ok {
				return malformed(fmt.Sprintf("payload ends before column %q (field id %d) presence byte", f.Name, f.ID))
			}
			switch present {
			case 0x00:
				continue
			case 0x01:
			default:
				return malformed(fmt.Sprintf("column %q (field id %d) has presence byte %#02x, which is neither absent (0x00) nor present (0x01)", f.Name, f.ID, present))
			}
		}

		v, err := readValue(&c, table, f)
		if err != nil {
			return nil, err
		}
		row[i] = v
	}

	if c.remaining() != 0 {
		return malformed(fmt.Sprintf("payload has %d trailing bytes after its last column", c.remaining()))
	}

	return row, nil
}

// readValue reads one non-null value in its column's declared form.
func readValue(c *cursor, table string, f Field) (any, error) {
	truncated := func(what string) (any, error) {
		return nil, errdef.NewEncodingError(errdef.EncodingKindFormat, table, f.Name, f.ID,
			fmt.Sprintf("payload ends inside this column's %s", what))
	}

	if width, fixed := f.Kind.fixedWidth(); fixed {
		raw, ok := c.bytes(uint32(width))
		if !ok {
			return truncated("value")
		}

		switch f.Kind {
		case KindBoolean:
			switch raw[0] {
			case 0x00:
				return false, nil
			case 0x01:
				return true, nil
			default:
				return nil, errdef.NewEncodingError(errdef.EncodingKindFormat, table, f.Name, f.ID,
					fmt.Sprintf("boolean value is %#02x, which is neither false (0x00) nor true (0x01)", raw[0]))
			}
		case KindInt:
			return int32(binary.BigEndian.Uint32(raw)), nil
		case KindFloat:
			return math.Float32frombits(binary.BigEndian.Uint32(raw)), nil
		case KindDouble:
			return math.Float64frombits(binary.BigEndian.Uint64(raw)), nil
		default: // KindLong, KindDecimal, KindTimestampTz
			return int64(binary.BigEndian.Uint64(raw)), nil
		}
	}

	n, ok := c.uint32()
	if !ok {
		return truncated("length prefix")
	}
	raw, ok := c.bytes(n)
	if !ok {
		return truncated("value")
	}

	if f.Kind == KindString {
		return string(raw), nil
	}
	// Copied, never aliased: a decoded Row must stay valid after the payload
	// buffer it came from is reused or freed.
	return append([]byte{}, raw...), nil
}
