package canon

import (
	"github.com/gvkhna/icelake/internal/errdef"
)

// Remap moves a row decoded under one recorded shape onto another, matching
// columns by permanent field id and never by name or position.
//
// This is the whole of the schema-fingerprint decision, expressed as a
// function. A row staged before a deploy is decoded by the shape recorded
// alongside it, then remapped onto the shape the current binary declares:
//
//   - A column both shapes have carries its value across, whatever it is called
//     on either side. A rename is invisible here, which is correct — identity is
//     the id, so renaming a column is not a change to what its values are.
//   - A column only the target shape has becomes null. This is the ordinary
//     added-field case, and null is exactly right because a field added after a
//     table exists is always optional.
//   - A column only the source shape has fails loudly. Staged data exists that
//     the current declaration cannot even represent, so half-opening the writer
//     would silently drop a column's accepted values. The operator drains the
//     table with the previous binary first.
//   - A column the target requires but the source has no value for — either
//     because the source shape lacks it or because it recorded a null there —
//     fails loudly too. There is no honest value to invent, and a zero would be
//     invented data.
//   - A column both shapes have under different types fails loudly, rather than
//     reinterpreting staged bytes under a type that was not what wrote them.
//
// Every refusal is an [errdef.StagingSchemaError] carrying the field id and
// which case it was.
func Remap(table string, from, to *Descriptor, row Row) (Row, error) {
	if len(row) != len(from.fields) {
		return nil, errdef.NewEncodingError(errdef.EncodingKindValue, table, "", 0,
			"row length does not match the shape it was decoded under")
	}
	if err := CompatibleShapes(table, from, to); err != nil {
		return nil, err
	}

	out := make(Row, len(to.fields))
	for i, want := range to.fields {
		j, ok := from.IndexOfFieldID(want.ID)
		if !ok {
			continue
		}

		v := row[j]
		if v == nil && want.Required {
			return nil, errdef.StagingSchemaError{
				Table:   table,
				FieldID: want.ID,
				Kind:    errdef.StagingSchemaKindFieldRequired,
			}
		}
		out[i] = v
	}

	return out, nil
}

// CompatibleShapes reports whether every row written under one recorded shape
// can be mapped onto another, by field id, without inventing or dropping a
// value. It is the shape-level half of [Remap], separated so that a writer
// opening against staged rows can refuse the whole table up front rather than
// discovering the same thing one row at a time at flush.
//
// The three refusals are exactly [Remap]'s, and in the same order. The
// removed-field case is checked first and across the whole source shape, so
// that a deploy dropping a staged column is reported as exactly that even when
// the same deploy also added one: it is the most serious of the three and the
// one with a specific operator action attached.
//
// One case is deliberately not here and cannot be: a source row that recorded a
// null for a column the target requires. That is a property of a value rather
// than of a shape, so it can only surface when the row itself is remapped.
func CompatibleShapes(table string, from, to *Descriptor) error {
	for _, f := range from.fields {
		if _, ok := to.IndexOfFieldID(f.ID); !ok {
			return errdef.StagingSchemaError{
				Table:   table,
				FieldID: f.ID,
				Kind:    errdef.StagingSchemaKindFieldRemoved,
			}
		}
	}

	for _, want := range to.fields {
		j, ok := from.IndexOfFieldID(want.ID)
		if !ok {
			if want.Required {
				return errdef.StagingSchemaError{
					Table:   table,
					FieldID: want.ID,
					Kind:    errdef.StagingSchemaKindFieldRequired,
				}
			}

			continue
		}

		have := from.fields[j]
		if have.Kind != want.Kind || have.Precision != want.Precision || have.Scale != want.Scale {
			return errdef.StagingSchemaError{
				Table:   table,
				FieldID: want.ID,
				Kind:    errdef.StagingSchemaKindFieldRetyped,
			}
		}
	}

	return nil
}

// Transcode re-encodes a stored payload from the shape it was written under
// into the shape currently declared, in one call: decode by the recorded
// descriptor, remap by field id, encode under the new one.
//
// This is the seal-time step that keeps a sealed batch fingerprint-homogeneous.
// Any staged row whose fingerprint differs from the current schema's is
// transcoded in the same transaction that stamps its batch key, so the batch
// hash is always computed over payload bytes that agree with the schema
// component of its own input — and so a batch never mixes two shapes' bytes
// under one key.
//
// A row already under the target shape needs none of this; callers compare
// fingerprints first and transcode only the rows that differ.
func Transcode(table string, from, to *Descriptor, payload []byte) ([]byte, error) {
	row, err := Decode(table, from, payload)
	if err != nil {
		return nil, err
	}
	remapped, err := Remap(table, from, to, row)
	if err != nil {
		return nil, err
	}
	return Encode(table, to, remapped)
}
