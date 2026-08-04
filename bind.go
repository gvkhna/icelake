package icelake

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/array/arreflect"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/schemamap"
)

// rowCodec converts between a caller's records and the two forms the write path
// needs: the canonical row a record is staged as, and the Arrow record a batch
// of them is encoded from.
//
// It exists because a table's shape now has two sources. A declaration struct
// and a runtime schema document produce the same *Declaration and the same
// descriptor, so everything from staging onwards is one implementation — but the
// two front doors take different things from a caller, a typed Go value on one
// side and parsed JSON on the other, and converting those is the one step that
// cannot be shared. Everything else a writer does is above or below this
// interface, which is what keeps "there is exactly one writer implementation in
// this library" true.
//
// A codec is immutable once built and safe for concurrent use, because Insert
// calls Bind outside the writer's lock.
type rowCodec[T any] interface {
	// Bind converts one record into its canonical row form, in ascending
	// field-id order. The struct codec cannot fail here — the declaration was
	// checked against the shape when the codec was built — and the dynamic codec
	// can, because its input has not been through a compiler.
	Bind(v T) (canon.Row, error)

	// Build turns a batch's decoded canonical rows into the Arrow record its
	// data file is written from, at the file schema's own types. The returned
	// record is released by the caller.
	Build(rows []canon.Row) (arrow.RecordBatch, error)

	// FileSchema returns the Arrow schema Build builds records at: the declared
	// schema with millisecond timestamps widened to the microseconds Iceberg
	// defines. The writer takes its copy from here rather than deriving a second
	// one, so a table has exactly one file schema and the record a codec builds
	// is validated against the schema it was built with.
	FileSchema() *arrow.Schema
}

// binder converts between a caller's declaration struct and the canonical row
// form, in both directions, matching columns by permanent field id.
//
// Both directions are load-bearing and neither is avoidable. Insert needs
// struct-to-row, because the canonical payload is what gets durably staged and
// what a batch's content hash is taken over. Replay needs row-to-struct,
// because after a crash the staged payloads are all that is left of the
// records: they are decoded by the shape recorded alongside them, remapped onto
// the current shape by field id, and only then turned back into values the
// row-data reflection can build Arrow columns from.
//
// This is the hand-written mapping SCHEMA.md describes rather than a library
// call: no library maps "values keyed by permanent field id" onto "this Go
// struct", because the permanent-id discipline is icelake's own.
//
// A binder is immutable once built and safe for concurrent use.
//
// It is the struct half of [rowCodec]; internal/schemamap's document path and
// the dynamic codec beside it are the other half.
type binder[T any] struct {
	table  string
	fields []boundField
	// file is the Arrow schema data files are written with — the declared schema
	// with millisecond timestamps widened to the microseconds Iceberg defines.
	// Build adapts every column to it.
	file *arrow.Schema
}

// boundField is one descriptor column resolved against one struct field. The
// slice of these is parallel to the descriptor's own field order — ascending
// field-id order — so conversion is an indexed walk with no lookups per row.
type boundField struct {
	// index is the struct field's index within T.
	index int
	// column is the descriptor column, carried for its kind, id, name and
	// requiredness.
	column canon.Field
	// pointer is true for an optional column, declared as a pointer field.
	pointer bool
	// elem is the field's type with any pointer stripped, which is what values
	// are read from and written into.
	elem reflect.Type
}

// newBinder resolves a declaration struct T against a recorded schema shape.
//
// Columns are matched to struct fields by name, using the same rule the
// row-data reflection layer applies — the arrow struct tag's name, or the Go
// field name when the tag names nothing — because that layer's column order is
// what the Arrow record is ultimately built from, and the declaration's
// startup cross-check has already required those names to equal the declared
// schema's.
//
// Every mismatch fails here rather than per row: a column with no field, a
// field whose Go kind cannot hold its column's canonical type, or an optional
// column bound to a non-pointer field. In normal operation none of these can
// happen, because Declare validated the same struct against the same schema
// before this is ever called; they are checked because a silent mismatch would
// write one column's values into another's.
func newBinder[T any](tableName string, desc *canon.Descriptor, file *arrow.Schema) (*binder[T], error) {
	declType := reflect.TypeFor[T]()
	if declType.Kind() != reflect.Struct {
		return nil, errdef.DeclarationError{Table: tableName, Kind: errdef.DeclarationKindNotStruct}
	}

	byName := make(map[string]int, declType.NumField())
	for i := range declType.NumField() {
		f := declType.Field(i)
		if !f.IsExported() {
			continue
		}
		byName[rowDataColumnName(f)] = i
	}

	fields := make([]boundField, 0, desc.Len())
	for _, column := range desc.Fields() {
		i, ok := byName[column.Name]
		if !ok {
			return nil, errdef.NewCrossCheckError(tableName, column.Name, fmt.Sprintf(
				"field id %d is in the table's shape but no field of the declaration struct names it", column.ID))
		}

		f := declType.Field(i)
		elem := f.Type
		pointer := elem.Kind() == reflect.Pointer
		if pointer {
			elem = elem.Elem()
		}

		if pointer != !column.Required {
			return nil, errdef.NewCrossCheckError(tableName, column.Name, fmt.Sprintf(
				"column is %s but the declaration field is %s; an optional column is declared as a pointer and a required one is not",
				requiredWord(column.Required), pointerWord(pointer)))
		}
		if !kindHoldsColumn(elem.Kind(), column.Kind) {
			return nil, errdef.NewCrossCheckError(tableName, column.Name, fmt.Sprintf(
				"declaration field has Go type %s, which cannot hold a %s column", f.Type, column.Kind))
		}

		fields = append(fields, boundField{index: i, column: column, pointer: pointer, elem: elem})
	}

	return &binder[T]{table: tableName, fields: fields, file: file}, nil
}

// rowDataColumnName is the column name the row-data reflection layer gives a
// struct field: the arrow tag's name, or the Go field name when the tag is
// absent or names nothing. It is duplicated here rather than asked for because
// that layer exposes no accessor for it, and getting it wrong would bind a
// value to the wrong column.
func rowDataColumnName(f reflect.StructField) string {
	tag, ok := f.Tag.Lookup("arrow")
	if !ok {
		return f.Name
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return f.Name
	}
	return name
}

// requiredWord and pointerWord keep the mismatch message readable without
// building it out of booleans.
func requiredWord(required bool) string {
	if required {
		return "required"
	}
	return "optional"
}

func pointerWord(pointer bool) string {
	if pointer {
		return "a pointer"
	}
	return "not a pointer"
}

// kindHoldsColumn reports whether a Go kind can carry a canonical column type.
// It is a kind check rather than a type check on purpose: a caller may declare
// a named type over one of the whitelisted underlying types, and that is a
// perfectly ordinary Go declaration the derivation already accepts.
func kindHoldsColumn(k reflect.Kind, c canon.Kind) bool {
	switch c {
	case canon.KindBoolean:
		return k == reflect.Bool
	case canon.KindInt:
		return k == reflect.Int32
	case canon.KindLong, canon.KindDecimal, canon.KindTimestampTz:
		return k == reflect.Int64
	case canon.KindFloat:
		return k == reflect.Float32
	case canon.KindDouble:
		return k == reflect.Float64
	case canon.KindString:
		return k == reflect.String
	case canon.KindBinary:
		return k == reflect.Slice
	default:
		return false
	}
}

// Bind converts one record into its canonical row form, in ascending field-id
// order. A nil pointer field becomes a null; every other value is read out at
// the exact Go type its column's canonical encoding accepts.
//
// It cannot fail. Every way a struct could disagree with its table's shape was
// refused when this binder was built, so there is nothing left to judge per
// record — the error in [rowCodec]'s signature is the dynamic codec's, whose
// input no compiler has looked at.
func (b *binder[T]) Bind(v T) (canon.Row, error) {
	rv := reflect.ValueOf(v)
	row := make(canon.Row, len(b.fields))

	for i, f := range b.fields {
		fv := rv.Field(f.index)
		if f.pointer {
			if fv.IsNil() {
				continue
			}
			fv = fv.Elem()
		}

		switch f.column.Kind {
		case canon.KindBoolean:
			row[i] = fv.Bool()
		case canon.KindInt:
			row[i] = int32(fv.Int())
		case canon.KindLong, canon.KindDecimal, canon.KindTimestampTz:
			row[i] = fv.Int()
		case canon.KindFloat:
			row[i] = float32(fv.Float())
		case canon.KindDouble:
			row[i] = fv.Float()
		case canon.KindString:
			row[i] = fv.String()
		case canon.KindBinary:
			row[i] = fv.Bytes()
		}
	}

	return row, nil
}

// Value converts a canonical row back into a record.
//
// A null becomes a nil pointer field, which is the same thing the row-data
// reflection layer turns back into a null Arrow slot — so a value that was
// never supplied stays absent all the way from staging to the committed data
// file, rather than becoming a zero somewhere in between.
//
// A row whose length or value types do not match the shape this binder was
// built for is refused rather than partially applied.
func (b *binder[T]) Value(row canon.Row) (T, error) {
	var out T
	if len(row) != len(b.fields) {
		return out, errdef.NewEncodingError(errdef.EncodingKindValue, b.table, "", 0, fmt.Sprintf(
			"row has %d values but the table's shape has %d columns", len(row), len(b.fields)))
	}

	rv := reflect.ValueOf(&out).Elem()
	for i, f := range b.fields {
		v := row[i]
		if v == nil {
			if f.column.Required {
				return out, errdef.NewEncodingError(errdef.EncodingKindValue, b.table, f.column.Name, f.column.ID,
					"column is required and cannot hold a null")
			}
			continue
		}

		dst := rv.Field(f.index)
		if f.pointer {
			dst.Set(reflect.New(f.elem))
			dst = dst.Elem()
		}
		if err := b.set(dst, f, v); err != nil {
			return out, err
		}
	}

	return out, nil
}

// set writes one decoded value into one struct field, refusing any Go type
// other than the single one that column's canonical encoding produces.
func (b *binder[T]) set(dst reflect.Value, f boundField, v any) error {
	wrongType := func() error {
		return errdef.NewEncodingError(errdef.EncodingKindValue, b.table, f.column.Name, f.column.ID, fmt.Sprintf(
			"decoded value has Go type %T, which is not what a %s column holds", v, f.column.Kind))
	}

	switch f.column.Kind {
	case canon.KindBoolean:
		x, ok := v.(bool)
		if !ok {
			return wrongType()
		}
		dst.SetBool(x)
	case canon.KindInt:
		x, ok := v.(int32)
		if !ok {
			return wrongType()
		}
		dst.SetInt(int64(x))
	case canon.KindLong, canon.KindDecimal, canon.KindTimestampTz:
		x, ok := v.(int64)
		if !ok {
			return wrongType()
		}
		dst.SetInt(x)
	case canon.KindFloat:
		x, ok := v.(float32)
		if !ok {
			return wrongType()
		}
		dst.SetFloat(float64(x))
	case canon.KindDouble:
		x, ok := v.(float64)
		if !ok {
			return wrongType()
		}
		dst.SetFloat(x)
	case canon.KindString:
		x, ok := v.(string)
		if !ok {
			return wrongType()
		}
		dst.SetString(x)
	case canon.KindBinary:
		x, ok := v.([]byte)
		if !ok {
			return wrongType()
		}
		dst.SetBytes(x)
	default:
		return errdef.NewEncodingError(errdef.EncodingKindUnsupportedType, b.table, f.column.Name, f.column.ID,
			fmt.Sprintf("column has %s", f.column.Kind))
	}

	return nil
}

// FileSchema returns the Arrow schema this codec builds records at. See
// [rowCodec] for why the writer takes its copy from here.
func (b *binder[T]) FileSchema() *arrow.Schema { return b.file }

// Build turns a batch's decoded rows into the Arrow record its data file is
// written from. It is the one generic step of the flush path, which is why the
// flush package takes it as a function.
//
// Three things happen here, in order: the canonical rows become values of the
// caller's own declaration type; the row-data reflection layer builds Arrow
// columns from those values; and each column is adapted to the type the file
// schema gives it — a real per-value widening for a decimal, and for a
// timestamp the rescale into the microseconds Iceberg's timestamptz is defined
// in. The record is built with the file schema itself, so every column keeps
// its permanent field id.
//
// The target types come from the file schema, never from the declared one, and
// that is what decides which arm of the adapter runs for a timestamp column.
// [schemamap.FileSchema] has already replaced every declared millisecond UTC
// column with a microsecond one, so no field reaching AdaptColumn from here can
// ask for milliseconds and the free re-wrap the adapter also offers is not
// reachable on this path. (Corrected at the completeness audit's fifth round,
// 2026-08-01: this comment used to promise "a re-wrap for a timestamp", naming
// the one timestamp conversion a flush never performs and understating the cost
// of the one it always performs — a pass over every value and a fresh buffer,
// per timestamp column, per batch.)
func (b *binder[T]) Build(rows []canon.Row) (record arrow.RecordBatch, err error) {
	values := make([]T, len(rows))
	for i, row := range rows {
		if values[i], err = b.Value(row); err != nil {
			return nil, err
		}
	}

	mem := memory.DefaultAllocator
	raw, err := arreflect.FromSlice(values, mem)
	if err != nil {
		return nil, err
	}
	defer raw.Release()

	columns, ok := raw.(*array.Struct)
	if !ok {
		return nil, errdef.NewCrossCheckError(b.table, "",
			fmt.Sprintf("the row-data reflection produced %s rather than a struct of columns", raw.DataType()))
	}

	fields := b.file.Fields()
	if columns.NumField() != len(fields) {
		return nil, errdef.NewCrossCheckError(b.table, "", fmt.Sprintf(
			"the row-data reflection produced %d columns for a table whose declared schema has %d",
			columns.NumField(), len(fields)))
	}

	adapted := make([]arrow.Array, 0, len(fields))
	defer func() {
		for _, c := range adapted {
			c.Release()
		}
	}()
	for i, f := range fields {
		column, err := schemamap.AdaptColumn(b.table, f.Name, columns.Field(i), f.Type, mem)
		if err != nil {
			return nil, err
		}
		adapted = append(adapted, column)
	}

	// The record constructor validates column count and type and reports a
	// disagreement by panicking. Nothing here can reach that — the adapter has
	// just produced each column at the schema's own type — but a background
	// flush must not be able to take a caller's process down, so it is turned
	// back into an error.
	defer func() {
		if r := recover(); r != nil {
			record, err = nil, errdef.NewCrossCheckError(b.table, "",
				fmt.Sprintf("the adapted columns do not match the declared schema: %v", r))
		}
	}()

	return array.NewRecordBatch(b.file, adapted, int64(len(rows))), nil
}
