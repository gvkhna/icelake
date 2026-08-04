package schemamap

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/gvkhna/icelake/internal/errdef"
)

// Declaration is one table's declared shape: the three schemas derived from a
// caller's tagged declaration struct, plus the identity of the table they
// describe. It is produced by [Declare] and is pure in-memory state — nothing
// in this package opens a file, touches a catalog, or makes a network call.
type Declaration struct {
	namespace string
	table     string

	parquet *pqschema.Schema
	arrow   *arrow.Schema
	iceberg *iceberg.Schema
}

// Namespace returns the validated namespace this declaration belongs to.
func (d *Declaration) Namespace() string { return d.namespace }

// Table returns the validated table name this declaration belongs to.
func (d *Declaration) Table() string { return d.table }

// ParquetSchema returns the Parquet schema tree derived from the declaration
// struct — the same shape the Parquet writer itself consumes.
func (d *Declaration) ParquetSchema() *pqschema.Schema { return d.parquet }

// ArrowSchema returns the Arrow schema the record batches for this table must
// be built with. Every field carries its permanent field ID in the
// "PARQUET:field_id" metadata key, which is what makes the Iceberg schema
// below derivable and what rides untouched through record construction.
func (d *Declaration) ArrowSchema() *arrow.Schema { return d.arrow }

// IcebergSchema returns the schema used to create the table, or to compare
// against a live one. Its field IDs are the ones the declaration struct asked
// for; a catalog's CreateTable reassigns them in pre-order, which is exactly
// why [Declare] refuses any declaration whose IDs are not already that
// numbering.
func (d *Declaration) IcebergSchema() *iceberg.Schema { return d.iceberg }

// Declare validates and derives everything icelake can know about a table
// before it ever talks to storage: the names, the three schemas, the field-ID
// numbering, and the agreement between the two independent reflection layers
// that read the declaration struct T.
//
// The order of the checks is load-bearing, because each one exists to produce
// a clearer failure than the step after it would:
//
//  1. Namespace and table names, first, so an unusable name is reported as a
//     name problem rather than surfacing later as an unreachable table.
//  2. The shape of the Go type itself — it must be a struct, every field must
//     be exported, and every field's Go type must be one of the seven this
//     design declares columns for. An unexported field still becomes a real
//     declared column, but carries no usable tags, so left alone it fails later
//     as a confusing missing-field-ID complaint about a field the caller cannot
//     tag. A field typed int, uint or a narrow integer derives to a perfectly
//     valid column of the wrong width — silently platform-dependent, in int's
//     case — which is why the whitelist runs before any derivation rather than
//     letting the result look reasonable; see [checkGoFieldTypes].
//  3. The struct-to-Parquet and Parquet-to-Arrow derivations.
//  4. A zero-column refusal: a declaration deriving to no columns would create
//     a zero-column table with no error from anything downstream.
//  5. Nested-group rejection, before anything consumes the schema: the
//     reconciliation loop diffs single-segment field paths only, so a nested
//     column would either silently do nothing or produce a wrong AddColumn
//     against a top-level path.
//  6. The pre-order field-ID check, which names the offending field. Without
//     it the Arrow-to-Iceberg call below still refuses a missing field ID, but
//     refuses the whole schema without saying which field caused it, and a
//     merely-gapped numbering is not refused at all — it is silently
//     renumbered at table creation.
//  7. The Arrow-to-Iceberg derivation.
//  8. The two-derivation cross-check against arreflect's data-path inference.
//
// Every failure is matchable with errors.As: [errdef.NameError] for the names,
// [errdef.DeclarationError] for a declaration icelake cannot derive a schema
// from at all (including a malformed struct tag, whose underlying library
// error stays reachable through Unwrap), [errdef.FieldIDError] for the field-ID
// rules, and [errdef.CrossCheckError] for a disagreement between the two
// derivations, including the nested-group rejection.
func Declare[T any](namespace, tableName string) (*Declaration, error) {
	if err := ValidateName(errdef.NameKindNamespace, namespace); err != nil {
		return nil, err
	}
	if err := ValidateName(errdef.NameKindTable, tableName); err != nil {
		return nil, err
	}

	declType := reflect.TypeFor[T]()
	if declType.Kind() != reflect.Struct {
		return nil, errdef.DeclarationError{Table: tableName, Kind: errdef.DeclarationKindNotStruct}
	}
	// The declared side turns an unexported field into a real column, and the
	// row-data side skips it, so the two derivations would disagree — but the
	// field-ID check fires first and complains about a missing fieldid tag on
	// a field no tag can help. Name the actual problem instead.
	for i := range declType.NumField() {
		if f := declType.Field(i); !f.IsExported() {
			return nil, errdef.DeclarationError{
				Table: tableName,
				Field: f.Name,
				Kind:  errdef.DeclarationKindUnexportedField,
			}
		}
	}
	if err := checkGoFieldTypes(tableName, declType); err != nil {
		return nil, err
	}

	var zero T

	// Call 1 of 3. The tag parser reports errors by panicking internally;
	// NewSchemaFromStruct recovers those and returns them, so a malformed tag
	// is a failed startup rather than a crash.
	parquetSchema, err := pqschema.NewSchemaFromStruct(zero)
	if err != nil {
		return nil, derivationError(tableName, "deriving parquet schema", err)
	}

	// Calls 2 and 3, and the four refusals around them, in [derive] — the tail
	// this path shares with [DeclareFields] rather than duplicating, so that a
	// declaration stated as a Go type and a declaration stated as a list of
	// columns are the same artifact produced by the same functions.
	decl, err := derive(namespace, tableName, parquetSchema)
	if err != nil {
		return nil, err
	}

	if err := crossCheck[T](tableName, decl.arrow); err != nil {
		return nil, err
	}

	return decl, nil
}

// checkGoFieldTypes enforces the closed set of Go field types a declaration may
// use: bool, int32, int64, float32, float64, string and []byte, plus a pointer
// to any of them for an optional column.
//
// The set exists because a declaration's Go types reach durable state. A field's
// Go type decides its Iceberg type, which is recorded in the schema descriptor,
// which is hashed into the schema fingerprint and into every batch's content
// hash — so a Go type whose meaning is not fixed makes a durable artifact that
// is not fixed either.
//
// A plain int is exactly that hazard, and it is the reason this check exists
// rather than being left to convention. It derives cleanly today: arrow-go's
// reflection branches on the type's bit width, so an int column becomes Iceberg
// long on a 64-bit build and Iceberg int on a 32-bit one. Nothing downstream
// notices — both types are perfectly valid — so the same source, compiled for a
// different word size, would produce a different descriptor, a different
// fingerprint and a different batch key for identical data. uint and uintptr
// carry the same hazard.
//
// The fixed-width integer types below 32 bits are refused for a second,
// independent reason: int8, int16, uint8, uint16 and uint32 all derive to a
// column type whose canonical encoding accepts a single Go type, and a uint32
// value above the signed 32-bit ceiling has no representation in it. Rather
// than encode a silent wrap, or grow the canonical encoding a case for every
// Go integer width, they are refused where the caller can still fix the
// declaration. uint64 is refused on the same grounds against int64.
//
// The refusal deliberately happens here, before any derivation runs, so the
// error names the Go field and its type rather than surfacing later as a
// perfectly reasonable-looking column of the wrong width.
// This check owns scalar column types and nothing else. A composite field — a
// nested struct, a slice of structs, an array, a map — is not a wrong column
// type, it is not a single column at all, and the flat-only scoping rejection
// and the derivation chain already refuse those with errors that name the
// actual problem. Those kinds are deliberately passed through here rather than
// being swept into a type-whitelist complaint that would be true but unhelpful.
func checkGoFieldTypes(tableName string, declType reflect.Type) error {
	for i := range declType.NumField() {
		f := declType.Field(i)
		if goFieldTypeVerdict(f.Type) == goTypeRefused {
			return errdef.NewUnsupportedGoTypeError(tableName, f.Name, f.Type.String())
		}
	}
	return nil
}

// goTypeVerdict is what this check has to say about one Go field type.
type goTypeVerdict int

const (
	// goTypeRefused is a scalar Go type outside the declarable whitelist.
	goTypeRefused goTypeVerdict = iota
	// goTypeDeclarable is one of the seven whitelisted types, or a pointer to
	// one.
	goTypeDeclarable
	// goTypeDeferred is a composite shape this check does not judge, left to
	// the nested-group rejection and the derivation chain.
	goTypeDeferred
)

// goFieldTypeVerdict classifies one field's Go type. A single pointer
// indirection means an optional column; a pointer to a pointer has no meaning
// in this design and is refused rather than unwrapped twice.
func goFieldTypeVerdict(t reflect.Type) goTypeVerdict {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Bool, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64, reflect.String:
		return goTypeDeclarable

	case reflect.Slice:
		// []byte, and any named type whose underlying element is a byte, is a
		// binary column. A slice of anything else is a list, which is a
		// composite shape this check leaves alone.
		if t.Elem().Kind() == reflect.Uint8 {
			return goTypeDeclarable
		}
		return goTypeDeferred

	case reflect.Struct, reflect.Array, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer:
		return goTypeDeferred

	default:
		// Everything left is a scalar this design does not declare columns for:
		// int, uint and uintptr, the narrow and unsigned fixed-width integers,
		// the complex types, and a double pointer.
		return goTypeRefused
	}
}

// derivationError wraps a failure from one of the three library calls in the
// derivation chain — a malformed logical type, a non-numeric fieldid, an
// unknown arrow tag option — as a typed, matchable error that keeps the
// library's own error reachable through Unwrap.
func derivationError(tableName, stage string, err error) error {
	return errdef.DeclarationError{
		Table: tableName,
		Kind:  errdef.DeclarationKindDerivation,
		Err:   fmt.Errorf("%s: %w", stage, err),
	}
}

// rejectNested enforces the flat-only scoping decision: a declared struct
// containing a nested group is refused at declaration time rather than
// half-supported. A slice of structs cannot be declared through this path at
// all — arrow-go gives the list's synthesized element node no field ID — so it
// is caught here (or, if it survives this far, by the Arrow-to-Iceberg call)
// rather than becoming a wrong AddColumn later.
func rejectNested(tableName string, sc *arrow.Schema) error {
	for _, f := range sc.Fields() {
		if arrow.IsNested(f.Type.ID()) {
			return errdef.NewCrossCheckError(tableName, f.Name,
				fmt.Sprintf("declared column has nested type %s; only flat, top-level fields are supported", f.Type))
		}
	}
	return nil
}

// checkPreorderFieldIDs requires every declared field ID to equal that field's
// position in a pre-order traversal of the declared schema, starting at 1.
//
// The traversal is written as a real depth-first walk rather than a 1..N loop
// even though flat-only scoping means only flat structs reach it today, where
// pre-order and declaration order coincide. Pre-order is what a catalog's
// CreateTable actually assigns, and the two orders diverge the moment anything
// nests, so the check stays correct rather than merely lucky if nesting is
// ever unblocked.
func checkPreorderFieldIDs(tableName string, sc *arrow.Schema) error {
	next := 1

	var walk func(fields []arrow.Field) error
	walk = func(fields []arrow.Field) error {
		for _, f := range fields {
			want := next
			next++

			if got := declaredFieldID(f); got != want {
				return errdef.FieldIDError{
					Table:   tableName,
					Field:   f.Name,
					FieldID: got,
					WantID:  want,
				}
			}

			// Unreachable today: rejectNested runs first, so no field
			// reaching here has a struct type. Verified correct at M1 by
			// bypassing that rejection; it becomes live the moment nesting is
			// unblocked, which is the whole reason it is written this way.
			if st, ok := f.Type.(*arrow.StructType); ok {
				if err := walk(st.Fields()); err != nil {
					return err
				}
			}
		}
		return nil
	}

	return walk(sc.Fields())
}

// declaredFieldID reads a field's permanent ID back out of the Arrow metadata
// key the Parquet-to-Arrow conversion writes it under. A field whose struct
// tag carried no fieldid is numbered -1 by arrow-go and is reported as -1 here,
// which is the value a missing tag surfaces as in a FieldIDError.
func declaredFieldID(f arrow.Field) int {
	raw, ok := f.Metadata.GetValue(table.ArrowParquetFieldIDKey)
	if !ok {
		return -1
	}
	id, err := strconv.Atoi(raw)
	if err != nil {
		return -1
	}
	return id
}
