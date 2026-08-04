package schemamap

import (
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/gvkhna/icelake/internal/errdef"
)

// DeclareFields derives a declaration from a list of columns rather than from a
// Go type: a programmatic twin of the tag parser, and then the identical tail.
//
// SCHEMA.md's "Deriving it: a programmatic twin of the first library call"
// section is the design. The struct path is three chained library calls, and
// this path replaces only the first — where schema.NewSchemaFromStruct reads
// tags off a Go type to build a Parquet schema tree, this builds the same tree
// node by node through arrow-go's own constructors. Calls two and three are
// [derive], shared with [Declare], so both sources run the same code rather
// than two copies of it.
//
// The column vocabulary is [iceberg.NestedField] — an id, a name, a type and a
// required flag — because that is the one shape vocabulary both sides of this
// library already speak: it is what the third call produces, what a table is
// created from, and what internal/canon describes a payload's shape in terms
// of. A second spelling of the same nine types would be a second thing to keep
// in step, which is the trade SCHEMA.md refuses everywhere else.
//
// It has two callers. The transition drain reaches it through
// [DeclareFromDescriptor] below, with the descriptor a spool file was written
// under; and the root package's runtime schema document reaches it directly,
// with the columns that document declares. Neither adds a derivation: the
// document path adds its own parsing above this and its own
// descriptor-fingerprint cross-check beside it, and that check exists precisely
// because this function is a twin of a library call rather than the call itself.
//
// Two details of the twin are load-bearing and are recorded here as well as in
// SCHEMA.md so nobody "simplifies" them. A decimal column is built on physical
// INT64, because that is what the struct path derives from an int64 field, and
// it is where the precision-18 ceiling comes from. A timestamp column is built
// at millisecond unit with isAdjustedToUTC true: the declaration says
// milliseconds and the microsecond upcast Iceberg wants happens in the Parquet
// write path, in [FileSchema], exactly as it does for a struct. Neither detail
// is guarded by the fingerprint cross-check — a decimal on a different physical
// type and a timestamp at a different unit both derive the identical Iceberg
// column and the identical fingerprint (SCHEMA.md's M12 correction records the
// measurement) — so the guard is TestStructAndFieldsDeriveOneSchema, which
// compares the two declarations' Parquet schemas and fails on both mutations.
func DeclareFields(namespace, tableName string, fields []iceberg.NestedField) (*Declaration, error) {
	if err := ValidateName(errdef.NameKindNamespace, namespace); err != nil {
		return nil, err
	}
	if err := ValidateName(errdef.NameKindTable, tableName); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, errdef.DeclarationError{Table: tableName, Kind: errdef.DeclarationKindNoFields}
	}

	nodes := make(pqschema.FieldList, 0, len(fields))
	for _, f := range fields {
		node, err := primitiveNode(tableName, f)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	// The root group is named for the table and carries no field id, which is
	// what the tag parser produces for a declaration struct (it names the root
	// after the Go type). Neither the name nor the id reaches the Arrow schema:
	// the Parquet-to-Arrow call with no key-value metadata builds its fields
	// from the root's children and gives the schema no metadata of its own.
	root, err := pqschema.NewGroupNode(tableName, parquet.Repetitions.Repeated, nodes, -1)
	if err != nil {
		return nil, derivationError(tableName, "building the parquet schema", err)
	}

	return derive(namespace, tableName, pqschema.NewSchema(root))
}

// DeclareFromDescriptor derives a declaration from a recorded schema shape.
//
// It is the third entry onto the same path and it exists for one caller: the
// transition drain has to create or reconcile a table to the shape of a spool
// file written under a declaration that no longer exists anywhere in the
// process, and the descriptor the spool stored beside that file is the only
// statement of that shape there is. A descriptor's field already carries an id,
// a name, a type, a precision and scale and a required flag, so this is a
// conversion into [DeclareFields] rather than a fourth derivation — which is
// what makes the pre-order field-ID rule apply to a table the drain creates
// exactly as it applies to any other.
//
// The descriptor's columns arrive in ascending field-id order, which is the
// order the pre-order rule requires them to be declared in.
func DeclareFromDescriptor(namespace, tableName string, fields []iceberg.NestedField) (*Declaration, error) {
	return DeclareFields(namespace, tableName, fields)
}

// derive is the shared tail: the two library calls after the Parquet schema
// exists, and the four refusals between and after them.
//
// It is shared rather than duplicated because the artifact a caller gets back
// has to be the same artifact whichever source stated the shape — not merely an
// equivalent one. Everything downstream of a *Declaration reads its Iceberg
// schema, its Arrow schema and its field ids, and two derivations that agreed
// today would be two things to keep agreeing forever.
func derive(namespace, tableName string, parquetSchema *pqschema.Schema) (*Declaration, error) {
	// Call 2 of 3. Carries every field's permanent ID across into the Arrow
	// field's own metadata, under the key "PARQUET:field_id".
	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		return nil, derivationError(tableName, "deriving arrow schema", err)
	}

	// A zero-column declaration is refused here rather than downstream,
	// because nothing downstream refuses it: an empty Iceberg schema creates a
	// zero-column table, successfully and uselessly.
	if len(arrowSchema.Fields()) == 0 {
		return nil, errdef.DeclarationError{Table: tableName, Kind: errdef.DeclarationKindNoFields}
	}

	if err := rejectNested(tableName, arrowSchema); err != nil {
		return nil, err
	}
	if err := checkPreorderFieldIDs(tableName, arrowSchema); err != nil {
		return nil, err
	}

	// Call 3 of 3. Reads "PARQUET:field_id" back out and produces the schema
	// used to create the table or compare against a live one.
	icebergSchema, err := table.ArrowSchemaToIceberg(arrowSchema, false, nil)
	if err != nil {
		return nil, derivationError(tableName, "deriving iceberg schema", err)
	}

	return &Declaration{
		namespace: namespace,
		table:     tableName,
		parquet:   parquetSchema,
		arrow:     arrowSchema,
		iceberg:   icebergSchema,
	}, nil
}

// primitiveNode builds one column's Parquet node, matching what the tag parser
// builds for the equivalent struct field.
//
// The three kinds that carry no logical type go through the converted-type
// constructor with ConvertedTypes.None, which is the call the tag parser makes
// for an untagged bool, float32, float64 and []byte field; the rest carry the
// logical type their tag would have set. Matching the constructor and not only
// the resulting type is deliberate — the two produce nodes that differ in their
// recorded type length, and a twin that agreed on everything a reader looks at
// while disagreeing on a field a future arrow-go release starts looking at
// would be a drift nothing here would catch.
func primitiveNode(tableName string, f iceberg.NestedField) (pqschema.Node, error) {
	repetition := parquet.Repetitions.Optional
	if f.Required {
		repetition = parquet.Repetitions.Required
	}
	id := int32(f.ID) //nolint:gosec // G115: a field id is checked against MaxInt32 wherever one is recorded

	logical := func(lt pqschema.LogicalType, physical parquet.Type) (pqschema.Node, error) {
		n, err := pqschema.NewPrimitiveNodeLogical(f.Name, repetition, lt, physical, 0, id)
		if err != nil {
			return nil, derivationError(tableName, fmt.Sprintf("building the parquet node for column %q", f.Name), err)
		}

		return n, nil
	}
	plain := func(physical parquet.Type) (pqschema.Node, error) {
		n, err := pqschema.NewPrimitiveNodeConverted(f.Name, repetition, physical, pqschema.ConvertedTypes.None, 0, 0, 0, id)
		if err != nil {
			return nil, derivationError(tableName, fmt.Sprintf("building the parquet node for column %q", f.Name), err)
		}

		return n, nil
	}

	switch typ := f.Type.(type) {
	case iceberg.BooleanType:
		return plain(parquet.Types.Boolean)
	case iceberg.Int32Type:
		return logical(pqschema.NewIntLogicalType(32, true), parquet.Types.Int32)
	case iceberg.Int64Type:
		return logical(pqschema.NewIntLogicalType(64, true), parquet.Types.Int64)
	case iceberg.Float32Type:
		return plain(parquet.Types.Float)
	case iceberg.Float64Type:
		return plain(parquet.Types.Double)
	case iceberg.DecimalType:
		//nolint:gosec // G115: precision and scale are validated against the 18-digit ceiling before a shape is recorded
		return logical(pqschema.NewDecimalLogicalType(int32(typ.Precision()), int32(typ.Scale())), parquet.Types.Int64)
	case iceberg.TimestampTzType:
		return logical(pqschema.NewTimestampLogicalType(true, pqschema.TimeUnitMillis), parquet.Types.Int64)
	case iceberg.StringType:
		return logical(pqschema.StringLogicalType{}, parquet.Types.ByteArray)
	case iceberg.BinaryType:
		return plain(parquet.Types.ByteArray)
	default:
		return nil, errdef.DeclarationError{
			Table: tableName,
			Field: f.Name,
			Kind:  errdef.DeclarationKindDerivation,
			Err: fmt.Errorf("column %q has type %s, which no declaration source can state; the nine declarable types are boolean, int, long, float, double, decimal, timestamptz, string and binary",
				f.Name, f.Type),
		}
	}
}
