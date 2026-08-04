package icelake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"

	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/schemamap"
)

// maxDocDecimalPrecision is the widest precision a declared decimal column may
// have, and it is not a policy this file invented: the canonical encoding
// carries a decimal's unscaled value as an int64, and 18 digits is what an int64
// holds. The struct path arrives at the same ceiling from the other side,
// through arrow-go's own applicability check on an INT64-backed decimal.
const maxDocDecimalPrecision = 18

// FieldSpec is one column of a table declared at runtime: everything about it
// that a schema document states, and nothing else.
//
// It is the same type the document decodes into, deliberately, so the public
// spelling of a column and the file format's spelling cannot drift apart — there
// is one struct with one set of rules, and building a [TableSchema] by hand runs
// exactly the validation a document runs.
type FieldSpec struct {
	// Name is the column name.
	Name string `json:"name"`

	// Type is the column's type, spelled as one of the nine the canonical
	// encoding carries: "boolean", "int", "long", "float", "double", "string",
	// "binary", "timestamptz", or "decimal(P,S)".
	//
	// That vocabulary is the nine Iceberg types this design's encoding speaks,
	// not the Go type whitelist the struct path uses and not arrow-go's tag
	// spellings. The Go whitelist exists to work around an ambiguity a document
	// does not have — reflection cannot say whether a plain int meant 32 bits or
	// 64 — so a document names the column type outright and the smallest honest
	// vocabulary is the one the rest of the system already uses.
	//
	// A "timestamptz" value is epoch milliseconds and is stored as the Iceberg
	// microseconds the format defines, which is the identical contract the
	// struct path's logical.unit=millis tag creates, upcast by the identical
	// adapter. There is no _ms naming convention here because the unit is in the
	// type rather than in the column's name.
	Type string `json:"type"`

	// FieldID is the column's permanent field ID, and it is mandatory. It
	// carries exactly the meaning it carries in a struct tag: permanent, never
	// reordered, never reused. It is subject to the same pre-order rule at table
	// creation, which for the flat shapes this path can express means 1..N in
	// declaration order.
	FieldID int `json:"fieldid"`

	// Optional makes the column nullable. Absent means required, for the same
	// reason a value type is required on the struct path: a column that is
	// optional by omission is a column nobody decided about.
	Optional bool `json:"optional"`
}

// TableSchema is one table's shape, stated at runtime rather than by a Go type:
// the field specs a declaration is derived from, and nothing else.
//
// It is produced by [ParseSchemaDocument] and by [NewTableSchema], and by the
// time it exists every rule of the format has been checked — the names, the
// types, the field IDs, the duplicates — so [OpenDynamicWriter] can derive from
// it without judging it again.
//
// It carries no namespace or table name. Those belong to the [DynamicTableConfig]
// the schema is opened under, exactly as a declaration struct carries neither.
type TableSchema struct {
	// fields is the columns in declaration order, already validated and already
	// converted into the one shape vocabulary both declaration sources share.
	fields []iceberg.NestedField
	// specs is what the caller stated, kept so [TableSchema.Fields] can hand it
	// back without reconstructing a spelling.
	specs []FieldSpec
}

// Fields returns the column specs this schema was built from, in declaration
// order. The result is a copy.
func (s *TableSchema) Fields() []FieldSpec { return append([]FieldSpec(nil), s.specs...) }

// Len returns the number of columns.
func (s *TableSchema) Len() int { return len(s.specs) }

// DynamicTableConfig identifies one table declared at runtime and, optionally,
// overrides how it batches. It is [TableConfig] for a caller with no Go type to
// name.
type DynamicTableConfig struct {
	// Namespace is the table's namespace. Required, and validated by the same
	// rule and with the same [NameError] the struct path uses.
	Namespace string

	// Table is the table name. Required, same rule, same error.
	Table string

	// Schema is the table's shape. Required.
	Schema *TableSchema

	// Flush overrides the store's batching thresholds for this table. Optional;
	// nil means inherit them, exactly as in [TableConfig].
	Flush *FlushPolicy

	// OnAccept is called with every record this table accepts, synchronously, on
	// the caller's own goroutine, immediately after the staging write commits
	// and before Insert returns. Optional. Its whole contract — what it is for,
	// what an error from it means, what it must not do — is [TableConfig.OnAccept]'s,
	// with a parsed [Record] in place of a declaration struct.
	//
	// The map it is handed is the one the writer accepted, and nothing else
	// reads it afterwards, so a callback may keep it.
	OnAccept func(row Record) error
}

// ParseSchemaDocument reads a schema document and returns one table
// configuration per table it declares, with Flush and OnAccept left zero for the
// caller to fill in.
//
// The format is one object with a "tables" array, each entry naming a namespace,
// a table and its fields:
//
//	{"tables": [
//	  {"namespace": "market", "table": "fills", "fields": [
//	    {"name": "symbol", "type": "string",        "fieldid": 1},
//	    {"name": "price",  "type": "decimal(18,9)", "fieldid": 2},
//	    {"name": "ts_ms",  "type": "timestamptz",   "fieldid": 3},
//	    {"name": "note",   "type": "string",        "fieldid": 4, "optional": true}
//	  ]}
//	]}
//
// It is parsed with encoding/json and DisallowUnknownFields, so a misspelled key
// is refused rather than ignored — a "feildid" that parsed as "no field id"
// would be the exact class of silent shape change the permanent-ID rule exists to
// make impossible.
//
// Every refusal happens here, before any table is touched, and a document with
// anything wrong in it opens nothing: an unknown key, a duplicate
// namespace/table pair, a table with no fields, two fields sharing a name, a
// type spelling outside the nine, a malformed decimal(P,S) or one past the
// 18-digit ceiling, and a decimal whose scale exceeds its precision are all
// [SchemaDocError]. Two refusals deliberately reuse the errors the struct path
// raises rather than restating them: a bad namespace or table name is a
// [NameError], and a field ID that is missing or does not equal its pre-order
// position is a [FieldIDError]. The rule being broken in each case is the same
// rule whichever source stated the shape, and a caller matching on it should not
// have to know which one did.
//
// A document declaring no tables is not an error. It parses to an empty slice,
// and what to do about a configuration that declares nothing is the caller's
// judgement rather than this function's.
func ParseSchemaDocument(doc []byte) ([]DynamicTableConfig, error) {
	var parsed schemaDocument

	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&parsed); err != nil {
		return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindSyntax, "", "", "the document is not the JSON this format is", err)
	}
	// One document is one JSON value. Anything after it is not a second
	// document, it is a document somebody has half-edited.
	if dec.More() {
		return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindSyntax, "", "",
			"the document carries more than one JSON value", nil)
	}

	out := make([]DynamicTableConfig, 0, len(parsed.Tables))
	seen := make(map[string]bool, len(parsed.Tables))

	for _, t := range parsed.Tables {
		if err := schemamap.ValidateName(errdef.NameKindNamespace, t.Namespace); err != nil {
			return nil, err
		}
		if err := schemamap.ValidateName(errdef.NameKindTable, t.Table); err != nil {
			return nil, err
		}

		id := t.Namespace + "." + t.Table
		if seen[id] {
			return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindDuplicateTable, id, "",
				"the document declares this namespace and table twice", nil)
		}
		seen[id] = true

		schema, err := newTableSchema(id, t.Fields)
		if err != nil {
			return nil, err
		}

		out = append(out, DynamicTableConfig{Namespace: t.Namespace, Table: t.Table, Schema: schema})
	}

	return out, nil
}

// NewTableSchema builds a table's shape from field specs directly, for a caller
// that has one to state and no document to state it in.
//
// It runs exactly the validation [ParseSchemaDocument] runs and raises exactly
// the same errors, because it is the same code: the only thing a document adds
// is the JSON around these specs. The table name is used to name the table in
// any error and is not otherwise kept — a schema belongs to whichever
// [DynamicTableConfig] opens it.
func NewTableSchema(tableName string, fields []FieldSpec) (*TableSchema, error) {
	return newTableSchema(tableName, fields)
}

// schemaDocument is the document's top level. Its shape is the format, and
// DisallowUnknownFields is what makes that a fact rather than a hope.
type schemaDocument struct {
	Tables []schemaDocTable `json:"tables"`
}

// schemaDocTable is one table entry: an identity and a list of columns.
type schemaDocTable struct {
	Namespace string      `json:"namespace"`
	Table     string      `json:"table"`
	Fields    []FieldSpec `json:"fields"`
}

// newTableSchema validates a table's field specs and converts them into the one
// shape vocabulary both declaration sources share.
//
// The conversion target is [iceberg.NestedField] rather than a spec type of this
// package's own, and that is the M11 correction recorded in SCHEMA.md: the
// Iceberg field list is what internal/schemamap's derivation takes and what
// internal/canon describes a shape in terms of, so using it here means the nine
// types have exactly one spelling downstream of this function.
func newTableSchema(tableName string, fields []FieldSpec) (*TableSchema, error) {
	if len(fields) == 0 {
		return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindNoFields, tableName, "",
			"the table declares no fields; a zero-column table is created successfully and uselessly", nil)
	}

	out := make([]iceberg.NestedField, 0, len(fields))
	names := make(map[string]bool, len(fields))

	for i, f := range fields {
		if f.Name == "" {
			return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindDuplicateField, tableName, "",
				fmt.Sprintf("field %d has no name", i+1), nil)
		}
		if names[f.Name] {
			return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindDuplicateField, tableName, f.Name,
				"the table declares this column twice", nil)
		}
		names[f.Name] = true

		// The pre-order rule, checked here so a document is judged whole before
		// any table is touched. It is checked again inside the derivation, on
		// the schema the derivation actually built, which is where the struct
		// path meets it too; this earlier copy exists so that a caller reading a
		// document gets told which field is wrong rather than being told at open
		// time.
		//
		// A missing fieldid decodes as zero, and zero is never a legal ID, so
		// the two are one mistake and are reported as the one the struct path
		// reports for a field that states no ID at all.
		if want := i + 1; f.FieldID != want {
			id := f.FieldID
			if id == 0 {
				id = -1
			}

			return nil, errdef.FieldIDError{Table: tableName, Field: f.Name, FieldID: id, WantID: want}
		}

		typ, err := parseColumnType(tableName, f.Name, f.Type)
		if err != nil {
			return nil, err
		}

		out = append(out, iceberg.NestedField{
			ID:       f.FieldID,
			Name:     f.Name,
			Type:     typ,
			Required: !f.Optional,
		})
	}

	return &TableSchema{fields: out, specs: append([]FieldSpec(nil), fields...)}, nil
}

// parseColumnType turns one of the nine type spellings into the Iceberg type it
// names.
//
// The set is closed, and a spelling outside it is refused rather than guessed
// at — the same discipline as the Go type whitelist and the column adapter's,
// applied to the one place a column type arrives as text.
func parseColumnType(tableName, field, spelling string) (iceberg.Type, error) {
	switch spelling {
	case "boolean":
		return iceberg.PrimitiveTypes.Bool, nil
	case "int":
		return iceberg.PrimitiveTypes.Int32, nil
	case "long":
		return iceberg.PrimitiveTypes.Int64, nil
	case "float":
		return iceberg.PrimitiveTypes.Float32, nil
	case "double":
		return iceberg.PrimitiveTypes.Float64, nil
	case "string":
		return iceberg.PrimitiveTypes.String, nil
	case "binary":
		return iceberg.PrimitiveTypes.Binary, nil
	case "timestamptz":
		return iceberg.PrimitiveTypes.TimestampTz, nil
	}

	// Anything beginning with "decimal" is a decimal somebody spelled wrong,
	// which is a more useful thing to be told than that the type is unknown.
	if strings.HasPrefix(spelling, "decimal") {
		return parseDecimalType(tableName, field, spelling)
	}

	return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindUnknownType, tableName, field, fmt.Sprintf(
		"type %q is not one of boolean, int, long, float, double, string, binary, timestamptz or decimal(P,S)", spelling), nil)
}

// parseDecimalType reads decimal(P,S) and refuses everything else about it.
//
// Exactly one spelling is accepted — no spaces, both numbers present — for the
// same reason exactly one base64 spelling is accepted for a binary value: a type
// that can be written several ways is a type two documents can disagree about
// while looking identical.
func parseDecimalType(tableName, field, spelling string) (iceberg.Type, error) {
	bad := func(detail string) (iceberg.Type, error) {
		return nil, errdef.NewSchemaDocError(errdef.SchemaDocKindDecimal, tableName, field, detail, nil)
	}

	inner, ok := strings.CutPrefix(spelling, "decimal(")
	if !ok {
		return bad(fmt.Sprintf("type %q is not decimal(P,S)", spelling))
	}
	inner, ok = strings.CutSuffix(inner, ")")
	if !ok {
		return bad(fmt.Sprintf("type %q is not decimal(P,S)", spelling))
	}
	precisionText, scaleText, ok := strings.Cut(inner, ",")
	if !ok {
		return bad(fmt.Sprintf("type %q states no scale; a decimal is decimal(P,S)", spelling))
	}

	precision, err := strconv.Atoi(precisionText)
	if err != nil {
		return bad(fmt.Sprintf("type %q has a precision that is not a number", spelling))
	}
	scale, err := strconv.Atoi(scaleText)
	if err != nil {
		return bad(fmt.Sprintf("type %q has a scale that is not a number", spelling))
	}

	if precision < 1 || precision > maxDocDecimalPrecision {
		return bad(fmt.Sprintf(
			"precision %d is out of range; an int64 unscaled value holds 1 to %d digits", precision, maxDocDecimalPrecision))
	}
	if scale < 0 || scale > precision {
		return bad(fmt.Sprintf("scale %d is out of range for precision %d", scale, precision))
	}

	return iceberg.DecimalTypeOf(precision, scale), nil
}
