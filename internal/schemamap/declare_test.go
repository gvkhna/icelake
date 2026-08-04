package schemamap

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"

	"github.com/gvkhna/icelake/internal/errdef"
)

// The tests in this file are TESTING.md's second sanctioned category — a
// package-internal behaviour test of a layer that is pure computation — and the
// claim shape they hold is the first of the two that category allows: the
// caller-facing half of every one of them is already proven end to end through
// the public API, and these pin the same seam one declaration at a time.
// OpenWriter's refusals reach a caller as typed errors in errors_test.go
// (NameError, DeclarationError, CrossCheckError, each matched with errors.As
// against the exported alias), and scenario 7 drives the name rules and both
// cross-check disagreements through OpenWriter against the real substrate. What
// no substrate-backed test can afford is the enumeration: every rejected name
// shape, every Go field type on and off the whitelist, the decimal precision
// ceiling from both sides, the order the declaration checks run in, and the
// seven ways the two reflections can disagree that
// TestDeclareRejectsCrossCheckDisagreements drives — one container start per
// case would buy nothing, because the layer under test never reaches a
// container. Three of those seven are held at a coarser grain on the
// caller-facing side than their own shape pair, and that is said here rather
// than rounded up: errors_test.go's CrossCheckError case proves OpenWriter
// hands a caller this type with its table and column, driven by a name
// disagreement, so the trailing parquet:"-" skip and the two nullability
// directions are proven as a type reaching a caller and enumerated only here.
//
// They drive the real libraries — real reflection, real derivation — with no
// container, no disk and no network, because the whole derivation chain is pure
// in-memory computation. Every error assertion goes through errors.As and typed
// fields; none of them looks at message text.
//
// This header used to describe the file as "the storage-free slices of
// TESTING.md scenario 7", which was a sanction that document had never granted
// — invented in good faith because the tests were plainly worth having, at a
// time when the philosophy as written covered none of them. The document was
// corrected on 2026-08-01 to describe the tier it actually governs, and this
// header was rewritten to cite it rather than to grant itself permission.

// -- The two example declarations from TESTING.md, tags copied from SCHEMA.md.

// caseAFill is TESTING.md's Case A: pure structured facts, no blob payload.
type caseAFill struct {
	Symbol      string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side        string `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price       int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity    int64  `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID  int64  `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID     string `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source      string `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
	VenueTimeMS int64  `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
}

// caseAFillEvolved is Case A after TESTING.md's schema-evolution step: one
// extra field, optional as the evolution rules require, carrying a fresh
// field ID that continues the pre-order numbering. Written out in full rather
// than embedding caseAFill, because the two reflection layers disagree about
// what an embedded struct means — the declared side makes it a nested group,
// the row-data side flattens it — which is a disagreement about Go embedding,
// not about anything this test is trying to prove.
type caseAFillEvolved struct {
	Symbol         string  `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Side           string  `parquet:"name=side, logical=String, fieldid=2" arrow:"side"`
	Price          int64   `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=3" arrow:"price"`
	Quantity       int64   `parquet:"name=quantity, logical=decimal, precision=18, scale=9, fieldid=4" arrow:"quantity"`
	SequenceID     int64   `parquet:"name=sequence_id, fieldid=5" arrow:"sequence_id"`
	OrderID        string  `parquet:"name=order_id, logical=String, fieldid=6" arrow:"order_id"`
	Source         string  `parquet:"name=source, logical=String, fieldid=7" arrow:"source"`
	VenueTimeMS    int64   `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"venue_timestamp_ms"`
	VenueOrderType *string `parquet:"name=venue_order_type, logical=String, fieldid=9" arrow:"venue_order_type"`
}

// caseBEventArchive is TESTING.md's Case B: the same machinery with a raw
// binary payload column.
type caseBEventArchive struct {
	SourceID        string `parquet:"name=source_id, logical=String, fieldid=1" arrow:"source_id"`
	EventKind       string `parquet:"name=event_kind, logical=String, fieldid=2" arrow:"event_kind"`
	SequenceOrdinal int64  `parquet:"name=sequence_ordinal, fieldid=3" arrow:"sequence_ordinal"`
	ObservedAtMS    int64  `parquet:"name=observed_at_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=4" arrow:"observed_at_ms"`
	Payload         []byte `parquet:"name=payload, type=BYTE_ARRAY, fieldid=5" arrow:"payload"`
	PayloadSHA256   string `parquet:"name=payload_sha256, logical=String, fieldid=6" arrow:"payload_sha256"`
}

// -- Declarations that must be refused.

type innerGroup struct {
	X int64 `parquet:"name=x, fieldid=10" arrow:"x"`
	Y int64 `parquet:"name=y, fieldid=11" arrow:"y"`
}

// nestedDecl declares a plain sub-struct. It survives the derivation chain
// intact, which is exactly why it has to be refused deliberately: the
// reconciliation loop diffs single-segment field paths only.
type nestedDecl struct {
	A   string     `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	Grp innerGroup `parquet:"name=grp, fieldid=2" arrow:"grp"`
	C   string     `parquet:"name=c, logical=String, fieldid=3" arrow:"c"`
}

// listElement is the struct listOfGroupsDecl's slice field carries. Its field
// IDs continue the pre-order numbering for the same reason innerGroup's do, so
// the declaration is refused for being a list of groups rather than for a
// numbering mistake that would mask it.
type listElement struct {
	X int64 `parquet:"name=x, fieldid=3" arrow:"x"`
	Y int64 `parquet:"name=y, fieldid=4" arrow:"y"`
}

// listOfGroupsDecl declares a slice of structs. Like nestedDecl above it is
// stopped by rejectNested, which refuses any nested top-level column and does
// not care that this one is a LIST rather than a STRUCT. Unlike nestedDecl it
// could not survive the rest of the chain even if that check were lifted:
// arrow-go gives the list's synthesized element node no field ID, and no tag
// supplies one, because a fieldid on the slice field names the list rather
// than its element.
type listOfGroupsDecl struct {
	A    string        `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	Rows []listElement `parquet:"name=rows, fieldid=2" arrow:"rows"`
}

// The three declarations below sit either side of the decimal precision
// ceiling: the last precision an INT64-backed decimal allows, the first it does
// not, and — on the other physical type arrow-go permits a decimal on — a
// precision that check accepts and icelake still refuses.

type decimal18Decl struct {
	Symbol string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Price  int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=2" arrow:"price"`
}

type decimal19Decl struct {
	Symbol string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Price  int64  `parquet:"name=price, logical=decimal, precision=19, scale=9, fieldid=2" arrow:"price"`
}

type decimal9Int32Decl struct {
	Symbol string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Price  int32  `parquet:"name=price, logical=decimal, precision=9, scale=2, fieldid=2" arrow:"price"`
}

// gappedIDsDecl is the natural result of deleting a column before a table is
// first created. Nothing in the library refuses it: the table would simply be
// created as 1, 2, 3 and the declaration could never match it again.
type gappedIDsDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, logical=String, fieldid=5" arrow:"b"`
	C string `parquet:"name=c, logical=String, fieldid=9" arrow:"c"`
}

// missingIDDecl omits a fieldid tag on one field of several.
type missingIDDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, logical=String" arrow:"b"`
	C string `parquet:"name=c, logical=String, fieldid=3" arrow:"c"`
}

// mismatchedNamesDecl names the same column differently in the two tag
// namespaces the two derivations read.
type mismatchedNamesDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, logical=String, fieldid=2" arrow:"bee"`
}

// skippedColumnDecl keeps a column on the declared side that the row-data side
// skips entirely.
type skippedColumnDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, logical=String, fieldid=2" arrow:"b"`
	C string `parquet:"name=c, logical=String, fieldid=3" arrow:"-"`
}

// declaredOnlySkipDecl is skippedColumnDecl's mirror: the *declared* side skips
// a column the row-data side keeps, and the skipped field is the last one.
// Position matters here and is the reason this shape is easy to miss. If a
// parquet:"-" field sits anywhere but last, the two derivations fall out of step
// at that index and the name-disagreement arm fires first; only a trailing skip
// leaves the two prefixes identical and the row-data side one column longer,
// which is the arm this declaration reaches.
type declaredOnlySkipDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"-" arrow:"b"`
}

// optionalRepetitionDecl makes a non-pointer field nullable on the declared side
// with an explicit repetition tag. The row-data side infers nullability from
// pointer-ness alone and has no way to see that tag, so it calls the same column
// required.
type optionalRepetitionDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, logical=String, repetition=optional, fieldid=2" arrow:"b"`
}

// requiredRepetitionDecl is the mirror of the above: a pointer field forced back
// to required on the declared side, which the row-data side still infers as
// nullable.
type requiredRepetitionDecl struct {
	A string  `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B *string `parquet:"name=b, logical=String, repetition=required, fieldid=2" arrow:"b"`
}

// wideDecimalDecl asks for a decimal wider than 38 digits. It survives the
// parquet derivation and the arrow conversion — arrow-go is happy to build a
// decimal256 — and is refused only at the third derivation call, where the Arrow
// schema is converted to an Iceberg one and Iceberg has no such type.
type wideDecimalDecl struct {
	A []byte `parquet:"name=a, type=FIXED_LEN_BYTE_ARRAY, length=18, logical=decimal, precision=40, scale=2, fieldid=1" arrow:"a"`
}

// bareStringDecl leaves logical=String off one string column. The declared
// side then makes it a binary blob while the row-data side makes it utf8 —
// the disagreement that bites first, on every table, if the rule is not
// enforced.
type bareStringDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, fieldid=2" arrow:"b"`
}

// microsTimestampDecl asks for a timestamp at a unit outside the whitelist.
type microsTimestampDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	T int64  `parquet:"name=t, logical=timestamp, logical.unit=micros, logical.isadjustedutc=true, fieldid=2" arrow:"t"`
}

// emptyDecl declares no columns at all. Nothing downstream refuses it: it
// would create a zero-column table, successfully.
type emptyDecl struct{}

// unexportedFieldDecl carries a field the caller cannot tag. The declared side
// still turns it into a real column.
type unexportedFieldDecl struct {
	A      string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	secret string //nolint:unused // present precisely to be rejected by Declare
	C      string `parquet:"name=c, logical=String, fieldid=2" arrow:"c"`
}

// badLogicalDecl names a logical type the tag parser does not know.
type badLogicalDecl struct {
	A string `parquet:"name=a, logical=NotAThing, fieldid=1" arrow:"a"`
}

// badFieldIDTagDecl gives fieldid a value that is not a number.
type badFieldIDTagDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=one" arrow:"a"`
}

// badArrowOptionDecl uses an option the arrow tag parser does not know. Only
// the row-data side reads that tag namespace, so this fails there.
type badArrowOptionDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a,nonsense"`
}

// The declarations below all derive perfectly cleanly through the three-call
// chain and pass the cross-check — which is exactly why they need refusing
// here. Nothing further down the write path has any reason to object to an
// Iceberg int or long column; the problem is that the Go type on the left does
// not pin which one you get, or does not survive the canonical encoding's value
// contract intact.

// plainIntDecl uses a plain int, whose width is the build's word size.
type plainIntDecl struct {
	Count int `parquet:"name=count, fieldid=1" arrow:"count"`
}

// plainUintDecl is the unsigned form of the same hazard.
type plainUintDecl struct {
	Count uint `parquet:"name=count, fieldid=1" arrow:"count"`
}

// uint32Decl is fixed-width, but its values do not fit the int32 the canonical
// encoding carries an Iceberg int column as.
type uint32Decl struct {
	Count uint32 `parquet:"name=count, fieldid=1" arrow:"count"`
}

// int16Decl stands for the narrow signed integers, which derive to an Iceberg
// int column the canonical encoding carries as an int32.
type int16Decl struct {
	Count int16 `parquet:"name=count, fieldid=1" arrow:"count"`
}

// pointerIntDecl is the optional form of plainIntDecl: unwrapping the pointer
// must not lose the refusal.
type pointerIntDecl struct {
	Count *int `parquet:"name=count, fieldid=1" arrow:"count"`
}

// -- Name validation (scenario 7, "Namespace/table name validation").

func TestValidateNameRejects(t *testing.T) {
	rejected := []struct {
		name  string
		value string
	}{
		// The load-bearing one: a slash silently creates a third path segment
		// and makes the table invisible to rebuild's two-level discovery.
		{"slash", "fills/inner"},
		{"dot", "fills.inner"},
		{"hyphen", "event-archive"},
		{"uppercase", "Fills"},
		{"leading underscore", "_fills"},
		{"empty", ""},
		{"over 63 bytes", strings.Repeat("a", 64)},
		{"space", "event archive"},
		{"leading dot", ".fills"},
	}

	for _, kind := range []errdef.NameKind{errdef.NameKindNamespace, errdef.NameKindTable} {
		for _, r := range rejected {
			t.Run(string(kind)+"/"+r.name, func(t *testing.T) {
				err := ValidateName(kind, r.value)
				if err == nil {
					t.Fatalf("ValidateName(%s, %q) accepted it, want a refusal", kind, r.value)
				}

				var ne errdef.NameError
				if !errors.As(err, &ne) {
					t.Fatalf("error is %T, want errdef.NameError", err)
				}
				if ne.Kind != kind {
					t.Errorf("NameError.Kind = %q, want %q", ne.Kind, kind)
				}
				if ne.Value != r.value {
					t.Errorf("NameError.Value = %q, want the caller's own value %q", ne.Value, r.value)
				}
			})
		}
	}
}

func TestValidateNameAccepts(t *testing.T) {
	accepted := []string{
		"fills",
		"event_archive",
		"a",
		"0",
		"table_2",
		strings.Repeat("a", 63),
	}

	for _, v := range accepted {
		if err := ValidateName(errdef.NameKindTable, v); err != nil {
			t.Errorf("ValidateName(table, %q) = %v, want nil", v, err)
		}
	}
}

// TestDeclareValidatesNamesFirst proves the name check runs before anything
// else in the declaration path, and reports which of the two names was bad.
func TestDeclareValidatesNamesFirst(t *testing.T) {
	cases := []struct {
		name      string
		namespace string
		table     string
		wantKind  errdef.NameKind
		wantValue string
	}{
		{"bad namespace", "Trading", "fills", errdef.NameKindNamespace, "Trading"},
		{"bad table", "trading", "fills/inner", errdef.NameKindTable, "fills/inner"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Declare[caseAFill](c.namespace, c.table)
			if err == nil {
				t.Fatal("Declare accepted an invalid name, want a refusal")
			}

			var ne errdef.NameError
			if !errors.As(err, &ne) {
				t.Fatalf("error is %T, want errdef.NameError", err)
			}
			if ne.Kind != c.wantKind {
				t.Errorf("NameError.Kind = %q, want %q", ne.Kind, c.wantKind)
			}
			if ne.Value != c.wantValue {
				t.Errorf("NameError.Value = %q, want %q", ne.Value, c.wantValue)
			}
		})
	}
}

// -- Declarations icelake cannot derive a schema from at all.

// TestDeclareRejectsUnusableDeclarations covers the refusals that happen
// before the two derivations can even be compared. Each one has to be typed
// and matchable rather than a bare formatted string, because a fail-loudly
// promise that can only be checked by grepping a message is a promise no test
// can hold. The Kind field is what lets these be told apart without doing
// exactly that.
func TestDeclareRejectsUnusableDeclarations(t *testing.T) {
	cases := []struct {
		name      string
		declare   func(namespace, table string) (*Declaration, error)
		wantKind  errdef.DeclarationKind
		wantField string
		wantCause bool
	}{
		{
			name:     "a declaration type that is not a struct",
			declare:  Declare[int],
			wantKind: errdef.DeclarationKindNotStruct,
		},
		{
			// Without this, Declare happily returns an empty Iceberg schema
			// and the table is created with no columns and no error.
			name:     "a struct with no fields",
			declare:  Declare[emptyDecl],
			wantKind: errdef.DeclarationKindNoFields,
		},
		{
			name:      "an unexported field",
			declare:   Declare[unexportedFieldDecl],
			wantKind:  errdef.DeclarationKindUnexportedField,
			wantField: "secret",
		},
		{
			name:      "a logical type the tag parser does not know",
			declare:   Declare[badLogicalDecl],
			wantKind:  errdef.DeclarationKindDerivation,
			wantCause: true,
		},
		{
			name:      "a fieldid that is not a number",
			declare:   Declare[badFieldIDTagDecl],
			wantKind:  errdef.DeclarationKindDerivation,
			wantCause: true,
		},
		{
			name:      "an unknown arrow tag option",
			declare:   Declare[badArrowOptionDecl],
			wantKind:  errdef.DeclarationKindDerivation,
			wantCause: true,
		},
		{
			// The third derivation call, and the only refusal that lives there.
			// The first two calls accept this declaration: parquet builds a
			// 40-digit decimal and arrow-go widens it to decimal256 without
			// complaint. Iceberg's decimal stops at 38 digits, so the
			// conversion to an Iceberg schema is the one place a caller asking
			// for a wider one is told. Without it the failure would surface as
			// whatever the catalog said about a type it had never been given.
			name:      "a decimal wider than Iceberg's 38-digit ceiling",
			declare:   Declare[wideDecimalDecl],
			wantKind:  errdef.DeclarationKindDerivation,
			wantCause: true,
		},
		{
			// The load-bearing one. A plain int derives cleanly, and to a
			// different Iceberg type depending on the word size of the build:
			// long on a 64-bit target, int on a 32-bit one. Nothing downstream
			// objects, because both are valid columns — so the same source
			// compiled twice would produce two different schema fingerprints
			// and two different batch keys for identical data. It has to be
			// refused here or it is not refused anywhere.
			name:      "a plain int, whose width is the platform's",
			declare:   Declare[plainIntDecl],
			wantKind:  errdef.DeclarationKindUnsupportedGoType,
			wantField: "Count",
		},
		{
			name:      "a plain uint, with the same hazard",
			declare:   Declare[plainUintDecl],
			wantKind:  errdef.DeclarationKindUnsupportedGoType,
			wantField: "Count",
		},
		{
			// Fixed width, so no platform hazard — but the canonical encoding
			// carries a uint32 column as an int32, so every value above the
			// signed ceiling would wrap silently once rows were staged.
			name:      "a uint32, which the canonical encoding cannot carry",
			declare:   Declare[uint32Decl],
			wantKind:  errdef.DeclarationKindUnsupportedGoType,
			wantField: "Count",
		},
		{
			name:      "a narrow signed integer",
			declare:   Declare[int16Decl],
			wantKind:  errdef.DeclarationKindUnsupportedGoType,
			wantField: "Count",
		},
		{
			// The optional form of the same mistake, which pointer-unwrapping
			// must not let through.
			name:      "a pointer to a plain int",
			declare:   Declare[pointerIntDecl],
			wantKind:  errdef.DeclarationKindUnsupportedGoType,
			wantField: "Count",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.declare("trading", "unusable")
			if err == nil {
				t.Fatal("Declare accepted an unusable declaration, want a refusal")
			}

			var de errdef.DeclarationError
			if !errors.As(err, &de) {
				t.Fatalf("error is %T, want errdef.DeclarationError", err)
			}
			if de.Table != "unusable" {
				t.Errorf("DeclarationError.Table = %q, want %q", de.Table, "unusable")
			}
			if de.Kind != c.wantKind {
				t.Errorf("DeclarationError.Kind = %q, want %q", de.Kind, c.wantKind)
			}
			if de.Field != c.wantField {
				t.Errorf("DeclarationError.Field = %q, want %q", de.Field, c.wantField)
			}
			// A derivation failure must keep the library's own error
			// reachable; a structural refusal has no library error to keep.
			if got := errors.Unwrap(de); (got != nil) != c.wantCause {
				t.Errorf("unwrapped cause = %v, want a cause: %t", got, c.wantCause)
			}
		})
	}
}

// -- Nested-struct rejection (scenario 7, "Nested struct rejection").

func TestDeclareRejectsNestedStruct(t *testing.T) {
	_, err := Declare[nestedDecl]("trading", "nested")
	if err == nil {
		t.Fatal("Declare accepted a nested declaration, want a refusal")
	}

	var ce errdef.CrossCheckError
	if !errors.As(err, &ce) {
		t.Fatalf("error is %T, want errdef.CrossCheckError", err)
	}
	if ce.Table != "nested" {
		t.Errorf("CrossCheckError.Table = %q, want %q", ce.Table, "nested")
	}
	if ce.Column != "grp" {
		t.Errorf("CrossCheckError.Column = %q, want the nested column %q", ce.Column, "grp")
	}
}

// TestDeclareRejectsListOfGroups covers the other half of SCHEMA.md's nesting
// paragraph. The refusal it asserts is the same one the test above asserts, and
// deliberately so: rejectNested rejects every nested top-level column, so calls
// one and two of the chain succeed for a slice of structs and rejectNested then
// refuses it with a CrossCheckError naming the list column, exactly as it does
// for a plain sub-struct — a LIST where that case has a STRUCT.
//
// What makes this a second claim rather than a second copy is what each
// declaration would do if that check were lifted. A plain sub-struct derives
// through the whole three-call chain intact, so unblocking nested evolution
// would simply support it. A slice of structs would fail the third call anyway,
// because arrow-go gives the list's synthesized element node no field ID and no
// tag supplies one — a fieldid on the slice field names the list, not its
// element. That is the difference in lifetime SCHEMA.md is recording, and it is
// why rejectNested's own comment says this shape is caught there "or, if it
// survives this far, by the Arrow-to-Iceberg call".
func TestDeclareRejectsListOfGroups(t *testing.T) {
	_, err := Declare[listOfGroupsDecl]("trading", "listed")
	if err == nil {
		t.Fatal("Declare accepted a list-of-groups declaration, want a refusal")
	}

	var ce errdef.CrossCheckError
	if !errors.As(err, &ce) {
		t.Fatalf("error is %T, want errdef.CrossCheckError", err)
	}
	if ce.Table != "listed" {
		t.Errorf("CrossCheckError.Table = %q, want %q", ce.Table, "listed")
	}
	if ce.Column != "rows" {
		t.Errorf("CrossCheckError.Column = %q, want the list column %q", ce.Column, "rows")
	}
}

// -- The decimal precision ceiling (SCHEMA.md, "Column types: money, time, and
// raw payloads").

// TestDeclarePinsTheDecimalPrecisionCeiling asserts both sides of the ceiling,
// which is the only way to assert a ceiling at all: a test that only checked a
// refusal would still pass if the cap moved down and took the declared example
// tables with it, and a test that only checked an acceptance would still pass if
// the cap vanished entirely.
//
// The cap is worth holding because it is invisible everywhere else. It comes out
// of arrow-go's own applicability check — precision 1-9 on INT32, 1-18 on INT64,
// more only on a fixed-length byte array — and it applies to the *declared*
// schema, which is INT64-backed. The files icelake actually writes are built
// from Arrow Decimal128 columns and would carry a 19-digit decimal without
// complaint, so nothing downstream of this seam has any reason to object. A
// dependency upgrade that moved this check in either direction would change what
// icelake's callers may declare, and the declaration is the only place it shows.
//
// The int32 case is the one worth reading twice, and it is why this test is
// written against icelake's declarable range rather than against arrow-go's
// table. arrow-go accepts precision 9 on an INT32 column, and icelake refuses it
// anyway — one seam later, at the cross-check, because the column adapter
// whitelists int64 to decimal128 and nothing else. So the INT32 arm of that
// applicability check is unreachable through this path at any precision, and
// icelake's real rule is narrower than the library's: a declarable decimal is
// INT64-backed, precision 1-18.
func TestDeclarePinsTheDecimalPrecisionCeiling(t *testing.T) {
	t.Run("the last precision an int64 column allows", func(t *testing.T) {
		d, err := Declare[decimal18Decl]("trading", "priced")
		if err != nil {
			t.Fatalf("Declare refused a precision inside the ceiling: %v", err)
		}
		want := iceberg.DecimalTypeOf(18, 9)
		if got := d.IcebergSchema().Field(1).Type; !got.Equals(want) {
			t.Errorf("declared column type = %s, want %s", got, want)
		}
	})

	t.Run("one digit past the int64 ceiling", func(t *testing.T) {
		_, err := Declare[decimal19Decl]("trading", "priced")
		if err == nil {
			t.Fatal("Declare accepted a precision past the ceiling, want a refusal")
		}

		var de errdef.DeclarationError
		if !errors.As(err, &de) {
			t.Fatalf("error is %T, want errdef.DeclarationError", err)
		}
		// The refusal comes out of the first of the three derivation calls, so
		// it is a derivation failure carrying the library's own error rather
		// than one of icelake's structural kinds.
		if de.Kind != errdef.DeclarationKindDerivation {
			t.Errorf("DeclarationError.Kind = %q, want %q", de.Kind, errdef.DeclarationKindDerivation)
		}
		if errors.Unwrap(de) == nil {
			t.Error("DeclarationError carries no cause; a derivation failure must keep the library's own error reachable")
		}
	})

	t.Run("an int32-backed decimal arrow-go would allow", func(t *testing.T) {
		_, err := Declare[decimal9Int32Decl]("trading", "priced")
		if err == nil {
			t.Fatal("Declare accepted an int32-backed decimal, want a refusal")
		}

		// Not a derivation failure: this one gets past arrow-go's applicability
		// check and is stopped by icelake's own cross-check, because the column
		// adapter converts int64 to decimal128 and nothing else.
		var ce errdef.CrossCheckError
		if !errors.As(err, &ce) {
			t.Fatalf("error is %T, want errdef.CrossCheckError", err)
		}
		if ce.Column != "price" {
			t.Errorf("CrossCheckError.Column = %q, want %q", ce.Column, "price")
		}
	})
}

// -- Pre-order field IDs (SCHEMA.md, "Field IDs at creation").

func TestDeclareRejectsFieldIDViolations(t *testing.T) {
	t.Run("gapped numbering", func(t *testing.T) {
		_, err := Declare[gappedIDsDecl]("trading", "gapped")
		if err == nil {
			t.Fatal("Declare accepted a gapped field-ID numbering, want a refusal")
		}

		var fe errdef.FieldIDError
		if !errors.As(err, &fe) {
			t.Fatalf("error is %T, want errdef.FieldIDError", err)
		}
		// The first field is 1 and passes; the second declares 5 where the
		// pre-order traversal is at 2.
		if fe.Field != "b" {
			t.Errorf("FieldIDError.Field = %q, want %q", fe.Field, "b")
		}
		if fe.FieldID != 5 {
			t.Errorf("FieldIDError.FieldID = %d, want the declared 5", fe.FieldID)
		}
		if fe.WantID != 2 {
			t.Errorf("FieldIDError.WantID = %d, want the pre-order position 2", fe.WantID)
		}
		if fe.Table != "gapped" {
			t.Errorf("FieldIDError.Table = %q, want %q", fe.Table, "gapped")
		}
	})

	t.Run("missing fieldid tag", func(t *testing.T) {
		_, err := Declare[missingIDDecl]("trading", "missing")
		if err == nil {
			t.Fatal("Declare accepted a field with no fieldid tag, want a refusal")
		}

		var fe errdef.FieldIDError
		if !errors.As(err, &fe) {
			t.Fatalf("error is %T, want errdef.FieldIDError", err)
		}
		if fe.Field != "b" {
			t.Errorf("FieldIDError.Field = %q, want the untagged field %q", fe.Field, "b")
		}
		// arrow-go numbers an untagged field -1; that is what a missing tag
		// looks like, and naming the field is the whole reason this check
		// exists on top of the library's own blanket refusal.
		if fe.FieldID != -1 {
			t.Errorf("FieldIDError.FieldID = %d, want -1 for an absent tag", fe.FieldID)
		}
		if fe.WantID != 2 {
			t.Errorf("FieldIDError.WantID = %d, want the pre-order position 2", fe.WantID)
		}
	})
}

// -- The two-derivation cross-check (scenario 7, "Two-derivation cross-check").

func TestDeclareRejectsCrossCheckDisagreements(t *testing.T) {
	cases := []struct {
		name       string
		declare    func(namespace, table string) (*Declaration, error)
		wantColumn string
	}{
		{
			name:       "the two tag namespaces name the column differently",
			declare:    Declare[mismatchedNamesDecl],
			wantColumn: "b",
		},
		{
			name:       "the row-data side skips a column the declared side keeps",
			declare:    Declare[skippedColumnDecl],
			wantColumn: "c",
		},
		{
			name:       "a bare string column is binary on one side and utf8 on the other",
			declare:    Declare[bareStringDecl],
			wantColumn: "b",
		},
		{
			name:       "a timestamp outside the whitelisted millisecond/UTC unit",
			declare:    Declare[microsTimestampDecl],
			wantColumn: "t",
		},
		// The declared side skipping a trailing column, which is the mirror of
		// the second case above and reaches the opposite length arm.
		{
			name:       "the declared side skips a trailing column the row-data side keeps",
			declare:    Declare[declaredOnlySkipDecl],
			wantColumn: "b",
		},
		// The two nullability directions. SCHEMA.md states that the two
		// derivations must agree on whether a column may hold a null, and that
		// a disagreement fails the open naming the column; these are the only
		// two ways a caller can produce one, because the row-data side reads
		// nullability off the Go type and cannot see a repetition tag at all.
		// What it costs to lose: the declared schema is what creates the
		// Iceberg column and what the canonical encoding refuses nulls against,
		// while the row-data schema is what a flush actually builds its Arrow
		// arrays at — so a column required on one side and optional on the
		// other is a table whose first null is either written into a required
		// column or rejected at flush time, long after the caller was told the
		// record had been accepted.
		{
			name:       "a non-pointer field the declared side is told is optional",
			declare:    Declare[optionalRepetitionDecl],
			wantColumn: "b",
		},
		{
			name:       "a pointer field the declared side is told is required",
			declare:    Declare[requiredRepetitionDecl],
			wantColumn: "b",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.declare("trading", "disagreeing")
			if err == nil {
				t.Fatal("Declare accepted a disagreeing declaration, want a refusal")
			}

			var ce errdef.CrossCheckError
			if !errors.As(err, &ce) {
				t.Fatalf("error is %T, want errdef.CrossCheckError", err)
			}
			if ce.Table != "disagreeing" {
				t.Errorf("CrossCheckError.Table = %q, want %q", ce.Table, "disagreeing")
			}
			if ce.Column != c.wantColumn {
				t.Errorf("CrossCheckError.Column = %q, want %q", ce.Column, c.wantColumn)
			}
		})
	}
}

// -- The example schemas, derived through the real three-call chain.

// icebergField is one expected row of a derived Iceberg schema.
type icebergField struct {
	id       int
	name     string
	typ      iceberg.Type
	required bool
}

func assertIcebergSchema(t *testing.T, got *iceberg.Schema, want []icebergField) {
	t.Helper()

	fields := got.Fields()
	if len(fields) != len(want) {
		t.Fatalf("derived %d fields, want %d:\n%v", len(fields), len(want), got)
	}
	for i, w := range want {
		f := fields[i]
		if f.ID != w.id {
			t.Errorf("field %d: ID = %d, want %d", i, f.ID, w.id)
		}
		if f.Name != w.name {
			t.Errorf("field %d: Name = %q, want %q", i, f.Name, w.name)
		}
		if !f.Type.Equals(w.typ) {
			t.Errorf("field %q: Type = %s, want %s", f.Name, f.Type, w.typ)
		}
		if f.Required != w.required {
			t.Errorf("field %q: Required = %t, want %t", f.Name, f.Required, w.required)
		}
	}
}

// TestDeclareCaseA runs TESTING.md's Case A declaration through the whole
// chain and pins the Iceberg schema it produces. The two columns worth staring
// at are price/quantity, which must land as decimal(18,9) rather than a float,
// and venue_timestamp_ms, which must land as timestamptz rather than a bare
// long that every consumer has to reinterpret.
func TestDeclareCaseA(t *testing.T) {
	d, err := Declare[caseAFill]("trading", "fills")
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}

	if d.Namespace() != "trading" || d.Table() != "fills" {
		t.Errorf("declaration identity = %q/%q, want trading/fills", d.Namespace(), d.Table())
	}

	assertIcebergSchema(t, d.IcebergSchema(), []icebergField{
		{1, "symbol", iceberg.PrimitiveTypes.String, true},
		{2, "side", iceberg.PrimitiveTypes.String, true},
		{3, "price", iceberg.DecimalTypeOf(18, 9), true},
		{4, "quantity", iceberg.DecimalTypeOf(18, 9), true},
		{5, "sequence_id", iceberg.PrimitiveTypes.Int64, true},
		{6, "order_id", iceberg.PrimitiveTypes.String, true},
		{7, "source", iceberg.PrimitiveTypes.String, true},
		{8, "venue_timestamp_ms", iceberg.PrimitiveTypes.TimestampTz, true},
	})

	// The declared Arrow schema is what record batches are built with, and it
	// is where the two adapted column types actually live.
	wantArrow := map[string]arrow.DataType{
		"price":              &arrow.Decimal128Type{Precision: 18, Scale: 9},
		"quantity":           &arrow.Decimal128Type{Precision: 18, Scale: 9},
		"venue_timestamp_ms": &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"},
		"sequence_id":        arrow.PrimitiveTypes.Int64,
		"symbol":             arrow.BinaryTypes.String,
	}
	for name, want := range wantArrow {
		idx := d.ArrowSchema().FieldIndices(name)
		if len(idx) != 1 {
			t.Fatalf("arrow schema has %d fields named %q, want 1", len(idx), name)
		}
		if got := d.ArrowSchema().Field(idx[0]).Type; !arrow.TypeEqual(got, want) {
			t.Errorf("arrow column %q = %s, want %s", name, got, want)
		}
	}

	// The Parquet schema is what the writer itself consumes, and it is the one
	// place the physical storage decisions are visible: the money columns must
	// really be INT64-backed DECIMAL(18,9) rather than a float that happens to
	// print the same, and the timestamp must really carry its unit and UTC
	// adjustment rather than being a bare integer downstream readers guess at.
	pq := d.ParquetSchema()
	if got := pq.NumColumns(); got != 8 {
		t.Fatalf("parquet schema has %d columns, want 8", got)
	}

	priceIdx := pq.ColumnIndexByName("price")
	if priceIdx < 0 {
		t.Fatalf("parquet schema has no column named price:\n%s", pq)
	}
	price := pq.Column(priceIdx)
	if got := price.PhysicalType(); got != parquet.Types.Int64 {
		t.Errorf("price physical type = %s, want INT64", got)
	}
	if dec, ok := price.LogicalType().(pqschema.DecimalLogicalType); !ok {
		t.Errorf("price logical type = %s, want a decimal", price.LogicalType())
	} else if dec.Precision() != 18 || dec.Scale() != 9 {
		t.Errorf("price decimal = (%d,%d), want (18,9)", dec.Precision(), dec.Scale())
	}

	tsIdx := pq.ColumnIndexByName("venue_timestamp_ms")
	if tsIdx < 0 {
		t.Fatalf("parquet schema has no column named venue_timestamp_ms:\n%s", pq)
	}
	ts, ok := pq.Column(tsIdx).LogicalType().(pqschema.TimestampLogicalType)
	if !ok {
		t.Fatalf("venue_timestamp_ms logical type = %s, want a timestamp", pq.Column(tsIdx).LogicalType())
	}
	if ts.TimeUnit() != pqschema.TimeUnitMillis {
		t.Errorf("venue_timestamp_ms time unit = %v, want milliseconds", ts.TimeUnit())
	}
	if !ts.IsAdjustedToUTC() {
		t.Error("venue_timestamp_ms is not adjusted to UTC; the column must be a real instant, not a wall-clock reading")
	}
}

// TestDeclareCaseB runs TESTING.md's Case B declaration through the whole
// chain. The payload column is the point: plain bytes with no logical
// annotation, landing as Iceberg binary rather than as a string or a JSON
// claim some future payload would violate.
func TestDeclareCaseB(t *testing.T) {
	d, err := Declare[caseBEventArchive]("archive", "event_archive")
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}

	assertIcebergSchema(t, d.IcebergSchema(), []icebergField{
		{1, "source_id", iceberg.PrimitiveTypes.String, true},
		{2, "event_kind", iceberg.PrimitiveTypes.String, true},
		{3, "sequence_ordinal", iceberg.PrimitiveTypes.Int64, true},
		{4, "observed_at_ms", iceberg.PrimitiveTypes.TimestampTz, true},
		{5, "payload", iceberg.PrimitiveTypes.Binary, true},
		{6, "payload_sha256", iceberg.PrimitiveTypes.String, true},
	})
}

// TestDeclareCaseAEvolved proves a pointer field is treated as nullable by
// both derivations independently — Optional repetition on the declared side,
// Nullable on the row-data side — so the "fields added after a table exists
// must be optional" rule survives the cross-check with no special handling,
// and the new column derives to an optional Iceberg field.
func TestDeclareCaseAEvolved(t *testing.T) {
	d, err := Declare[caseAFillEvolved]("trading", "fills")
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}

	fields := d.IcebergSchema().Fields()
	if len(fields) != 9 {
		t.Fatalf("derived %d fields, want 9:\n%v", len(fields), d.IcebergSchema())
	}

	added := fields[8]
	if added.ID != 9 || added.Name != "venue_order_type" {
		t.Fatalf("added field = %d:%s, want 9:venue_order_type", added.ID, added.Name)
	}
	if added.Required {
		t.Error("added field is required; a field added after a table exists must be optional")
	}
	if !added.Type.Equals(iceberg.PrimitiveTypes.String) {
		t.Errorf("added field type = %s, want string", added.Type)
	}
}
