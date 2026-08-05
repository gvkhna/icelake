package chmirror

import (
	"errors"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
)

// These are the two package-internal tests TESTING.md sanctions for this
// package, and the claim shape each holds is **pure computation**: both are
// about a string this package derives from values in memory, with no server,
// no file and no substrate anywhere near them. Everything else about the mirror
// — the DDL actually running, the reconciliation diff, the insert — is a claim
// about what a real server does and is proven end to end in scenarios 16 and 17.

// TestGeneratedDDLMatchesTheMappingTable pins the CREATE TABLE text against an
// expected string written out from SCHEMA.md's "The way out: the ClickHouse
// mirror's type mapping" table, rather than from the generator. All nine types
// appear, each in both nullabilities, so a mapping that changed for one type
// cannot hide behind the others.
func TestGeneratedDDLMatchesTheMappingTable(t *testing.T) {
	desc := describeAllNine(t)

	cols, err := columnsFor("ns", "tbl", "icelake.ns__tbl", desc)
	if err != nil {
		t.Fatalf("columnsFor: %v", err)
	}

	want := "CREATE TABLE IF NOT EXISTS `icelake`.`ns__tbl` (\n" +
		"  `r_boolean` Bool,\n" +
		"  `r_int` Int32,\n" +
		"  `r_long` Int64,\n" +
		"  `r_float` Float32,\n" +
		"  `r_double` Float64,\n" +
		"  `r_decimal` Decimal(18, 9),\n" +
		"  `r_timestamptz` DateTime64(6, 'UTC'),\n" +
		"  `r_string` String,\n" +
		"  `r_binary` String,\n" +
		"  `o_boolean` Nullable(Bool),\n" +
		"  `o_int` Nullable(Int32),\n" +
		"  `o_long` Nullable(Int64),\n" +
		"  `o_float` Nullable(Float32),\n" +
		"  `o_double` Nullable(Float64),\n" +
		"  `o_decimal` Nullable(Decimal(18, 9)),\n" +
		"  `o_timestamptz` Nullable(DateTime64(6, 'UTC')),\n" +
		"  `o_string` Nullable(String),\n" +
		"  `o_binary` Nullable(String),\n" +
		"  `_batch_key` String\n" +
		") ENGINE = MergeTree\n" +
		"ORDER BY tuple()\n" +
		"SETTINGS non_replicated_deduplication_window = 1000"

	if got := createTable("icelake", "ns__tbl", cols); got != want {
		t.Errorf("the generated DDL is\n%s\n\nand SCHEMA.md's mapping gives\n%s", got, want)
	}
}

// TestTableNameRefusesWhatItCannotMapBack is the naming half: the pairs that
// map, and the pairs that would collide.
func TestTableNameRefusesWhatItCannotMapBack(t *testing.T) {
	if got, err := TableName("market", "fills"); err != nil || got != "market__fills" {
		t.Errorf("TableName(market, fills) = %q, %v; want market__fills", got, err)
	}
	// A single underscore is legal on both sides and must keep mapping, because
	// it is what most real names look like.
	if got, err := TableName("my_service", "raw_events"); err != nil || got != "my_service__raw_events" {
		t.Errorf("TableName(my_service, raw_events) = %q, %v; want my_service__raw_events", got, err)
	}

	// The collision the separator buys, refused on both sides of it: these two
	// pairs would otherwise want the same ClickHouse table.
	for _, tc := range [][2]string{{"a__b", "c"}, {"a", "b__c"}} {
		_, err := TableName(tc[0], tc[1])
		var typed errdef.ClickHouseError
		if !errors.As(err, &typed) || typed.Kind != errdef.ClickHouseKindConflict {
			t.Errorf("TableName(%q, %q) returned %v, want a conflict", tc[0], tc[1], err)
		}
	}
}

// TestABatchKeyColumnInTheDeclarationIsRefused covers the one name the mirror
// adds: a declaration already using it would produce a CREATE naming one column
// twice.
func TestABatchKeyColumnInTheDeclarationIsRefused(t *testing.T) {
	desc, err := canon.FromFields("tbl", []iceberg.NestedField{
		{ID: 1, Name: BatchKeyColumn, Type: iceberg.PrimitiveTypes.String, Required: true},
	})
	if err != nil {
		t.Fatalf("FromFields: %v", err)
	}

	_, err = columnsFor("ns", "tbl", "icelake.ns__tbl", desc)
	var typed errdef.ClickHouseError
	if !errors.As(err, &typed) || typed.Kind != errdef.ClickHouseKindConflict || typed.Column != BatchKeyColumn {
		t.Errorf("a declaration carrying a %s column returned %v, want a conflict naming it", BatchKeyColumn, err)
	}
}

// describeAllNine builds a descriptor covering every declared type twice, once
// required and once optional.
func describeAllNine(tb testing.TB) *canon.Descriptor {
	tb.Helper()

	types := []struct {
		name string
		typ  iceberg.Type
	}{
		{"boolean", iceberg.PrimitiveTypes.Bool},
		{"int", iceberg.PrimitiveTypes.Int32},
		{"long", iceberg.PrimitiveTypes.Int64},
		{"float", iceberg.PrimitiveTypes.Float32},
		{"double", iceberg.PrimitiveTypes.Float64},
		{"decimal", iceberg.DecimalTypeOf(18, 9)},
		{"timestamptz", iceberg.PrimitiveTypes.TimestampTz},
		{"string", iceberg.PrimitiveTypes.String},
		{"binary", iceberg.PrimitiveTypes.Binary},
	}

	var fields []iceberg.NestedField
	id := 1
	for _, t := range types {
		fields = append(fields, iceberg.NestedField{ID: id, Name: "r_" + t.name, Type: t.typ, Required: true})
		id++
	}
	for _, t := range types {
		fields = append(fields, iceberg.NestedField{ID: id, Name: "o_" + t.name, Type: t.typ, Required: false})
		id++
	}

	desc, err := canon.FromFields("tbl", fields)
	if err != nil {
		tb.Fatalf("FromFields: %v", err)
	}

	return desc
}
