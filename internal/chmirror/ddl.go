package chmirror

import (
	"fmt"
	"strings"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
)

// BatchKeyColumn is the one column icelake adds to a mirrored table: the
// batch's content hash, which is also the name of its Parquet object in the
// warehouse and of its file in the local spool.
//
// It is data rather than server state, and that is the point. The
// insert_deduplication_token the insert carries answers "have I seen this
// insert recently" and forgets; this answers "which batch is this row from"
// forever, from a query, which is what makes a user's own reconciliation
// against the lake possible without icelake owning one.
const BatchKeyColumn = "_batch_key"

// nameSeparator joins a namespace and a table into one mirror table name.
//
// A double underscore rather than a single one because a single one is legal
// inside both names and would collide immediately. A double one collides only
// for names that themselves contain a double underscore, which is refused by
// [TableName] rather than escaped — see the decision in ARCHITECTURE.md, and
// the rule every name check in this project follows: rewriting a caller's table
// name is how two services end up sharing a table.
const nameSeparator = "__"

// dedupWindow is the value of non_replicated_deduplication_window in the
// generated CREATE TABLE.
//
// It is not a tuning knob. Plain MergeTree has insert deduplication *off* by
// default, and this setting is what turns it on for a non-replicated table — so
// without it, insert_deduplication_token is accepted and ignored, and every
// retry and every replayed crash duplicates a batch. TESTING.md records the
// measurement on the pinned server: three identical inserts under one token
// produce three copies of the batch without this setting and one with it.
//
// 1000 is a window of inserts, not of rows, so at one insert per flush it is a
// thousand flushes of protection — far more than any retry or replay needs, and
// small enough to cost nothing.
const dedupWindow = 1000

// TableName maps an icelake namespace and table onto the mirror's table name.
//
// It refuses a name containing [nameSeparator], because the mapping would stop
// being injective: namespace "a__b" with table "c" and namespace "a" with table
// "b__c" would want the same mirror table, and two icelake tables quietly
// sharing one ClickHouse table is precisely the failure this project refuses
// everywhere else. The refusal is a conflict rather than a name error, because
// it is a statement about the mirror's own naming scheme and not about whether
// icelake will accept the table at all: the same two tables are perfectly legal
// with no mirror configured.
func TableName(namespace, table string) (string, error) {
	for _, part := range []string{namespace, table} {
		if strings.Contains(part, nameSeparator) {
			return "", errdef.ClickHouseError{
				Namespace: namespace,
				Table:     table,
				Target:    namespace + nameSeparator + table,
				Kind:      errdef.ClickHouseKindConflict,
				Err: fmt.Errorf(
					"%q contains %q, which the mirror's <namespace>%s<table> naming would not map back uniquely; rename the table or leave the mirror unconfigured",
					part, nameSeparator, nameSeparator),
			}
		}
	}

	return namespace + nameSeparator + table, nil
}

// column is one column of a mirrored table: the name it has in ClickHouse,
// which is the declared column's own name, and the ClickHouse type it was
// created with.
type column struct {
	name string
	typ  string
	// field is the declared column this one carries. It is what the insert
	// reads values by; the [BatchKeyColumn] carries a zero one, which nothing
	// reads because that column is appended from the batch key rather than from
	// the record.
	field canon.Field
}

// columnType renders one declared column's ClickHouse type, spelled the way
// system.columns spells it back so that the reconciliation diff compares two
// strings produced the same way.
//
// SCHEMA.md's "The way out: the ClickHouse mirror's type mapping" is the
// authoritative table and this is its implementation; a change to either is a
// change to both. Two entries carry an argument rather than a correspondence.
// A decimal keeps its declared precision and scale, never becoming a float,
// which is the money decision reaching the last place a value could quietly
// become binary. And a timestamp is DateTime64 at scale 6 because that is the
// scale of the *data file* — the declaration says milliseconds and FileSchema
// widens it on the way out — with the further rule, held in the insert rather
// than here, that a value reaches it as a time.Time and never as a bare integer
// ClickHouse would read as seconds.
//
// It cannot fail: a Descriptor's constructors refuse any field whose kind is
// not one of the nine and any decimal whose precision or scale is out of range,
// so every field of an existing Descriptor has a type on the other side of this
// switch. It is the same guarantee, for the same reason, that
// canon.Descriptor.IcebergFields states.
func columnType(f canon.Field) string {
	var t string
	switch f.Kind {
	case canon.KindBoolean:
		t = "Bool"
	case canon.KindInt:
		t = "Int32"
	case canon.KindLong:
		t = "Int64"
	case canon.KindFloat:
		t = "Float32"
	case canon.KindDouble:
		t = "Float64"
	case canon.KindDecimal:
		t = fmt.Sprintf("Decimal(%d, %d)", f.Precision, f.Scale)
	case canon.KindTimestampTz:
		t = "DateTime64(6, 'UTC')"
	case canon.KindString, canon.KindBinary:
		// ClickHouse's String is an arbitrary byte sequence with no encoding
		// rule attached, which is exactly what a binary column is. Deliberately
		// not FixedString, which pads, and deliberately not a base64 text
		// column, which would invent a spelling the lake does not have.
		t = "String"
	}

	// Always, with no exception, and the reason is a hazard rather than a
	// preference: input_format_null_as_default defaults to true, so a null read
	// out of a Parquet file into a non-Nullable column is silently written as
	// that type's zero value. The nulls and the zeros are indistinguishable
	// afterwards.
	if !f.Required {
		t = "Nullable(" + t + ")"
	}

	return t
}

// columnsFor derives a mirrored table's columns from a recorded schema shape,
// in ascending field-id order, with [BatchKeyColumn] last.
//
// It refuses a declaration that already has a column called [BatchKeyColumn],
// as a conflict: the alternative is a CREATE TABLE naming one column twice,
// which the server would refuse with a message about icelake's own arrangement
// rather than about the caller's declaration.
func columnsFor(namespace, table, target string, d *canon.Descriptor) ([]column, error) {
	fields := d.Fields()
	cols := make([]column, 0, len(fields)+1)
	for _, f := range fields {
		if f.Name == BatchKeyColumn {
			return nil, errdef.ClickHouseError{
				Namespace: namespace,
				Table:     table,
				Target:    target,
				Column:    f.Name,
				Kind:      errdef.ClickHouseKindConflict,
				Err: fmt.Errorf("the declaration has a column called %q, which is the name the mirror adds for the batch key; rename that column or leave the mirror unconfigured",
					BatchKeyColumn),
			}
		}
		cols = append(cols, column{name: f.Name, typ: columnType(f), field: f})
	}

	return append(cols, column{name: BatchKeyColumn, typ: "String"}), nil
}

// createDatabase renders the CREATE DATABASE statement.
func createDatabase(database string) string {
	return "CREATE DATABASE IF NOT EXISTS " + quoteIdent(database)
}

// createTable renders the CREATE TABLE statement for a mirrored table.
//
// The engine and the two clauses after it are decided in ARCHITECTURE.md and
// each is doing work. MergeTree with ORDER BY tuple() is the default rather
// than a recommendation: icelake does not know what an operator will query by,
// and a sorting key it guessed would cost merges forever — an operator who does
// know pre-creates the table with their own and keeps it, because
// reconciliation verifies columns and nothing else. The settings clause is what
// makes the batch key work at all; see [dedupWindow].
func createTable(database, table string, cols []column) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s.%s (\n", quoteIdent(database), quoteIdent(table))
	for i, c := range cols {
		if i > 0 {
			b.WriteString(",\n")
		}
		fmt.Fprintf(&b, "  %s %s", quoteIdent(c.name), c.typ)
	}
	fmt.Fprintf(&b, "\n) ENGINE = MergeTree\nORDER BY tuple()\nSETTINGS non_replicated_deduplication_window = %d", dedupWindow)

	return b.String()
}

// addColumn renders the one repair the reconciliation is allowed to make.
func addColumn(database, table string, c column) string {
	return fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s",
		quoteIdent(database), quoteIdent(table), quoteIdent(c.name), c.typ)
}

// insertStatement renders the insert's header: the target and the explicit
// column list, in the order the caller will append them.
//
// The column list is explicit and never positional or a star, which is the
// property that lets an operator add columns of their own to a mirrored table
// without every insert breaking, and that keeps the insert correct if
// ClickHouse ever reports a different column order than the one icelake created.
func insertStatement(database, table string, names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = quoteIdent(n)
	}

	return fmt.Sprintf("INSERT INTO %s.%s (%s)", quoteIdent(database), quoteIdent(table), strings.Join(quoted, ", "))
}
