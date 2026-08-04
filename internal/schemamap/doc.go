// Package schemamap derives an Iceberg schema from a table's declared shape,
// whichever of the two ways that shape was stated.
//
// [Declare] reads a caller's tagged declaration struct; [DeclareFields] takes a
// list of columns, which is what a runtime schema document parses to and what a
// spool file's persisted descriptor converts to through [DeclareFromDescriptor].
// The three are one path: only the first of the derivation's three library calls
// differs, and calls two and three are the same shared tail, so what they produce
// is not merely equivalent but the same artifact produced by the same functions.
//
// It owns that derivation chain, the namespace and table name
// rules, the startup cross-check between the two independent reflection layers
// that read the same struct (parquet/schema for the declared schema,
// arrow/array/arreflect for the row data), the whitelisted column adapter that
// bridges the two, the pre-order field-ID rule that makes a declaration's
// permanent ids the ones a catalog would assign anyway, and the storage schema
// a table's Parquet files are written with.
//
// Everything here is pure in-memory computation: no file is opened, no catalog
// is read, and no network call is made. That boundary is why the reconciliation
// loop is not in this package and should not move here. Diffing a declaration
// against a live table and issuing the resulting AddColumn, RenameColumn and
// UpdateColumn calls needs a loaded table and a catalog transaction to commit
// through, and both of those are the root package's business: the loop is
// Store.reconcileTable in the root package's catalog.go, called from openTable
// on every start, and SCHEMA.md's schema-evolution section is the long-form
// account of what it does and why. What this package contributes to it is the
// declared side of that comparison — the schema, and the permanent field ids
// the comparison is made by — which is exactly the part that can be computed
// with nothing but the caller's struct in hand.
//
// Corrected at the post-M9 completeness audit (2026-08-01), because this
// comment was wrong in two opposite directions at once and a maintainer
// running "go doc ./internal/schemamap" was being told that a shipped, tested
// loop does not exist. The opening sentence claimed this package reconciles a
// declaration against a live table, which it has never done and cannot do
// without breaking the no-IO boundary above; the paragraph that followed then
// claimed the reconciliation half "lands with the write path and is not
// present yet", which stopped being true at M4, where SCHEMA.md records the
// mechanism as confirmed and where reconcile_test.go began exercising the add,
// rename and retype cases through the public API. The middle paragraph carried
// the same mistake in miniature, claiming this package owns "the
// permanent-field-ID diff that decides which schema-evolution calls a table
// needs"; it owns the rule that fixes those ids, not the diff taken over them.
// All three were written at M1, when this was the only schema code in the
// repository and the split had not been made yet, and none of them was
// revisited when M4 made the split and put the loop where it belongs.
package schemamap
