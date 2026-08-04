package icelake

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	// The S3 file IO iceberg-go itself uses to read and write a table's own
	// metadata and manifests. It registers itself for the s3 scheme on import;
	// there is no other seam through which a table living in a bucket can be
	// read or committed. See ARCHITECTURE.md's catalog-design section for what
	// this import costs and why it is nonetheless the chosen route.
	_ "github.com/apache/iceberg-go/io/gocloud"

	"github.com/gvkhna/icelake/internal/catalogdb"
	"github.com/gvkhna/icelake/internal/errdef"
	"github.com/gvkhna/icelake/internal/schemamap"
)

// forceVirtualAddressingKey is the one storage property icelake records on a
// table it creates. It lives in internal/catalogdb because catalog rebuild sets
// the same property on the catalog it reconstructs.
const forceVirtualAddressingKey = catalogdb.ForceVirtualAddressingKey

// tableLocation is where one table's own metadata/ and data/ directories live:
// <bucket>/<warehouse prefix>/<namespace>/<table>. Exactly two segments sit
// between the prefix and the table, because catalog rebuild discovers what
// exists by listing those two levels and nothing else tells it what should.
func (s *Store) tableLocation(namespace, name string) string {
	return "s3://" + path.Join(s.cfg.Bucket, s.cfg.WarehousePrefix, namespace, name)
}

// openTable runs one table's whole startup lifecycle: create it if it does not
// exist, reconcile it against the declaration if it does, and in both cases
// return only a table whose live shape has been verified to match what the
// declaration says.
//
// It runs unconditionally on every start. There is no migration log to consult
// because there does not need to be one: every column's identity is a permanent
// number, so one direct comparison of "what the struct says now" against "what
// the table is now" always yields the complete set of changes, however many
// versions ago they were introduced.
func (s *Store) openTable(ctx context.Context, decl *schemamap.Declaration) (*table.Table, error) {
	ident := table.Identifier{decl.Namespace(), decl.Table()}

	tbl, err := s.catalog.LoadTable(ctx, ident)
	switch {
	case err == nil:
		tbl, err = s.reconcileTable(ctx, tbl, decl)
	case errors.Is(err, catalog.ErrNoSuchTable):
		tbl, err = s.createTable(ctx, ident, decl)
	default:
		return nil, errdef.NewReconcileError(decl.Table(), "loading the table", err)
	}
	if err != nil {
		return nil, err
	}

	if err := verifySchema(decl, tbl.Schema()); err != nil {
		return nil, err
	}

	return tbl, nil
}

// createTable creates the namespace if it is missing and then the table.
//
// Nothing is compared on this path: there is nothing yet to compare against, so
// every column can be required from day one if the domain calls for it. The
// table is created unpartitioned, unsorted and at format version 2 — which is
// what passing neither a partition spec nor a sort order produces, and is
// recorded as a decision rather than left to a library default. Unpartitioned
// is deliberate: a partition spec would split every batch into one data file
// per partition value it touches, multiplying small files, which is the exact
// anti-pattern this library exists to end.
func (s *Store) createTable(ctx context.Context, ident table.Identifier, decl *schemamap.Declaration) (*table.Table, error) {
	exists, err := s.catalog.CheckNamespaceExists(ctx, table.Identifier{decl.Namespace()})
	if err != nil {
		return nil, errdef.NewReconcileError(decl.Table(), "checking whether the namespace exists", err)
	}
	if !exists {
		if err := s.catalog.CreateNamespace(ctx, table.Identifier{decl.Namespace()}, nil); err != nil &&
			!errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			return nil, errdef.NewReconcileError(decl.Table(), "creating the namespace", err)
		}
	}

	tbl, err := s.catalog.CreateTable(ctx, ident, decl.IcebergSchema(),
		catalog.WithLocation(s.tableLocation(decl.Namespace(), decl.Table())),
		catalog.WithProperties(iceberg.Properties{forceVirtualAddressingKey: "false"}))
	if err != nil {
		return nil, errdef.NewReconcileError(decl.Table(), "creating the table", err)
	}

	return tbl, nil
}

// reconcileTable diffs the declared shape against the live one by permanent
// field id and applies the difference.
//
// The three cases are the whole of schema evolution as this design supports it.
// A declared field the table does not have is added, and must be optional,
// because there is no value to give the rows already written. A field whose
// name changed is renamed, which is only recognizable at all because identity
// is the id rather than the name. A field whose type changed is retyped, and
// iceberg-go itself refuses the unsafe direction — icelake reimplements none of
// that, it only has to keep passing allowIncompatibleChanges as false.
//
// Nothing walks the live schema looking for a column the declaration dropped.
// A field missing from the declaration is left alone, always: dropping a column
// must be a separate, explicit act, never something inferred from an omission,
// because an accidentally-omitted field is a far more dangerous mistake than a
// deliberately-kept column.
func (s *Store) reconcileTable(ctx context.Context, tbl *table.Table, decl *schemamap.Declaration) (*table.Table, error) {
	live := tbl.Schema()
	txn := tbl.NewTransaction()
	update := txn.UpdateSchema(true, false)

	changes := 0
	for _, want := range decl.IcebergSchema().Fields() {
		have, found := live.FindFieldByID(want.ID)
		switch {
		case !found:
			if want.Required {
				return nil, errdef.NewReconcileError(decl.Table(), fmt.Sprintf(
					"field id %d (%q) is new but required; a field added after a table exists must be optional, because there is no value to give the rows already written",
					want.ID, want.Name), nil)
			}
			update.AddColumn([]string{want.Name}, want.Type, "", false, nil)
			changes++
		case have.Name != want.Name:
			update.RenameColumn([]string{have.Name}, want.Name)
			changes++
		case !have.Type.Equals(want.Type):
			update.UpdateColumn([]string{have.Name}, table.ColumnUpdate{
				FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: want.Type},
			})
			changes++
		}
	}

	// A table created before this property existed would have its metadata
	// written through the wrong bucket addressing on its next commit, so it is
	// topped up here rather than assumed.
	if tbl.Properties().Get(forceVirtualAddressingKey, "") == "" {
		if err := txn.SetProperties(iceberg.Properties{forceVirtualAddressingKey: "false"}); err != nil {
			return nil, errdef.NewReconcileError(decl.Table(), "recording the table's storage addressing property", err)
		}
		changes++
	}

	if changes == 0 {
		return tbl, nil
	}

	// Two commits, and both are required. The first runs the queued operations,
	// validates them — this is where an unsafe change is refused — and stages
	// the result into the transaction. The second writes the new metadata and
	// updates the catalog row. Calling only the first leaves the change staged
	// and lost.
	if err := update.Commit(); err != nil {
		return nil, errdef.NewReconcileError(decl.Table(), "applying the schema difference", err)
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		return nil, errdef.NewReconcileError(decl.Table(), "committing the schema change", err)
	}

	return updated, nil
}

// verifySchema requires the live table to carry exactly the declared columns,
// by id, name and type.
//
// This is the guard against the one failure this design cannot otherwise see. A
// catalog's CreateTable renumbers a new table's fields in pre-order and does so
// silently, so a declaration whose ids disagreed would produce a table with no
// error raised whose ids the declaration can never match again — and every
// later start would try to add columns that already exist. The declaration
// check refuses that before creation; this one proves it afterwards, against
// the table the catalog actually made.
//
// It also guarantees what everything downstream assumes: the schema descriptor
// the canonical encoding is taken over, and therefore every batch key, is
// derived from the declared schema, so the declared schema being the live one
// is not a hope.
func verifySchema(decl *schemamap.Declaration, live *iceberg.Schema) error {
	for _, want := range decl.IcebergSchema().Fields() {
		have, found := live.FindFieldByID(want.ID)
		switch {
		case !found:
			return errdef.NewReconcileError(decl.Table(), fmt.Sprintf(
				"the live table has no field with id %d (%q) after reconciliation", want.ID, want.Name), nil)
		case have.Name != want.Name:
			return errdef.NewReconcileError(decl.Table(), fmt.Sprintf(
				"field id %d is named %q in the live table and %q in the declaration after reconciliation", want.ID, have.Name, want.Name), nil)
		case !have.Type.Equals(want.Type):
			return errdef.NewReconcileError(decl.Table(), fmt.Sprintf(
				"field id %d (%q) has type %s in the live table and %s in the declaration after reconciliation", want.ID, want.Name, have.Type, want.Type), nil)
		}
	}

	return nil
}
