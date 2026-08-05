package chmirror

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
)

// Table is one icelake table's mirror.
//
// It is owned by that table's flush worker and touched from that goroutine
// only, which is the same single-owner rule the worker's Iceberg table handle
// follows and the reason nothing here holds a lock.
type Table struct {
	conn *Conn

	namespace string
	table     string
	// name is the mirror's table name, and target is what an operator would
	// type: database.table. Both are computed once.
	name   string
	target string

	// cols are the columns the declaration wants, in ascending field-id order
	// with the batch key last. They are what the DDL is rendered from and what
	// the reconciliation diff is taken against.
	cols []column
	// byName resolves a record's column to the declared field it carries.
	byName map[string]column

	// ready is set once the database and the table have been created and
	// reconciled. Until it is, every insert tries the whole preparation again,
	// which is what makes a server that was down when the writer opened heal by
	// itself at the next flush.
	ready bool
	// stmt is the insert header for the column order this table's records
	// arrive in, built once on the first insert.
	stmt string
	// order is the record's column order the header was built for, kept so a
	// record arriving with a different shape rebuilds the header rather than
	// inserting under the wrong one. Nothing in this design can change a
	// worker's record shape mid-life; this is the second line.
	order []string

	// ttl is the declared row expiry, or nil for none. It is rendered into the
	// CREATE and reconciled onto a live table that lacks it.
	ttl *TTL
}

// NewTable prepares one table's mirror. It makes no network call: the name
// mapping and the column derivation are the two things that can be refused
// before a server is involved at all, and both are conflicts.
func NewTable(conn *Conn, namespace, table string, d *canon.Descriptor, ttl *TTL) (*Table, error) {
	name, err := TableName(namespace, table)
	if err != nil {
		return nil, err
	}
	target := conn.database + "." + name

	cols, err := columnsFor(namespace, table, target, d)
	if err != nil {
		return nil, err
	}

	if err := validateTTL(namespace, table, target, d, ttl); err != nil {
		return nil, err
	}

	byName := make(map[string]column, len(cols))
	for _, c := range cols {
		byName[c.name] = c
	}

	return &Table{
		conn:      conn,
		namespace: namespace,
		table:     table,
		name:      name,
		target:    target,
		cols:      cols,
		byName:    byName,
		ttl:       ttl,
	}, nil
}

// TTL declares how long the mirror keeps this table's raw rows: a row expires
// when the driving column's value is more than Seconds old, by the server's
// own clock, applied at its merges.
type TTL struct {
	// Column is the timestamptz column whose value drives expiry. It must be a
	// required column: the server refuses a Nullable expression in a TTL, and
	// refusing it here names the rule instead of relaying a server message.
	Column string
	// Seconds is how long past the column's value a row survives. Positive.
	Seconds int64
}

// validateTTL refuses a TTL declaration the server would refuse or misread,
// as a conflict — it is a statement about configuration, exactly like an
// unmappable name, and it is refused at open rather than discovered at the
// first flush.
func validateTTL(namespace, table, target string, d *canon.Descriptor, ttl *TTL) error {
	if ttl == nil {
		return nil
	}

	conflict := func(detail string) error {
		return errdef.ClickHouseError{
			Namespace: namespace, Table: table, Target: target, Column: ttl.Column,
			Kind: errdef.ClickHouseKindConflict,
			Err:  errors.New(detail),
		}
	}

	if ttl.Seconds <= 0 {
		return conflict("the mirror TTL must be a positive duration")
	}
	for _, f := range d.Fields() {
		if f.Name != ttl.Column {
			continue
		}
		if f.Kind != canon.KindTimestampTz {
			return conflict(fmt.Sprintf("the mirror TTL names column %q, which is not a timestamptz column; expiry is driven by a point in time", ttl.Column))
		}
		if !f.Required {
			return conflict(fmt.Sprintf("the mirror TTL names column %q, which is optional; a row with no value there would have no expiry, so the server refuses a Nullable TTL and so does this", ttl.Column))
		}

		return nil
	}

	return conflict(fmt.Sprintf("the mirror TTL names column %q, which this table does not declare", ttl.Column))
}

// Target returns the ClickHouse object this mirror writes to, as an operator
// would type it.
func (t *Table) Target() string { return t.target }

// fail wraps a driver failure into this package's typed error.
func (t *Table) fail(kind errdef.ClickHouseKind, detail string, err error) error {
	e := errdef.NewClickHouseError(kind, t.namespace, t.table, t.target, detail, err)

	return e
}

// conflict reports a live table icelake will not repair.
func (t *Table) conflict(col, detail string) error {
	return errdef.ClickHouseError{
		Namespace: t.namespace,
		Table:     t.table,
		Target:    t.target,
		Column:    col,
		Kind:      errdef.ClickHouseKindConflict,
		Err:       fmt.Errorf("%s; the mirror is reproducible from the lake, so the repair is to drop %s and let it be created again", detail, t.target),
	}
}

// Ensure creates the database and the table if they are absent, and reconciles
// a table that is present.
//
// It is idempotent and cheap enough to be called on every insert that finds the
// mirror unprepared: two IF NOT EXISTS statements and one read of
// system.columns, once per table per process on the happy path.
//
// The reconciliation is add-only by decision, not by accident. A column the
// declaration has and the server does not is added when it is optional — which
// is the only kind of column icelake's own schema evolution ever adds — and
// every other disagreement is a conflict that names the column and tells the
// operator to rebuild. That instruction is acceptable only because the thing
// being dropped is reproducible from the lake, which is the sentence this whole
// package rests on. Columns the server has and the declaration does not are
// left alone: an operator's own column is their business, and the insert names
// its own columns explicitly so it does not depend on them.
func (t *Table) Ensure(ctx context.Context) error {
	if t.ready {
		return nil
	}

	if err := t.conn.db.Exec(ctx, createDatabase(t.conn.database)); err != nil {
		return t.fail(kindFor(err), "creating the database", err)
	}
	if err := t.conn.db.Exec(ctx, createTable(t.conn.database, t.name, t.cols, t.ttl)); err != nil {
		return t.fail(kindFor(err), "creating the table", err)
	}

	live, err := t.liveColumns(ctx)
	if err != nil {
		return err
	}

	for _, c := range t.cols {
		have, ok := live[c.name]
		switch {
		case ok && have == c.typ:
		case ok:
			return t.conflict(c.name, fmt.Sprintf("column %q is %s and the declaration says %s", c.name, have, c.typ))
		// A missing column is added only when adding it cannot invent data,
		// which means only when it is Nullable. A required column that is
		// absent — including the batch key — is a table icelake did not create,
		// and filling it with a default would be inventing exactly the values
		// the rest of this library refuses to invent.
		case isNullable(c.typ):
			if err := t.conn.db.Exec(ctx, addColumn(t.conn.database, t.name, c)); err != nil {
				return t.fail(kindFor(err), "adding column "+c.name, err)
			}
		default:
			return t.conflict(c.name, fmt.Sprintf("required column %q is missing", c.name))
		}
	}

	if err := t.ensureTTL(ctx); err != nil {
		return err
	}

	t.ready = true

	return nil
}

// ensureTTL reconciles a live table's row expiry with the declared one.
//
// Unlike the column reconciliation, which only adds, this one overwrites: a
// TTL is mirror tuning rather than data shape, changing it invents no values,
// and the declaration is the operator's own statement of what they want — so a
// live table whose expiry disagrees is moved to the declared one rather than
// refused. A table with no TTL declared is left exactly as found, whatever
// expiry an operator gave it themselves.
func (t *Table) ensureTTL(ctx context.Context) error {
	if t.ttl == nil {
		return nil
	}

	// engine_full is the server's own rendering of the table's engine clause,
	// TTL included. The check is for the expression this package renders —
	// which the server normalizes to the same shape — and the repair when it
	// is absent is MODIFY TTL with that same expression, which is idempotent.
	// Reading first is what keeps the repair from running on every open: a
	// MODIFY TTL can rewrite parts, and rewriting parts to change nothing is
	// work a healthy open has no business doing.
	var engineFull string
	row := t.conn.db.QueryRow(ctx,
		"SELECT engine_full FROM system.tables WHERE database = ? AND name = ?", t.conn.database, t.name)
	if err := row.Scan(&engineFull); err != nil {
		return t.fail(kindFor(err), "reading the table's engine clause", err)
	}

	want := ttlExpr(t.ttl)
	if strings.Contains(engineFull, "TTL "+want) || strings.Contains(engineFull, "TTL "+stripIdentQuotes(want)) {
		return nil
	}
	if err := t.conn.db.Exec(ctx, modifyTTL(t.conn.database, t.name, t.ttl)); err != nil {
		return t.fail(kindFor(err), "setting the table's TTL", err)
	}

	return nil
}

// liveColumns reads the table's current columns from the server's own catalog.
//
// This is the one genuine query in the mirror, it is parameterised, and it
// reads a schema this repository does not own — which is the same reason
// internal/catalogdb's one statement is a sanctioned exception to the sqlc rule
// (AGENTS.md, "Database Access").
func (t *Table) liveColumns(ctx context.Context) (map[string]string, error) {
	rows, err := t.conn.db.Query(ctx,
		"SELECT name, type FROM system.columns WHERE database = ? AND table = ?", t.conn.database, t.name)
	if err != nil {
		return nil, t.fail(kindFor(err), "reading the table's columns", err)
	}
	defer func() { _ = rows.Close() }()

	live := make(map[string]string)
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, t.fail(errdef.ClickHouseKindSchema, "reading the table's columns", err)
		}
		live[name] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, t.fail(kindFor(err), "reading the table's columns", err)
	}

	return live, nil
}

// Insert writes one batch into the mirror.
//
// The record is the one the batch's Parquet data file was written from, still
// alive, so the values here are the same in-memory columns Parquet got rather
// than a second derivation of them. That identity is the design's spine and is
// what TESTING.md's scenario 17 proves.
//
// The batch key travels twice, deliberately. As insert_deduplication_token it
// makes the insert idempotent: a retry, or a replay after a crash between this
// call and the catalog commit, re-sends the identical batch under the identical
// token and the server collapses it. As the [BatchKeyColumn] value it stays in
// the data, where a user's own query can join the mirror back to the objects in
// the warehouse. Neither one replaces the other: the token has a window and
// forgets, and the column does not.
func (t *Table) Insert(ctx context.Context, batchKey string, rec arrow.RecordBatch) error {
	if err := t.Ensure(ctx); err != nil {
		return err
	}

	fields := rec.Schema().Fields()
	names := make([]string, 0, len(fields)+1)
	for _, f := range fields {
		names = append(names, f.Name)
	}
	names = append(names, BatchKeyColumn)

	if t.stmt == "" || !sameOrder(t.order, names) {
		t.stmt, t.order = insertStatement(t.conn.database, t.name, names), names
	}

	rows := int(rec.NumRows())
	// The token is a query setting rather than part of the statement, which is
	// the driver's own mechanism for it and keeps the batch key out of the SQL
	// text entirely.
	ictx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplication_token": batchKey,
	}))

	batch, err := t.conn.db.PrepareBatch(ictx, t.stmt)
	if err != nil {
		return t.insertFail(batchKey, rows, kindFor(err), "preparing the batch", err)
	}
	// A prepared batch holds a pooled connection until Send or Abort releases
	// it. The early returns below — a column the declaration does not know, an
	// append the driver refused — would otherwise leak that connection for
	// good, and the pool is ten deep: ten such failures and every later
	// PrepareBatch blocks for the mirror's full bounded wait. Abort after a
	// path that already released is answered with a sentinel and ignored.
	sent := false
	defer func() {
		if !sent {
			_ = batch.Abort()
		}
	}()

	for i, f := range fields {
		c, ok := t.byName[f.Name]
		if !ok {
			return t.insertFail(batchKey, rows, errdef.ClickHouseKindConflict, "record column "+f.Name+" is not in the declaration", nil)
		}
		if err := appendColumn(batch.Column(i), c.field, rec.Column(i)); err != nil {
			return t.insertFail(batchKey, rows, errdef.ClickHouseKindInsert, "appending column "+f.Name, err)
		}
	}

	keys := make([]string, rows)
	for i := range keys {
		keys[i] = batchKey
	}
	if err := batch.Column(len(fields)).Append(keys); err != nil {
		return t.insertFail(batchKey, rows, errdef.ClickHouseKindInsert, "appending "+BatchKeyColumn, err)
	}

	// Send releases the connection whether it succeeds or fails, so the
	// deferred Abort stands down from here on.
	sent = true
	if err := batch.Send(); err != nil {
		return t.insertFail(batchKey, rows, kindFor(err), "sending the batch", err)
	}

	return nil
}

// insertFail wraps a failure that concerns one batch, carrying which batch it
// was and how many records went with it.
func (t *Table) insertFail(batchKey string, records int, kind errdef.ClickHouseKind, detail string, err error) error {
	e := errdef.NewClickHouseError(kind, t.namespace, t.table, t.target, detail, err)
	e.BatchKey, e.Records = batchKey, records

	return e
}

// kindFor decides whether a driver failure is about reaching the server or
// about what the server said.
//
// The distinction matters to an operator and to nobody else — both are reported
// the same way and both heal the same way — so it is drawn on the one signal
// that is reliable without parsing a message: a failure the driver reports
// before the server has answered anything carries no server exception, and one
// the server produced does. Anything unrecognised is reported as a connection
// failure, which is the honest default for "the mirror could not do its work
// and the reason is not a statement the server made".
func kindFor(err error) errdef.ClickHouseKind {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		return errdef.ClickHouseKindSchema
	}

	return errdef.ClickHouseKindConnect
}

// isNullable reports whether a rendered ClickHouse type is a Nullable one,
// which is exactly the set of columns the reconciliation may add.
func isNullable(t string) bool { return len(t) > 9 && t[:9] == "Nullable(" }

// sameOrder reports whether two column orders are identical.
func sameOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
