// Package errdef defines icelake's typed errors.
//
// It exists as a leaf package — it imports nothing from icelake — so that the
// internal packages the root package imports can construct exactly the error
// types the public packages publish, with no import cycle. Every type and
// sentinel declared here is re-exported by the package that returns it — types
// as aliases, sentinels as variables — so an error returned from deep inside
// the library still matches against the public name: errors.As(err,
// &icelake.NameError{}) and errors.As(err, &errdef.NameError{}) are the same
// assertion on the same type.
//
// "The package that returns it" is almost always the root package. The four
// names catalog rebuild owns — ErrCatalogExists, RebuildPrefixKind,
// RebuildPrefixError and RebuildStorageError — are re-exported from the rebuild
// package instead, and deliberately not from the root one: a name on the root
// package promises something the root package can deliver.
//
// ARCHITECTURE.md's "Error taxonomy" section is the authoritative list, and the
// field sets here are exactly the ones that document specifies. What lives here
// is one error per real failure mode across every layer that has one so far:
// configuration, schema declaration from either of its two sources, record
// acceptance through either front door, the canonical encoding, the staging
// store, the local Parquet spool, the catalog database, table reconciliation,
// flushing, and catalog rebuild.
package errdef

import (
	"errors"
	"fmt"
)

// NameKind identifies which of the two name positions a [NameError] refers to.
type NameKind string

const (
	// NameKindNamespace marks a rejected Iceberg namespace name.
	NameKindNamespace NameKind = "namespace"
	// NameKindTable marks a rejected table name.
	NameKindTable NameKind = "table"
)

// NameError reports a namespace or table name that icelake refuses.
//
// Names must match ^[a-z0-9][a-z0-9_]*$ and be at most 63 bytes long, because
// the warehouse key layout gives each table exactly two path segments and the
// catalog-rebuild tool discovers tables by parsing those segments back out. A
// slash, dot, space, hyphen, uppercase letter, leading underscore, empty
// string, or over-long name is rejected rather than normalized: silently
// rewriting a caller's table name is how two services end up sharing a table.
//
// Kind says which of the two names was bad; Value is the offending name
// exactly as the caller supplied it.
type NameError struct {
	Kind  NameKind
	Value string
}

// Error implements the error interface.
func (e NameError) Error() string {
	return fmt.Sprintf("icelake: invalid %s name %q: must match ^[a-z0-9][a-z0-9_]*$ and be at most 63 bytes", e.Kind, e.Value)
}

// PoisonReason says which representability rule a rejected value broke, so a
// caller (and a test) can tell the three cases apart without reading the
// message text.
//
// It is a small enumeration rather than the free-form string ARCHITECTURE.md's
// field list might suggest, and the reason is the testing policy: a Reason a
// test could only assert on by matching a prose fragment would be a fail-loudly
// promise the suite cannot hold. The cases are the ones that decision names,
// and nothing else is a poison — see [PoisonError].
type PoisonReason string

const (
	// PoisonReasonInvalidUTF8 marks a string value carrying bytes that are not
	// valid UTF-8. Parquet's String logical type is defined as UTF-8, so such a
	// value has no honest representation in the declared column.
	PoisonReasonInvalidUTF8 PoisonReason = "invalid_utf8"
	// PoisonReasonDecimalRange marks an unscaled decimal value with more digits
	// than the column's declared precision can hold. The value would otherwise
	// be sign-extended into the data file unchanged and read back as a number
	// the column claims it cannot represent.
	PoisonReasonDecimalRange PoisonReason = "decimal_range"
	// PoisonReasonTooLong marks a string or binary value longer than the
	// canonical encoding's four-byte length prefix can address.
	PoisonReasonTooLong PoisonReason = "too_long"
)

// PoisonError reports a record Insert refused because one of its field values
// is not representable in the column that field is declared as.
//
// Records are typed Go values, but not every value of a Go type fits the
// declared Parquet column, and the cases are narrow and known: a string that is
// not valid UTF-8, a decimal whose unscaled value does not fit the declared
// precision, and a string or binary value longer than the encoding can address.
// Insert checks exactly those, per field, before the record is written to
// staging — so the record is refused at the boundary by the caller who supplied
// it, where the context to fix it exists, rather than poisoning a batch that
// then fails to encode forever.
//
// The refused record never enters staging and the writer stays fully usable.
// Table and Field name where the problem is, Field being the declared column
// name; Reason says which rule was broken. Which value it was is carried in the
// message only, so no test can come to depend on that wording.
type PoisonError struct {
	Table  string
	Field  string
	Reason PoisonReason

	// detail is deliberately unexported, for the same reason
	// [CrossCheckError]'s is: a useful message for a human, with nothing in it
	// for a test to assert on.
	detail string
}

// NewPoisonError builds a PoisonError for one field of one record. The detail
// is used only in the error message; assertions must go through Table, Field
// and Reason.
func NewPoisonError(reason PoisonReason, table, field, detail string) PoisonError {
	return PoisonError{Table: table, Field: field, Reason: reason, detail: detail}
}

// Error implements the error interface.
func (e PoisonError) Error() string {
	return fmt.Sprintf("icelake: table %q field %q: %s; the record was not accepted", e.Table, e.Field, e.detail)
}

// DeclarationKind says what is wrong with a declaration struct, so a caller or
// a test can tell the structural refusals apart from a library derivation
// failure without reading the message text.
type DeclarationKind string

const (
	// DeclarationKindNotStruct marks a declaration type that is not a struct.
	DeclarationKindNotStruct DeclarationKind = "not_struct"
	// DeclarationKindNoFields marks a declaration that derives to zero
	// columns, which would create a zero-column table.
	DeclarationKindNoFields DeclarationKind = "no_fields"
	// DeclarationKindUnexportedField marks a declaration carrying an
	// unexported field. Field names the offending Go field.
	DeclarationKindUnexportedField DeclarationKind = "unexported_field"
	// DeclarationKindDerivation marks a failure inside one of the three
	// library calls that turn a declaration struct into an Iceberg schema —
	// a malformed struct tag, an unparseable fieldid, an unknown tag option.
	// Err carries the underlying library error.
	DeclarationKindDerivation DeclarationKind = "derivation"
	// DeclarationKindUnsupportedGoType marks a declaration field whose Go type
	// icelake refuses to declare a column for. Field names the offending Go
	// field.
	DeclarationKindUnsupportedGoType DeclarationKind = "unsupported_go_type"
)

// DeclarationError reports a declaration struct that icelake cannot turn into
// a table schema at all.
//
// It covers the refusals that happen before the two derivations can even be
// compared: a declaration type that is not a struct, one that would produce a
// zero-column table, one carrying an unexported field (which the declared side
// turns into a real column the caller has no way to tag), one carrying a field
// whose Go type is outside the declarable whitelist, and any failure inside the
// three-call derivation chain itself — a malformed logical type, a non-numeric
// fieldid, an unknown arrow tag option.
//
// Kind says which. Field names the offending Go field where one field is at
// fault, and is empty when the problem is the struct as a whole. Err carries
// the underlying library error for DeclarationKindDerivation and is nil for
// the structural kinds; it is wrapped, so a library error stays reachable
// through errors.Is and errors.As.
type DeclarationError struct {
	Table string
	Field string
	Kind  DeclarationKind
	Err   error

	// detail carries the offending Go type for
	// DeclarationKindUnsupportedGoType. It is unexported for the same reason
	// [CrossCheckError]'s is: it makes the message useful to a human without
	// inviting a test to assert on message text.
	detail string
}

// NewUnsupportedGoTypeError reports a declaration field whose Go type icelake
// will not declare a column for. goType is the type's own spelling, used only
// in the message; assertions go through Field and Kind.
func NewUnsupportedGoTypeError(table, field, goType string) DeclarationError {
	return DeclarationError{
		Table:  table,
		Field:  field,
		Kind:   DeclarationKindUnsupportedGoType,
		detail: goType,
	}
}

// Error implements the error interface.
func (e DeclarationError) Error() string {
	switch e.Kind {
	case DeclarationKindNotStruct:
		return fmt.Sprintf("icelake: table %q: declaration type is not a struct", e.Table)
	case DeclarationKindNoFields:
		return fmt.Sprintf("icelake: table %q: declaration has no columns; a table needs at least one field", e.Table)
	case DeclarationKindUnexportedField:
		return fmt.Sprintf("icelake: table %q: declaration field %q is unexported; every declared field must be exported and carry parquet and arrow tags", e.Table, e.Field)
	case DeclarationKindUnsupportedGoType:
		return fmt.Sprintf("icelake: table %q: declaration field %q has Go type %s, which icelake does not declare columns for; use bool, int32, int64, float32, float64, string or []byte, or a pointer to one for an optional column", e.Table, e.Field, e.detail)
	default:
		return fmt.Sprintf("icelake: table %q: deriving schema from declaration: %v", e.Table, e.Err)
	}
}

// Unwrap exposes the underlying library error, so a caller can reach past the
// declaration failure to whatever arrow-go or iceberg-go actually reported.
func (e DeclarationError) Unwrap() error { return e.Err }

// CrossCheckError reports that a declaration struct is not usable as written,
// found by the startup cross-check between the two independent reflection
// layers that read the same struct: parquet/schema, which derives the declared
// schema, and arrow/array/arreflect, which derives the row data.
//
// The two layers read different struct-tag namespaces, so they can disagree
// about a column's name, its nullability, or its type. Only two type
// disagreements are adaptable (Int64 to Timestamp(ms, UTC) and Int64 to
// Decimal128); everything else fails the table open before a single row is
// accepted. The same error also carries the flat-only scoping rejection: a
// declared struct containing a nested group never reaches the reconciliation
// loop, which diffs single-segment field paths only.
//
// Column names the column the disagreement is about, using the name the
// declared (parquet-side) schema gives it. Table is the table the declaration
// belongs to. Those two fields are the whole assertable surface on purpose:
// which disagreement it was is carried in the message only, so that a test
// cannot come to depend on that wording.
type CrossCheckError struct {
	Table  string
	Column string

	// detail is deliberately unexported: it makes the message useful to a
	// human without inviting a test to assert on message text, which this
	// project's testing policy bans outright.
	detail string
}

// NewCrossCheckError builds a CrossCheckError for one column of one table.
// The detail is used only in the error message; assertions must go through the
// exported Table and Column fields.
func NewCrossCheckError(table, column, detail string) CrossCheckError {
	return CrossCheckError{Table: table, Column: column, detail: detail}
}

// Error implements the error interface.
func (e CrossCheckError) Error() string {
	return fmt.Sprintf("icelake: table %q column %q: %s", e.Table, e.Column, e.detail)
}

// StagingSchemaKind says why a staged row's recorded shape cannot be
// reconciled with the current declaration, so an operator (and a test) can tell
// the three cases apart without reading the message text. Each one calls for a
// different action, which is why this is exported where
// [CrossCheckError]'s detail deliberately is not.
type StagingSchemaKind string

const (
	// StagingSchemaKindFieldRemoved marks a field id present in the recorded
	// shape and absent from the current declaration. Staged data exists that
	// the current struct cannot even represent; the table must be drained with
	// the previous binary first.
	StagingSchemaKindFieldRemoved StagingSchemaKind = "field_removed"
	// StagingSchemaKindFieldRequired marks a field the current declaration
	// requires for which the staged row has no value — either because the
	// recorded shape has no such field id, or because it recorded a null there.
	// A required column cannot be conjured, and substituting a zero value would
	// invent data.
	StagingSchemaKindFieldRequired StagingSchemaKind = "field_required"
	// StagingSchemaKindFieldRetyped marks a field id whose canonical type
	// differs between the recorded shape and the current declaration.
	// Reinterpreting the staged bytes under the new type would silently
	// misread them.
	StagingSchemaKindFieldRetyped StagingSchemaKind = "field_retyped"
)

// StagingSchemaError reports staged rows whose recorded schema shape cannot be
// mapped into the current declaration.
//
// Every staged row records the fingerprint of the shape its payload was encoded
// under, and replay decodes each row by that recorded shape, never by the
// current one. Mapping the decoded values into the current declaration is by
// permanent field id: a field the current declaration added since is simply
// null, which is exactly right because a later-added field is always optional.
// The unmappable cases fail loudly rather than half-opening a writer and
// quietly dropping or inventing a column's staged values.
//
// FieldID names the field at fault and Kind says which case it is. Table is the
// table whose replay found the problem.
//
// The advice all three messages below give — drain the table with the
// previous binary before deploying this one — has one exception, recorded
// here at the completeness audit's fifth round (2026-08-01) because this type
// is where the advice is written down. It does not hold for
// [StagingSchemaKindFieldRetyped] when the retype is one schema
// reconciliation accepts. A declaration going from int32 to int64 is a
// widening, and OpenWriter reconciles and commits that widening against the
// live table before it ever looks at what is staged, so by the time this
// error reaches a caller the live column is already the wider type.
// Redeploying the previous binary then fails earlier and for a different
// reason: its narrower declaration is now a narrowing of the live column,
// which reconciliation refuses outright. Neither binary can open the table,
// and dropping the column from the declaration only changes which kind
// refuses, since reconciliation reads an omission as "leave the live column
// alone" and the staged value for it then fails as
// [StagingSchemaKindFieldRemoved]. What is left is the staging file itself,
// which is where the README already sends an operator for rows that cannot be
// encoded: read the affected rows out of `staging.db` and delete them,
// accepting that they are lost, or copy the file aside first if they are
// worth recovering by hand. All three kinds are reached through changes
// reconciliation accepts, so that is not what singles this one out; what does
// is that the accepted change here alters the live column at fault, and the
// other two do not. [StagingSchemaKindFieldRequired] arrives when the current
// declaration requires a column the staged rows' recorded shape has no value
// for, and [StagingSchemaKindFieldRemoved] when the declaration simply omits
// a live column and the staged rows hold a value for it — an omission
// reconciliation reads as "leave that column alone" rather than as a drop. In
// both, redeploying the previous binary reconciles cleanly, because the live
// column at fault is exactly as it was; and on the assumption the advice
// already makes — that the previous binary is the one those rows were staged
// under — its declaration is one they map into, so the drain happens. The
// retyped case is the one where rolling back is refused before any of that
// can be tried. This is recorded and deliberately not asserted anywhere: it
// is a statement about what an operator should be told, and the place to fix
// it is the text, not a test.
type StagingSchemaError struct {
	Table   string
	FieldID int
	Kind    StagingSchemaKind
}

// Error implements the error interface.
func (e StagingSchemaError) Error() string {
	switch e.Kind {
	case StagingSchemaKindFieldRequired:
		return fmt.Sprintf("icelake: table %q: staged rows have no value for field id %d, which the current declaration requires; drain the table with the previous binary before deploying this one", e.Table, e.FieldID)
	case StagingSchemaKindFieldRetyped:
		return fmt.Sprintf("icelake: table %q: staged rows recorded field id %d with a different type than the current declaration gives it; drain the table with the previous binary before deploying this one", e.Table, e.FieldID)
	default:
		return fmt.Sprintf("icelake: table %q: staged rows recorded field id %d, which the current declaration no longer has; drain the table with the previous binary before deploying this one", e.Table, e.FieldID)
	}
}

// StagingKind says which part of the local staging store failed, so a caller
// can tell a database that would not open from one that would not migrate, and
// either of those from an ordinary read or write failure against a database
// that is otherwise fine.
type StagingKind string

const (
	// StagingKindOpen marks a staging database that could not be opened or
	// configured: the file could not be created, one of the pragmas icelake
	// requires (WAL, synchronous=FULL, a busy timeout) did not take effect, or
	// the path itself is one the SQLite driver would misread. Every pragma is
	// asserted at open rather than fired and hoped for, because the durability
	// guarantee this layer exists to provide is exactly what they buy.
	StagingKindOpen StagingKind = "open"
	// StagingKindClose marks a staging database that did not close cleanly. It
	// is its own kind rather than folded into the open failure because the two
	// mean opposite things to an operator: an open failure means nothing was
	// served, and a close failure means work was served and the final flush of
	// it is in doubt.
	StagingKindClose StagingKind = "close"
	// StagingKindMigrate marks a failed schema migration. Startup stops here
	// rather than serving writes against a database whose schema state is
	// unknown; the previous binary can still use the database, because a failed
	// migration is rolled back and never recorded as applied.
	StagingKindMigrate StagingKind = "migrate"
	// StagingKindRead marks a failed read against an open, migrated database.
	StagingKindRead StagingKind = "read"
	// StagingKindWrite marks a failed write against an open, migrated database.
	// A record whose staging write fails is not accepted: there is no in-memory
	// fallback buffer, because buffering in memory for "the durability layer is
	// broken" would silently downgrade the guarantee that layer exists to give.
	StagingKindWrite StagingKind = "write"
	// StagingKindState marks a staging database that is not in the state the
	// operation requires: a row that was to be sealed is already sealed into
	// another batch or has been quarantined, a row read back by id is gone, or a
	// staged row names a schema fingerprint the store has no descriptor for.
	// These are refusals rather than repairs — each one means some assumption
	// about who owns which rows has already been violated, and continuing would
	// move accepted records between batches or decode them under a shape that
	// did not write them.
	StagingKindState StagingKind = "state"
)

// StagingError reports a failure of the local write-ahead staging store — the
// SQLite database every record is durably written to before Insert returns.
//
// Staging is the fast, local, reliable part of icelake, so a failure here is
// never absorbed: it fails the call that provoked it, immediately, and the
// caller keeps ownership of the record it tried to hand over. Path is the
// staging database the failure is about, which is the one file an operator has
// to have a backup story for. Kind says which part failed and Err carries the
// underlying driver, migration or query error, wrapped so it stays reachable
// through errors.Is and errors.As.
type StagingError struct {
	Path string
	Kind StagingKind
	Err  error

	// detail is deliberately unexported, for the same reason
	// [CrossCheckError]'s is: it makes the message useful to a human without
	// inviting a test to assert on message text.
	detail string
}

// NewStagingError builds a StagingError. err may be nil when icelake itself is
// refusing rather than reporting a driver failure. The detail is used only in
// the error message; assertions must go through Path, Kind and Err.
func NewStagingError(kind StagingKind, path, detail string, err error) StagingError {
	return StagingError{Path: path, Kind: kind, Err: err, detail: detail}
}

// Error implements the error interface.
func (e StagingError) Error() string {
	switch {
	case e.detail != "" && e.Err != nil:
		return fmt.Sprintf("icelake: staging %q: %s: %v", e.Path, e.detail, e.Err)
	case e.detail != "":
		return fmt.Sprintf("icelake: staging %q: %s", e.Path, e.detail)
	default:
		return fmt.Sprintf("icelake: staging %q: %v", e.Path, e.Err)
	}
}

// Unwrap exposes the underlying driver, migration or query error, so a caller
// can reach past the staging failure to whatever SQLite or goose reported.
func (e StagingError) Unwrap() error { return e.Err }

// EncodingKind says which layer of the canonical encoding refused, so a caller
// can tell a declaration icelake cannot encode at all from a value it will not
// encode and from bytes it cannot read back.
type EncodingKind string

const (
	// EncodingKindUnsupportedType marks a declared column whose type the
	// canonical encoding has no representation for. Field names the column.
	EncodingKindUnsupportedType EncodingKind = "unsupported_type"
	// EncodingKindValue marks a value the canonical encoding cannot write for
	// its declared column: the wrong Go type for that column, a null in a
	// required column, or a string or binary value longer than the encoding's
	// addressable limit.
	EncodingKindValue EncodingKind = "value"
	// EncodingKindFormat marks stored bytes that do not conform to the
	// canonical format: an unknown format version, a truncated payload or
	// descriptor, an unknown type tag, or a descriptor that is not in canonical
	// form.
	EncodingKindFormat EncodingKind = "format"
	// EncodingKindEmptyBatch marks an attempt to take a batch content hash over
	// zero records. A batch key must name real bytes; hashing nothing would
	// produce a valid-looking key for an empty file.
	EncodingKindEmptyBatch EncodingKind = "empty_batch"
)

// EncodingError reports a failure in icelake's own canonical record encoding —
// the byte form a record is staged in, and the form a batch's content hash, and
// therefore its object key, is computed over.
//
// That encoding is deliberately icelake's own rather than gob or JSON, because
// upload idempotency rests on the exact bytes never drifting, so every way it
// can refuse is a typed, matchable failure rather than a message to grep.
//
// Kind says which layer refused. Table is the table involved. Field and FieldID
// name the offending column where one column is at fault, and are empty and
// zero when the problem is the payload or descriptor as a whole. Which
// malformation it was is carried in the message only, so no test can come to
// depend on that wording.
type EncodingError struct {
	Table   string
	Field   string
	FieldID int
	Kind    EncodingKind

	// detail is deliberately unexported, for the same reason
	// [CrossCheckError]'s is: a useful message for a human, with nothing in it
	// for a test to assert on.
	detail string
}

// NewEncodingError builds an EncodingError. Field and fieldID may be zero when
// no single column is at fault. The detail is used only in the error message;
// assertions must go through the exported fields.
func NewEncodingError(kind EncodingKind, table, field string, fieldID int, detail string) EncodingError {
	return EncodingError{Table: table, Field: field, FieldID: fieldID, Kind: kind, detail: detail}
}

// Error implements the error interface.
func (e EncodingError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("icelake: table %q field %q (id %d): %s", e.Table, e.Field, e.FieldID, e.detail)
	}
	return fmt.Sprintf("icelake: table %q: %s", e.Table, e.detail)
}

// FieldIDError reports a declared field ID that violates the rules in
// SCHEMA.md's "Field IDs at creation".
//
// Two things produce it. A field that states no ID at all — a struct field with
// no fieldid tag, which arrow-go numbers -1, or a schema document field with no
// fieldid key — reported here as FieldID == -1. And, at table creation, an ID
// that does not equal that field's position in a pre-order traversal of the
// declared schema, with WantID carrying the number the traversal expects.
//
// Both declaration sources produce it, deliberately: the rule being broken is
// the same permanence rule whichever way the shape was stated, and a caller
// matching on it should not have to know which source the table came from.
//
// The check exists because a catalog's CreateTable renumbers a new table's
// fields itself, in pre-order, and does so silently: a declaration that
// disagrees creates a table with no error raised whose IDs the declaration can
// then never match again, so every later reconciliation pass tries to add
// columns that already exist. Pre-order rather than declaration order because
// a nested group's children consume IDs between the group and its next
// sibling.
type FieldIDError struct {
	Table   string
	Field   string
	FieldID int
	WantID  int
}

// Error implements the error interface.
func (e FieldIDError) Error() string {
	if e.FieldID < 0 {
		return fmt.Sprintf("icelake: table %q field %q states no field id; pre-order numbering requires fieldid=%d", e.Table, e.Field, e.WantID)
	}
	return fmt.Sprintf("icelake: table %q field %q declares fieldid=%d but pre-order numbering requires fieldid=%d", e.Table, e.Field, e.FieldID, e.WantID)
}

// ErrClosed reports an operation on a writer, or a store, that has already been
// closed. Insert returns it once Close has stopped the writer accepting
// records; the records already accepted are unaffected, because Close drains
// them before it returns.
var ErrClosed = errors.New("icelake: closed")

// ErrStagingFull reports that the staging ceiling has been reached and the
// record was not accepted.
//
// Refusing is deliberately chosen over blocking: a blocking Insert turns a
// storage problem into an invisible latency problem in the caller's own hot
// path, and can deadlock a caller that inserts from the goroutine that would
// otherwise drain something. The refused record never enters staging, so the
// caller knows precisely which records it still owns.
var ErrStagingFull = errors.New("icelake: staging store is full")

// ErrTableInUse reports a second writer opened for a table that already has a
// live one in the same store.
//
// One writer per table is an assumption the whole design rests on: two writers
// on one table interleave batches of the same staged rows and commit against
// each other's metadata, so the loser's commits start failing against a
// snapshot the winner has already moved past. Across two processes nothing can
// see the other side and the rule stays an operational one. Within one process
// it is a map lookup, so it is enforced rather than documented.
var ErrTableInUse = errors.New("icelake: table already has a writer in this store")

// RecordKind says why a JSON row was refused, so a caller can tell a key it did
// not expect from a value it cannot represent, and either of those from a value
// that is simply outside a column's range.
//
// It is an exported enumeration for the same reason [PoisonReason] is one: the
// testing policy bans asserting on message text, so a refusal a test could only
// recognise by its prose is a promise the suite cannot hold.
type RecordKind string

const (
	// RecordKindUnknownField marks a key the declaration does not have. It is
	// refused rather than ignored because an unknown key is either a typo or a
	// schema the document has not caught up with, and both want a person.
	RecordKindUnknownField RecordKind = "unknown_field"
	// RecordKindMissing marks a required column the row has no key for. A
	// required column has no zero value to substitute, and substituting one is
	// inventing data.
	RecordKindMissing RecordKind = "missing"
	// RecordKindNull marks a null in a required column, which is the same
	// refusal as a missing key reached through a key that is present.
	RecordKindNull RecordKind = "null"
	// RecordKindType marks a JSON value whose type cannot supply the declared
	// column at all: a number for a boolean, a float for a decimal, an object
	// where a scalar belongs.
	RecordKindType RecordKind = "type"
	// RecordKindInexact marks a value that could only be stored by losing
	// something: a non-integral number for an integer column, a float64 past
	// 2^53 whose digits are already not the digits that were written, a decimal
	// with more fraction digits than its scale, a timestamp carrying
	// microseconds. Every one of them is refused rather than rounded.
	RecordKindInexact RecordKind = "inexact"
	// RecordKindRange marks a value outside the declared column's range.
	RecordKindRange RecordKind = "range"
	// RecordKindMalformed marks text that does not parse as what the column
	// requires — base64 that is not base64, a timestamp that is not RFC3339 —
	// and a line that is not a JSON object at all.
	RecordKindMalformed RecordKind = "malformed"
)

// RecordError reports a JSON row the dynamic writer refuses.
//
// It is the runtime-schema counterpart of [PoisonError], and it is a separate
// type rather than a reuse because the two answer different questions: a
// PoisonError says a well-typed Go value cannot live in its declared column,
// while this says the caller's JSON does not describe a row of this table at
// all. A refused row is not accepted — nothing enters staging — so the caller
// still owns it, exactly as with a poison refusal.
//
// Namespace, Table and Field identify the column at fault; Field is empty when
// the problem is the row as a whole. Kind says which rule was broken and is what
// a caller or a test acts on. What was actually seen travels in the message and
// nowhere else, exactly as it does for [CrossCheckError] and [EncodingError], so
// that no test can come to depend on that wording.
type RecordError struct {
	Namespace string
	Table     string
	Field     string
	Kind      RecordKind

	// detail is deliberately unexported, for the same reason [StagingError]'s
	// is: it makes the message useful to a human without inviting an assertion
	// on message text.
	detail string
}

// NewRecordError builds a RecordError. The detail is used only in the error
// message; assertions must go through Namespace, Table, Field and Kind.
func NewRecordError(kind RecordKind, namespace, table, field, detail string) RecordError {
	return RecordError{Namespace: namespace, Table: table, Field: field, Kind: kind, detail: detail}
}

// Error implements the error interface.
func (e RecordError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("icelake: table %s.%s: %s: %s", e.Namespace, e.Table, e.Kind, e.detail)
	}

	return fmt.Sprintf("icelake: table %s.%s field %q: %s: %s", e.Namespace, e.Table, e.Field, e.Kind, e.detail)
}

// SchemaDocKind says which rule of the schema document format was broken.
type SchemaDocKind string

const (
	// SchemaDocKindSyntax marks a document that is not the JSON this format is,
	// including one carrying a key the format does not define. Unknown keys are
	// refused rather than ignored because a misspelled "feildid" that parsed as
	// "no field id" would be the exact class of silent shape change the
	// permanent-ID rule exists to make impossible.
	SchemaDocKindSyntax SchemaDocKind = "syntax"
	// SchemaDocKindDuplicateTable marks a namespace and table pair declared
	// twice in one document.
	SchemaDocKindDuplicateTable SchemaDocKind = "duplicate_table"
	// SchemaDocKindNoFields marks a table declaring no columns, which would
	// derive to a zero-column table: created successfully and uselessly.
	SchemaDocKindNoFields SchemaDocKind = "no_fields"
	// SchemaDocKindDuplicateField marks a column whose name cannot identify it:
	// two columns of one table sharing a name, or a column with no name at all.
	SchemaDocKindDuplicateField SchemaDocKind = "duplicate_field"
	// SchemaDocKindUnknownType marks a type spelling outside the nine the
	// canonical encoding carries.
	SchemaDocKindUnknownType SchemaDocKind = "unknown_type"
	// SchemaDocKindDecimal marks a malformed decimal(P,S): unparseable, a
	// precision past the 18 an int64 unscaled value holds, or a scale larger
	// than its own precision.
	SchemaDocKindDecimal SchemaDocKind = "decimal"
)

// SchemaDocError reports a schema document [ParseSchemaDocument] will not
// accept.
//
// Every refusal it carries happens before any table is touched: a document is
// judged whole, and a document with anything wrong in it opens nothing. Table
// names the table at fault where the document got far enough to have one, Field
// names the column where one column is at fault, Kind says which rule was
// broken, and Err carries the underlying JSON error where there was one.
//
// Field-ID violations inside a document are deliberately *not* this type and
// reuse [FieldIDError]: the rule being broken is the same permanence rule a
// struct declaration breaks, and a caller matching on it should not have to know
// which of the two declaration sources produced the table. Namespace and table
// names are the same, and reuse [NameError].
type SchemaDocError struct {
	Table string
	Field string
	Kind  SchemaDocKind
	Err   error

	// detail is deliberately unexported, for the same reason [StagingError]'s
	// is: it makes the message useful to a human without inviting a test to
	// assert on message text.
	detail string
}

// NewSchemaDocError builds a SchemaDocError. err may be nil when icelake itself
// is refusing rather than reporting a JSON failure.
func NewSchemaDocError(kind SchemaDocKind, table, field, detail string, err error) SchemaDocError {
	return SchemaDocError{Table: table, Field: field, Kind: kind, Err: err, detail: detail}
}

// Error implements the error interface.
func (e SchemaDocError) Error() string {
	where := "schema document"
	switch {
	case e.Table != "" && e.Field != "":
		where = fmt.Sprintf("schema document table %q field %q", e.Table, e.Field)
	case e.Table != "":
		where = fmt.Sprintf("schema document table %q", e.Table)
	}
	if e.Err != nil {
		return fmt.Sprintf("icelake: %s: %s: %v", where, e.detail, e.Err)
	}

	return fmt.Sprintf("icelake: %s: %s", where, e.detail)
}

// Unwrap exposes the underlying JSON error, so a caller can reach past the
// refusal to what encoding/json reported.
func (e SchemaDocError) Unwrap() error { return e.Err }

// ErrLocalOnly reports a [Store] method that needs a bucket, called against a
// store opened with LocalOnly set.
//
// Such a store never built an object-storage client and never opened a catalog,
// so the honest answer is the mode rather than whatever a nil client would have
// said. Exactly one method produces it — the spool drain, which exists to move
// a local-only backlog into a bucket and therefore needs one. Catalog rebuild is
// deliberately not on that list and never produces this: it takes its own
// options, opens its own client and has never heard of a Store, so there is no
// local-only store for it to be refused by.
var ErrLocalOnly = errors.New("icelake: store was opened in local-only mode and has no bucket")

// SpoolKind says which part of the local Parquet spool failed, because the four
// mean very different things about what is at risk.
type SpoolKind string

const (
	// SpoolKindWrite marks bytes that could not be put on disk: a full disk, a
	// bad path, a permission. In bucket mode the flush fails and the batch stays
	// staged; in local-only mode this is the only durable act there is, so its
	// failure is the whole flush's failure.
	SpoolKindWrite SpoolKind = "write"
	// SpoolKindRecord marks a file that landed while its database row did not,
	// so a file exists that no sweep will evict and no drain will find until the
	// batch's own retry rewrites both.
	SpoolKindRecord SpoolKind = "record"
	// SpoolKindMark marks an upload and a commit that both succeeded where
	// committed_at could not then be set. It is the safe direction: the file is
	// merely unevictable, and its records are already in the table.
	SpoolKindMark SpoolKind = "mark"
	// SpoolKindDrain marks a backlog file that could not be uploaded, committed,
	// or reconciled to. It blocks that one table's open exactly as a
	// [ReconcileError] does.
	SpoolKindDrain SpoolKind = "drain"
)

// SpoolError reports a failure of the local Parquet spool — the cache directory
// a flushed batch's bytes are written to before they are uploaded, and the two
// tables in the staging database that record what is in it.
//
// It is surfaced by the two paths that have a caller to answer: the flush
// worker and the transition drain. There is deliberately no eviction kind. A
// retention sweep has no caller to return to and, by design, reports to nobody,
// so an eviction kind would be a name in this taxonomy that no errors.As
// anywhere could ever match — and this list is of errors that cross the public
// boundary.
//
// Dir is the configured cache directory and Path the file the failure is about,
// so an operator meeting one is looking at a real path; BatchKey names the batch,
// which is also the file's name and the object's; Err is wrapped, so the
// underlying filesystem or database error stays reachable.
type SpoolError struct {
	Kind     SpoolKind
	Dir      string
	Path     string
	BatchKey string
	Err      error

	// detail is deliberately unexported, for the same reason [StagingError]'s
	// is: it makes the message useful to a human without inviting a test to
	// assert on message text.
	detail string
}

// NewSpoolError builds a SpoolError. err may be nil when icelake itself is
// refusing rather than reporting a filesystem or database failure. The detail is
// used only in the error message; assertions must go through the exported
// fields.
func NewSpoolError(kind SpoolKind, dir, path, batchKey, detail string, err error) SpoolError {
	return SpoolError{Kind: kind, Dir: dir, Path: path, BatchKey: batchKey, Err: err, detail: detail}
}

// Error implements the error interface.
func (e SpoolError) Error() string {
	where := e.Path
	if where == "" {
		where = e.Dir
	}
	switch {
	case e.detail != "" && e.Err != nil:
		return fmt.Sprintf("icelake: spool %q: %s: %v", where, e.detail, e.Err)
	case e.detail != "":
		return fmt.Sprintf("icelake: spool %q: %s", where, e.detail)
	default:
		return fmt.Sprintf("icelake: spool %q: %v", where, e.Err)
	}
}

// Unwrap exposes the underlying filesystem, database or commit error.
func (e SpoolError) Unwrap() error { return e.Err }

// ConfigFieldError reports one configuration field that fails one rule. Every
// violation in a Config produces one of these, and they are joined into a
// single [ConfigError], so a caller fixing a config file learns everything
// wrong with it in one run rather than one violation per run.
//
// Field is the Config or FlushPolicy field name as a caller would spell it in
// Go; Rule states what that field must satisfy. Both are stable enough to
// assert on Field; assertions must not read Rule's wording.
type ConfigFieldError struct {
	Field string
	Rule  string
}

// Error implements the error interface.
func (e ConfigFieldError) Error() string {
	return fmt.Sprintf("icelake: config field %s: %s", e.Field, e.Rule)
}

// ConfigError reports an invalid configuration. It wraps the errors.Join of one
// [ConfigFieldError] per violation, which is what makes "every violation at
// once" assertable per field rather than per message.
//
// Nothing is opened, created or contacted before this is returned: an invalid
// Config fails before a file is touched or a network call is made.
type ConfigError struct {
	Err error
}

// Error implements the error interface.
func (e ConfigError) Error() string {
	return fmt.Sprintf("icelake: invalid configuration: %v", e.Err)
}

// Unwrap exposes the joined per-field violations, so errors.As finds each
// [ConfigFieldError] through this error.
func (e ConfigError) Unwrap() error { return e.Err }

// CatalogKind says which part of the local catalog database failed.
type CatalogKind string

const (
	// CatalogKindOpen marks a catalog database that could not be opened,
	// configured, handed to iceberg-go's SQL catalog, or — since M6 — even
	// checked for existence, which catalog rebuild has to do before it can
	// decide whether it is allowed to write one.
	CatalogKindOpen CatalogKind = "open"
	// CatalogKindClose marks a catalog database that did not close cleanly.
	CatalogKindClose CatalogKind = "close"
	// CatalogKindWrite marks a failed write against the catalog database, or
	// against the file that holds it.
	//
	// Added at M6, with the rebuild path that produced the first one. Every
	// other write to this file goes through iceberg-go's own commit path, which
	// reports through [ReconcileError] because it concerns one declared table;
	// rebuild writes rows into the file directly, from a bucket scan, with no
	// declaration anywhere in sight, so its failures are about the file and
	// belong here. It also covers the filesystem half of the same act — the
	// atomic replacement that puts a fully-built catalog in place — because to
	// an operator "the rows would not go in" and "the finished file would not
	// move into place" are one failure with one consequence: no new catalog.
	CatalogKindWrite CatalogKind = "write"
)

// CatalogError reports a failure of the local catalog database — the SQLite
// file iceberg-go's SQL catalog keeps "which metadata file is current" in.
//
// It is about that file as a file: opening it, closing it, and the writes
// catalog rebuild makes into it and around it. A failure to create, load or
// evolve a table is a [ReconcileError], because that is a statement about one
// table rather than about the catalog as a whole, and it must block only that
// table.
//
// Unlike the staging database, this file is disposable: it is rebuildable from
// the bucket at any time, so a caller meeting this error has lost a cache, not
// data.
type CatalogError struct {
	Path string
	Kind CatalogKind
	Err  error

	// detail is deliberately unexported, for the same reason
	// [CrossCheckError]'s is: a useful message for a human, with nothing in it
	// for a test to assert on.
	detail string
}

// NewCatalogError builds a CatalogError. The detail is used only in the error
// message; assertions must go through Path, Kind and Err.
func NewCatalogError(kind CatalogKind, path, detail string, err error) CatalogError {
	return CatalogError{Path: path, Kind: kind, Err: err, detail: detail}
}

// Error implements the error interface.
func (e CatalogError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("icelake: catalog %q: %s", e.Path, e.detail)
	}
	return fmt.Sprintf("icelake: catalog %q: %s: %v", e.Path, e.detail, e.Err)
}

// Unwrap exposes the underlying driver or catalog error.
func (e CatalogError) Unwrap() error { return e.Err }

// ReconcileError reports a failure of one table's startup lifecycle: creating
// the table, loading it, diffing the declared shape against the live one, or
// committing the resulting schema update.
//
// It blocks exactly one table. Reconciliation happens in the per-table
// constructor, so one table's bad declaration returns an error from one call
// and leaves every other writer in the process untouched — icelake never
// decides for a caller whether that is fatal, but it never hands back a usable
// writer for a table it could not verify either.
//
// Table names the table. Err carries the underlying iceberg-go error, wrapped,
// so an unsafe change the library itself refused stays reachable through
// errors.Is and errors.As.
type ReconcileError struct {
	Table string
	Err   error

	// detail is deliberately unexported, for the same reason
	// [CrossCheckError]'s is.
	detail string
}

// NewReconcileError builds a ReconcileError. The detail is used only in the
// error message; assertions must go through Table and Err.
func NewReconcileError(table, detail string, err error) ReconcileError {
	return ReconcileError{Table: table, Err: err, detail: detail}
}

// Error implements the error interface.
func (e ReconcileError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("icelake: table %q: %s", e.Table, e.detail)
	}
	return fmt.Sprintf("icelake: table %q: %s: %v", e.Table, e.detail, e.Err)
}

// Unwrap exposes the underlying iceberg-go error.
func (e ReconcileError) Unwrap() error { return e.Err }

// FlushError reports that one batch could not be turned into a committed
// snapshot: it could not be encoded, uploaded, or committed.
//
// Nothing is lost when this happens. The batch's records stay in staging with
// their batch key intact — staging is pruned only after a commit is confirmed —
// so the same batch is retried, under the same key, on the next flush trigger
// or by the next run of the process. The one exception is not an exception to
// that: a batch that cannot be *encoded* is quarantined rather than retried,
// which still keeps every record in staging, marked, for an operator to read.
//
// BatchKey is the batch's content hash, which is also the name of the object it
// uploads to; it is empty when the failure happened before the batch was
// sealed. Records is how many records the batch holds. Attempts is how many
// attempts the flush cycle this error ended made — at most the configured
// FlushMaxAttempts, and counted per cycle rather than for the batch's whole
// life, because each flush trigger starts a fresh budget against the same
// batch.
//
// It is also what travels through the OnFlushError callback. The events that
// fire it are enumerated once, in Config.OnFlushError's own documentation,
// rather than restated here where a later change would have to find them both.
type FlushError struct {
	Namespace string
	Table     string
	BatchKey  string
	Records   int
	Attempts  int
	// Quarantined distinguishes the one failure whose meaning is the opposite
	// of every other's. False — the ordinary case — means the batch is still
	// queued and the next trigger retries it; the failure is self-healing and
	// a caller's only decision is whether to alert on repetition. True means
	// the batch could not be encoded at all, has been marked unflushable in
	// the staging database (quarantined_at, quarantine_error) and will never
	// be retried: the rows wait there for a person to inspect, export or
	// delete, and they count against the staging ceiling until removed. A
	// caller reporting this error to an operator must not describe the two
	// cases in the same words, which is why the field exists (2026-08-05,
	// after a log line did exactly that).
	Quarantined bool
	Err         error
}

// Error implements the error interface.
func (e FlushError) Error() string {
	s := fmt.Sprintf("icelake: flushing batch %s of table %s.%s (%d records, attempt %d): %v",
		e.BatchKey, e.Namespace, e.Table, e.Records, e.Attempts, e.Err)
	if e.Quarantined {
		s += " (quarantined: this batch will not be retried)"
	}

	return s
}

// Unwrap exposes the underlying encode, upload or commit error.
func (e FlushError) Unwrap() error { return e.Err }

// ClickHouseKind says which part of the optional ClickHouse mirror failed,
// because the operator's next action differs for each and because two of the
// four mean something a caller may not act on at all.
type ClickHouseKind string

const (
	// ClickHouseKindConnect marks a server that could not be reached or would
	// not answer. Nothing is lost: the lake is unaffected and the next flush
	// tries the whole preparation again.
	ClickHouseKindConnect ClickHouseKind = "connect"
	// ClickHouseKindSchema marks a CREATE DATABASE, a CREATE TABLE, an
	// ALTER TABLE ... ADD COLUMN, or the system.columns read that the
	// reconciliation diff is taken over. It is the same instruction to an
	// operator as a connect failure — the server is the thing that is unwell —
	// and it is a separate kind only because the two are diagnosed differently.
	ClickHouseKindSchema ClickHouseKind = "schema"
	// ClickHouseKindConflict marks the one failure that is a statement about
	// configuration rather than about reachability: a live mirror table whose
	// columns disagree with the declaration in a way the add-only rule refuses,
	// or a namespace and table pair that cannot be mapped to a mirror table
	// name injectively. It cannot heal by itself, so it is the one kind
	// OpenWriter returns rather than reporting.
	ClickHouseKindConflict ClickHouseKind = "conflict"
	// ClickHouseKindInsert marks the batch insert itself. The flush proceeded
	// and the lake holds the batch; the mirror is missing it until the mirror
	// is rebuilt.
	ClickHouseKindInsert ClickHouseKind = "insert"
)

// ClickHouseError reports a failure of the optional ClickHouse mirror — the
// disposable index ARCHITECTURE.md decides, fed from the flush path's encode
// seam.
//
// It is a separate type from [FlushError] because the two are opposite
// instructions. A FlushError says the batch is still in staging and will be
// retried, so a caller may have work to do about it; this says the batch is on
// its way into the lake exactly as it should be and only the disposable index
// fell behind. A caller that could not tell them apart would act on a problem
// that is not theirs, which is the same argument that made [MirrorError] its
// own type.
//
// Namespace and Table name the icelake table. Target is the ClickHouse object
// the failure is about, spelled as an operator would type it — database.table,
// or the database alone — so the error is about a real name rather than about a
// Go field. Column names the column for a conflict and is empty otherwise.
// BatchKey and Records identify the batch for an insert and are empty and zero
// otherwise. Err is the driver's own error, wrapped, so a refused connection or
// a rejected credential stays reachable through errors.Is and errors.As.
//
// It travels through Config.OnClickHouseError on the flush path, and is
// returned from OpenWriter for [ClickHouseKindConflict] only.
type ClickHouseError struct {
	Namespace string
	Table     string
	Target    string
	Column    string
	BatchKey  string
	Records   int
	Kind      ClickHouseKind
	Err       error

	// detail is deliberately unexported, for the same reason [StagingError]'s
	// is: it makes the message useful to a human without inviting a test to
	// assert on message text.
	detail string
}

// NewClickHouseError builds a ClickHouseError. err may be nil when icelake
// itself is refusing rather than reporting a driver failure. The detail is used
// only in the error message; assertions must go through the exported fields.
func NewClickHouseError(kind ClickHouseKind, namespace, table, target, detail string, err error) ClickHouseError {
	return ClickHouseError{
		Namespace: namespace,
		Table:     table,
		Target:    target,
		Kind:      kind,
		Err:       err,
		detail:    detail,
	}
}

// Error implements the error interface.
func (e ClickHouseError) Error() string {
	what := e.detail
	if what == "" {
		what = string(e.Kind)
	}
	if e.Err != nil {
		return fmt.Sprintf("icelake: clickhouse mirror %s for table %s.%s: %s: %v", e.Target, e.Namespace, e.Table, what, e.Err)
	}

	return fmt.Sprintf("icelake: clickhouse mirror %s for table %s.%s: %s", e.Target, e.Namespace, e.Table, what)
}

// Unwrap exposes the underlying driver error.
func (e ClickHouseError) Unwrap() error { return e.Err }

// MirrorError reports that a table's OnAccept callback returned an error.
//
// The record it concerns was accepted. It was durably committed to staging
// before the callback ran at all, and it is batched, uploaded and committed
// exactly like every other record — a failing mirror never costs icelake's own
// durability guarantee, which is the whole point of running the callback after
// the staging write rather than before it.
//
// It exists as a type rather than as the callback's own error passed straight
// back because the two answers a caller has to tell apart are opposite
// instructions: a refusal ([PoisonError], ErrStagingFull, ErrClosed) means the
// record is still the caller's to fix, retry or drop, and this one means the
// record is icelake's now and only the caller's own mirror fell behind.
// Retrying the Insert on this error would write the record twice.
//
// Err is what the callback returned, wrapped, so the caller's own sentinels and
// types stay reachable through errors.Is and errors.As.
type MirrorError struct {
	Namespace string
	Table     string
	Err       error
}

// Error implements the error interface.
func (e MirrorError) Error() string {
	return fmt.Sprintf("icelake: table %s.%s: the OnAccept callback failed; the record is staged and will still be committed: %v",
		e.Namespace, e.Table, e.Err)
}

// Unwrap exposes the error the callback returned.
func (e MirrorError) Unwrap() error { return e.Err }

// ErrCatalogExists reports that catalog rebuild was pointed at a catalog
// database that already exists, without being told it may replace it.
//
// Rebuild reconstructs the whole file from the bucket, so running it over a
// live catalog would throw away rows nothing asked it to throw away. Refusing is
// the safe default; the caller opts in explicitly when replacing is what they
// meant. A dry run never trips it, because a dry run writes nothing and so has
// nothing to protect.
var ErrCatalogExists = errors.New("icelake: catalog database already exists")

// RebuildPrefixKind says which way a prefix defeated catalog rebuild, because
// the operator's next action differs for each: fix a stray prefix, point the
// tool at the right warehouse, or investigate a table whose metadata is gone.
type RebuildPrefixKind string

const (
	// RebuildPrefixKindUnparseable marks a prefix discovered under the
	// warehouse that is not a valid namespace or table name. Such a prefix is
	// refused rather than passed over: a table discovery cannot see is the one
	// failure this tool must never have, and skipping quietly is how it would
	// have it.
	RebuildPrefixKindUnparseable RebuildPrefixKind = "unparseable"
	// RebuildPrefixKindNoMetadata marks a discovered table prefix with no
	// metadata file that could be read as Iceberg table metadata. There is
	// nothing to register it from, and registering nothing would silently leave
	// a table out of the rebuilt catalog.
	RebuildPrefixKindNoMetadata RebuildPrefixKind = "no_metadata"
	// RebuildPrefixKindEmpty marks a warehouse prefix that yielded no tables at
	// all. It is almost always a mistyped prefix or the wrong bucket, and the
	// alternative to refusing is handing back an empty catalog that reports
	// success — after which the next writer would create fresh tables over
	// tables that already exist somewhere else.
	RebuildPrefixKindEmpty RebuildPrefixKind = "empty"
)

// RebuildStorageError reports a read against the warehouse bucket that catalog
// rebuild could not complete: a listing, or a fetch of a metadata file it had
// just listed.
//
// It exists because rebuild reads object storage directly rather than through
// any of the paths that already have a typed failure — it never opens a table,
// never commits, and never uploads — so a bucket that answers with an error had
// nowhere in the taxonomy to be reported from. Bucket and Key name what was
// being read, Key being a prefix for a listing and an object key for a fetch.
// Err carries the wrapped SDK error, so a missing bucket or a rejected
// credential stays reachable through errors.Is and errors.As.
type RebuildStorageError struct {
	Bucket string
	Key    string
	Err    error
}

// Error implements the error interface.
func (e RebuildStorageError) Error() string {
	return fmt.Sprintf("icelake: rebuild: reading %s/%s: %v", e.Bucket, e.Key, e.Err)
}

// Unwrap exposes the underlying SDK error.
func (e RebuildStorageError) Unwrap() error { return e.Err }

// RebuildPrefixError reports a prefix catalog rebuild refuses: the configured
// warehouse prefix itself, or one discovered beneath it.
//
// Discovery reads the two-level <namespace>/<table> key layout and nothing
// else — no caller-supplied table list, no side-channel state — which makes that
// layout load-bearing correctness rather than a naming nicety. Anything under
// the warehouse that the layout cannot explain is therefore reported here rather
// than passed over.
//
// Prefix is the offending prefix, relative to the bucket's warehouse prefix for
// a discovered one and the warehouse prefix itself when the whole warehouse came
// back empty. Kind says which refusal it was.
type RebuildPrefixError struct {
	Prefix string
	Kind   RebuildPrefixKind
}

// Error implements the error interface.
func (e RebuildPrefixError) Error() string {
	switch e.Kind {
	case RebuildPrefixKindNoMetadata:
		return fmt.Sprintf("icelake: rebuild: %q has no readable Iceberg metadata file; a table cannot be registered from it", e.Prefix)
	case RebuildPrefixKindEmpty:
		return fmt.Sprintf("icelake: rebuild: no tables found under %q; check the bucket and the warehouse prefix", e.Prefix)
	default:
		return fmt.Sprintf("icelake: rebuild: %q does not parse as a <namespace>/<table> pair; names must match ^[a-z0-9][a-z0-9_]*$ and be at most 63 bytes", e.Prefix)
	}
}

// BatchError reports which row of a batch was refused, wrapping the refusal
// that row earned on its own.
//
// It exists because the batch doors take a slice and answer once. Every
// per-record refusal the single-record doors can return — a [PoisonError], a
// [RecordError], an [EncodingError], a malformed JSON line — is still exactly
// the right error about the row it concerns, and still says everything a caller
// needs to fix that row; what it cannot say, once it arrives from a call that
// was handed a hundred rows, is which of them it is about. Index is that answer
// and nothing more: the caller still holds the slice it passed in, so a position
// in it is more useful than a copy of the row, and it costs nothing to carry.
//
// Err is wrapped rather than replaced, so every assertion a caller already
// writes about a single-row refusal keeps working unchanged through errors.Is
// and errors.As on a batch's error.
//
// It is deliberately not returned for the refusals that are about the batch as a
// whole rather than about any row in it — a closed writer, a staging ceiling
// that the batch does not fit under, a staging store that failed. Those come
// back exactly as the single-record doors return them, because there is no row
// at fault and naming one would be a lie a caller could act on.
type BatchError struct {
	// Index is the position, in the slice the caller passed in, of the row this
	// error is about.
	Index int
	// Err is that row's own refusal.
	Err error
}

// Error implements the error interface.
func (e BatchError) Error() string {
	return fmt.Sprintf("icelake: row %d of the batch was refused, so none of the batch was accepted: %v", e.Index, e.Err)
}

// Unwrap exposes the row's own refusal, so the typed error a caller matches on
// stays reachable through the batch wrapper.
func (e BatchError) Unwrap() error { return e.Err }

// IngestKind says why a stream of records stopped being read, so a caller can
// tell a record the stream's own grammar refuses from one the table refuses
// from a failure that is about neither.
//
// It is an exported enumeration for the reason every other Kind in this package
// is one: the testing policy bans asserting on message text, so a refusal a
// test could only recognise by its prose is a promise the suite cannot hold.
type IngestKind string

const (
	// IngestKindGrammar marks an item that is not an envelope at all: bytes
	// that will not decode as one, or that carry a key the envelope does not
	// define. Err carries the decoder's own error.
	IngestKindGrammar IngestKind = "grammar"
	// IngestKindEnvelope marks an item that decoded as an envelope and is not
	// one: a missing table, or a missing row.
	IngestKindEnvelope IngestKind = "envelope"
	// IngestKindUnknownTable marks an envelope naming a table the caller opened
	// no writer for. Table carries the name it asked for.
	IngestKindUnknownTable IngestKind = "unknown_table"
	// IngestKindTooLarge marks a record longer than the configured maximum. It
	// is a refusal rather than a truncation because half a record is not a
	// record, and writing one would be exactly the quiet mangling the rest of
	// this taxonomy exists to avoid.
	IngestKindTooLarge IngestKind = "too_large"
	// IngestKindRead marks a failure of the reader itself, with the reader's
	// own error in Err. Record is zero: no record is at fault.
	IngestKindRead IngestKind = "read"
	// IngestKindRefused marks a record a table's own writer refused, with that
	// refusal in Err — a RecordError, a PoisonError, an EncodingError. The
	// stream's grammar was satisfied and the table's rules were not.
	IngestKindRefused IngestKind = "refused"
	// IngestKindWrite marks a failure that is about neither the record nor the
	// stream: a staging store that failed, a writer that is closed. Record
	// names the record the failing write started at, because that is the only
	// position there is to report, and Err carries the failure.
	IngestKindWrite IngestKind = "write"
)

// IngestError reports the record a stream of records stopped at, and why.
//
// It exists because a stream has a coordinate system of its own. Every refusal
// underneath it — a [RecordError] about a column, a [BatchError] about a row of
// a slice the ingest layer built — is still exactly the right error about the
// thing it concerns, and none of them can say which record of the caller's own
// input that thing was. Record is that answer: an absolute position in the
// stream, counted from one, and it is meaningful in every stream because every
// record has one.
//
// Line is the second answer, and it is present only when it means something.
// The rule is one sentence: **it is zero exactly when the record is known to be
// a CBOR one**, because a CBOR record is not on a line and naming one would be
// inventing a coordinate. Everything else reports the physical line — a JSON
// record reports the line it is on, and so does a byte the grammar does not
// recognise as the start of either, because somebody meeting that refusal is
// looking at a file. In a stream of nothing but JSON the two numbers coincide
// apart from blank lines, which are lines and are not records, so a file with
// two blank lines before its third record reports record 3 on line 5.
//
// Kind says which layer refused. Table is the table the record named, when it
// named one this layer could resolve, and is empty otherwise. Err is the
// underlying refusal, wrapped, so every assertion a caller already writes about
// a record refusal keeps working through errors.Is and errors.As.
type IngestError struct {
	// Kind says why the stream stopped.
	Kind IngestKind
	// Record is the record's absolute position in the stream, counted from one.
	// It is zero when the failure is about no particular record.
	Record int
	// Line is the record's line number when it was a JSON record, and zero when
	// it was a CBOR one or when there is no record at fault.
	Line int
	// Table is the table the record named, when the envelope carried one.
	Table string
	// Err is the underlying refusal, when there is one.
	Err error

	// detail is deliberately unexported, for the same reason [StagingError]'s
	// is: it makes the message useful to a human without inviting an assertion
	// on message text.
	detail string
}

// NewIngestError builds an IngestError. The detail is used only in the error
// message; assertions must go through Kind, Record, Line, Table and Err.
func NewIngestError(kind IngestKind, record, line int, table string, err error, detail string) IngestError {
	return IngestError{Kind: kind, Record: record, Line: line, Table: table, Err: err, detail: detail}
}

// IngestPosition renders a record's place in a stream the way icelake's own
// messages render one: the record number always, and the line number beside it
// when the record was on a line.
//
// It is exported so that a program printing its own message about an
// [IngestError] or an ingest notice says exactly what icelake would have said,
// rather than inventing a second way to write the same position down.
func IngestPosition(record, line int) string {
	if line > 0 {
		return fmt.Sprintf("record %d (line %d)", record, line)
	}

	return fmt.Sprintf("record %d", record)
}

// Error implements the error interface.
func (e IngestError) Error() string {
	what := e.detail
	switch {
	case what == "" && e.Err != nil:
		what = e.Err.Error()
	case what != "" && e.Err != nil:
		what = what + ": " + e.Err.Error()
	case what == "":
		what = string(e.Kind)
	}
	if e.Record <= 0 {
		return "icelake: " + what
	}

	return fmt.Sprintf("icelake: %s: %s", IngestPosition(e.Record, e.Line), what)
}

// Unwrap exposes the underlying refusal, so the typed error a caller matches on
// stays reachable through the stream wrapper.
func (e IngestError) Unwrap() error { return e.Err }
