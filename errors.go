package icelake

import "github.com/gvkhna/icelake/internal/errdef"

// Every error that crosses this package's boundary is matchable with
// errors.Is or errors.As — no bare fmt.Errorf ever escapes, because a
// fail-loudly promise that can only be tested by grepping a message is a
// promise a test suite cannot hold.
//
// The typed errors are declared in an internal leaf package and re-exported
// here as aliases. An alias is the same type, not a copy, so
// errors.As(err, &icelake.NameError{}) matches an error constructed anywhere
// inside the library, and the internal packages that construct them never have
// to import this one.

// NameKind identifies which of the two name positions a [NameError] refers to:
// the namespace or the table.
type NameKind = errdef.NameKind

const (
	// NameKindNamespace marks a rejected Iceberg namespace name.
	NameKindNamespace = errdef.NameKindNamespace
	// NameKindTable marks a rejected table name.
	NameKindTable = errdef.NameKindTable
)

// NameError reports a namespace or table name icelake refuses. Names must
// match ^[a-z0-9][a-z0-9_]*$ and be at most 63 bytes, because the warehouse
// key layout gives each table exactly two path segments and catalog rebuild
// discovers tables by parsing those segments back out. Kind says which name
// was bad; Value is the offending name, never normalized.
type NameError = errdef.NameError

// PoisonReason says which representability rule an [Insert]-refused value
// broke: a string that is not valid UTF-8, a decimal that does not fit its
// declared precision, or a value longer than the canonical encoding can
// address.
//
// It is an enumeration rather than free prose so that a test can assert which
// refusal fired without matching message text.
type PoisonReason = errdef.PoisonReason

const (
	// PoisonReasonInvalidUTF8 marks a string value that is not valid UTF-8,
	// which Parquet's String logical type cannot hold.
	PoisonReasonInvalidUTF8 = errdef.PoisonReasonInvalidUTF8
	// PoisonReasonDecimalRange marks an unscaled decimal value with more digits
	// than its column's declared precision holds.
	PoisonReasonDecimalRange = errdef.PoisonReasonDecimalRange
	// PoisonReasonTooLong marks a string or binary value longer than the
	// canonical encoding's four-byte length prefix can address.
	PoisonReasonTooLong = errdef.PoisonReasonTooLong
)

// PoisonError reports a record [Writer.Insert] refused because one field's
// value is not representable in the column it is declared as. Table and Field
// name the offending column, Reason says which rule it broke. The record never
// enters staging and the writer stays usable, so the caller can fix or drop
// exactly that record.
type PoisonError = errdef.PoisonError

// DeclarationKind says what is wrong with a declaration struct: it is not a
// struct, it has no columns, it carries an unexported field, or the derivation
// chain itself failed.
type DeclarationKind = errdef.DeclarationKind

const (
	// DeclarationKindNotStruct marks a declaration type that is not a struct.
	DeclarationKindNotStruct = errdef.DeclarationKindNotStruct
	// DeclarationKindNoFields marks a declaration that would produce a
	// zero-column table.
	DeclarationKindNoFields = errdef.DeclarationKindNoFields
	// DeclarationKindUnexportedField marks a declaration carrying an
	// unexported field.
	DeclarationKindUnexportedField = errdef.DeclarationKindUnexportedField
	// DeclarationKindDerivation marks a failure inside the struct-to-Iceberg
	// derivation chain, such as a malformed struct tag.
	DeclarationKindDerivation = errdef.DeclarationKindDerivation
	// DeclarationKindUnsupportedGoType marks a declaration field whose Go type
	// icelake does not declare columns for. Declared columns are bool, int32,
	// int64, float32, float64, string or []byte, or a pointer to one for an
	// optional column. A plain int or uint is refused because its width, and
	// therefore the column type it would derive to, depends on the platform the
	// binary was built for — which would reach the durable schema fingerprint
	// and batch keys.
	DeclarationKindUnsupportedGoType = errdef.DeclarationKindUnsupportedGoType
)

// DeclarationError reports a declaration struct icelake cannot turn into a
// table schema at all: not a struct, no columns, an unexported field, a field
// whose Go type is outside the declarable whitelist, or a failure inside the
// derivation chain itself. Kind says which, Field names the offending field
// where one is at fault, and Err carries the wrapped library error for
// derivation failures.
type DeclarationError = errdef.DeclarationError

// CrossCheckError reports a declaration struct that the startup cross-check
// refuses: the two reflection layers that read the struct — one for the
// declared schema, one for the row data — disagree about the named column's
// name, nullability or type in a way icelake will not adapt. It also carries
// the rejection of a declared struct containing a nested group, which the
// flat-only reconciliation scope does not support.
type CrossCheckError = errdef.CrossCheckError

// StagingSchemaKind says why a staged row's recorded schema shape cannot be
// mapped into the current declaration: a field the declaration dropped, a field
// it requires that the staged row has no value for, or a field whose type
// changed.
type StagingSchemaKind = errdef.StagingSchemaKind

const (
	// StagingSchemaKindFieldRemoved marks a field id in the recorded shape that
	// the current declaration no longer has.
	StagingSchemaKindFieldRemoved = errdef.StagingSchemaKindFieldRemoved
	// StagingSchemaKindFieldRequired marks a field the current declaration
	// requires for which the staged row has no value.
	StagingSchemaKindFieldRequired = errdef.StagingSchemaKindFieldRequired
	// StagingSchemaKindFieldRetyped marks a field id whose type differs between
	// the recorded shape and the current declaration.
	StagingSchemaKindFieldRetyped = errdef.StagingSchemaKindFieldRetyped
)

// StagingSchemaError reports staged rows whose recorded schema shape cannot be
// mapped into the current declaration. Replay decodes every staged row by the
// shape recorded alongside it and maps the values in by permanent field id; a
// field the declaration added since is simply null. The cases that cannot be
// mapped honestly fail here instead, naming the field id and which case it was,
// so the operator drains the table with the previous binary rather than losing
// or inventing a column's staged values.
//
// That last instruction has one exception, recorded here at the completeness
// audit's fifth round (2026-08-01) because this is where a caller reads the
// advice. For [StagingSchemaKindFieldRetyped] reached through a widening the
// schema reconciliation accepts — int32 to int64, say — the widening is already
// committed against the live table by the time the staged rows are checked, so
// the previous binary's narrower declaration is refused as a narrowing and
// neither binary can open the table. The staged rows have to come out of
// `staging.db` by hand, the way quarantined rows do. The other two kinds
// reconcile cleanly under the previous binary and the advice holds for them.
type StagingSchemaError = errdef.StagingSchemaError

// StagingKind says which part of the local staging store failed: it would not
// open or configure, a schema migration failed, a read failed, a write failed,
// or the database is not in the state the operation requires.
type StagingKind = errdef.StagingKind

const (
	// StagingKindOpen marks a staging database that could not be opened, one of
	// whose required pragmas did not take effect, or whose path is one the
	// SQLite driver would misread.
	StagingKindOpen = errdef.StagingKindOpen
	// StagingKindClose marks a staging database that did not close cleanly.
	StagingKindClose = errdef.StagingKindClose
	// StagingKindMigrate marks a failed schema migration. Startup stops rather
	// than serving writes against a database in an unknown schema state.
	StagingKindMigrate = errdef.StagingKindMigrate
	// StagingKindRead marks a failed read against the staging database.
	StagingKindRead = errdef.StagingKindRead
	// StagingKindWrite marks a failed write against the staging database. The
	// record is not accepted; the caller still owns it.
	StagingKindWrite = errdef.StagingKindWrite
	// StagingKindState marks a staging database that is not in the state the
	// operation requires — a row already sealed into another batch, a row that
	// is gone, or a staged row naming a fingerprint with no recorded descriptor.
	StagingKindState = errdef.StagingKindState
)

// StagingError reports a failure of the local write-ahead staging store, the
// SQLite database every record is durably written to before Insert returns. A
// failure here is never absorbed into an in-memory buffer: it fails the call
// that provoked it, and the caller keeps the record. Path names the staging
// database, Kind says which part failed, and Err carries the wrapped SQLite,
// goose or query error.
type StagingError = errdef.StagingError

// EncodingKind says which layer of icelake's canonical record encoding refused:
// a declared column type it cannot encode, a value it will not encode, stored
// bytes it cannot read back, or a batch with no records.
type EncodingKind = errdef.EncodingKind

const (
	// EncodingKindUnsupportedType marks a declared column type the canonical
	// encoding has no representation for.
	EncodingKindUnsupportedType = errdef.EncodingKindUnsupportedType
	// EncodingKindValue marks a value the canonical encoding cannot write for
	// its declared column.
	EncodingKindValue = errdef.EncodingKindValue
	// EncodingKindFormat marks stored bytes that do not conform to the
	// canonical format.
	EncodingKindFormat = errdef.EncodingKindFormat
	// EncodingKindEmptyBatch marks an attempt to take a batch content hash over
	// zero records.
	EncodingKindEmptyBatch = errdef.EncodingKindEmptyBatch
)

// EncodingError reports a failure in icelake's own canonical record encoding —
// the byte form a record is staged in, and the form a batch's content hash, and
// therefore its object key, is computed over. Kind says which layer refused;
// Field and FieldID name the column where one column is at fault.
type EncodingError = errdef.EncodingError

// FieldIDError reports a declared field ID that breaks the field-ID rules: a
// field with no fieldid tag (reported as FieldID == -1), or, when creating a
// table, an ID that does not equal the field's position in a pre-order
// traversal of the declared schema. WantID carries the number the traversal
// expects. This exists because table creation renumbers fields in pre-order
// silently, so a disagreeing declaration would create a table whose IDs it can
// never match again.
type FieldIDError = errdef.FieldIDError

// ErrClosed reports an operation on a closed writer or store. Insert returns it
// once Close has stopped the writer accepting records; everything accepted
// before that was drained by Close itself.
var ErrClosed = errdef.ErrClosed

// ErrStagingFull reports that the staging ceiling has been reached and the
// record was not accepted. It is the loud backstop for a table that cannot
// reach object storage, never the intended flow-control mechanism — a caller
// that wants to throttle itself watches Pending instead. The refused record
// never enters staging, so the caller still owns it.
var ErrStagingFull = errdef.ErrStagingFull

// ErrTableInUse reports a second writer opened for a table that already has a
// live one in the same store. One writer per table is a standing assumption:
// two writers on one table would interleave batches of the same staged rows and
// commit against each other's metadata. Across processes the rule is
// operational and unenforceable from here; within one store it is checked.
// Closing a writer releases its table, so a table can be reopened afterwards.
var ErrTableInUse = errdef.ErrTableInUse

// RecordKind says why the dynamic writer refused a JSON row: a key the
// declaration does not have, a required column with nothing in it, a JSON type
// the column cannot hold, a value that could only be stored by losing something,
// a value outside the column's range, or text that does not parse.
type RecordKind = errdef.RecordKind

const (
	// RecordKindUnknownField marks a key the declaration does not have.
	RecordKindUnknownField = errdef.RecordKindUnknownField
	// RecordKindMissing marks a required column the row has no key for.
	RecordKindMissing = errdef.RecordKindMissing
	// RecordKindNull marks a null in a required column.
	RecordKindNull = errdef.RecordKindNull
	// RecordKindType marks a JSON value whose type cannot supply the declared
	// column at all — a number for a boolean, a float for a decimal.
	RecordKindType = errdef.RecordKindType
	// RecordKindInexact marks a value that could only be stored by losing
	// something: a non-integral number for an integer column, a float64 past
	// 2^53, a decimal with more fraction digits than its scale, a timestamp
	// carrying microseconds.
	RecordKindInexact = errdef.RecordKindInexact
	// RecordKindRange marks a value outside the declared column's range.
	RecordKindRange = errdef.RecordKindRange
	// RecordKindMalformed marks text that does not parse as what the column
	// requires, and a line that is not a JSON object at all.
	RecordKindMalformed = errdef.RecordKindMalformed
)

// RecordError reports a JSON row [DynamicWriter.InsertJSON] or
// [DynamicWriter.Insert] refused. It is the runtime-schema counterpart of
// [PoisonError] and a separate type because the two say different things: a
// PoisonError says a well-typed Go value cannot live in its declared column,
// this says the caller's JSON does not describe a row of this table. Nothing
// enters staging, so the caller still owns the row. Namespace, Table and Field
// name the column at fault — Field is empty when the row as a whole is — and
// Kind says which rule was broken. What was actually seen is in the message
// only, so that no test comes to depend on that wording; assertions go through
// Kind and Field. SCHEMA.md's coercion table is the full list of what is
// accepted.
type RecordError = errdef.RecordError

// SchemaDocKind says which rule of the schema document format was broken: its
// syntax (including an unknown key), a duplicate namespace/table pair, a table
// with no columns, a duplicate column name, an unknown type spelling, or a
// malformed decimal(P,S).
type SchemaDocKind = errdef.SchemaDocKind

const (
	// SchemaDocKindSyntax marks a document that is not the JSON this format is,
	// including one carrying a key the format does not define.
	SchemaDocKindSyntax = errdef.SchemaDocKindSyntax
	// SchemaDocKindDuplicateTable marks a namespace and table pair declared
	// twice in one document.
	SchemaDocKindDuplicateTable = errdef.SchemaDocKindDuplicateTable
	// SchemaDocKindNoFields marks a table declaring no columns.
	SchemaDocKindNoFields = errdef.SchemaDocKindNoFields
	// SchemaDocKindDuplicateField marks two columns of one table sharing a name.
	SchemaDocKindDuplicateField = errdef.SchemaDocKindDuplicateField
	// SchemaDocKindUnknownType marks a type spelling outside the nine.
	SchemaDocKindUnknownType = errdef.SchemaDocKindUnknownType
	// SchemaDocKindDecimal marks a malformed decimal(P,S).
	SchemaDocKindDecimal = errdef.SchemaDocKindDecimal
)

// SchemaDocError reports a schema document [ParseSchemaDocument] will not
// accept. A document is judged whole and one with anything wrong in it opens
// nothing. Field-ID violations inside a document are deliberately not this type
// and reuse [FieldIDError], because the rule being broken is the same permanence
// rule a struct declaration breaks; names reuse [NameError] for the same reason.
type SchemaDocError = errdef.SchemaDocError

// ErrLocalOnly reports a [Store] method that needs a bucket, called against a
// store opened with [Config.LocalOnly] set. Such a store built no object-storage
// client and opened no catalog, so the honest answer is the mode rather than
// whatever a nil client would have said. [Store.DrainSpool] is the only method
// that returns it; catalog rebuild is deliberately not one, because it opens its
// own client and never sees a Store at all.
var ErrLocalOnly = errdef.ErrLocalOnly

// SpoolKind says which part of the local Parquet spool failed: the bytes would
// not land on disk, the file landed and its row did not, a committed file could
// not be marked committed, or a backlog file could not be drained into the
// bucket.
type SpoolKind = errdef.SpoolKind

const (
	// SpoolKindWrite marks bytes that could not be written to the cache
	// directory. In bucket mode the flush fails and the batch stays staged; in
	// local-only mode this is the flush's only durable act, so its failure is
	// the flush's failure.
	SpoolKindWrite = errdef.SpoolKindWrite
	// SpoolKindRecord marks a spool file that landed while its database row did
	// not, leaving a file no sweep will evict and no drain will find until the
	// batch's own retry rewrites both.
	SpoolKindRecord = errdef.SpoolKindRecord
	// SpoolKindMark marks a batch whose upload and commit both succeeded and
	// whose spool file could not then be marked committed. It is the safe
	// direction: the file is merely unevictable.
	SpoolKindMark = errdef.SpoolKindMark
	// SpoolKindDrain marks a backlog file that could not be uploaded, committed
	// or reconciled to, which blocks that one table's open.
	SpoolKindDrain = errdef.SpoolKindDrain
)

// SpoolError reports a failure of the local Parquet spool: the cache directory
// [Config.CacheDir] names, and the rows in the staging database that record what
// is in it. It reaches a caller from the two paths that have one — a flush, and
// the transition drain a bucket-mode [OpenWriter] runs over a local-only
// backlog. A retention sweep deliberately reports to nobody and so has no kind
// here. Dir and Path name the directory and the file, BatchKey names the batch,
// and Err carries the wrapped filesystem, database or commit error.
type SpoolError = errdef.SpoolError

// ConfigFieldError reports one configuration field that fails one rule. Field
// is the field name as a caller spells it in Go. Every violation in a Config
// produces one of these, joined inside a single [ConfigError].
type ConfigFieldError = errdef.ConfigFieldError

// ConfigError reports an invalid configuration, wrapping the join of one
// [ConfigFieldError] per violation so that a caller fixing a config file learns
// everything wrong with it in one run. Open returns it before any file is
// touched or any network call is made.
type ConfigError = errdef.ConfigError

// CatalogKind says which part of the local catalog database failed: it would
// not open, it would not close, or a write against it did not land.
type CatalogKind = errdef.CatalogKind

const (
	// CatalogKindOpen marks a catalog database that could not be opened,
	// configured, or checked for existence.
	CatalogKindOpen = errdef.CatalogKindOpen
	// CatalogKindClose marks a catalog database that did not close cleanly.
	CatalogKindClose = errdef.CatalogKindClose
	// CatalogKindWrite marks a failed write against the catalog database or the
	// file holding it, including the atomic replacement that puts a rebuilt
	// catalog in place. Only catalog rebuild produces it: every other write to
	// this file goes through iceberg-go's commit path and is reported as a
	// [ReconcileError], because it concerns one declared table.
	CatalogKindWrite = errdef.CatalogKindWrite
)

// CatalogError reports a failure of the local catalog database, the SQLite file
// that records which metadata file is current for each table. It covers that
// file as a file — opening it, closing it, and the writes catalog rebuild makes
// into it; a failure that concerns one declared table is a [ReconcileError].
// Losing this file is losing a cache, not data: it is rebuildable from the
// bucket.
type CatalogError = errdef.CatalogError

// ReconcileError reports a failure of one table's startup lifecycle — creating
// it, loading it, diffing the declared shape against the live one, or
// committing the resulting schema change. It blocks that one table: OpenWriter
// returns it and no usable writer, and every other table in the process is
// unaffected. Err carries the underlying iceberg-go error, so a change the
// library itself refused as unsafe stays reachable.
type ReconcileError = errdef.ReconcileError

// FlushError reports that one batch could not be encoded, uploaded or
// committed. Nothing is lost: the batch stays in staging under its own key and
// is retried on the next flush trigger or by the next run — or, if it cannot be
// encoded at all, is quarantined there for an operator rather than retried
// forever. Flush and Close return it synchronously — including, exactly once, a
// quarantine that happened earlier with nobody waiting — and
// [Config.OnFlushError] receives it in the background; BatchKey names both the
// batch and the object it uploads to, and Attempts is what the failing cycle
// spent.
type FlushError = errdef.FlushError

// MirrorError reports that a table's [TableConfig.OnAccept] callback returned
// an error. The record it concerns was accepted: it was durably staged before
// the callback ran and is committed like any other, so this says only that the
// caller's own mirror fell behind. It is a distinct type precisely because the
// caller's next move is the opposite of a refusal's — re-inserting the record
// on this error would write it twice. Err is what the callback returned,
// wrapped, so the caller's own error values stay reachable.
type MirrorError = errdef.MirrorError

// BatchError reports which row of a batch [Writer.InsertBatch] or
// [DynamicWriter.InsertJSONBatch] refused, wrapping that row's own refusal.
// Index is the row's position in the slice the caller passed in, and Err is the
// error that row would have earned from the single-record door — a
// [PoisonError], a [RecordError], an [EncodingError] — wrapped, so errors.Is
// and errors.As still reach it. Nothing from a refused batch enters staging.
// The batch-wide refusals ([ErrClosed], [ErrStagingFull], a [StagingError]) are
// deliberately not wrapped in this, because no row is at fault.
type BatchError = errdef.BatchError
