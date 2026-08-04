package schemamap

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array/arreflect"

	"github.com/gvkhna/icelake/internal/errdef"
)

// adaptKind names what may happen to one column between the type arreflect
// infers from the Go struct and the type the column is wanted at. It has five
// members and the const block below declares all five: the loud default
// [adaptUnsupported], the identity, and three conversions —
// [adaptTimestampMillisUTC], [adaptDecimal128] and [adaptTimestampMicrosUTC].
//
// Three conversions, two whitelists, and the two numbers are different on
// purpose. [adaptationFor] answers "may a caller declare this?" and can return
// only two of the three: the millisecond re-wrap and the decimal
// sign-extension. [storageAdaptationFor] answers "may icelake write this?" and
// can return all three, because the microsecond rescale is not a caller's
// choice at all — it is what [FileSchema] has already asked for by the time a
// batch is encoded. So no single count describes both, and each is stated with
// its own below rather than one of them standing in for the type.
//
// Corrected at the completeness audit's fifth round (2026-08-01): this comment
// used to say the whitelisted conversions were "exactly two, plus the identity,
// plus the loud default", directly above a const block that declares three of
// them. It is the twin of the sentence corrected in adapt.go on the same day —
// same package, thirty lines away, same count, and both went stale at the same
// moment. M4 added adaptTimestampMicrosUTC with its own constant here, its own
// arm in [AdaptColumn] and its own test, and neither of the two prose summaries
// sitting above a list was read again afterwards. This copy was the worse
// placed of the two: adapt.go's corrected paragraph now sends a reader to "the
// two different whitelists [storageAdaptationFor] documents", and
// storageAdaptationFor is declared directly beneath this sentence, so a reader
// following the corrected comment arrived at the uncorrected one. The round
// that corrected adapt.go enumerated the copies it knew of — SCHEMA.md in two
// places, PLAN.md's package list — and did not name this one, which is this
// audit's own recurring shape: the survey checked the documents and missed the
// file it was standing in. One nearby sentence is deliberately not changed,
// because it is not a third copy of the mistake: crossCheck's comment below
// requires per column "either an identical Arrow type or exactly one of the two
// whitelisted adaptations", and two is the true count there, since that
// function consults adaptationFor and never storageAdaptationFor.
type adaptKind int

const (
	// adaptUnsupported is the zero value on purpose: anything the whitelist
	// below does not name explicitly is a hard error, never a best-effort cast.
	adaptUnsupported adaptKind = iota
	// adaptIdentity means the two derivations already agree.
	adaptIdentity
	// adaptTimestampMillisUTC re-wraps an Int64 buffer as Timestamp(ms, UTC).
	adaptTimestampMillisUTC
	// adaptDecimal128 sign-extends each Int64 value into 16 bytes.
	adaptDecimal128
	// adaptTimestampMicrosUTC multiplies each Int64 millisecond value by 1000
	// into a microsecond timestamp. It is deliberately absent from
	// [adaptationFor] and reachable only through [storageAdaptationFor]; see
	// that function for why the two whitelists differ.
	adaptTimestampMicrosUTC
)

// adaptationFor is the single source of truth for what icelake will reinterpret
// and what it will refuse. It is consulted twice: once by the cross-check,
// before any row exists, and once per column by [AdaptColumn] when rows do —
// so the check and the conversion can never drift apart.
//
// Only two disagreements are adaptable, and both come from the same root
// cause: the schema path can express a Parquet logical type that the data path
// cannot, because arreflect infers from the Go type alone and a Go int64 is
// just an int64 to it.
//
//   - Int64 to Timestamp(ms, UTC): Arrow stores timestamps as int64, so this
//     is a re-wrap of the same buffer with a different type.
//   - Int64 to Decimal128(P, S): a real per-value sign-extension into 16 bytes.
//
// The timestamp case is deliberately pinned to milliseconds and UTC rather
// than accepting any timestamp type: those are the units SCHEMA.md's column
// decision declares, and a declaration asking for anything else should fail
// loudly rather than be silently reinterpreted at some other unit.
func adaptationFor(have, want arrow.DataType) adaptKind {
	if arrow.TypeEqual(have, want) {
		return adaptIdentity
	}
	if have.ID() != arrow.INT64 {
		return adaptUnsupported
	}
	switch w := want.(type) {
	case *arrow.TimestampType:
		if w.Unit == arrow.Millisecond && w.TimeZone == "UTC" {
			return adaptTimestampMillisUTC
		}
	case *arrow.Decimal128Type:
		return adaptDecimal128
	}
	return adaptUnsupported
}

// storageAdaptationFor is [adaptationFor] plus the one conversion that exists
// only on the way to a data file: an Int64 column of epoch milliseconds into a
// microsecond timestamp.
//
// The two whitelists differ on purpose, and the difference is the whole reason
// there are two. adaptationFor answers "may a caller declare this?", and there
// the timestamp case is pinned to milliseconds because that is the one unit
// SCHEMA.md's column decision declares — a declaration asking for anything else
// must fail loudly rather than be silently reinterpreted at some other unit.
// storageAdaptationFor answers "may icelake write this?", and there the
// microsecond widening is not a caller's choice at all: it is what Iceberg's
// timestamptz is defined as, applied by [FileSchema] to every declared
// millisecond column. Folding the two together would quietly make a
// micros-declaring struct legal.
func storageAdaptationFor(have, want arrow.DataType) adaptKind {
	if have.ID() == arrow.INT64 {
		if w, ok := want.(*arrow.TimestampType); ok && w.Unit == arrow.Microsecond && w.TimeZone == "UTC" {
			return adaptTimestampMicrosUTC
		}
	}
	return adaptationFor(have, want)
}

// crossCheck compares the two independent derivations of the same declaration
// struct and requires them to agree: identical column count, identical order,
// identical names, identical nullability, and per column either an identical
// Arrow type or exactly one of the two whitelisted adaptations.
//
// The two sides are written separately and read different struct-tag
// namespaces — parquet:"..." for the declared schema, arrow:"..." for the row
// data — so a column can be named one thing on one side and another on the
// other, or exist on one side and not the other, with nothing but this check
// standing between that and a table that fails on its first batch. Any
// disagreement names the column and fails the open.
//
// This also settles, per real table on every start, what would otherwise be an
// assumption: that arreflect's column order matches the declared order. It is
// no longer assumed; it is checked against the actual struct in use.
func crossCheck[T any](tableName string, declared *arrow.Schema) error {
	// An unknown option in an arrow tag fails here rather than at the declared
	// derivation, because only this side reads that tag namespace.
	inferred, err := arreflect.InferSchema[T]()
	if err != nil {
		return derivationError(tableName, "inferring arrow schema from row data", err)
	}

	declaredFields := declared.Fields()
	inferredFields := inferred.Fields()

	shorter := min(len(declaredFields), len(inferredFields))
	for i := range shorter {
		d, inf := declaredFields[i], inferredFields[i]

		if d.Name != inf.Name {
			return errdef.NewCrossCheckError(tableName, d.Name, fmt.Sprintf(
				"the two derivations disagree at column %d: the declared schema calls it %q, the row-data schema calls it %q — the parquet and arrow struct tags must name the identical column",
				i, d.Name, inf.Name))
		}
		if d.Nullable != inf.Nullable {
			return errdef.NewCrossCheckError(tableName, d.Name, fmt.Sprintf(
				"the two derivations disagree on nullability: declared nullable=%t, row-data nullable=%t",
				d.Nullable, inf.Nullable))
		}
		if adaptationFor(inf.Type, d.Type) == adaptUnsupported {
			return errdef.NewCrossCheckError(tableName, d.Name, fmt.Sprintf(
				"the two derivations disagree on type and no adaptation exists: declared %s, row-data %s (a declaration may only ask for int64 to timestamp[ms, tz=UTC] or int64 to decimal128; the microsecond widening Iceberg's timestamptz needs is applied by the write path, not declarable)",
				d.Type, inf.Type))
		}
	}

	switch {
	case len(declaredFields) > len(inferredFields):
		extra := declaredFields[shorter]
		return errdef.NewCrossCheckError(tableName, extra.Name, fmt.Sprintf(
			"column is declared (%d declared columns) but absent from the row-data schema (%d columns) — every field needs both a parquet and an arrow tag naming it",
			len(declaredFields), len(inferredFields)))
	case len(inferredFields) > len(declaredFields):
		extra := inferredFields[shorter]
		return errdef.NewCrossCheckError(tableName, extra.Name, fmt.Sprintf(
			"column is present in the row-data schema (%d columns) but absent from the declared schema (%d declared columns)",
			len(inferredFields), len(declaredFields)))
	}

	return nil
}
