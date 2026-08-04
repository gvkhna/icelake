package schemamap

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/gvkhna/icelake/internal/errdef"
)

// AdaptColumn converts one column produced by the data-path reflection into
// the type the declared schema gives it.
//
// This is the only place icelake reinterprets a caller's data, and it is
// deliberately one switch with an identity arm, three conversion arms and a
// loud default. arrow-go offers no "build this Arrow record from these Go
// values according to this existing schema" entry point — the data path infers
// its types from the Go types alone and takes no target schema — so this
// conversion is unavoidable, but it is bounded by an explicit whitelist rather
// than being an open-ended cast:
//
//   - Int64 to Timestamp(ms, UTC) re-wraps the same buffer with a different
//     type. Arrow stores timestamps as int64, so no value is touched and the
//     output shares the input's value buffer.
//   - Int64 to Timestamp(us, UTC) multiplies each value by 1000. This is the
//     one conversion that exists only on the way to a data file, because
//     Iceberg's timestamptz is defined in microseconds while the declaration
//     says milliseconds; see [FileSchema] and [storageAdaptationFor].
//   - Int64 to Decimal128(P, S) sign-extends each value into 16 bytes.
//   - Types that already agree pass through unchanged.
//
// The two timestamp arms are not alternatives a column chooses between: they
// belong to the two different whitelists [storageAdaptationFor] documents. The
// millisecond re-wrap is what a declaration may ask for; the microsecond
// rescale is what the write path always ends up asking for, because the flush
// path adapts against [FileSchema]'s output and that schema has already
// replaced every declared millisecond UTC column with a microsecond one. So the
// cheap arm is the declarable one and the arm that reaches a data file is the
// one that costs a pass over every value and a new buffer.
//
// Corrected at the completeness audit's fifth round (2026-08-01): the sentence
// above called this "one switch with two cases and a loud default", which the
// bullet list directly beneath it had already contradicted since M4, when the
// microsecond arm was added. SCHEMA.md was repeating the same count in two
// places, its adapter decision and its toolchain summary, and both were
// corrected in the same round; PLAN.md's package list carries it too and was
// owned by someone else in that round. Worth noticing about how this survived:
// the count was written when it was true, the arm that falsified it was added
// with its own bullet and its own test, and nobody re-read the paragraph the
// bullet was hanging off. A prose summary sitting above a list is exactly where
// a count goes stale, because the list is what an author edits.
//
// Amended at that round's repair pass (2026-08-01), because the enumeration
// above was one short and the copy it missed was in this package. It named
// SCHEMA.md and PLAN.md; it did not name crosscheck.go, whose adaptKind doc
// comment said the whitelisted conversions were "exactly two, plus the
// identity, plus the loud default" directly above a const block declaring
// three. Same sentence, same M4 staleness, thirty lines away — and standing
// directly above the [storageAdaptationFor] the paragraph above sends a reader
// to, so following this corrected comment landed on the uncorrected one. It is
// corrected there now, in its own words, and PLAN.md's copy was closed in the
// same pass rather than left to another owner. What is instructive is where the
// survey looked: it swept the design documents, found two of the three copies,
// and did not grep the package the correction was being written into.
//
// Any other type pair is a hard error naming the column, not a best-effort
// cast. In normal operation that default is unreachable, because [Declare] runs
// the declaration-time whitelist over the whole schema before a single row is
// accepted, and the write-time whitelist this function switches on is that same
// list plus the one widening [FileSchema] applies; it is here so that a future
// caller that skips that check fails loudly instead of writing something wrong.
//
// Contract: this function moves bits and validates nothing about the values
// themselves. It assumes every value has already been checked against the
// declared column — in particular that a decimal-bound value fits the declared
// precision, and that the caller has scaled it into the declared scale. An
// int64 too large for a DECIMAL(5,0) column is sign-extended here without
// complaint. That check belongs at the boundary where the caller supplied the
// record and the context to fix it exists: it is Insert's per-field poison
// check, which rejects the record before it is ever staged. Splitting it that
// way is deliberate — a value problem must be refused where it entered, not
// discovered inside a batch conversion that has no caller to return to.
//
// The returned array is owned by the caller and must be released. A
// pass-through column is retained before it is returned, so every result can
// be released uniformly regardless of which branch produced it. mem may be
// nil, in which case the default allocator is used.
func AdaptColumn(tableName, column string, col arrow.Array, want arrow.DataType, mem memory.Allocator) (arrow.Array, error) {
	switch storageAdaptationFor(col.DataType(), want) {
	case adaptIdentity:
		col.Retain()
		return col, nil

	case adaptTimestampMillisUTC:
		return rewrapAsTimestamp(col.(*array.Int64), want.(*arrow.TimestampType)), nil

	case adaptTimestampMicrosUTC:
		return rescaleTimestamp(col.(*array.Int64), want.(*arrow.TimestampType), mem), nil

	case adaptDecimal128:
		return signExtendToDecimal128(col.(*array.Int64), want.(*arrow.Decimal128Type), mem), nil

	default:
		return nil, errdef.NewCrossCheckError(tableName, column, fmt.Sprintf(
			"no adaptation from row-data type %s to target type %s (int64 adapts to timestamp[ms, tz=UTC], to timestamp[us, tz=UTC] and to decimal128; nothing else is adapted)",
			col.DataType(), want))
	}
}

// rewrapAsTimestamp reinterprets an Int64 column as a timestamp column without
// touching a single value: Arrow's timestamp arrays are backed by the same
// int64 buffer layout, so the output points at the input's validity and value
// buffers directly. Null slots survive because the validity buffer and null
// count come across with them.
func rewrapAsTimestamp(src *array.Int64, want *arrow.TimestampType) arrow.Array {
	d := src.Data()
	// NewData retains each buffer it is handed, so the result stays valid
	// independently of the source array's own lifetime.
	rewrapped := array.NewData(want, d.Len(), d.Buffers(), nil, d.NullN(), d.Offset())
	defer rewrapped.Release()
	return array.MakeFromData(rewrapped)
}

// signExtendToDecimal128 widens each int64 into a two's-complement 128-bit
// value. Unlike the timestamp case this is a real per-value conversion: the
// high 64 bits are all zeros for a non-negative value and all ones for a
// negative one, which is exactly what an arithmetic right shift by 63 yields.
// The low 64 bits are the original value's bit pattern.
//
// The caller is responsible for having scaled its values into the declared
// scale already; this function moves bits, it does not rescale.
func signExtendToDecimal128(src *array.Int64, want *arrow.Decimal128Type, mem memory.Allocator) arrow.Array {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	b := array.NewDecimal128Builder(mem, want)
	defer b.Release()
	b.Reserve(src.Len())

	for i := range src.Len() {
		if src.IsNull(i) {
			b.AppendNull()
			continue
		}
		v := src.Value(i)
		b.Append(decimal128.New(v>>63, uint64(v)))
	}

	return b.NewArray()
}
