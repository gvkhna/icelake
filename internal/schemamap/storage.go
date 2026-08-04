package schemamap

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FileSchema returns the Arrow schema a table's Parquet data files are actually
// written with: the declared schema, with every millisecond UTC timestamp
// column widened to microseconds.
//
// The widening is not cosmetic and it is not optional. Iceberg's timestamptz is
// defined as microseconds since the epoch, so a data file that stored
// milliseconds under a column the table calls timestamptz would be read a
// thousand times too small by anything that trusts the table's own schema. The
// declaration deliberately keeps saying milliseconds — that is the precision
// the caller's int64 actually carries, and the column keeps its _ms name for
// the same reason — and the upcast happens here, at the one place bytes are
// written, exactly as ARCHITECTURE.md's canonical-encoding decision says it
// should ("the microsecond upcast Iceberg wants happens later, in the Parquet
// write path").
//
// Everything else is carried across untouched, including each field's
// nullability and its metadata — which is where the permanent field id lives,
// so the written file names its columns by the same ids the table does.
func FileSchema(declared *arrow.Schema) *arrow.Schema {
	fields := declared.Fields()
	out := make([]arrow.Field, len(fields))
	for i, f := range fields {
		out[i] = f
		if ts, ok := f.Type.(*arrow.TimestampType); ok && ts.Unit == arrow.Millisecond && ts.TimeZone == "UTC" {
			out[i].Type = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		}
	}

	md := declared.Metadata()

	return arrow.NewSchema(out, &md)
}

// rescaleTimestamp converts an Int64 column of epoch milliseconds into a
// microsecond timestamp column, multiplying each value by 1000.
//
// Unlike the millisecond re-wrap, this is a real per-value conversion, so it
// allocates a new buffer rather than sharing the input's. Null slots stay null.
//
// It moves bits and validates nothing, the same contract [AdaptColumn] states:
// a millisecond value large enough to overflow an int64 when multiplied is a
// value-domain problem, refused at the boundary where the caller supplied it,
// not silently repaired inside a batch conversion with no caller to return to.
func rescaleTimestamp(src *array.Int64, want *arrow.TimestampType, mem memory.Allocator) arrow.Array {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	b := array.NewTimestampBuilder(mem, want)
	defer b.Release()
	b.Reserve(src.Len())

	for i := range src.Len() {
		if src.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(arrow.Timestamp(src.Value(i) * 1000))
	}

	return b.NewArray()
}
