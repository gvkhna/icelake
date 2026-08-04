package schemamap

import (
	"errors"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/gvkhna/icelake/internal/errdef"
)

// Three of the four tests in this file are TESTING.md's first sanctioned
// category: a calculation whose exact output matters, asserted against
// independently stated expected values rather than against the code's own prior
// output re-run. The adapter is the only place icelake reinterprets a caller's
// data, so its exact bits are the claim — the sign extension into sixteen
// bytes, the re-wrap of int64 as millisecond timestamps, and the widening to
// microseconds that only happens on the way to a data file.
//
// TestAdaptColumnRefusesEveryOtherTypePair is not a calculation and does not
// belong in that category, which is closed. It is the second category, and it
// holds that category's second claim shape: a defence this layer keeps against
// its own in-process callers. In normal operation the default arm is
// unreachable, because crossCheck runs the same whitelist — minus the one
// microsecond widening that is not declarable — over the whole schema inside
// Declare, before a single row is accepted, and refuses the declaration at
// OpenWriter with the same typed error naming the same column; scenario 7's
// cross-check clause proves end to end that such a declaration fails OpenWriter
// rather than a batch. An end-to-end test could therefore only ever show that
// today's public path does not reach this arm. The invariant is
// the one AdaptColumn's own doc comment states: this is the only place icelake
// reinterprets a caller's data, and it is a bounded whitelist rather than an
// open-ended cast. Lose it and a future caller that skips the declaration check
// gets a best-effort conversion written into a committed data file, with
// nothing anywhere saying the values are no longer the ones that were handed
// in. It asserts the typed refusal such a caller would meet — errors.As on
// errdef.CrossCheckError with its Table and Column read off it, never the
// message — for all six pairs at once, one pair at a time, which is what makes
// it cheaper and more complete than any substrate could be.

// int64Column builds a source column shaped like what the data-path reflection
// produces: the given values in order, then one null slot.
func int64Column(t *testing.T, mem memory.Allocator, vals []int64) *array.Int64 {
	t.Helper()

	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, v := range vals {
		b.Append(v)
	}
	b.AppendNull()

	return b.NewInt64Array()
}

// TestAdaptColumnSignExtendsInt64ToDecimal128 pins the exact 128-bit output of
// the Int64-to-Decimal128 conversion, including the two's-complement
// representation of negative values and both int64 extremes. A decimal column
// exists in this design precisely because money must not round; a wrong
// sign-extension would corrupt every negative value silently, so the expected
// high and low words are written out here by hand rather than computed.
func TestAdaptColumnSignExtendsInt64ToDecimal128(t *testing.T) {
	cases := []struct {
		name   string
		in     int64
		wantHi int64
		wantLo uint64
	}{
		// A non-negative value has an all-zero high word and its own bit
		// pattern in the low word.
		{"zero", 0, 0, 0x0000000000000000},
		{"one", 1, 0, 0x0000000000000001},
		{"price 1.234567890 unscaled at scale 9", 1234567890, 0, 0x00000000499602D2},
		{"int64 max", math.MaxInt64, 0, 0x7FFFFFFFFFFFFFFF},

		// A negative value has an all-ones high word: the 128-bit two's
		// complement of -n is 2^128 - n, whose top 64 bits are all set.
		{"minus one", -1, -1, 0xFFFFFFFFFFFFFFFF},
		// 2^64 - 1234567890 == 0xFFFFFFFFB669FD2E.
		{"price -1.234567890 unscaled at scale 9", -1234567890, -1, 0xFFFFFFFFB669FD2E},
		// 2^64 - 2^63 == 2^63 == 0x8000000000000000.
		{"int64 min", math.MinInt64, -1, 0x8000000000000000},
	}

	vals := make([]int64, len(cases))
	for i, c := range cases {
		vals[i] = c.in
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	src := int64Column(t, mem, vals)
	defer src.Release()

	want := &arrow.Decimal128Type{Precision: 18, Scale: 9}
	adapted, err := AdaptColumn("fills", "price", src, want, mem)
	if err != nil {
		t.Fatalf("AdaptColumn: %v", err)
	}
	defer adapted.Release()

	if !arrow.TypeEqual(adapted.DataType(), want) {
		t.Fatalf("adapted column type = %s, want %s", adapted.DataType(), want)
	}
	if adapted.Len() != len(cases)+1 {
		t.Fatalf("adapted column length = %d, want %d", adapted.Len(), len(cases)+1)
	}

	out, ok := adapted.(*array.Decimal128)
	if !ok {
		t.Fatalf("adapted column is %T, want *array.Decimal128", adapted)
	}

	for i, c := range cases {
		if out.IsNull(i) {
			t.Errorf("%s: slot %d is null, want a value", c.name, i)
			continue
		}
		got := out.Value(i)
		if got.HighBits() != c.wantHi || got.LowBits() != c.wantLo {
			t.Errorf("%s: %d sign-extended to hi=%#016x lo=%#016x, want hi=%#016x lo=%#016x",
				c.name, c.in, uint64(got.HighBits()), got.LowBits(), uint64(c.wantHi), c.wantLo)
		}
	}

	// The null slot must survive the conversion as a null, not as a zero.
	if !out.IsNull(len(cases)) {
		t.Errorf("null slot %d came back as a value, want null", len(cases))
	}
	if out.NullN() != 1 {
		t.Errorf("adapted column null count = %d, want 1", out.NullN())
	}
}

// TestAdaptColumnRewrapsInt64AsTimestampMillisUTC proves the timestamp
// adaptation is a re-wrap and not a conversion: the same underlying int64
// values, the declared millisecond/UTC type, null slots intact, and the output
// pointing at the input's own value buffer rather than a copy of it. That last
// assertion is what makes "no value is touched" a checked property instead of
// a claim in a comment.
func TestAdaptColumnRewrapsInt64AsTimestampMillisUTC(t *testing.T) {
	vals := []int64{0, 1, 1700000000000, math.MaxInt64, math.MinInt64, -1}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	src := int64Column(t, mem, vals)
	defer src.Release()

	want := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
	adapted, err := AdaptColumn("fills", "venue_timestamp_ms", src, want, mem)
	if err != nil {
		t.Fatalf("AdaptColumn: %v", err)
	}
	defer adapted.Release()

	if !arrow.TypeEqual(adapted.DataType(), want) {
		t.Fatalf("adapted column type = %s, want %s", adapted.DataType(), want)
	}

	out, ok := adapted.(*array.Timestamp)
	if !ok {
		t.Fatalf("adapted column is %T, want *array.Timestamp", adapted)
	}
	if out.Len() != len(vals)+1 {
		t.Fatalf("adapted column length = %d, want %d", out.Len(), len(vals)+1)
	}

	for i, v := range vals {
		if out.IsNull(i) {
			t.Errorf("slot %d is null, want %d", i, v)
			continue
		}
		if got := int64(out.Value(i)); got != v {
			t.Errorf("slot %d = %d, want the untouched input %d", i, got, v)
		}
	}
	if !out.IsNull(len(vals)) {
		t.Errorf("null slot %d came back as a value, want null", len(vals))
	}

	// Buffer identity: the values buffer is the source's own, not a copy.
	if got, src := out.Data().Buffers()[1], src.Data().Buffers()[1]; got != src {
		t.Errorf("adapted column has a different values buffer (%p) from its source (%p); the timestamp adaptation must re-wrap, not copy", got, src)
	}
}

// TestAdaptColumnUpcastsMillisToMicros pins the one conversion that happens
// only on the way to a data file: a declared millisecond column becomes the
// microsecond timestamptz Iceberg defines, by multiplying each value by 1000.
//
// The expected values are derived independently — they are the inputs times
// 1000, written out — because getting this wrong is not a crash but a table
// whose every timestamp reads a thousand times too small.
func TestAdaptColumnUpcastsMillisToMicros(t *testing.T) {
	vals := []int64{0, 1, -1, 1700000000000}
	want := []int64{0, 1000, -1000, 1700000000000000}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	src := int64Column(t, mem, vals)
	defer src.Release()

	wantType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	adapted, err := AdaptColumn("fills", "venue_timestamp_ms", src, wantType, mem)
	if err != nil {
		t.Fatalf("AdaptColumn: %v", err)
	}
	defer adapted.Release()

	if !arrow.TypeEqual(adapted.DataType(), wantType) {
		t.Fatalf("adapted column type = %s, want %s", adapted.DataType(), wantType)
	}
	out, ok := adapted.(*array.Timestamp)
	if !ok {
		t.Fatalf("adapted column is %T, want *array.Timestamp", adapted)
	}
	for i, w := range want {
		if got := int64(out.Value(i)); got != w {
			t.Errorf("slot %d = %d microseconds, want %d", i, got, w)
		}
	}
	if !out.IsNull(len(vals)) {
		t.Errorf("null slot %d came back as a value, want null", len(vals))
	}
}

// TestAdaptColumnRefusesEveryOtherTypePair proves the adapter's default is
// loud. The whitelist is deliberately narrow — including on the timestamp
// zone, which is pinned to UTC — so anything else must fail rather than be
// reinterpreted.
//
// The microsecond unit is not in this list, and the asymmetry is the point:
// icelake itself widens a declared millisecond column to microseconds when it
// writes a data file, because that is what Iceberg's timestamptz is defined as.
// What stays refused is a *declaration* that asks for microseconds, which is
// checked at the declaration seam that owns it — see the cross-check cases in
// declare_test.go.
func TestAdaptColumnRefusesEveryOtherTypePair(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	int64Src := int64Column(t, mem, []int64{1, 2, 3})
	defer int64Src.Release()

	strBuilder := array.NewStringBuilder(mem)
	strBuilder.AppendValues([]string{"a", "b"}, nil)
	stringSrc := strBuilder.NewStringArray()
	strBuilder.Release()
	defer stringSrc.Release()

	cases := []struct {
		name string
		col  arrow.Array
		want arrow.DataType
	}{
		{"int64 to timestamp at an unsupported unit", int64Src, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{"int64 to timestamp with no zone", int64Src, &arrow.TimestampType{Unit: arrow.Millisecond}},
		{"int64 to float64", int64Src, arrow.PrimitiveTypes.Float64},
		{"int64 to string", int64Src, arrow.BinaryTypes.String},
		{"string to decimal128", stringSrc, &arrow.Decimal128Type{Precision: 18, Scale: 9}},
		{"string to binary", stringSrc, arrow.BinaryTypes.Binary},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			adapted, err := AdaptColumn("fills", "some_column", c.col, c.want, mem)
			if err == nil {
				adapted.Release()
				t.Fatalf("AdaptColumn(%s -> %s) succeeded, want a refusal", c.col.DataType(), c.want)
			}

			var ce errdef.CrossCheckError
			if !errors.As(err, &ce) {
				t.Fatalf("error is %T, want errdef.CrossCheckError", err)
			}
			if ce.Table != "fills" {
				t.Errorf("CrossCheckError.Table = %q, want %q", ce.Table, "fills")
			}
			if ce.Column != "some_column" {
				t.Errorf("CrossCheckError.Column = %q, want %q", ce.Column, "some_column")
			}
		})
	}
}
