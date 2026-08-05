package chmirror

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gvkhna/icelake/internal/canon"
)

// appendColumn hands one Arrow column to the driver's typed batch column.
//
// It is written column-wise rather than row-wise because that is the shape both
// sides already have: an Arrow record is columns, and the driver's batch is
// columns, so a row-wise loop would take both apart to put them back together.
//
// Two rules run through every case below and are the whole of why this file is
// a switch on the declared kind rather than a generic conversion.
//
// **A value is handed over as the Go type the ClickHouse column actually is**,
// never as something the server would be asked to interpret. The sharp case is
// timestamptz: ClickHouse reads a bare integer pushed at a DateTime64 as
// *seconds*, so a microsecond value that took that path would land in 1970 and
// look like a plausible timestamp. Here every timestamp travels as a
// [time.Time], built from the record's own microseconds, which the driver
// encodes at the column's own scale.
//
// **An optional column travels as a slice of pointers**, so a null is a nil and
// not a zero. That is the same hazard the Nullable(T) mapping exists for, one
// layer down: a null flattened to a zero here would be indistinguishable from a
// real zero forever, and no assertion downstream could tell them apart.
func appendColumn(dst driver.BatchColumn, f canon.Field, src arrow.Array) error {
	if src.Len() == 0 {
		return nil
	}

	switch f.Kind {
	case canon.KindBoolean:
		col, err := typed[*array.Boolean](f, src)
		if err != nil {
			return err
		}

		return appendValues(dst, f, col.Len(), col.IsNull, col.Value)
	case canon.KindInt:
		col, err := typed[*array.Int32](f, src)
		if err != nil {
			return err
		}

		return appendValues(dst, f, col.Len(), col.IsNull, col.Value)
	case canon.KindLong:
		col, err := typed[*array.Int64](f, src)
		if err != nil {
			return err
		}

		return appendValues(dst, f, col.Len(), col.IsNull, col.Value)
	case canon.KindFloat:
		col, err := typed[*array.Float32](f, src)
		if err != nil {
			return err
		}

		return appendValues(dst, f, col.Len(), col.IsNull, col.Value)
	case canon.KindDouble:
		col, err := typed[*array.Float64](f, src)
		if err != nil {
			return err
		}

		return appendValues(dst, f, col.Len(), col.IsNull, col.Value)
	case canon.KindDecimal:
		col, err := typed[*array.Decimal128](f, src)
		if err != nil {
			return err
		}

		return appendValues(dst, f, col.Len(), col.IsNull, func(i int) decimalValue {
			return unscaledToDecimal(col.Value(i), f.Scale)
		})
	case canon.KindTimestampTz:
		col, err := typed[*array.Timestamp](f, src)
		if err != nil {
			return err
		}

		return appendValues(dst, f, col.Len(), col.IsNull, func(i int) time.Time {
			return time.UnixMicro(int64(col.Value(i))).UTC()
		})
	case canon.KindString:
		col, err := typed[*array.String](f, src)
		if err != nil {
			return err
		}

		return appendValues(dst, f, col.Len(), col.IsNull, col.Value)
	case canon.KindBinary:
		col, err := typed[*array.Binary](f, src)
		if err != nil {
			return err
		}

		// Into a ClickHouse String, which is an arbitrary byte sequence. A Go
		// string is the same thing, so this conversion moves no bytes and
		// imposes no encoding — which is the point, since a binary column's
		// contents are by definition not text.
		return appendValues(dst, f, col.Len(), col.IsNull, func(i int) string {
			return string(col.Value(i))
		})
	default:
		return fmt.Errorf("column %q has %s, which the mirror has no ClickHouse type for", f.Name, f.Kind)
	}
}

// typed asserts an Arrow column to the array type its declared kind produces.
//
// Nothing on the flush path can reach the failure: the record is built at the
// file schema, whose types are derived from the same declaration this field
// came from. It is checked rather than asserted blind because the alternative
// is a panic on a background goroutine, and a mirror is never allowed to take a
// caller's process down.
func typed[T arrow.Array](f canon.Field, src arrow.Array) (T, error) {
	col, ok := src.(T)
	if !ok {
		var zero T

		return zero, fmt.Errorf("column %q is declared %s and the record holds %s", f.Name, f.Kind, src.DataType())
	}

	return col, nil
}

// appendValues builds the driver's slice for one column and hands it over: a
// slice of values for a required column, and a slice of pointers for an
// optional one so that a null is a nil.
func appendValues[T any](dst driver.BatchColumn, f canon.Field, n int, isNull func(int) bool, value func(int) T) error {
	if f.Required {
		values := make([]T, n)
		for i := range n {
			values[i] = value(i)
		}

		return dst.Append(values)
	}

	values := make([]*T, n)
	// One backing array for the values, so an optional column costs one
	// allocation for the pointers and one for what they point at rather than
	// one per non-null row.
	backing := make([]T, n)
	for i := range n {
		if isNull(i) {
			continue
		}
		backing[i] = value(i)
		values[i] = &backing[i]
	}

	return dst.Append(values)
}
