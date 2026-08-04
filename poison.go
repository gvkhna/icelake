package icelake

import (
	"fmt"
	"unicode/utf8"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/errdef"
)

// pow10 holds 10^n for every precision an int64-backed decimal column can
// declare. Index n is the exclusive bound on an unscaled value with n digits,
// and the table stops at 18 because that is the widest precision the
// declaration path and the canonical descriptor allow — 10^18 is the last power
// of ten an int64 holds.
var pow10 = [...]int64{
	1,
	10,
	100,
	1_000,
	10_000,
	100_000,
	1_000_000,
	10_000_000,
	100_000_000,
	1_000_000_000,
	10_000_000_000,
	100_000_000_000,
	1_000_000_000_000,
	10_000_000_000_000,
	100_000_000_000_000,
	1_000_000_000_000_000,
	10_000_000_000_000_000,
	100_000_000_000_000_000,
	1_000_000_000_000_000_000,
}

// checkPoison is Insert's per-field poison check: the refusal of a record whose
// values are legal in their own right but not representable in the columns they
// are declared as.
//
// It runs on the bound row — after whichever codec turned a caller's record into
// canonical values, and before those values are encoded and staged — so a
// refused record never reaches the staging database and the caller keeps a
// record it can still fix. It takes the row and the shape rather than hanging
// off one codec, because both front doors run it: a struct writer and a dynamic
// writer produce the same canonical values for the same table, and the question
// this asks is about the values.
//
// For two of the three cases below, this is not the first line of defence — it
// is the only one, and that is worth stating plainly because it sets how
// carefully this function has to be right. Nothing beneath it validates a
// value's domain: the canonical encoding moves bits and says so, and the column
// adapter documents that it assumes this check already ran. An invalid-UTF-8
// string or an out-of-precision decimal that got past here would encode, stage,
// upload and commit with no layer objecting, and would sit in the Parquet file
// as a value the column's own type says it cannot hold. The batch quarantine
// ARCHITECTURE.md describes as the belt-and-braces second line does not cover
// them either: it catches batches that fail to *encode*, and these two encode
// perfectly. Only the over-long case is refused again lower down, by the
// encoder's four-byte length prefix.
//
// What it checks, which is exactly what ARCHITECTURE.md's poison decision
// names:
//
//   - A string value that is not valid UTF-8. Parquet's String logical type is
//     defined as UTF-8, so such a value has no honest representation.
//   - A decimal value whose unscaled magnitude needs more digits than the
//     declared precision. Nothing downstream would notice: the adapter
//     sign-extends whatever int64 it is given into 16 bytes, and the reader is
//     then told the column holds a number it says it cannot.
//   - A string or binary value longer than the canonical encoding's four-byte
//     length prefix can address.
//
// What it deliberately does not check, so that the list above stays the whole
// contract rather than a starting point:
//
//   - Nothing about a value's meaning: a timestamp far outside any plausible
//     range, an application-level enum column holding an unknown string, a
//     negative quantity. Every one of those is perfectly representable in its
//     declared column, and a library that guessed at them would be enforcing
//     the caller's business rules from the wrong side of the boundary.
//   - Scale. A decimal's value is the unscaled integer the caller already
//     scaled, and no scale is wrong for it — only its digit count can be.
//   - A null in a required column, or a value of the wrong Go type for its
//     column. Both are unrepresentable rather than out of range, the binder and
//     the canonical encoder already refuse them by construction with errors
//     that name the field, and re-checking them here would be a second opinion
//     about a case that cannot arise.
//
// The row is the one a codec just produced, so its values are already at the
// exact Go types their columns accept; a value that somehow is not is left alone
// for the encoder to refuse, rather than reported as a poison it is not.
func checkPoison(table string, desc *canon.Descriptor, row canon.Row) error {
	columns := desc.Fields()
	if len(row) != len(columns) {
		return nil
	}

	for i, column := range columns {
		if row[i] == nil {
			continue
		}

		switch column.Kind {
		case canon.KindString:
			s, ok := row[i].(string)
			if !ok {
				continue
			}
			if !utf8.ValidString(s) {
				return errdef.NewPoisonError(errdef.PoisonReasonInvalidUTF8, table, column.Name,
					"value is not valid UTF-8, which is what a string column is defined to hold")
			}
			if err := checkLen(table, column, len(s)); err != nil {
				return err
			}

		case canon.KindBinary:
			raw, ok := row[i].([]byte)
			if !ok {
				continue
			}
			if err := checkLen(table, column, len(raw)); err != nil {
				return err
			}

		case canon.KindDecimal:
			v, ok := row[i].(int64)
			if !ok {
				continue
			}
			if !fitsPrecision(v, column.Precision) {
				return errdef.NewPoisonError(errdef.PoisonReasonDecimalRange, table, column.Name, fmt.Sprintf(
					"unscaled value %d does not fit decimal(%d, %d), whose unscaled values run from %d to %d",
					v, column.Precision, column.Scale, -(pow10[column.Precision]-1), pow10[column.Precision]-1))
			}
		}
	}

	return nil
}

// checkLen refuses a string or binary value longer than the canonical
// encoding's length prefix can address.
func checkLen(table string, column canon.Field, n int) error {
	if uint64(n) <= uint64(canon.MaxValueBytes) {
		return nil
	}

	return errdef.NewPoisonError(errdef.PoisonReasonTooLong, table, column.Name, fmt.Sprintf(
		"value is %d bytes, longer than the %d the canonical encoding can address", n, canon.MaxValueBytes))
}

// fitsPrecision reports whether an unscaled decimal value fits a declared
// precision — that is, whether it has at most that many digits.
//
// A precision outside the declarable range is treated as fitting rather than as
// failing: the descriptor's own constructor already refuses such a column, so a
// value reaching here under one would be reported as the wrong problem.
func fitsPrecision(v int64, precision int) bool {
	if precision < 1 || precision >= len(pow10) {
		return true
	}
	limit := pow10[precision]

	return v > -limit && v < limit
}
