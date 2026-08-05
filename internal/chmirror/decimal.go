package chmirror

import (
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/shopspring/decimal"
)

// decimalValue is the Go type the driver's Decimal(P, S) column takes.
//
// It is a third-party type, and it is the only one in this library's data path
// that is. The alternative is a float64, which is the one thing a decimal
// column exists to never be — SCHEMA.md's money decision is that a decimal
// never travels through a binary float, and the mirror is the last place a
// value could quietly become one. So the dependency is the price of not
// undoing that decision at the very end of the write path.
type decimalValue = decimal.Decimal

// unscaledToDecimal turns one Arrow decimal into the driver's decimal, exactly,
// with no intermediate float and no big.Int allocation.
//
// The low 64 bits are the whole value in two's complement, and reading them as
// a signed int64 is exact: a declared decimal's precision is at most 18 —
// enforced by the declaration path, because the caller's unscaled value is an
// int64 — so every value here is inside ±10^18, comfortably inside int64's
// range, and the high word is nothing but the sign extension of the low one.
// The negative case is the one worth naming rather than trusting: -5 arrives as
// a high word of -1 and a low word of 0xFFFF...FB, and int64 of that low word
// is -5.
func unscaledToDecimal(n decimal128.Num, scale int) decimalValue {
	//nolint:gosec // G115: the conversion is the two's-complement read the doc comment argues is exact for P<=18.
	return decimal.New(int64(n.LowBits()), int32(-scale))
}
