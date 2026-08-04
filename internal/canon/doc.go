// Package canon implements icelake's canonical record encoding, the recorded
// schema shape a payload is decoded by, and the batch identity derived from
// both.
//
// The encoding is deliberately icelake's own rather than gob or JSON: a data
// file's object key is a content hash over the canonical pre-encoding form of
// its batch, so a retried upload after a crash rewrites the same object instead
// of creating a second one. That idempotency rests on these exact bytes never
// drifting, which is why this package carries golden-fixture tests.
//
// Everything here is pure computation. Nothing in this package opens a file,
// touches a catalog, or makes a network call.
//
// # Durability
//
// Three of the four artifacts this package produces are durable and outlive the
// build that wrote them: the record payload (stored in staging.payload), the
// descriptor (stored in staging_schemas.descriptor), and the fingerprint that
// keys it (staging_schemas.fp). The fourth, a batch hash, becomes an object
// name in a bucket that is never rewritten. So both byte formats carry their own
// version byte, both are refused rather than guessed at when that byte is
// unfamiliar, and neither is ever changed in place — a format change takes the
// next version number and leaves the old one readable.
//
// # Record payload layout
//
// One version byte, then each column's value in ascending field-ID order, with
// no per-field framing. Which shape wrote a payload is recorded alongside it as
// a fingerprint, not inside it.
//
//	offset 0   1 byte   format version, currently 0x01
//	then, per column, in ascending field-ID order:
//	           1 byte   presence, for an optional column only:
//	                    0x00 null (no value bytes follow), 0x01 present
//	           value bytes, by the column's type:
//	             boolean       1 byte   0x00 false, 0x01 true
//	             int           4 bytes  big-endian two's-complement int32
//	             long          8 bytes  big-endian two's-complement int64
//	             float         4 bytes  big-endian IEEE-754 binary32
//	             double        8 bytes  big-endian IEEE-754 binary64
//	             decimal(P,S)  8 bytes  big-endian two's-complement int64,
//	                                    the unscaled value
//	             timestamptz   8 bytes  big-endian two's-complement int64,
//	                                    epoch milliseconds
//	             string        4 bytes  big-endian uint32 byte length,
//	                                    then that many bytes
//	             binary        4 bytes  big-endian uint32 byte length,
//	                                    then that many bytes
//
// A string column's bytes are written and read back exactly as given: this
// package does not validate UTF-8, and the two length-prefixed types differ
// here only in what they decode to (a string or a []byte). Parquet's String
// logical type does require validity, but that is a value-domain check, and
// this package's contract is that a value problem is refused at the boundary
// where the caller supplied it and the context to fix it exists — Insert's
// per-field poison check — not inside an encoder with no caller to return to.
//
// A required column has no presence byte at all, which makes a null in a
// required column unrepresentable rather than merely forbidden. Big-endian is
// chosen over little for one reason worth having: a golden fixture is read by a
// human, and a big-endian integer reads left to right in the order its digits
// are written.
//
// # Descriptor layout
//
//	offset 0   1 byte   format version, currently 0x01
//	offset 1   4 bytes  big-endian uint32 column count
//	then, per column, in ascending field-ID order:
//	           4 bytes  big-endian uint32 permanent field id
//	           1 byte   type tag (see Kind)
//	           1 byte   flags; bit 0 set means required, every other bit
//	                    reserved and required to be zero
//	           2 bytes  decimal precision then scale, for a decimal column only
//	           4 bytes  big-endian uint32 name byte length
//	           N bytes  the column name
//
// Exactly one byte string can describe a given shape: ordering is fixed,
// every field is fixed-width or length-prefixed, and reserved bits must be
// zero. [ParseDescriptor] enforces that structurally rather than by rule — it
// re-serializes what it read and requires the result to be identical to the
// input — which is what lets the fingerprint simply be a hash of these bytes.
//
// # Fingerprint and batch hash
//
// Both are SHA-256, from the standard library, rendered as lowercase hex. Both
// hash inputs start with a distinct domain tag, so a schema fingerprint and a
// batch hash can never be the same 32 bytes.
//
//	schema_fp  = hex(sha256( "icelake/schema-fp/v1\n" || descriptor ))
//
//	batch key  = hex(sha256( "icelake/batch/v1\n"
//	                         || uint32be(len(namespace))  || namespace
//	                         || uint32be(len(table))      || table
//	                         || uint32be(len(descriptor)) || descriptor
//	                         || uint64be(record count)
//	                         || for each record, in batch order:
//	                              uint32be(len(payload))  || payload ))
package canon
