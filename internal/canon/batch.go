package canon

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/gvkhna/icelake/internal/errdef"
)

// batchDomain prefixes the batch hash's input so it can never coincide with a
// schema fingerprint, whatever the rest of the input happens to be.
const batchDomain = "icelake/batch/v1\n"

// BatchHash computes a batch's content hash: the lowercase hex SHA-256 that
// becomes its key, and therefore the name of the one object in the bucket that
// batch is ever allowed to produce.
//
// The key is a pure function of what is in the batch, never a sequence number
// and never a hash of the encoded Parquet bytes. A sequence number would need a
// durably persisted counter recovered exactly across a crash, and two accidental
// concurrent writers would produce the same name for different data. Hashing the
// final Parquet output would make idempotency hostage to a third-party encoder's
// determinism — a compression level change, a dictionary heuristic, or an
// embedded writer-version string would silently produce a different key for
// identical data, at exactly the moment the key needs to be the same.
//
// The hashed input is the domain tag above followed by, in order and each
// length-prefixed with four big-endian bytes: the namespace, the table name, the
// serialized descriptor of the shape every record in the batch is encoded
// under, an eight-byte big-endian record count, and each record's canonical
// payload in batch order.
//
// Order is part of the identity: the same records in a different order are a
// different batch and get a different key, because replay must reproduce the
// batch it is retrying byte for byte, not merely a set-equal one.
//
// A batch with no records is refused. A key must name real bytes, and hashing
// nothing would hand back a perfectly valid-looking name for an empty file.
func BatchHash(namespace, table string, d *Descriptor, payloads [][]byte) (string, error) {
	if len(payloads) == 0 {
		return "", errdef.NewEncodingError(errdef.EncodingKindEmptyBatch, table, "", 0,
			"a batch with no records has no content to hash")
	}

	h := sha256.New()
	h.Write([]byte(batchDomain))

	var prefix [8]byte
	writeChunk := func(b []byte) {
		binary.BigEndian.PutUint32(prefix[:4], uint32(len(b)))
		h.Write(prefix[:4])
		h.Write(b)
	}

	for _, s := range []string{namespace, table} {
		if uint64(len(s)) > uint64(MaxValueBytes) {
			return "", errdef.NewEncodingError(errdef.EncodingKindValue, table, "", 0,
				"namespace or table name is longer than the hash input can address")
		}
		writeChunk([]byte(s))
	}

	// Checked for the same reason its neighbours are: every length in this
	// preimage is written as a uint32, and a length that truncated would let
	// two different inputs hash identically — the one thing the prefixes are
	// here to rule out.
	if uint64(len(d.encoded)) > uint64(MaxValueBytes) {
		return "", errdef.NewEncodingError(errdef.EncodingKindValue, table, "", 0,
			"serialized descriptor is longer than the hash input can address")
	}
	writeChunk(d.encoded)

	binary.BigEndian.PutUint64(prefix[:], uint64(len(payloads)))
	h.Write(prefix[:])

	for i, p := range payloads {
		if uint64(len(p)) > uint64(MaxValueBytes) {
			return "", errdef.NewEncodingError(errdef.EncodingKindValue, table, "", 0,
				fmt.Sprintf("record %d's payload is longer than the hash input can address", i))
		}
		writeChunk(p)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
