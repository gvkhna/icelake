package canon

import (
	"errors"
	"testing"

	"github.com/gvkhna/icelake/internal/errdef"
)

// This file holds both of TESTING.md's sanctioned tiers, and which test is
// which is stated here rather than left to be inferred.
//
// TestBatchHashIsGolden, TestBatchHashOfASingleRecordIsGolden and
// TestBatchHashIsOrderSensitive are the first category: the batch content hash
// is named on that document's closed list of calculations whose exact output
// matters. Every expected hash below was computed outside this program, by
// assembling the hash input from the specification in this package's doc
// comment and feeding those exact bytes to sha256sum — including the reversed
// key, which is why the order test asserts an independently derived value
// rather than merely "something different". A hash captured from the code under
// test would assert nothing, and this particular number is the most durable one
// in the project: it is the name of an object in a bucket, and a crash between
// upload and commit is recovered by recomputing it and landing on the same key.
//
// The other three are the second category, and they do not all hold the same
// claim shape.
//
// TestBatchHashDependsOnEveryComponentOfItsInput holds the first shape. The
// caller-facing half is already proven end to end: two tables writing from one
// Store commit and read back as their own rows and no one else's, and scenario
// 2 crashes between the upload and the commit, then requires the replayed batch
// to recompute this hash to the same value it was sealed under and to leave one
// data file rather than two. What the substrate cannot arrange is
// the enumeration — namespace, table, descriptor, a changed record, a record
// removed, one component at a time, with no container to start — because end to
// end a collision only ever surfaces as a data file that went missing, and only
// if two batches happened to be shaped so that they would collide at all. The
// invariant is that two batches never share an object name; lose it and one
// batch's upload silently overwrites another's data file while both tables'
// manifests go on pointing at it.
//
// TestBatchHashSplitsAreNotAmbiguous holds the second shape. The batch
// boundaries are chosen by internal/flush, not by a caller: the public API
// offers no way to ask for a particular split, so an end-to-end test could only
// ever show that today's split rule does not happen to produce a colliding
// pair, while this layer would still be wrong on the day that rule changes.
// What it defends is the same invariant one step lower down — a batch's stored
// key names its own bytes, boundaries included — and losing it means a batch
// boundary can move without the key noticing, which is exactly what replay
// depends on noticing.
//
// TestBatchHashRefusesAnEmptyBatch holds the second shape too. No caller
// reaches it: Writer.sealLocked returns without enqueueing anything when the
// pending batch is empty, and the staging layer refuses a seal with no rows as
// well. It is here because this package must not hand back a perfectly ordinary
// looking object name for a batch with nothing in it on the day some other
// caller asks it to — an empty data file uploaded and committed as though it
// were data is a table that reports rows it does not have. It asserts the typed
// refusal a caller would meet, with errors.As on errdef.EncodingError and its
// Kind and Table read off it, not the message.

// goldenBatchKey is the content hash of a two-record batch of goldenPayload1
// then goldenPayload2, for namespace "market" and table "fills" under the
// golden descriptor.
//
// It was computed outside this program, by assembling the hash input from the
// specification in this package's doc comment — the 17-byte domain tag, then
// each of namespace, table and descriptor behind a four-byte big-endian length,
// then an eight-byte big-endian record count of 2, then each payload behind its
// own four-byte length — and feeding those exact bytes to sha256sum. The
// component lengths are 6, 5, 169, 70 and 58, each of which is stated
// independently by the fixtures those bytes come from.
const goldenBatchKey = "f4ea0a800871c81823ef4d1ca03011d96eb446ed93b70bb05342915b565634b5"

// goldenBatchKeyReversed is the same batch with its two records swapped,
// computed the same way. It exists so the order-sensitivity test asserts
// against an independently derived value rather than merely against "something
// different".
const goldenBatchKeyReversed = "5489c48f4563d622c9e7b9144706bb6486e0de20a321af73bfca0e0e7fbb80f4"

// goldenBatchKeySingle is a one-record batch containing only goldenPayload1,
// computed the same way with a record count of 1.
const goldenBatchKeySingle = "9459474cb86fdf722af12d744f17550a891f998ad9759431e3bd5509bd35e6ad"

// TestBatchHashIsGolden pins the batch key for a known batch. This is the
// single most durable number in the project: it is the name of an object in a
// bucket, and a crash between upload and commit is recovered by recomputing it
// and landing on the same key. A drift here does not fail loudly — it silently
// orphans an uploaded file and writes a second copy of the same data under a
// new name.
func TestBatchHashIsGolden(t *testing.T) {
	d := goldenDescriptor(t)

	got, err := BatchHash("market", "fills", d, [][]byte{goldenPayload1, goldenPayload2})
	if err != nil {
		t.Fatalf("BatchHash: %v", err)
	}
	if got != goldenBatchKey {
		t.Errorf("BatchHash = %s, want %s", got, goldenBatchKey)
	}
}

// TestBatchHashOfASingleRecordIsGolden covers the one-record batch separately.
// It is the shape a forced flush below the threshold produces — Close on a
// handful of rows — so it is a real case, not a degenerate one, and the record
// count in the hash input makes it genuinely distinct rather than incidentally
// so.
func TestBatchHashOfASingleRecordIsGolden(t *testing.T) {
	d := goldenDescriptor(t)

	got, err := BatchHash("market", "fills", d, [][]byte{goldenPayload1})
	if err != nil {
		t.Fatalf("BatchHash: %v", err)
	}
	if got != goldenBatchKeySingle {
		t.Errorf("BatchHash = %s, want %s", got, goldenBatchKeySingle)
	}
	if got == goldenBatchKey {
		t.Error("a one-record batch hashed identically to the two-record batch that starts with the same record")
	}
}

// TestBatchHashIsOrderSensitive proves the batch's order is part of its
// identity. Replay reassembles a sealed batch in staged-row id order precisely
// so it reproduces the same key; if order did not matter, a re-batching that
// merely happened to contain the same records would be indistinguishable from
// the real one, and the guarantee would be weaker than it looks.
func TestBatchHashIsOrderSensitive(t *testing.T) {
	d := goldenDescriptor(t)

	got, err := BatchHash("market", "fills", d, [][]byte{goldenPayload2, goldenPayload1})
	if err != nil {
		t.Fatalf("BatchHash: %v", err)
	}
	if got != goldenBatchKeyReversed {
		t.Errorf("BatchHash = %s, want %s", got, goldenBatchKeyReversed)
	}
	if got == goldenBatchKey {
		t.Error("reversing the records left the batch key unchanged")
	}
}

// TestBatchHashDependsOnEveryComponentOfItsInput walks each part of the hash
// input in turn and proves changing it changes the key. Two of these matter for
// safety rather than tidiness: the namespace and table keep two tables' batches
// from ever colliding on one object name, and the descriptor keeps two schema
// versions of one table from doing the same.
func TestBatchHashDependsOnEveryComponentOfItsInput(t *testing.T) {
	d := goldenDescriptor(t)
	payloads := [][]byte{goldenPayload1, goldenPayload2}

	base, err := BatchHash("market", "fills", d, payloads)
	if err != nil {
		t.Fatalf("BatchHash: %v", err)
	}

	other := shape(t,
		Field{ID: 1, Name: "flag", Kind: KindBoolean, Required: true},
	)
	otherPayload, err := Encode("fills", other, Row{true})
	if err != nil {
		t.Fatalf("Encode under the other shape: %v", err)
	}

	cases := []struct {
		name      string
		namespace string
		table     string
		d         *Descriptor
		payloads  [][]byte
	}{
		{"a different namespace", "markets", "fills", d, payloads},
		{"a different table", "market", "fill", d, payloads},
		{"a different schema shape", "market", "fills", other, [][]byte{otherPayload}},
		{"a different record", "market", "fills", d, [][]byte{goldenPayload1, goldenPayload1}},
		{"one record fewer", "market", "fills", d, payloads[:1]},
	}

	seen := map[string]string{base: "the golden batch"}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := BatchHash(c.namespace, c.table, c.d, c.payloads)
			if err != nil {
				t.Fatalf("BatchHash: %v", err)
			}
			if prior, dup := seen[got]; dup {
				t.Fatalf("hashed identically to %s (%s)", prior, got)
			}
			seen[got] = c.name
		})
	}
}

// TestBatchHashSplitsAreNotAmbiguous proves the length prefixes do their job.
// Two records whose bytes concatenate to the same string as one longer record
// must not produce the same key — otherwise a batch boundary could move without
// the key noticing, which is exactly the property replay depends on.
func TestBatchHashSplitsAreNotAmbiguous(t *testing.T) {
	d := shape(t, Field{ID: 1, Name: "payload", Kind: KindBinary, Required: true})

	// Encoded as: version 0x01, four-byte length, then the bytes.
	ab, err := Encode("fills", d, Row{[]byte{0xaa, 0xbb}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	a, err := Encode("fills", d, Row{[]byte{0xaa}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b, err := Encode("fills", d, Row{[]byte{0xbb}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	one, err := BatchHash("market", "fills", d, [][]byte{ab})
	if err != nil {
		t.Fatalf("BatchHash: %v", err)
	}
	two, err := BatchHash("market", "fills", d, [][]byte{a, b})
	if err != nil {
		t.Fatalf("BatchHash: %v", err)
	}
	if one == two {
		t.Errorf("one record of two bytes and two records of one byte share the key %s", one)
	}
}

// TestBatchHashRefusesAnEmptyBatch keeps the batch key meaning what it says. A
// key names the bytes of a data file; handing back a perfectly valid-looking
// name for a batch with nothing in it would let an empty object be uploaded and
// committed as though it were data.
func TestBatchHashRefusesAnEmptyBatch(t *testing.T) {
	d := goldenDescriptor(t)

	for _, payloads := range [][][]byte{nil, {}} {
		key, err := BatchHash("market", "fills", d, payloads)
		if err == nil {
			t.Fatalf("BatchHash of an empty batch returned %s, want a refusal", key)
		}

		var ee errdef.EncodingError
		if !errors.As(err, &ee) {
			t.Fatalf("error is %T, want errdef.EncodingError", err)
		}
		if ee.Kind != errdef.EncodingKindEmptyBatch {
			t.Errorf("EncodingError.Kind = %q, want %q", ee.Kind, errdef.EncodingKindEmptyBatch)
		}
		if ee.Table != "fills" {
			t.Errorf("EncodingError.Table = %q, want %q", ee.Table, "fills")
		}
	}
}
