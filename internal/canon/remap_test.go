package canon

import (
	"errors"
	"slices"
	"testing"

	"github.com/gvkhna/icelake/internal/errdef"
)

// The tests below are the replay story from ARCHITECTURE.md's schema-fingerprint
// decision, exercised as pure computation: rows staged under one shape, read
// back by a build that declares a different one. Descriptors are built directly
// from field lists here rather than through a declaration struct, because these
// cases are about pairs of shapes and expressing "the same table, one field
// different" as two Go structs would obscure which single thing changed.
//
// They are TESTING.md's second sanctioned category. What that section's third
// rule asks for is the claim shape each test holds, and this file does not hold
// one shape throughout, so every test below is placed by name rather than
// covered by a sentence about the file. What they have in common is only the
// grain: one shape pair at a time, one changed field at a time, with no
// container to start and no table to evolve.
//
// Corrected at the completeness audit's fifth round (2026-08-01), because the
// paragraph that used to stand here was wrong twice over. It said these tests
// "hold its first claim shape throughout: the caller-facing half of every one
// of them is already proven end to end through the public API", and named
// scenario 2 and reconcile_test.go as where that half lives. For the refusals
// that was false. The only end-to-end assertions on this error's Kind were
// scenario 2's TestStagedRowsUnderAnUnknownFieldRefuseTheOpen and
// reconcile_test.go's TestOpenWriterRefusesStagedRowsItCannotRepresent, and
// both assert StagingSchemaKindFieldRemoved, so two of the three kinds a caller
// can be handed had no caller-facing half at all — a hole the sentence's own
// confidence was hiding. And "throughout" was the blanket quantifier this audit
// keeps being broken by: one test here holds the other claim shape outright.
// The missing halves were written rather than the sentence softened, per the
// precedent TESTING.md records for the migration test that could not name its
// claim shape: TestOpenWriterRefusesStagedRowsUnderAChangedColumn in
// reconcile_test.go now drives both remaining kinds to a caller through
// OpenWriter, against the real substrate, with real rows in a real staging file.
//
// First claim shape, with the caller-facing half proven end to end at this same
// grain — the same shape pair, not merely the same family:
//
//   - TestTranscodeCarriesValuesAcrossAnAddedOptionalColumn. Scenario 2's
//     variant 3 stages rows, restarts under a declaration carrying one more
//     optional field, and reads them back committed with that column null.
//   - The "a column the current declaration no longer has" case of
//     TestTranscodeRefusesShapesItCannotMapHonestly. Both end-to-end tests
//     named above catch StagingSchemaKindFieldRemoved out of OpenWriter with
//     the field id on it.
//   - The "a required column the staged shape never had" and "a column whose
//     type changed" cases, as of this round: the two subtests of
//     TestOpenWriterRefusesStagedRowsUnderAChangedColumn catch exactly those
//     two kinds out of OpenWriter, with the field id on them.
//   - TestRemapReportsARemovedColumnEvenWhenAnotherWasAdded, for the refusal it
//     reports, which is the removed column above. Its extra claim — that the
//     removed column wins when the added-required case applies at the same time
//     — is not itself driven end to end, and reaching it would take a
//     declaration that drops a staged column while requiring a different column
//     the live table already carries. Recorded rather than claimed.
//
// First claim shape at a coarser grain, which is a real difference and is
// spelled out here rather than blurred into the list above. The claim these
// four share with the public API is the one scenario 2's variant 3 proves end
// to end — a staged row crosses a declaration change without losing or
// inventing a value, and is refused rather than guessed at when it cannot — but
// no end-to-end test puts staged rows across these four particular changes, and
// where a change could not be put there at all the reason is checked rather
// than assumed:
//
//   - TestTranscodeIgnoresARename. Reachable: reconciliation applies a rename,
//     and staged rows recorded under the old name would then be transcoded at
//     seal. Simply has no end-to-end test of its own — the rename that is
//     proven end to end, in TestReconcileRenamesAndWidensColumns, crosses
//     committed rows rather than staged ones.
//   - TestTranscodeCarriesAValueIntoARelaxedColumn. Reachable in the same way,
//     and more quietly: reconciliation does not diff nullability at all, so a
//     declaration that relaxes a column is accepted with no table change, and
//     the staged rows cross the relaxation at seal. No end-to-end test of its
//     own either.
//   - The "a null in a column that is now required" case. Not reachable through
//     OpenWriter by construction: the up-front check is over shapes, and a null
//     is a property of one value rather than of a shape. It is reachable, on
//     the flush path, where it reaches a caller through Flush or Close and
//     through OnFlushError rather than through the open — and it is untested
//     there.
//   - The "a decimal column whose scale changed" case. Not reachable at all,
//     from either direction: reconciliation refuses a moved scale and a
//     narrowed precision before the staged rows are consulted, and the one
//     remaining difference a declaration could express — a wider precision at
//     the same scale — is refused at declaration, because 18 is the ceiling an
//     int64 can carry.
//
// Second claim shape — a defence this layer holds against its own in-process
// callers, which the public path is designed never to reach:
//
//   - TestTranscodeIntoTheSameShapeIsTheIdentity. The flush path compares
//     fingerprints and transcodes only the rows that differ, so no public call
//     ever reaches Transcode with one descriptor on both sides. What it defends
//     is the batch key: a sealed batch may mix transcoded rows with freshly
//     encoded ones, and its stored key names its own bytes only if those two
//     productions agree byte for byte. A general path that quietly differed
//     from the shortcut would leave a re-uploaded batch under a key that no
//     longer hashes to it, which is the one failure content-hash keying cannot
//     survive.

// shape builds a descriptor from a field list, failing the test if it is not a
// shape this package can describe.
func shape(t *testing.T, fields ...Field) *Descriptor {
	t.Helper()

	d, err := newDescriptor("fills", fields)
	if err != nil {
		t.Fatalf("newDescriptor: %v", err)
	}
	return d
}

// oldShape is the shape a batch of rows was staged under: two required columns
// and one optional one.
func oldShape(t *testing.T) *Descriptor {
	t.Helper()
	return shape(t,
		Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
		Field{ID: 2, Name: "price", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
		Field{ID: 3, Name: "note", Kind: KindString},
	)
}

// TestTranscodeCarriesValuesAcrossAnAddedOptionalColumn is the ordinary
// schema-evolution replay: the process crashed, a new build deployed with one
// more optional column, and the staged rows must come back under the new shape
// with the new column null. Null is exactly right — a field added after a table
// exists is always optional — and getting it wrong would either drop the row or
// invent a value for it.
func TestTranscodeCarriesValuesAcrossAnAddedOptionalColumn(t *testing.T) {
	from := oldShape(t)
	to := shape(t,
		Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
		Field{ID: 2, Name: "price", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
		Field{ID: 3, Name: "note", Kind: KindString},
		Field{ID: 4, Name: "venue_order_type", Kind: KindString},
	)

	staged, err := Encode("fills", from, Row{"btc_usd", int64(1234567890), "staged earlier"})
	if err != nil {
		t.Fatalf("Encode under the old shape: %v", err)
	}

	got, err := Transcode("fills", from, to, staged)
	if err != nil {
		t.Fatalf("Transcode: %v", err)
	}

	// Stated independently: the same row, written directly under the new shape
	// with the added column null, must produce byte-identical output. That is
	// the property seal-time re-encoding depends on — a transcoded row and a
	// freshly encoded one are the same bytes, so a batch mixing the two hashes
	// the same as a batch of either.
	want, err := Encode("fills", to, Row{"btc_usd", int64(1234567890), "staged earlier", nil})
	if err != nil {
		t.Fatalf("Encode under the new shape: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("transcoded payload:\n got %x\nwant %x", got, want)
	}

	row, err := Decode("fills", to, got)
	if err != nil {
		t.Fatalf("Decode under the new shape: %v", err)
	}
	assertRowsEqual(t, row, Row{"btc_usd", int64(1234567890), "staged earlier", nil})
}

// TestTranscodeIgnoresARename proves identity is the permanent field id and
// nothing else. A rename changes the fingerprint, so the row does get
// transcoded — but its values must cross unchanged, because renaming a column
// says nothing about what its values are.
func TestTranscodeIgnoresARename(t *testing.T) {
	from := oldShape(t)
	to := shape(t,
		Field{ID: 1, Name: "instrument", Kind: KindString, Required: true},
		Field{ID: 2, Name: "price", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
		Field{ID: 3, Name: "note", Kind: KindString},
	)

	if from.Fingerprint() == to.Fingerprint() {
		t.Fatal("a rename must change the fingerprint, or this case is not the one it claims to be")
	}

	staged, err := Encode("fills", from, Row{"btc_usd", int64(7), nil})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := Transcode("fills", from, to, staged)
	if err != nil {
		t.Fatalf("Transcode: %v", err)
	}
	// Only the names differ, so the value bytes are identical.
	if !slices.Equal(got, staged) {
		t.Errorf("transcoding across a rename changed the payload:\n got %x\nwant %x", got, staged)
	}

	row, err := Decode("fills", to, got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	assertRowsEqual(t, row, Row{"btc_usd", int64(7), nil})
}

// TestTranscodeCarriesAValueIntoARelaxedColumn covers the safe half of a
// nullability change: a column that used to be required is now optional, so the
// staged value still fits and simply gains a presence byte.
func TestTranscodeCarriesAValueIntoARelaxedColumn(t *testing.T) {
	from := oldShape(t)
	to := shape(t,
		Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
		Field{ID: 2, Name: "price", Kind: KindDecimal, Precision: 18, Scale: 9},
		Field{ID: 3, Name: "note", Kind: KindString},
	)

	staged, err := Encode("fills", from, Row{"btc_usd", int64(5), nil})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := Transcode("fills", from, to, staged)
	if err != nil {
		t.Fatalf("Transcode: %v", err)
	}

	row, err := Decode("fills", to, got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	assertRowsEqual(t, row, Row{"btc_usd", int64(5), nil})
}

// TestTranscodeRefusesShapesItCannotMapHonestly is the fail-loudly half, and
// the one ARCHITECTURE.md names by type. Each case is a real deploy someone
// could ship, and in each one the only honest answers are "refuse" or "invent
// data"; every refusal names the field id and which case it was, so an operator
// knows what to do and a test can assert it without reading prose.
func TestTranscodeRefusesShapesItCannotMapHonestly(t *testing.T) {
	from := oldShape(t)

	cases := []struct {
		name        string
		to          *Descriptor
		row         Row
		wantKind    errdef.StagingSchemaKind
		wantFieldID int
	}{
		{
			// The documented case: the current declaration dropped a column
			// that staged rows still carry values for.
			name: "a column the current declaration no longer has",
			to: shape(t,
				Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
				Field{ID: 3, Name: "note", Kind: KindString},
			),
			row:         Row{"btc_usd", int64(1), nil},
			wantKind:    errdef.StagingSchemaKindFieldRemoved,
			wantFieldID: 2,
		},
		{
			// A required column added while rows were already staged. This is
			// forbidden against a table that already exists, but a table whose
			// first flush never succeeded has no live schema to forbid it.
			name: "a required column the staged shape never had",
			to: shape(t,
				Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
				Field{ID: 2, Name: "price", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
				Field{ID: 3, Name: "note", Kind: KindString},
				Field{ID: 4, Name: "source", Kind: KindString, Required: true},
			),
			row:         Row{"btc_usd", int64(1), nil},
			wantKind:    errdef.StagingSchemaKindFieldRequired,
			wantFieldID: 4,
		},
		{
			// The other half of a nullability change: a column that was
			// optional is now required, and this row recorded a null in it.
			name: "a null in a column that is now required",
			to: shape(t,
				Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
				Field{ID: 2, Name: "price", Kind: KindDecimal, Precision: 18, Scale: 9, Required: true},
				Field{ID: 3, Name: "note", Kind: KindString, Required: true},
			),
			row:         Row{"btc_usd", int64(1), nil},
			wantKind:    errdef.StagingSchemaKindFieldRequired,
			wantFieldID: 3,
		},
		{
			name: "a column whose type changed",
			to: shape(t,
				Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
				Field{ID: 2, Name: "price", Kind: KindLong, Required: true},
				Field{ID: 3, Name: "note", Kind: KindString},
			),
			row:         Row{"btc_usd", int64(1), nil},
			wantKind:    errdef.StagingSchemaKindFieldRetyped,
			wantFieldID: 2,
		},
		{
			// A decimal whose scale moved is a retype too: the same int64
			// unscaled value would silently mean a different number.
			name: "a decimal column whose scale changed",
			to: shape(t,
				Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
				Field{ID: 2, Name: "price", Kind: KindDecimal, Precision: 18, Scale: 2, Required: true},
				Field{ID: 3, Name: "note", Kind: KindString},
			),
			row:         Row{"btc_usd", int64(1), nil},
			wantKind:    errdef.StagingSchemaKindFieldRetyped,
			wantFieldID: 2,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			staged, err := Encode("fills", from, c.row)
			if err != nil {
				t.Fatalf("Encode under the staged shape: %v", err)
			}

			out, err := Transcode("fills", from, c.to, staged)
			if err == nil {
				t.Fatalf("Transcode succeeded, producing %x; want a refusal", out)
			}

			var se errdef.StagingSchemaError
			if !errors.As(err, &se) {
				t.Fatalf("error is %T, want errdef.StagingSchemaError", err)
			}
			if se.Kind != c.wantKind {
				t.Errorf("StagingSchemaError.Kind = %q, want %q", se.Kind, c.wantKind)
			}
			if se.FieldID != c.wantFieldID {
				t.Errorf("StagingSchemaError.FieldID = %d, want %d", se.FieldID, c.wantFieldID)
			}
			if se.Table != "fills" {
				t.Errorf("StagingSchemaError.Table = %q, want %q", se.Table, "fills")
			}
		})
	}
}

// TestRemapReportsARemovedColumnEvenWhenAnotherWasAdded pins the priority
// between two refusals that can apply at once. A deploy that both drops a
// staged column and adds a required one must be reported as the dropped
// column: that is the case with staged data at risk and a specific operator
// action attached, and reporting the other one would send the operator after
// the wrong change.
func TestRemapReportsARemovedColumnEvenWhenAnotherWasAdded(t *testing.T) {
	from := oldShape(t)
	to := shape(t,
		Field{ID: 1, Name: "symbol", Kind: KindString, Required: true},
		Field{ID: 3, Name: "note", Kind: KindString},
		Field{ID: 4, Name: "source", Kind: KindString, Required: true},
	)

	_, err := Remap("fills", from, to, Row{"btc_usd", int64(1), nil})
	if err == nil {
		t.Fatal("Remap succeeded, want a refusal")
	}

	var se errdef.StagingSchemaError
	if !errors.As(err, &se) {
		t.Fatalf("error is %T, want errdef.StagingSchemaError", err)
	}
	if se.Kind != errdef.StagingSchemaKindFieldRemoved || se.FieldID != 2 {
		t.Errorf("refusal was %q on field %d, want %q on field 2", se.Kind, se.FieldID, errdef.StagingSchemaKindFieldRemoved)
	}
}

// TestTranscodeIntoTheSameShapeIsTheIdentity states the boundary case
// explicitly: a row already under the target shape comes back byte-identical.
// The write path skips transcoding for these rows entirely — they are the
// overwhelmingly normal case — so this pins that the shortcut and the general
// path cannot disagree.
func TestTranscodeIntoTheSameShapeIsTheIdentity(t *testing.T) {
	d := goldenDescriptor(t)

	staged, err := Encode("fills", d, goldenRow1)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := Transcode("fills", d, d, staged)
	if err != nil {
		t.Fatalf("Transcode: %v", err)
	}
	if !slices.Equal(got, staged) {
		t.Errorf("transcoding into the same shape changed the payload:\n got %x\nwant %x", got, staged)
	}
}
