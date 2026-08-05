package icelake_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/gvkhna/icelake"
)

// Scenario 15: the ingest entry point, and the second input format.
//
// Everything here runs in local-only mode and needs no container, for the reason
// scenario 12's first half needs none: what these claims are about is the path
// from a caller's bytes to a durable row in `staging.db`, and that path is the
// same objects doing the same work whether or not a bucket exists on the other
// side of it. The two clauses that genuinely are about a bucket — a real pipe
// into the built binary, and a signal — stay in scenario 13 where they already
// are.

// ingestDocument declares the two tables these tests stream into. Two rather
// than one is load-bearing in exactly one clause — the residue a chunk carrying
// two tables leaves behind a refused row — and harmless everywhere else.
const ingestDocument = `{"tables": [
  {"namespace": "market", "table": "fills", "fields": [
    {"name": "symbol",  "type": "string",        "fieldid": 1},
    {"name": "price",   "type": "decimal(18,9)", "fieldid": 2},
    {"name": "ts_ms",   "type": "timestamptz",   "fieldid": 3},
    {"name": "note",    "type": "string",        "fieldid": 4, "optional": true}
  ]},
  {"namespace": "archive", "table": "events", "fields": [
    {"name": "kind",    "type": "string",        "fieldid": 1},
    {"name": "payload", "type": "binary",        "fieldid": 2},
    {"name": "at",      "type": "timestamptz",   "fieldid": 3}
  ]}
]}`

// openIngestWriters opens one writer per table the document declares, keyed the
// way [icelake.IngestStream] reads its map: by the envelope's own table field.
func openIngestWriters(tb testing.TB, store *icelake.Store) map[string]*icelake.DynamicWriter {
	tb.Helper()

	tables, err := icelake.ParseSchemaDocument([]byte(ingestDocument))
	if err != nil {
		tb.Fatalf("ParseSchemaDocument: %v", err)
	}

	writers := make(map[string]*icelake.DynamicWriter, len(tables))
	for _, tc := range tables {
		w, err := icelake.OpenDynamicWriter(context.Background(), store, tc)
		if err != nil {
			tb.Fatalf("OpenDynamicWriter(%s.%s): %v", tc.Namespace, tc.Table, err)
		}
		writers[tc.Namespace+"."+tc.Table] = w
	}

	return writers
}

// ingestConfig is a local-only store whose flush thresholds are far out of
// reach, so that a test reading `staging.db` sees exactly what the ingest call
// accepted rather than whatever a flush had already pruned. The two clauses that
// want a flush lower the thresholds themselves.
func ingestConfig(dir string) icelake.Config {
	return icelake.Config{
		StagingPath:       filepath.Join(dir, "staging.db"),
		CacheDir:          filepath.Join(dir, "cache"),
		LocalOnly:         true,
		FlushMaxRecords:   1 << 20,
		FlushMaxBytes:     1 << 30,
		FlushInterval:     time.Hour,
		MinFlushInterval:  time.Nanosecond,
		ZSTDLevel:         1,
		StagingMaxRecords: 1 << 20,
		StagingMaxBytes:   1 << 30,
	}
}

// cachedRecords counts what DuckDB finds in one table's local Parquet cache,
// which is where a record ends up once the writer has drained.
func cachedRecords(tb testing.TB, tableDir string) int {
	tb.Helper()

	glob := filepath.Join(tableDir, "data", "*.parquet")
	matches, err := filepath.Glob(glob)
	if err != nil {
		tb.Fatalf("globbing %s: %v", glob, err)
	}
	if len(matches) == 0 {
		return 0
	}

	var n int
	openLocalDuckDB(tb).queryRow(tb, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", glob), &n)

	return n
}

// fillRow is one record of market.fills, as a map, so that the same record can
// be rendered into both formats and the equivalence clause below can mean it.
func fillRow(i int) map[string]any {
	return map[string]any{
		"symbol": fmt.Sprintf("SYM%03d", i%97),
		"price":  fmt.Sprintf("%d.000000001", i),
		"ts_ms":  int64(1_700_000_000_000 + i),
		"note":   fmt.Sprintf("note %d", i),
	}
}

// eventRow is one record of archive.events, whose binary column is what makes
// the two formats' handling of bytes observable.
func eventRow(i int) map[string]any {
	return map[string]any{
		"kind":    fmt.Sprintf("kind-%d", i%7),
		"payload": []byte{byte(i), byte(i >> 8), 0x00, 0xff},
		"at":      int64(1_700_000_000_000 + i),
	}
}

// jsonEnvelopeFor renders one record as a line of JSON.
//
// The binary column is base64 here and raw bytes in the CBOR twin below, which
// is the one place the two renderings deliberately differ: it is the difference
// the equivalence clause exists to prove does not reach the staged bytes.
func jsonEnvelopeFor(tb testing.TB, table string, row map[string]any) []byte {
	tb.Helper()

	encoded := make(map[string]any, len(row))
	for k, v := range row {
		if b, ok := v.([]byte); ok {
			encoded[k] = base64.StdEncoding.EncodeToString(b)

			continue
		}
		encoded[k] = v
	}

	line, err := json.Marshal(map[string]any{"table": table, "row": encoded})
	if err != nil {
		tb.Fatalf("rendering a JSON envelope: %v", err)
	}

	return line
}

// cborEnvelopeFor renders the same record as one CBOR data item.
func cborEnvelopeFor(tb testing.TB, table string, row map[string]any) []byte {
	tb.Helper()

	item, err := cbor.Marshal(map[string]any{"table": table, "row": row})
	if err != nil {
		tb.Fatalf("rendering a CBOR envelope: %v", err)
	}

	return item
}

// encoding names one of the two spellings a record may arrive in. There is no
// such type in the library — the grammar decides per record from the first byte
// and nothing configures it — so it exists here only to run a clause twice.
type encoding string

const (
	asJSON encoding = "json"
	asCBOR encoding = "cbor"
)

// bothEncodings is the pair every clause below that is not about one spelling in
// particular runs twice against.
var bothEncodings = []encoding{asJSON, asCBOR}

// envelopeFor renders a record in whichever spelling is being run, already
// framed: a JSON record carries its own newline, and a CBOR record carries
// nothing, because a CBOR item says its own length.
func envelopeFor(tb testing.TB, enc encoding, table string, row map[string]any) []byte {
	tb.Helper()

	if enc == asCBOR {
		return cborEnvelopeFor(tb, table, row)
	}

	return append(jsonEnvelopeFor(tb, table, row), '\n')
}

// streamOf concatenates already-framed records. There is nothing to put between
// them: a JSON record ends at its newline and a CBOR one ends where its own
// length says it does, which is exactly why the two can be interleaved.
func streamOf(items [][]byte) []byte {
	var buf bytes.Buffer
	for _, item := range items {
		buf.Write(item)
	}

	return buf.Bytes()
}

// TestIngestStreamIsTheWholeIngestLoop holds every claim the ingest entry point
// makes, in both formats, against an in-memory reader.
//
// It is the library-level half of what scenario 13's streaming tests hold
// through a process. Those stay, and are the proof that the command is wired to
// this call at all; these are where the loop's own rules are asserted, because
// a rule that can only be reached by starting a binary and writing to a pipe is
// a rule most of whose cases nobody will ever write a test for.
func TestIngestStreamIsTheWholeIngestLoop(t *testing.T) {
	t.Run("a stream is read in chunks and every record is durable", func(t *testing.T) {
		for _, enc := range bothEncodings {
			t.Run(string(enc), func(t *testing.T) {
				dir := t.TempDir()
				cfg := ingestConfig(dir)
				store := openStore(t, cfg)
				writers := openIngestWriters(t, store)

				const records = 500
				items := make([][]byte, 0, records)
				for i := range records {
					items = append(items, envelopeFor(t, enc, "market.fills", fillRow(i)))
				}

				err := icelake.IngestStream(context.Background(), writers,
					bytes.NewReader(streamOf(items)), icelake.IngestOptions{})
				if err != nil {
					t.Fatalf("IngestStream: %v", err)
				}

				// Every record is durable in staging when the call returns,
				// which is the promise, and it is read off the file rather than
				// asked of the library that wrote it.
				if n := len(stagedRowsFor(t, cfg.StagingPath, "market", "fills")); n != records {
					t.Errorf("%d rows are staged, want %d", n, records)
				}
			})
		}
	})

	t.Run("a blank line is skipped and still counted", func(t *testing.T) {
		// JSON only, deliberately: a CBOR sequence has no blank item to skip,
		// which is stated in usage.md rather than left to be discovered, and a
		// test that pretended otherwise would be asserting on nothing.
		dir := t.TempDir()
		cfg := ingestConfig(dir)
		store := openStore(t, cfg)
		writers := openIngestWriters(t, store)

		// Three good records with three blank lines among them, and then a record
		// this layer refuses. The bad record is the fourth *record* and it is on
		// the seventh *line*, and both numbers have to be reported: a loop that
		// counted only the lines it kept would say line 4, and one that counted
		// blank lines as records would say record 7.
		stream := strings.Join([]string{
			string(jsonEnvelopeFor(t, "market.fills", fillRow(0))),
			"",
			"   ",
			"",
			string(jsonEnvelopeFor(t, "market.fills", fillRow(1))),
			string(jsonEnvelopeFor(t, "market.fills", fillRow(2))),
			`{"table":"market.fills","row":{"symbol":"X","price":"1.0","ts_ms":1,"typo":true}}`,
		}, "\n") + "\n"

		err := icelake.IngestStream(context.Background(), writers, strings.NewReader(stream), icelake.IngestOptions{})

		var refusal icelake.IngestError
		if !errors.As(err, &refusal) {
			t.Fatalf("IngestStream returned %v (%T), want an icelake.IngestError", err, err)
		}
		if refusal.Record != 4 {
			t.Errorf("the refusal names record %d, want 4: a blank line is not a record", refusal.Record)
		}
		if refusal.Line != 7 {
			t.Errorf("the refusal names line %d, want 7: blank lines are skipped and still counted", refusal.Line)
		}
		if n := len(stagedRowsFor(t, cfg.StagingPath, "market", "fills")); n != 3 {
			t.Errorf("%d rows are staged, want the 3 written before the bad line", n)
		}
	})

	t.Run("a refused record stages the good prefix and names its absolute number", func(t *testing.T) {
		for _, enc := range bothEncodings {
			t.Run(string(enc), func(t *testing.T) {
				dir := t.TempDir()
				cfg := ingestConfig(dir)
				store := openStore(t, cfg)
				writers := openIngestWriters(t, store)

				// The events record comes first in each pair so that its group
				// is written before the fills group that carries the bad row.
				// This is the shape that puts the truncate-and-rewrite under
				// pressure: the fills group is refused whole, and the records
				// before the bad one are only durable because the group is
				// written a second time, cut at it.
				const pairs = 6
				const badAt = 2*pairs + 1

				items := make([][]byte, 0, 2*pairs+1)
				for i := range pairs {
					items = append(items, envelopeFor(t, enc, "archive.events", eventRow(i)))
					items = append(items, envelopeFor(t, enc, "market.fills", fillRow(i)))
				}
				// A well-formed envelope for a declared table whose row carries a
				// key the table does not have, which only the writer can refuse.
				bad := fillRow(pairs)
				bad["typo"] = true
				items = append(items, envelopeFor(t, enc, "market.fills", bad))

				err := icelake.IngestStream(context.Background(), writers,
					bytes.NewReader(streamOf(items)), icelake.IngestOptions{})

				var refusal icelake.IngestError
				if !errors.As(err, &refusal) {
					t.Fatalf("IngestStream returned %v (%T), want an icelake.IngestError", err, err)
				}
				if refusal.Kind != icelake.IngestKindRefused {
					t.Errorf("kind = %q, want %q", refusal.Kind, icelake.IngestKindRefused)
				}
				if refusal.Record != badAt {
					t.Errorf("the refusal names record %d, want %d", refusal.Record, badAt)
				}
				if refusal.Table != "fills" {
					t.Errorf("the refusal names table %q, want fills", refusal.Table)
				}

				// The writer's own refusal is still reachable through it, which
				// is what makes the wrapper additive rather than a replacement.
				var record icelake.RecordError
				if !errors.As(err, &record) {
					t.Fatalf("the refusal does not unwrap to an icelake.RecordError: %v", err)
				}
				if record.Kind != icelake.RecordKindUnknownField || record.Field != "typo" {
					t.Errorf("kind/field = %q/%q, want unknown_field/typo", record.Kind, record.Field)
				}

				// And the batch's own row index is deliberately *not* reachable:
				// it counts from the start of a slice this library built, and the
				// number a caller acts on counts from the start of their stream.
				// Carrying both would put two coordinate systems in one error.
				var batch icelake.BatchError
				if errors.As(err, &batch) {
					t.Errorf("the refusal still carries the library's own row index %d beside the record number", batch.Index)
				}

				// The load-bearing assertion, and the one that fails if the
				// truncated rewrite is ever dropped.
				if n := len(stagedRowsFor(t, cfg.StagingPath, "market", "fills")); n != pairs {
					t.Errorf("%d fills rows are durable, want the %d written before the bad record", n, pairs)
				}

				// The documented residue, stated as the outcomes the design
				// permits rather than as one: the whole stream arrived as a
				// single chunk here, so the events group was written before the
				// fills group failed, and it carries no record after the bad one
				// because there is none to carry.
				if n := len(stagedRowsFor(t, cfg.StagingPath, "archive", "events")); n != pairs {
					t.Errorf("%d events rows are durable, want %d", n, pairs)
				}
			})
		}
	})

	t.Run("the stream's own refusals are typed and name the record", func(t *testing.T) {
		cases := []struct {
			name   string
			stream []byte
			kind   icelake.IngestKind
			record int
		}{
			{"a line that is not JSON", []byte("{\"table\":\n"), icelake.IngestKindGrammar, 1},
			{"a bare row rather than an envelope", []byte("{\"symbol\":\"X\"}\n"), icelake.IngestKindGrammar, 1},
			{"an envelope carrying an unknown key",
				[]byte(`{"table":"market.fills","row":{},"extra":1}` + "\n"), icelake.IngestKindGrammar, 1},
			{"an envelope with no row", []byte(`{"table":"market.fills"}` + "\n"), icelake.IngestKindEnvelope, 1},
			{"a table with no writer",
				[]byte(`{"table":"market.nope","row":{"symbol":"X"}}` + "\n"), icelake.IngestKindUnknownTable, 1},

			{"a CBOR envelope carrying an unknown key",
				cborRaw(map[string]any{"table": "market.fills", "row": map[string]any{}, "extra": 1}),
				icelake.IngestKindGrammar, 1},
			{"a CBOR envelope with no row",
				cborRaw(map[string]any{"table": "market.fills"}), icelake.IngestKindEnvelope, 1},
			{"a CBOR table with no writer",
				cborRaw(map[string]any{"table": "market.nope", "row": map[string]any{"symbol": "X"}}),
				icelake.IngestKindUnknownTable, 1},

			// The first byte is the whole of the dispatch, so a stream whose
			// first byte is neither of the two starts is refused there rather
			// than being tried as one and then the other.
			{"a record starting with a byte that is neither start", []byte("hello\n"), icelake.IngestKindGrammar, 1},
			{"a stream opening with a byte-order mark",
				append([]byte{0xef, 0xbb, 0xbf}, []byte(`{"table":"market.fills","row":{}}`+"\n")...),
				icelake.IngestKindGrammar, 1},
			{"a record that is an indefinite-length CBOR map",
				[]byte{0xbf, 0x65, 't', 'a', 'b', 'l', 'e', 0x61, 'x', 0xff}, icelake.IngestKindGrammar, 1},
			{"a record that is a top-level CBOR array rather than a map",
				cborRaw([]any{1, 2}), icelake.IngestKindGrammar, 1},
			{"a truncated CBOR item at the end of the stream",
				cborRaw(map[string]any{"table": "market.fills", "row": map[string]any{"symbol": "X"}})[:6],
				icelake.IngestKindGrammar, 1},
		}

		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				dir := t.TempDir()
				cfg := ingestConfig(dir)
				store := openStore(t, cfg)
				writers := openIngestWriters(t, store)

				err := icelake.IngestStream(context.Background(), writers, bytes.NewReader(c.stream),
					icelake.IngestOptions{})

				var refusal icelake.IngestError
				if !errors.As(err, &refusal) {
					t.Fatalf("IngestStream returned %v (%T), want an icelake.IngestError", err, err)
				}
				if refusal.Kind != c.kind {
					t.Errorf("kind = %q, want %q", refusal.Kind, c.kind)
				}
				if refusal.Record != c.record {
					t.Errorf("record = %d, want %d", refusal.Record, c.record)
				}
				if n := stagedRows(t, cfg.StagingPath); n != 0 {
					t.Errorf("%d rows are staged after a refusal on the first record", n)
				}
			})
		}
	})

	t.Run("a record over the size bound is refused rather than truncated", func(t *testing.T) {
		for _, enc := range bothEncodings {
			t.Run(string(enc), func(t *testing.T) {
				dir := t.TempDir()
				cfg := ingestConfig(dir)
				store := openStore(t, cfg)
				writers := openIngestWriters(t, store)

				small := envelopeFor(t, enc, "market.fills", fillRow(0))
				big := fillRow(1)
				big["note"] = strings.Repeat("x", 4096)
				items := [][]byte{small, envelopeFor(t, enc, "market.fills", big)}

				// A bound above the first record and below the second, so the
				// test is about the bound rather than about refusing everything.
				bound := len(small) + 16
				err := icelake.IngestStream(context.Background(), writers,
					bytes.NewReader(streamOf(items)),
					icelake.IngestOptions{MaxRecordBytes: bound})

				var refusal icelake.IngestError
				if !errors.As(err, &refusal) {
					t.Fatalf("IngestStream returned %v (%T), want an icelake.IngestError", err, err)
				}
				if refusal.Kind != icelake.IngestKindTooLarge {
					t.Errorf("kind = %q, want %q", refusal.Kind, icelake.IngestKindTooLarge)
				}
				if refusal.Record != 2 {
					t.Errorf("record = %d, want 2", refusal.Record)
				}
				// The record before it is still durable: a size refusal is a
				// refusal like any other and loses nothing that came earlier.
				if n := len(stagedRowsFor(t, cfg.StagingPath, "market", "fills")); n != 1 {
					t.Errorf("%d rows are staged, want the 1 written before the oversized record", n)
				}
			})
		}
	})

	t.Run("a group the ceiling refuses is halved rather than held for good", func(t *testing.T) {
		// The chunk here is five times the whole staging ceiling, which is the
		// case a loop that only slept and retried could never get past: the
		// group is larger than the store's entire capacity, so no amount of
		// draining ever makes the identical group fit. Halving is what turns
		// that into the largest prefix that currently fits.
		//
		// The context is bounded rather than left open so that the failure mode
		// of a regression here is a test that fails with every record accounted
		// for, rather than one that hangs.
		for _, enc := range bothEncodings {
			t.Run(string(enc), func(t *testing.T) {
				dir := t.TempDir()
				cfg := ingestConfig(dir)
				// Small enough to reach in a few records, and a flush that
				// empties it again promptly: in local-only mode a commit is a
				// file being written, so the ceiling really does recover.
				cfg.StagingMaxRecords = 8
				cfg.StagingMaxBytes = 1 << 20
				cfg.FlushMaxBytes = 1 << 16
				cfg.FlushMaxRecords = 4
				store := openStore(t, cfg)
				writers := openIngestWriters(t, store)

				const records = 40
				items := make([][]byte, 0, records)
				for i := range records {
					items = append(items, envelopeFor(t, enc, "market.fills", fillRow(i)))
				}

				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				if err := icelake.IngestStream(ctx, writers, bytes.NewReader(streamOf(items)),
					icelake.IngestOptions{}); err != nil {
					t.Fatalf("IngestStream: %v", err)
				}

				// Records are taken in input order and every attempt is one
				// transaction, so what has to be true at the end is that all of
				// them arrived — through staging, or through the flushes that
				// made room. Closing the writers is what makes the count
				// readable in one place.
				for _, w := range writers {
					if err := w.Close(context.Background()); err != nil {
						t.Fatalf("Close: %v", err)
					}
				}
				if n := cachedRecords(t, filepath.Join(dir, "cache", "market", "fills")); n != records {
					t.Errorf("%d records reached the cache, want %d: a halved group must lose nothing", n, records)
				}
			})
		}
	})

	t.Run("a cancelled context is a clean stop rather than a failure", func(t *testing.T) {
		// Two arms, and the second is the one worth the trouble. A cancellation
		// that lands between chunks is seen by the loop's own select; one that
		// lands while a chunk is being written comes back from the staging store
		// as a failure, and reading that as a runtime failure would make a clean
		// shutdown fail whenever the signal happened to arrive inside a chunk.
		// Both must answer nil.
		t.Run("cancelled between chunks", func(t *testing.T) {
			dir := t.TempDir()
			store := openStore(t, ingestConfig(dir))
			writers := openIngestWriters(t, store)

			reader, producer := io.Pipe()
			ctx, cancel := context.WithCancel(context.Background())

			done := make(chan error, 1)
			go func() { done <- icelake.IngestStream(ctx, writers, reader, icelake.IngestOptions{}) }()

			if _, err := producer.Write(envelopeFor(t, asJSON, "market.fills", fillRow(0))); err != nil {
				t.Fatalf("writing to the pipe: %v", err)
			}
			// Long enough that the loop is certainly back at its blocking read
			// rather than still draining the record above, which is what makes
			// this the between-chunks arm rather than a race with the other one.
			time.Sleep(300 * time.Millisecond)
			cancel()

			select {
			case err := <-done:
				if err != nil {
					t.Errorf("IngestStream returned %v after a cancellation, want nil", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("IngestStream did not return after its context was cancelled")
			}
			_ = producer.Close()
		})

		t.Run("cancelled with records in hand", func(t *testing.T) {
			// The context is already dead when the call starts and the reader is
			// already full, so which of the two arms runs is decided by a select
			// over two ready channels — that is, at random. Several rounds
			// therefore reach both, and the assertion is the same for both,
			// which is the point: neither ending is a failure.
			for round := range 20 {
				dir := t.TempDir()
				store := openStore(t, ingestConfig(dir))
				writers := openIngestWriters(t, store)

				var stream bytes.Buffer
				for i := range 50 {
					stream.Write(jsonEnvelopeFor(t, "market.fills", fillRow(i)))
					stream.WriteByte('\n')
				}

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				if err := icelake.IngestStream(ctx, writers, bytes.NewReader(stream.Bytes()),
					icelake.IngestOptions{}); err != nil {
					t.Fatalf("round %d: IngestStream returned %v for a cancelled context, want nil", round, err)
				}
				if err := store.Close(context.Background()); err != nil {
					t.Fatalf("round %d: Close: %v", round, err)
				}
			}
		})
	})

	t.Run("options are refused before a byte is read", func(t *testing.T) {
		dir := t.TempDir()
		store := openStore(t, ingestConfig(dir))
		writers := openIngestWriters(t, store)

		err := icelake.IngestStream(context.Background(), writers, strings.NewReader("{}\n"),
			icelake.IngestOptions{BackoffMin: time.Minute, BackoffMax: time.Second})

		var invalid icelake.ConfigError
		if !errors.As(err, &invalid) {
			t.Fatalf("IngestStream returned %v (%T) for backwards backoff bounds, want an icelake.ConfigError", err, err)
		}
		var field icelake.ConfigFieldError
		if !errors.As(err, &field) || field.Field != "BackoffMax" {
			t.Errorf("the refusal does not name the BackoffMax field: %v", err)
		}
	})
}

// TestTheTwoFormatsMeanExactlyOneThing is the clause that pins "two formats, one
// meaning" as a fact about bytes rather than a claim about intent.
//
// The same records go into two stores, one as JSON lines and one as a CBOR
// sequence, and what is compared afterwards is the canonically encoded payload
// of every staged row, byte for byte, together with the schema fingerprint it
// was recorded under. Those payloads are what the batch hash is taken over and
// therefore what every object name in the warehouse is derived from, so equal
// payloads mean the two formats produce the same table, the same files and the
// same names — which is the whole of what a second input format is allowed to
// mean.
//
// Comparing the parquet the two runs write would be a weaker test that looked
// stronger: it would pass on any two encodings that happen to round-trip to the
// same values, where this fails the moment one format's decode admits a value
// the other's does not.
func TestTheTwoFormatsMeanExactlyOneThing(t *testing.T) {
	const records = 120

	// pick decides which spelling each record is written in. Three runs: all
	// JSON, all CBOR, and a stream that alternates between them record by
	// record — which is the arrangement the grammar exists for and the one no
	// format setting could express at all.
	staged := func(name string, pick func(i int) encoding) []stagedRow {
		dir := t.TempDir()
		cfg := ingestConfig(dir)
		store := openStore(t, cfg)
		writers := openIngestWriters(t, store)

		items := make([][]byte, 0, 2*records)
		for i := range records {
			items = append(items, envelopeFor(t, pick(2*i), "market.fills", fillRow(i)))
			items = append(items, envelopeFor(t, pick(2*i+1), "archive.events", eventRow(i)))
		}

		if err := icelake.IngestStream(context.Background(), writers, bytes.NewReader(streamOf(items)),
			icelake.IngestOptions{}); err != nil {
			t.Fatalf("IngestStream(%s): %v", name, err)
		}

		return append(
			stagedRowsFor(t, cfg.StagingPath, "market", "fills"),
			stagedRowsFor(t, cfg.StagingPath, "archive", "events")...)
	}

	runs := map[string][]stagedRow{
		"json":  staged("json", func(int) encoding { return asJSON }),
		"cbor":  staged("cbor", func(int) encoding { return asCBOR }),
		"mixed": staged("mixed", func(i int) encoding { return bothEncodings[i%2] }),
	}

	reference := runs["json"]
	if len(reference) != 2*records {
		t.Fatalf("%d rows staged from the JSON run, want %d", len(reference), 2*records)
	}
	for name, rows := range runs {
		if len(rows) != len(reference) {
			t.Fatalf("the %s run staged %d rows and the json run staged %d", name, len(rows), len(reference))
		}
		for i := range rows {
			if rows[i].schemaFP != reference[i].schemaFP {
				t.Fatalf("the %s run recorded row %d under fingerprint %s and the json run under %s",
					name, i, rows[i].schemaFP, reference[i].schemaFP)
			}
			if !bytes.Equal(rows[i].payload, reference[i].payload) {
				t.Fatalf("the %s run staged %d bytes for row %d and the json run staged %d, and they differ: "+
					"one record has one meaning whichever spelling it arrived in",
					name, len(rows[i].payload), i, len(reference[i].payload))
			}
		}
	}
}

// TestAMixedStreamNumbersRecordsAndLinesSeparately is the refusal half of the
// mixed-stream claim: in a stream that carries both spellings, the record number
// counts every record and the line number counts only lines.
//
// It is the clause that would have been impossible under a format setting, and
// it is also where the two numbers visibly stop being the same number — which is
// the reason [icelake.IngestError] carries both rather than one relabelled.
func TestAMixedStreamNumbersRecordsAndLinesSeparately(t *testing.T) {
	dir := t.TempDir()
	cfg := ingestConfig(dir)
	store := openStore(t, cfg)
	writers := openIngestWriters(t, store)

	// Record 1 is JSON on line 1. Records 2 and 3 are CBOR and are on no line
	// at all — they consume no newline, so the line counter does not move.
	// Record 4 is JSON and is therefore still on line 2. A blank line follows,
	// putting record 5 on line 4, and record 5 is the bad one.
	bad := fillRow(99)
	bad["typo"] = true

	var stream bytes.Buffer
	stream.Write(envelopeFor(t, asJSON, "market.fills", fillRow(0)))
	stream.Write(envelopeFor(t, asCBOR, "market.fills", fillRow(1)))
	stream.Write(envelopeFor(t, asCBOR, "archive.events", eventRow(2)))
	stream.Write(envelopeFor(t, asJSON, "market.fills", fillRow(3)))
	stream.WriteString("\n")
	stream.Write(envelopeFor(t, asJSON, "market.fills", bad))

	err := icelake.IngestStream(context.Background(), writers, bytes.NewReader(stream.Bytes()), icelake.IngestOptions{})

	var refusal icelake.IngestError
	if !errors.As(err, &refusal) {
		t.Fatalf("IngestStream returned %v (%T), want an icelake.IngestError", err, err)
	}
	if refusal.Record != 5 {
		t.Errorf("the refusal names record %d, want 5", refusal.Record)
	}
	if refusal.Line != 4 {
		t.Errorf("the refusal names line %d, want 4: a CBOR record is on no line and consumes no newline", refusal.Line)
	}
	if !strings.Contains(refusal.Error(), "record 5 (line 4)") {
		t.Errorf("the message does not carry both numbers: %v", refusal)
	}

	// And everything before the bad record is durable, mixed spellings and all.
	fills := len(stagedRowsFor(t, cfg.StagingPath, "market", "fills"))
	events := len(stagedRowsFor(t, cfg.StagingPath, "archive", "events"))
	if fills != 3 || events != 1 {
		t.Errorf("%d fills and %d events rows are durable, want 3 and 1", fills, events)
	}

	// A CBOR record refused in a mixed stream reports no line, because there is
	// no line for it to be on.
	t.Run("a refused CBOR record reports no line", func(t *testing.T) {
		dir := t.TempDir()
		cfg := ingestConfig(dir)
		store := openStore(t, cfg)
		writers := openIngestWriters(t, store)

		var stream bytes.Buffer
		stream.Write(envelopeFor(t, asJSON, "market.fills", fillRow(0)))
		stream.Write(envelopeFor(t, asCBOR, "market.fills", bad))

		err := icelake.IngestStream(context.Background(), writers, bytes.NewReader(stream.Bytes()),
			icelake.IngestOptions{})

		var refusal icelake.IngestError
		if !errors.As(err, &refusal) {
			t.Fatalf("IngestStream returned %v (%T), want an icelake.IngestError", err, err)
		}
		if refusal.Record != 2 || refusal.Line != 0 {
			t.Errorf("record/line = %d/%d, want 2/0", refusal.Record, refusal.Line)
		}
		if strings.Contains(refusal.Error(), "line") {
			t.Errorf("the message calls a CBOR record a line, which is a thing this spelling does not have: %v", refusal)
		}
		_ = cfg
	})
}

// coerceCBORDocument is the eight-column shape the JSON coercion matrix uses, so
// that the two matrices are the same table asked in two languages.
const coerceCBORDocument = `{"tables": [
  {"namespace": "market", "table": "coerce", "fields": [
    {"name": "flag",    "type": "boolean",       "fieldid": 1},
    {"name": "count",   "type": "int",           "fieldid": 2},
    {"name": "seq",     "type": "long",          "fieldid": 3},
    {"name": "price",   "type": "decimal(18,9)", "fieldid": 4},
    {"name": "at",      "type": "timestamptz",   "fieldid": 5},
    {"name": "symbol",  "type": "string",        "fieldid": 6},
    {"name": "payload", "type": "binary",        "fieldid": 7},
    {"name": "note",    "type": "string",        "fieldid": 8, "optional": true},
    {"name": "ratio",   "type": "double",        "fieldid": 9, "optional": true}
  ]}
]}`

// cborRaw marshals a value with the default encoder, for the cases whose bytes
// are ordinary. The exotic ones below are written out as bytes, because what
// they are testing is an encoding no encoder configured this way would produce.
func cborRaw(v any) []byte {
	item, err := cbor.Marshal(v)
	if err != nil {
		panic("marshalling a CBOR fixture: " + err.Error())
	}

	return item
}

// TestCBORCoercionMatrix is SCHEMA.md's CBOR column, one case per row, asserted
// on the typed error's Kind and Field rather than on its message.
//
// It mirrors the JSON matrix in scenario 10 deliberately and case for case where
// the two tables share a rule, because the whole claim of this format is that it
// does not have rules of its own — and a matrix that only listed the CBOR
// refusals would leave "the shared rows still hold here" untested. The rows that
// exist only here are the ones a format that can express more than JSON makes
// reachable: a byte string, an integer past 2^53, a tag, and undefined.
//
// The declared shape is the JSON matrix's eight columns plus one `double`, and
// the extra column is named here rather than left to be noticed: CBOR carries
// three float widths where JSON has one spelling, so the float rows have
// somewhere to land that the eight-column shape does not give them.
func TestCBORCoercionMatrix(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := ingestConfig(dir)
	store := openStore(t, cfg)

	writer, err := icelake.OpenDynamicWriter(ctx, store, tableFromDocument(t, coerceCBORDocument, "market", "coerce"))
	if err != nil {
		t.Fatalf("OpenDynamicWriter: %v", err)
	}

	// A record that is accepted, so every case below differs from a good one in
	// exactly one way.
	good := map[string]any{
		"flag":    true,
		"count":   7,
		"seq":     9,
		"price":   "1.5",
		"at":      1_700_000_000_000,
		"symbol":  "SYM",
		"payload": []byte{0x01, 0x02},
		"ratio":   1.5,
	}
	with := func(mutate func(row map[string]any)) []byte {
		row := make(map[string]any, len(good))
		for k, v := range good {
			row[k] = v
		}
		mutate(row)

		return cborRaw(row)
	}

	refusals := []struct {
		name  string
		item  []byte
		field string
		kind  icelake.RecordKind
	}{
		// The rows this table shares with the JSON one, asked in CBOR.
		{"a key the declaration does not have", with(func(r map[string]any) { r["sybmol"] = "SYM" }), "sybmol", icelake.RecordKindUnknownField},
		{"a required column with no key", with(func(r map[string]any) { delete(r, "symbol") }), "symbol", icelake.RecordKindMissing},
		{"a null in a required column", with(func(r map[string]any) { r["symbol"] = nil }), "symbol", icelake.RecordKindNull},
		{"a boolean spelled as 0", with(func(r map[string]any) { r["flag"] = 0 }), "flag", icelake.RecordKindType},
		{`a boolean spelled as "true"`, with(func(r map[string]any) { r["flag"] = "true" }), "flag", icelake.RecordKindType},
		{"a number into a string column", with(func(r map[string]any) { r["symbol"] = 1 }), "symbol", icelake.RecordKindType},
		{"a string where a number belongs", with(func(r map[string]any) { r["seq"] = "12" }), "seq", icelake.RecordKindType},
		{"a float in an integer column", with(func(r map[string]any) { r["seq"] = 1.5 }), "seq", icelake.RecordKindInexact},
		{"an integer past the range of an int", with(func(r map[string]any) { r["count"] = 2147483648 }), "count", icelake.RecordKindRange},
		{"an integer below the range of an int", with(func(r map[string]any) { r["count"] = -2147483649 }), "count", icelake.RecordKindRange},
		{"a decimal with more fraction digits than its scale", with(func(r map[string]any) { r["price"] = "1.0123456789" }), "price", icelake.RecordKindInexact},
		{"a decimal written with an exponent", with(func(r map[string]any) { r["price"] = "1e3" }), "price", icelake.RecordKindInexact},
		{"a decimal with no digit before its point", with(func(r map[string]any) { r["price"] = ".5" }), "price", icelake.RecordKindMalformed},
		{"a timestamp finer than a millisecond", with(func(r map[string]any) { r["at"] = "2026-08-01T00:00:00.0001Z" }), "at", icelake.RecordKindInexact},
		{"a timestamp that is not RFC3339", with(func(r map[string]any) { r["at"] = "yesterday" }), "at", icelake.RecordKindMalformed},

		// A CBOR float is refused for a decimal exactly as a JSON float64 is,
		// and this is the row the format makes easy to get wrong: a producer
		// that would have had to type digits into JSON can hand over a double
		// here without noticing, so the refusal has to be unconditional.
		{"a float in a decimal column, whatever its value", with(func(r map[string]any) { r["price"] = 1.5 }), "price", icelake.RecordKindType},
		{"a whole float in a decimal column", with(func(r map[string]any) { r["price"] = 2.0 }), "price", icelake.RecordKindType},
		{"a non-integral float in a timestamptz column", with(func(r map[string]any) { r["at"] = 1.5 }), "at", icelake.RecordKindInexact},
		{"an integral float above 2^53 in a long column", with(func(r map[string]any) { r["seq"] = float64(1 << 60) }), "seq", icelake.RecordKindInexact},

		// The rows that exist only in this format.
		{"a byte string in a string column", with(func(r map[string]any) { r["symbol"] = []byte("SYM") }), "symbol", icelake.RecordKindType},
		{"a byte string in a double column", with(func(r map[string]any) { r["ratio"] = []byte{0x01} }), "ratio", icelake.RecordKindType},
		{"a byte string in a decimal column", with(func(r map[string]any) { r["price"] = []byte("1.5") }), "price", icelake.RecordKindType},
		{"a text string in a binary column that is not base64", with(func(r map[string]any) { r["payload"] = "not base64!" }), "payload", icelake.RecordKindMalformed},
		{"an integer past the range of a long", with(func(r map[string]any) { r["seq"] = uint64(math.MaxUint64) }), "seq", icelake.RecordKindRange},
		{"an array where a scalar belongs", with(func(r map[string]any) { r["symbol"] = []any{1, 2} }), "symbol", icelake.RecordKindType},
		{"a map where a scalar belongs", with(func(r map[string]any) { r["symbol"] = map[string]any{"a": 1} }), "symbol", icelake.RecordKindType},

		// Tags, all of them, named individually because each is a value a
		// producer might reasonably believe icelake would understand.
		{"a tag 0 date/time in a timestamptz column",
			cborWithRawField(with(func(r map[string]any) { delete(r, "at") }), "at",
				append([]byte{0xc0, 0x74}, []byte("2023-08-01T00:00:00Z")...)), "at", icelake.RecordKindType},
		{"a tag 1 epoch time in a timestamptz column",
			cborWithRawField(with(func(r map[string]any) { delete(r, "at") }), "at",
				[]byte{0xc1, 0x1a, 0x51, 0x4b, 0x67, 0xb0}), "at", icelake.RecordKindType},
		{"a tag 2 bignum in a decimal column",
			cborWithRawField(with(func(r map[string]any) { delete(r, "price") }), "price",
				[]byte{0xc2, 0x42, 0x01, 0x00}), "price", icelake.RecordKindType},
		{"a tag on a column that has nothing to do with tags",
			cborWithRawField(with(func(r map[string]any) { delete(r, "symbol") }), "symbol",
				[]byte{0xd8, 0x20, 0x63, 'a', 'b', 'c'}), "symbol", icelake.RecordKindType},

		// undefined is not null, and reading one as the other would decide
		// quietly that they are the same.
		{"undefined in an optional column",
			cborWithRawField(with(func(r map[string]any) { delete(r, "note") }), "note", []byte{0xf7}),
			"note", icelake.RecordKindType},
		{"undefined in a required column",
			cborWithRawField(with(func(r map[string]any) { delete(r, "symbol") }), "symbol", []byte{0xf7}),
			"symbol", icelake.RecordKindType},
	}

	for _, c := range refusals {
		t.Run(c.name, func(t *testing.T) {
			err := writer.InsertCBOR(ctx, c.item)

			var recErr icelake.RecordError
			if !errors.As(err, &recErr) {
				t.Fatalf("InsertCBOR returned %v (%T), want an icelake.RecordError", err, err)
			}
			if recErr.Kind != c.kind {
				t.Errorf("kind = %q, want %q", recErr.Kind, c.kind)
			}
			if recErr.Field != c.field {
				t.Errorf("field = %q, want %q", recErr.Field, c.field)
			}
			if recErr.Table != "coerce" || recErr.Namespace != "market" {
				t.Errorf("table = %s.%s, want market.coerce", recErr.Namespace, recErr.Table)
			}
		})
	}

	// The refusals about the item rather than about any one column.
	items := []struct {
		name string
		item []byte
	}{
		{"an item that is not a map", cborRaw([]any{1, 2, 3})},
		{"an item that is CBOR null", []byte{0xf6}},
		{"an item whose map has a key that is not text", []byte{0xa1, 0x01, 0x02}},
		{"an item carrying more than one CBOR value", append(cborRaw(map[string]any{}), cborRaw(map[string]any{})...)},
		{"an item that is a truncated CBOR value", []byte{0xa1, 0x61}},
		// A duplicate key is refused rather than resolved first-wins or
		// last-wins: two producers can disagree about which rule a decoder
		// applies while both believing they are right.
		{"an item whose map carries a key twice", []byte{0xa2, 0x61, 'a', 0x01, 0x61, 'a', 0x02}},
	}

	for _, c := range items {
		t.Run(c.name, func(t *testing.T) {
			err := writer.InsertCBOR(ctx, c.item)

			var recErr icelake.RecordError
			if !errors.As(err, &recErr) {
				t.Fatalf("InsertCBOR returned %v (%T), want an icelake.RecordError", err, err)
			}
			if recErr.Kind != icelake.RecordKindMalformed {
				t.Errorf("kind = %q, want %q", recErr.Kind, icelake.RecordKindMalformed)
			}
			if recErr.Field != "" {
				t.Errorf("field = %q, want the empty string: the item as a whole is what is wrong", recErr.Field)
			}
		})
	}

	// The acceptances, which are what stop a matrix of refusals being satisfied
	// by a door that refuses everything.
	accepted := []struct {
		name string
		item []byte
	}{
		{"the good record itself", cborRaw(good)},
		{"an integer past 2^53, which CBOR carries exactly",
			with(func(r map[string]any) { r["seq"] = int64(1)<<60 + 1 })},
		{"the largest integer a long holds", with(func(r map[string]any) { r["seq"] = int64(math.MaxInt64) })},
		{"the smallest integer a long holds", with(func(r map[string]any) { r["seq"] = int64(math.MinInt64) })},
		{"an integer in a decimal column", with(func(r map[string]any) { r["price"] = 3 })},
		// An integral float under 2^53 in an integer column is accepted on
		// exactly the terms a JSON one is: the value really did pass through a
		// double, and below that magnitude a double still knows which whole
		// number it is.
		{"an integral float in a timestamptz column", with(func(r map[string]any) { r["at"] = 1.7e12 })},
		{"a timestamp as an RFC3339 text string at whole milliseconds",
			with(func(r map[string]any) { r["at"] = "2026-08-01T00:00:00.001Z" })},
		{"an empty byte string in a binary column", with(func(r map[string]any) { r["payload"] = []byte{} })},
		// The three float widths are one column type. Nothing about a double
		// column changes because a producer chose fewer bytes for the value.
		{"a half-precision float in a double column",
			cborWithRawField(cborRaw(good), "ratio", []byte{0xf9, 0x3c, 0x00})},
		{"a single-precision float in a double column",
			cborWithRawField(cborRaw(good), "ratio", []byte{0xfa, 0x3f, 0xc0, 0x00, 0x00})},
		{"an integer in a double column", with(func(r map[string]any) { r["ratio"] = 3 })},
		{"a base64 text string in a binary column, which the JSON spelling still is",
			with(func(r map[string]any) { r["payload"] = base64.StdEncoding.EncodeToString([]byte{9, 9}) })},
		{"an optional column left out", with(func(r map[string]any) { delete(r, "note") })},
		{"an optional column explicitly null", with(func(r map[string]any) { r["note"] = nil })},
	}

	for _, c := range accepted {
		t.Run(c.name, func(t *testing.T) {
			if err := writer.InsertCBOR(ctx, c.item); err != nil {
				t.Fatalf("InsertCBOR refused a record the coercion table accepts: %v", err)
			}
		})
	}

	// A refused record is not accepted: nothing any of those cases sent reached
	// the staging database.
	if n := stagedRows(t, cfg.StagingPath); n != len(accepted) {
		t.Errorf("%d rows are staged after %d refusals and %d acceptances; a refused record is never accepted",
			n, len(refusals)+len(items), len(accepted))
	}

	// The poison check still runs underneath the coercion table, in this format
	// exactly as in the other.
	t.Run("a decimal past the column's precision is still a poison", func(t *testing.T) {
		var poison icelake.PoisonError
		item := with(func(r map[string]any) { r["price"] = "1000000000.5" })
		if err := writer.InsertCBOR(ctx, item); !errors.As(err, &poison) {
			t.Fatalf("InsertCBOR returned %v (%T), want an icelake.PoisonError", err, err)
		} else if poison.Reason != icelake.PoisonReasonDecimalRange {
			t.Errorf("reason = %q, want %q", poison.Reason, icelake.PoisonReasonDecimalRange)
		}
	})

	// And a text string that is not UTF-8 is refused by the decoder before the
	// poison check would refuse it, which is two lines of defence rather than a
	// duplicate: this one is about CBOR's own rules and that one is about what a
	// column may hold.
	t.Run("a text string that is not valid UTF-8", func(t *testing.T) {
		item := cborWithRawField(cborRaw(good), "symbol", []byte{0x62, 0xff, 0xfe})

		var recErr icelake.RecordError
		if err := writer.InsertCBOR(ctx, item); !errors.As(err, &recErr) {
			t.Fatalf("InsertCBOR returned %v (%T), want an icelake.RecordError", err, err)
		} else if recErr.Kind != icelake.RecordKindMalformed {
			t.Errorf("kind = %q, want %q", recErr.Kind, icelake.RecordKindMalformed)
		}
	})

	// The batch door names the item at fault by its index, exactly as the JSON
	// batch door does, and stages none of the slice.
	t.Run("the batch door names the index of the item at fault", func(t *testing.T) {
		before := stagedRows(t, cfg.StagingPath)
		items := [][]byte{
			cborRaw(good),
			cborRaw(good),
			with(func(r map[string]any) { r["price"] = 1.5 }),
			cborRaw(good),
		}

		var batch icelake.BatchError
		if err := writer.InsertCBORBatch(ctx, items); !errors.As(err, &batch) {
			t.Fatalf("InsertCBORBatch returned %v (%T), want an icelake.BatchError", err, err)
		} else if batch.Index != 2 {
			t.Errorf("index = %d, want 2", batch.Index)
		}
		var recErr icelake.RecordError
		if err := writer.InsertCBORBatch(ctx, items); !errors.As(err, &recErr) || recErr.Field != "price" {
			t.Errorf("the batch refusal does not unwrap to the record's own error about price: %v", err)
		}
		if n := stagedRows(t, cfg.StagingPath); n != before {
			t.Errorf("%d rows are staged, want %d: a refused batch stages none of itself", n, before)
		}
	})

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// cborWithRawField rewrites one field of an already-encoded CBOR map to
// arbitrary bytes.
//
// It exists because the cases that matter most in the matrix above are
// encodings the ordinary encoder will not produce — a tag, undefined, invalid
// UTF-8 — and hand-assembling a whole map around each of them would put more
// bytes in this file than the case under test.
func cborWithRawField(item []byte, field string, raw []byte) []byte {
	var fields map[string]cbor.RawMessage
	if err := cbor.Unmarshal(item, &fields); err != nil {
		panic("re-reading a CBOR fixture: " + err.Error())
	}
	fields[field] = raw

	out, err := cbor.Marshal(fields)
	if err != nil {
		panic("re-writing a CBOR fixture: " + err.Error())
	}

	return out
}
