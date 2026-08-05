package icelake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/gvkhna/icelake/internal/errdef"
)

// The defaults [IngestStream] fills a zero [IngestOptions] field in with. They
// are exported because an embedder that wants to raise one of them needs to
// know what it is raising from, and because the numbers are quoted in the
// documentation of every program that ships this library.
//
// They are the figures `cmd/icelake` ran with before this entry point existed,
// unchanged: this milestone moved the pump, and moving it was not the occasion
// to retune it.
const (
	// DefaultIngestMaxRecordBytes is the longest single record accepted. A
	// longer one is refused rather than truncated.
	DefaultIngestMaxRecordBytes = 16 << 20
	// DefaultIngestChunkMaxRecords is the most records one staging transaction
	// is given. It sits where the measured curve has long since flattened
	// (ARCHITECTURE.md, M15): the gain from a thousand records per transaction
	// to ten thousand is a fifth of the gain from a hundred to a thousand, so a
	// larger chunk would buy latency and memory for nothing.
	DefaultIngestChunkMaxRecords = 4096
	// DefaultIngestChunkMaxBytes is the most record bytes one chunk collects
	// before it is written. It bounds the chunk from the other end: a record
	// count alone would let 4096 records of a stream whose records are
	// megabytes become a gigabyte held in memory.
	DefaultIngestChunkMaxBytes = 8 << 20
	// DefaultIngestReadAhead is how many records may sit between the reader and
	// the chunk being written. Everything past it is still in the caller's
	// reader, which is where backpressure has to reach.
	DefaultIngestReadAhead = 1024
	// DefaultIngestBackoffMin is the first delay after the staging ceiling
	// refuses a group that is already down to a single record.
	DefaultIngestBackoffMin = 100 * time.Millisecond
	// DefaultIngestBackoffMax is the cap that backoff doubles up to.
	DefaultIngestBackoffMax = 5 * time.Second
)

// IngestNoticeKind says which of the two things worth telling a caller about
// has happened.
type IngestNoticeKind string

const (
	// IngestNoticeHeld means the staging ceiling has refused a single record,
	// so the reader has stopped and is waiting for the bucket to catch up.
	IngestNoticeHeld IngestNoticeKind = "held"
	// IngestNoticeResumed means room was found and reading has started again.
	IngestNoticeResumed IngestNoticeKind = "resumed"
)

// IngestNotice is one thing [IngestStream] tells a caller about a run that is
// still going.
//
// It exists because the library reports nothing itself, by design
// (ARCHITECTURE.md, "Observability"), and holding a caller's reader for minutes
// with no explanation is the one place that silence is indistinguishable from a
// hang. So the fact is handed to the caller and the caller decides what a log
// line looks like — exactly the arrangement [Config.OnFlushError] already uses,
// for the same reason.
type IngestNotice struct {
	// Kind says whether reading stopped or started again.
	Kind IngestNoticeKind
	// Namespace and Table name the table whose write was refused.
	Namespace string
	Table     string
	// Record is the position in the stream the notice is about, counted from
	// one: the first record of what the writer is holding.
	Record int
	// Line is the record's physical line number, and is zero when the record was
	// a CBOR one, which is not on a line. See [IngestError.Line].
	Line int
}

// Position renders this notice's place in the stream the way icelake's own
// messages render one, so a program printing it says what icelake would have.
func (n IngestNotice) Position() string { return errdef.IngestPosition(n.Record, n.Line) }

// IngestOptions is everything [IngestStream] can be told. Every field is
// optional; a zero field takes the documented default beside it.
//
// There is deliberately no field for the input format. The grammar decides each
// record's encoding from that record's own first byte, so there is nothing here
// for a caller to configure and nothing to get wrong — see [IngestStream].
type IngestOptions struct {
	// MaxRecordBytes is the longest single record accepted, refused rather than
	// truncated. It bounds one JSON line, not counting its terminator, and one
	// CBOR data item. Zero means [DefaultIngestMaxRecordBytes].
	MaxRecordBytes int
	// ChunkMaxRecords is the most records handed to one table's writer at once.
	// Zero means [DefaultIngestChunkMaxRecords].
	ChunkMaxRecords int
	// ChunkMaxBytes is the most record bytes one chunk collects. The bound is
	// checked before a record is taken rather than after it is added, so the
	// last record of a chunk may carry it past the figure: the true bound is
	// this plus one record, and it is stated that way rather than rounded down.
	// Zero means [DefaultIngestChunkMaxBytes].
	ChunkMaxBytes int
	// ReadAhead is how many records may sit between the reader and the chunk
	// being written. Zero means [DefaultIngestReadAhead]. It is a record count
	// and not a byte count, so what it costs in memory is this figure times the
	// length of the records actually arriving.
	ReadAhead int
	// BackoffMin and BackoffMax bound the delay between retries of a single
	// record the staging ceiling will not take. Zero means
	// [DefaultIngestBackoffMin] and [DefaultIngestBackoffMax].
	BackoffMin time.Duration
	BackoffMax time.Duration
	// OnNotice, if set, is called when reading stops because staging is full
	// and again when it starts. It runs on the goroutine that called
	// [IngestStream], between writes, so a slow one delays the stream; that is
	// the same contract [Config.OnAccept] has and for the same reason, since
	// this one exists to be printed.
	OnNotice func(IngestNotice)
}

// resolved fills in every default, so nothing below this has to ask twice
// whether a field was set.
func (o IngestOptions) resolved() IngestOptions {
	if o.MaxRecordBytes <= 0 {
		o.MaxRecordBytes = DefaultIngestMaxRecordBytes
	}
	if o.ChunkMaxRecords <= 0 {
		o.ChunkMaxRecords = DefaultIngestChunkMaxRecords
	}
	if o.ChunkMaxBytes <= 0 {
		o.ChunkMaxBytes = DefaultIngestChunkMaxBytes
	}
	if o.ReadAhead <= 0 {
		o.ReadAhead = DefaultIngestReadAhead
	}
	if o.BackoffMin <= 0 {
		o.BackoffMin = DefaultIngestBackoffMin
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = DefaultIngestBackoffMax
	}

	return o
}

// validate refuses an options value before a byte is read, in the shape
// [Config] refuses one: a [ConfigError] wrapping one [ConfigFieldError] per
// violation, so a caller fixing several learns all of them at once.
func (o IngestOptions) validate() error {
	var bad []error
	if o.BackoffMax < o.BackoffMin {
		bad = append(bad, errdef.ConfigFieldError{Field: "BackoffMax", Rule: "must not be smaller than BackoffMin"})
	}
	if len(bad) > 0 {
		return errdef.ConfigError{Err: errors.Join(bad...)}
	}

	return nil
}

// IngestStream reads envelopes from r and writes each one's row into the writer
// its table names, until the input ends or ctx does.
//
// **This is where a program that pipes records into icelake keeps its ingest
// loop, and the reason it is a library call rather than something each caller
// writes is that every decision inside it is a decision about durability.**
// Chunking, the size of a chunk, what happens to the records before a bad one,
// how a group the staging ceiling refuses is retried, and which failures mean
// "you were told to stop" rather than "something broke" are all answers this
// library owes rather than answers a caller should have to arrive at. A CLI
// over this is then exactly a translation — environment into options, a pipe
// into this call, a signal into ctx, an error into an exit code — which is the
// rule `AGENTS.md` states for every command in this repository.
//
// # The grammar
//
// A record is an envelope, and an envelope is a map with exactly the two keys
// `table` and `row`. It may be written in either of two encodings, and **which
// one it is, is decided per record from that record's own first byte**:
//
//   - `{` (0x7b) starts a JSON envelope. The record runs to the next newline,
//     LF or CRLF, and the terminator is not part of it — ordinary NDJSON.
//   - A definite-length CBOR map header (0xa0 to 0xbb) starts a CBOR envelope.
//     The record is that one data item and nothing else, which is what makes a
//     CBOR sequence (RFC 8742) self-delimiting: there is no separator, because
//     an item already says how long it is.
//   - Whitespace between records — space, tab, CR and LF — is skipped. That is
//     what makes a blank line in a JSON stream a blank line rather than a
//     record, and the newlines are counted, so the line numbers reported are
//     the line numbers in the operator's file.
//   - Any other byte is refused at that record, loudly, naming its position. A
//     UTF-8 byte-order mark is refused rather than skipped: it is not
//     whitespace, and a stream that begins with one was written by a tool that
//     believes it is producing text for a person.
//
// **There is no setting for the encoding, and the reason is that the two first
// bytes cannot collide.** In CBOR, 0x7b is the header of a text string with an
// eight-byte length, and a top-level text string is not a map and can therefore
// never be an envelope; in JSON, no encoding of any value begins with a byte in
// 0xa0 to 0xbb at all. So the two sets are disjoint by construction rather than
// by likelihood, and this is a dispatch rather than a sniff: nothing here
// guesses, scores, or falls back to a second attempt. What it buys is the thing
// a setting cannot — one producer can write JSON into a running stream and the
// next can write CBOR into the same one, interleaved record by record, with
// nothing configured anywhere and no restart between them.
//
// An indefinite-length map header (0xbf) is refused rather than accepted, and
// so is an indefinite-length item anywhere inside a record. RFC 8949's rules
// for deterministic encoding exclude them, and this library's posture on input
// is that a value has exactly one spelling: two encodings of one map are one
// thing two producers can disagree about while both believing they are right.
//
// # What it promises
//
// The writers map is keyed by the envelope's own `table` field, which is
// conventionally "namespace.name": an envelope naming a key this map does not
// have is refused with [IngestKindUnknownTable] rather than guessed at. The map
// is read and never written, and the writers are used from the calling
// goroutine only.
//
// **What it promises about a stream that goes wrong is the whole point, so it
// is exact.** Every record accepted before the refused one is durable in
// staging when this returns; whatever was still in r was never icelake's. A
// record this layer itself refuses — not an envelope, an envelope missing a
// field, a table with no writer, a row that will not decode, a record over the
// size bound — is found while a chunk is being read, before anything in that
// chunk has been written, so the chunk is cut at it. A row a *writer* refuses —
// a value its column cannot hold — is found while the chunk is being written,
// and it refuses that table's whole group because the group is one transaction;
// so the group is written again, cut at the bad record, and every later group
// is cut at it too. The one thing that cannot be undone is a group already
// written that carried records after the bad one, which is reachable only when
// a single chunk interleaves two tables.
//
// It returns nil for both ordinary endings — the end of r, and a ctx that was
// cancelled — because neither is a failure, and a caller needs to know whether
// it stopped because it was told to or because something is wrong. Every other
// ending is an [IngestError] naming the record it stopped at.
//
// It does not close the writers or the store. A caller drains on its own terms:
// the records this call accepted are durable whether or not that drain
// succeeds, and a drain wants a context that a signal has not already
// cancelled.
func IngestStream(ctx context.Context, writers map[string]*DynamicWriter, r io.Reader, opts IngestOptions) error {
	opts = opts.resolved()
	if err := opts.validate(); err != nil {
		return err
	}

	records, readErr := readRecords(ctx, r, opts)

	chunk := make([]numberedRecord, 0, min(opts.ChunkMaxRecords, DefaultIngestChunkMaxRecords))
	for {
		var outcome chunkOutcome
		chunk, outcome = nextChunk(ctx, records, chunk[:0], opts)

		if err := writeChunk(ctx, chunk, writers, opts); err != nil {
			if errors.Is(err, errIngestStopped) {
				return nil
			}

			return err
		}

		switch outcome {
		case chunkStopped:
			// ctx ended, and the only ending that can arrive with nothing to
			// write. What was accepted is already durable.
			return nil

		case chunkEnded:
			// The reader is finished: either r ended or it stopped because ctx
			// did. A framing error is the one thing that distinguishes an end of
			// input from a record this layer refuses to read at all.
			if err := <-readErr; err != nil {
				return err
			}

			return nil

		case chunkMore:
			// More input is coming, and there is nothing to do but read again.
			// This outcome is a third value rather than the absence of the other
			// two because **the first version of this loop, in the command this
			// moved from, treated "not ended and nothing to write" as the
			// cancellation case and returned nil** — so a blank line arriving on
			// a live pipe ended the run with a clean answer and silently dropped
			// the rest of the stream. Nothing in the suite noticed, because a
			// test that writes its whole input at once has the next record
			// queued before the blank one is drained, and the chunk therefore
			// never comes back empty. It takes a producer that pauses — which is
			// every real producer — to reach it.
		}
	}
}

// errIngestStopped means ctx ended while a chunk was being written. It never
// crosses the API boundary: [IngestStream] turns it back into the clean nil
// ending, exactly as it does for a cancellation that arrives between chunks.
var errIngestStopped = errors.New("icelake: the stream was stopped")

// numberedRecord is one record of input with everything a refusal needs to name
// it: its position in the stream, the line it was on if it was on one, and
// which of the two encodings it arrived in.
type numberedRecord struct {
	number int
	line   int
	json   bool
	item   []byte
}

// chunkOutcome says why [nextChunk] stopped collecting, which is three distinct
// answers and not two: the input ended, the context ended, or neither and there
// is more to read. Conflating the third with either of the others ends a live
// run.
type chunkOutcome int

const (
	// chunkMore means the input has not ended and neither has the context.
	chunkMore chunkOutcome = iota
	// chunkEnded means the record reader closed its channel.
	chunkEnded
	// chunkStopped means the context ended while waiting for a record.
	chunkStopped
)

// nextChunk blocks for one record and then takes whatever else has already
// arrived, up to the chunk bounds, without waiting for any of it.
//
// The blocking first receive is what makes a trickle of input behave exactly as
// it would if every record were written on its own, and the non-blocking drain
// after it is what makes a flood be written in groups: the chunk grows to
// whatever the producer has managed to queue, and no further. Nothing here
// waits on a timer, so a slow producer is never delayed by a batching
// heuristic.
//
// It returns the chunk and why it stopped. A chunk comes back empty only for
// the two endings, which is why the outcome distinguishes them rather than
// being a bare "ended" flag.
func nextChunk(
	ctx context.Context, records <-chan numberedRecord, chunk []numberedRecord, opts IngestOptions,
) ([]numberedRecord, chunkOutcome) {
	select {
	case rec, ok := <-records:
		if !ok {
			return chunk, chunkEnded
		}
		chunk = append(chunk, rec)
	case <-ctx.Done():
		// The record currently in the framing's hands, if any, is simply not
		// taken: it was never accepted, so it was never icelake's, and what was
		// accepted is already durable.
		return chunk, chunkStopped
	}

	size := len(chunk[0].item)
	// The byte bound is checked before a record is read rather than after it is
	// added, so the last record of a chunk may carry it past the figure: the
	// true bound is ChunkMaxBytes plus one record, and it is documented that
	// way rather than rounded down. Checking afterwards would mean either
	// putting a record back, which a channel cannot do, or holding it for a
	// chunk that may never be asked for.
	for len(chunk) < opts.ChunkMaxRecords && size < opts.ChunkMaxBytes {
		select {
		case rec, ok := <-records:
			if !ok {
				return chunk, chunkEnded
			}
			size += len(rec.item)
			chunk = append(chunk, rec)
		default:
			return chunk, chunkMore
		}
	}

	return chunk, chunkMore
}

// ingestGroup is one table's records out of one chunk, in input order, already
// decoded.
//
// The rows are [Record] values rather than the bytes they arrived as, and that
// is what lets one chunk carrying both encodings for one table still be one
// transaction: by the time a group exists, nothing below it can tell which
// encoding any of its rows came in. It is the same claim
// TestTheTwoFormatsMeanExactlyOneThing makes about the staged bytes, arrived at
// structurally rather than asserted.
type ingestGroup struct {
	writer  *DynamicWriter
	rows    []Record
	numbers []int
}

// upTo returns the rows of the group whose record numbers are below limit, and
// their numbers. A negative limit means all of them. The records are in
// ascending number order, so this is a prefix.
func (g *ingestGroup) upTo(limit int) ([]Record, []int) {
	if limit < 0 {
		return g.rows, g.numbers
	}
	for i, n := range g.numbers {
		if n >= limit {
			return g.rows[:i], g.numbers[:i]
		}
	}

	return g.rows, g.numbers
}

// writeChunk parses one chunk's envelopes and hands each table's rows to its
// writer in one batch.
//
// Every refusal here ends the stream, naming the record. That is the design
// rather than a shortcut: a loop that skipped a record it could not understand
// would be deciding, on its own, that some of a producer's data does not
// matter — and it would decide it silently, in the middle of a stream nobody is
// watching.
//
// What "accepted" means is the one thing chunked writing changes, and the order
// of work here is what keeps [IngestStream]'s promise exact rather than
// approximately exact. A record this layer itself refuses is found while the
// chunk is being parsed, before anything in it has been written, so the chunk
// is truncated at that record and every record before it is written and
// durable. A row the *writer* refuses is found while the chunk is being
// written, and it refuses that table's whole group, because the group is one
// transaction; so the group is written again, truncated at the bad record, and
// every later group is truncated at it too.
func writeChunk(ctx context.Context, chunk []numberedRecord, writers map[string]*DynamicWriter, opts IngestOptions) error {
	groups, bad := groupChunk(chunk, writers)

	// limit is the record number nothing at or after may be written under, and
	// it only ever falls: it starts at the record this layer refused while
	// parsing, if there was one, and drops again at any row a writer refuses.
	limit := -1
	var failure error
	if bad != nil {
		limit, failure = bad.Record, *bad
	}

	for _, g := range groups {
		rows, numbers := g.upTo(limit)
		if len(rows) == 0 {
			continue
		}

		err := writeGroup(ctx, g, rows, numbers, chunk, opts)
		if err == nil {
			continue
		}
		if errors.Is(err, errIngestStopped) {
			return err
		}

		at, refused, ok := refusedRecord(err, numbers)
		if !ok {
			// Nothing in the group is at fault — a staging failure, a writer
			// that closed — so there is no earlier record to fall back to and
			// nothing to do but report it against the group being written.
			return chunkError(errdef.IngestKindWrite, numbers[0], chunk, g.writer.decl.Table(), err, "")
		}

		// One row of this group was refused, so none of the group was written.
		// Everything before it still must be, which is the promise this
		// ordering exists to keep.
		limit = at
		failure = chunkError(errdef.IngestKindRefused, at, chunk, g.writer.decl.Table(), refused, "")
		if rows, numbers = g.upTo(limit); len(rows) > 0 {
			// This cannot be refused for the same reason twice: the batch door
			// refuses at the *first* row it cannot accept, so every row before
			// that one has already been converted successfully. Anything that
			// does fail here is a different failure and is the one to report.
			if err := writeGroup(ctx, g, rows, numbers, chunk, opts); err != nil {
				if errors.Is(err, errIngestStopped) {
					return err
				}

				return chunkError(errdef.IngestKindWrite, numbers[0], chunk, g.writer.decl.Table(), err, "")
			}
		}
	}

	return failure
}

// chunkError builds a refusal about one record of a chunk, looking that
// record's line number up in the chunk it came from.
//
// The lookup is here rather than at every call site because a line number is a
// property of the framing and every layer above the framing only ever has the
// record number in hand. A record not found — which cannot happen, since every
// number these calls carry came out of the chunk being searched — reports no
// line rather than a wrong one.
func chunkError(
	kind errdef.IngestKind, record int, chunk []numberedRecord, table string, err error, detail string,
) errdef.IngestError {
	line := 0
	for _, rec := range chunk {
		if rec.number == record {
			line = rec.lineOrNone()

			break
		}
	}

	return errdef.NewIngestError(kind, record, line, table, err, detail)
}

// lineOrNone is the record's line number when it is on a line, and zero when it
// is a CBOR record, which is not.
func (r numberedRecord) lineOrNone() int {
	if r.json {
		return r.line
	}

	return 0
}

// groupChunk parses and decodes a chunk's records in order and collects each
// table's rows, stopping at the first record this layer will not read.
//
// The groups come back in the order their tables first appeared, which is
// ascending first-record order: it is the order that leaves the smallest window
// in which a group written before a later group's refusal could carry records
// past it.
//
// The row is decoded here rather than at the writer's door, and that is the one
// piece of work this layer does that the door would otherwise do. It buys two
// things. A row that will not decode is found *before* anything in the chunk
// has been written, so the chunk is cut at it instead of a group having to be
// written twice. And a table that received both encodings inside one chunk is
// still one group and therefore still one transaction, because a decoded row no
// longer remembers which encoding it arrived in. The decode itself is the
// writer's own — the same function [DynamicWriter.InsertJSON] and
// [DynamicWriter.InsertCBOR] run — so there is exactly one reading of each
// grammar in this library.
func groupChunk(chunk []numberedRecord, writers map[string]*DynamicWriter) ([]*ingestGroup, *errdef.IngestError) {
	groups := make([]*ingestGroup, 0, len(writers))
	byTable := make(map[string]*ingestGroup, len(writers))

	refuse := func(kind errdef.IngestKind, rec numberedRecord, table string, err error, detail string) (
		[]*ingestGroup, *errdef.IngestError,
	) {
		bad := errdef.NewIngestError(kind, rec.number, rec.lineOrNone(), table, err, detail)

		return groups, &bad
	}

	for _, rec := range chunk {
		table, rowBytes, refusal := parseEnvelope(rec)
		if refusal != nil {
			return refuse(refusal.kind, rec, table, refusal.cause, refusal.detail)
		}

		writer, ok := writers[table]
		if !ok {
			return refuse(errdef.IngestKindUnknownTable, rec, table, nil,
				fmt.Sprintf("table %q has no writer in this stream", table))
		}

		var row Record
		var err error
		if rec.json {
			row, err = writer.decodeLine(rowBytes)
		} else {
			row, err = writer.decodeCBORItem(rowBytes)
		}
		if err != nil {
			return refuse(errdef.IngestKindRefused, rec, table, err, "")
		}

		g := byTable[table]
		if g == nil {
			g = &ingestGroup{writer: writer}
			byTable[table] = g
			groups = append(groups, g)
		}
		g.rows = append(g.rows, row)
		g.numbers = append(g.numbers, rec.number)
	}

	return groups, nil
}

// refusedRecord reports which input record a batch refusal was about, when it
// was about one row rather than about the batch as a whole, along with that
// row's own error.
//
// The row's own error is returned separately, and the [BatchError] wrapping it
// is deliberately dropped, because the two carry the same fact in two
// coordinate systems: the wrapper counts rows from the start of the slice this
// layer built, and an operator counts records from the start of their own
// input. Reporting both would put "row 3" next to "record 41" in a message
// somebody has to act on, and only one of those two numbers means anything
// outside this process.
//
// **The index is relative to the slice the failing attempt was made with, and
// that is only the same coordinate system as `numbers` because a row-level
// refusal can only ever come from the first, full-size attempt.** The batch
// door binds, poison-checks and encodes every row before it charges the staging
// ceiling, so a slice refused for one of its rows is refused before any smaller
// retry of it exists; the retries [writeGroup] makes are for [ErrStagingFull]
// alone, which is not a row-level refusal and never reaches here. If that order
// ever changes, this mapping is the thing it breaks.
func refusedRecord(err error, numbers []int) (int, error, bool) {
	var refusal BatchError
	if !errors.As(err, &refusal) {
		return 0, nil, false
	}
	if refusal.Index < 0 || refusal.Index >= len(numbers) {
		return 0, nil, false
	}

	return numbers[refusal.Index], refusal.Err, true
}

// writeGroup writes one table's rows from one chunk, holding the reader rather
// than giving up when the staging store is full.
//
// A full staging store is the one error this loop retries instead of failing
// on, and the reason is what it means: the ceiling has been reached because the
// bucket is not keeping up, so the records in hand are ones the producer still
// owns and the right answer is to stop reading. Nothing is read from r while
// this is looping, beyond the bounded read-ahead already queued, which is what
// propagates the outage back up a pipe to a producer that already knows what to
// do about a slow consumer. Failing instead would turn a bucket outage into
// upstream data loss.
//
// The refusal is all-or-nothing, so a group larger than the whole ceiling could
// never be accepted however long it waited — a real possibility, since the
// chunk size follows the producer and the ceiling is the operator's number. So
// a refused group is halved and tried again before anything sleeps: what that
// converges on is the largest prefix that currently fits, down to a single
// record, and only a single record that does not fit means the store is
// genuinely full. Each attempt is still one transaction that either lands whole
// or does not land at all, so nothing here can leave a partially written group
// behind, and the records are always taken in input order.
func writeGroup(
	ctx context.Context, g *ingestGroup, rows []Record, numbers []int, chunk []numberedRecord, opts IngestOptions,
) error {
	notify := func(kind IngestNoticeKind, at int) {
		if opts.OnNotice == nil {
			return
		}
		notice := IngestNotice{
			Kind: kind, Namespace: g.writer.decl.Namespace(), Table: g.writer.decl.Table(), Record: at,
		}
		for _, rec := range chunk {
			if rec.number == at {
				notice.Line = rec.lineOrNone()

				break
			}
		}
		opts.OnNotice(notice)
	}

	held := false
	take := len(rows)

	for delay := opts.BackoffMin; len(rows) > 0; {
		take = min(take, len(rows))

		err := g.writer.InsertBatch(ctx, rows[:take])
		switch {
		case err == nil:
			if held {
				notify(IngestNoticeResumed, numbers[0])
				held, delay = false, opts.BackoffMin
			}
			rows, numbers = rows[take:], numbers[take:]
			// Back to the whole of what is left: the room that has just been
			// found may be room for all of it.
			take = len(rows)

			continue

		case errors.Is(err, ErrStagingFull):
			if take > 1 {
				// Not necessarily full — possibly only fuller than this many
				// records. Halving is free and answers the question.
				take /= 2

				continue
			}
			if !held {
				held = true
				notify(IngestNoticeHeld, numbers[0])
			}

		default:
			// A failure that is the caller's own context ending is the
			// cancellation path wearing a staging failure's clothes: the write
			// was refused because the run was told to stop, the records were
			// not accepted, and they are the producer's exactly as they would
			// be had the cancellation landed one moment earlier between two
			// chunks. Reporting it as a runtime failure would make a clean
			// shutdown fail whenever the signal happened to arrive inside a
			// chunk — a window that is a whole chunk wide.
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return errIngestStopped
			}

			return err
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			// A cancellation during backpressure ends the run like any other,
			// and these records are ones the producer still owns.
			return errIngestStopped
		}
		if delay *= 2; delay > opts.BackoffMax {
			delay = opts.BackoffMax
		}
	}

	return nil
}

// envelopeRefusal is why a record is not an envelope, carried out of
// [parseEnvelope] so that the caller can attach the position it knows and the
// parse can stay a pure function of the bytes. It is the shape
// [decimalRefusal] uses one layer down, for the same reason.
type envelopeRefusal struct {
	kind   errdef.IngestKind
	detail string
	cause  error
}

// jsonEnvelope is the JSON spelling of the envelope.
//
// Always an envelope, never a bare row: two accepted shapes would let a
// malformed envelope be read as a row of some table, silently, and a record
// that means nothing is better refused than half-understood. The row travels as
// raw bytes so that it is decoded once, by the writer's own decoder, under the
// same rules that decide what a row of that table may contain.
type jsonEnvelope struct {
	Table string          `json:"table"`
	Row   json.RawMessage `json:"row"`
}

// cborEnvelope is the same two fields in the same shape, in the other encoding.
// The two structs are separate because the tags are, and one struct with both
// sets of tags would make an envelope key's spelling in one encoding a silent
// consequence of an edit made for the other.
type cborEnvelope struct {
	Table string          `cbor:"table"`
	Row   cbor.RawMessage `cbor:"row"`
}

// parseEnvelope reads one record's envelope: the table it names and the raw
// bytes of its row, still undecoded.
//
// Both encodings are strict in the same ways, deliberately: an unknown key in
// the envelope is refused rather than ignored, and a missing table or row is
// refused rather than defaulted. The refusals are the same because the grammar
// is the same; only the bytes differ.
func parseEnvelope(rec numberedRecord) (string, []byte, *envelopeRefusal) {
	if !rec.json {
		var env cborEnvelope
		if err := cborMode.Unmarshal(rec.item, &env); err != nil {
			return "", nil, &envelopeRefusal{errdef.IngestKindGrammar, "not an envelope", err}
		}
		if env.Table == "" || len(env.Row) == 0 {
			return env.Table, nil, &envelopeRefusal{errdef.IngestKindEnvelope,
				`an envelope is a CBOR map {"table": "namespace.name", "row": {...}}`, nil}
		}

		return env.Table, env.Row, nil
	}

	var env jsonEnvelope
	dec := json.NewDecoder(bytes.NewReader(rec.item))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return "", nil, &envelopeRefusal{errdef.IngestKindGrammar, "not an envelope", err}
	}
	if dec.More() {
		return env.Table, nil, &envelopeRefusal{errdef.IngestKindGrammar,
			"the line carries more than one JSON value", nil}
	}
	if env.Table == "" || len(env.Row) == 0 {
		return env.Table, nil, &envelopeRefusal{errdef.IngestKindEnvelope,
			`an envelope is {"table":"namespace.name","row":{...}}`, nil}
	}

	return env.Table, env.Row, nil
}

// readRecords runs the framing on its own goroutine and delivers records over a
// channel.
//
// The goroutine is what makes a cancellation work on an idle reader: a read
// blocked waiting for a producer cannot be cancelled, so the select in
// [nextChunk] waits on the context and this channel together, and the call
// returns without waiting for a record that may never come.
//
// The channel's buffer is the read-ahead bound, and it is the only thing
// standing between r and the chunk being written: a full buffer blocks the
// framing, which stops draining r, which is the backpressure a full staging
// store depends on reaching the producer. It is also what lets a chunk grow at
// all, since a chunk is whatever the non-blocking drain in [nextChunk] finds
// already queued.
func readRecords(ctx context.Context, r io.Reader, opts IngestOptions) (<-chan numberedRecord, <-chan error) {
	records := make(chan numberedRecord, opts.ReadAhead)
	failed := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(failed)

		scanner := &recordScanner{r: r, limit: opts.MaxRecordBytes}
		for {
			rec, err := scanner.next()
			switch {
			case errors.Is(err, io.EOF):
				return
			case err != nil:
				failed <- err

				return
			}
			select {
			case records <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()

	return records, failed
}

// recordBufferSlack is how far past the size bound the framing lets its own
// buffer grow.
//
// It exists because the buffer holds a whole record *and* whatever else arrived
// in the same read, so a buffer capped exactly at the bound would refuse a legal
// record because of bytes that are not part of it. The slack is what makes the
// buffer a backstop rather than the bound, exactly as the two bytes of slack the
// line scanner used to be given were: **the bound is the length check on the
// finished record**, and the buffer only has to stop a record that can never
// finish from being held in memory for ever.
const recordBufferSlack = 64 << 10

// recordScanner is the whole of icelake's input grammar: one buffer over the
// caller's reader, whitespace skipped between records, and each record's
// encoding decided by its own first byte.
//
// One buffer serving both encodings is not an economy, it is the reason the
// design works at all. A line scanner and a CBOR stream decoder cannot share a
// reader — each reads ahead into a buffer of its own, and whichever ran last has
// eaten bytes the other one needed — so a stream that may change encoding at any
// record has to be framed by something that owns the bytes. Owning them is also
// what makes one size bound mean the same thing for both: the check is on the
// finished record either way.
type recordScanner struct {
	r     io.Reader
	buf   []byte
	start int
	end   int
	// limit is MaxRecordBytes: the longest finished record accepted.
	limit int
	// eof records that r has ended, so a record whose bytes are incomplete can
	// be told from one whose remaining bytes simply have not arrived yet.
	eof bool
	// lines counts newline bytes consumed *as separators, or as a JSON record's
	// terminator* — never bytes that happened to be inside a record. That is
	// what makes the line number reported for a JSON record the line it is on in
	// the operator's file even when CBOR records, which are not on lines and
	// consume no newlines, are interleaved with it.
	lines int
	// records counts records handed out. It is the number every refusal names,
	// and the one coordinate that means something in every stream.
	records int
}

// isRecordSpace reports whether a byte is whitespace between records. It is
// deliberately the four bytes JSON itself calls whitespace and nothing else: a
// byte-order mark is not among them, and a stream that opens with one is
// refused rather than quietly accommodated.
func isRecordSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\r' || b == '\n' }

// cborMapHead reports whether a byte starts a definite-length CBOR map, which
// is the whole of what a CBOR envelope may begin with. Major type 5 with
// additional information 0 to 27 is a map whose length is in its head; 28 to 30
// are not well-formed CBOR at all, and 31 is the indefinite-length form this
// library refuses.
func cborMapHead(b byte) bool { return b>>5 == 5 && b&0x1f <= 27 }

// next returns the next record, or io.EOF at a clean end of input.
func (s *recordScanner) next() (numberedRecord, error) {
	if err := s.skipSpace(); err != nil {
		return numberedRecord{}, err
	}

	head := s.buffered()[0]
	s.records++
	switch {
	case head == '{':
		return s.jsonRecord()

	case cborMapHead(head):
		return s.cborRecord()

	case head == 0xbf:
		return numberedRecord{}, s.refuse(0, nil,
			"the record is an indefinite-length CBOR map, and an envelope is a definite-length one: "+
				"deterministic CBOR has no indefinite-length items, and one map with two encodings is one thing "+
				"two producers can disagree about while both believing they are right")

	case head == 0xef:
		return numberedRecord{}, s.refuse(s.nextLine(), nil,
			"the record starts with a byte-order mark, which is refused rather than skipped: a stream carrying "+
				"one was written by a tool that believes it is producing text for a person")

	default:
		return numberedRecord{}, s.refuse(s.nextLine(), nil, fmt.Sprintf(
			"the record starts with the byte 0x%02x, and a record starts with '{' for a JSON envelope "+
				"or with 0xa0 to 0xbb for a CBOR one", head))
	}
}

// refuse builds a grammar refusal about the record now being read.
//
// The two unrecognised-byte cases above pass a line and the CBOR ones pass
// zero, which is the same rule the rest of this file follows and is worth
// stating once: **a line number is reported unless the record is known to be a
// CBOR one.** A CBOR record is not on a line and naming one would be inventing
// a coordinate; a byte that is neither start is not known to be anything, and
// somebody meeting that refusal is looking at a file, where the line is the only
// handle they have.
func (s *recordScanner) refuse(line int, cause error, detail string) error {
	return errdef.NewIngestError(errdef.IngestKindGrammar, s.records, line, "", cause, detail)
}

// buffered is the bytes read and not yet consumed.
func (s *recordScanner) buffered() []byte { return s.buf[s.start:s.end] }

// nextLine is the line a record starting here is on: one more than the newlines
// consumed before it.
func (s *recordScanner) nextLine() int { return s.lines + 1 }

// skipSpace consumes whitespace between records, counting the newlines it
// passes, and returns io.EOF when nothing but whitespace is left.
func (s *recordScanner) skipSpace() error {
	for {
		b := s.buffered()
		i := 0
		for i < len(b) && isRecordSpace(b[i]) {
			if b[i] == '\n' {
				s.lines++
			}
			i++
		}
		s.start += i
		if s.start < s.end {
			return nil
		}
		if s.eof {
			return io.EOF
		}
		if err := s.fill(); err != nil {
			return err
		}
	}
}

// jsonRecord takes one line, not counting its terminator.
//
// The terminator is not part of the record, which is what makes the size bound
// mean the length of the line rather than the length of the line plus however
// the producer happens to end its lines: a record of exactly the bound is
// accepted with LF, with CRLF, and with no terminator at all, and one byte more
// is refused in all three cases.
func (s *recordScanner) jsonRecord() (numberedRecord, error) {
	line := s.nextLine()
	for {
		b := s.buffered()
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			item := b[:i]
			if len(item) > 0 && item[len(item)-1] == '\r' {
				item = item[:len(item)-1]
			}
			if len(item) > s.limit {
				return numberedRecord{}, s.tooLarge(len(item), line)
			}
			s.start += i + 1
			s.lines++

			return numberedRecord{number: s.records, line: line, json: true, item: bytes.Clone(item)}, nil
		}
		if s.eof {
			// The last line of a stream that does not end with a newline.
			if len(b) > s.limit {
				return numberedRecord{}, s.tooLarge(len(b), line)
			}
			s.start = s.end

			return numberedRecord{number: s.records, line: line, json: true, item: bytes.Clone(b)}, nil
		}
		if err := s.fill(); err != nil {
			return numberedRecord{}, err
		}
	}
}

// cborRecord takes one CBOR data item, which says its own length.
//
// The item's extent is found by decoding it and asking what was left over,
// rather than by walking the encoding here: a second implementation of CBOR's
// framing in this file would be a second thing that could disagree with the
// decoder about where a record ends, and the whole point of a self-delimiting
// format is that exactly one answer exists.
func (s *recordScanner) cborRecord() (numberedRecord, error) {
	for {
		b := s.buffered()

		var item cbor.RawMessage
		rest, err := cborMode.UnmarshalFirst(b, &item)
		switch {
		case err == nil:
			size := len(b) - len(rest)
			if size > s.limit {
				return numberedRecord{}, s.tooLarge(size, 0)
			}
			s.start += size

			return numberedRecord{number: s.records, item: bytes.Clone(b[:size])}, nil

		case (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) && !s.eof:
			// The item is not all here yet.
			if err := s.fill(); err != nil {
				return numberedRecord{}, err
			}

		default:
			return numberedRecord{}, s.refuse(0, err, "not a CBOR data item")
		}
	}
}

// tooLarge is the refusal a finished record over the bound earns. The bound is
// in the message rather than only in the kind because the number is the whole
// of what somebody meeting this has to change.
func (s *recordScanner) tooLarge(size, line int) error {
	return errdef.NewIngestError(errdef.IngestKindTooLarge, s.records, line, "", nil, fmt.Sprintf(
		"the record is %d bytes and the configured maximum is %d", size, s.limit))
}

// fill reads more input, growing the buffer when the record being read needs
// more room than it has.
//
// The buffer grows to the bound plus [recordBufferSlack] and no further. A
// record that has not finished by then can never finish inside the bound, so it
// is refused there rather than being read to its end first: the point of a size
// bound on a stream is that the whole of an over-long record is never held.
func (s *recordScanner) fill() error {
	ceiling := s.limit + recordBufferSlack
	switch {
	case s.buf == nil:
		s.buf = make([]byte, min(64<<10, ceiling))

	case s.end < len(s.buf):
		// There is already room to read into.

	case s.start > 0:
		// Room can be had by moving what is left to the front.
		s.end = copy(s.buf, s.buf[s.start:s.end])
		s.start = 0

	case len(s.buf) >= ceiling:
		return s.tooLarge(len(s.buf), s.nextLine())

	default:
		grown := make([]byte, min(2*len(s.buf), ceiling))
		s.end = copy(grown, s.buf[s.start:s.end])
		s.start = 0
		s.buf = grown
	}

	n, err := s.r.Read(s.buf[s.end:])
	s.end += n
	switch {
	case errors.Is(err, io.EOF):
		s.eof = true

		return nil
	case err != nil:
		return errdef.NewIngestError(errdef.IngestKindRead, s.records, 0, "", err, "reading the stream")
	}

	return nil
}
