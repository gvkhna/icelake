package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gvkhna/icelake"
)

// The backoff a full staging store is held off with.
//
// This is the one error the daemon retries instead of dying on, and the reason
// is what it means: the staging ceiling has been reached because the bucket is
// not keeping up, so the record in hand is one the caller still owns and the
// right answer is to stop reading. Holding stdin propagates that up the pipe as
// backpressure, which is what a producer on the other end of a pipe already
// knows how to handle. Exiting instead would turn a bucket outage into upstream
// data loss.
const (
	fullBackoffMin = 100 * time.Millisecond
	fullBackoffMax = 5 * time.Second
)

// envelope is the one line grammar this command accepts.
//
// Always an envelope, never a bare row: two accepted shapes would let a
// malformed envelope be read as a row of some table, silently, and a line that
// means nothing is better refused than half-understood. The row travels as raw
// JSON so that it is parsed once, by the library, through the same front door
// that decides what a row of that table may contain.
type envelope struct {
	Table string          `json:"table"`
	Row   json.RawMessage `json:"row"`
}

// runDaemon is "icelake run": configuration, writers, and then the pipe.
func runDaemon(ctx context.Context, stdin io.Reader, stderr io.Writer) error {
	cfg, err := readSettings(forRun)
	if err != nil {
		return err
	}

	// The whole of this command's observability, and the one line that answers
	// "what is this daemon actually configured with" without anybody guessing
	// from a unit file. The library reports nothing itself, by design; a program
	// that embeds it is entitled to do what any embedding service does, and this
	// one's convention is stderr.
	fmt.Fprintf(stderr, "icelake: %s\n", cfg.describe())

	document, err := os.ReadFile(cfg.schemaFile)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", errUsage, envSchemaFile.name, err)
	}
	tables, err := icelake.ParseSchemaDocument(document)
	if err != nil {
		// A schema document that will not parse is a configuration problem, not
		// a runtime one: no restart fixes it, and a supervisor needs to be told
		// that rather than left to retry a file that will never change by itself.
		return fmt.Errorf("%w: %s: %w", errUsage, envSchemaFile.name, err)
	}
	if len(tables) == 0 {
		return fmt.Errorf("%w: %s declares no tables", errUsage, envSchemaFile.name)
	}

	store, err := icelake.Open(ctx, cfg.config(func(e icelake.FlushError) {
		fmt.Fprintf(stderr, "icelake: flush failed: table %s.%s batch %s (%d records, %d attempts): %v\n",
			e.Namespace, e.Table, e.BatchKey, e.Records, e.Attempts, e.Err)
	}))
	if err != nil {
		// A configuration the library refuses is still a configuration refusal,
		// whichever of the two checked it, so it exits the same way.
		var invalid icelake.ConfigError
		if errors.As(err, &invalid) {
			return fmt.Errorf("%w: %w", errUsage, err)
		}

		return err
	}

	// Every writer is opened before a byte of stdin is read, which is what makes
	// replay happen first: a run that follows a crash commits what the previous
	// one accepted before it accepts anything new, and a schema that cannot be
	// reconciled stops the daemon while the pipe is still full rather than after
	// it has taken a thousand records it cannot write.
	writers := make(map[string]*icelake.DynamicWriter, len(tables))
	for _, tc := range tables {
		w, err := icelake.OpenDynamicWriter(ctx, store, tc)
		if err != nil {
			closeStore(context.WithoutCancel(ctx), store, cfg.shutdownTimeout)

			return err
		}
		writers[tc.Namespace+"."+tc.Table] = w
	}

	if err := pump(ctx, cfg, stdin, stderr, writers); err != nil {
		// A line this command will not read ends the run here, without draining.
		// That is the design rather than an oversight: what was accepted before
		// it is already durable in the staging database and is replayed and
		// committed by the next start, before that run reads a byte of new
		// input. Draining first would be doing work on the way out of a failure
		// nobody has diagnosed yet, and it would make "restart and it picks up
		// where it stopped" a slightly different sentence depending on how the
		// run ended. The process is exiting, so the databases go with it.
		return err
	}

	// The drain runs on a context of its own so that a signal, which is what
	// cancelled the one above, does not also cancel the flush it is supposed to
	// start. It is bounded: a shutdown has to end, and records that do not make
	// it are safe in staging for the next start rather than lost.
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.shutdownTimeout)
	defer cancel()

	err = drain(drainCtx, writers)
	if closeErr := store.Close(drainCtx); closeErr != nil && err == nil && !errors.Is(closeErr, icelake.ErrClosed) {
		err = closeErr
	}

	return err
}

// The bounds on how much of the pipe this command holds at once.
//
// Lines are written in chunks rather than one at a time, because one staging
// transaction is one fsync however many records are in it: the library's own
// measurements (ARCHITECTURE.md, M15) put one record per transaction at a few
// hundred a second and a thousand records per transaction at six figures, on the
// same disk. The daemon blocks for the first line of a chunk and then takes
// whatever else has already arrived without waiting for it, so a trickle is
// written as it arrives and a flood is written in groups — the chunk size
// follows the producer rather than a timer.
//
// chunkMaxLines and chunkMaxBytes bound one chunk from both ends, because either
// one alone is the wrong bound for half the traffic this command sees. A line
// count alone would let 4096 lines of a stream whose lines are megabytes become a
// gigabyte held in memory; a byte count alone would let a stream of tiny lines
// build an unboundedly long slice before it reached it. The line figure is set
// where the measured curve has long since flattened — the gain from a thousand
// records per transaction to ten thousand is a fifth of the gain from a hundred
// to a thousand — so a larger chunk would buy latency and memory for nothing.
// The byte figure is the same reasoning from the other side: 8 MiB is far more
// than a chunk of ordinary records needs and small enough to be an unremarkable
// allocation.
//
// readAheadLines is the buffer on the channel the scanner delivers into, and it
// is therefore exactly the read-ahead: at most this many lines sit between the
// pipe and the chunk being written, and everything past it is still in the pipe,
// which is where the backpressure a full staging store applies has to reach. It
// is deliberately a line count and not a byte count, and usage.md says so, since
// what it costs in memory is this figure times the length of the lines actually
// arriving.
const (
	chunkMaxLines  = 4096
	chunkMaxBytes  = 8 << 20
	readAheadLines = 1024
)

// errStopped means the context ended while a chunk was being written. It is
// internal to this file: [pump] turns it back into the clean nil ending, exactly
// as it does for a signal that arrives between chunks.
var errStopped = errors.New("icelake: the run was stopped")

// numbered is one line of input with the line number it arrived on. The number
// travels with the line because it is what every refusal in this command names,
// and a chunk regroups its lines by table before writing them.
type numbered struct {
	number int
	line   []byte
}

// group is one table's lines out of one chunk, in input order.
type group struct {
	writer  *icelake.DynamicWriter
	rows    [][]byte
	numbers []int
}

// upTo returns the rows of the group whose line numbers are below limit, and
// their numbers. A negative limit means all of them. The lines are in ascending
// number order, so this is a prefix.
func (g *group) upTo(limit int) ([][]byte, []int) {
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

// pump reads lines and writes records until the input ends or the context does.
//
// It returns nil for both of the ordinary endings — end of input, and a signal —
// because neither is a failure: what a supervisor needs to know is whether the
// daemon stopped because it was told to or because something is wrong.
func pump(ctx context.Context, cfg settings, stdin io.Reader, stderr io.Writer, writers map[string]*icelake.DynamicWriter) error {
	lines, scanErr := readLines(ctx, stdin, cfg.maxLineBytes)

	number := 0
	chunk := make([]numbered, 0, chunkMaxLines)
	for {
		var outcome chunkOutcome
		chunk, number, outcome = nextChunk(ctx, lines, chunk[:0], number)

		if err := writeChunk(ctx, chunk, writers, stderr); err != nil {
			if errors.Is(err, errStopped) {
				return nil
			}

			return err
		}

		switch outcome {
		case chunkStopped:
			// A signal, and the only ending that can arrive with nothing to
			// write. What was accepted is already durable.
			return nil

		case chunkEnded:
			// The reader is finished: either the pipe closed or it stopped
			// because the context did. A scanner error is the one thing that
			// distinguishes an end of input from a line this command refuses to
			// read at all.
			if err := <-scanErr; err != nil {
				return err
			}

			return nil

		case chunkMore:
			// More input is coming, and the chunk just written may well have
			// been empty: a blank line with nothing queued behind it produces
			// one, and so does a run of them. That case must go back to the
			// blocking read rather than end the run, which is why this outcome
			// is a third value rather than the absence of the other two.
			//
			// **Corrected at M15's review round, because the first version of
			// this loop got exactly that wrong.** It treated "not ended and
			// nothing to write" as the signal case and returned nil, so a blank
			// line arriving on a live stdin ended the daemon with exit zero and
			// silently dropped the rest of the pipe. Nothing in the suite
			// noticed, because a test that writes its whole input at once has
			// the next line queued before the blank one is drained, and the
			// chunk therefore never comes back empty. It takes a producer that
			// pauses — which is every real producer — to reach it.
		}
	}
}

// chunkOutcome says why [nextChunk] stopped collecting, which is three distinct
// answers and not two: the input ended, the context ended, or neither and there
// is more to read. The third is what a chunk of nothing but blank lines
// produces, and conflating it with either of the others ends a live run.
type chunkOutcome int

const (
	// chunkMore means the input has not ended and neither has the context.
	chunkMore chunkOutcome = iota
	// chunkEnded means the line reader closed its channel.
	chunkEnded
	// chunkStopped means the context ended while waiting for a line.
	chunkStopped
)

// nextChunk blocks for one line and then takes whatever else has already
// arrived, up to the chunk bounds, without waiting for any of it.
//
// The blocking first receive is what makes a trickle of input behave exactly as
// it did when every line was written on its own, and the non-blocking drain
// after it is what makes a flood be written in groups: the chunk grows to
// whatever the producer has managed to queue, and no further. Nothing here waits
// on a timer, so a slow producer is never delayed by a batching heuristic.
//
// Blank lines are skipped and still counted, so the numbers this command reports
// are the numbers in the operator's file.
//
// It returns the chunk, the line number reached, and why it stopped. A chunk may
// come back empty for any of the three reasons — a signal, an end of input, or a
// blank line with nothing queued behind it — which is exactly why the third
// return value distinguishes them rather than being a bare "ended" flag: only
// the first two end the run, and the last must go back to the blocking read.
func nextChunk(ctx context.Context, lines <-chan []byte, chunk []numbered, number int) ([]numbered, int, chunkOutcome) {
	take := func(line []byte) {
		number++
		if len(bytes.TrimSpace(line)) == 0 {
			return
		}
		chunk = append(chunk, numbered{number: number, line: line})
	}

	select {
	case line, ok := <-lines:
		if !ok {
			return chunk, number, chunkEnded
		}
		take(line)
	case <-ctx.Done():
		// A signal. The line currently in the reader's hands, if any, is simply
		// not taken: it was never accepted, so it was never icelake's, and what
		// was accepted is already durable.
		return chunk, number, chunkStopped
	}

	size := 0
	for _, item := range chunk {
		size += len(item.line)
	}
	// The byte bound is checked before a line is read rather than after it is
	// added, so the last line of a chunk may carry it past the figure: the true
	// bound is chunkMaxBytes plus one line, and usage.md states it that way
	// rather than rounding it down. Checking afterwards would mean either
	// putting a line back, which a channel cannot do, or holding it for a chunk
	// that may never be asked for.
	for len(chunk) < chunkMaxLines && size < chunkMaxBytes {
		select {
		case line, ok := <-lines:
			if !ok {
				return chunk, number, chunkEnded
			}
			size += len(line)
			take(line)
		default:
			return chunk, number, chunkMore
		}
	}

	return chunk, number, chunkMore
}

// writeChunk parses one chunk's envelopes and hands each table's rows to its
// writer in one batch.
//
// Every refusal here ends the run, loudly, naming the line. That is the design
// rather than a shortcut: a daemon that skipped a line it could not understand
// would be deciding, on its own, that some of an operator's data does not matter
// — and it would decide it silently, in the middle of a stream nobody is
// watching. What was accepted before the bad line is durable and replays on the
// next start; what was still in the pipe was never icelake's.
//
// What "accepted" means is the one thing chunked writing changes, and the order
// of work here is what keeps the sentence above true rather than approximately
// true. A line this command itself refuses — not JSON, not an envelope, a table
// the document does not declare — is found while the chunk is being parsed,
// before anything in it has been written, so the chunk is simply truncated at
// that line and every line before it is written and durable. A row the *library*
// refuses is found while the chunk is being written, and it refuses that table's
// whole group, because the group is one transaction; so the group is written
// again, truncated at the bad line, and every later group is truncated at it too.
// The one thing that cannot be undone is a group that was already written and
// carried lines after the bad one, which is reachable only when a single chunk
// interleaves two tables. usage.md states that plainly rather than leaving it to
// be discovered.
func writeChunk(ctx context.Context, chunk []numbered, writers map[string]*icelake.DynamicWriter, stderr io.Writer) error {
	groups, bad := groupChunk(chunk, writers)

	// limit is the line number nothing at or after may be written under, and it
	// only ever falls: it starts at the line this command refused while parsing,
	// if there was one, and drops again at any line the library refuses.
	limit := -1
	if bad != nil {
		limit = bad.number
	}

	failure := bad.err()
	for _, g := range groups {
		rows, numbers := g.upTo(limit)
		if len(rows) == 0 {
			continue
		}

		err := writeGroup(ctx, g, rows, numbers, stderr)
		if err == nil {
			continue
		}
		if errors.Is(err, errStopped) {
			return err
		}

		at, refused, ok := refusedLine(err, numbers)
		if !ok {
			// Nothing in the group is at fault — a staging failure, a store that
			// closed — so there is no earlier line to fall back to and nothing
			// this command can do but report it against the group it was writing.
			return fmt.Errorf("line %d: %w", numbers[0], err)
		}

		// One row of this group was refused, so none of the group was written.
		// Everything before it still must be, which is the promise this ordering
		// exists to keep.
		limit, failure = at, fmt.Errorf("line %d: %w", at, refused)
		if rows, numbers = g.upTo(limit); len(rows) > 0 {
			// This cannot be refused for the same reason twice: the batch door
			// refuses at the *first* row it cannot accept, so every row before
			// that one has already been converted successfully. Anything that
			// does fail here is a different failure and is the one to report.
			if err := writeGroup(ctx, g, rows, numbers, stderr); err != nil {
				if errors.Is(err, errStopped) {
					return err
				}

				return fmt.Errorf("line %d: %w", numbers[0], err)
			}
		}
	}

	return failure
}

// badLine is a line this command refuses while parsing a chunk, and the number
// it was on. It is a type rather than a bare error so that the line number stays
// available to the truncation above after the error has been built.
type badLine struct {
	number int
	reason error
}

// err returns the refusal, or nil when there was none, so a caller can return it
// without checking for a nil pointer first.
func (b *badLine) err() error {
	if b == nil {
		return nil
	}

	return b.reason
}

// groupChunk parses a chunk's envelopes in order and collects each table's rows,
// stopping at the first line this command will not read.
//
// The groups come back in the order their tables first appeared, which is
// ascending first-line order: it is the order that leaves the smallest window in
// which a group written before a later group's refusal could carry lines past
// it.
func groupChunk(chunk []numbered, writers map[string]*icelake.DynamicWriter) ([]*group, *badLine) {
	groups := make([]*group, 0, len(writers))
	byTable := make(map[string]*group, len(writers))

	for _, item := range chunk {
		var env envelope

		dec := json.NewDecoder(bytes.NewReader(item.line))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&env); err != nil {
			return groups, &badLine{number: item.number, reason: fmt.Errorf("line %d: not an envelope: %w", item.number, err)}
		}
		if env.Table == "" || len(env.Row) == 0 {
			return groups, &badLine{number: item.number, reason: fmt.Errorf(
				`line %d: an envelope is {"table":"namespace.name","row":{...}}`, item.number)}
		}

		writer, ok := writers[env.Table]
		if !ok {
			return groups, &badLine{number: item.number, reason: fmt.Errorf(
				"line %d: table %q is not declared in %s", item.number, env.Table, envSchemaFile.name)}
		}

		g := byTable[env.Table]
		if g == nil {
			g = &group{writer: writer}
			byTable[env.Table] = g
			groups = append(groups, g)
		}
		g.rows = append(g.rows, env.Row)
		g.numbers = append(g.numbers, item.number)
	}

	return groups, nil
}

// refusedLine reports which input line a batch refusal was about, when it was
// about one row rather than about the batch as a whole, along with that row's
// own error.
//
// The row's own error is returned separately, and the [icelake.BatchError]
// wrapping it is deliberately dropped, because the two carry the same fact in
// two coordinate systems: the wrapper counts rows from the start of whatever
// slice was handed to the library, and this command counts lines from the start
// of the operator's input. Printing both would put "row 3" next to "line 41" in
// a message an operator has to act on, and only one of those two numbers means
// anything outside this process.
//
// **The index is relative to the slice the failing attempt was made with, and
// that is only the same coordinate system as `numbers` because a row-level
// refusal can only ever come from the first, full-size attempt.** The batch door
// binds, poison-checks and encodes every row before it charges the staging
// ceiling, so a slice that is refused for one of its rows is refused before any
// smaller retry of it exists; the retries [writeGroup] makes are for
// ErrStagingFull alone, which is not a row-level refusal and never reaches here.
// If that order in the library ever changes, this mapping is the thing it breaks.
func refusedLine(err error, numbers []int) (int, error, bool) {
	var refusal icelake.BatchError
	if !errors.As(err, &refusal) {
		return 0, nil, false
	}
	if refusal.Index < 0 || refusal.Index >= len(numbers) {
		return 0, nil, false
	}

	return numbers[refusal.Index], refusal.Err, true
}

// writeGroup writes one table's rows from one chunk, holding stdin rather than
// giving up when the staging store is full.
//
// A full staging store is the one error this command retries instead of dying
// on, and the reason is what it means: the ceiling has been reached because the
// bucket is not keeping up, so the records in hand are ones the producer still
// owns and the right answer is to stop reading. Nothing is read from the pipe
// while this is looping, beyond the bounded read-ahead already queued, which is
// what propagates the outage back up it.
//
// The refusal is all-or-nothing, so a group larger than the whole ceiling could
// never be accepted however long it waited — a real possibility, since the chunk
// size follows the producer and the ceiling is the operator's number. So a
// refused group is halved and tried again before anything sleeps: what that
// converges on is the largest prefix that currently fits, down to a single
// record, and only a single record that does not fit means the store is
// genuinely full. Each attempt is still one transaction that either lands whole
// or does not land at all, so nothing here can leave a partially written group
// behind, and the records are always taken in input order.
func writeGroup(ctx context.Context, g *group, rows [][]byte, numbers []int, stderr io.Writer) error {
	held := false
	take := len(rows)

	for delay := fullBackoffMin; len(rows) > 0; {
		take = min(take, len(rows))

		err := g.writer.InsertJSONBatch(ctx, rows[:take])
		switch {
		case err == nil:
			if held {
				fmt.Fprintf(stderr, "icelake: staging has room again; reading resumed at line %d\n", numbers[0])
				held, delay = false, fullBackoffMin
			}
			rows, numbers = rows[take:], numbers[take:]
			// Back to the whole of what is left: the room that has just been
			// found may be room for all of it.
			take = len(rows)

			continue

		case errors.Is(err, icelake.ErrStagingFull):
			if take > 1 {
				// Not necessarily full — possibly only fuller than this many
				// records. Halving is free and answers the question.
				take /= 2

				continue
			}
			if !held {
				held = true
				fmt.Fprintf(stderr, "icelake: staging is full at line %d; holding stdin until it drains\n", numbers[0])
			}

		default:
			// A failure that is this command's own context ending is the signal
			// path wearing a staging failure's clothes: the write was refused
			// because the daemon was told to stop, the records were not
			// accepted, and they are the producer's exactly as they would be had
			// the signal landed one moment earlier between two chunks. Reporting
			// it as a runtime failure would make a clean shutdown exit one
			// whenever the signal happened to arrive inside a chunk — a window
			// that used to be one record wide and is now a whole chunk.
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return errStopped
			}

			return err
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			// A signal during backpressure ends the run like any other, and
			// these records are ones the producer still owns.
			return errStopped
		}
		if delay *= 2; delay > fullBackoffMax {
			delay = fullBackoffMax
		}
	}

	return nil
}

// readLines runs the scanner on its own goroutine and delivers lines over a
// channel.
//
// The goroutine is what makes a signal work on an idle pipe: a read that is
// blocked waiting for a producer cannot be cancelled, so the select in [pump]
// waits on the context and this channel together, and the process ends without
// waiting for a line that may never come.
//
// The line length is bounded by configuration rather than by the scanner's
// default, and a longer line is a failure rather than a truncation: half a JSON
// object is not a record, and silently writing one would be exactly the quiet
// mangling this command's whole error discipline exists to avoid.
func readLines(ctx context.Context, stdin io.Reader, maxLineBytes int) (<-chan []byte, <-chan error) {
	// The buffer is the read-ahead bound, and it is the only thing standing
	// between the pipe and the chunk being written: a full buffer blocks the
	// scanner, which stops draining the pipe, which is the backpressure a full
	// staging store depends on reaching the producer. It is also what lets a
	// chunk grow at all, since a chunk is whatever the non-blocking drain in
	// nextChunk finds already queued.
	lines := make(chan []byte, readAheadLines)
	failed := make(chan error, 1)

	go func() {
		defer close(lines)
		defer close(failed)

		sc := bufio.NewScanner(stdin)
		// Two details, and both are the difference between a bound that is
		// enforced and a number in a help message.
		//
		// The starting buffer must not exceed the bound, or the bound does not
		// apply below it: a scanner only refuses a token when it has to *grow*
		// past its maximum, so a 64 KiB starting buffer would happily read a
		// 60 KiB line for a daemon configured to accept 1 KiB.
		//
		// And the scanner is given two bytes more than the bound, because its
		// maximum covers the token *and* the terminator that ends it, and a
		// terminator is one byte for LF or two for CRLF. With less slack a
		// line of exactly the configured length would be refused or accepted
		// depending on where the file stops or on how the producer ends its
		// lines, which is a bound that depends on neither thing the variable
		// names. The variable means the longest line, and the line terminator,
		// LF or CRLF, is not part of the line.
		//
		// The slack means the buffer alone can no longer enforce the bound —
		// an LF-terminated line one byte over now fits in it — so the length
		// check on the token below is the bound, and the buffer's ErrTooLong
		// is only the backstop for lines too long to even terminate.
		limit := maxLineBytes + 2
		sc.Buffer(make([]byte, 0, min(64*1024, limit)), limit)

		for sc.Scan() {
			if len(sc.Bytes()) > maxLineBytes {
				failed <- fmt.Errorf("a line is longer than %s (%s)", envMaxLineBytes.name, envMaxLineBytes.value())
				return
			}
			// The scanner reuses its buffer, so the bytes have to be copied
			// before they cross the channel.
			line := append([]byte(nil), sc.Bytes()...)
			select {
			case lines <- line:
			case <-ctx.Done():
				return
			}
		}

		switch err := sc.Err(); {
		case err == nil:
		case errors.Is(err, bufio.ErrTooLong):
			failed <- fmt.Errorf("a line is longer than %s (%s)", envMaxLineBytes.name, envMaxLineBytes.value())
		default:
			failed <- fmt.Errorf("reading stdin: %w", err)
		}
	}()

	return lines, failed
}

// drain closes every writer, which flushes whatever each one is holding.
//
// Every writer is closed even if one fails, because a table that could not be
// drained is no reason to abandon the others, and the first failure is the one
// reported — the same shape the library's own Store.Close uses.
func drain(ctx context.Context, writers map[string]*icelake.DynamicWriter) error {
	var first error
	for name, w := range writers {
		if err := w.Close(ctx); err != nil && !errors.Is(err, icelake.ErrClosed) && first == nil {
			first = fmt.Errorf("draining %s: %w", name, err)
		}
	}

	return first
}

// closeStore shuts a store down on the early paths, where a writer failed to
// open and there is nothing to drain.
func closeStore(ctx context.Context, store *icelake.Store, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_ = store.Close(ctx)
}
