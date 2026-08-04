package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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

// pump reads lines and writes records until the input ends or the context does.
//
// It returns nil for both of the ordinary endings — end of input, and a signal —
// because neither is a failure: what a supervisor needs to know is whether the
// daemon stopped because it was told to or because something is wrong.
func pump(ctx context.Context, cfg settings, stdin io.Reader, stderr io.Writer, writers map[string]*icelake.DynamicWriter) error {
	lines, scanErr := readLines(ctx, stdin, cfg.maxLineBytes)

	number := 0
	for {
		var line []byte
		var ok bool

		select {
		case line, ok = <-lines:
			if !ok {
				// The reader is finished: either the pipe closed or it stopped
				// because the context did. A scanner error is the one thing that
				// distinguishes an end of input from a line this command refuses
				// to read at all.
				if err := <-scanErr; err != nil {
					return err
				}

				return nil
			}
		case <-ctx.Done():
			// A signal. The line currently in the reader's hands, if any, is
			// simply not taken: it was never accepted, so it was never icelake's,
			// and what was accepted is already durable.
			return nil
		}

		number++
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		if err := writeLine(ctx, line, number, writers, stderr); err != nil {
			return err
		}
	}
}

// writeLine parses one envelope and hands its row to the right writer.
//
// Every refusal here ends the run, loudly, naming the line. That is the design
// rather than a shortcut: a daemon that skipped a line it could not understand
// would be deciding, on its own, that some of an operator's data does not matter
// — and it would decide it silently, in the middle of a stream nobody is
// watching. What was accepted before the bad line is durable and replays on the
// next start; what was still in the pipe was never icelake's.
func writeLine(ctx context.Context, line []byte, number int, writers map[string]*icelake.DynamicWriter, stderr io.Writer) error {
	var env envelope

	dec := json.NewDecoder(strings.NewReader(string(line)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return fmt.Errorf("line %d: not an envelope: %w", number, err)
	}
	if env.Table == "" || len(env.Row) == 0 {
		return fmt.Errorf(`line %d: an envelope is {"table":"namespace.name","row":{...}}`, number)
	}

	writer, ok := writers[env.Table]
	if !ok {
		return fmt.Errorf("line %d: table %q is not declared in %s", number, env.Table, envSchemaFile.name)
	}

	// The one error that is not fatal. The record is not accepted, so it is
	// retried rather than dropped, and stdin is not read again until it lands:
	// that is the backpressure reaching the producer.
	held := false
	for delay := fullBackoffMin; ; {
		err := writer.InsertJSON(ctx, env.Row)
		switch {
		case err == nil:
			if held {
				fmt.Fprintf(stderr, "icelake: staging has room again; reading resumed at line %d\n", number)
			}

			return nil

		case errors.Is(err, icelake.ErrStagingFull):
			if !held {
				held = true
				fmt.Fprintf(stderr, "icelake: staging is full at line %d; holding stdin until it drains\n", number)
			}

		default:
			return fmt.Errorf("line %d: %w", number, err)
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			// A signal during backpressure ends the run like any other, and this
			// record is one the producer still owns.
			return nil
		}
		if delay *= 2; delay > fullBackoffMax {
			delay = fullBackoffMax
		}
	}
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
	lines := make(chan []byte)
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
