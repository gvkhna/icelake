package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gvkhna/icelake"
)

// runDaemon is "icelake run": configuration, writers, and then one library
// call.
//
// **The one library call is the point, and it is the test `AGENTS.md` states
// for every command in this repository**: a subcommand names the exported
// library entry point it delegates to, and a subcommand whose work cannot be
// named as one is the defect. This one names [icelake.IngestStream]. Everything
// between the two is translation and nothing else — environment into
// [icelake.Config] and [icelake.IngestOptions], stdin into the reader that call
// takes, a signal into the context it takes, and its errors into the exit codes
// below. Chunking, the size of a chunk, what happens to the records before a
// bad one, backing off when staging fills and halving a group the ceiling
// refuses all live in the library, where a crash test can reach them.
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

	// Bind each configured mirror expiry to its table. This is the second of
	// the four translations — environment into library values — finishing
	// here rather than in readSettings only because the entry is keyed by
	// table and which tables exist is the schema document's answer. An entry
	// naming a table the document does not declare is refused the way any
	// other unusable configuration is: it is a value nobody's table will ever
	// read, and dropping it silently would make a typo look like a decision.
	bound := 0
	for i := range tables {
		if ttl, ok := cfg.clickHouseTTL[tables[i].Namespace+"."+tables[i].Table]; ok {
			tables[i].MirrorTTL = &ttl
			bound++
		}
	}
	if bound != len(cfg.clickHouseTTL) {
		for key := range cfg.clickHouseTTL {
			found := false
			for _, tc := range tables {
				if key == tc.Namespace+"."+tc.Table {
					found = true

					break
				}
			}
			if !found {
				return fmt.Errorf("%w: %s names %q, which %s does not declare",
					errUsage, envClickHouseTTL.name, key, envSchemaFile.name)
			}
		}
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

	if err := icelake.IngestStream(ctx, writers, stdin, cfg.ingest(stderr)); err != nil {
		// A record this command will not read ends the run here, without
		// draining. That is the design rather than an oversight: what was
		// accepted before it is already durable in the staging database and is
		// replayed and committed by the next start, before that run reads a byte
		// of new input. Draining first would be doing work on the way out of a
		// failure nobody has diagnosed yet, and it would make "restart and it
		// picks up where it stopped" a slightly different sentence depending on
		// how the run ended. The process is exiting, so the databases go with it.
		return withVariableName(err)
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

// withVariableName puts this command's own vocabulary on a library refusal that
// is about a setting an operator wrote down.
//
// It is the fourth of the four translations a command in this repository may
// contain — library errors into exit codes and messages — and it is deliberately
// the narrowest possible version of it: exactly one kind is renamed, the size
// bound, because that is the one whose fix is to edit a specific variable in a
// unit file and the library cannot know what that variable is called. Every
// other refusal is passed through untouched, message and all, because the
// library's wording is already about the thing the operator has to fix.
func withVariableName(err error) error {
	var refusal icelake.IngestError
	if errors.As(err, &refusal) && refusal.Kind == icelake.IngestKindTooLarge {
		return fmt.Errorf("%w (%s is %s)", err, envMaxLineBytes.name, envMaxLineBytes.value())
	}

	return err
}

// ingest turns the resolved settings into the library's ingest options.
//
// One of the fields is set from the environment and the rest are deliberately
// left zero so they take the library's own documented defaults: the chunk
// bounds, the read-ahead and the backoff schedule are decisions about durability
// and backpressure, which is to say library decisions, and a command that named
// its own numbers for them would be making them again in a place no crash test
// can reach.
//
// There is nothing here that selects an input format, because there is no such
// setting: the library's grammar decides each record's encoding from that
// record's own first byte. This program got *smaller* when the second encoding
// arrived, which is the direction `AGENTS.md`'s placement rule points in.
func (s settings) ingest(stderr io.Writer) icelake.IngestOptions {
	return icelake.IngestOptions{
		MaxRecordBytes: s.maxLineBytes,
		OnNotice: func(n icelake.IngestNotice) {
			// The library hands over the fact and the words are this command's,
			// except for the position, which it renders itself so that this
			// program and the library's own refusals say it the same way.
			switch n.Kind {
			case icelake.IngestNoticeHeld:
				fmt.Fprintf(stderr, "icelake: staging is full at %s; holding stdin until it drains\n", n.Position())
			case icelake.IngestNoticeResumed:
				fmt.Fprintf(stderr, "icelake: staging has room again; reading resumed at %s\n", n.Position())
			}
		},
	}
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
