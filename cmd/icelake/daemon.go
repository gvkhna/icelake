package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
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
func runDaemon(ctx context.Context, cfg settings, tables []icelake.DynamicTableConfig, stdin io.Reader, log *slog.Logger, ready func()) error {
	// The one line that answers "what is this daemon actually configured
	// with" without anybody guessing from a unit file. The logger itself is
	// the caller's, built over whatever the mode decided the log is — stderr
	// attached, or the file a backgrounding parent redirected before this
	// process existed. The library reports nothing, by design; a program that
	// embeds it is entitled to do what any embedding service does, and this
	// one's convention is the log.
	log.Info("starting", cfg.logAttrs()...)

	// health remembers which tables have reported a failed flush cycle, so
	// the watcher below can log the recovery when one commits again. Without
	// it, an error would be the log's last word on a table that healed an
	// hour ago, and "no news" would be carrying two opposite meanings.
	health := &flushHealth{failedAt: make(map[string]time.Time)}

	store, err := icelake.Open(ctx, cfg.config(func(e icelake.FlushError) {
		if e.Quarantined {
			// The one flush failure that is not self-healing, and the line
			// says so in opposite words from the retried case: this batch has
			// left the queue for good, and the rows wait in staging for a
			// person.
			log.Error("a batch could not be encoded and is quarantined; it will not be retried",
				"table", e.Namespace+"."+e.Table, "batch", e.BatchKey,
				"quarantined", e.Records, "staging", cfg.stagingPath, "lost", 0,
				"action", "inspect the rows in staging.db (quarantined_at, quarantine_error), export or delete them; they count against the staging ceiling until removed",
				"err", e.Err)

			return
		}
		// Error, not warning: a whole flush cycle used up its retry budget.
		// Nothing is lost — the batch stays staged and the next trigger tries
		// again — but a log that keeps carrying this line is a lake that is
		// not committing, and eventually staging fills and backpressure
		// starts.
		log.Error("a flush cycle failed; the batch stays staged and will be retried",
			"table", e.Namespace+"."+e.Table, "batch", e.BatchKey,
			"staged", e.Records, "staging", cfg.stagingPath, "attempts", e.Attempts,
			"lost", 0,
			"action", "nothing, unless this line keeps repeating; the next trigger retries",
			"err", e.Err)
		health.markFailed(e.Namespace + "." + e.Table)
	}, func(e icelake.ClickHouseError) {
		// A warning, deliberately one level below the flush line: the mirror
		// losing a batch never fails a flush — the lake is the truth and the
		// mirror is rebuilt from it — so this is a degraded index, not data
		// at risk.
		attrs := []any{"table", e.Namespace + "." + e.Table, "kind", string(e.Kind),
			"target", e.Target, "lost", 0,
			"action", "none required; the mirror misses this batch until it is rebuilt from the lake",
			"err", e.Err}
		if e.BatchKey != "" {
			attrs = append(attrs, "batch", e.BatchKey, "records", e.Records)
		}
		log.Warn("the clickhouse mirror fell behind; the lake is unaffected", attrs...)
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
	backlog := 0
	for _, tc := range tables {
		key := tc.Namespace + "." + tc.Table
		// Announced before the open, not after: opening a table loads and
		// re-queues everything a previous run left staged, which can take
		// real time after a cut-short bulk load, and a slow open with no line
		// in front of it is a daemon that looks stuck.
		log.Info("opening table", "table", key)
		opened := time.Now()
		w, err := icelake.OpenDynamicWriter(ctx, store, tc)
		if err != nil {
			closeStore(context.WithoutCancel(ctx), store, cfg.shutdownTimeout)

			return err
		}
		writers[key] = w
		// What the open actually did with a previous run's staged records is
		// re-queue them: they commit in the background, alongside new input,
		// and this run's exit waits for them the same as for everything else.
		// The line says how many there are rather than claiming they are
		// already committed, because at this moment they are not.
		status := w.Status()
		backlog += status.PendingRecords
		log.Info("table ready",
			"table", key, "staged-from-previous-run", status.PendingRecords,
			"elapsed", time.Since(opened).Round(time.Millisecond).String())
	}

	// The daemon is now what it claims to be: every table reconciled, every
	// crash replayed, ready to read. This is the moment a backgrounding parent
	// is waiting to hear about before it exits, and it is deliberately after
	// the writers and before the first byte of input — a readiness that
	// arrived any earlier would be a promise about work still capable of
	// refusing.
	if ready != nil {
		ready()
	}
	log.Info("reading records", "tables", len(writers))

	// The heartbeat: at debug level, a periodic per-table line an operator can
	// follow activity by. It exists because a healthy daemon at info level is
	// deliberately silent between state changes, and "silent" and "stuck" look
	// identical until something says which. The recovery watcher runs at every
	// level, because what it logs is a state transition, not a period: the one
	// line that lets "no news" mean good news again after a flush failure.
	stopHeartbeat := startHeartbeat(writers, log)
	stopRecoveryWatch := health.watch(writers, log)

	err = icelake.IngestStream(ctx, writers, stdin, cfg.ingest(log))
	stopHeartbeat()
	if err != nil {
		// A record this command will not read ends the run here, without
		// draining. That is the design rather than an oversight: what was
		// accepted before it is already durable in the staging database and is
		// replayed and committed by the next run. Draining first would be doing
		// work on the way out of a failure nobody has diagnosed yet, and it
		// would make "restart and it picks up where it stopped" a slightly
		// different sentence depending on how the run ended. The process is
		// exiting, so the databases go with it — and the log's last structured
		// line is the account, because the plain report on the way to the exit
		// code goes to the terminal, which is not always the log.
		stopRecoveryWatch()
		_, staged := tally(writers)
		log.Error("a record this run refuses to read ended it",
			"staged", staged, "staging", cfg.stagingPath, "lost", 0,
			"action", "fix or remove the refused record and restart; the next run replays and commits everything staged",
			"err", err)

		return withVariableName(err)
	}

	err = drainAndClose(ctx, cfg, writers, store, backlog, log)
	stopRecoveryWatch()

	return err
}

// drainAndClose commits everything the run accepted, narrating as it goes, and
// its clock rule is the whole of the shutdown design:
//
// **End of input has no clock.** The producer is done, the remaining work is a
// finite pile whose size is known, and quitting it would buy nothing — the
// records would only wait in staging for the next start to commit instead.
// So an end-of-input drain runs to completion, however long that is, and
// [ICELAKE_SHUTDOWN_TIMEOUT] does not apply to it at all.
//
// **A signal has one.** A signal means someone wants this process gone soon,
// and the OS or a supervisor will escalate to a kill anyway; quitting on the
// configured budget is correct precisely because staging makes quitting safe.
// A signal that arrives in the middle of an end-of-input drain starts that
// same clock from that moment.
//
// Either way the log's last word says where every record is: committed, or
// staged with its count and its file, and in the second case that nothing is
// lost and the next start commits it. A drain must never end in silence or in
// jargon — that rule is `AGENTS.md`'s, set the day a bulk load's cut-short
// drain reported "context deadline exceeded" and nothing else.
func drainAndClose(ctx context.Context, cfg settings, writers map[string]*icelake.DynamicWriter, store *icelake.Store, backlog int, log *slog.Logger) error {
	accepted, remaining := tally(writers)
	signalled := ctx.Err() != nil
	if signalled {
		log.Info("stopped by a signal; committing everything accepted before exiting",
			"accepted-this-run", accepted, "staged-from-previous-run", backlog,
			"uncommitted", remaining, "timeout", cfg.shutdownTimeout.String())
	} else {
		log.Info("end of input; committing everything accepted before exiting",
			"accepted-this-run", accepted, "staged-from-previous-run", backlog,
			"uncommitted", remaining)
	}

	// The drain's own context, per the rule above: bounded from the start
	// after a signal, unbounded at end of input until a signal arrives, and
	// never the run's context itself — the cancellation that stopped the
	// reading must not also cancel the flush it is supposed to start.
	base := context.WithoutCancel(ctx)
	var drainCtx context.Context
	var cancel context.CancelFunc
	urgent := make(chan struct{})
	if signalled {
		close(urgent)
		drainCtx, cancel = context.WithTimeout(base, cfg.shutdownTimeout)
	} else {
		drainCtx, cancel = context.WithCancel(base)
		stopWatch := context.AfterFunc(ctx, func() {
			log.Warn("signal during the drain; it is bounded now",
				"variable", envShutdownTimeout.name, "timeout", cfg.shutdownTimeout.String())
			close(urgent)
			time.AfterFunc(cfg.shutdownTimeout, cancel)
		})
		defer stopWatch()
	}
	defer cancel()

	done := make(chan struct{})
	go narrateDrain(done, urgent, writers, log)

	err := drain(drainCtx, writers)
	if closeErr := store.Close(drainCtx); closeErr != nil && err == nil && !errors.Is(closeErr, icelake.ErrClosed) {
		err = closeErr
	}
	close(done)

	if err != nil {
		_, staged := tally(writers)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			log.Error("the shutdown timeout ended the drain with records still staged",
				"variable", envShutdownTimeout.name, "timeout", cfg.shutdownTimeout.String(),
				"staged", staged, "staging", cfg.stagingPath, "lost", 0,
				"action", "the next run replays and commits them")

			return fmt.Errorf("the drain did not finish inside %s=%s: %d records are still staged in %s; nothing is lost, and the next run replays and commits them",
				envShutdownTimeout.name, cfg.shutdownTimeout, staged, cfg.stagingPath)
		}
		log.Error("the drain failed", "staged", staged, "staging", cfg.stagingPath, "lost", 0,
			"action", "the next run replays and commits what is staged", "err", err)

		return err
	}

	log.Info("drained; every accepted record is committed",
		"accepted-this-run", accepted, "committed-from-previous-run", backlog)

	return nil
}

// flushHealth is the memory behind the one recovery line: which tables have
// reported a failed flush cycle, and when. The flush callback writes it, the
// watcher reads it, and the line it exists for is logged exactly once per
// outage — a state transition, which is why the watcher may run at info level
// where nothing periodic is allowed.
type flushHealth struct {
	mu       sync.Mutex
	failedAt map[string]time.Time
}

func (h *flushHealth) markFailed(table string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, already := h.failedAt[table]; !already {
		h.failedAt[table] = time.Now()
	}
}

// watch logs the recovery of any table whose last reported state was a failed
// flush cycle, the moment a commit newer than the failure appears with no
// newer failure beside it. It polls, because a commit is the library's own
// quiet business; it logs only on the transition, so a healthy log stays as
// quiet as the level rules promise.
func (h *flushHealth) watch(writers map[string]*icelake.DynamicWriter, log *slog.Logger) func() {
	const every = 15 * time.Second

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				h.mu.Lock()
				for table, failedAt := range h.failedAt {
					w, ok := writers[table]
					if !ok {
						delete(h.failedAt, table)

						continue
					}
					s := w.Status()
					if s.LastFlushErr == nil && s.LastCommitAt.After(failedAt) {
						log.Info("flushing recovered; the backlog is committing again",
							"table", table, "committed-batches", s.FlushesCommitted,
							"uncommitted", s.PendingRecords,
							"outage", time.Since(failedAt).Round(time.Second).String())
						delete(h.failedAt, table)
					}
				}
				h.mu.Unlock()
			}
		}
	}()

	return func() { close(done) }
}

// narrateDrain reports the drain's progress until it is told the drain ended:
// how much is left, how fast it is going, and about how long the rest will
// take. Quietly — every five minutes — while nothing is waiting on it, and
// every five seconds once a signal means someone is.
func narrateDrain(done, urgent <-chan struct{}, writers map[string]*icelake.DynamicWriter, log *slog.Logger) {
	const (
		quietEvery  = 5 * time.Minute
		urgentEvery = 5 * time.Second
	)

	interval := quietEvery
	select {
	case <-urgent:
		interval = urgentEvery
	default:
	}
	// Debug level follows activity closely by definition, so the quiet cadence
	// does not apply there.
	if log.Enabled(context.Background(), slog.LevelDebug) {
		interval = urgentEvery
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	_, lastRemaining := tally(writers)
	lastAt := time.Now()
	for {
		select {
		case <-done:
			return
		case <-urgent:
			ticker.Reset(urgentEvery)
			urgent = nil
		case now := <-ticker.C:
			_, remaining := tally(writers)
			attrs := []any{"uncommitted", remaining}
			if elapsed := now.Sub(lastAt).Seconds(); elapsed > 0 && remaining < lastRemaining {
				rate := float64(lastRemaining-remaining) / elapsed
				attrs = append(attrs, "rate", fmt.Sprintf("%.0f/s", rate),
					"eta", (time.Duration(float64(remaining) / rate * float64(time.Second))).Round(time.Second).String())
			}
			log.Info("committing", attrs...)
			lastRemaining, lastAt = remaining, now
		}
	}
}

// startHeartbeat begins the debug-level activity line and returns the function
// that stops it. At any level above debug it starts nothing and the returned
// stop does nothing, which is what keeps a healthy info-level log free of
// periodic noise.
func startHeartbeat(writers map[string]*icelake.DynamicWriter, log *slog.Logger) func() {
	const every = 30 * time.Second

	if !log.Enabled(context.Background(), slog.LevelDebug) {
		return func() {}
	}

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for key, w := range writers {
					s := w.Status()
					attrs := []any{"table", key,
						"accepted", s.RecordsAccepted,
						"uncommitted", s.PendingRecords, "uncommitted-bytes", s.PendingBytes,
						"committed-batches", s.FlushesCommitted}
					if !s.LastCommitAt.IsZero() {
						attrs = append(attrs, "last-commit-ago", time.Since(s.LastCommitAt).Round(time.Second).String())
					}
					if s.LastFlushErr != nil {
						attrs = append(attrs, "last-flush-err", s.LastFlushErr.Error())
					}
					log.Debug("activity", attrs...)
				}
			}
		}
	}()

	return func() { close(done) }
}

// tally sums what the writers have taken and what they still owe: records
// accepted since open, and records accepted but not yet committed.
func tally(writers map[string]*icelake.DynamicWriter) (accepted uint64, uncommitted int) {
	for _, w := range writers {
		s := w.Status()
		accepted += s.RecordsAccepted
		uncommitted += s.PendingRecords
	}

	return accepted, uncommitted
}

// loadTables reads the schema document and binds the environment's per-table
// mirror expiries onto the declarations it finds.
//
// It runs before anything else the daemon does — in a backgrounding run it
// runs in the parent, before the process that will do the work exists — so
// that every refusal it can make lands on the terminal of whoever typed the
// command rather than in a log file they have not found yet.
func loadTables(cfg settings) ([]icelake.DynamicTableConfig, error) {
	document, err := os.ReadFile(cfg.schemaFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", errUsage, envSchemaFile.name, err)
	}
	tables, err := icelake.ParseSchemaDocument(document)
	if err != nil {
		// A schema document that will not parse is a configuration problem, not
		// a runtime one: no restart fixes it, and a supervisor needs to be told
		// that rather than left to retry a file that will never change by itself.
		return nil, fmt.Errorf("%w: %s: %w", errUsage, envSchemaFile.name, err)
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("%w: %s declares no tables", errUsage, envSchemaFile.name)
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
				return nil, fmt.Errorf("%w: %s names %q, which %s does not declare",
					errUsage, envClickHouseTTL.name, key, envSchemaFile.name)
			}
		}
	}

	return tables, nil
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
func (s settings) ingest(log *slog.Logger) icelake.IngestOptions {
	return icelake.IngestOptions{
		MaxRecordBytes: s.maxLineBytes,
		OnNotice: func(n icelake.IngestNotice) {
			// The library hands over the fact and the words are this command's,
			// except for the position, which it renders itself so that this
			// program and the library's own refusals say it the same way.
			switch n.Kind {
			case icelake.IngestNoticeHeld:
				log.Warn("staging is full; holding stdin until it drains",
					"position", n.Position(), "lost", 0,
					"variables", envStagingMaxRecords.name+" "+envStagingMaxBytes.name,
					"action", "none if a flush is in flight; if this never resumes, look for flush errors or quarantined rows above")
			case icelake.IngestNoticeResumed:
				log.Info("staging has room again; reading resumed", "position", n.Position())
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
