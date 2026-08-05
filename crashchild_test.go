package icelake_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/flush"
)

// childPlanEnv carries the child's whole plan, JSON-encoded. Its presence is
// also what selects the helper path in [TestMain], so one variable both chooses
// the personality and configures it — there is no way to be half a child.
const childPlanEnv = "ICELAKE_CRASH_CHILD_PLAN"

// The exit codes the child uses, which are the parent's evidence about how it
// died.
const (
	// childExitCrashPoint is the exit a named crash point takes. It is the
	// value TESTING.md names for the hook, and the parent asserts on it: a
	// child that exited any other way did not die where the test needed it to.
	childExitCrashPoint = 1
	// childExitBroken is the exit for a child that could not do its job at all
	// — a store that would not open, an insert that failed. It is deliberately
	// distinct from the crash-point exit so a broken child can never be
	// mistaken for a successful crash.
	childExitBroken = 2
)

// The lines the child writes to stdout. They are a progress protocol, not log
// output: the parent blocks on exactly one of them before it kills the child,
// which is what makes a crash test deterministic rather than a race against a
// sleep.
const (
	// lineHeld says the flush path has reached the named crash point and is
	// being held there.
	lineHeld = "held"
	// lineStaged says every record the plan asked for has been accepted.
	lineStaged = "staged"
	// lineReady says the child has reached its target state and is now parked,
	// doing nothing, waiting to be killed.
	lineReady = "ready"
)

// acceptedLine is the acknowledgement the child prints after each Insert
// returns. Insert returns only once the record is durable in staging, so a line
// the parent has read is a record the parent knows survives a kill.
func acceptedLine(table string, i int) string { return fmt.Sprintf("ok %s %d", table, i) }

// The child scripts. Each one exists to leave the process in one specific state
// at the instant it dies; between them they cover scenario 2's three variants.
const (
	// scriptStream inserts records and acknowledges each one, either forever or
	// up to a bound, and is what a SIGKILL mid-stream interrupts.
	scriptStream = "stream"
	// scriptFlushCrash inserts a batch and then flushes it, so the flush path
	// runs with nothing else happening in the process and a named crash point
	// takes it down at a known instant.
	scriptFlushCrash = "flush-crash"
	// scriptDrain opens one writer and nothing else, which is what runs the
	// transition drain: a backlog of spool files is walked, uploaded and
	// committed inside OpenWriter, before the table is loaded. It exists so a
	// parent can kill a process part-way through that walk, which is the one
	// state scenario 12's idempotency claim is about and which no in-process
	// test can reach.
	scriptDrain = "drain"
	// scriptHoldSeal seals one batch, holds the flush path at the crash point
	// rather than exiting there, stages further rows behind it, and parks. It
	// is the only way to reach variant 3's state — a sealed batch and unsealed
	// rows in one staging file — deterministically, because a seal takes every
	// row in the current batch, so the unsealed ones can only be staged after
	// the seal has already happened.
	scriptHoldSeal = "hold-seal"
	// scriptBatchCrash accepts a few records one at a time, acknowledges them,
	// and then enters one InsertBatch large enough that the parent can kill the
	// process while its staging transaction is still open. It is the only way to
	// reach the state M15's durability decision is about — a crash *between two
	// rows of one batch* — because that transaction is open for microseconds
	// unless the batch is big enough to make it milliseconds.
	scriptBatchCrash = "batch-crash"
)

// The two things a named crash point can do when it fires.
const (
	// crashExit is TESTING.md's own mechanism: an immediate os.Exit(1) at the
	// point, with no deferred cleanup and no drain.
	crashExit = "exit"
	// crashHold stops the flush path dead at the point and leaves it there. The
	// process still dies by SIGKILL from the parent, so it is no less a crash —
	// what the hold buys is a deterministic *state* at the moment of death,
	// which an immediate exit cannot express when the state needs work to
	// happen after the point is reached.
	crashHold = "hold"
)

// The declaration the child writes its Case A table under. Which one it is
// matters to the crash tests: variant 3 restarts under a different one.
const (
	declFill   = "fill"
	declFillV2 = "fillv2"
)

// unbounded asks a script to keep inserting until it is killed, which is what
// makes a mid-stream kill genuinely mid-stream rather than a kill that happened
// to land after the last insert.
const unbounded = -1

// childPlan is everything the child needs to run a real icelake workload: the
// substrate to write to, the files to write through, the thresholds to run
// under, what to insert, and where to die.
//
// It travels as JSON in one environment variable rather than as a dozen
// separate ones, so adding a knob cannot leave the two sides disagreeing about
// which variables exist.
type childPlan struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	WarehousePrefix string
	StagingPath     string
	CatalogPath     string

	// CacheDir and CacheMaxBytes configure the local Parquet spool, which the
	// scenario-11 crash variants need: the seam they die at only exists when
	// there is a cache to write to. They are empty and zero for every earlier
	// crash test, which is what keeps those tests running on the configuration
	// they were written against.
	CacheDir      string
	CacheMaxBytes int64
	// LocalOnly runs the child against no bucket at all, which is the only way
	// to leave a spool file behind that has never been anywhere: in bucket mode
	// a crash at the spool seam is a file the next run uploads, and in this mode
	// it is a file the next run re-encodes and only a later transition uploads.
	LocalOnly bool

	// ClickHouseAddr, ClickHouseUsername and ClickHousePassword configure the
	// optional mirror, which scenario 16's replay clause needs: the claim that
	// a replayed batch does not duplicate in the mirror is only reachable by
	// killing a process between the mirror insert and the lake commit, which is
	// a real crash at a named point. They are empty for every other crash test.
	ClickHouseAddr     string
	ClickHouseUsername string
	ClickHousePassword string

	FlushMaxRecords   int
	FlushMaxBytes     int64
	FlushInterval     time.Duration
	MinFlushInterval  time.Duration
	ZSTDLevel         int
	StagingMaxRecords int64
	StagingMaxBytes   int64

	// Script names what the child does; Decl names which declaration its Case A
	// table is written under.
	Script string
	Decl   string

	// FillNamespace/FillTable and EventNamespace/EventTable identify the two
	// tables. They are part of the plan rather than constants so that several
	// subtests can share one bucket without colliding.
	FillNamespace  string
	FillTable      string
	EventNamespace string
	EventTable     string

	// Fills and Events are how many records to insert into each table, or
	// [unbounded]. Extra is how many further records to stage after the crash
	// point is reached, which only scriptHoldSeal uses.
	Fills  int
	Events int
	// Batch is how many records scriptBatchCrash hands to InsertBatch in one
	// call, after the Fills records it inserts one at a time. It is large on
	// purpose: the transaction has to be open long enough for a parent to kill
	// the process inside it.
	Batch int
	Extra int

	// CrashPoint is the named point to die at, empty for a script that is
	// killed from outside instead, and CrashMode is what happens when it fires.
	CrashPoint string
	CrashMode  string
}

// crashPlan builds the plan for a child that writes through the same
// configuration the parent test is holding, which is the only way the two can
// be talking about the same staging file and the same bucket.
func crashPlan(cfg icelake.Config) childPlan {
	return childPlan{
		Endpoint:        cfg.Endpoint,
		Region:          cfg.Region,
		Bucket:          cfg.Bucket,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		WarehousePrefix: cfg.WarehousePrefix,
		StagingPath:     cfg.StagingPath,
		CatalogPath:     cfg.CatalogPath,
		CacheDir:        cfg.CacheDir,
		CacheMaxBytes:   cfg.CacheMaxBytes,
		LocalOnly:       cfg.LocalOnly,

		ClickHouseAddr:     clickHouseField(cfg, func(c *icelake.ClickHouseConfig) string { return c.Addr }),
		ClickHouseUsername: clickHouseField(cfg, func(c *icelake.ClickHouseConfig) string { return c.Username }),
		ClickHousePassword: clickHouseField(cfg, func(c *icelake.ClickHouseConfig) string { return c.Password }),

		FlushMaxRecords:   cfg.FlushMaxRecords,
		FlushMaxBytes:     cfg.FlushMaxBytes,
		FlushInterval:     cfg.FlushInterval,
		MinFlushInterval:  cfg.MinFlushInterval,
		ZSTDLevel:         cfg.ZSTDLevel,
		StagingMaxRecords: cfg.StagingMaxRecords,
		StagingMaxBytes:   cfg.StagingMaxBytes,

		Script: scriptStream,
		Decl:   declFill,

		FillNamespace:  "market",
		FillTable:      "fills",
		EventNamespace: "archive",
		EventTable:     "events",
	}
}

// clickHouseField reads one field of an optional mirror configuration, or
// returns empty when there is none.
func clickHouseField(cfg icelake.Config, read func(*icelake.ClickHouseConfig) string) string {
	if cfg.ClickHouse == nil {
		return ""
	}

	return read(cfg.ClickHouse)
}

// config rebuilds the icelake configuration the child runs under.
func (p childPlan) config() icelake.Config {
	var mirror *icelake.ClickHouseConfig
	if p.ClickHouseAddr != "" {
		mirror = &icelake.ClickHouseConfig{
			Addr:     p.ClickHouseAddr,
			Username: p.ClickHouseUsername,
			Password: p.ClickHousePassword,
		}
	}

	return icelake.Config{
		ClickHouse:        mirror,
		StagingPath:       p.StagingPath,
		CatalogPath:       p.CatalogPath,
		CacheDir:          p.CacheDir,
		CacheMaxBytes:     p.CacheMaxBytes,
		LocalOnly:         p.LocalOnly,
		Endpoint:          p.Endpoint,
		Bucket:            p.Bucket,
		WarehousePrefix:   p.WarehousePrefix,
		Region:            p.Region,
		AccessKeyID:       p.AccessKeyID,
		SecretAccessKey:   p.SecretAccessKey,
		FlushMaxRecords:   p.FlushMaxRecords,
		FlushMaxBytes:     p.FlushMaxBytes,
		FlushInterval:     p.FlushInterval,
		MinFlushInterval:  p.MinFlushInterval,
		ZSTDLevel:         p.ZSTDLevel,
		StagingMaxRecords: p.StagingMaxRecords,
		StagingMaxBytes:   p.StagingMaxBytes,
	}
}

// TestMain gives this test binary its second personality.
//
// A crash test needs a real process to kill, running icelake's real write path
// against the real substrate — not this process, which would take the whole
// suite down with it, and not a stub, which would prove nothing. The standard
// Go answer is to re-execute the test binary with an environment variable
// selecting a helper main, and that is what this is: with the variable set the
// binary is the workload, without it it is the test suite. The child never
// touches testing.T, and none of its code is reachable from an ordinary run.
func TestMain(m *testing.M) {
	if plan := os.Getenv(childPlanEnv); plan != "" {
		runCrashChild(plan)
	}

	os.Exit(m.Run())
}

// say writes one progress line. It is unbuffered on purpose: the parent's next
// move depends on having seen it, so a line still sitting in a buffer when the
// process dies would be a line that never existed.
func say(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stdout, format+"\n", args...)
}

// childFatal ends a child that cannot do its job, distinctly from one that
// crashed where it was told to.
func childFatal(what string, err error) {
	_, _ = fmt.Fprintf(os.Stderr, "crash child: %s: %v\n", what, err)
	os.Exit(childExitBroken)
}

// errNoCrash reports a flush that returned normally when the plan said the
// process should have died inside it.
var errNoCrash = errors.New("the flush returned instead of crashing at the named point")

// park blocks forever without blocking the runtime's deadlock detector, which
// is how a child says "I am in the state you asked for; kill me now".
func park() {
	for {
		time.Sleep(time.Hour)
	}
}

// runCrashChild is the helper main. It never returns: either a crash point
// takes the process down, or the parent kills it while it is parked.
func runCrashChild(encoded string) {
	var p childPlan
	if err := json.Unmarshal([]byte(encoded), &p); err != nil {
		childFatal("reading the plan", err)
	}

	held := installCrashPoint(p)

	ctx := context.Background()
	store, err := icelake.Open(ctx, p.config())
	if err != nil {
		childFatal("Open", err)
	}
	// Deliberately never closed. This process is going to die where it stands,
	// and a deferred close would be the graceful shutdown a crash test exists
	// not to be.

	switch p.Script {
	case scriptStream:
		childStream(ctx, store, p)
	case scriptFlushCrash:
		childFlushCrash(ctx, store, p)
	case scriptDrain:
		childDrain(ctx, store, p)
	case scriptHoldSeal:
		childHoldSeal(ctx, store, p, held)
	case scriptBatchCrash:
		childBatchCrash(ctx, store, p)
	default:
		childFatal("running the plan", fmt.Errorf("unknown script %q", p.Script))
	}
}

// installCrashPoint wires the flush path's test-only hook to the plan's named
// point, and returns the channel that closes when a held point has been
// reached.
//
// Reading the point from the environment happens here, in the test helper, and
// nowhere else: the production hook is a nil function variable, so a build that
// never runs this code has no crash points at all.
func installCrashPoint(p childPlan) <-chan struct{} {
	held := make(chan struct{})
	if p.CrashPoint == "" {
		return held
	}

	var once sync.Once
	flush.Crash = func(point string) {
		if point != p.CrashPoint {
			return
		}
		if p.CrashMode == crashHold {
			once.Do(func() {
				say(lineHeld)
				close(held)
			})
			park()

			return
		}
		// No deferred cleanup, no drain, no chance to tidy anything up. Only a
		// process that dies exactly here proves what recovery from exactly here
		// is worth.
		os.Exit(childExitCrashPoint)
	}

	return held
}

// childTable is one open writer, reduced to what a script does with it. It
// exists because Writer is generic over the declaration type and the scripts
// are not.
type childTable struct {
	insert      func(ctx context.Context, i int) error
	insertBatch func(ctx context.Context, from, to int) error
	flush       func(ctx context.Context) error
}

// openFills opens the Case A table under whichever declaration the plan names.
func openFills(ctx context.Context, store *icelake.Store, p childPlan) childTable {
	if p.Decl == declFillV2 {
		w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[FillV2]{
			Namespace: p.FillNamespace, Table: p.FillTable,
		})
		if err != nil {
			childFatal("OpenWriter", err)
		}

		return childTable{
			insert: func(ctx context.Context, i int) error { return w.Insert(ctx, makeFillV2(i)) },
			insertBatch: func(ctx context.Context, from, to int) error {
				rows := make([]FillV2, 0, to-from)
				for i := from; i < to; i++ {
					rows = append(rows, makeFillV2(i))
				}

				return w.InsertBatch(ctx, rows)
			},
			flush: w.Flush,
		}
	}

	w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
		Namespace: p.FillNamespace, Table: p.FillTable,
	})
	if err != nil {
		childFatal("OpenWriter", err)
	}

	return childTable{
		insert: func(ctx context.Context, i int) error { return w.Insert(ctx, makeFill(i)) },
		insertBatch: func(ctx context.Context, from, to int) error {
			rows := make([]Fill, 0, to-from)
			for i := from; i < to; i++ {
				rows = append(rows, makeFill(i))
			}

			return w.InsertBatch(ctx, rows)
		},
		flush: w.Flush,
	}
}

// openEvents opens the Case B table, which the crash tests use to prove that
// two tables sharing one staging file are both recovered and neither one's rows
// arrive in the other.
func openEvents(ctx context.Context, store *icelake.Store, p childPlan) childTable {
	w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{
		Namespace: p.EventNamespace, Table: p.EventTable,
	})
	if err != nil {
		childFatal("OpenWriter", err)
	}

	return childTable{
		insert: func(ctx context.Context, i int) error { return w.Insert(ctx, makeEvent(i)) },
		flush:  w.Flush,
	}
}

// insertRange inserts a half-open range of records, acknowledging each one.
func insertRange(ctx context.Context, t childTable, label string, from, to int) {
	for i := from; i < to; i++ {
		if err := t.insert(ctx, i); err != nil {
			childFatal("Insert", err)
		}
		say(acceptedLine(label, i))
	}
}

// childStream inserts into one or both tables, acknowledging every record, and
// then parks — or never stops, if the plan asked for an unbounded stream.
//
// The unbounded form is what makes a mid-stream kill honest: the process is
// inside its insert loop when the signal arrives, because the loop has no end.
func childStream(ctx context.Context, store *icelake.Store, p childPlan) {
	fills := openFills(ctx, store, p)
	var events childTable
	if p.Events != 0 {
		events = openEvents(ctx, store, p)
	}

	for i := 0; p.Fills == unbounded || i < p.Fills; i++ {
		if err := fills.insert(ctx, i); err != nil {
			childFatal("Insert", err)
		}
		say(acceptedLine("fill", i))

		if p.Events == unbounded || i < p.Events {
			if err := events.insert(ctx, i); err != nil {
				childFatal("Insert", err)
			}
			say(acceptedLine("event", i))
		}
	}

	say(lineReady)
	park()
}

// childBatchCrash accepts a few records one at a time, says so, and then enters
// one large InsertBatch and stays in it.
//
// The records inserted first are what the parent knows is durable, exactly as in
// every other script here: Insert returns only once a record is committed to
// staging. The line printed after them is the parent's signal to kill, and the
// batch that follows it is sized so the staging transaction is still open when
// the signal lands. The line after the batch is deliberately printed only on the
// path where the batch finished, so a parent that sees it knows its kill was too
// late and can say so rather than quietly asserting nothing.
func childBatchCrash(ctx context.Context, store *icelake.Store, p childPlan) {
	fills := openFills(ctx, store, p)
	insertRange(ctx, fills, "fill", 0, p.Fills)
	say(lineStaged)

	if err := fills.insertBatch(ctx, p.Fills, p.Fills+p.Batch); err != nil {
		childFatal("InsertBatch", err)
	}

	say(lineReady)
	park()
}

// childDrain opens one writer, which is what walks a spool backlog into the
// bucket, and then parks so the parent can kill it whenever it likes.
//
// Nothing is inserted and nothing is flushed: every commit this process makes is
// the drain's, so a kill during it is unambiguously a kill during a drain.
func childDrain(ctx context.Context, store *icelake.Store, p childPlan) {
	openFills(ctx, store, p)

	say(lineReady)
	park()
}

// childFlushCrash stages a batch and flushes it, with nothing else happening in
// the process, so the named crash point fires at a known instant against a
// known set of records.
func childFlushCrash(ctx context.Context, store *icelake.Store, p childPlan) {
	fills := openFills(ctx, store, p)
	insertRange(ctx, fills, "fill", 0, p.Fills)
	say(lineStaged)

	if err := fills.flush(ctx); err != nil {
		childFatal("Flush", err)
	}
	childFatal("Flush", errNoCrash)
}

// childHoldSeal leaves the one state variant 3 needs: a batch sealed durably
// and never encoded, and further rows staged behind it that were never sealed
// at all.
//
// The order is forced by what a seal is. Sealing takes every row in the current
// batch, so rows that must stay unsealed can only be staged after the seal has
// already happened — which means the process cannot die at the crash point, it
// has to stop there and be killed once the rest of the state exists.
func childHoldSeal(ctx context.Context, store *icelake.Store, p childPlan, held <-chan struct{}) {
	fills := openFills(ctx, store, p)

	// The last of these trips the size threshold, which seals the batch and
	// hands it to the worker; the worker stamps the batch key durably and is
	// then held at the crash point.
	insertRange(ctx, fills, "fill", 0, p.Fills)
	<-held

	// Behind the sealed batch: rows that are durable and carry no batch key.
	insertRange(ctx, fills, "fill", p.Fills, p.Fills+p.Extra)

	if p.Events != 0 {
		events := openEvents(ctx, store, p)
		insertRange(ctx, events, "event", 0, p.Events)
	}

	say(lineReady)
	park()
}

// childLineTimeout bounds how long the parent waits for a progress line. It is
// generous: the child pulls no images and starts no containers, but it does run
// under the race detector and does create a table over the network.
const childLineTimeout = 2 * time.Minute

// crashChild is a running child process as the parent test drives it.
type crashChild struct {
	cmd     *exec.Cmd
	lines   chan string
	stderr  string
	log     *os.File
	scanned chan struct{}

	// reap runs the shutdown sequence exactly once, whichever of the test body
	// and the cleanup gets there first, and gives whoever gets there second a
	// synchronized view of err rather than a race with the first.
	reap     sync.Once
	err      error
	reported bool

	// said records every line the child ever printed, so that a claim about
	// something it did *not* say can be made after the kill. The scanning
	// goroutine fills it and the test body reads it, which is why it has a lock
	// of its own rather than riding on reap.
	mu   sync.Mutex
	said map[string]bool
}

// saw reports whether the child ever printed a line, and is only meaningful once
// the child has been reaped — [crashChild.sigkill] drains its output before it
// returns, so by then the answer is final.
//
// What it is for is the one thing a crash test cannot assume: that the kill
// landed where the test meant it to. A child that finished the work it was
// killed in the middle of would leave a state the assertions might still
// accept, and this is how a test says so out loud instead.
func (c *crashChild) saw(line string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.said[line]
}

// startCrashChild re-executes this test binary as the workload described by
// plan, and stops it when the test ends however the test ends.
func startCrashChild(tb testing.TB, plan childPlan) *crashChild {
	tb.Helper()

	encoded, err := json.Marshal(plan)
	if err != nil {
		tb.Fatalf("encoding the child's plan: %v", err)
	}

	path := filepath.Join(tb.TempDir(), "child.stderr")
	log, err := os.Create(path)
	if err != nil {
		tb.Fatalf("creating the child's error log: %v", err)
	}

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), childPlanEnv+"="+string(encoded))
	cmd.Stderr = log
	// The backstop for the paths where no cleanup runs at all: a parent that
	// panics on its own timeout, or is killed outright, must not leave a parked
	// child holding staging.db open forever.
	reapWithParent(cmd)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		tb.Fatalf("opening the child's output: %v", err)
	}

	c := &crashChild{
		cmd:     cmd,
		lines:   make(chan string, 4096),
		stderr:  path,
		log:     log,
		scanned: make(chan struct{}),
		said:    make(map[string]bool),
	}

	if err := cmd.Start(); err != nil {
		tb.Fatalf("starting the child: %v", err)
	}

	go func() {
		defer close(c.scanned)
		defer close(c.lines)

		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			c.mu.Lock()
			c.said[line] = true
			c.mu.Unlock()
			c.lines <- line
		}
	}()

	tb.Cleanup(func() {
		// Unconditional: killing a process that has already been reaped is a
		// no-op, and a child still parked has to go before its output can end.
		_ = c.cmd.Process.Kill()
		c.finish()
		c.report(tb)
	})

	return c
}

// finish drains the child's output, reaps it, and records how it died. Draining
// first is what lets the scanning goroutine reach end-of-file rather than
// blocking on a channel nobody is reading.
func (c *crashChild) finish() {
	c.reap.Do(func() {
		for range c.lines { //nolint:revive // empty-block: draining is the whole point
		}
		<-c.scanned

		c.err = c.cmd.Wait()
		_ = c.log.Close()
	})
}

// report puts whatever the child wrote to its standard error into the test's
// own output, once, so a child that failed explains itself rather than leaving
// the parent to guess.
func (c *crashChild) report(tb testing.TB) {
	tb.Helper()

	if c.reported {
		return
	}
	c.reported = true

	out, err := os.ReadFile(c.stderr)
	if err != nil || len(out) == 0 {
		return
	}
	tb.Logf("crash child stderr:\n%s", out)
}

// await blocks until the child prints exactly this line.
//
// This is where a crash test's determinism comes from. The parent does not
// sleep and hope: it acts only once the child has told it, on a pipe, that the
// state the test needs actually exists.
func (c *crashChild) await(tb testing.TB, want string) {
	tb.Helper()

	deadline := time.After(childLineTimeout)
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				c.finish()
				c.report(tb)
				tb.Fatalf("the child stopped (%v) before it said %q", c.err, want)
			}
			if line == want {
				return
			}
		case <-deadline:
			tb.Fatalf("the child did not say %q within %s", want, childLineTimeout)
		}
	}
}

// sigkill kills the child and asserts it really was killed rather than having
// exited on its own beforehand.
//
// Nothing softer will do. A signal a process can handle, or an in-process
// "simulated crash", still runs deferred cleanup and drains in-memory state,
// which would leave the test proving recovery from a tidy shutdown.
func (c *crashChild) sigkill(tb testing.TB) {
	tb.Helper()

	if err := c.cmd.Process.Kill(); err != nil {
		tb.Fatalf("killing the child: %v", err)
	}
	c.finish()

	var exit *exec.ExitError
	if !errors.As(c.err, &exit) {
		tb.Fatalf("the child ended with %v; a crash test needs a process that was killed", c.err)
	}
	// A process that died from a signal has no exit code of its own, which the
	// standard library reports as -1. Any real code here would mean the child
	// exited by itself before the kill landed.
	if code := exit.ExitCode(); code != -1 {
		c.report(tb)
		tb.Fatalf("the child exited with code %d before it could be killed", code)
	}
}

// awaitCrashPoint waits for a child that is expected to take itself down at a
// named crash point, and asserts that that is what happened.
func (c *crashChild) awaitCrashPoint(tb testing.TB, point string) {
	tb.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.finish()
	}()

	select {
	case <-done:
	case <-time.After(childLineTimeout):
		tb.Fatalf("the child did not crash at %q within %s", point, childLineTimeout)
	}

	var exit *exec.ExitError
	if !errors.As(c.err, &exit) {
		c.report(tb)
		tb.Fatalf("the child ended with %v, want the exit a crash point takes", c.err)
	}
	if code := exit.ExitCode(); code != childExitCrashPoint {
		c.report(tb)
		tb.Fatalf("the child exited with code %d, want %d — it did not die at %q",
			code, childExitCrashPoint, point)
	}
}

// makeFillV2 is [makeFill] plus the column added by the evolved declaration, so
// the record written under the new shape is the same record in every other
// respect.
func makeFillV2(i int) FillV2 {
	base := makeFill(i)
	kind := []string{"limit", "market"}[i%2]

	return FillV2{
		Symbol:         base.Symbol,
		Side:           base.Side,
		Price:          base.Price,
		Quantity:       base.Quantity,
		SequenceID:     base.SequenceID,
		OrderID:        base.OrderID,
		Source:         base.Source,
		VenueTimeMS:    base.VenueTimeMS,
		VenueOrderType: &kind,
	}
}
