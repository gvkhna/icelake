package icelake_test

import (
	"context"
	"sync"
	"time"

	"github.com/gvkhna/icelake"
)

// recordingClock is icelake's own clock with one thing changed: a retry backoff
// sleep is recorded and returns immediately instead of being waited out.
//
// TESTING.md's philosophy allows exactly one seam, and this is it — the clock
// controls an *input*, and everything else in the tests that use it is real:
// the same MinIO container, the same staging file, the same DuckDB read-back.
// It is what makes the backoff schedule assertable at all. The delays the
// worker asks for are the only evidence that the schedule reached it, and
// waiting them out for real would trade that evidence for a slower suite and
// timing-dependent assertions.
//
// Now and NewTimer are the real ones, deliberately. The flush interval and the
// staged-record timestamps are not what these tests are about, and leaving them
// real keeps the fake surface to the one method the claim under test concerns.
type recordingClock struct {
	mu     sync.Mutex
	sleeps []time.Duration
}

// Now returns the real current time.
func (c *recordingClock) Now() time.Time { return time.Now() }

// NewTimer returns a real timer.
func (c *recordingClock) NewTimer(d time.Duration) icelake.Timer {
	return &recordingTimer{t: time.NewTimer(d)}
}

// Sleep records the delay asked for and returns at once, unless ctx is already
// done — in which case it answers exactly as the real clock would, so the
// shutdown behaviour of the code under test is unchanged by this seam.
func (c *recordingClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Sleeps returns every delay asked for so far, in order.
func (c *recordingClock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]time.Duration(nil), c.sleeps...)
}

// recordingTimer is a real timer behind the [icelake.Timer] interface.
type recordingTimer struct{ t *time.Timer }

// C returns the underlying timer's channel.
func (r *recordingTimer) C() <-chan time.Time { return r.t.C }

// Reset restarts the underlying timer.
func (r *recordingTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// Stop halts the underlying timer.
func (r *recordingTimer) Stop() bool { return r.t.Stop() }

// blockingClock makes a retry sleep last until something interrupts it.
//
// It exists for one claim: a shutdown must not have to wait out a backoff. Real
// backoff is jittered, so a test that used the real clock would be asserting
// against a delay that might have been milliseconds — proving nothing on the
// runs where the draw came out small. Pinning the sleep here removes that
// ambiguity in the only way the design allows, through the same injected clock
// production callers never touch, and leaves the interruption itself entirely
// to the code under test: this sleep returns when its context says so, and
// never otherwise.
type blockingClock struct {
	// entered is closed the first time a sleep begins.
	entered chan struct{}
	once    sync.Once

	mu          sync.Mutex
	interrupted bool
}

// Now returns the real current time.
func (c *blockingClock) Now() time.Time { return time.Now() }

// NewTimer returns a real timer.
func (c *blockingClock) NewTimer(d time.Duration) icelake.Timer {
	return &recordingTimer{t: time.NewTimer(d)}
}

// Sleep blocks until ctx is done, whatever delay it was asked for.
func (c *blockingClock) Sleep(ctx context.Context, _ time.Duration) error {
	c.once.Do(func() { close(c.entered) })

	<-ctx.Done()

	c.mu.Lock()
	c.interrupted = true
	c.mu.Unlock()

	return ctx.Err()
}

// Interrupted reports whether a sleep ended because its context was done.
func (c *blockingClock) Interrupted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.interrupted
}

// steppedClock stops time and lets the test move it forward itself.
//
// It exists for the flush floor, which is the one behaviour in this library
// whose whole point is how much time has passed since something else happened.
// Against a real clock the floor test would be asserting on how long a hundred
// fsynced inserts happened to take on the machine running it — true on a
// developer's laptop, a coin flip in CI. Freezing time makes "the floor has not
// elapsed" exact, and advancing it makes "and then it does, and the coalesced
// batch commits" exact too.
//
// Only Now is controlled, which is the input the floor reads. Timers are real,
// so the interval trigger keeps arriving on its own and the test is watching
// the real timer loop rather than a substitute for it. A retry sleep returns at
// once, as [recordingClock]'s does, so nothing here waits out a backoff.
type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

// newSteppedClock starts a stopped clock at a fixed instant.
func newSteppedClock() *steppedClock {
	return &steppedClock{now: time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)}
}

// Now returns the current time, which only [steppedClock.Advance] changes.
func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

// Advance moves the clock forward by d.
func (c *steppedClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// NewTimer returns a real timer: the interval trigger is left alone.
func (c *steppedClock) NewTimer(d time.Duration) icelake.Timer {
	return &recordingTimer{t: time.NewTimer(d)}
}

// Sleep returns immediately unless ctx is already done, so a retry backoff
// costs the test nothing.
func (c *steppedClock) Sleep(ctx context.Context, _ time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// flushErrors collects what the OnFlushError callback was handed, which is the
// same thing a production caller would do with it — log it, count it, page
// someone — reduced to what a test can assert on.
type flushErrors struct {
	mu     sync.Mutex
	events []icelake.FlushError
	// block, when non-nil, is waited on inside the callback, which is how a
	// test makes the callback slow on purpose.
	block chan struct{}
	// panics makes the callback panic, which is how a test proves a caller's
	// bug in it cannot take the flush path down.
	panics bool
}

// record is the callback itself, in the shape Config takes.
func (f *flushErrors) record(e icelake.FlushError) {
	f.mu.Lock()
	f.events = append(f.events, e)
	block, shouldPanic := f.block, f.panics
	f.mu.Unlock()

	if block != nil {
		<-block
	}
	if shouldPanic {
		panic("the caller's flush-error callback is broken")
	}
}

// Events returns every invocation so far, in order.
func (f *flushErrors) Events() []icelake.FlushError {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]icelake.FlushError(nil), f.events...)
}

// Len returns how many times the callback has been invoked.
func (f *flushErrors) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.events)
}
