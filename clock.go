package icelake

import (
	"context"
	"time"
)

// Clock is the single time source icelake reads. Every flush-interval timer,
// every flush floor, every retry sleep and every staged record's timestamp goes
// through it.
//
// It exists for exactly one consumer, the test suite, and is not a general
// extension point: timer-driven behaviour cannot be exercised deterministically
// against a real clock, and without this seam a test either sleeps for real or
// asserts loose bounds that go flaky under load. Production callers leave
// [Config.Clock] nil and get the real clock.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer returns a timer that fires once after d, like time.NewTimer.
	NewTimer(d time.Duration) Timer
	// Sleep blocks for d and returns nil, or returns ctx.Err() if ctx is done
	// first. A context that is *already* done is answered immediately, with its
	// error, whatever d is — including a d of zero, which full jitter can
	// genuinely draw. Every sleep inside icelake is interruptible for this
	// reason: a shutdown must not have to wait out a backoff, and must not be
	// able to lose a race with a delay of no length.
	Sleep(ctx context.Context, d time.Duration) error
}

// Timer is one pending timer, shaped like time.Timer so that the real
// implementation is a thin wrapper rather than a reimplementation.
type Timer interface {
	// C returns the channel the timer fires on.
	C() <-chan time.Time
	// Reset restarts the timer for d, reporting whether it was still active.
	Reset(d time.Duration) bool
	// Stop halts the timer, reporting whether it was still active.
	Stop() bool
}

// realClock is the production time source: time.Now, time.NewTimer, and a
// context-aware sleep.
type realClock struct{}

// Now returns the current time.
func (realClock) Now() time.Time { return time.Now() }

// NewTimer returns a timer backed by time.Timer.
func (realClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

// Sleep blocks for d or until ctx is done, whichever comes first.
//
// A context that is already done is answered before the timer is started, not
// by racing it. Without that, a zero-length delay — which full jitter draws
// sometimes, and drew here as often as any other value — would return nil half
// the time on a cancelled context, so a shutdown would be told a sleep it had
// interrupted had completed normally.
func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// realTimer adapts time.Timer to [Timer].
type realTimer struct{ t *time.Timer }

// C returns the underlying timer's channel.
func (r *realTimer) C() <-chan time.Time { return r.t.C }

// Reset restarts the underlying timer.
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// Stop halts the underlying timer.
func (r *realTimer) Stop() bool { return r.t.Stop() }
