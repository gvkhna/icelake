package flush

import (
	"math/rand/v2"
	"time"
)

// Retry is the bounded retry policy one flush cycle runs under.
//
// A cycle makes at most MaxAttempts attempts at one batch and sleeps between
// them, so an upload or a commit that failed because the endpoint was briefly
// unreachable is retried without a caller doing anything. When the budget is
// spent the batch is not dropped and not retried immediately: it stays at the
// head of its table's queue, and the next flush trigger starts a fresh budget
// against the same batch. That is what makes a table stuck on an outage heal by
// itself when the outage ends, without spinning against a dead endpoint in
// between.
//
// The delays double from BaseDelay and are capped at MaxDelay, with full
// jitter: the sleep after attempt n is drawn uniformly from zero up to that
// attempt's ceiling. Jitter is not decoration. Every table in a process that
// lost its endpoint fails at the same moment, and a schedule without jitter
// would send them all back at the same instants forever — the retry storm that
// keeps a recovering endpoint down. Full jitter, rather than a fraction of the
// ceiling, is the variant that spreads attempts across the whole window.
type Retry struct {
	// MaxAttempts is how many attempts one cycle makes, including the first.
	// One means no retry at all.
	MaxAttempts int
	// BaseDelay is the ceiling of the sleep after the first failed attempt.
	BaseDelay time.Duration
	// MaxDelay caps every ceiling however many attempts have been made.
	MaxDelay time.Duration
}

// attempts is the attempt budget, never below one: a policy that permitted zero
// attempts would be a writer that never flushes.
func (r Retry) attempts() int {
	if r.MaxAttempts < 1 {
		return 1
	}
	return r.MaxAttempts
}

// ceiling returns the un-jittered upper bound of the sleep that follows attempt
// n, counting the first attempt as 1: BaseDelay doubled n-1 times, capped at
// MaxDelay.
//
// The doubling is a loop rather than a shift so that a large attempt number
// cannot overflow into a negative duration — the one way a backoff schedule can
// turn into no backoff at all.
func (r Retry) ceiling(attempt int) time.Duration {
	if attempt < 1 || r.BaseDelay <= 0 {
		return 0
	}

	d := r.BaseDelay
	for range attempt - 1 {
		if d > r.MaxDelay/2 {
			return r.MaxDelay
		}
		d *= 2
	}
	if d > r.MaxDelay {
		return r.MaxDelay
	}

	return d
}

// delay returns the sleep that follows attempt n: full jitter over that
// attempt's ceiling, drawn from unit, which returns a number in [0, 1).
//
// The random source is a parameter rather than a package-level call so the
// schedule is a pure calculation with an exact expected output — a schedule
// whose only source of randomness were the global generator could not be
// asserted at all, which is a design signal rather than a testing
// inconvenience.
func (r Retry) delay(attempt int, unit func() float64) time.Duration {
	ceiling := r.ceiling(attempt)
	if ceiling <= 0 {
		return 0
	}

	u := unit()
	switch {
	case u <= 0:
		return 0
	case u >= 1:
		return ceiling
	}

	return time.Duration(float64(ceiling) * u)
}

// Jitter is a test-only hook replacing the backoff's random source, and is nil
// in production.
//
// It is here for the same reason [Crash] is: the behaviour it controls is
// unobservable otherwise. Full jitter means the delay after an attempt is drawn
// from a *window*, so a test against the real source can only ever assert that
// a delay fell inside its window — which every wrong schedule with the same cap
// also satisfies. Pinning the draw makes the delays the worker asks for the
// ceilings themselves, and the ceilings are the schedule.
//
// The unit test that owns the schedule needs none of this: [Retry.delay] takes
// its source as a parameter, so the calculation is assertable on its own. This
// hook exists for the end-to-end test one layer up, whose claim is that the
// worker sleeps the schedule the calculation produces.
var Jitter func() float64

// jitter draws the unit random number full jitter scales by: a uniform number
// in [0, 1) from the standard library's own generator, which is safe for
// concurrent use and needs no seeding.
func jitter() float64 {
	if Jitter != nil {
		return Jitter()
	}

	return rand.Float64()
}
