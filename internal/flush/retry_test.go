package flush

import (
	"testing"
	"time"
)

// This file is the one unit test the retry policy is entitled to under
// TESTING.md's philosophy: the backoff schedule is on the closed list of
// calculations whose exact output matters, alongside the canonical encoding and
// the decimal adapter. Everything else about failure handling — that the budget
// is bounded, that the callback fires, that a stuck table heals — is a claim
// about behaviour and is proven end to end against the real substrate in
// scenario8_test.go.
//
// Every expectation below is derived independently of the implementation, from
// the schedule ARCHITECTURE.md states: start at RetryBaseDelay, double, cap at
// RetryMaxDelay, full jitter. None of it is the code's own prior output
// recorded and re-asserted, which would prove nothing.
//
// This file used to hold one more test, TestAttemptBudgetIsNeverBelowOne, which
// called the unexported (Retry).attempts with a budget of zero and required it
// to resolve to one. It was deleted at the post-M9 completeness audit's repair
// round (2026-08-01) because it was outside every category TESTING.md permits
// and nothing about it could be brought inside one: the attempt budget is not on
// that document's closed list of calculations, the package-internal category
// does not reach flush at all (this layer talks to the object store), and the
// input it pinned is unreachable — Config refuses a negative FlushMaxAttempts
// and maps zero to the documented default, so a Retry with a budget below one
// cannot be built through the public API. The clamp in attempts stays as the
// cheap in-code defence it always was; what went is the test that dressed an
// unreachable line up as a proven claim.

// TestBackoffCeilingsAreTheDocumentedSchedule pins the exact un-jittered delay
// sequence at the documented defaults and at a test-shaped policy.
//
// The defaults are RetryBaseDelay 1s and RetryMaxDelay 60s, so the sequence is
// 1, 2, 4, 8, 16, 32 and then 60 forever — 64 is the first doubling that passes
// the cap. These numbers are written out here rather than computed, so a change
// to the schedule fails this test instead of quietly agreeing with it.
func TestBackoffCeilingsAreTheDocumentedSchedule(t *testing.T) {
	const s = time.Second

	cases := []struct {
		name  string
		retry Retry
		want  []time.Duration
	}{
		{
			name:  "the documented defaults",
			retry: Retry{MaxAttempts: 5, BaseDelay: s, MaxDelay: 60 * s},
			want:  []time.Duration{1 * s, 2 * s, 4 * s, 8 * s, 16 * s, 32 * s, 60 * s, 60 * s, 60 * s},
		},
		{
			// The shape TESTING.md prescribes for the suite: a base in the tens
			// of milliseconds, so a test that forces a failure does not spend
			// half a minute backing off.
			name:  "a test-shaped policy",
			retry: Retry{MaxAttempts: 2, BaseDelay: 20 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
			want: []time.Duration{
				20 * time.Millisecond,
				40 * time.Millisecond,
				80 * time.Millisecond,
				100 * time.Millisecond,
				100 * time.Millisecond,
			},
		},
		{
			// A cap below the base is refused by configuration validation. It is
			// checked here anyway, because a schedule that answered a nonsensical
			// policy with a delay longer than its own cap would be a worse
			// failure than the configuration that produced it.
			name:  "a cap under the base",
			retry: Retry{MaxAttempts: 3, BaseDelay: 10 * s, MaxDelay: s},
			want:  []time.Duration{s, s, s},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for i, want := range c.want {
				attempt := i + 1
				if got := c.retry.ceiling(attempt); got != want {
					t.Errorf("ceiling after attempt %d = %s, want %s", attempt, got, want)
				}
			}
		})
	}

	// The attempt number that would overflow a shift: 1s doubled 62 times is
	// already larger than the largest representable duration, and a schedule
	// that overflowed would come back negative — which is a sleep of no time at
	// all, in other words no backoff at exactly the point the endpoint is worst
	// off.
	huge := Retry{MaxAttempts: 1000, BaseDelay: s, MaxDelay: 60 * s}
	for _, attempt := range []int{63, 64, 1000, 1 << 20} {
		if got := huge.ceiling(attempt); got != 60*s {
			t.Errorf("ceiling after attempt %d = %s, want the %s cap", attempt, got, 60*s)
		}
	}
}

// TestBackoffJitterStaysWithinItsCeiling asserts the jitter is full jitter:
// uniform from zero up to the attempt's ceiling, inclusive of neither end being
// special-cased away.
//
// The exact values come from the definition — a draw of u yields u times the
// ceiling — so a quarter of a four-second ceiling is one second, computed here
// by hand rather than by re-running the implementation.
func TestBackoffJitterStaysWithinItsCeiling(t *testing.T) {
	const s = time.Second
	retry := Retry{MaxAttempts: 5, BaseDelay: s, MaxDelay: 60 * s}

	exact := []struct {
		attempt int
		draw    float64
		want    time.Duration
	}{
		{attempt: 1, draw: 0, want: 0},
		{attempt: 1, draw: 0.5, want: 500 * time.Millisecond},
		{attempt: 2, draw: 0.25, want: 500 * time.Millisecond},
		{attempt: 3, draw: 0.25, want: s},
		{attempt: 3, draw: 0.75, want: 3 * s},
		{attempt: 4, draw: 1, want: 8 * s},
		// A source that misbehaves must not produce a delay outside the window:
		// a negative draw is a zero-length sleep, not a negative one.
		{attempt: 4, draw: -1, want: 0},
		{attempt: 4, draw: 2, want: 8 * s},
	}
	for _, c := range exact {
		if got := retry.delay(c.attempt, func() float64 { return c.draw }); got != c.want {
			t.Errorf("delay after attempt %d with a draw of %v = %s, want %s", c.attempt, c.draw, got, c.want)
		}
	}

	// Across a deterministic sweep of the whole unit interval, every delay sits
	// inside its own ceiling and the schedule is monotonic in the draw. The
	// sweep is fixed rather than random so this test says the same thing on
	// every machine and on every run.
	for attempt := 1; attempt <= 8; attempt++ {
		ceiling := retry.ceiling(attempt)
		previous := time.Duration(-1)
		for step := range 101 {
			draw := float64(step) / 100
			got := retry.delay(attempt, func() float64 { return draw })
			if got < 0 || got > ceiling {
				t.Fatalf("delay after attempt %d with a draw of %v = %s, want between 0 and the %s ceiling", attempt, draw, got, ceiling)
			}
			if got < previous {
				t.Fatalf("delay after attempt %d fell from %s to %s as the draw rose to %v", attempt, previous, got, draw)
			}
			previous = got
		}
	}

	// The production source is the standard library's generator. What matters
	// here is only that it stays in the unit interval, which is what every
	// bound above rests on.
	for range 1000 {
		if u := jitter(); u < 0 || u >= 1 {
			t.Fatalf("the jitter source produced %v, want a number in [0, 1)", u)
		}
	}
}
