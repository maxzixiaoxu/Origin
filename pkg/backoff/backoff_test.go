package backoff

import (
	"math"
	"testing"
	"time"
)

func TestCeilingDoublesFromBase(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Hour, Multiplier: 2}

	want := []time.Duration{
		1 * time.Second,  // attempt 0
		2 * time.Second,  // attempt 1
		4 * time.Second,  // attempt 2
		8 * time.Second,  // attempt 3
		16 * time.Second, // attempt 4
	}
	for attempt, w := range want {
		if got := p.Ceiling(attempt); got != w {
			t.Errorf("Ceiling(%d) = %s, want %s", attempt, got, w)
		}
	}
}

func TestCeilingClampsToCap(t *testing.T) {
	p := Policy{Base: time.Second, Cap: 10 * time.Second, Multiplier: 2}

	// 2^4 = 16s exceeds the 10s cap.
	for _, attempt := range []int{4, 5, 10, 50} {
		if got := p.Ceiling(attempt); got != 10*time.Second {
			t.Errorf("Ceiling(%d) = %s, want the 10s cap", attempt, got)
		}
	}
}

// The reason Ceiling computes in float64 rather than by integer shifting.
//
// A job that somehow reaches a very high attempt count -- a stuck retry loop, a
// corrupted counter, an operator replaying a dead job repeatedly -- must not
// produce a negative duration. base * 2^attempt overflows int64 around attempt
// 63; if that wrapped negative it would compare as "less than Cap", be returned
// as-is, and a negative delay means the retry fires instantly. That converts a
// slow retry ladder into an unbounded hot loop against a failing dependency,
// which is exactly the outage the backoff exists to prevent.
func TestCeilingDoesNotOverflowAtExtremeAttempts(t *testing.T) {
	p := Policy{Base: time.Second, Cap: 5 * time.Minute, Multiplier: 2}

	for _, attempt := range []int{62, 63, 64, 100, 1000, math.MaxInt32} {
		got := p.Ceiling(attempt)
		if got <= 0 {
			t.Fatalf("Ceiling(%d) = %s, must never be zero or negative", attempt, got)
		}
		if got != 5*time.Minute {
			t.Errorf("Ceiling(%d) = %s, want the 5m cap", attempt, got)
		}
	}
}

func TestCeilingTreatsNegativeAttemptAsZero(t *testing.T) {
	p := Default()
	if got, want := p.Ceiling(-5), p.Ceiling(0); got != want {
		t.Errorf("Ceiling(-5) = %s, want %s", got, want)
	}
}

// A partially-specified policy -- which is what a misconfigured queue row
// produces -- must degrade to sane defaults rather than to a zero delay.
func TestZeroPolicyDegradesToDefaults(t *testing.T) {
	var p Policy // all fields zero

	if got := p.Ceiling(0); got != DefaultBase {
		t.Errorf("Ceiling(0) on zero policy = %s, want %s", got, DefaultBase)
	}
	if got := p.Ceiling(100); got != DefaultCap {
		t.Errorf("Ceiling(100) on zero policy = %s, want %s", got, DefaultCap)
	}
	// The failure this guards against: a zero delay means retries fire with no
	// pause at all.
	if got := p.Delay(0); got < 0 {
		t.Errorf("Delay(0) = %s, must not be negative", got)
	}
}

func TestNormalizeRaisesCapBelowBase(t *testing.T) {
	// A queue configured with cap < base is contradictory; the cap should rise
	// to meet the base rather than silently truncating every delay.
	p := Policy{Base: 10 * time.Second, Cap: time.Second, Multiplier: 2}
	if got := p.Ceiling(0); got != 10*time.Second {
		t.Errorf("Ceiling(0) = %s, want base 10s", got)
	}
}

func TestDelayStaysWithinCeiling(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Minute, Multiplier: 2}

	for attempt := 0; attempt < 8; attempt++ {
		ceiling := p.Ceiling(attempt)
		for i := 0; i < 200; i++ {
			d := p.Delay(attempt)
			if d < 0 || d > ceiling {
				t.Fatalf("Delay(%d) = %s, outside [0, %s]", attempt, d, ceiling)
			}
		}
	}
}

func TestDelayUsesInjectedRand(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Minute, Multiplier: 2}

	p.Rand = func() float64 { return 0 }
	if got := p.Delay(3); got != 0 {
		t.Errorf("Delay with rand=0 returned %s, want 0", got)
	}

	// rand() returns [0,1), so the ceiling itself is approached but not hit.
	p.Rand = func() float64 { return 0.5 }
	if got, want := p.Delay(3), 4*time.Second; got != want {
		t.Errorf("Delay with rand=0.5 at attempt 3 = %s, want %s", got, want)
	}
}

// The property the whole package exists for.
//
// Fixed exponential backoff synchronises retries: every job that failed during
// the same outage retries at the same instant, re-overloads the recovering
// dependency, and fails together again on a longer period. Full jitter must
// spread a cohort of same-attempt retries across the entire interval. This test
// asserts that spread rather than just asserting bounds, because a buggy
// implementation returning a constant would still pass a bounds check.
func TestFullJitterSpreadsRetriesAcrossInterval(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Minute, Multiplier: 2}

	const (
		cohort  = 2000
		attempt = 5 // ceiling = 32s
	)
	ceiling := p.Ceiling(attempt)

	// Bucket the cohort into tenths of the interval. A correct full-jitter
	// implementation populates all ten roughly evenly; a fixed-delay
	// implementation would pile every sample into one bucket.
	const buckets = 10
	var counts [buckets]int
	for i := 0; i < cohort; i++ {
		d := p.Delay(attempt)
		idx := int(float64(buckets) * float64(d) / float64(ceiling))
		if idx >= buckets {
			idx = buckets - 1
		}
		counts[idx]++
	}

	// With 2000 samples over 10 buckets the expectation is 200 each. Allow a
	// generous band so the test is not flaky, while still failing hard on any
	// clustered or constant implementation.
	const minPerBucket, maxPerBucket = 120, 280
	for i, c := range counts {
		if c < minPerBucket || c > maxPerBucket {
			t.Errorf("bucket %d/%d held %d samples, want %d-%d "+
				"(retries are clustering instead of spreading)",
				i, buckets, c, minPerBucket, maxPerBucket)
		}
	}
}

func TestNextRunAtOffsetsFromNow(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Minute, Multiplier: 2,
		Rand: func() float64 { return 1 }}

	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	// rand=1 yields the full ceiling: attempt 2 -> 4s.
	if got, want := p.NextRunAt(now, 2), now.Add(4*time.Second); !got.Equal(want) {
		t.Errorf("NextRunAt = %s, want %s", got, want)
	}
}
