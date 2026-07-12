// Package backoff computes retry delays for failed jobs.
//
// The policy is exponential backoff with *full jitter*. The naive alternative —
// a fixed doubling schedule — is what turns one transient downstream outage
// into a self-inflicted thundering herd: every job that failed at the same
// moment retries at the same moment, hammers the recovering dependency, fails
// together again, and repeats on a longer period. Randomising each delay over
// the whole interval [0, ceiling] spreads those retries out. It costs nothing
// and is the difference between a dependency recovering and a dependency being
// held down by its own clients.
package backoff

import (
	"math"
	"math/rand/v2"
	"time"
)

// Default policy values, chosen so a five-attempt job gives up after roughly a
// minute of wall-clock in the common case rather than immediately (too eager
// to dead-letter) or after an hour (operator never sees the failure in time).
const (
	DefaultBase       = 1 * time.Second
	DefaultCap        = 5 * time.Minute
	DefaultMultiplier = 2.0
)

// Policy describes an exponential-backoff-with-full-jitter schedule.
//
// The zero value is not useful; use Default() or set all fields.
type Policy struct {
	// Base is the ceiling for the first retry (attempt 0).
	Base time.Duration
	// Cap bounds the ceiling no matter how many attempts have elapsed.
	Cap time.Duration
	// Multiplier is the growth factor per attempt. 2.0 is standard doubling.
	Multiplier float64
	// Rand returns a float in [0.0, 1.0). Injectable so tests can be
	// deterministic; nil means use the global source.
	Rand func() float64
}

// Default returns the standard policy: 1s base, 5m cap, doubling.
func Default() Policy {
	return Policy{Base: DefaultBase, Cap: DefaultCap, Multiplier: DefaultMultiplier}
}

// normalized fills in sane values for a partially-specified policy so that a
// misconfigured queue degrades to the default rather than producing zero-length
// delays and hot-looping a failing job.
func (p Policy) normalized() Policy {
	if p.Base <= 0 {
		p.Base = DefaultBase
	}
	if p.Cap <= 0 {
		p.Cap = DefaultCap
	}
	if p.Cap < p.Base {
		p.Cap = p.Base
	}
	if p.Multiplier < 1 {
		p.Multiplier = DefaultMultiplier
	}
	return p
}

// Ceiling returns the un-jittered upper bound for the given attempt: the
// classic base * multiplier^attempt, clamped to Cap.
//
// attempt is zero-based — attempt 0 is the delay before the first retry.
// Exposed separately from Delay because it is the deterministic half of the
// computation and therefore the half that can be asserted on in tests.
func (p Policy) Ceiling(attempt int) time.Duration {
	p = p.normalized()
	if attempt < 0 {
		attempt = 0
	}

	// Compute in float64 rather than by repeated integer multiplication:
	// base * 2^attempt overflows int64 at around attempt 63 and would wrap to
	// a negative duration, which would then be "less than Cap" and get used.
	// A float64 saturates to +Inf instead, which clamps correctly below.
	growth := math.Pow(p.Multiplier, float64(attempt))
	ceiling := float64(p.Base) * growth

	if math.IsInf(ceiling, 1) || ceiling > float64(p.Cap) {
		return p.Cap
	}
	return time.Duration(ceiling)
}

// Delay returns the jittered delay to wait before retrying, drawn uniformly
// from [0, Ceiling(attempt)].
//
// Drawing from zero rather than from some fraction of the ceiling is the "full
// jitter" variant. It gives the widest possible spread, which is what we want:
// the goal is decorrelating retries, not guaranteeing any particular minimum
// wait. A job that happens to draw a near-zero delay is harmless; a thousand
// jobs that all draw the same delay is not.
func (p Policy) Delay(attempt int) time.Duration {
	ceiling := p.Ceiling(attempt)
	if ceiling <= 0 {
		return 0
	}

	r := p.Rand
	if r == nil {
		r = rand.Float64
	}
	return time.Duration(r() * float64(ceiling))
}

// NextRunAt returns the absolute time a retry should become eligible. The
// queue service stores this as the job's run_at and as the score in the
// scheduled sorted set, so promotion is a plain range scan by time.
func (p Policy) NextRunAt(now time.Time, attempt int) time.Time {
	return now.Add(p.Delay(attempt))
}
