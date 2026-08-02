package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
)

// Handlers used by the load generator and the chaos demos. They are NOT
// pretending to be real work, and the README says so.
//
// bench.noop in particular is a measurement instrument. Throughput numbers are
// supposed to describe the queue -- how fast it can lease, track, and complete
// jobs -- so the handler must contribute as close to zero as possible. Running
// the benchmark against image.derive would produce a number describing
// libjpeg's decode speed instead, which is not what any of the claims are
// about.

// BenchNoop returns immediately.
//
// The lower bound on per-job cost: everything it measures is queue overhead.
func BenchNoop() HandlerFunc {
	return func(_ context.Context, _ jobtypes.Envelope) ([]byte, error) {
		return nil, nil
	}
}

// SleepPayload configures bench.sleep.
type SleepPayload struct {
	Milliseconds int `json:"ms"`
}

// BenchSleep waits for the requested duration, respecting cancellation.
//
// Used to hold concurrency slots open while testing backpressure and lease
// renewal, where the point is occupying a slot rather than doing work. It
// honours ctx so a cancelled or lease-lost job stops immediately -- a handler
// that ignored cancellation would make the drain and reap tests meaningless.
func BenchSleep() HandlerFunc {
	return func(ctx context.Context, job jobtypes.Envelope) ([]byte, error) {
		var p SleepPayload
		if len(job.Payload) > 0 {
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return nil, jobtypes.Permanentf("bad bench.sleep payload: %v", err)
			}
		}
		if p.Milliseconds <= 0 {
			return nil, nil
		}

		timer := time.NewTimer(time.Duration(p.Milliseconds) * time.Millisecond)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, nil
		}
	}
}

// FlakyPayload configures bench.flaky.
type FlakyPayload struct {
	// FailTimes is how many attempts fail before one succeeds.
	FailTimes int `json:"fail_times"`
}

// BenchFlaky fails a fixed number of times, then succeeds.
//
// Exists to make the retry ladder visible in the dashboard: the attempt
// timeline shows real, widening backoff gaps followed by a success, which is
// hard to demonstrate convincingly with a handler that either always works or
// always fails.
func BenchFlaky() HandlerFunc {
	return func(_ context.Context, job jobtypes.Envelope) ([]byte, error) {
		var p FlakyPayload
		if len(job.Payload) > 0 {
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return nil, jobtypes.Permanentf("bad bench.flaky payload: %v", err)
			}
		}
		if p.FailTimes <= 0 {
			p.FailTimes = 2
		}

		// Envelope.Attempt is the attempt about to run, so attempts 1..FailTimes
		// fail and FailTimes+1 succeeds. Driving this from the attempt counter
		// rather than from worker-local state means it behaves identically no
		// matter which worker picks the job up.
		if job.Attempt <= p.FailTimes {
			return nil, jobtypes.Retryable(
				&transientError{attempt: job.Attempt, of: p.FailTimes})
		}

		return json.Marshal(map[string]any{
			"succeeded_on_attempt": job.Attempt,
			"failures_before":      p.FailTimes,
		})
	}
}

// BenchPoison always fails permanently, to populate the dead-letter queue for
// the DLQ browse-and-replay demo.
func BenchPoison() HandlerFunc {
	return func(_ context.Context, job jobtypes.Envelope) ([]byte, error) {
		return nil, jobtypes.Permanentf(
			"bench.poison always fails (attempt %d)", job.Attempt)
	}
}

type transientError struct {
	attempt int
	of      int
}

func (e *transientError) Error() string {
	return fmt.Sprintf("simulated transient failure %d of %d", e.attempt, e.of)
}

// RegisterBench adds the benchmark and chaos handlers to a registry.
func RegisterBench(r *Registry) {
	r.RegisterFunc("bench.noop", BenchNoop())
	r.RegisterFunc("bench.sleep", BenchSleep())
	r.RegisterFunc("bench.flaky", BenchFlaky())
	r.RegisterFunc("bench.poison", BenchPoison())
}
