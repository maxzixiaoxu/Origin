package pool

import (
	"context"
	"time"
)

// The heartbeater is one goroutine for the whole pool, not one per job.
//
// A worker running 64 jobs would otherwise open 64 renewal calls every tick,
// making RPC load scale with in-flight jobs rather than with workers -- and
// putting the heartbeat itself on the critical path for lease expiry exactly
// when the worker is busiest. One batched call per tick keeps renewal cost flat
// no matter how deep the concurrency goes.
//
// The more interesting half is what it does with the answer. A job whose lease
// was reclaimed is already running on another worker, so this worker's copy is
// duplicate effort whose result will be rejected at ack time. Cancelling that
// job's context stops it immediately and, crucially, does so through ordinary
// Go cancellation -- the handler sees a cancelled ctx and unwinds the same way
// it would for a timeout. No special "you lost your lease" path has to be
// written into every handler, and any handler that already respects
// context.Context gets correct behaviour for free.

// heartbeatLoop renews leases until ctx is cancelled.
func (p *Pool) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(p.opts.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.renewLeases(ctx)
		}
	}
}

// renewLeases extends every held lease in one call and cancels any that were
// lost.
func (p *Pool) renewLeases(ctx context.Context) {
	ids, running := p.snapshotInFlight()
	if len(ids) == 0 {
		return
	}

	// Bounded independently of the job timeout: a renewal that hangs must not
	// delay the next tick past the visibility timeout, or the worker would lose
	// every lease while waiting to find out about one.
	callCtx, cancel := context.WithTimeout(ctx, p.opts.HeartbeatInterval)
	defer cancel()

	results, err := p.opts.Client.ExtendLeases(callCtx, p.opts.WorkerID, ids)
	if err != nil {
		// A failed renewal is NOT evidence that the leases were lost -- far
		// more likely the queue service is briefly unreachable. Cancelling jobs
		// here would turn a network blip into mass abandonment of work that is
		// running fine, and the leases have not expired yet anyway. Log and let
		// the next tick decide.
		p.log.WarnContext(ctx, "lease renewal failed; keeping jobs running",
			"held", len(ids), "error", err)
		return
	}

	var lost int
	for _, r := range results {
		if r.Extended {
			continue
		}
		lost++

		job, ok := running[r.JobID]
		if !ok {
			// Finished between the snapshot and the response. Nothing to do.
			continue
		}

		p.log.WarnContext(ctx, "lease lost; cancelling the running job",
			"job_id", r.JobID,
			"job_type", job.envelope.Type,
			"ran_for", time.Since(job.started))

		// The cause is what execute() inspects to distinguish this from a
		// timeout, so the job is abandoned silently instead of being nacked
		// against a lease another worker now holds.
		job.cancel(ErrLeaseLost)
	}

	if lost > 0 {
		p.log.WarnContext(ctx, "cancelled jobs whose leases were reclaimed",
			"lost", lost, "held", len(ids))
	}
}

// snapshotInFlight copies the current in-flight set.
//
// Copied rather than iterated during the RPC because sync.Map's Range holds no
// consistent view across a network call, and jobs start and finish constantly.
// The snapshot may be stale by the time the response arrives, which is why the
// result handling above tolerates ids that are no longer present.
func (p *Pool) snapshotInFlight() ([]string, map[string]*runningJob) {
	ids := make([]string, 0, p.opts.Concurrency)
	running := make(map[string]*runningJob, p.opts.Concurrency)

	p.inflight.Range(func(k, v any) bool {
		id, ok := k.(string)
		if !ok {
			return true
		}
		job, ok := v.(*runningJob)
		if !ok {
			return true
		}
		ids = append(ids, id)
		running[id] = job
		return true
	})

	return ids, running
}
