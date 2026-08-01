package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// Operations driven by the background loops. Each is safe to run concurrently
// with itself, because the leader lock that gates them cannot actually
// guarantee single-writer execution -- see package leader for why.

// ActiveQueues lists queues known to Redis.
//
// Read from the registry set rather than from Postgres so a tick costs one
// Redis round trip instead of a database query. The set is written by every
// enqueue and requeue, so it converges on the truth; the reconciler seeds it
// from Postgres at boot to cover queues that existed before this Redis did.
func (b *Broker) ActiveQueues(ctx context.Context) ([]string, error) {
	queues, err := b.rdb.SMembers(ctx, Registry()).Result()
	if err != nil {
		return nil, fmt.Errorf("list active queues: %w", err)
	}
	return queues, nil
}

// RegisterQueue adds a queue to the registry so background loops sweep it even
// before its first enqueue.
func (b *Broker) RegisterQueue(ctx context.Context, queue string) error {
	return b.rdb.SAdd(ctx, Registry(), queue).Err()
}

// Depth is the live per-state size of one queue.
type Depth struct {
	Queue     string
	Ready     int64
	Scheduled int64
	// Running is the lease-set cardinality, which is the live concurrency.
	Running int64
}

// Total returns all outstanding work in the queue.
func (d Depth) Total() int64 { return d.Ready + d.Scheduled + d.Running }

// Depth reads live counts for one queue.
func (b *Broker) Depth(ctx context.Context, queue string) (Depth, error) {
	keys := KeysFor(queue)

	// Pipelined: three round trips become one. Called for every queue on every
	// metrics scrape, so the saving compounds.
	pipe := b.rdb.Pipeline()
	ready := pipe.ZCard(ctx, keys.Ready())
	scheduled := pipe.ZCard(ctx, keys.Scheduled())
	running := pipe.ZCard(ctx, keys.Leases())

	if _, err := pipe.Exec(ctx); err != nil {
		return Depth{}, fmt.Errorf("read depth for %s: %w", queue, err)
	}

	return Depth{
		Queue:     queue,
		Ready:     ready.Val(),
		Scheduled: scheduled.Val(),
		Running:   running.Val(),
	}, nil
}

// Depths reads live counts for many queues.
func (b *Broker) Depths(ctx context.Context, queues []string) ([]Depth, error) {
	out := make([]Depth, 0, len(queues))
	for _, q := range queues {
		d, err := b.Depth(ctx, q)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// --- promotion ------------------------------------------------------------

// PromoteResult reports one promotion tick.
type PromoteResult struct {
	Promoted int64
	// Remaining is how many jobs are still due after this batch. Non-zero means
	// the caller should tick again immediately rather than sleeping, so a large
	// backlog drains at full speed instead of one batch per interval.
	Remaining int64
}

// Promote moves scheduled jobs whose run_at has arrived into the ready set.
func (b *Broker) Promote(ctx context.Context, queue string, limit int) (PromoteResult, error) {
	keys := KeysFor(queue)

	res, err := b.scripts.promote.Run(ctx, b.rdb,
		[]string{keys.Scheduled(), keys.Ready()},
		b.now().UnixMilli(),
		limit,
		keys.JobPrefix(),
		jobtypes.PriorityDefault,
	).Slice()
	if err != nil {
		return PromoteResult{}, fmt.Errorf("promote on %s: %w", queue, err)
	}

	var out PromoteResult
	if len(res) >= 1 {
		out.Promoted = toInt64(res[0])
	}
	if len(res) >= 2 {
		out.Remaining = toInt64(res[1])
	}

	// Postgres still shows these as 'scheduled'. Correcting each row would put
	// a write behind every promotion, and the distinction between "scheduled
	// for a time that has passed" and "pending" is invisible to every consumer:
	// the dashboard renders both as waiting, and the next dequeue sets the row
	// to 'running' anyway. Left deliberately.
	return out, nil
}

// --- reaping --------------------------------------------------------------

// ReapResult reports one reap tick.
type ReapResult struct {
	// Requeued jobs whose worker died and which have retries left.
	Requeued int64
	// DeadLettered jobs that ran out of attempts while a worker held them.
	DeadLettered int64
	Remaining    int64
}

// Reap reclaims leases whose holders stopped heartbeating.
//
// This is the mechanism behind the system's central claim: kill a worker
// mid-job and nothing is lost.
//
// Reclaimed jobs go back to the ready set immediately, with no backoff. A lease
// expiry is an infrastructure event -- a container was killed, a host went away
// -- not a signal that the job itself is failing, so making it wait would add
// latency for no protective benefit. The guard against a genuinely poisonous
// job that kills every worker it touches is max_attempts, which still applies:
// such a job burns its attempts quickly and dead-letters, which is exactly the
// desired outcome and far better than a slow crash loop.
func (b *Broker) Reap(ctx context.Context, queue string, limit int) (ReapResult, error) {
	keys := KeysFor(queue)

	raw, err := b.scripts.reap.Run(ctx, b.rdb,
		[]string{keys.Leases(), keys.Inflight()},
		b.now().UnixMilli(),
		limit,
	).Slice()
	if err != nil {
		return ReapResult{}, fmt.Errorf("reap on %s: %w", queue, err)
	}

	ids, workers, remaining := parseReapResult(raw)
	out := ReapResult{Remaining: remaining}
	if len(ids) == 0 {
		return out, nil
	}

	workerFor := make(map[string]string, len(ids))
	for i, id := range ids {
		if i < len(workers) {
			workerFor[id] = workers[i]
		}
	}

	// Only rows still marked 'running' are reaped. A job that finished between
	// the lease expiring and this query is already terminal, and resurrecting
	// it would run completed work a second time.
	jobs, err := b.store.LoadForReap(ctx, ids)
	if err != nil {
		return out, err
	}

	attempts := make([]*store.NewAttempt, 0, len(jobs))
	now := b.now()

	for _, job := range jobs {
		workerID := workerFor[job.ID]
		if workerID == "" {
			workerID = "unknown"
		}

		// Recorded as lease_expired rather than failed. Keeping the two
		// distinct is what makes "how often are workers dying?" answerable
		// separately from "how buggy are the handlers?".
		attempts = append(attempts, &store.NewAttempt{
			JobID:    job.ID,
			Attempt:  job.Attempt,
			WorkerID: workerID,
			Outcome:  jobtypes.OutcomeLeaseExpired,
			Error:    "worker stopped heartbeating; lease expired",
		})

		if job.Attempt >= job.MaxAttempts {
			if err := b.store.MarkDead(ctx, job.ID, fmt.Sprintf(
				"lease expired on attempt %d of %d (worker %s)",
				job.Attempt, job.MaxAttempts, workerID)); err != nil {
				b.log.ErrorContext(ctx, "reap: could not dead-letter job",
					"job_id", job.ID, "error", err)
				continue
			}
			out.DeadLettered++
			continue
		}

		envelope := job.Envelope()
		envelope.Attempt = job.Attempt + 1

		if err := b.requeue(ctx, job.Queue, job.ID, envelope, targetReady,
			jobtypes.PriorityScore(job.Priority, job.EnqueuedAt)); err != nil {
			b.log.ErrorContext(ctx, "reap: could not requeue job",
				"job_id", job.ID, "error", err)
			continue
		}

		if err := b.store.MarkRetrying(ctx, job.ID, now,
			"lease expired; requeued by reaper"); err != nil {
			b.log.WarnContext(ctx, "reap: requeued in redis but row not updated",
				"job_id", job.ID, "error", err)
		}
		out.Requeued++
	}

	if err := b.store.RecordAttempts(ctx, attempts); err != nil {
		b.log.WarnContext(ctx, "reap: could not record attempt history",
			"count", len(attempts), "error", err)
	}

	return out, nil
}

func parseReapResult(raw []any) (ids, workers []string, remaining int64) {
	if len(raw) >= 1 {
		if list, ok := raw[0].([]any); ok {
			ids = make([]string, len(list))
			for i, v := range list {
				ids[i] = toString(v)
			}
		}
	}
	if len(raw) >= 2 {
		if list, ok := raw[1].([]any); ok {
			workers = make([]string, len(list))
			for i, v := range list {
				workers[i] = toString(v)
			}
		}
	}
	if len(raw) >= 3 {
		remaining = toInt64(raw[2])
	}
	return ids, workers, remaining
}

// --- reconciliation -------------------------------------------------------

// ReconcileResult reports one reconciliation pass.
type ReconcileResult struct {
	Scanned  int
	Restored int
	// Stranded jobs were marked 'running' with an expired lease and no Redis
	// presence -- the shape left behind when the service died between clearing
	// a lease and requeueing the job.
	Stranded int
}

// Reconcile rebuilds Redis state from Postgres.
//
// This is what lets Redis be treated as a rebuildable cache rather than a
// system of record. Run at boot and periodically thereafter, it finds durable
// jobs that should be dispatchable but are absent from Redis and puts them
// back. A `redis-cli FLUSHALL` therefore costs throughput, not work.
//
// It also closes the one real gap in the reap path. reap.lua clears a lease and
// returns the ids; Go then requeues them. If the process dies in between, the
// job is in neither the lease set nor the ready set while Postgres still says
// 'running'. Nothing else would ever notice -- there is no lease left to
// expire. Scanning on lease expiry, not just on status, is what catches it.
func (b *Broker) Reconcile(ctx context.Context, batch int) (ReconcileResult, error) {
	var out ReconcileResult

	// Seed the registry from Postgres so loops sweep queues that predate this
	// Redis instance.
	names, err := b.store.QueueNames(ctx)
	if err != nil {
		return out, err
	}
	for _, name := range names {
		if err := b.RegisterQueue(ctx, name); err != nil {
			return out, err
		}
	}

	jobs, err := b.store.LoadForReconcile(ctx, b.now(), batch)
	if err != nil {
		return out, err
	}
	out.Scanned = len(jobs)

	for _, job := range jobs {
		keys := KeysFor(job.Queue)

		present, err := b.isPresent(ctx, keys, job)
		if err != nil {
			return out, err
		}
		if present {
			continue
		}

		envelope := job.Envelope()

		target := targetReady
		score := jobtypes.PriorityScore(job.Priority, job.EnqueuedAt)

		switch job.Status {
		case jobtypes.StatusScheduled:
			if job.RunAt.After(b.now()) {
				target = targetScheduled
				score = float64(job.RunAt.UnixMilli())
			}
			envelope.Attempt = job.Attempt + 1

		case jobtypes.StatusRunning:
			// Stranded. Its worker is gone and its lease is already gone, so
			// nothing else will ever recover it.
			out.Stranded++
			envelope.Attempt = job.Attempt + 1

			if job.Attempt >= job.MaxAttempts {
				if err := b.store.MarkDead(ctx, job.ID,
					"stranded: lease lost with no worker and no retries left"); err != nil {
					b.log.ErrorContext(ctx, "reconcile: could not dead-letter stranded job",
						"job_id", job.ID, "error", err)
				}
				continue
			}
			if err := b.store.MarkRetrying(ctx, job.ID, b.now(),
				"stranded: recovered by reconciler"); err != nil {
				b.log.WarnContext(ctx, "reconcile: could not update stranded row",
					"job_id", job.ID, "error", err)
			}

		default: // pending
			envelope.Attempt = job.Attempt + 1
		}

		if err := b.requeue(ctx, job.Queue, job.ID, envelope, target, score); err != nil {
			b.log.ErrorContext(ctx, "reconcile: could not restore job",
				"job_id", job.ID, "queue", job.Queue, "error", err)
			continue
		}
		out.Restored++
	}

	if out.Restored > 0 || out.Stranded > 0 {
		b.log.InfoContext(ctx, "reconcile: restored jobs missing from redis",
			"scanned", out.Scanned, "restored", out.Restored, "stranded", out.Stranded)
	}

	return out, nil
}

// isPresent reports whether Redis already knows about a job, in any state.
//
// All three lookups go in one pipeline. Exec returns redis.Nil when any command
// in the batch missed, which is the ordinary case here rather than a failure --
// a job is expected to be in at most one of the three sets. So the pipeline
// error is ignored and each command's own result is inspected instead. Any
// genuine connection failure surfaces identically on all three and is reported.
func (b *Broker) isPresent(ctx context.Context, keys Keys, job *store.Job) (bool, error) {
	pipe := b.rdb.Pipeline()
	cmds := []*redis.FloatCmd{
		pipe.ZScore(ctx, keys.Ready(), job.ID),
		pipe.ZScore(ctx, keys.Scheduled(), job.ID),
		pipe.ZScore(ctx, keys.Leases(), job.ID),
	}
	_, _ = pipe.Exec(ctx)

	for _, cmd := range cmds {
		switch err := cmd.Err(); {
		case err == nil:
			return true, nil
		case errors.Is(err, redis.Nil):
			// Absent from this set; keep checking the others.
		default:
			return false, fmt.Errorf("check redis presence of job %s: %w", job.ID, err)
		}
	}
	return false, nil
}

// --- rollups --------------------------------------------------------------

// Rollup aggregates recently finished jobs into per-minute buckets.
//
// The dashboard's charts read only from queue_stats_minute, so plotting a day
// of throughput never scans the jobs table. That is the difference between a
// dashboard that stays responsive at ten million rows and one that does not.
//
// Only the window length crosses this boundary, never a timestamp: the
// aggregated columns are all database-written, so Postgres resolves the window
// against its own clock. See store.RollupMinutes.
func (b *Broker) Rollup(ctx context.Context, window time.Duration) (int64, error) {
	return b.store.RollupMinutes(ctx, window)
}
