package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// ThrottleReason explains why a Dequeue returned nothing.
//
// Returning a bare empty list would force every worker to guess how long to
// wait. A worker that knows it was rate-limited for 180ms sleeps exactly 180ms;
// one that only sees "empty" either spins against Redis or waits a fixed
// interval and adds latency that was never necessary.
type ThrottleReason int

const (
	ThrottleNone ThrottleReason = iota
	// ThrottleEmpty: no work available. Poll again after the idle interval.
	ThrottleEmpty
	// ThrottleRateLimit: token bucket dry. Honour RetryAfter exactly.
	ThrottleRateLimit
	// ThrottleConcurrency: the queue is at its in-flight ceiling. More workers
	// will not help; wait for running jobs to finish.
	ThrottleConcurrency
	// ThrottlePaused: an operator paused the queue. Back off hard -- this is
	// not transient and will not clear on its own.
	ThrottlePaused
)

func (t ThrottleReason) String() string {
	switch t {
	case ThrottleNone:
		return "none"
	case ThrottleEmpty:
		return "empty"
	case ThrottleRateLimit:
		return "rate_limit"
	case ThrottleConcurrency:
		return "concurrency"
	case ThrottlePaused:
		return "paused"
	default:
		return "unknown"
	}
}

// DequeueRequest asks for work.
type DequeueRequest struct {
	// Queues are tried in order, so one worker can serve a high-priority queue
	// first and fall back to a bulk queue when it is empty.
	Queues   []string
	MaxJobs  int
	WorkerID string
}

// DequeueResult carries leased jobs, or the reason there were none.
type DequeueResult struct {
	Jobs           []jobtypes.Envelope
	ThrottleReason ThrottleReason
	RetryAfter     time.Duration
	// Queue names the queue the jobs came from; empty when none were leased.
	Queue string
}

// Dequeue leases up to MaxJobs for a worker.
//
// Queues are attempted in order and the first one to yield work wins. Blending
// results across queues would defeat the point of listing them in priority
// order -- a worker asking for 16 jobs should get 16 from the critical queue if
// it has them, not 4 critical and 12 bulk.
func (b *Broker) Dequeue(ctx context.Context, req DequeueRequest) (*DequeueResult, error) {
	if req.WorkerID == "" {
		return nil, fmt.Errorf("worker id is required")
	}
	if len(req.Queues) == 0 {
		return nil, fmt.Errorf("at least one queue is required")
	}

	maxJobs := req.MaxJobs
	if maxJobs < 1 {
		maxJobs = 1
	}
	if maxJobs > b.maxDequeueBatch {
		maxJobs = b.maxDequeueBatch
	}

	// Remember the least-discouraging reason across all queues. If one queue is
	// paused and another is merely empty, the worker should hear "empty" and
	// keep its normal poll cadence rather than backing off hard.
	best := ThrottlePaused
	var bestRetry time.Duration

	for _, queue := range req.Queues {
		if err := ValidateQueueName(queue); err != nil {
			return nil, err
		}

		res, err := b.dequeueFrom(ctx, queue, maxJobs, req.WorkerID)
		if err != nil {
			return nil, err
		}
		if len(res.Jobs) > 0 {
			return res, nil
		}

		if res.ThrottleReason < best {
			best = res.ThrottleReason
			bestRetry = res.RetryAfter
		} else if res.ThrottleReason == best && res.RetryAfter < bestRetry {
			// Prefer the shortest wait among equally-severe reasons.
			bestRetry = res.RetryAfter
		}
	}

	return &DequeueResult{ThrottleReason: best, RetryAfter: bestRetry}, nil
}

// dequeueFrom runs the dequeue script against one queue.
func (b *Broker) dequeueFrom(
	ctx context.Context,
	queue string,
	maxJobs int,
	workerID string,
) (*DequeueResult, error) {
	cfg, err := b.queues.get(ctx, queue)
	if err != nil {
		return nil, err
	}

	paused := 0
	if cfg.Paused {
		paused = 1
	}

	now := b.now()
	keys := KeysFor(queue)

	raw, err := b.scripts.dequeue.Run(ctx, b.rdb,
		[]string{keys.Ready(), keys.Leases(), keys.Inflight(), keys.Rate()},
		now.UnixMilli(),
		maxJobs,
		workerID,
		cfg.VisibilityTimeout.Milliseconds(),
		cfg.MaxConcurrency,
		cfg.RatePerSec(),
		cfg.Burst(),
		paused,
		keys.JobPrefix(),
	).Slice()
	if err != nil {
		return nil, fmt.Errorf("dequeue from %s: %w", queue, err)
	}

	reason, retryAfterMS, expiresAtMS, ids, payloads := parseDequeueResult(raw)

	if len(ids) == 0 {
		return &DequeueResult{
			ThrottleReason: throttleFromCode(reason),
			RetryAfter:     time.Duration(retryAfterMS) * time.Millisecond,
		}, nil
	}

	leaseExpiresAt := time.UnixMilli(expiresAtMS)

	envelopes, err := b.hydrate(ctx, queue, ids, payloads, leaseExpiresAt)
	if err != nil {
		return nil, err
	}

	// Record the lease durably. This happens after Redis has already committed
	// the lease, which is the correct order: Redis is the arbiter of ownership,
	// and its decision must not wait on a Postgres write. If this update fails,
	// the job still runs and still acks -- the row is briefly stale, and the
	// reconciler repairs it.
	records := make([]store.LeaseRecord, len(envelopes))
	for i, e := range envelopes {
		records[i] = store.LeaseRecord{JobID: e.ID, Attempt: e.Attempt}
	}
	if err := b.store.MarkRunning(ctx, records, workerID, leaseExpiresAt); err != nil {
		b.log.ErrorContext(ctx, "dequeue: lease granted but not recorded durably",
			"queue", queue, "count", len(records), "worker_id", workerID, "error", err)
	}

	return &DequeueResult{Jobs: envelopes, Queue: queue}, nil
}

// hydrate turns the raw script output into envelopes, refilling from Postgres
// any whose cached copy was missing.
//
// A missing envelope is expected, not exceptional: it is what a flushed Redis,
// or an expired TTL on a long-scheduled job, looks like from here. Recovering
// silently is what makes losing Redis a latency event rather than a data-loss
// event, and it is the behaviour the FLUSHALL test in the README exercises.
func (b *Broker) hydrate(
	ctx context.Context,
	queue string,
	ids []string,
	payloads []string,
	leaseExpiresAt time.Time,
) ([]jobtypes.Envelope, error) {
	envelopes := make([]jobtypes.Envelope, 0, len(ids))
	var missing []string

	for i, id := range ids {
		if i >= len(payloads) || payloads[i] == "" {
			missing = append(missing, id)
			continue
		}

		var e jobtypes.Envelope
		if err := json.Unmarshal([]byte(payloads[i]), &e); err != nil {
			// A corrupt envelope is recoverable the same way a missing one is.
			b.log.WarnContext(ctx, "dequeue: unreadable cached envelope, rehydrating",
				"job_id", id, "queue", queue, "error", err)
			missing = append(missing, id)
			continue
		}

		e.LeaseExpiresAt = leaseExpiresAt
		envelopes = append(envelopes, e)
	}

	if len(missing) == 0 {
		return envelopes, nil
	}

	b.log.InfoContext(ctx, "dequeue: rehydrating envelopes from postgres",
		"queue", queue, "count", len(missing))

	jobs, err := b.store.GetJobs(ctx, missing)
	if err != nil {
		return nil, fmt.Errorf("rehydrate %d envelopes: %w", len(missing), err)
	}

	for _, id := range missing {
		job, ok := jobs[id]
		if !ok {
			// In Redis but not Postgres. The durable record is authoritative,
			// so this id refers to nothing real -- drop it rather than handing
			// a worker a job that does not exist. Its lease will expire and the
			// reaper will find no row to act on.
			b.log.WarnContext(ctx, "dequeue: leased id has no durable record, dropping",
				"job_id", id, "queue", queue)
			continue
		}

		e := job.Envelope()
		// jobs.attempt counts attempts already started; the worker is about to
		// perform the next one.
		e.Attempt = job.Attempt + 1
		e.LeaseExpiresAt = leaseExpiresAt
		envelopes = append(envelopes, e)
	}

	return envelopes, nil
}

// parseDequeueResult decodes { reason, retry_after_ms, expires_at_ms, ids,
// envelopes } from the Lua reply.
func parseDequeueResult(raw []any) (
	reason, retryAfterMS, expiresAtMS int64,
	ids, payloads []string,
) {
	if len(raw) >= 1 {
		reason = toInt64(raw[0])
	}
	if len(raw) >= 2 {
		retryAfterMS = toInt64(raw[1])
	}
	if len(raw) >= 3 {
		expiresAtMS = toInt64(raw[2])
	}
	if len(raw) >= 4 {
		if list, ok := raw[3].([]any); ok {
			ids = make([]string, len(list))
			for i, v := range list {
				ids[i] = toString(v)
			}
		}
	}
	if len(raw) >= 5 {
		if list, ok := raw[4].([]any); ok {
			payloads = make([]string, len(list))
			for i, v := range list {
				payloads[i] = toString(v)
			}
		}
	}
	return reason, retryAfterMS, expiresAtMS, ids, payloads
}

func throttleFromCode(code int64) ThrottleReason {
	switch code {
	case reasonOK:
		return ThrottleNone
	case reasonEmpty:
		return ThrottleEmpty
	case reasonRateLimit:
		return ThrottleRateLimit
	case reasonConcurrency:
		return ThrottleConcurrency
	case reasonPaused:
		return ThrottlePaused
	default:
		return ThrottleEmpty
	}
}
