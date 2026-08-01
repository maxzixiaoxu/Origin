// Package broker is the queue's core. It owns the ordering between Postgres
// and Redis, and every rule about when a job may move between states.
//
// The one invariant everything else rests on:
//
//	Postgres is written before Redis on the way in, and Redis is the arbiter
//	on the way out.
//
// Enqueue makes the job durable first, then makes it dispatchable. If the
// second step fails the job simply waits for the reconciler -- a delay. The
// reverse order would allow a dispatchable job with no durable record, which is
// unrecoverable: a worker leases something that does not exist, and no operator
// can ever explain where it came from.
//
// On the way out, Redis decides. Only the lease set can answer "who owns this
// job right now?", and it answers atomically. Postgres learns about the
// decision afterwards.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/maxzixiaoxu/Origin/pkg/backoff"
	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// Errors callers are expected to branch on.
var (
	// ErrLeaseLost means the caller no longer owns the job: its lease expired
	// and was reclaimed, or another worker now holds it. The correct response
	// is to abandon the work, not to retry the call.
	ErrLeaseLost = errors.New("lease lost")

	// ErrQueuePaused is returned by operations that cannot proceed while a
	// queue is paused.
	ErrQueuePaused = errors.New("queue is paused")

	// ErrNotFound mirrors store.ErrNotFound at the broker boundary.
	ErrNotFound = store.ErrNotFound
)

// envelopeTTL bounds how long a cached envelope survives in Redis.
//
// A backstop against leaked keys, not a correctness mechanism. If it expires
// while a job is still queued, dequeue returns an empty payload and the broker
// rehydrates from Postgres, so the job still runs -- just a microsecond slower.
// Seven days comfortably exceeds any plausible scheduled delay while still
// bounding the damage from a key that somehow escapes cleanup.
const envelopeTTL = 7 * 24 * time.Hour

// Options configures a Broker.
type Options struct {
	Redis *redis.Client
	Store *store.Store
	Log   *slog.Logger

	// MaxDequeueBatch caps how many jobs a single Dequeue may return,
	// regardless of what the caller asks for.
	MaxDequeueBatch int

	// QueueConfigTTL is how long queue configuration is cached in memory.
	QueueConfigTTL time.Duration

	// Now is injectable so tests can drive lease expiry without sleeping.
	Now func() time.Time
}

// Broker coordinates Redis and Postgres.
type Broker struct {
	rdb     *redis.Client
	store   *store.Store
	scripts *scriptSet
	log     *slog.Logger

	queues *queueCache

	maxDequeueBatch int
	now             func() time.Time
}

// New builds a Broker and preloads its Lua scripts.
func New(ctx context.Context, opts Options) (*Broker, error) {
	if opts.Redis == nil {
		return nil, errors.New("broker: Redis client is required")
	}
	if opts.Store == nil {
		return nil, errors.New("broker: Store is required")
	}

	scripts, err := loadScripts()
	if err != nil {
		return nil, err
	}
	if err := scripts.preload(ctx, opts.Redis); err != nil {
		return nil, err
	}

	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	maxBatch := opts.MaxDequeueBatch
	if maxBatch < 1 {
		maxBatch = 64
	}
	ttl := opts.QueueConfigTTL
	if ttl <= 0 {
		ttl = time.Second
	}

	return &Broker{
		rdb:             opts.Redis,
		store:           opts.Store,
		scripts:         scripts,
		log:             log,
		queues:          newQueueCache(opts.Store, ttl, now),
		maxDequeueBatch: maxBatch,
		now:             now,
	}, nil
}

// Store exposes the durable layer for read-only admin queries.
func (b *Broker) Store() *store.Store { return b.store }

// Redis exposes the client for health checks and background loops.
func (b *Broker) Redis() *redis.Client { return b.rdb }

// Now returns the broker's clock.
func (b *Broker) Now() time.Time { return b.now() }

// InvalidateQueue drops a queue's cached configuration, so a change made
// through the admin API takes effect on the next dequeue instead of after the
// cache TTL.
func (b *Broker) InvalidateQueue(name string) { b.queues.invalidate(name) }

// --- enqueue --------------------------------------------------------------

// EnqueueRequest describes a job to submit.
type EnqueueRequest struct {
	Queue   string
	Type    string
	Payload []byte

	// Priority is clamped to 0-9. Nil uses the default.
	Priority *int
	// MaxAttempts nil inherits the queue's setting.
	MaxAttempts *int
	// IdempotencyKey, when set, makes resubmission a no-op.
	IdempotencyKey string
	// RunAt in the future starts the job SCHEDULED.
	RunAt   time.Time
	TraceID string
}

// EnqueueResult reports what happened.
type EnqueueResult struct {
	JobID  string
	Status jobtypes.Status
	// Deduplicated is true when an idempotency key matched and no new job was
	// created. Surfaced rather than hidden so a caller can tell "accepted" from
	// "you already sent this".
	Deduplicated bool
	Depth        int64
}

// Enqueue submits one job.
func (b *Broker) Enqueue(ctx context.Context, req EnqueueRequest) (*EnqueueResult, error) {
	if err := ValidateQueueName(req.Queue); err != nil {
		return nil, err
	}
	if req.Type == "" {
		return nil, errors.New("job type is required")
	}

	cfg, err := b.queues.get(ctx, req.Queue)
	if err != nil {
		return nil, err
	}

	now := b.now()
	runAt := req.RunAt
	if runAt.IsZero() {
		runAt = now
	}

	priority := jobtypes.PriorityDefault
	if req.Priority != nil {
		priority = jobtypes.ClampPriority(*req.Priority)
	}
	maxAttempts := cfg.MaxAttempts
	if req.MaxAttempts != nil && *req.MaxAttempts > 0 {
		maxAttempts = *req.MaxAttempts
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	// The broker owns the clock, so it -- not the store -- decides whether this
	// job starts queued or scheduled. Both the durable row and the Redis
	// placement below are derived from this one comparison, which is what keeps
	// them from ever disagreeing.
	status := jobtypes.StatusPending
	if runAt.After(now) {
		status = jobtypes.StatusScheduled
	}

	// Step 1: durability. Nothing becomes dispatchable before this succeeds.
	job, created, err := b.store.CreateJob(ctx, &store.NewJob{
		ID:             uuid.NewString(),
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        payload,
		Priority:       priority,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: req.IdempotencyKey,
		RunAt:          runAt,
		TraceID:        req.TraceID,
		Status:         status,
	})
	if err != nil {
		return nil, err
	}

	if !created {
		// Already submitted. Do not touch Redis: the original job is wherever
		// it belongs, possibly already running, and re-adding it to the ready
		// set would create a genuine duplicate execution -- precisely what the
		// idempotency key exists to prevent.
		return &EnqueueResult{
			JobID:        job.ID,
			Status:       job.Status,
			Deduplicated: true,
		}, nil
	}

	// Step 2: dispatchability.
	envelope := jobtypes.Envelope{
		ID:          job.ID,
		Queue:       job.Queue,
		Type:        job.Type,
		Payload:     job.Payload,
		Attempt:     1, // the attempt a worker taking this job will perform
		MaxAttempts: job.MaxAttempts,
		Priority:    job.Priority,
		EnqueuedAt:  job.EnqueuedAt,
		TraceID:     req.TraceID,
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}

	keys := KeysFor(req.Queue)
	res, err := b.scripts.enqueue.Run(ctx, b.rdb,
		[]string{keys.Ready(), keys.Scheduled(), keys.Job(job.ID), Registry()},
		job.ID,
		now.UnixMilli(),
		runAt.UnixMilli(),
		jobtypes.PriorityScore(priority, job.EnqueuedAt),
		string(encoded),
		envelopeTTL.Milliseconds(),
		req.Queue,
	).Slice()
	if err != nil {
		// The job is durable. The reconciler will place it in Redis, so this is
		// a latency event rather than lost work -- log it and tell the caller
		// the job was accepted.
		b.log.ErrorContext(ctx, "enqueue: job is durable but not yet dispatchable",
			"job_id", job.ID, "queue", req.Queue, "error", err)
		return &EnqueueResult{JobID: job.ID, Status: job.Status}, nil
	}

	state, depth := parseEnqueueResult(res)

	// Go and Lua evaluate the same comparison against the same instant, so the
	// script's placement must match the status written to Postgres. If it ever
	// does not, the durable row and the Redis set disagree about what this job
	// is -- worth a loud log rather than silently trusting one of them.
	scriptSaysScheduled := state == 0
	if scriptSaysScheduled != (status == jobtypes.StatusScheduled) {
		b.log.ErrorContext(ctx, "enqueue: redis placement disagrees with stored status",
			"job_id", job.ID, "queue", req.Queue,
			"stored_status", status, "script_scheduled", scriptSaysScheduled)
	}

	return &EnqueueResult{JobID: job.ID, Status: status, Depth: depth}, nil
}

func parseEnqueueResult(res []any) (state, depth int64) {
	if len(res) >= 1 {
		state, _ = res[0].(int64)
	}
	if len(res) >= 2 {
		depth, _ = res[1].(int64)
	}
	return state, depth
}

// --- lease renewal --------------------------------------------------------

// LeaseResult is the per-job verdict from ExtendLeases.
type LeaseResult struct {
	JobID     string
	Extended  bool
	ExpiresAt time.Time
}

// ExtendLeases renews every lease a worker holds, grouped by queue.
//
// Jobs are grouped because the lease set is per-queue and a script must not
// span queues -- each queue's keys hash to a different Redis Cluster slot. One
// script call per queue per heartbeat is still flat in the number of jobs,
// which is the property that matters.
func (b *Broker) ExtendLeases(
	ctx context.Context,
	workerID string,
	jobIDs []string,
) ([]LeaseResult, error) {
	if len(jobIDs) == 0 {
		return nil, nil
	}

	// Resolving each job to its queue costs one batched Postgres read. Worth it
	// versus making the worker track queue membership: the worker would then
	// have to keep that mapping correct across retries and requeues, and any
	// drift would silently renew the wrong lease set.
	jobs, err := b.store.GetJobs(ctx, jobIDs)
	if err != nil {
		return nil, err
	}

	byQueue := make(map[string][]string)
	results := make([]LeaseResult, 0, len(jobIDs))

	for _, id := range jobIDs {
		job, ok := jobs[id]
		if !ok {
			// Unknown job: report it lost so the worker cancels rather than
			// continuing to run something with no durable record.
			results = append(results, LeaseResult{JobID: id, Extended: false})
			continue
		}
		byQueue[job.Queue] = append(byQueue[job.Queue], id)
	}

	now := b.now()

	for queue, ids := range byQueue {
		cfg, err := b.queues.get(ctx, queue)
		if err != nil {
			return nil, err
		}
		expiresAt := now.Add(cfg.VisibilityTimeout)
		keys := KeysFor(queue)

		args := make([]any, 0, len(ids)+2)
		args = append(args, workerID, expiresAt.UnixMilli())
		for _, id := range ids {
			args = append(args, id)
		}

		raw, err := b.scripts.extendLeases.Run(ctx, b.rdb,
			[]string{keys.Leases(), keys.Inflight()}, args...).Slice()
		if err != nil {
			return nil, fmt.Errorf("extend leases on queue %s: %w", queue, err)
		}

		for i, id := range ids {
			extended := i < len(raw) && toInt64(raw[i]) == 1
			r := LeaseResult{JobID: id, Extended: extended}
			if extended {
				r.ExpiresAt = expiresAt
			}
			results = append(results, r)
		}
	}

	return results, nil
}

// --- cancellation ---------------------------------------------------------

// Cancel removes a job from circulation.
//
// A running job cannot be stopped directly -- there is no way to reach into
// another process. Deleting its lease is enough: the worker's next heartbeat
// finds the lease gone, and the pool cancels that job's context. The handler
// unwinds through ordinary Go cancellation within one heartbeat interval.
func (b *Broker) Cancel(ctx context.Context, jobID string) (bool, error) {
	job, err := b.store.GetJob(ctx, jobID)
	if err != nil {
		return false, err
	}
	if job.Status.Terminal() {
		return false, nil
	}

	keys := KeysFor(job.Queue)
	if _, err := b.scripts.cancel.Run(ctx, b.rdb,
		[]string{keys.Ready(), keys.Scheduled(), keys.Leases(),
			keys.Inflight(), keys.Job(jobID)},
		jobID,
	).Slice(); err != nil {
		return false, fmt.Errorf("cancel job %s in redis: %w", jobID, err)
	}

	return b.store.MarkCancelled(ctx, jobID)
}

// Retry returns a dead, failed, or cancelled job to the ready set. This is what
// dead-letter replay calls.
func (b *Broker) Retry(ctx context.Context, jobID string, resetAttempts bool) error {
	job, err := b.store.MarkPendingRetry(ctx, jobID, resetAttempts)
	if err != nil {
		return err
	}

	attempt := job.Attempt + 1
	envelope := jobtypes.Envelope{
		ID:          job.ID,
		Queue:       job.Queue,
		Type:        job.Type,
		Payload:     job.Payload,
		Attempt:     attempt,
		MaxAttempts: job.MaxAttempts,
		Priority:    job.Priority,
		EnqueuedAt:  job.EnqueuedAt,
	}
	if job.TraceID != nil {
		envelope.TraceID = *job.TraceID
	}

	return b.requeue(ctx, job.Queue, job.ID, envelope, targetReady,
		jobtypes.PriorityScore(job.Priority, b.now()))
}

// requeue places a job back into a set without holding its lease. Shared by
// dead-letter replay, the reaper, and the reconciler.
func (b *Broker) requeue(
	ctx context.Context,
	queue, jobID string,
	envelope jobtypes.Envelope,
	target int64,
	score float64,
) error {
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}

	keys := KeysFor(queue)
	if err := b.scripts.requeue.Run(ctx, b.rdb,
		[]string{keys.Ready(), keys.Scheduled(), keys.Job(jobID), Registry()},
		jobID, target, score, string(encoded), envelopeTTL.Milliseconds(), queue,
	).Err(); err != nil {
		return fmt.Errorf("requeue job %s: %w", jobID, err)
	}
	return nil
}

// backoffFor builds the retry policy for a queue from its stored settings.
func backoffFor(cfg *store.QueueConfig) backoff.Policy {
	return backoff.Policy{
		Base:       cfg.BackoffBase,
		Cap:        cfg.BackoffCap,
		Multiplier: backoff.DefaultMultiplier,
	}
}

// toInt64 coerces the untyped values Redis returns from a Lua table.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// toString coerces an untyped Redis value to a string.
func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return ""
	}
}
