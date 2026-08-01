package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
)

// Attempt counting, stated once because it is subtle and three components
// depend on agreeing about it:
//
//   jobs.attempt (Postgres) counts attempts that have STARTED. It is 0 for a
//   job that has never been dequeued, and is set to N when the Nth execution
//   begins.
//
//   Envelope.Attempt (Redis, and what the worker sees) is the number of the
//   execution the worker is ABOUT to perform. It is 1 on a freshly enqueued
//   job, and is incremented when a retry is scheduled.
//
// So a worker holding Attempt=3 is running the third try, jobs.attempt reads 3
// while it runs, and the job is on its final try when Attempt == max_attempts.
// Keeping the "about to run" number in the envelope is what lets a handler ask
// IsFinalAttempt() and log accordingly, without a second lookup.

const jobColumns = `
    id, queue_name, job_type, payload, status, priority,
    attempt, max_attempts, idempotency_key, run_at,
    lease_expires_at, locked_by, last_error, result, trace_id,
    enqueued_at, started_at, finished_at, created_at, updated_at`

func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(
		&j.ID, &j.Queue, &j.Type, &j.Payload, &j.Status, &j.Priority,
		&j.Attempt, &j.MaxAttempts, &j.IdempotencyKey, &j.RunAt,
		&j.LeaseExpiresAt, &j.LockedBy, &j.LastError, &j.Result, &j.TraceID,
		&j.EnqueuedAt, &j.StartedAt, &j.FinishedAt, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan job: %w", err)
	}

	// Normalise here, at the single point where jsonb columns enter the
	// program, so no caller has to remember that Postgres and Go format JSON
	// differently. See compactJSON for why byte-identity matters.
	j.Payload = compactJSON(j.Payload)
	j.Result = compactJSON(j.Result)

	return &j, nil
}

// NewJob carries the fields needed to create a job.
type NewJob struct {
	ID             string
	Queue          string
	Type           string
	Payload        []byte
	Priority       int
	MaxAttempts    int
	IdempotencyKey string
	RunAt          time.Time
	TraceID        string

	// Status is supplied by the caller rather than derived here.
	//
	// This layer deliberately makes no time-based decisions. Deciding
	// pending-vs-scheduled requires knowing "now", and the broker already owns
	// a clock -- one that is injectable, so lease expiry and scheduling can be
	// tested without sleeping. Reading time.Now() here would mean the store
	// silently disagreed with the broker whenever the two clocks differed,
	// which is exactly what happened before this field existed: a job scheduled
	// for the future was stored as 'pending', and reconciliation then made it
	// immediately dispatchable because its row did not say 'scheduled'.
	//
	// Keeping the clock in one place is what makes that class of bug
	// impossible rather than merely fixed.
	Status jobtypes.Status
}

// CreateJob inserts a job, or returns the existing one when its idempotency key
// has already been used on this queue.
//
// Deduplication is resolved by the unique index, not by a read-then-write
// check. Two concurrent submissions of the same key -- a client retrying a
// request it thought timed out is the everyday case -- would both pass a
// read-then-write check and create two jobs. Here one insert wins, the other
// hits ON CONFLICT DO NOTHING, returns no row, and reads back the winner. The
// database arbitrates, so there is no window to lose.
//
// Returns the job and whether this call created it.
func (s *Store) CreateJob(ctx context.Context, n *NewJob) (*Job, bool, error) {
	status := n.Status
	if status == "" {
		status = jobtypes.StatusPending
	}
	if !status.Valid() {
		return nil, false, fmt.Errorf("invalid status %q for new job", status)
	}

	const insertSQL = `
        INSERT INTO jobs (
            id, queue_name, job_type, payload, status, priority,
            max_attempts, idempotency_key, run_at, trace_id, enqueued_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
        ON CONFLICT (queue_name, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            DO NOTHING
        RETURNING` + jobColumns

	job, err := scanJob(s.pool.QueryRow(ctx, insertSQL,
		n.ID, n.Queue, n.Type, n.Payload, status, jobtypes.ClampPriority(n.Priority),
		n.MaxAttempts, nullableString(n.IdempotencyKey), n.RunAt,
		nullableString(n.TraceID),
	))
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, false, fmt.Errorf("insert job: %w", err)
	}

	// No row returned means the conflict fired: this key already exists.
	if n.IdempotencyKey == "" {
		// Cannot happen -- a NULL key is outside the partial index predicate,
		// so it can never conflict. Report loudly rather than silently
		// returning a nil job if that assumption is ever broken.
		return nil, false, errors.New(
			"insert produced no row for a job with no idempotency key")
	}

	existing, err := s.GetJobByIdempotencyKey(ctx, n.Queue, n.IdempotencyKey)
	if err != nil {
		return nil, false, fmt.Errorf("load deduplicated job: %w", err)
	}
	return existing, false, nil
}

// GetJob loads one job by id.
func (s *Store) GetJob(ctx context.Context, id string) (*Job, error) {
	return scanJob(s.pool.QueryRow(ctx,
		`SELECT`+jobColumns+` FROM jobs WHERE id = $1`, id))
}

// GetJobByIdempotencyKey loads the job that owns a key on a queue.
func (s *Store) GetJobByIdempotencyKey(ctx context.Context, queue, key string) (*Job, error) {
	return scanJob(s.pool.QueryRow(ctx,
		`SELECT`+jobColumns+` FROM jobs
         WHERE queue_name = $1 AND idempotency_key = $2`, queue, key))
}

// GetJobs loads many jobs in one round trip.
//
// This is the rehydration path: when Redis returns a job id whose cached
// envelope is missing -- expired TTL, or a flushed Redis -- the broker refills
// from here. Batched because a dequeue of 64 jobs that all missed would
// otherwise become 64 sequential queries on the hot path.
func (s *Store) GetJobs(ctx context.Context, ids []string) (map[string]*Job, error) {
	if len(ids) == 0 {
		return map[string]*Job{}, nil
	}

	rows, err := s.pool.Query(ctx,
		`SELECT`+jobColumns+` FROM jobs WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("batch load jobs: %w", err)
	}
	defer rows.Close()

	out := make(map[string]*Job, len(ids))
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out[j.ID] = j
	}
	return out, rows.Err()
}

// LeaseRecord pairs a job with the attempt number its execution will carry.
type LeaseRecord struct {
	JobID   string
	Attempt int
}

// MarkRunning records that a batch of jobs was leased.
//
// One statement for the whole batch via unnest, rather than a loop of updates:
// a 64-job dequeue should cost one round trip, not 64. started_at uses COALESCE
// so it keeps the timestamp of the FIRST attempt across retries, which is what
// makes end-to-end latency measurable separately from per-attempt duration.
func (s *Store) MarkRunning(
	ctx context.Context,
	records []LeaseRecord,
	workerID string,
	leaseExpiresAt time.Time,
) error {
	if len(records) == 0 {
		return nil
	}

	ids := make([]string, len(records))
	attempts := make([]int32, len(records))
	for i, r := range records {
		ids[i] = r.JobID
		attempts[i] = int32(r.Attempt)
	}

	const q = `
        UPDATE jobs j SET
            status           = 'running',
            locked_by        = $3,
            lease_expires_at = $4,
            attempt          = v.attempt,
            started_at       = COALESCE(j.started_at, now())
        FROM unnest($1::uuid[], $2::int[]) AS v(id, attempt)
        WHERE j.id = v.id`

	if _, err := s.pool.Exec(ctx, q, ids, attempts, workerID, leaseExpiresAt); err != nil {
		return fmt.Errorf("mark %d jobs running: %w", len(records), err)
	}
	return nil
}

// MarkSucceeded records terminal success.
//
// The status guard is not redundant. A worker whose lease was reaped can return
// late and try to ack a job another worker has since completed or dead-lettered.
// Redis rejects that ack on ownership, but this guard is the second line: it
// means even a direct call cannot resurrect a terminal job into 'succeeded'.
func (s *Store) MarkSucceeded(ctx context.Context, id string, result []byte) error {
	const q = `
        UPDATE jobs SET
            status           = 'succeeded',
            result           = $2,
            finished_at      = now(),
            lease_expires_at = NULL,
            locked_by        = NULL,
            last_error       = NULL
        WHERE id = $1 AND status = 'running'`

	tag, err := s.pool.Exec(ctx, q, id, result)
	if err != nil {
		return fmt.Errorf("mark job %s succeeded: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkRetrying schedules the next attempt.
func (s *Store) MarkRetrying(ctx context.Context, id string, runAt time.Time, errMsg string) error {
	const q = `
        UPDATE jobs SET
            status           = 'scheduled',
            run_at           = $2,
            last_error       = $3,
            lease_expires_at = NULL,
            locked_by        = NULL
        WHERE id = $1 AND status IN ('running', 'pending')`

	tag, err := s.pool.Exec(ctx, q, id, runAt, nullableString(errMsg))
	if err != nil {
		return fmt.Errorf("mark job %s retrying: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkDead moves a job to the dead-letter state.
func (s *Store) MarkDead(ctx context.Context, id string, errMsg string) error {
	const q = `
        UPDATE jobs SET
            status           = 'dead',
            last_error       = $2,
            finished_at      = now(),
            lease_expires_at = NULL,
            locked_by        = NULL
        WHERE id = $1 AND status NOT IN ('succeeded', 'dead', 'cancelled')`

	tag, err := s.pool.Exec(ctx, q, id, nullableString(errMsg))
	if err != nil {
		return fmt.Errorf("mark job %s dead: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkCancelled cancels a job that has not already finished.
func (s *Store) MarkCancelled(ctx context.Context, id string) (bool, error) {
	const q = `
        UPDATE jobs SET
            status           = 'cancelled',
            finished_at      = now(),
            lease_expires_at = NULL,
            locked_by        = NULL
        WHERE id = $1 AND status NOT IN ('succeeded', 'dead', 'cancelled')`

	tag, err := s.pool.Exec(ctx, q, id)
	if err != nil {
		return false, fmt.Errorf("cancel job %s: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}

// MarkPendingRetry returns a dead or failed job to the ready state, for
// dead-letter replay from the admin UI.
func (s *Store) MarkPendingRetry(ctx context.Context, id string, resetAttempts bool) (*Job, error) {
	const q = `
        UPDATE jobs SET
            status           = 'pending',
            run_at           = now(),
            attempt          = CASE WHEN $2 THEN 0 ELSE attempt END,
            finished_at      = NULL,
            lease_expires_at = NULL,
            locked_by        = NULL
        WHERE id = $1 AND status IN ('dead', 'failed', 'cancelled')
        RETURNING` + jobColumns

	return scanJob(s.pool.QueryRow(ctx, q, id, resetAttempts))
}

// ExpiredLease describes a job reclaimed from a worker that stopped reporting.
type ExpiredLease struct {
	Job      *Job
	WorkerID string
}

// LoadForReap fetches the durable rows for jobs whose leases just expired, so
// the reaper can decide retry-versus-dead-letter using the same attempt and
// max_attempts values the nack path uses.
func (s *Store) LoadForReap(ctx context.Context, ids []string) ([]*Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx,
		`SELECT`+jobColumns+` FROM jobs
         WHERE id = ANY($1) AND status = 'running'`, ids)
	if err != nil {
		return nil, fmt.Errorf("load jobs for reap: %w", err)
	}
	defer rows.Close()

	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
