package store

import (
	"context"
	"fmt"
	"time"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
)

// NewAttempt records the outcome of one execution.
type NewAttempt struct {
	JobID    string
	Attempt  int
	WorkerID string
	Outcome  jobtypes.Outcome
	Error    string
	Duration time.Duration
}

// RecordAttempt appends one row to the execution audit trail.
//
// Rows are appended, never updated, and nothing here enforces one row per
// attempt number. Under at-least-once delivery a single attempt legitimately
// produces two records: the reaper writes 'lease_expired' for a worker it
// presumed dead, and that worker -- merely slow, not dead -- later reports its
// own outcome. Both are true observations. Collapsing them would hide exactly
// the event an operator is trying to understand.
func (s *Store) RecordAttempt(ctx context.Context, a *NewAttempt) error {
	const q = `
        INSERT INTO job_attempts (
            job_id, attempt, worker_id, outcome, error,
            started_at, finished_at, duration_ms
        ) VALUES ($1, $2, $3, $4, $5, now() - ($6::bigint * interval '1 millisecond'), now(), $6)`

	ms := a.Duration.Milliseconds()
	if ms < 0 {
		ms = 0
	}

	if _, err := s.pool.Exec(ctx, q,
		a.JobID, a.Attempt, a.WorkerID, string(a.Outcome),
		nullableString(a.Error), ms,
	); err != nil {
		return fmt.Errorf("record attempt for job %s: %w", a.JobID, err)
	}
	return nil
}

// RecordAttempts writes many attempt rows in one round trip. Used by the
// reaper, which routinely reclaims a whole batch of leases at once when a
// worker container dies.
func (s *Store) RecordAttempts(ctx context.Context, attempts []*NewAttempt) error {
	if len(attempts) == 0 {
		return nil
	}

	jobIDs := make([]string, len(attempts))
	nums := make([]int32, len(attempts))
	workers := make([]string, len(attempts))
	outcomes := make([]string, len(attempts))
	errs := make([]*string, len(attempts))
	durations := make([]int64, len(attempts))

	for i, a := range attempts {
		jobIDs[i] = a.JobID
		nums[i] = int32(a.Attempt)
		workers[i] = a.WorkerID
		outcomes[i] = string(a.Outcome)
		errs[i] = nullableString(a.Error)
		if ms := a.Duration.Milliseconds(); ms > 0 {
			durations[i] = ms
		}
	}

	const q = `
        INSERT INTO job_attempts (
            job_id, attempt, worker_id, outcome, error,
            started_at, finished_at, duration_ms
        )
        SELECT
            v.job_id, v.attempt, v.worker_id, v.outcome, v.error,
            now() - (v.duration_ms * interval '1 millisecond'), now(), v.duration_ms
        FROM unnest($1::uuid[], $2::int[], $3::text[], $4::text[], $5::text[], $6::bigint[])
             AS v(job_id, attempt, worker_id, outcome, error, duration_ms)`

	if _, err := s.pool.Exec(ctx, q,
		jobIDs, nums, workers, outcomes, errs, durations,
	); err != nil {
		return fmt.Errorf("record %d attempts: %w", len(attempts), err)
	}
	return nil
}

// ListAttempts returns the execution timeline for one job, oldest first. This
// is what the dashboard's attempt timeline renders: each retry, how long it
// ran, which worker took it, and why it failed.
func (s *Store) ListAttempts(ctx context.Context, jobID string) ([]*Attempt, error) {
	const q = `
        SELECT id, job_id, attempt, worker_id, outcome, error,
               started_at, finished_at, duration_ms
        FROM job_attempts
        WHERE job_id = $1
        ORDER BY attempt, started_at`

	rows, err := s.pool.Query(ctx, q, jobID)
	if err != nil {
		return nil, fmt.Errorf("list attempts for job %s: %w", jobID, err)
	}
	defer rows.Close()

	var out []*Attempt
	for rows.Next() {
		var (
			a       Attempt
			outcome *string
		)
		if err := rows.Scan(
			&a.ID, &a.JobID, &a.Attempt, &a.WorkerID, &outcome, &a.Error,
			&a.StartedAt, &a.FinishedAt, &a.DurationMS,
		); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		if outcome != nil {
			a.Outcome = jobtypes.Outcome(*outcome)
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}
