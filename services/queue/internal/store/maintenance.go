package store

import (
	"context"
	"fmt"
	"time"
)

// LoadForReconcile finds durable jobs that should have a presence in Redis.
//
// Three shapes are collected, and the third is the subtle one:
//
//	pending    should be in the ready set
//	scheduled  should be in the scheduled set
//	running with an expired lease  -- stranded
//
// A stranded job is what remains when the queue service died between reap.lua
// clearing a lease and Go requeueing the job. It is in no Redis set at all, and
// crucially there is no longer a lease left to expire, so the reaper will never
// look at it again. Without this query such a job would sit at 'running'
// forever with nothing in the system aware it exists. Scanning on lease expiry
// rather than status alone is what makes the reap path's non-atomicity
// recoverable instead of a slow leak.
//
// Ordered oldest-first so a large backlog is restored in roughly the order it
// was submitted.
func (s *Store) LoadForReconcile(ctx context.Context, now time.Time, limit int) ([]*Job, error) {
	if limit < 1 {
		limit = 1000
	}

	const q = `
        SELECT` + jobColumns + `
        FROM jobs
        WHERE status IN ('pending', 'scheduled')
           OR (status = 'running' AND lease_expires_at IS NOT NULL
               AND lease_expires_at < $1)
        ORDER BY enqueued_at
        LIMIT $2`

	rows, err := s.pool.Query(ctx, q, now, limit)
	if err != nil {
		return nil, fmt.Errorf("load jobs for reconcile: %w", err)
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

// RollupMinutes recomputes per-minute statistics for the trailing window.
//
// Recompute-and-upsert rather than incremental counters. The window is
// deliberately wider than the tick interval so every bucket is rewritten
// several times before it goes cold, which makes the job idempotent: running it
// twice, or on two replicas at once during a leadership overlap, produces
// identical rows. Incremental counters would double-count in exactly that
// situation, and the error would be permanent and invisible.
//
// The window boundary is computed by Postgres from its own now(), not passed in
// from Go. Every timestamp being aggregated -- enqueued_at, finished_at -- was
// written by a database default, so comparing them against an application
// clock introduces a skew term for no benefit. Any drift between the queue
// service host and the database would silently shift the window and drop or
// double-count whole minutes at the edges. Using one clock for both sides of
// the comparison removes the failure mode rather than making it small.
//
// One pass over jobs plus one over job_attempts, both bounded by the window, so
// cost is proportional to recent activity rather than to table size.
func (s *Store) RollupMinutes(ctx context.Context, window time.Duration) (int64, error) {
	if window <= 0 {
		window = 10 * time.Minute
	}

	const q = `
        WITH enqueued AS (
            SELECT date_trunc('minute', enqueued_at) AS bucket,
                   queue_name,
                   count(*) AS n
            FROM jobs
            WHERE enqueued_at >= now() - $1::interval
            GROUP BY 1, 2
        ),
        finished AS (
            SELECT date_trunc('minute', finished_at) AS bucket,
                   queue_name,
                   count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
                   count(*) FILTER (WHERE status = 'dead')      AS dead,
                   -- Queue wait: enqueue to first start. The real service-level
                   -- signal -- a queue can have fast handlers and still be
                   -- badly backed up, and only this number shows it.
                   percentile_disc(0.50) WITHIN GROUP (
                       ORDER BY EXTRACT(EPOCH FROM (started_at - enqueued_at)) * 1000
                   ) FILTER (WHERE started_at IS NOT NULL) AS wait_p50,
                   percentile_disc(0.95) WITHIN GROUP (
                       ORDER BY EXTRACT(EPOCH FROM (started_at - enqueued_at)) * 1000
                   ) FILTER (WHERE started_at IS NOT NULL) AS wait_p95,
                   percentile_disc(0.99) WITHIN GROUP (
                       ORDER BY EXTRACT(EPOCH FROM (started_at - enqueued_at)) * 1000
                   ) FILTER (WHERE started_at IS NOT NULL) AS wait_p99
            FROM jobs
            WHERE finished_at >= now() - $1::interval
            GROUP BY 1, 2
        ),
        attempted AS (
            SELECT date_trunc('minute', a.finished_at) AS bucket,
                   j.queue_name,
                   count(*) FILTER (WHERE a.outcome <> 'succeeded') AS failed,
                   count(*) FILTER (WHERE a.attempt > 1)            AS retried,
                   percentile_disc(0.50) WITHIN GROUP (ORDER BY a.duration_ms) AS d50,
                   percentile_disc(0.95) WITHIN GROUP (ORDER BY a.duration_ms) AS d95,
                   percentile_disc(0.99) WITHIN GROUP (ORDER BY a.duration_ms) AS d99
            FROM job_attempts a
            JOIN jobs j ON j.id = a.job_id
            WHERE a.finished_at >= now() - $1::interval
            GROUP BY 1, 2
        ),
        combined AS (
            SELECT bucket, queue_name FROM enqueued
            UNION
            SELECT bucket, queue_name FROM finished
            UNION
            SELECT bucket, queue_name FROM attempted
        )
        INSERT INTO queue_stats_minute (
            bucket, queue_name,
            enqueued, succeeded, failed, dead, retried,
            duration_p50_ms, duration_p95_ms, duration_p99_ms,
            wait_p50_ms, wait_p95_ms, wait_p99_ms
        )
        SELECT
            c.bucket, c.queue_name,
            COALESCE(e.n, 0),
            COALESCE(f.succeeded, 0),
            COALESCE(a.failed, 0),
            COALESCE(f.dead, 0),
            COALESCE(a.retried, 0),
            a.d50::int, a.d95::int, a.d99::int,
            f.wait_p50::int, f.wait_p95::int, f.wait_p99::int
        FROM combined c
        LEFT JOIN enqueued  e ON e.bucket = c.bucket AND e.queue_name = c.queue_name
        LEFT JOIN finished  f ON f.bucket = c.bucket AND f.queue_name = c.queue_name
        LEFT JOIN attempted a ON a.bucket = c.bucket AND a.queue_name = c.queue_name
        ON CONFLICT (bucket, queue_name) DO UPDATE SET
            enqueued        = EXCLUDED.enqueued,
            succeeded       = EXCLUDED.succeeded,
            failed          = EXCLUDED.failed,
            dead            = EXCLUDED.dead,
            retried         = EXCLUDED.retried,
            duration_p50_ms = EXCLUDED.duration_p50_ms,
            duration_p95_ms = EXCLUDED.duration_p95_ms,
            duration_p99_ms = EXCLUDED.duration_p99_ms,
            wait_p50_ms     = EXCLUDED.wait_p50_ms,
            wait_p95_ms     = EXCLUDED.wait_p95_ms,
            wait_p99_ms     = EXCLUDED.wait_p99_ms`

	tag, err := s.pool.Exec(ctx, q, window.String())
	if err != nil {
		return 0, fmt.Errorf("roll up minute stats: %w", err)
	}
	return tag.RowsAffected(), nil
}

// QueueStat is one row of the per-minute rollup, for the dashboard charts.
type QueueStat struct {
	Bucket    time.Time
	Queue     string
	Enqueued  int
	Succeeded int
	Failed    int
	Dead      int
	Retried   int

	DurationP50 *int
	DurationP95 *int
	DurationP99 *int
	WaitP50     *int
	WaitP95     *int
	WaitP99     *int
}

// QueueStats reads rollups for a queue over a window, oldest first.
func (s *Store) QueueStats(
	ctx context.Context,
	queue string,
	from, to time.Time,
) ([]*QueueStat, error) {
	const q = `
        SELECT bucket, queue_name, enqueued, succeeded, failed, dead, retried,
               duration_p50_ms, duration_p95_ms, duration_p99_ms,
               wait_p50_ms, wait_p95_ms, wait_p99_ms
        FROM queue_stats_minute
        WHERE queue_name = $1 AND bucket >= $2 AND bucket < $3
        ORDER BY bucket`

	rows, err := s.pool.Query(ctx, q, queue, from, to)
	if err != nil {
		return nil, fmt.Errorf("read queue stats for %s: %w", queue, err)
	}
	defer rows.Close()

	var out []*QueueStat
	for rows.Next() {
		var st QueueStat
		if err := rows.Scan(
			&st.Bucket, &st.Queue, &st.Enqueued, &st.Succeeded,
			&st.Failed, &st.Dead, &st.Retried,
			&st.DurationP50, &st.DurationP95, &st.DurationP99,
			&st.WaitP50, &st.WaitP95, &st.WaitP99,
		); err != nil {
			return nil, fmt.Errorf("scan queue stat: %w", err)
		}
		out = append(out, &st)
	}
	return out, rows.Err()
}

// PurgeFinished deletes terminal jobs older than a cutoff, returning the count.
//
// Without this the jobs table grows without bound and every dashboard query
// gets slower forever. Deletion cascades to job_attempts via the foreign key.
// Bounded per call so a first run against a large backlog does not hold a long
// transaction and block the hot path.
func (s *Store) PurgeFinished(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit < 1 {
		limit = 10000
	}

	const q = `
        DELETE FROM jobs
        WHERE id IN (
            SELECT id FROM jobs
            WHERE status IN ('succeeded', 'cancelled')
              AND finished_at IS NOT NULL
              AND finished_at < $1
            ORDER BY finished_at
            LIMIT $2
        )`

	// 'dead' is deliberately excluded. Dead-lettered jobs are the ones an
	// operator most needs to inspect and replay, so they are retained until
	// explicitly cleared from the admin UI.
	tag, err := s.pool.Exec(ctx, q, before, limit)
	if err != nil {
		return 0, fmt.Errorf("purge finished jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}
