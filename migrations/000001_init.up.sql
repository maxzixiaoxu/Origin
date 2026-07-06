-- Initial schema for the distributed job queue.
--
-- Postgres is the durable source of truth. Redis holds the hot path (ordering,
-- leases, rate-limit buckets) and is treated as a rebuildable cache: if Redis
-- is flushed, the reconciler replays state from these tables. That is why the
-- indexes below are shaped for two very different consumers -- the reconciler's
-- "what should be in Redis right now?" scans, and the dashboard's filtered,
-- paginated browsing.

BEGIN;

-- Lifecycle states. Mirrors jobtypes.Status in Go; change both together.
--
--   pending    in the ready set, awaiting a worker
--   scheduled  waiting for run_at (user delay or retry backoff)
--   running    a worker holds a lease
--   succeeded  terminal
--   failed     transient: an attempt failed, retries remain
--   dead       terminal: retries exhausted or error was permanent
--   cancelled  terminal: operator action
CREATE TYPE job_status AS ENUM (
    'pending',
    'scheduled',
    'running',
    'succeeded',
    'failed',
    'dead',
    'cancelled'
);

-- Per-queue configuration. Every knob here is live-editable from the admin UI;
-- nothing requires a service restart.
CREATE TABLE queues (
    name TEXT PRIMARY KEY,

    -- Queue names are interpolated into Redis keys (q:{name}:ready). Constrain
    -- the charset at the database boundary so a malformed name cannot reach the
    -- keyspace at all, rather than relying on every call site to validate.
    CONSTRAINT queues_name_format CHECK (name ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'),

    -- Ceiling on simultaneously-leased jobs across all workers. This is the
    -- backpressure valve for protecting a downstream dependency: it caps
    -- in-flight work regardless of how many workers are running.
    max_concurrency INTEGER NOT NULL DEFAULT 100
        CHECK (max_concurrency >= 0),

    -- Token-bucket dispatch limit. NULL means unlimited.
    rate_limit_per_sec INTEGER
        CHECK (rate_limit_per_sec IS NULL OR rate_limit_per_sec > 0),
    rate_limit_burst INTEGER
        CHECK (rate_limit_burst IS NULL OR rate_limit_burst > 0),

    max_attempts INTEGER NOT NULL DEFAULT 5
        CHECK (max_attempts >= 1),

    -- How long a worker may hold a job without heartbeating before the reaper
    -- reclaims it. This is the dominant term in crash-recovery latency.
    visibility_timeout_sec INTEGER NOT NULL DEFAULT 30
        CHECK (visibility_timeout_sec > 0),

    backoff_base_ms INTEGER NOT NULL DEFAULT 1000
        CHECK (backoff_base_ms > 0),
    backoff_cap_ms INTEGER NOT NULL DEFAULT 300000
        CHECK (backoff_cap_ms > 0),
    CONSTRAINT queues_backoff_cap_gte_base CHECK (backoff_cap_ms >= backoff_base_ms),

    -- Paused queues accept enqueues but dispatch nothing. Draining a queue
    -- without losing submissions is the common incident-response move.
    paused BOOLEAN NOT NULL DEFAULT FALSE,

    description TEXT NOT NULL DEFAULT '',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A burst without a rate is meaningless, so require them together.
ALTER TABLE queues ADD CONSTRAINT queues_rate_limit_pair CHECK (
    (rate_limit_per_sec IS NULL AND rate_limit_burst IS NULL)
    OR (rate_limit_per_sec IS NOT NULL AND rate_limit_burst IS NOT NULL)
);

-- The durable record of every job. Redis caches an execution envelope for
-- in-flight work, but this table is what survives a Redis flush.
CREATE TABLE jobs (
    id UUID PRIMARY KEY,

    -- ON UPDATE CASCADE so a queue rename does not orphan history; no
    -- ON DELETE rule, because deleting a queue with jobs should fail loudly.
    queue_name TEXT NOT NULL REFERENCES queues(name) ON UPDATE CASCADE,

    job_type TEXT NOT NULL CHECK (job_type <> ''),
    payload  JSONB NOT NULL DEFAULT '{}'::jsonb,

    status   job_status NOT NULL DEFAULT 'pending',

    -- Lower is dispatched first. Bounded because the Redis ready-set score
    -- packs priority into the high digits of a float64.
    priority SMALLINT NOT NULL DEFAULT 5
        CHECK (priority BETWEEN 0 AND 9),

    attempt      INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),

    -- Caller-supplied dedupe token. Uniqueness is enforced by the partial index
    -- below rather than in application code, so a race between two concurrent
    -- submissions of the same key resolves in the database instead of both
    -- passing a read-then-write check.
    idempotency_key TEXT,

    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    lease_expires_at TIMESTAMPTZ,
    locked_by        TEXT,

    last_error TEXT,
    result     JSONB,

    -- Correlates this job with the HTTP request that created it, all the way
    -- back through the Rails access log.
    trace_id TEXT,

    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotent enqueue, enforced by the database. Partial so the vast majority of
-- jobs, which carry no key, cost nothing to index.
CREATE UNIQUE INDEX jobs_idempotency_key_uniq
    ON jobs (queue_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Dashboard listing: filter by queue and status, order by priority then age.
CREATE INDEX jobs_queue_status_priority_idx
    ON jobs (queue_name, status, priority, enqueued_at DESC);

-- Reconciler: find work that should be present in Redis but may not be.
-- Partial, because only non-terminal jobs are ever reconciled and terminal rows
-- quickly dominate the table.
CREATE INDEX jobs_reconcile_idx
    ON jobs (status, run_at)
    WHERE status IN ('pending', 'scheduled');

-- Reaper fallback: find leases that expired while Redis was unavailable.
CREATE INDEX jobs_running_lease_idx
    ON jobs (lease_expires_at)
    WHERE status = 'running';

-- Dead-letter browsing and "recently finished" views.
CREATE INDEX jobs_finished_idx
    ON jobs (finished_at DESC)
    WHERE status IN ('succeeded', 'dead', 'cancelled');

-- Filtering the jobs table by handler type.
CREATE INDEX jobs_type_idx ON jobs (job_type, enqueued_at DESC);

-- Tracing a request to the jobs it produced.
CREATE INDEX jobs_trace_idx ON jobs (trace_id) WHERE trace_id IS NOT NULL;

-- One row per execution attempt: the audit trail behind the dashboard's
-- attempt timeline, and the data that makes "how often do workers die
-- mid-job?" answerable by distinguishing lease_expired from failed.
CREATE TABLE job_attempts (
    id BIGSERIAL PRIMARY KEY,

    job_id  UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt >= 0),

    worker_id TEXT NOT NULL,

    -- succeeded | failed | timeout | lease_expired | panic | cancelled
    outcome TEXT,
    error   TEXT,

    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    duration_ms INTEGER CHECK (duration_ms IS NULL OR duration_ms >= 0)
);

-- Deliberately NOT unique on (job_id, attempt). Under at-least-once delivery a
-- single attempt number can legitimately produce two rows: the reaper records
-- 'lease_expired' for a worker presumed dead, and a merely-slow worker may then
-- report its own outcome for the same attempt. Recording both is the honest
-- audit trail, and hiding one behind a constraint violation would turn a
-- normal distributed-systems event into a write error.
CREATE INDEX job_attempts_job_idx ON job_attempts (job_id, attempt, started_at);
CREATE INDEX job_attempts_worker_idx ON job_attempts (worker_id, started_at DESC);
CREATE INDEX job_attempts_outcome_idx ON job_attempts (outcome, started_at DESC);

-- Pre-aggregated per-minute rollups. The dashboard's charts read only from
-- here, so plotting a day of throughput never scans the jobs table -- which is
-- the difference between a dashboard that stays fast at ten million rows and
-- one that does not.
CREATE TABLE queue_stats_minute (
    bucket     TIMESTAMPTZ NOT NULL,
    queue_name TEXT NOT NULL REFERENCES queues(name) ON UPDATE CASCADE,

    enqueued  INTEGER NOT NULL DEFAULT 0,
    succeeded INTEGER NOT NULL DEFAULT 0,
    failed    INTEGER NOT NULL DEFAULT 0,
    dead      INTEGER NOT NULL DEFAULT 0,
    retried   INTEGER NOT NULL DEFAULT 0,

    -- Execution time: how long the handler ran.
    duration_p50_ms INTEGER,
    duration_p95_ms INTEGER,
    duration_p99_ms INTEGER,

    -- Queue wait: enqueue to start. This is the real service-level signal --
    -- a queue can have fast handlers and still be badly backed up.
    wait_p50_ms INTEGER,
    wait_p95_ms INTEGER,
    wait_p99_ms INTEGER,

    PRIMARY KEY (bucket, queue_name)
);

CREATE INDEX queue_stats_minute_bucket_idx ON queue_stats_minute (bucket DESC);

-- Keep updated_at honest without requiring every writer to remember it.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER queues_set_updated_at
    BEFORE UPDATE ON queues
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER jobs_set_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- The system needs at least one queue to be usable out of the box. This is
-- bootstrap, not demo data -- sample queues live in the seed script.
INSERT INTO queues (name, description) VALUES
    ('default', 'Default queue');

COMMIT;
