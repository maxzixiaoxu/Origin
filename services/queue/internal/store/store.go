// Package store is the Postgres layer: the durable record of every job, and
// the query surface the admin dashboard reads.
//
// The division of labour with Redis is strict. Redis decides *what runs next*
// and is authoritative for leases; Postgres decides *what exists* and is
// authoritative for everything an operator or auditor needs after the fact.
// When the two disagree, Postgres wins and the reconciler rebuilds Redis.
package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/maxzixiaoxu/Origin/pkg/config"
	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
)

// ErrNotFound is returned when a lookup matches no row. Callers distinguish it
// from a real failure with errors.Is, so a missing job produces a 404 rather
// than a 500.
var ErrNotFound = errors.New("not found")

// Store owns the connection pool and all SQL.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a pool and verifies connectivity.
//
// Connectivity is checked eagerly rather than lazily so a bad DSN or an
// unreachable database fails at startup, in the logs, instead of on the first
// user request minutes later.
func New(ctx context.Context, cfg config.Postgres) (*Store, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse POSTGRES_URL: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	// A server-side statement timeout is a backstop against one pathological
	// query holding a pool connection indefinitely. Without it, a single slow
	// dashboard filter can starve the hot path of connections, because both
	// share this pool.
	if cfg.StatementTimeout > 0 {
		if poolCfg.ConnConfig.RuntimeParams == nil {
			poolCfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		poolCfg.ConnConfig.RuntimeParams["statement_timeout"] =
			fmt.Sprintf("%d", cfg.StatementTimeout.Milliseconds())
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Store{pool: pool}, nil
}

// NewWithPool wraps an existing pool. Used by tests that manage their own
// container lifecycle.
func NewWithPool(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the underlying pool for migrations and health checks.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Ping reports database reachability for the health endpoint.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// --- domain types ---------------------------------------------------------

// QueueConfig is one row of the queues table.
type QueueConfig struct {
	Name string

	MaxConcurrency int

	// Nil means unlimited. The pair is present or absent together, enforced by
	// the queues_rate_limit_pair constraint.
	RateLimitPerSec *int
	RateLimitBurst  *int

	MaxAttempts       int
	VisibilityTimeout time.Duration
	BackoffBase       time.Duration
	BackoffCap        time.Duration

	Paused      bool
	Description string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// RateLimited reports whether dispatch from this queue is throttled.
func (q *QueueConfig) RateLimited() bool {
	return q.RateLimitPerSec != nil && *q.RateLimitPerSec > 0
}

// RatePerSec returns the configured rate, or 0 when unlimited. Scripts take 0
// to mean "skip the token bucket entirely".
func (q *QueueConfig) RatePerSec() int {
	if !q.RateLimited() {
		return 0
	}
	return *q.RateLimitPerSec
}

// Burst returns the configured burst, defaulting to the per-second rate when
// only a rate was somehow stored. A burst below 1 would make the bucket unable
// to ever grant a token and would wedge the queue shut.
func (q *QueueConfig) Burst() int {
	if !q.RateLimited() {
		return 0
	}
	if q.RateLimitBurst == nil || *q.RateLimitBurst < 1 {
		return *q.RateLimitPerSec
	}
	return *q.RateLimitBurst
}

// Job is one row of the jobs table.
type Job struct {
	ID    string
	Queue string
	Type  string
	// Payload is opaque JSON. The queue never interprets it.
	Payload []byte

	Status   jobtypes.Status
	Priority int

	Attempt     int
	MaxAttempts int

	IdempotencyKey *string

	RunAt          time.Time
	LeaseExpiresAt *time.Time
	LockedBy       *string

	LastError *string
	Result    []byte
	TraceID   *string

	EnqueuedAt time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Envelope projects the durable row down to what a worker needs to execute it.
// Used when rehydrating a job whose Redis envelope is missing.
func (j *Job) Envelope() jobtypes.Envelope {
	e := jobtypes.Envelope{
		ID:          j.ID,
		Queue:       j.Queue,
		Type:        j.Type,
		Payload:     j.Payload,
		Attempt:     j.Attempt,
		MaxAttempts: j.MaxAttempts,
		Priority:    j.Priority,
		EnqueuedAt:  j.EnqueuedAt,
	}
	if j.LeaseExpiresAt != nil {
		e.LeaseExpiresAt = *j.LeaseExpiresAt
	}
	if j.TraceID != nil {
		e.TraceID = *j.TraceID
	}
	return e
}

// Attempt is one row of the job_attempts table.
type Attempt struct {
	ID       int64
	JobID    string
	Attempt  int
	WorkerID string

	Outcome jobtypes.Outcome
	Error   *string

	StartedAt  time.Time
	FinishedAt *time.Time
	DurationMS *int
}

// --- helpers --------------------------------------------------------------

// txFunc runs inside a transaction.
type txFunc func(ctx context.Context, tx pgx.Tx) error

// withTx runs fn in a transaction, rolling back on error or panic.
//
// The deferred rollback is unconditional and its error is discarded on purpose:
// after a successful Commit the rollback is a no-op returning ErrTxClosed, and
// treating that as a failure would turn every successful transaction into a
// spurious error.
func (s *Store) withTx(ctx context.Context, fn txFunc) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// nullableString converts an empty string to nil, so absent values are stored
// as SQL NULL rather than as empty text. That distinction matters for the
// partial indexes, which use IS NOT NULL to stay small.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// compactJSON removes insignificant whitespace from a JSON document.
//
// This exists to keep two code paths byte-identical, and the bug it fixes was
// genuinely subtle.
//
// A worker can receive its payload two ways: from the cached Redis envelope on
// the hot path, or rebuilt from Postgres when that cache is missing. Those
// produced different bytes for the same job. Go's encoding/json compacts a
// json.RawMessage when marshalling the envelope, so the Redis copy had no
// spaces; Postgres renders jsonb with a space after every ':' and ',', so the
// rehydrated copy did. Same keys, same values, different bytes.
//
// Semantically identical is not good enough here. A handler that verifies an
// HMAC over the raw payload, or hashes it for deduplication, would work
// perfectly in every test and then start failing only for jobs that outlived
// their Redis cache -- after a restart, a FLUSHALL, or a TTL expiry. That is a
// bug that appears in production, under recovery conditions, and looks like
// nondeterminism.
//
// Compacting everything on the way out of Postgres makes the two paths agree by
// construction. jsonb has already validated the document, so a parse failure
// here is impossible in practice; returning the input unchanged is the safe
// fallback rather than dropping a payload over a formatting concern.
func compactJSON(raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}

	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return raw
	}
	return buf.Bytes()
}
