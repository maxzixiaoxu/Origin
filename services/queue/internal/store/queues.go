package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// queueColumns is the shared SELECT list, so every scan site stays in step with
// scanQueue below.
const queueColumns = `
    name, max_concurrency, rate_limit_per_sec, rate_limit_burst,
    max_attempts, visibility_timeout_sec, backoff_base_ms, backoff_cap_ms,
    paused, description, created_at, updated_at`

// scanQueue decodes one queues row. Durations are stored in their natural SQL
// units (seconds, milliseconds) and converted here, so the rest of the program
// only ever handles time.Duration and cannot confuse the two.
func scanQueue(row pgx.Row) (*QueueConfig, error) {
	var (
		q           QueueConfig
		visibleSec  int
		backoffBase int
		backoffCap  int
	)

	err := row.Scan(
		&q.Name,
		&q.MaxConcurrency,
		&q.RateLimitPerSec,
		&q.RateLimitBurst,
		&q.MaxAttempts,
		&visibleSec,
		&backoffBase,
		&backoffCap,
		&q.Paused,
		&q.Description,
		&q.CreatedAt,
		&q.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan queue: %w", err)
	}

	q.VisibilityTimeout = time.Duration(visibleSec) * time.Second
	q.BackoffBase = time.Duration(backoffBase) * time.Millisecond
	q.BackoffCap = time.Duration(backoffCap) * time.Millisecond

	return &q, nil
}

// GetQueue loads one queue's configuration.
func (s *Store) GetQueue(ctx context.Context, name string) (*QueueConfig, error) {
	return scanQueue(s.pool.QueryRow(ctx,
		`SELECT`+queueColumns+` FROM queues WHERE name = $1`, name))
}

// ListQueues returns every queue, ordered by name for a stable dashboard.
func (s *Store) ListQueues(ctx context.Context) ([]*QueueConfig, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT`+queueColumns+` FROM queues ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	defer rows.Close()

	var out []*QueueConfig
	for rows.Next() {
		q, err := scanQueue(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

// UpsertQueue creates or updates a queue and returns the stored row.
//
// COALESCE on every optional column makes this a genuine partial update: the
// admin UI can send "pause this queue" without having to first read and then
// echo back the other nine settings. Round-tripping the full row would create a
// lost-update race between two operators editing the same queue during an
// incident, which is exactly when two people are most likely to be doing so.
func (s *Store) UpsertQueue(ctx context.Context, q *QueueConfig) (*QueueConfig, bool, error) {
	var (
		visibleSec  *int
		backoffBase *int
		backoffCap  *int
		maxConc     *int
		maxAttempts *int
	)
	if q.VisibilityTimeout > 0 {
		v := int(q.VisibilityTimeout.Seconds())
		visibleSec = &v
	}
	if q.BackoffBase > 0 {
		v := int(q.BackoffBase.Milliseconds())
		backoffBase = &v
	}
	if q.BackoffCap > 0 {
		v := int(q.BackoffCap.Milliseconds())
		backoffCap = &v
	}
	if q.MaxConcurrency > 0 {
		maxConc = &q.MaxConcurrency
	}
	if q.MaxAttempts > 0 {
		maxAttempts = &q.MaxAttempts
	}

	// xmax = 0 identifies a freshly inserted row as opposed to one updated by
	// the DO UPDATE branch. It is the standard way to make an upsert report
	// which of the two it did, without a second query.
	const q1 = `
        INSERT INTO queues (
            name, max_concurrency, rate_limit_per_sec, rate_limit_burst,
            max_attempts, visibility_timeout_sec, backoff_base_ms, backoff_cap_ms,
            paused, description
        ) VALUES (
            $1,
            COALESCE($2, 100),
            $3, $4,
            COALESCE($5, 5),
            COALESCE($6, 30),
            COALESCE($7, 1000),
            COALESCE($8, 300000),
            COALESCE($9, FALSE),
            COALESCE($10, '')
        )
        ON CONFLICT (name) DO UPDATE SET
            max_concurrency        = COALESCE($2,  queues.max_concurrency),
            rate_limit_per_sec     = $3,
            rate_limit_burst       = $4,
            max_attempts           = COALESCE($5,  queues.max_attempts),
            visibility_timeout_sec = COALESCE($6,  queues.visibility_timeout_sec),
            backoff_base_ms        = COALESCE($7,  queues.backoff_base_ms),
            backoff_cap_ms         = COALESCE($8,  queues.backoff_cap_ms),
            paused                 = COALESCE($9,  queues.paused),
            description            = COALESCE($10, queues.description)
        RETURNING (xmax = 0) AS created,` + queueColumns

	var (
		created     bool
		out         QueueConfig
		vSec        int
		bBase, bCap int
	)

	err := s.pool.QueryRow(ctx, q1,
		q.Name, maxConc, q.RateLimitPerSec, q.RateLimitBurst,
		maxAttempts, visibleSec, backoffBase, backoffCap,
		q.Paused, nullableString(q.Description),
	).Scan(
		&created,
		&out.Name, &out.MaxConcurrency, &out.RateLimitPerSec, &out.RateLimitBurst,
		&out.MaxAttempts, &vSec, &bBase, &bCap,
		&out.Paused, &out.Description, &out.CreatedAt, &out.UpdatedAt,
	)
	if err != nil {
		return nil, false, fmt.Errorf("upsert queue %q: %w", q.Name, err)
	}

	out.VisibilityTimeout = time.Duration(vSec) * time.Second
	out.BackoffBase = time.Duration(bBase) * time.Millisecond
	out.BackoffCap = time.Duration(bCap) * time.Millisecond

	return &out, created, nil
}

// SetPaused toggles dispatch for a queue. Separate from UpsertQueue because
// pausing is the one incident-response action that must be a single obvious
// call rather than a partial update with nine nils.
func (s *Store) SetPaused(ctx context.Context, name string, paused bool) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE queues SET paused = $2 WHERE name = $1`, name, paused)
	if err != nil {
		return fmt.Errorf("set paused on %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// EnsureQueue creates a queue with defaults if it does not already exist.
//
// Called on the enqueue path so submitting to a new queue name just works
// rather than failing with a foreign-key violation the caller cannot act on.
// Returns the resulting configuration either way.
func (s *Store) EnsureQueue(ctx context.Context, name string) (*QueueConfig, error) {
	if q, err := s.GetQueue(ctx, name); err == nil {
		return q, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	// ON CONFLICT DO NOTHING rather than a check-then-insert: two concurrent
	// first-submissions to the same new queue would otherwise race, and one
	// would fail on the primary key.
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO queues (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`,
		name); err != nil {
		return nil, fmt.Errorf("ensure queue %q: %w", name, err)
	}

	return s.GetQueue(ctx, name)
}

// DeleteQueue removes a queue. Fails if jobs still reference it, which is the
// intended behaviour: silently cascading away job history would destroy the
// audit trail an operator is most likely to want right after deciding a queue
// was a mistake.
func (s *Store) DeleteQueue(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM queues WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete queue %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// QueueNames lists queue names only, for background loops that sweep every
// queue and do not need full configuration.
func (s *Store) QueueNames(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT name FROM queues ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list queue names: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}
