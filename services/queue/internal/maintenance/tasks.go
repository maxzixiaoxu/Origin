package maintenance

import (
	"context"
	"log/slog"
	"time"

	"github.com/maxzixiaoxu/Origin/services/queue/internal/broker"
)

// The four background tasks. Each sweeps every active queue once per tick and
// reports whether more work is already waiting, so the Runner can catch up on a
// backlog instead of pacing it at one batch per interval.

// --- promoter -------------------------------------------------------------

// Promoter moves scheduled jobs into the ready set once their run_at arrives.
//
// The tick interval is the floor on retry-delay accuracy: a job scheduled for T
// becomes dispatchable somewhere in [T, T+interval]. At one second that is
// invisible next to backoff delays measured in seconds to minutes.
type Promoter struct {
	broker *broker.Broker
	batch  int
	log    *slog.Logger
}

// NewPromoter builds the promotion task.
func NewPromoter(b *broker.Broker, batch int, log *slog.Logger) *Promoter {
	if batch < 1 {
		batch = 500
	}
	return &Promoter{broker: b, batch: batch, log: loggerOr(log)}
}

func (p *Promoter) Name() string { return "promoter" }

// Tick promotes one batch per queue.
func (p *Promoter) Tick(ctx context.Context) (bool, error) {
	queues, err := p.broker.ActiveQueues(ctx)
	if err != nil {
		return false, err
	}

	var (
		promoted int64
		more     bool
	)

	for _, queue := range queues {
		res, err := p.broker.Promote(ctx, queue, p.batch)
		if err != nil {
			// One bad queue must not stop the sweep, or a single malformed
			// queue name would stall promotion for every other queue.
			p.log.ErrorContext(ctx, "promote failed for queue",
				"queue", queue, "error", err)
			continue
		}
		promoted += res.Promoted
		if res.Remaining > 0 {
			more = true
		}
	}

	if promoted > 0 {
		p.log.DebugContext(ctx, "promoted scheduled jobs",
			"count", promoted, "more_waiting", more)
	}
	return more, nil
}

// --- reaper ---------------------------------------------------------------

// Reaper reclaims leases whose holders stopped heartbeating.
//
// The tick interval dominates crash-recovery latency: a job is recoverable only
// once its lease expires, and is recovered only on the following tick. Total
// worst case is visibility_timeout + interval.
type Reaper struct {
	broker *broker.Broker
	batch  int
	log    *slog.Logger
}

// NewReaper builds the reaping task.
func NewReaper(b *broker.Broker, batch int, log *slog.Logger) *Reaper {
	if batch < 1 {
		batch = 500
	}
	return &Reaper{broker: b, batch: batch, log: loggerOr(log)}
}

func (r *Reaper) Name() string { return "reaper" }

// Tick reclaims one batch of expired leases per queue.
func (r *Reaper) Tick(ctx context.Context) (bool, error) {
	queues, err := r.broker.ActiveQueues(ctx)
	if err != nil {
		return false, err
	}

	var (
		requeued, dead int64
		more           bool
	)

	for _, queue := range queues {
		res, err := r.broker.Reap(ctx, queue, r.batch)
		if err != nil {
			r.log.ErrorContext(ctx, "reap failed for queue",
				"queue", queue, "error", err)
			continue
		}
		requeued += res.Requeued
		dead += res.DeadLettered
		if res.Remaining > 0 {
			more = true
		}
	}

	// Logged at info, not debug: a reap means a worker died, which is a real
	// operational event even when recovery is automatic. Silent self-healing is
	// how a slowly failing host goes unnoticed for a week.
	if requeued > 0 || dead > 0 {
		r.log.InfoContext(ctx, "reclaimed expired leases",
			"requeued", requeued, "dead_lettered", dead, "more_waiting", more)
	}
	return more, nil
}

// --- reconciler -----------------------------------------------------------

// Reconciler rebuilds Redis state from Postgres.
//
// Runs periodically rather than only at boot, because the gaps it closes are
// not boot-specific: a failed enqueue write to Redis, or a crash between
// reap.lua clearing a lease and Go requeueing the job, can strand work at any
// time. A slow background sweep converts those from permanent losses into
// bounded delays.
type Reconciler struct {
	broker *broker.Broker
	batch  int
	log    *slog.Logger
}

// NewReconciler builds the reconciliation task.
func NewReconciler(b *broker.Broker, batch int, log *slog.Logger) *Reconciler {
	if batch < 1 {
		batch = 1000
	}
	return &Reconciler{broker: b, batch: batch, log: loggerOr(log)}
}

func (r *Reconciler) Name() string { return "reconciler" }

// Tick runs one reconciliation pass.
func (r *Reconciler) Tick(ctx context.Context) (bool, error) {
	res, err := r.broker.Reconcile(ctx, r.batch)
	if err != nil {
		return false, err
	}

	// A full batch means there may be more to restore. This is the path that
	// drains a post-FLUSHALL rebuild quickly instead of a batch per interval.
	return res.Scanned >= r.batch, nil
}

// --- rollup ---------------------------------------------------------------

// Rollup aggregates finished jobs into the per-minute statistics the dashboard
// charts read.
type Rollup struct {
	broker *broker.Broker
	window time.Duration
	log    *slog.Logger
}

// NewRollup builds the statistics task.
//
// The window is deliberately wider than the tick interval so each minute bucket
// is recomputed several times before going cold. Recomputation is idempotent --
// the upsert overwrites with the same values -- which is what makes the task
// safe to run twice during a leadership overlap. Incremental counters would
// double-count in exactly that situation, permanently and invisibly.
func NewRollup(b *broker.Broker, window time.Duration, log *slog.Logger) *Rollup {
	if window <= 0 {
		window = 10 * time.Minute
	}
	return &Rollup{broker: b, window: window, log: loggerOr(log)}
}

func (r *Rollup) Name() string { return "rollup" }

// Tick recomputes the rollup window.
func (r *Rollup) Tick(ctx context.Context) (bool, error) {
	rows, err := r.broker.Rollup(ctx, r.window)
	if err != nil {
		return false, err
	}
	if rows > 0 {
		r.log.DebugContext(ctx, "rolled up queue statistics", "buckets", rows)
	}
	// Fixed window, so there is never a backlog to catch up on.
	return false, nil
}

func loggerOr(log *slog.Logger) *slog.Logger {
	if log == nil {
		return slog.Default()
	}
	return log
}
