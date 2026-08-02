// Package maintenance runs the queue's background loops: promotion, reaping,
// reconciliation, and statistics rollup.
//
// The four are one package rather than four because they share a single shape
// -- acquire leadership, tick on an interval, sweep every active queue, stop
// cleanly when leadership is lost -- and that shape is worth writing once. The
// per-loop files hold only the work each tick performs.
package maintenance

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/maxzixiaoxu/Origin/services/queue/internal/leader"
)

// Task is one unit of periodic work.
//
// Returning `again = true` means there is more work already waiting, and the
// runner should tick immediately instead of sleeping. This is what lets a large
// backlog drain at full speed rather than one batch per interval -- without it,
// a promoter with a 500-job batch and a one-second tick would need over three
// minutes to clear 100k scheduled jobs.
type Task interface {
	Name() string
	Tick(ctx context.Context) (again bool, err error)
}

// Runner executes a Task on an interval while holding a leader lock.
type Runner struct {
	task     Task
	elector  *leader.Elector
	interval time.Duration
	log      *slog.Logger
}

// RunnerOptions configures a Runner.
type RunnerOptions struct {
	Task     Task
	Elector  *leader.Elector
	Interval time.Duration
	Log      *slog.Logger
}

// NewRunner builds a Runner.
func NewRunner(opts RunnerOptions) (*Runner, error) {
	if opts.Task == nil {
		return nil, errors.New("maintenance: Task is required")
	}
	if opts.Elector == nil {
		return nil, errors.New("maintenance: Elector is required")
	}
	if opts.Interval <= 0 {
		return nil, errors.New("maintenance: Interval must be positive")
	}

	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	return &Runner{
		task:     opts.Task,
		elector:  opts.Elector,
		interval: opts.Interval,
		log:      log.With("loop", opts.Task.Name()),
	}, nil
}

// Run campaigns for leadership and ticks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	return r.elector.Run(ctx, r.loop)
}

// loop is invoked by the elector with a context cancelled the instant
// leadership is lost, so a demoted replica stops working within one tick
// instead of continuing to act as leader.
func (r *Runner) loop(ctx context.Context) error {
	r.log.InfoContext(ctx, "maintenance loop started", "interval", r.interval)
	defer r.log.InfoContext(ctx, "maintenance loop stopped")

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		// Drain whatever is already due before sleeping. Bounded by
		// maxCatchUpTicks so one enormous backlog cannot monopolise the loop
		// and starve the leadership-loss check between iterations.
		const maxCatchUpTicks = 20

		for i := 0; i < maxCatchUpTicks; i++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			again, err := r.task.Tick(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				// Transient failures -- a Redis blip, a Postgres timeout -- must
				// not kill the loop. Losing the reaper permanently because of
				// one failed round trip would silently disable crash recovery,
				// and nothing would report it.
				r.log.ErrorContext(ctx, "maintenance tick failed, continuing",
					"error", err)
				break
			}
			if !again {
				break
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Group runs several Runners concurrently and returns when the first stops.
type Group struct {
	runners []*Runner
	log     *slog.Logger
}

// NewGroup bundles runners.
func NewGroup(log *slog.Logger, runners ...*Runner) *Group {
	if log == nil {
		log = slog.Default()
	}
	return &Group{runners: runners, log: log}
}

// Run starts every runner. Each campaigns for its own lock independently, so
// leadership of the four loops can land on different replicas -- there is no
// reason for one node to own all of them, and spreading them balances load.
func (g *Group) Run(ctx context.Context) error {
	errCh := make(chan error, len(g.runners))

	for _, r := range g.runners {
		go func(r *Runner) { errCh <- r.Run(ctx) }(r)
	}

	// The first non-cancellation error wins; cancellation is the normal exit.
	var first error
	for range g.runners {
		if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) && first == nil {
			first = err
		}
	}
	return first
}
