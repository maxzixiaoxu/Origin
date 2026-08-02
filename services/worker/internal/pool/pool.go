// Package pool runs jobs concurrently with bounded parallelism, backpressure,
// and lease renewal.
//
// Three cooperating goroutine groups:
//
//	fetcher      pulls work, but only as much as there is capacity to start
//	workers      N goroutines executing handlers
//	heartbeater  one goroutine renewing every held lease in a single call
//
// The design constraint that shapes all three is that a lease is a promise with
// a deadline. Holding a job you are not running is worse than not holding it:
// the lease ticks down while the job sits idle, expires, gets reaped, and is
// handed to another worker -- so the system does the work twice, under load,
// which is exactly when it can least afford to.
//
// Everything below follows from refusing to hold work that is not running.
package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/pkg/logx"
	"github.com/maxzixiaoxu/Origin/services/worker/internal/handlers"
)

// Client is the queue service, as the pool needs it.
//
// Declared here rather than imported so the pool can be tested against a fake
// without a gRPC server, and so the dependency arrow points from the pool to an
// interface it owns.
type Client interface {
	Dequeue(ctx context.Context, queues []string, maxJobs int, workerID string) (*DequeueResult, error)
	Ack(ctx context.Context, jobID, workerID string, result []byte, duration time.Duration) error
	Nack(ctx context.Context, req NackRequest) error
	ExtendLeases(ctx context.Context, workerID string, jobIDs []string) ([]LeaseResult, error)
}

// DequeueResult carries leased jobs or the reason there were none.
type DequeueResult struct {
	Jobs []jobtypes.Envelope
	// RetryAfter is how long the server asked the caller to wait. Honouring it
	// exactly is what keeps a rate-limited worker from spinning.
	RetryAfter time.Duration
	// Throttled is true when the empty result was caused by a limit rather than
	// an empty queue.
	Throttled bool
	// Paused means an operator stopped the queue; back off harder.
	Paused bool
}

// NackRequest reports a failed or abandoned execution.
type NackRequest struct {
	JobID              string
	WorkerID           string
	Error              string
	Permanent          bool
	RequeueImmediately bool
	Outcome            jobtypes.Outcome
	Duration           time.Duration
}

// LeaseResult is the per-job verdict from a heartbeat.
type LeaseResult struct {
	JobID    string
	Extended bool
}

// Options configures a Pool.
type Options struct {
	Client   Client
	Registry *handlers.Registry
	Log      *slog.Logger

	WorkerID string
	Queues   []string

	// Concurrency is the number of jobs executed simultaneously.
	Concurrency int
	// FetchBatch caps jobs requested per call; must not exceed Concurrency.
	FetchBatch int

	// EmptyPollInterval is the wait after an empty dequeue.
	EmptyPollInterval time.Duration
	// JobTimeout bounds a single execution.
	JobTimeout time.Duration
	// HeartbeatInterval is how often leases are renewed.
	HeartbeatInterval time.Duration
	// DrainTimeout bounds graceful shutdown.
	DrainTimeout time.Duration
}

// Stats is a snapshot of pool activity.
type Stats struct {
	InFlight  int64
	Completed int64
	Failed    int64
	LeaseLost int64
	Panics    int64
}

// Pool executes jobs.
type Pool struct {
	opts Options
	log  *slog.Logger

	// slots is a counting semaphore held as tokens. It is pre-filled with
	// Concurrency tokens; the fetcher drains as many as are free and requests
	// exactly that many jobs, and each finished job returns one.
	//
	// A plain counter would be racy against the fetcher, and a mutex-guarded
	// int would need its own condition variable to block on. A buffered channel
	// is both the counter and the blocking primitive.
	slots chan struct{}

	// jobs is unbuffered. Buffering here would reintroduce the exact problem
	// the slot budget exists to prevent -- jobs sitting in a queue with live
	// leases and no goroutine running them.
	jobs chan jobtypes.Envelope

	// inflight tracks running jobs so the heartbeater knows what to renew and
	// can cancel one whose lease was lost.
	inflight sync.Map // jobID -> *runningJob

	stats struct {
		inFlight  atomic.Int64
		completed atomic.Int64
		failed    atomic.Int64
		leaseLost atomic.Int64
		panics    atomic.Int64
	}
}

// runningJob is the bookkeeping for one in-flight execution.
type runningJob struct {
	envelope jobtypes.Envelope
	// cancel stops the handler. Called when the lease is lost, which is how a
	// distributed failure becomes ordinary Go cancellation.
	cancel  context.CancelCauseFunc
	started time.Time
}

// Cancellation causes.
//
// Both mean "stop, and do NOT report this job" -- but for different reasons,
// and the distinction is worth keeping because they are counted separately in
// the stats and diagnosed differently in an incident.
var (
	// ErrLeaseLost means the lease was reclaimed mid-execution. Reporting would
	// be rejected on ownership anyway, and could disturb the worker that now
	// owns the job.
	ErrLeaseLost = errors.New("lease lost; another worker owns this job")

	// ErrDraining means the drain deadline passed and releaseInFlight already
	// handed this job back.
	//
	// Without this marker the job is nacked twice: once deliberately by the
	// drain, and once more when the cancelled handler returns an error and the
	// normal failure path fires. The second nack is rejected by the server on
	// ownership -- the first already released the lease -- so it is harmless
	// but wrong, and it logs an error on every single job during every deploy,
	// which is exactly the kind of routine noise that trains people to ignore
	// error logs.
	ErrDraining = errors.New("worker draining; job handed back")
)

// New builds a Pool.
func New(opts Options) (*Pool, error) {
	if opts.Client == nil {
		return nil, errors.New("pool: Client is required")
	}
	if opts.Registry == nil {
		return nil, errors.New("pool: Registry is required")
	}
	if opts.WorkerID == "" {
		return nil, errors.New("pool: WorkerID is required")
	}
	if len(opts.Queues) == 0 {
		return nil, errors.New("pool: at least one queue is required")
	}
	if opts.Concurrency < 1 {
		return nil, fmt.Errorf("pool: Concurrency must be >= 1, got %d", opts.Concurrency)
	}
	if opts.FetchBatch < 1 {
		opts.FetchBatch = opts.Concurrency
	}
	if opts.FetchBatch > opts.Concurrency {
		return nil, fmt.Errorf(
			"pool: FetchBatch (%d) exceeds Concurrency (%d): jobs would be "+
				"leased with no slot to run them",
			opts.FetchBatch, opts.Concurrency)
	}
	if opts.EmptyPollInterval <= 0 {
		opts.EmptyPollInterval = 250 * time.Millisecond
	}
	if opts.JobTimeout <= 0 {
		opts.JobTimeout = 5 * time.Minute
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 5 * time.Second
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = 30 * time.Second
	}

	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	p := &Pool{
		opts:  opts,
		log:   log.With("worker_id", opts.WorkerID),
		slots: make(chan struct{}, opts.Concurrency),
		jobs:  make(chan jobtypes.Envelope),
	}
	for i := 0; i < opts.Concurrency; i++ {
		p.slots <- struct{}{}
	}
	return p, nil
}

// Stats returns a snapshot.
func (p *Pool) Stats() Stats {
	return Stats{
		InFlight:  p.stats.inFlight.Load(),
		Completed: p.stats.completed.Load(),
		Failed:    p.stats.failed.Load(),
		LeaseLost: p.stats.leaseLost.Load(),
		Panics:    p.stats.panics.Load(),
	}
}

// Run executes jobs until ctx is cancelled, then drains.
//
// Shutdown sequence, in order and for a reason:
//
//  1. Stop fetching. No new leases are taken.
//  2. Close the jobs channel so worker goroutines exit once idle.
//  3. Wait up to DrainTimeout for in-flight work to finish.
//  4. Explicitly nack whatever is still running, with requeue_immediately.
//
// Step 4 is what makes a deploy cheap. Without it, a killed worker's jobs sit
// invisible until their leases expire -- up to a full visibility timeout of
// added latency per job, per rolling restart. Handing them back deliberately
// means another worker picks them up in milliseconds, and because the nack is
// flagged as a drain rather than a failure, no retry is consumed.
func (p *Pool) Run(ctx context.Context) error {
	p.log.Info("worker pool starting",
		"queues", p.opts.Queues,
		"concurrency", p.opts.Concurrency,
		"fetch_batch", p.opts.FetchBatch,
		"job_timeout", p.opts.JobTimeout,
		"heartbeat", p.opts.HeartbeatInterval)

	var workers sync.WaitGroup
	for i := 0; i < p.opts.Concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			p.workerLoop()
		}()
	}

	// The heartbeater outlives the fetcher: leases must keep being renewed
	// throughout the drain, or jobs still finishing would be reaped out from
	// under a worker that is about to complete them.
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	var heartbeater sync.WaitGroup
	heartbeater.Add(1)
	go func() {
		defer heartbeater.Done()
		p.heartbeatLoop(heartbeatCtx)
	}()

	p.fetchLoop(ctx)

	// Fetching has stopped. Let the workers finish what they hold.
	close(p.jobs)

	drained := make(chan struct{})
	go func() {
		workers.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		p.log.Info("all in-flight jobs completed during drain")
	case <-time.After(p.opts.DrainTimeout):
		p.log.Warn("drain timeout reached; returning in-flight jobs to the queue",
			"timeout", p.opts.DrainTimeout,
			"still_running", p.stats.inFlight.Load())
		p.releaseInFlight()
		<-drained
	}

	stopHeartbeat()
	heartbeater.Wait()

	s := p.Stats()
	p.log.Info("worker pool stopped",
		"completed", s.Completed, "failed", s.Failed,
		"lease_lost", s.LeaseLost, "panics", s.Panics)

	return nil
}

// fetchLoop pulls work while there is capacity to run it.
func (p *Pool) fetchLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		// Block until at least one slot is free, then take every other free
		// slot without blocking. Requesting exactly the available capacity is
		// the backpressure mechanism: a saturated worker asks for nothing, so
		// its share of the queue stays available to workers that can run it.
		acquired, ok := p.acquireSlots(ctx)
		if !ok {
			return
		}
		if acquired == 0 {
			continue
		}

		res, err := p.opts.Client.Dequeue(ctx, p.opts.Queues, acquired, p.opts.WorkerID)
		if err != nil {
			p.releaseSlots(acquired)
			if ctx.Err() != nil {
				return
			}
			// A dequeue failure is usually the queue service restarting. Back
			// off and retry rather than exiting -- a worker that gives up on
			// the first error would need manual restarting after any deploy.
			p.log.Warn("dequeue failed, backing off", "error", err)
			if !sleepCtx(ctx, p.opts.EmptyPollInterval) {
				return
			}
			continue
		}

		// Return slots for jobs that were not granted.
		if unused := acquired - len(res.Jobs); unused > 0 {
			p.releaseSlots(unused)
		}

		if len(res.Jobs) == 0 {
			if !sleepCtx(ctx, p.idleWait(res)) {
				return
			}
			continue
		}

		for _, job := range res.Jobs {
			select {
			case p.jobs <- job:
			case <-ctx.Done():
				// Shutting down with a job in hand. Give it straight back
				// rather than letting its lease expire.
				p.returnJob(job, "worker shut down before starting the job")
				p.releaseSlots(1)
				return
			}
		}
	}
}

// idleWait decides how long to pause after an empty dequeue.
//
// The server explains why it returned nothing, so the client does not have to
// guess. Rate-limited means wait precisely as long as the token bucket needs;
// paused means an operator intervened and polling fast helps nobody.
func (p *Pool) idleWait(res *DequeueResult) time.Duration {
	if res.RetryAfter > 0 {
		return res.RetryAfter
	}
	if res.Paused {
		return 2 * time.Second
	}
	return p.opts.EmptyPollInterval
}

// acquireSlots blocks for one slot, then takes any others that are free.
func (p *Pool) acquireSlots(ctx context.Context) (int, bool) {
	select {
	case <-p.slots:
	case <-ctx.Done():
		return 0, false
	}

	acquired := 1
	for acquired < p.opts.FetchBatch {
		select {
		case <-p.slots:
			acquired++
		default:
			return acquired, true
		}
	}
	return acquired, true
}

func (p *Pool) releaseSlots(n int) {
	for i := 0; i < n; i++ {
		select {
		case p.slots <- struct{}{}:
		default:
			// Unreachable: never more tokens are returned than were taken.
			// Guarded anyway so a bookkeeping bug cannot deadlock the fetcher.
			p.log.Error("slot budget overflow; ignoring surplus token")
			return
		}
	}
}

// workerLoop executes jobs until the channel closes.
func (p *Pool) workerLoop() {
	for job := range p.jobs {
		p.execute(job)
		p.releaseSlots(1)
	}
}

// execute runs one job and reports the outcome.
func (p *Pool) execute(job jobtypes.Envelope) {
	// Background, not the run context: a job that has started must not be
	// cancelled merely because shutdown began. It gets DrainTimeout to finish,
	// and only then is it handed back deliberately.
	jobCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	timeoutCtx, cancelTimeout := context.WithTimeout(jobCtx, p.opts.JobTimeout)
	defer cancelTimeout()

	ctx := logx.WithJob(
		logx.WithWorker(
			logx.WithTrace(timeoutCtx, job.TraceID),
			p.opts.WorkerID),
		job.ID)

	running := &runningJob{envelope: job, cancel: cancel, started: time.Now()}
	p.inflight.Store(job.ID, running)
	p.stats.inFlight.Add(1)

	defer func() {
		p.inflight.Delete(job.ID)
		p.stats.inFlight.Add(-1)
	}()

	result, err := p.runHandler(ctx, job)
	duration := time.Since(running.started)

	switch {
	case err == nil:
		p.stats.completed.Add(1)
		p.ack(job, result, duration)

	case errors.Is(context.Cause(jobCtx), ErrLeaseLost):
		// The lease was reclaimed mid-execution. Reporting anything would be
		// rejected on ownership, and could disturb the worker that now owns it.
		// Drop the work silently but count it, so lease loss is measurable.
		p.stats.leaseLost.Add(1)
		p.log.WarnContext(ctx, "abandoned job after losing its lease",
			"job_type", job.Type, "attempt", job.Attempt, "ran_for", duration)

	case errors.Is(context.Cause(jobCtx), ErrDraining):
		// releaseInFlight already handed this job back. Nacking again would be
		// a duplicate report against a lease this worker no longer holds.
		p.log.InfoContext(ctx, "job stopped by drain and already returned",
			"job_type", job.Type, "attempt", job.Attempt, "ran_for", duration)

	case errors.Is(err, context.DeadlineExceeded):
		p.stats.failed.Add(1)
		p.nack(job, NackRequest{
			Error:    fmt.Sprintf("exceeded the %s job timeout", p.opts.JobTimeout),
			Outcome:  jobtypes.OutcomeTimeout,
			Duration: duration,
		})

	default:
		p.stats.failed.Add(1)
		p.nack(job, NackRequest{
			Error: err.Error(),
			// The handler's classification decides retry-versus-dead-letter.
			Permanent: jobtypes.IsPermanent(err),
			Outcome:   jobtypes.OutcomeFailed,
			Duration:  duration,
		})
	}
}

// runHandler invokes the handler, converting a panic into an error.
//
// A panicking handler must not take down the process. One bad image, or one nil
// dereference on an unusual payload, would otherwise kill every other job the
// worker was running -- all of which then have to be reaped and retried
// elsewhere. Recovery turns a fleet-wide event into one failed job.
func (p *Pool) runHandler(
	ctx context.Context,
	job jobtypes.Envelope,
) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			p.stats.panics.Add(1)
			stack := debug.Stack()
			p.log.ErrorContext(ctx, "handler panicked",
				"job_type", job.Type, "panic", r, "stack", string(stack))
			// Permanent: a panic is a code bug, and retrying the same input
			// against the same code will panic identically.
			err = jobtypes.Permanentf("handler panicked: %v", r)
		}
	}()

	return p.opts.Registry.Dispatch(ctx, job)
}

// ack reports success, using a fresh context so a shutdown in progress cannot
// prevent completed work from being recorded.
func (p *Pool) ack(job jobtypes.Envelope, result []byte, duration time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.opts.Client.Ack(ctx, job.ID, p.opts.WorkerID, result, duration); err != nil {
		// The job ran successfully but the ack did not land, so its lease will
		// expire and it will run again. At-least-once delivery in action, and
		// the reason handlers should be idempotent.
		p.log.ErrorContext(ctx, "could not acknowledge a completed job; it will be retried",
			"job_id", job.ID, "job_type", job.Type, "error", err)
	}
}

// nack reports failure.
func (p *Pool) nack(job jobtypes.Envelope, req NackRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req.JobID = job.ID
	req.WorkerID = p.opts.WorkerID

	if err := p.opts.Client.Nack(ctx, req); err != nil {
		p.log.ErrorContext(ctx, "could not report a failed job; its lease will expire",
			"job_id", job.ID, "job_type", job.Type, "error", err)
	}
}

// returnJob hands a leased-but-unstarted job straight back.
func (p *Pool) returnJob(job jobtypes.Envelope, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := p.opts.Client.Nack(ctx, NackRequest{
		JobID:              job.ID,
		WorkerID:           p.opts.WorkerID,
		Error:              reason,
		RequeueImmediately: true,
		Outcome:            jobtypes.OutcomeCancelled,
	})
	if err != nil {
		p.log.Warn("could not return an unstarted job; its lease will expire",
			"job_id", job.ID, "error", err)
	}
}

// releaseInFlight hands back every job still running at the drain deadline.
func (p *Pool) releaseInFlight() {
	p.inflight.Range(func(_, v any) bool {
		running, ok := v.(*runningJob)
		if !ok {
			return true
		}

		// Stop the handler first, then return the job, so the two workers do
		// not briefly run it at once. The cause marks this job as already
		// reported, so the cancelled handler's error path does not nack it a
		// second time.
		running.cancel(ErrDraining)
		p.returnJob(running.envelope, "worker drained before the job finished")
		return true
	})
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
