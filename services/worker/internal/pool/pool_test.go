package pool_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/services/worker/internal/handlers"
	"github.com/maxzixiaoxu/Origin/services/worker/internal/pool"
)

// The pool is tested against a fake queue rather than a real one. Everything
// worth checking here -- that capacity is never exceeded, that a lost lease
// cancels its handler, that a drain hands work back -- is a property of the
// pool's own goroutine choreography, and a real gRPC server would only add
// latency and flakiness between the assertion and the behaviour.

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeClient is a controllable stand-in for the queue service.
type fakeClient struct {
	mu sync.Mutex

	// available is the queue of jobs handed out on demand.
	available []jobtypes.Envelope

	acked  []string
	nacked []pool.NackRequest

	// lostLeases marks jobs whose next heartbeat reports the lease gone.
	lostLeases map[string]bool

	dequeueCalls atomic.Int64
	extendCalls  atomic.Int64

	// maxRequested records the largest batch ever asked for, which is what the
	// backpressure assertions check.
	maxRequested atomic.Int64
}

func newFakeClient(jobs ...jobtypes.Envelope) *fakeClient {
	return &fakeClient{available: jobs, lostLeases: map[string]bool{}}
}

func (f *fakeClient) Dequeue(
	_ context.Context, _ []string, maxJobs int, _ string,
) (*pool.DequeueResult, error) {
	f.dequeueCalls.Add(1)

	for {
		prev := f.maxRequested.Load()
		if int64(maxJobs) <= prev || f.maxRequested.CompareAndSwap(prev, int64(maxJobs)) {
			break
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.available) == 0 {
		return &pool.DequeueResult{}, nil
	}
	n := maxJobs
	if n > len(f.available) {
		n = len(f.available)
	}
	batch := f.available[:n]
	f.available = f.available[n:]

	return &pool.DequeueResult{Jobs: batch}, nil
}

func (f *fakeClient) Ack(
	_ context.Context, jobID, _ string, _ []byte, _ time.Duration,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, jobID)
	return nil
}

func (f *fakeClient) Nack(_ context.Context, req pool.NackRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacked = append(f.nacked, req)
	return nil
}

func (f *fakeClient) ExtendLeases(
	_ context.Context, _ string, jobIDs []string,
) ([]pool.LeaseResult, error) {
	f.extendCalls.Add(1)

	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]pool.LeaseResult, 0, len(jobIDs))
	for _, id := range jobIDs {
		out = append(out, pool.LeaseResult{JobID: id, Extended: !f.lostLeases[id]})
	}
	return out, nil
}

func (f *fakeClient) loseLease(jobID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lostLeases[jobID] = true
}

func (f *fakeClient) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acked...)
}

func (f *fakeClient) nackedRequests() []pool.NackRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pool.NackRequest(nil), f.nacked...)
}

func makeJobs(n int, jobType string) []jobtypes.Envelope {
	out := make([]jobtypes.Envelope, n)
	for i := range out {
		out[i] = jobtypes.Envelope{
			ID:          jobID(i),
			Queue:       "test",
			Type:        jobType,
			Payload:     []byte(`{}`),
			Attempt:     1,
			MaxAttempts: 3,
		}
	}
	return out
}

func jobID(i int) string {
	return "job-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
}

func newTestPool(t *testing.T, c pool.Client, r *handlers.Registry, mutate func(*pool.Options)) *pool.Pool {
	t.Helper()

	opts := pool.Options{
		Client:            c,
		Registry:          r,
		Log:               quietLog(),
		WorkerID:          "test-worker",
		Queues:            []string{"test"},
		Concurrency:       4,
		FetchBatch:        4,
		EmptyPollInterval: 10 * time.Millisecond,
		JobTimeout:        5 * time.Second,
		HeartbeatInterval: 50 * time.Millisecond,
		DrainTimeout:      2 * time.Second,
	}
	if mutate != nil {
		mutate(&opts)
	}

	p, err := pool.New(opts)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	return p
}

// --- backpressure ---------------------------------------------------------

// The core invariant: the pool never runs more jobs than its concurrency, and
// never asks for more than it can immediately start.
//
// Leasing work you cannot run is worse than not leasing it -- the lease ticks
// down while the job sits idle, expires, and gets handed to another worker, so
// the system does the work twice under load.
func TestNeverExceedsConcurrencyOrOverFetches(t *testing.T) {
	const concurrency = 4

	var (
		current atomic.Int64
		peak    atomic.Int64
	)

	registry := handlers.NewRegistry()
	registry.RegisterFunc("slow", func(ctx context.Context, _ jobtypes.Envelope) ([]byte, error) {
		n := current.Add(1)
		defer current.Add(-1)

		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}

		select {
		case <-time.After(30 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil, nil
	})

	client := newFakeClient(makeJobs(60, "slow")...)
	p := newTestPool(t, client, registry, func(o *pool.Options) {
		o.Concurrency = concurrency
		o.FetchBatch = concurrency
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	waitFor(t, 15*time.Second, func() bool {
		return len(client.ackedIDs()) == 60
	}, "all 60 jobs to complete")

	cancel()
	<-done

	if got := peak.Load(); got > concurrency {
		t.Errorf("%d handlers ran simultaneously, above the concurrency of %d",
			got, concurrency)
	}
	if got := client.maxRequested.Load(); got > concurrency {
		t.Errorf("requested a batch of %d with only %d slots; those jobs would "+
			"hold leases with nothing to run them", got, concurrency)
	}
	if got := len(client.ackedIDs()); got != 60 {
		t.Errorf("acked %d jobs, want 60", got)
	}
}

// A saturated pool must stop asking for work entirely, rather than polling and
// discarding.
func TestSaturatedPoolStopsRequestingWork(t *testing.T) {
	release := make(chan struct{})

	registry := handlers.NewRegistry()
	registry.RegisterFunc("block", func(ctx context.Context, _ jobtypes.Envelope) ([]byte, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	})

	client := newFakeClient(makeJobs(2, "block")...)
	p := newTestPool(t, client, registry, func(o *pool.Options) {
		o.Concurrency = 2
		o.FetchBatch = 2
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	// Let both slots fill.
	waitFor(t, 5*time.Second, func() bool {
		return p.Stats().InFlight == 2
	}, "the pool to saturate")

	callsWhenFull := client.dequeueCalls.Load()
	time.Sleep(300 * time.Millisecond) // many poll intervals

	if extra := client.dequeueCalls.Load() - callsWhenFull; extra > 0 {
		t.Errorf("a fully saturated pool made %d further dequeue calls; it "+
			"should be blocked on capacity, not polling", extra)
	}

	close(release)
	cancel()
	<-done
}

// --- lease loss -----------------------------------------------------------

// The mechanism that makes lease loss cheap: the heartbeater cancels the job's
// context, so a distributed failure surfaces as ordinary Go cancellation and
// any handler that respects ctx stops immediately -- with no lease-specific
// code in the handler at all.
func TestLostLeaseCancelsTheRunningHandler(t *testing.T) {
	started := make(chan struct{})
	var cancelled atomic.Bool

	registry := handlers.NewRegistry()
	registry.RegisterFunc("watch", func(ctx context.Context, _ jobtypes.Envelope) ([]byte, error) {
		close(started)
		<-ctx.Done()
		cancelled.Store(true)
		return nil, ctx.Err()
	})

	jobs := makeJobs(1, "watch")
	client := newFakeClient(jobs...)
	p := newTestPool(t, client, registry, func(o *pool.Options) {
		o.Concurrency = 1
		o.FetchBatch = 1
		o.HeartbeatInterval = 30 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	// The reaper takes the job while it is running.
	client.loseLease(jobs[0].ID)

	waitFor(t, 5*time.Second, func() bool { return cancelled.Load() },
		"the handler to be cancelled after losing its lease")

	waitFor(t, 5*time.Second, func() bool {
		return p.Stats().LeaseLost == 1
	}, "the lease loss to be counted")

	cancel()
	<-done

	// A job whose lease was lost must not be reported at all. Another worker
	// owns it now, and a stale nack could disturb that worker's state.
	if got := client.ackedIDs(); len(got) != 0 {
		t.Errorf("acked %v after losing the lease", got)
	}
	for _, n := range client.nackedRequests() {
		if n.JobID == jobs[0].ID {
			t.Errorf("nacked a job whose lease was lost: %+v", n)
		}
	}
}

func TestHeartbeatBatchesAllHeldLeases(t *testing.T) {
	release := make(chan struct{})

	registry := handlers.NewRegistry()
	registry.RegisterFunc("hold", func(ctx context.Context, _ jobtypes.Envelope) ([]byte, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	})

	client := newFakeClient(makeJobs(8, "hold")...)
	p := newTestPool(t, client, registry, func(o *pool.Options) {
		o.Concurrency = 8
		o.FetchBatch = 8
		o.HeartbeatInterval = 40 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool { return p.Stats().InFlight == 8 }, "8 jobs to start")

	before := client.extendCalls.Load()
	time.Sleep(200 * time.Millisecond) // ~5 heartbeat ticks
	calls := client.extendCalls.Load() - before

	// Eight jobs across roughly five ticks: one call per tick, not one per job.
	// Anything approaching 40 would mean per-job renewal.
	if calls > 12 {
		t.Errorf("%d ExtendLeases calls for 8 jobs over ~5 ticks; renewal is "+
			"not batched and RPC load scales with in-flight jobs", calls)
	}
	if calls == 0 {
		t.Error("no lease renewals were sent; held jobs would expire")
	}

	close(release)
	cancel()
	<-done
}

// --- failure handling -----------------------------------------------------

func TestHandlerErrorIsNackedWithClassification(t *testing.T) {
	registry := handlers.NewRegistry()
	registry.RegisterFunc("permanent", func(context.Context, jobtypes.Envelope) ([]byte, error) {
		return nil, jobtypes.Permanentf("payload is malformed")
	})
	registry.RegisterFunc("transient", func(context.Context, jobtypes.Envelope) ([]byte, error) {
		return nil, errors.New("connection refused")
	})

	jobs := []jobtypes.Envelope{
		{ID: "perm", Queue: "test", Type: "permanent", Payload: []byte(`{}`), Attempt: 1, MaxAttempts: 3},
		{ID: "temp", Queue: "test", Type: "transient", Payload: []byte(`{}`), Attempt: 1, MaxAttempts: 3},
	}
	client := newFakeClient(jobs...)
	p := newTestPool(t, client, registry, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		return len(client.nackedRequests()) == 2
	}, "both jobs to be nacked")

	cancel()
	<-done

	byID := map[string]pool.NackRequest{}
	for _, n := range client.nackedRequests() {
		byID[n.JobID] = n
	}

	if !byID["perm"].Permanent {
		t.Error("a permanent error was not flagged permanent; it would burn " +
			"the full retry ladder before dead-lettering")
	}
	if byID["temp"].Permanent {
		t.Error("an unclassified error was flagged permanent; it would be " +
			"dead-lettered without ever being retried")
	}
}

// A panicking handler must fail one job, not the process. Crashing would take
// down every other job the worker holds, all of which then need reaping.
func TestPanicIsContainedToOneJob(t *testing.T) {
	registry := handlers.NewRegistry()
	registry.RegisterFunc("boom", func(context.Context, jobtypes.Envelope) ([]byte, error) {
		panic("handler exploded")
	})
	registry.RegisterFunc("fine", func(context.Context, jobtypes.Envelope) ([]byte, error) {
		return []byte(`{"ok":true}`), nil
	})

	jobs := []jobtypes.Envelope{
		{ID: "boom-1", Queue: "test", Type: "boom", Payload: []byte(`{}`), Attempt: 1, MaxAttempts: 3},
		{ID: "fine-1", Queue: "test", Type: "fine", Payload: []byte(`{}`), Attempt: 1, MaxAttempts: 3},
		{ID: "fine-2", Queue: "test", Type: "fine", Payload: []byte(`{}`), Attempt: 1, MaxAttempts: 3},
	}
	client := newFakeClient(jobs...)
	p := newTestPool(t, client, registry, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		return len(client.ackedIDs()) == 2 && len(client.nackedRequests()) == 1
	}, "the healthy jobs to succeed and the panicking one to fail")

	cancel()
	<-done

	if got := p.Stats().Panics; got != 1 {
		t.Errorf("recorded %d panics, want 1", got)
	}
	nacked := client.nackedRequests()
	if len(nacked) != 1 || nacked[0].JobID != "boom-1" {
		t.Fatalf("unexpected nacks: %+v", nacked)
	}
	// A panic is a code bug: the same input against the same code panics again,
	// so retrying wastes the ladder.
	if !nacked[0].Permanent {
		t.Error("a panic was not classified permanent")
	}
}

func TestJobTimeoutIsReportedAsTimeout(t *testing.T) {
	registry := handlers.NewRegistry()
	registry.RegisterFunc("hang", func(ctx context.Context, _ jobtypes.Envelope) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	client := newFakeClient(makeJobs(1, "hang")...)
	p := newTestPool(t, client, registry, func(o *pool.Options) {
		o.Concurrency = 1
		o.FetchBatch = 1
		o.JobTimeout = 100 * time.Millisecond
		// Long enough that the heartbeater cannot interfere with the timeout.
		o.HeartbeatInterval = 10 * time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		return len(client.nackedRequests()) == 1
	}, "the job to time out")

	cancel()
	<-done

	n := client.nackedRequests()[0]
	if n.Outcome != jobtypes.OutcomeTimeout {
		t.Errorf("outcome = %q, want %q: a timeout must be distinguishable "+
			"from a handler error", n.Outcome, jobtypes.OutcomeTimeout)
	}
	if n.Permanent {
		t.Error("a timeout was marked permanent; a slow job may well succeed on retry")
	}
}

// --- graceful drain -------------------------------------------------------

// Jobs still running when the drain deadline passes must be handed back
// explicitly, not abandoned.
//
// Abandoning them means waiting out a full visibility timeout before another
// worker can start -- per job, per rolling restart. Handing them back costs
// milliseconds. The requeue is flagged as a drain rather than a failure, so no
// retry is consumed: otherwise enough deploys would slowly dead-letter healthy
// jobs.
func TestDrainHandsBackUnfinishedJobsWithoutConsumingRetries(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once

	registry := handlers.NewRegistry()
	registry.RegisterFunc("endless", func(ctx context.Context, _ jobtypes.Envelope) ([]byte, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	})

	client := newFakeClient(makeJobs(2, "endless")...)
	p := newTestPool(t, client, registry, func(o *pool.Options) {
		o.Concurrency = 2
		o.FetchBatch = 2
		o.DrainTimeout = 200 * time.Millisecond
		o.JobTimeout = time.Minute
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handlers never started")
	}
	waitFor(t, 5*time.Second, func() bool { return p.Stats().InFlight == 2 }, "both jobs running")

	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("pool did not finish draining")
	}

	nacked := client.nackedRequests()
	if len(nacked) != 2 {
		t.Fatalf("handed back %d jobs, want 2: the rest would sit invisible "+
			"until their leases expired", len(nacked))
	}
	for _, n := range nacked {
		if !n.RequeueImmediately {
			t.Errorf("job %s was not requeued immediately; another worker would "+
				"wait out a full visibility timeout", n.JobID)
		}
		if n.Permanent {
			t.Errorf("job %s was dead-lettered by a drain", n.JobID)
		}
		if n.Outcome != jobtypes.OutcomeCancelled {
			t.Errorf("job %s recorded outcome %q, want %q: a deploy is not a "+
				"job failure", n.JobID, n.Outcome, jobtypes.OutcomeCancelled)
		}
	}
}

func TestDrainWaitsForJobsThatFinishInTime(t *testing.T) {
	registry := handlers.NewRegistry()
	registry.RegisterFunc("brief", func(ctx context.Context, _ jobtypes.Envelope) ([]byte, error) {
		select {
		case <-time.After(80 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, nil
	})

	client := newFakeClient(makeJobs(2, "brief")...)
	p := newTestPool(t, client, registry, func(o *pool.Options) {
		o.Concurrency = 2
		o.FetchBatch = 2
		o.DrainTimeout = 3 * time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool { return p.Stats().InFlight == 2 }, "both jobs running")
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not complete")
	}

	if got := len(client.ackedIDs()); got != 2 {
		t.Errorf("acked %d jobs, want 2: in-flight work should finish within "+
			"the drain window rather than being handed back", got)
	}
	if got := client.nackedRequests(); len(got) != 0 {
		t.Errorf("handed back %d jobs that had time to finish", len(got))
	}
}

// --- configuration --------------------------------------------------------

func TestRejectsFetchBatchAboveConcurrency(t *testing.T) {
	_, err := pool.New(pool.Options{
		Client:      newFakeClient(),
		Registry:    handlers.NewRegistry(),
		WorkerID:    "w",
		Queues:      []string{"test"},
		Concurrency: 2,
		FetchBatch:  8,
	})
	if err == nil {
		t.Fatal("expected fetch batch > concurrency to be rejected")
	}
}

func TestUnknownJobTypeIsPermanent(t *testing.T) {
	client := newFakeClient(makeJobs(1, "no.such.handler")...)
	p := newTestPool(t, client, handlers.NewRegistry(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		return len(client.nackedRequests()) == 1
	}, "the unhandled job to fail")

	cancel()
	<-done

	// Retrying a job type this worker does not know is pointless -- it will not
	// learn the type. Dead-lettering surfaces the deploy mismatch immediately.
	if !client.nackedRequests()[0].Permanent {
		t.Error("an unknown job type was not classified permanent")
	}
}

// --- helpers --------------------------------------------------------------

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
