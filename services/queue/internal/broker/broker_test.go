package broker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/maxzixiaoxu/Origin/internal/testsupport"
	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/broker"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testsupport.Shutdown()
	os.Exit(code)
}

// harness bundles a broker with the store and a controllable clock.
type harness struct {
	*broker.Broker
	env   *testsupport.Env
	store *store.Store

	mu  sync.Mutex
	now time.Time
}

// advance moves the injected clock forward. Lease expiry is a function of time,
// and testing it by sleeping would make the suite slow and flaky in exactly the
// place where correctness matters most.
func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	h.now = h.now.Add(d)
	h.mu.Unlock()
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	env := testsupport.Start(t)
	env.Reset(t)

	h := &harness{
		env:   env,
		store: store.NewWithPool(env.Pool),
		now:   time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC),
	}

	b, err := broker.New(context.Background(), broker.Options{
		Redis:           env.Redis,
		Store:           h.store,
		Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxDequeueBatch: 64,
		// Zero TTL on the config cache: tests change queue settings and expect
		// the next call to see them, and a cache is not what is under test here.
		QueueConfigTTL: time.Nanosecond,
		Now: func() time.Time {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.now
		},
	})
	if err != nil {
		t.Fatalf("create broker: %v", err)
	}
	h.Broker = b
	return h
}

func (h *harness) configureQueue(t *testing.T, cfg *store.QueueConfig) {
	t.Helper()
	if _, _, err := h.store.UpsertQueue(context.Background(), cfg); err != nil {
		t.Fatalf("configure queue %s: %v", cfg.Name, err)
	}
	h.InvalidateQueue(cfg.Name)
}

func (h *harness) enqueue(t *testing.T, queue, typ string) string {
	t.Helper()
	res, err := h.Enqueue(context.Background(), broker.EnqueueRequest{
		Queue: queue, Type: typ, Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return res.JobID
}

func intPtr(n int) *int { return &n }

// redisZ builds a sorted-set member for tests that stand in for the promoter.
func redisZ(member string, score float64) redis.Z {
	return redis.Z{Score: score, Member: member}
}

// --- the central correctness test -----------------------------------------

// The claim this system rests on: no job is ever delivered to two workers.
//
// Everything else -- leases, reaping, retries -- is built on the assumption
// that dequeue hands out each job exactly once even under heavy contention. A
// sequential test cannot check that, because the race window only exists when
// clients overlap. This runs 50 concurrent workers against a shared queue and
// asserts that the union of everything they received contains no duplicates and
// loses nothing.
//
// If dequeue.lua were ever split into separate ZPOPMIN and ZADD round trips,
// this is the test that would catch it.
func TestConcurrentDequeueNeverDeliversAJobTwice(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const (
		workers  = 50
		jobCount = 500
	)

	h.configureQueue(t, &store.QueueConfig{
		Name:              "race",
		MaxConcurrency:    100000, // not what is under test
		MaxAttempts:       5,
		VisibilityTimeout: 30 * time.Second,
		BackoffBase:       time.Second,
		BackoffCap:        time.Minute,
	})

	expected := make(map[string]bool, jobCount)
	for i := 0; i < jobCount; i++ {
		expected[h.enqueue(t, "race", "noop")] = true
	}

	var (
		mu       sync.Mutex
		received []string
		wg       sync.WaitGroup
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-%d", workerNum)

			// Each worker drains until the queue reports empty twice in a row,
			// so no worker exits while another is still mid-batch.
			emptyStreak := 0
			for emptyStreak < 3 {
				res, err := h.Dequeue(ctx, broker.DequeueRequest{
					Queues:   []string{"race"},
					MaxJobs:  7, // deliberately not a divisor of jobCount
					WorkerID: workerID,
				})
				if err != nil {
					t.Errorf("worker %s dequeue: %v", workerID, err)
					return
				}
				if len(res.Jobs) == 0 {
					emptyStreak++
					continue
				}
				emptyStreak = 0

				mu.Lock()
				for _, job := range res.Jobs {
					received = append(received, job.ID)
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	seen := make(map[string]int, len(received))
	for _, id := range received {
		seen[id]++
	}

	var duplicates []string
	for id, count := range seen {
		if count > 1 {
			duplicates = append(duplicates, fmt.Sprintf("%s delivered %d times", id, count))
		}
	}
	if len(duplicates) > 0 {
		t.Fatalf("dequeue is not atomic: %d jobs delivered more than once:\n  %v",
			len(duplicates), duplicates)
	}

	if len(received) != jobCount {
		t.Errorf("received %d jobs, want %d (%d lost)",
			len(received), jobCount, jobCount-len(received))
	}
	for id := range expected {
		if seen[id] == 0 {
			t.Errorf("job %s was enqueued but never delivered", id)
		}
	}
}

// --- idempotency ----------------------------------------------------------

// Concurrent submissions of the same idempotency key must produce exactly one
// job. This is the client-retries-a-timed-out-request scenario, and it is why
// deduplication is enforced by a unique index rather than a read-then-write
// check: both racers would pass the check and create two jobs.
func TestConcurrentEnqueueWithSameKeyCreatesOneJob(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const attempts = 25

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[string]int{}
		created int
	)

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := h.Enqueue(ctx, broker.EnqueueRequest{
				Queue:          "default",
				Type:           "charge.card",
				Payload:        []byte(`{"amount":100}`),
				IdempotencyKey: "order-4417",
			})
			if err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			mu.Lock()
			ids[res.JobID]++
			if !res.Deduplicated {
				created++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(ids) != 1 {
		t.Errorf("got %d distinct job ids, want exactly 1: %v", len(ids), ids)
	}
	if created != 1 {
		t.Errorf("%d callers were told they created the job, want exactly 1", created)
	}

	var count int
	if err := h.env.Pool.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE idempotency_key = 'order-4417'`,
	).Scan(&count); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if count != 1 {
		t.Errorf("%d rows in postgres, want 1", count)
	}
}

// A deduplicated enqueue must not touch Redis. Re-adding an already-running job
// to the ready set would create the exact duplicate execution the idempotency
// key exists to prevent.
func TestDeduplicatedEnqueueDoesNotRequeueRunningJob(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	first, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "t", IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 10, WorkerID: "w1",
	})
	if err != nil || len(res.Jobs) != 1 {
		t.Fatalf("dequeue: got %d jobs, err %v", len(res.Jobs), err)
	}

	// Resubmit while the job is in flight.
	second, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "t", IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if !second.Deduplicated {
		t.Error("resubmission was not reported as deduplicated")
	}
	if second.JobID != first.JobID {
		t.Errorf("got job id %s, want the original %s", second.JobID, first.JobID)
	}

	// Nothing should be dispatchable: the job is leased, not queued.
	again, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 10, WorkerID: "w2",
	})
	if err != nil {
		t.Fatalf("second dequeue: %v", err)
	}
	if len(again.Jobs) != 0 {
		t.Errorf("a running job was made dispatchable again by a deduplicated enqueue")
	}
}

// --- priority and ordering ------------------------------------------------

func TestDispatchOrderRespectsPriorityThenAge(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	type spec struct {
		label    string
		priority int
	}
	// Enqueued in an order unrelated to expected dispatch order.
	specs := []spec{
		{"bulk-first", 9},
		{"normal-first", 5},
		{"urgent-first", 0},
		{"normal-second", 5},
		{"urgent-second", 0},
	}

	labels := map[string]string{}
	for _, s := range specs {
		res, err := h.Enqueue(ctx, broker.EnqueueRequest{
			Queue: "default", Type: s.label, Priority: intPtr(s.priority),
		})
		if err != nil {
			t.Fatalf("enqueue %s: %v", s.label, err)
		}
		labels[res.JobID] = s.label
		// Distinct enqueue times so FIFO within a band is observable.
		h.advance(time.Millisecond)
	}

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 10, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	var got []string
	for _, job := range res.Jobs {
		got = append(got, labels[job.ID])
	}

	want := []string{"urgent-first", "urgent-second", "normal-first",
		"normal-second", "bulk-first"}
	if len(got) != len(want) {
		t.Fatalf("dequeued %d jobs (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d = %q, want %q (full order: %v)", i, got[i], want[i], got)
		}
	}
}

// --- concurrency ceiling --------------------------------------------------

// max_concurrency must cap in-flight jobs across ALL workers, not per worker.
// It is the backpressure valve protecting a downstream dependency, so a limit
// that scaled with worker count would be no limit at all.
func TestConcurrencyCeilingIsGlobalAcrossWorkers(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.configureQueue(t, &store.QueueConfig{
		Name:              "capped",
		MaxConcurrency:    3,
		MaxAttempts:       5,
		VisibilityTimeout: 30 * time.Second,
		BackoffBase:       time.Second,
		BackoffCap:        time.Minute,
	})

	for i := 0; i < 20; i++ {
		h.enqueue(t, "capped", "noop")
	}

	leased := 0
	for w := 0; w < 6; w++ {
		res, err := h.Dequeue(ctx, broker.DequeueRequest{
			Queues:   []string{"capped"},
			MaxJobs:  10,
			WorkerID: fmt.Sprintf("w%d", w),
		})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		leased += len(res.Jobs)

		if len(res.Jobs) == 0 && res.ThrottleReason != broker.ThrottleConcurrency {
			t.Errorf("worker %d got nothing with reason %q, want %q",
				w, res.ThrottleReason, broker.ThrottleConcurrency)
		}
	}

	if leased != 3 {
		t.Errorf("%d jobs leased across 6 workers, want the ceiling of 3", leased)
	}
}

// --- pausing --------------------------------------------------------------

// Pausing stops dispatch but must not reject submissions. Draining a
// misbehaving queue during an incident depends on the backlog accumulating
// safely rather than callers starting to fail.
func TestPausedQueueAcceptsEnqueuesButDispatchesNothing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.configureQueue(t, &store.QueueConfig{
		Name:              "paused-q",
		MaxConcurrency:    10,
		MaxAttempts:       5,
		VisibilityTimeout: 30 * time.Second,
		BackoffBase:       time.Second,
		BackoffCap:        time.Minute,
		Paused:            true,
	})

	if _, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "paused-q", Type: "noop",
	}); err != nil {
		t.Fatalf("enqueue to a paused queue must succeed: %v", err)
	}

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"paused-q"}, MaxJobs: 10, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(res.Jobs) != 0 {
		t.Errorf("paused queue dispatched %d jobs", len(res.Jobs))
	}
	if res.ThrottleReason != broker.ThrottlePaused {
		t.Errorf("throttle reason = %q, want %q", res.ThrottleReason, broker.ThrottlePaused)
	}

	// Resuming must release the accumulated backlog.
	if err := h.store.SetPaused(ctx, "paused-q", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	h.InvalidateQueue("paused-q")

	res, err = h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"paused-q"}, MaxJobs: 10, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue after resume: %v", err)
	}
	if len(res.Jobs) != 1 {
		t.Errorf("got %d jobs after resume, want the 1 that accumulated", len(res.Jobs))
	}
}

// --- lease ownership ------------------------------------------------------

// The scenario the ownership check exists for: a worker is presumed dead, its
// lease is taken, and it then comes back and tries to report success. It must
// be refused, or it would clear a lease it no longer holds -- deleting the
// second worker's claim and letting the job be handed to a third.
func TestAckFromWorkerThatLostItsLeaseIsRejected(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	jobID := h.enqueue(t, "default", "slow")

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "slow-worker",
	})
	if err != nil || len(res.Jobs) != 1 {
		t.Fatalf("dequeue: %d jobs, err %v", len(res.Jobs), err)
	}

	// Simulate the reaper reclaiming the lease and another worker taking it.
	if err := h.env.Redis.HSet(ctx,
		"q:{default}:inflight", jobID, "other-worker").Err(); err != nil {
		t.Fatalf("simulate reassignment: %v", err)
	}

	err = h.Ack(ctx, broker.AckRequest{JobID: jobID, WorkerID: "slow-worker"})
	if !errors.Is(err, broker.ErrLeaseLost) {
		t.Fatalf("ack from the displaced worker returned %v, want ErrLeaseLost", err)
	}

	// The rightful owner's lease must be intact.
	owner, err := h.env.Redis.HGet(ctx, "q:{default}:inflight", jobID).Result()
	if err != nil {
		t.Fatalf("read lease owner: %v", err)
	}
	if owner != "other-worker" {
		t.Errorf("lease owner is now %q; the stale ack clobbered the real owner", owner)
	}
}

func TestExtendLeasesReportsLostLeasesPerJob(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		h.enqueue(t, "default", "noop")
	}

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 3, WorkerID: "w1",
	})
	if err != nil || len(res.Jobs) != 3 {
		t.Fatalf("dequeue: %d jobs, err %v", len(res.Jobs), err)
	}

	ids := []string{res.Jobs[0].ID, res.Jobs[1].ID, res.Jobs[2].ID}

	// Reap the middle one out from under the worker.
	if err := h.env.Redis.ZRem(ctx, "q:{default}:leases", ids[1]).Err(); err != nil {
		t.Fatalf("simulate reap: %v", err)
	}
	if err := h.env.Redis.HDel(ctx, "q:{default}:inflight", ids[1]).Err(); err != nil {
		t.Fatalf("simulate reap: %v", err)
	}

	results, err := h.ExtendLeases(ctx, "w1", ids)
	if err != nil {
		t.Fatalf("extend leases: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results for 3 jobs", len(results))
	}

	byID := map[string]bool{}
	for _, r := range results {
		byID[r.JobID] = r.Extended
	}
	if !byID[ids[0]] || !byID[ids[2]] {
		t.Error("still-held leases were not extended")
	}
	if byID[ids[1]] {
		t.Error("the reaped lease was reported as extended; the worker would " +
			"keep running a job another worker now owns")
	}
}

// --- retry and dead-letter ------------------------------------------------

func TestNackSchedulesRetryWithBackoff(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	jobID := h.enqueue(t, "default", "flaky")

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w1",
	})
	if err != nil || len(res.Jobs) != 1 {
		t.Fatalf("dequeue: %v", err)
	}
	if got := res.Jobs[0].Attempt; got != 1 {
		t.Errorf("first execution reported attempt %d, want 1", got)
	}

	nack, err := h.Nack(ctx, broker.NackRequest{
		JobID: jobID, WorkerID: "w1", Error: "connection refused",
	})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if nack.Status != jobtypes.StatusScheduled {
		t.Errorf("status = %s, want scheduled", nack.Status)
	}
	if !nack.RetryAt.After(h.Now()) {
		t.Errorf("retry_at %s is not in the future (now %s)", nack.RetryAt, h.Now())
	}

	// The job belongs in the scheduled set, not the ready set.
	if n, _ := h.env.Redis.ZCard(ctx, "q:{default}:ready").Result(); n != 0 {
		t.Errorf("%d jobs are immediately dispatchable; backoff was skipped", n)
	}
	if n, _ := h.env.Redis.ZCard(ctx, "q:{default}:scheduled").Result(); n != 1 {
		t.Errorf("%d jobs in the scheduled set, want 1", n)
	}

	// The cached envelope must already advertise the next attempt number.
	raw, err := h.env.Redis.Get(ctx, "q:{default}:job:"+jobID).Result()
	if err != nil {
		t.Fatalf("read envelope: %v", err)
	}
	var env jobtypes.Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Attempt != 2 {
		t.Errorf("requeued envelope advertises attempt %d, want 2", env.Attempt)
	}
}

func TestPermanentErrorSkipsRetryLadder(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	jobID := h.enqueue(t, "default", "bad-payload")

	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	nack, err := h.Nack(ctx, broker.NackRequest{
		JobID: jobID, WorkerID: "w1",
		Error: "payload is not valid JSON", Permanent: true,
	})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if nack.Status != jobtypes.StatusDead {
		t.Errorf("status = %s, want dead on first permanent failure", nack.Status)
	}

	job, err := h.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("reload job: %v", err)
	}
	if job.Status != jobtypes.StatusDead {
		t.Errorf("durable status = %s, want dead", job.Status)
	}
	if job.Attempt != 1 {
		t.Errorf("attempt = %d, want 1: a permanent error must not burn retries",
			job.Attempt)
	}
}

func TestExhaustedAttemptsGoToDeadLetter(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.configureQueue(t, &store.QueueConfig{
		Name:              "short",
		MaxConcurrency:    10,
		MaxAttempts:       2,
		VisibilityTimeout: 30 * time.Second,
		BackoffBase:       time.Millisecond,
		BackoffCap:        time.Millisecond,
	})

	jobID := h.enqueue(t, "short", "always-fails")

	for attempt := 1; attempt <= 2; attempt++ {
		// Move the job from scheduled back to ready between attempts, which is
		// what the promoter does in production.
		if attempt > 1 {
			h.advance(time.Second)
			if err := h.env.Redis.ZRem(ctx, "q:{short}:scheduled", jobID).Err(); err != nil {
				t.Fatalf("promote: %v", err)
			}
			if err := h.env.Redis.ZAdd(ctx, "q:{short}:ready",
				redisZ(jobID, 1)).Err(); err != nil {
				t.Fatalf("promote: %v", err)
			}
		}

		res, err := h.Dequeue(ctx, broker.DequeueRequest{
			Queues: []string{"short"}, MaxJobs: 1, WorkerID: "w1",
		})
		if err != nil {
			t.Fatalf("dequeue attempt %d: %v", attempt, err)
		}
		if len(res.Jobs) != 1 {
			t.Fatalf("attempt %d: got %d jobs, want 1", attempt, len(res.Jobs))
		}

		nack, err := h.Nack(ctx, broker.NackRequest{
			JobID: jobID, WorkerID: "w1", Error: "still broken",
		})
		if err != nil {
			t.Fatalf("nack attempt %d: %v", attempt, err)
		}

		if attempt < 2 && nack.Status != jobtypes.StatusScheduled {
			t.Errorf("attempt %d: status %s, want scheduled", attempt, nack.Status)
		}
		if attempt == 2 && nack.Status != jobtypes.StatusDead {
			t.Errorf("final attempt: status %s, want dead", nack.Status)
		}
	}

	// Every attempt must appear in the audit trail.
	attempts, err := h.store.ListAttempts(ctx, jobID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 2 {
		t.Errorf("recorded %d attempts, want 2", len(attempts))
	}
}

// --- rehydration ----------------------------------------------------------

// Losing Redis must be a latency event, not a data-loss event. Here the cached
// envelope is deleted while the job stays queued -- the shape a FLUSHALL or an
// expired TTL leaves behind -- and the job must still execute with correct
// contents rebuilt from Postgres.
func TestDequeueRehydratesMissingEnvelopeFromPostgres(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	res, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue:    "default",
		Type:     "report.generate",
		Payload:  []byte(`{"month":"2026-07"}`),
		Priority: intPtr(2),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if err := h.env.Redis.Del(ctx, "q:{default}:job:"+res.JobID).Err(); err != nil {
		t.Fatalf("delete cached envelope: %v", err)
	}

	got, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(got.Jobs) != 1 {
		t.Fatalf("got %d jobs, want 1 rehydrated from postgres", len(got.Jobs))
	}

	job := got.Jobs[0]
	if job.Type != "report.generate" {
		t.Errorf("type = %q, want report.generate", job.Type)
	}
	if job.Priority != 2 {
		t.Errorf("priority = %d, want 2", job.Priority)
	}
	if job.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", job.Attempt)
	}
	assertJSONEqual(t, job.Payload, []byte(`{"month":"2026-07"}`))
}

// Payloads are stored as jsonb, which normalises them: whitespace is stripped,
// object keys are reordered, and duplicate keys collapse to the last value. So
// the bytes a worker receives are not necessarily the bytes the client sent.
//
// That is acceptable -- jsonb is what makes the dashboard able to filter on
// payload->>'user_id', which is worth far more than byte fidelity for a job
// argument. What would NOT be acceptable is the two dequeue paths disagreeing:
// if the cached envelope carried raw client bytes while rehydration returned
// normalised ones, a job would behave differently depending on whether Redis
// happened to still have it. Any handler doing something byte-sensitive, such
// as verifying a signature over the payload, would then fail only after a Redis
// restart -- the worst possible way to discover the inconsistency.
//
// Enqueue avoids that by building the Redis envelope from the row Postgres
// returns rather than from the client's buffer, so both paths emit identical
// bytes. This test pins that down.
func TestBothDequeuePathsReturnIdenticalPayloadBytes(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Deliberately messy: unsorted keys, extra whitespace, nesting.
	raw := []byte(`{"zebra": 1,   "alpha":{"n":  2},"middle":[3,4]}`)

	first, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "t", Payload: raw,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Path A: served from the Redis envelope.
	got, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w1",
	})
	if err != nil || len(got.Jobs) != 1 {
		t.Fatalf("cached dequeue: %d jobs, err %v", len(got.Jobs), err)
	}
	cached := append([]byte(nil), got.Jobs[0].Payload...)

	// Return the job to the queue, then remove its cached envelope so the next
	// dequeue must rebuild it from Postgres.
	if _, err := h.Nack(ctx, broker.NackRequest{
		JobID: first.JobID, WorkerID: "w1", RequeueImmediately: true,
	}); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if err := h.env.Redis.Del(ctx, "q:{default}:job:"+first.JobID).Err(); err != nil {
		t.Fatalf("delete cached envelope: %v", err)
	}

	// Path B: rehydrated from Postgres.
	got, err = h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w2",
	})
	if err != nil || len(got.Jobs) != 1 {
		t.Fatalf("rehydrated dequeue: %d jobs, err %v", len(got.Jobs), err)
	}
	rehydrated := got.Jobs[0].Payload

	if string(cached) != string(rehydrated) {
		t.Errorf("the two dequeue paths disagree on payload bytes:\n"+
			"  cached:      %s\n  rehydrated:  %s\n"+
			"a byte-sensitive handler would behave differently after a Redis restart",
			cached, rehydrated)
	}
	assertJSONEqual(t, cached, raw)
}

// assertJSONEqual compares two JSON documents semantically, ignoring the key
// ordering and whitespace that jsonb normalises away.
func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()

	var gotVal, wantVal any
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("got is not valid JSON (%s): %v", got, err)
	}
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("want is not valid JSON (%s): %v", want, err)
	}

	gotCanon, _ := json.Marshal(gotVal)
	wantCanon, _ := json.Marshal(wantVal)
	if string(gotCanon) != string(wantCanon) {
		t.Errorf("payload mismatch:\n  got:  %s\n  want: %s", gotCanon, wantCanon)
	}
}

// --- graceful drain -------------------------------------------------------

// A drain is not a failure. The job must return to ready with no backoff and no
// attempt consumed, or a rolling restart would slowly dead-letter healthy jobs.
func TestDrainRequeuesImmediatelyWithoutConsumingAnAttempt(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	jobID := h.enqueue(t, "default", "long-running")

	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	nack, err := h.Nack(ctx, broker.NackRequest{
		JobID: jobID, WorkerID: "w1",
		Error: "worker draining", RequeueImmediately: true,
	})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if nack.Status != jobtypes.StatusPending {
		t.Errorf("status = %s, want pending", nack.Status)
	}

	// Immediately dispatchable -- no waiting out a visibility timeout.
	if n, _ := h.env.Redis.ZCard(ctx, "q:{default}:ready").Result(); n != 1 {
		t.Errorf("%d jobs ready, want 1 available for instant pickup", n)
	}

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w2",
	})
	if err != nil || len(res.Jobs) != 1 {
		t.Fatalf("second worker could not pick up the drained job: %v", err)
	}
	if got := res.Jobs[0].Attempt; got != 1 {
		t.Errorf("attempt = %d after a drain, want still 1", got)
	}
}

// --- cancellation ---------------------------------------------------------

func TestCancelRemovesQueuedJob(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	jobID := h.enqueue(t, "default", "noop")

	cancelled, err := h.Cancel(ctx, jobID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !cancelled {
		t.Fatal("cancel reported no effect on a pending job")
	}

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 10, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(res.Jobs) != 0 {
		t.Errorf("a cancelled job was still dispatched")
	}

	job, err := h.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if job.Status != jobtypes.StatusCancelled {
		t.Errorf("status = %s, want cancelled", job.Status)
	}
}

// Cancelling a running job works by deleting its lease. The worker learns on
// its next heartbeat, which is the same path reaping uses.
func TestCancelRunningJobRevokesTheLease(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	jobID := h.enqueue(t, "default", "long")

	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if _, err := h.Cancel(ctx, jobID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	results, err := h.ExtendLeases(ctx, "w1", []string{jobID})
	if err != nil {
		t.Fatalf("extend leases: %v", err)
	}
	if len(results) != 1 || results[0].Extended {
		t.Error("the worker's heartbeat succeeded after cancellation; " +
			"it would keep running a cancelled job")
	}
}

// --- throttle reporting ---------------------------------------------------

func TestEmptyQueueReportsEmptyNotPaused(t *testing.T) {
	h := newHarness(t)

	res, err := h.Dequeue(context.Background(), broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 5, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if res.ThrottleReason != broker.ThrottleEmpty {
		t.Errorf("reason = %q, want %q", res.ThrottleReason, broker.ThrottleEmpty)
	}
}

// When several queues are polled and they disagree, the worker should hear the
// least discouraging answer -- otherwise one paused queue in its list would
// make it back off hard while another queue had work waiting.
func TestMultiQueueReportsLeastDiscouragingReason(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.configureQueue(t, &store.QueueConfig{
		Name: "halted", MaxConcurrency: 10, MaxAttempts: 5,
		VisibilityTimeout: 30 * time.Second,
		BackoffBase:       time.Second, BackoffCap: time.Minute,
		Paused: true,
	})

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues:   []string{"halted", "default"},
		MaxJobs:  5,
		WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if res.ThrottleReason != broker.ThrottleEmpty {
		t.Errorf("reason = %q, want %q: an empty queue in the list means the "+
			"worker should keep its normal cadence",
			res.ThrottleReason, broker.ThrottleEmpty)
	}
}
