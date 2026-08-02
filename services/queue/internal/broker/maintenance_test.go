package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/broker"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// --- reaping: the central claim -------------------------------------------

// The headline guarantee: kill a worker mid-job and the job still runs.
//
// A worker leases a job and then vanishes -- no ack, no nack, no heartbeat,
// exactly what `docker kill` produces. Once the lease expires the reaper must
// reclaim it, hand it to another worker, and record what happened. If this ever
// fails, the "zero job loss under worker crash" claim is false.
func TestReaperRecoversJobFromCrashedWorker(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.configureQueue(t, &store.QueueConfig{
		Name:              "crashy",
		MaxConcurrency:    10,
		MaxAttempts:       5,
		VisibilityTimeout: 30 * time.Second,
		BackoffBase:       time.Second,
		BackoffCap:        time.Minute,
	})

	jobID := h.enqueue(t, "crashy", "image.derive")

	res, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"crashy"}, MaxJobs: 1, WorkerID: "doomed-worker",
	})
	if err != nil || len(res.Jobs) != 1 {
		t.Fatalf("dequeue: %d jobs, err %v", len(res.Jobs), err)
	}
	if res.Jobs[0].Attempt != 1 {
		t.Fatalf("first attempt = %d, want 1", res.Jobs[0].Attempt)
	}

	// The worker dies here. No ack, no nack, no heartbeat.

	// Before the lease expires, nothing should be recoverable -- reaping early
	// would steal jobs from workers that are merely slow.
	reaped, err := h.Reap(ctx, "crashy", 100)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if reaped.Requeued != 0 {
		t.Errorf("reaped %d jobs before the lease expired; a slow worker "+
			"would have its job stolen", reaped.Requeued)
	}

	h.advance(31 * time.Second)

	reaped, err = h.Reap(ctx, "crashy", 100)
	if err != nil {
		t.Fatalf("reap after expiry: %v", err)
	}
	if reaped.Requeued != 1 {
		t.Fatalf("reaped %d jobs, want 1", reaped.Requeued)
	}
	if reaped.DeadLettered != 0 {
		t.Errorf("dead-lettered %d jobs with retries remaining", reaped.DeadLettered)
	}

	// A different worker must be able to pick it up immediately.
	res, err = h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"crashy"}, MaxJobs: 1, WorkerID: "survivor",
	})
	if err != nil {
		t.Fatalf("dequeue after reap: %v", err)
	}
	if len(res.Jobs) != 1 {
		t.Fatalf("the crashed worker's job was not redelivered (got %d jobs)",
			len(res.Jobs))
	}
	if res.Jobs[0].ID != jobID {
		t.Errorf("redelivered job %s, want %s", res.Jobs[0].ID, jobID)
	}
	if got := res.Jobs[0].Attempt; got != 2 {
		t.Errorf("redelivered as attempt %d, want 2", got)
	}

	// The audit trail must distinguish a dead worker from a failing handler.
	attempts, err := h.store.ListAttempts(ctx, jobID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("recorded %d attempts, want 1", len(attempts))
	}
	if attempts[0].Outcome != jobtypes.OutcomeLeaseExpired {
		t.Errorf("outcome = %q, want %q: a crashed worker must be "+
			"distinguishable from a handler bug",
			attempts[0].Outcome, jobtypes.OutcomeLeaseExpired)
	}
	if attempts[0].WorkerID != "doomed-worker" {
		t.Errorf("attributed to %q, want doomed-worker: losing the identity of "+
			"the worker that died makes 'which host is unhealthy?' unanswerable",
			attempts[0].WorkerID)
	}
}

// A job whose worker dies on its final attempt has no retries left and must be
// dead-lettered rather than silently vanishing.
func TestReaperDeadLettersJobWithNoAttemptsLeft(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.configureQueue(t, &store.QueueConfig{
		Name:              "lastchance",
		MaxConcurrency:    10,
		MaxAttempts:       1,
		VisibilityTimeout: 10 * time.Second,
		BackoffBase:       time.Second,
		BackoffCap:        time.Minute,
	})

	jobID := h.enqueue(t, "lastchance", "doomed")

	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"lastchance"}, MaxJobs: 1, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	h.advance(11 * time.Second)

	res, err := h.Reap(ctx, "lastchance", 100)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if res.DeadLettered != 1 {
		t.Errorf("dead-lettered %d, want 1", res.DeadLettered)
	}
	if res.Requeued != 0 {
		t.Errorf("requeued %d jobs that had no attempts left", res.Requeued)
	}

	job, err := h.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if job.Status != jobtypes.StatusDead {
		t.Errorf("status = %s, want dead", job.Status)
	}
}

// Reaping must be idempotent, because the leader lock cannot actually guarantee
// a single reaper. Two overlapping leaders running the same sweep must not
// requeue a job twice.
func TestReapIsIdempotentAcrossOverlappingLeaders(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.configureQueue(t, &store.QueueConfig{
		Name: "dup", MaxConcurrency: 10, MaxAttempts: 5,
		VisibilityTimeout: 5 * time.Second,
		BackoffBase:       time.Second, BackoffCap: time.Minute,
	})

	for i := 0; i < 5; i++ {
		h.enqueue(t, "dup", "noop")
	}
	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"dup"}, MaxJobs: 5, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	h.advance(6 * time.Second)

	first, err := h.Reap(ctx, "dup", 100)
	if err != nil {
		t.Fatalf("first reap: %v", err)
	}
	second, err := h.Reap(ctx, "dup", 100)
	if err != nil {
		t.Fatalf("second reap: %v", err)
	}

	if first.Requeued != 5 {
		t.Errorf("first reap requeued %d, want 5", first.Requeued)
	}
	if second.Requeued != 0 {
		t.Errorf("second reap requeued %d more; the sweep is not idempotent "+
			"and two leaders would duplicate work", second.Requeued)
	}

	// Exactly five jobs must be dispatchable -- not ten.
	depth, err := h.Depth(ctx, "dup")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if depth.Ready != 5 {
		t.Errorf("ready depth = %d, want 5: duplicate requeues inflated the queue",
			depth.Ready)
	}
}

// --- promotion ------------------------------------------------------------

func TestPromoterMovesDueJobsOnly(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	now := h.Now()
	soon, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "soon", RunAt: now.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "later", RunAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Neither is due yet.
	res, err := h.Promote(ctx, "default", 100)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if res.Promoted != 0 {
		t.Errorf("promoted %d jobs before they were due", res.Promoted)
	}

	h.advance(6 * time.Second)

	res, err = h.Promote(ctx, "default", 100)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if res.Promoted != 1 {
		t.Fatalf("promoted %d, want exactly the 1 that came due", res.Promoted)
	}

	got, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 10, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(got.Jobs) != 1 || got.Jobs[0].ID != soon.JobID {
		t.Errorf("wrong job became dispatchable: %+v", got.Jobs)
	}
}

// Promotion must score by the job's own run_at, not by when the promoter got
// around to it. Otherwise a promoter that falls behind -- a leader election, a
// restart -- stamps every delayed job as freshly enqueued and sends it to the
// back of its priority band, behind work submitted while it was waiting.
func TestPromotionPreservesOriginalOrderingWhenPromoterIsLate(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	now := h.Now()

	// Scheduled long ago; the promoter is late picking it up.
	overdue, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "overdue", RunAt: now.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("enqueue overdue: %v", err)
	}

	// Submitted now, same priority, while the overdue job waited.
	fresh, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "fresh",
	})
	if err != nil {
		t.Fatalf("enqueue fresh: %v", err)
	}

	if _, err := h.Promote(ctx, "default", 100); err != nil {
		t.Fatalf("promote: %v", err)
	}

	got, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 10, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(got.Jobs) != 2 {
		t.Fatalf("got %d jobs, want 2", len(got.Jobs))
	}
	if got.Jobs[0].ID != overdue.JobID {
		t.Errorf("the overdue job lost its place to one enqueued later; "+
			"promotion is scoring by promotion time instead of run_at "+
			"(first out was %s, wanted %s)", got.Jobs[0].ID, overdue.JobID)
	}
	if got.Jobs[1].ID != fresh.JobID {
		t.Errorf("unexpected second job %s", got.Jobs[1].ID)
	}
}

func TestPromoteReportsRemainingSoBacklogsDrainFast(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Must be enqueued in the future and then made due by advancing the clock.
	// A job submitted with a run_at already in the past goes straight to the
	// ready set at enqueue time and never enters the scheduled set at all, so
	// there would be nothing for the promoter to find.
	now := h.Now()
	for i := 0; i < 10; i++ {
		if _, err := h.Enqueue(ctx, broker.EnqueueRequest{
			Queue: "default", Type: "batched", RunAt: now.Add(time.Minute),
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	h.advance(2 * time.Minute)

	res, err := h.Promote(ctx, "default", 3)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if res.Promoted != 3 {
		t.Errorf("promoted %d, want the batch limit of 3", res.Promoted)
	}
	if res.Remaining != 7 {
		t.Errorf("remaining = %d, want 7; without this the runner would sleep "+
			"a full interval between batches", res.Remaining)
	}
}

// --- reconciliation -------------------------------------------------------

// Redis is a rebuildable cache, not a system of record. A total flush must cost
// throughput, not work. This is the FLUSHALL scenario from the README, run for
// real.
func TestReconcilerRebuildsEverythingAfterRedisFlush(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	var ids []string
	for i := 0; i < 12; i++ {
		ids = append(ids, h.enqueue(t, "default", "image.derive"))
	}
	// One scheduled for later, which must come back as scheduled rather than
	// being made immediately dispatchable.
	future, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "later", RunAt: h.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("enqueue scheduled: %v", err)
	}

	// Lose Redis entirely.
	if err := h.env.Redis.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}

	before, err := h.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if before.Total() != 0 {
		t.Fatalf("expected an empty redis after flush, found %+v", before)
	}

	res, err := h.Reconcile(ctx, 1000)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Restored != 13 {
		t.Errorf("restored %d jobs, want all 13", res.Restored)
	}

	after, err := h.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("depth after reconcile: %v", err)
	}
	if after.Ready != 12 {
		t.Errorf("ready = %d, want 12", after.Ready)
	}
	if after.Scheduled != 1 {
		t.Errorf("scheduled = %d, want 1: a future job must not be made "+
			"immediately dispatchable by reconciliation", after.Scheduled)
	}

	// Every restored job must actually be executable, with its payload intact.
	got, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 64, WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("dequeue after reconcile: %v", err)
	}
	if len(got.Jobs) != 12 {
		t.Fatalf("dequeued %d after reconcile, want 12", len(got.Jobs))
	}

	seen := map[string]bool{}
	for _, j := range got.Jobs {
		seen[j.ID] = true
		if j.Type != "image.derive" {
			t.Errorf("job %s restored with type %q", j.ID, j.Type)
		}
	}
	for _, id := range ids {
		if !seen[id] {
			t.Errorf("job %s was lost across the flush", id)
		}
	}
	if seen[future.JobID] {
		t.Error("the future-scheduled job became dispatchable early")
	}
}

// The gap in the reap path made concrete.
//
// reap.lua clears a lease and returns the ids; Go then requeues them. If the
// process dies in between, the job is in no Redis set while Postgres still says
// 'running' -- and there is no lease left to expire, so the reaper will never
// look at it again. Only the reconciler can find it.
func TestReconcilerRecoversJobStrandedBetweenReapAndRequeue(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.configureQueue(t, &store.QueueConfig{
		Name: "stranded", MaxConcurrency: 10, MaxAttempts: 5,
		VisibilityTimeout: 5 * time.Second,
		BackoffBase:       time.Second, BackoffCap: time.Minute,
	})

	jobID := h.enqueue(t, "stranded", "image.derive")

	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"stranded"}, MaxJobs: 1, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	h.advance(6 * time.Second)

	// Simulate a crash exactly between the two halves of reaping: the lease is
	// cleared but the job was never put back.
	keys := broker.KeysFor("stranded")
	if err := h.env.Redis.ZRem(ctx, keys.Leases(), jobID).Err(); err != nil {
		t.Fatalf("clear lease: %v", err)
	}
	if err := h.env.Redis.HDel(ctx, keys.Inflight(), jobID).Err(); err != nil {
		t.Fatalf("clear inflight: %v", err)
	}

	// The reaper cannot help: there is no lease left for it to find.
	reaped, err := h.Reap(ctx, "stranded", 100)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if reaped.Requeued != 0 {
		t.Fatalf("reaper unexpectedly recovered the job; the test no longer "+
			"exercises the stranded path (requeued %d)", reaped.Requeued)
	}

	res, err := h.Reconcile(ctx, 1000)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Stranded != 1 {
		t.Errorf("detected %d stranded jobs, want 1", res.Stranded)
	}

	got, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"stranded"}, MaxJobs: 1, WorkerID: "w2",
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(got.Jobs) != 1 || got.Jobs[0].ID != jobID {
		t.Fatalf("the stranded job was never recovered; nothing else in the " +
			"system would ever have noticed it")
	}
}

// Reconciliation must not disturb jobs Redis already knows about. Re-adding a
// leased job to the ready set would hand a running job to a second worker --
// turning a repair mechanism into a duplicate-execution bug.
func TestReconcilerLeavesHealthyStateAlone(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		h.enqueue(t, "default", "noop")
	}
	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 2, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	res, err := h.Reconcile(ctx, 1000)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Restored != 0 {
		t.Errorf("restored %d jobs that were already present", res.Restored)
	}

	depth, err := h.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if depth.Ready != 3 {
		t.Errorf("ready = %d, want 3", depth.Ready)
	}
	if depth.Running != 2 {
		t.Errorf("running = %d, want 2: reconciliation disturbed live leases",
			depth.Running)
	}
}

// --- rollups --------------------------------------------------------------

// Recomputing a rollup window must be idempotent, since two overlapping leaders
// can run it at once. Incremental counters would double-count permanently and
// invisibly; recompute-and-upsert cannot.
func TestRollupIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	jobID := h.enqueue(t, "default", "noop")
	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 1, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if err := h.Ack(ctx, broker.AckRequest{
		JobID: jobID, WorkerID: "w1", Duration: 120 * time.Millisecond,
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if _, err := h.Rollup(ctx, time.Hour); err != nil {
		t.Fatalf("first rollup: %v", err)
	}
	if _, err := h.Rollup(ctx, time.Hour); err != nil {
		t.Fatalf("second rollup: %v", err)
	}
	if _, err := h.Rollup(ctx, time.Hour); err != nil {
		t.Fatalf("third rollup: %v", err)
	}

	// Buckets are stamped by the database clock, not the harness clock, so the
	// read window is expressed in real time.
	stats, err := h.store.QueueStats(ctx, "default",
		time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}

	var succeeded, enqueued int
	for _, s := range stats {
		succeeded += s.Succeeded
		enqueued += s.Enqueued
	}
	if succeeded != 1 {
		t.Errorf("succeeded = %d after three rollups, want 1: the aggregation "+
			"is double-counting", succeeded)
	}
	if enqueued != 1 {
		t.Errorf("enqueued = %d after three rollups, want 1", enqueued)
	}
}

// --- depth ----------------------------------------------------------------

func TestDepthReportsEachStateSeparately(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		h.enqueue(t, "default", "noop")
	}
	if _, err := h.Enqueue(ctx, broker.EnqueueRequest{
		Queue: "default", Type: "later", RunAt: h.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("enqueue scheduled: %v", err)
	}
	if _, err := h.Dequeue(ctx, broker.DequeueRequest{
		Queues: []string{"default"}, MaxJobs: 2, WorkerID: "w1",
	}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	depth, err := h.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if depth.Ready != 2 {
		t.Errorf("ready = %d, want 2", depth.Ready)
	}
	if depth.Scheduled != 1 {
		t.Errorf("scheduled = %d, want 1", depth.Scheduled)
	}
	if depth.Running != 2 {
		t.Errorf("running = %d, want 2", depth.Running)
	}
	if depth.Total() != 5 {
		t.Errorf("total = %d, want 5", depth.Total())
	}
}
