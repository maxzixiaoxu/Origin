package config

import (
	"strings"
	"testing"
	"time"
)

func TestEnvHelpersFallBackOnJunk(t *testing.T) {
	// A typo in an env var must not crash a service at boot or, worse, silently
	// produce a zero value. Falling back to the documented default keeps the
	// service running with known-good settings.
	t.Setenv("JOBQ_TEST_INT", "not-a-number")
	if got := Int("JOBQ_TEST_INT", 42); got != 42 {
		t.Errorf("Int with junk = %d, want fallback 42", got)
	}

	t.Setenv("JOBQ_TEST_DUR", "5 fortnights")
	if got := Dur("JOBQ_TEST_DUR", time.Second); got != time.Second {
		t.Errorf("Dur with junk = %s, want fallback 1s", got)
	}

	t.Setenv("JOBQ_TEST_BOOL", "yes-ish")
	if got := Bool("JOBQ_TEST_BOOL", true); got != true {
		t.Errorf("Bool with junk = %v, want fallback true", got)
	}

	// Whitespace-only is treated as unset, since that is what an empty
	// docker-compose interpolation produces.
	t.Setenv("JOBQ_TEST_STR", "   ")
	if got := Str("JOBQ_TEST_STR", "fallback"); got != "fallback" {
		t.Errorf("Str with blank = %q, want fallback", got)
	}
}

func TestListParsing(t *testing.T) {
	t.Setenv("JOBQ_TEST_LIST", "critical, default ,,bulk")
	got := List("JOBQ_TEST_LIST", nil)

	want := []string{"critical", "default", "bulk"}
	if len(got) != len(want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("List[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// A list that parses to nothing must fall back rather than leaving a worker
	// subscribed to zero queues and idling forever while looking healthy.
	t.Setenv("JOBQ_TEST_LIST", " , , ")
	if got := List("JOBQ_TEST_LIST", []string{"default"}); len(got) != 1 || got[0] != "default" {
		t.Errorf("List of separators = %v, want fallback [default]", got)
	}
}

func TestWorkerDefaultsAreValid(t *testing.T) {
	c, err := LoadWorker()
	if err != nil {
		t.Fatalf("default worker config is invalid: %v", err)
	}
	if c.ID == "" {
		t.Error("worker ID must default to something unique, not empty")
	}
}

func TestQueueServiceDefaultsAreValid(t *testing.T) {
	if _, err := LoadQueueService(); err != nil {
		t.Fatalf("default queue-service config is invalid: %v", err)
	}
}

// The most important validation in the package.
//
// If a worker may fetch more jobs than it has goroutines to run, the surplus
// sits in a local buffer holding leases that are already ticking down. Those
// leases expire while the jobs have never started, the reaper hands them to
// another worker, and the system does redundant work under load -- precisely
// when it can least afford it. Rejecting the config at boot is the only place
// this is cheap to catch.
func TestFetchBatchMayNotExceedConcurrency(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "4")
	t.Setenv("WORKER_FETCH_BATCH", "16")

	_, err := LoadWorker()
	if err == nil {
		t.Fatal("expected fetch batch > concurrency to be rejected")
	}
	if !strings.Contains(err.Error(), "no slot to run them") {
		t.Errorf("error should explain the consequence, got: %v", err)
	}
}

func TestFetchBatchEqualToConcurrencyIsAllowed(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "8")
	t.Setenv("WORKER_FETCH_BATCH", "8")

	if _, err := LoadWorker(); err != nil {
		t.Errorf("batch == concurrency should be allowed, got: %v", err)
	}
}

func TestWorkerRejectsNonPositiveConcurrency(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "0")
	if _, err := LoadWorker(); err == nil {
		t.Error("expected zero concurrency to be rejected; a worker that " +
			"processes nothing must not report itself healthy")
	}
}

// A leader lock renews at TTL/3. Set the TTL too low and ordinary scheduling
// jitter -- a GC pause, a slow Redis round trip -- causes leadership to flap,
// so the reaper and promoter restart constantly and make no steady progress.
func TestLeaderTTLFloor(t *testing.T) {
	t.Setenv("LEADER_TTL", "1s")

	_, err := LoadQueueService()
	if err == nil {
		t.Fatal("expected a sub-3s LEADER_TTL to be rejected")
	}
	if !strings.Contains(err.Error(), "renewal jitter") {
		t.Errorf("error should explain why, got: %v", err)
	}
}

func TestPostgresPoolBoundsAreCoherent(t *testing.T) {
	t.Setenv("POSTGRES_MIN_CONNS", "50")
	t.Setenv("POSTGRES_MAX_CONNS", "10")

	if _, err := LoadQueueService(); err == nil {
		t.Error("expected min > max connections to be rejected")
	}
}

func TestInstanceIDsAreUniquePerPrefix(t *testing.T) {
	// Two workers sharing an ID would each believe they own the other's leases,
	// which defeats the ownership check that makes ack/nack safe.
	a := defaultInstanceID("worker")
	if !strings.HasPrefix(a, "worker-") {
		t.Errorf("instance ID %q lacks its prefix", a)
	}
	if b := defaultInstanceID("queued"); strings.TrimPrefix(a, "worker") == strings.TrimPrefix(b, "queued") {
		// Same host and PID is expected within one test process; the prefix is
		// what must differ.
		t.Logf("same host/pid produces %q and %q (expected in-process)", a, b)
	}
}
