package leader_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxzixiaoxu/Origin/internal/testsupport"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/leader"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testsupport.Shutdown()
	os.Exit(code)
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newElector(t *testing.T, env *testsupport.Env, instance, key string, ttl time.Duration) *leader.Elector {
	t.Helper()

	e, err := leader.New(leader.Options{
		Redis:         env.Redis,
		Key:           key,
		FenceKey:      "test:fence",
		InstanceID:    instance,
		TTL:           ttl,
		RetryInterval: 50 * time.Millisecond,
		Log:           quietLog(),
	})
	if err != nil {
		t.Fatalf("create elector %s: %v", instance, err)
	}
	return e
}

// Under normal operation exactly one of several candidates does the work. This
// is the property the lock exists to provide -- not as a safety guarantee, but
// as the optimisation that keeps duplicate work rare.
func TestOnlyOneCandidateRunsAtATime(t *testing.T) {
	env := testsupport.Start(t)
	env.Reset(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		concurrent atomic.Int32
		maxSeen    atomic.Int32
		totalRuns  atomic.Int32
		wg         sync.WaitGroup
	)

	work := func(runCtx context.Context) error {
		n := concurrent.Add(1)
		defer concurrent.Add(-1)

		// Track the high-water mark of simultaneous workers.
		for {
			prev := maxSeen.Load()
			if n <= prev || maxSeen.CompareAndSwap(prev, n) {
				break
			}
		}
		totalRuns.Add(1)

		<-runCtx.Done()
		return runCtx.Err()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		e := newElector(t, env, "candidate-"+string(rune('a'+i)),
			"test:lock:solo", 3*time.Second)
		go func() {
			defer wg.Done()
			_ = e.Run(ctx, work)
		}()
	}

	time.Sleep(600 * time.Millisecond)
	cancel()
	wg.Wait()

	if got := maxSeen.Load(); got != 1 {
		t.Errorf("%d candidates ran simultaneously, want 1", got)
	}
	if got := totalRuns.Load(); got != 1 {
		t.Errorf("work started %d times, want 1", got)
	}
}

// When the leader goes away, a follower must take over promptly. Without this,
// a single replica restart would silently disable reaping until it came back --
// crash recovery would stop working and nothing would report it.
func TestFollowerTakesOverWhenLeaderStops(t *testing.T) {
	env := testsupport.Start(t)
	env.Reset(t)

	const key = "test:lock:failover"

	leaderCtx, stopLeader := context.WithCancel(context.Background())
	followerCtx, stopFollower := context.WithCancel(context.Background())
	defer stopFollower()

	var (
		firstRan  = make(chan string, 4)
		wg        sync.WaitGroup
		announced sync.Map
	)

	announce := func(name string) func(context.Context) error {
		return func(ctx context.Context) error {
			if _, loaded := announced.LoadOrStore(name, true); !loaded {
				firstRan <- name
			}
			<-ctx.Done()
			return ctx.Err()
		}
	}

	e1 := newElector(t, env, "instance-1", key, 3*time.Second)
	wg.Add(1)
	go func() { defer wg.Done(); _ = e1.Run(leaderCtx, announce("instance-1")) }()

	// Let instance-1 win before instance-2 starts campaigning.
	select {
	case name := <-firstRan:
		if name != "instance-1" {
			t.Fatalf("unexpected first leader %q", name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no leader was elected")
	}

	e2 := newElector(t, env, "instance-2", key, 3*time.Second)
	wg.Add(1)
	go func() { defer wg.Done(); _ = e2.Run(followerCtx, announce("instance-2")) }()

	// instance-2 must stay a follower while instance-1 holds the lock.
	select {
	case name := <-firstRan:
		t.Fatalf("%q started work while another instance held the lock", name)
	case <-time.After(400 * time.Millisecond):
	}

	// Leader shuts down cleanly. Release-on-shutdown means the successor should
	// take over almost immediately rather than waiting out the full TTL.
	start := time.Now()
	stopLeader()

	select {
	case name := <-firstRan:
		if name != "instance-2" {
			t.Fatalf("unexpected successor %q", name)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("failover took %s; a clean shutdown should release the "+
				"lock rather than let it expire", elapsed)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("no successor took over after the leader stopped")
	}

	stopFollower()
	wg.Wait()
}

// Losing the lock must cancel the work context.
//
// This is what stops a demoted replica from continuing to act as leader. The
// package cannot prevent the overlap window -- no Redis lock can -- but it can
// make the window as short as one renewal interval by turning lock loss into
// ordinary context cancellation.
func TestLosingLockCancelsTheWorkContext(t *testing.T) {
	env := testsupport.Start(t)
	env.Reset(t)

	const key = "test:lock:revoked"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	cancelled := make(chan struct{})

	e := newElector(t, env, "instance-1", key, 3*time.Second)

	var once sync.Once
	go func() {
		_ = e.Run(ctx, func(runCtx context.Context) error {
			once.Do(func() { close(started) })
			<-runCtx.Done()
			select {
			case <-cancelled:
			default:
				close(cancelled)
			}
			return runCtx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("work never started")
	}

	if !e.IsLeader() {
		t.Error("IsLeader() is false while the instance is serving")
	}
	if e.Fence() == 0 {
		t.Error("no fencing token was issued for the leadership term")
	}

	// Simulate the lock being taken by someone else -- the observable effect of
	// the GC-pause scenario in the package docs.
	if err := env.Redis.Set(ctx, key, "someone-else", 30*time.Second).Err(); err != nil {
		t.Fatalf("steal lock: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(6 * time.Second):
		t.Fatal("work was not cancelled after the lock was lost; a demoted " +
			"replica would keep acting as leader indefinitely")
	}
}

// Fencing tokens must increase monotonically across terms. They cannot be
// enforced at the Redis resource, but a strictly increasing value is what makes
// two overlapping leaders distinguishable in the logs.
func TestFencingTokensIncreaseAcrossTerms(t *testing.T) {
	env := testsupport.Start(t)
	env.Reset(t)

	const key = "test:lock:fencing"

	var tokens []int64

	for term := 0; term < 3; term++ {
		ctx, cancel := context.WithCancel(context.Background())

		e := newElector(t, env, "instance-1", key, 3*time.Second)
		got := make(chan int64, 1)

		go func() {
			_ = e.Run(ctx, func(runCtx context.Context) error {
				got <- e.Fence()
				<-runCtx.Done()
				return runCtx.Err()
			})
		}()

		select {
		case tok := <-got:
			tokens = append(tokens, tok)
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("term %d never acquired the lock", term)
		}

		cancel()
		// Let the release land so the next term is not delayed by the TTL.
		time.Sleep(150 * time.Millisecond)
	}

	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			t.Errorf("fencing token did not advance between terms: %v", tokens)
			break
		}
	}
}

// A shutting-down leader must not delete a lock another instance already holds.
// An unconditional DEL on shutdown would do exactly that, causing a second,
// needless election immediately after the first.
func TestReleaseDoesNotStealAnotherHoldersLock(t *testing.T) {
	env := testsupport.Start(t)
	env.Reset(t)

	const key = "test:lock:release"

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	e := newElector(t, env, "instance-1", key, 3*time.Second)

	var once sync.Once
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Run(ctx, func(runCtx context.Context) error {
			once.Do(func() { close(started) })
			<-runCtx.Done()
			return runCtx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("work never started")
	}

	// Another instance takes the lock while this one still thinks it is leader.
	if err := env.Redis.Set(ctx, key, "instance-2", time.Minute).Err(); err != nil {
		t.Fatalf("simulate takeover: %v", err)
	}

	cancel()
	<-done

	// instance-2's lock must survive instance-1's shutdown.
	val, err := env.Redis.Get(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("the departing instance deleted the new holder's lock: %v", err)
	}
	if val != "instance-2" {
		t.Errorf("lock value = %q, want instance-2", val)
	}
}

func TestRejectsTooShortTTL(t *testing.T) {
	env := testsupport.Start(t)

	_, err := leader.New(leader.Options{
		Redis: env.Redis,
		Key:   "test:lock:short",
		TTL:   100 * time.Millisecond,
	})
	if err == nil {
		t.Error("expected a sub-second TTL to be rejected: renewal happens at " +
			"TTL/3, so leadership would flap under ordinary scheduling jitter")
	}
}
