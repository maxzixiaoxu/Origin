// Package leader provides single-writer election for the background loops.
//
// The promoter, reaper, and rollup must not run on every replica. Two promoters
// racing would not corrupt anything -- the scripts are idempotent -- but they
// would double the Redis load and make "how far behind is the promoter?"
// unanswerable.
//
// # This lock is not a mutex, and the system does not need it to be
//
// A Redis lock cannot provide mutual exclusion in the presence of process
// pauses. The failure is well known (Kleppmann, "How to do distributed locking",
// 2016) and unavoidable without a consensus system:
//
//  1. Replica A acquires the lock with a 15s TTL.
//  2. A enters a 20s GC pause, or its host is descheduled.
//  3. The lock expires. Replica B acquires it. B is now the leader.
//  4. A wakes up. It has not yet noticed anything is wrong and continues
//     acting as leader for up to one renewal interval.
//
// For a moment, two replicas both believe they hold the lock. No TTL tuning
// fixes this; it only shrinks the window. Fencing tokens are the standard
// remedy, but they only work if the *resource* rejects stale tokens -- and
// Redis sorted sets have no such capability.
//
// This system tolerates the overlap because every operation the elected loops
// perform is idempotent:
//
//	promote  ZADD to ready then ZREM from scheduled. Running it twice moves
//	         the same job to the same place with the same score.
//	reap     ZRANGEBYSCORE over expired leases. The second run sees an empty
//	         range because the first already cleared them.
//	requeue  ZADD NX, so a second placement is a no-op rather than a score
//	         reset (see requeue.lua -- this is where most of the idempotence
//	         actually lives).
//	rollup   INSERT ... ON CONFLICT DO UPDATE into a keyed bucket, so a
//	         recomputation overwrites with the same values.
//
// So the lock is an *optimisation* that keeps duplicate work rare, not a
// correctness mechanism. Correctness comes from the operations themselves. That
// is the honest framing, and it is why this file does not pretend to offer
// mutual exclusion.
//
// A fencing token is still issued and logged. It cannot be enforced at the
// Redis resource, but it makes split-brain *observable*: two leaders acting at
// once show up as two different token values in the logs, which turns an
// invisible condition into a greppable one.
package leader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// renewDivisor sets the renewal interval at TTL/3, so leadership survives two
// consecutive missed renewals before expiring. One missed renewal is ordinary
// jitter -- a slow Redis round trip, a scheduling delay -- and losing
// leadership over it would cause constant churn.
const renewDivisor = 3

// releaseScript deletes the lock only if this instance still owns it.
//
// A plain DEL would be a bug: if this instance already lost the lock to a
// timeout and a new leader acquired it, an unconditional DEL on shutdown would
// delete the *new* leader's lock, causing a second, needless election.
var releaseScript = redis.NewScript(`
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
`)

// renewScript extends the TTL only while this instance still owns the lock.
// Returns 0 when ownership was lost, which is how a paused leader discovers it
// has been replaced.
var renewScript = redis.NewScript(`
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('PEXPIRE', KEYS[1], ARGV[2])
    end
    return 0
`)

// Options configures an Elector.
type Options struct {
	Redis *redis.Client
	// Key is the lock key, typically broker.Lock("reaper").
	Key string
	// InstanceID identifies this replica in logs.
	InstanceID string
	// FenceKey names the monotonic counter issuing fencing tokens.
	FenceKey string
	// TTL is how long the lock survives without renewal.
	TTL time.Duration
	// RetryInterval is how often a follower attempts to acquire.
	RetryInterval time.Duration
	Log           *slog.Logger
}

// Elector campaigns for leadership and runs a function while it holds it.
type Elector struct {
	rdb        *redis.Client
	key        string
	fenceKey   string
	instanceID string
	ttl        time.Duration
	retry      time.Duration
	log        *slog.Logger

	// value is the unique token proving ownership of the current lock. It
	// combines instance id and fencing token so a log line identifies both who
	// holds the lock and which generation of leadership it is.
	value atomic.Value // string

	isLeader atomic.Bool
	fence    atomic.Int64
}

// New builds an Elector.
func New(opts Options) (*Elector, error) {
	if opts.Redis == nil {
		return nil, errors.New("leader: Redis client is required")
	}
	if opts.Key == "" {
		return nil, errors.New("leader: lock key is required")
	}
	if opts.TTL < time.Second {
		return nil, fmt.Errorf("leader: TTL %s is too short to survive renewal jitter", opts.TTL)
	}

	retry := opts.RetryInterval
	if retry <= 0 {
		retry = opts.TTL / renewDivisor
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	e := &Elector{
		rdb:        opts.Redis,
		key:        opts.Key,
		fenceKey:   opts.FenceKey,
		instanceID: opts.InstanceID,
		ttl:        opts.TTL,
		retry:      retry,
		log:        log,
	}
	e.value.Store("")
	return e, nil
}

// IsLeader reports whether this instance currently believes it holds the lock.
//
// "Believes" is the operative word: see the package documentation. Use it for
// reporting and health output, never as a guard around an operation that would
// be unsafe to run twice.
func (e *Elector) IsLeader() bool { return e.isLeader.Load() }

// Fence returns the fencing token of the current leadership term, or 0 when not
// leader.
func (e *Elector) Fence() int64 { return e.fence.Load() }

// Run campaigns for leadership until ctx is cancelled.
//
// While this instance is leader, fn runs with a context that is cancelled the
// moment leadership is lost. That cancellation is the important part: a loop
// that keeps running after its lock expired is precisely the split-brain window
// the package documentation describes, and propagating loss as ordinary context
// cancellation means the loops need no leadership logic of their own.
func (e *Elector) Run(ctx context.Context, fn func(context.Context) error) error {
	ticker := time.NewTicker(e.retry)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		acquired, err := e.acquire(ctx)
		if err != nil {
			e.log.WarnContext(ctx, "leader: could not attempt acquisition",
				"lock", e.key, "error", err)
		}

		if acquired {
			if err := e.serve(ctx, fn); err != nil && !errors.Is(err, context.Canceled) {
				e.log.ErrorContext(ctx, "leader: term ended with an error",
					"lock", e.key, "error", err)
			}
			// Fall through and campaign again immediately rather than waiting
			// out a tick: if this instance lost leadership because of a blip,
			// it should be a candidate right away.
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// acquire attempts to take the lock, issuing a fresh fencing token on success.
func (e *Elector) acquire(ctx context.Context) (bool, error) {
	// The token is minted before the SET so that the value stored in Redis
	// already carries it. Two overlapping leaders then hold visibly different
	// values, which is what makes the overlap greppable.
	fence, err := e.rdb.Incr(ctx, e.fenceKey).Result()
	if err != nil {
		return false, fmt.Errorf("issue fencing token: %w", err)
	}

	value := fmt.Sprintf("%s#%d", e.instanceID, fence)

	ok, err := e.rdb.SetNX(ctx, e.key, value, e.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("acquire lock %s: %w", e.key, err)
	}
	if !ok {
		return false, nil
	}

	e.value.Store(value)
	e.fence.Store(fence)
	e.isLeader.Store(true)

	e.log.InfoContext(ctx, "leader: acquired",
		"lock", e.key, "instance", e.instanceID, "fence", fence, "ttl", e.ttl)

	return true, nil
}

// serve runs fn and renews the lock until either finishes.
func (e *Elector) serve(ctx context.Context, fn func(context.Context) error) error {
	termCtx, cancelTerm := context.WithCancel(ctx)
	defer cancelTerm()

	defer func() {
		e.isLeader.Store(false)
		e.fence.Store(0)
		e.release()
	}()

	errCh := make(chan error, 1)
	go func() { errCh <- fn(termCtx) }()

	ticker := time.NewTicker(e.ttl / renewDivisor)
	defer ticker.Stop()

	for {
		select {
		case err := <-errCh:
			return err

		case <-termCtx.Done():
			// Wait for fn to unwind so a caller of Run knows the loop has
			// actually stopped before another replica takes over.
			<-errCh
			return termCtx.Err()

		case <-ticker.C:
			held, err := e.renew(termCtx)
			if err != nil {
				// A renewal error is not proof of loss -- it may be a transient
				// Redis hiccup. Keep serving; the TTL is the backstop, and a
				// subsequent renewal will either succeed or report loss.
				e.log.WarnContext(termCtx, "leader: renewal failed, still holding",
					"lock", e.key, "fence", e.fence.Load(), "error", err)
				continue
			}
			if !held {
				e.log.WarnContext(termCtx, "leader: lost the lock, stopping work",
					"lock", e.key, "instance", e.instanceID, "fence", e.fence.Load())
				cancelTerm()
				<-errCh
				return nil
			}
		}
	}
}

// renew extends the lock, reporting whether this instance still owns it.
func (e *Elector) renew(ctx context.Context) (bool, error) {
	value, _ := e.value.Load().(string)
	if value == "" {
		return false, nil
	}

	res, err := renewScript.Run(ctx, e.rdb,
		[]string{e.key}, value, e.ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// release drops the lock on shutdown so a successor can take over immediately
// instead of waiting out the TTL. Uses a fresh context because the term context
// is already cancelled by the time this runs.
func (e *Elector) release() {
	value, _ := e.value.Load().(string)
	if value == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := releaseScript.Run(ctx, e.rdb, []string{e.key}, value).Err(); err != nil {
		// Not worth escalating: the TTL will expire the lock shortly anyway.
		e.log.WarnContext(ctx, "leader: could not release lock, TTL will expire it",
			"lock", e.key, "error", err)
	}
	e.value.Store("")
}
