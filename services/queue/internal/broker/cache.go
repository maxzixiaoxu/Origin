package broker

import (
	"context"
	"sync"
	"time"

	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// queueCache keeps queue configuration in process memory for a short interval.
//
// Every Dequeue needs a queue's concurrency ceiling, rate limit, visibility
// timeout, and pause flag. Reading that row from Postgres on each call would
// put a database round trip in front of every batch of work -- at a few
// thousand dequeues per second, the config lookup would cost more than the
// dispatch it guards, and Postgres would be the throughput ceiling of a system
// whose hot path was carefully designed to live in Redis.
//
// The tradeoff is staleness: a change made in the admin UI takes up to one TTL
// to reach every replica. That is acceptable for the settings involved --
// nothing here is a safety property, and a concurrency limit applied 500ms late
// is indistinguishable from one applied on time. The single exception is
// pausing during an incident, where the operator wants it NOW, so writes
// through the admin API call InvalidateQueue and skip the wait entirely.
//
// Bounding staleness to a TTL is also what makes this safe across replicas: a
// stale entry heals on its own, so a dropped invalidation degrades to a short
// delay rather than a permanently wrong config.
type queueCache struct {
	store *store.Store
	ttl   time.Duration
	now   func() time.Time

	mu      sync.RWMutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	cfg       *store.QueueConfig
	expiresAt time.Time
}

func newQueueCache(s *store.Store, ttl time.Duration, now func() time.Time) *queueCache {
	return &queueCache{
		store:   s,
		ttl:     ttl,
		now:     now,
		entries: make(map[string]cacheEntry),
	}
}

// get returns a queue's configuration, creating the queue with defaults if it
// does not exist yet.
//
// Two callers missing the same key will both query Postgres. That is a
// deliberate non-optimisation: single-flight machinery would add locking to the
// hot path to save a duplicate read that happens at most once per TTL per
// queue. The duplicate is cheaper than the coordination.
func (c *queueCache) get(ctx context.Context, name string) (*store.QueueConfig, error) {
	now := c.now()

	c.mu.RLock()
	entry, ok := c.entries[name]
	c.mu.RUnlock()

	if ok && now.Before(entry.expiresAt) {
		return entry.cfg, nil
	}

	// EnsureQueue rather than GetQueue: submitting to a queue name that does
	// not exist yet should just work, rather than failing with a foreign-key
	// violation the caller cannot act on.
	cfg, err := c.store.EnsureQueue(ctx, name)
	if err != nil {
		// Serve a stale entry rather than failing the request. If Postgres is
		// briefly unavailable, continuing to dispatch with slightly old limits
		// is far better than halting a queue that is otherwise healthy --
		// Redis, which owns the actual hot path, is still fine.
		if ok {
			return entry.cfg, nil
		}
		return nil, err
	}

	c.mu.Lock()
	c.entries[name] = cacheEntry{cfg: cfg, expiresAt: now.Add(c.ttl)}
	c.mu.Unlock()

	return cfg, nil
}

// invalidate drops one queue's entry so the next read reloads it. Called after
// any admin write, which is what makes "pause this queue" take effect
// immediately rather than after the TTL.
func (c *queueCache) invalidate(name string) {
	c.mu.Lock()
	delete(c.entries, name)
	c.mu.Unlock()
}

// invalidateAll clears the cache.
func (c *queueCache) invalidateAll() {
	c.mu.Lock()
	c.entries = make(map[string]cacheEntry)
	c.mu.Unlock()
}
