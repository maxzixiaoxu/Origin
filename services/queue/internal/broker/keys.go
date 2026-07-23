package broker

import (
	"fmt"
	"regexp"
	"strings"
)

// Redis keyspace.
//
// Every per-queue key is spelled q:{name}:... and the braces are not
// decoration -- they are Redis Cluster hash-tag syntax. Cluster hashes only the
// substring inside the first {...} when choosing a slot, so q:{orders}:ready,
// q:{orders}:leases, and q:{orders}:job:abc all land on the same node.
//
// This matters because a Lua script is executed on one node and Redis rejects
// any script whose keys span slots with CROSSSLOT. Naming the job envelope
// job:<id> instead of q:{queue}:job:<id> would work perfectly on a single
// Redis and then fail on the first day of cluster operation, with the failure
// surfacing inside dequeue -- the hottest path in the system. Paying attention
// to it now costs one extra path segment and makes every single-queue script
// cluster-safe by construction.
//
// Cross-queue work is deliberately done in Go, one script invocation per queue,
// rather than in a single script over many queues.
const (
	keyPrefix = "q"

	// Global keys. These are never touched by the same script as a queue key,
	// so they need no hash tag.
	registryKey  = "jobq:queues"
	lockPrefix   = "jobq:lock:"
	fenceCounter = "jobq:fence"
)

// queueNamePattern mirrors the queues_name_format CHECK constraint in
// migrations/000001_init.up.sql. Queue names are interpolated into Redis keys,
// so validating here as well means a name can never reach the keyspace even if
// something writes to Postgres out of band.
var queueNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

// ValidateQueueName rejects names that would produce a malformed or ambiguous
// Redis key.
func ValidateQueueName(name string) error {
	if name == "" {
		return fmt.Errorf("queue name is empty")
	}
	if !queueNamePattern.MatchString(name) {
		return fmt.Errorf("invalid queue name %q: must match %s",
			name, queueNamePattern.String())
	}
	return nil
}

// Keys builds the Redis key names for one queue. It is a value type; construct
// it with KeysFor and pass it around freely.
type Keys struct {
	// Queue is the validated queue name.
	Queue string
	// tag is "q:{queue}" -- the shared hash-tag prefix.
	tag string
}

// KeysFor returns the key set for a queue. The name is assumed validated;
// callers reaching Redis go through Broker methods, which validate at the
// boundary.
func KeysFor(queue string) Keys {
	return Keys{
		Queue: queue,
		tag:   keyPrefix + ":{" + queue + "}",
	}
}

// Ready is the sorted set of dispatchable jobs, scored by
// priority*1e13 + enqueueMillis so that priority dominates and age breaks ties.
func (k Keys) Ready() string { return k.tag + ":ready" }

// Scheduled is the sorted set of jobs waiting for their run_at, scored by that
// timestamp. Both user-requested delays and retry backoff live here.
func (k Keys) Scheduled() string { return k.tag + ":scheduled" }

// Leases is the sorted set of in-flight jobs, scored by lease expiry. Its
// cardinality is the queue's live concurrency, which is why the concurrency
// ceiling costs a single ZCARD rather than a separate counter that could drift
// out of sync with reality.
func (k Keys) Leases() string { return k.tag + ":leases" }

// Inflight maps job id to the worker holding its lease. Ownership is checked on
// ack, nack, and lease extension so a worker whose lease was reaped cannot
// report on a job another worker now owns.
func (k Keys) Inflight() string { return k.tag + ":inflight" }

// Rate holds the token-bucket state: {tokens, ts}.
func (k Keys) Rate() string { return k.tag + ":rate" }

// JobPrefix is the prefix for envelope keys. Scripts append the job id to it
// rather than receiving each envelope key in KEYS, because the set of ids is
// only known after ZPOPMIN runs inside the script.
//
// This is the one place the code knowingly steps outside the "declare all keys
// in KEYS" rule that Redis Cluster expects. It is safe precisely because of the
// hash tag: every key the script derives shares the queue's slot, so it can
// never route off-node.
func (k Keys) JobPrefix() string { return k.tag + ":job:" }

// Job is the envelope key for a single job.
func (k Keys) Job(id string) string { return k.JobPrefix() + id }

// Registry is the global set of known queue names, used by background loops to
// discover which queues to sweep without querying Postgres each tick.
func Registry() string { return registryKey }

// Lock names a leader-election lock.
func Lock(name string) string { return lockPrefix + name }

// FenceCounter names the monotonic counter issuing fencing tokens.
func FenceCounter() string { return fenceCounter }

// QueueFromKey extracts the queue name from any per-queue key. Used when
// reporting errors about a key returned by Redis rather than one we built.
func QueueFromKey(key string) string {
	open := strings.Index(key, "{")
	if open < 0 {
		return ""
	}
	close := strings.Index(key[open:], "}")
	if close < 0 {
		return ""
	}
	return key[open+1 : open+close]
}
