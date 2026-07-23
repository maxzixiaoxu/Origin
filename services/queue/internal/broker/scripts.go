package broker

import (
	"context"
	"embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Lua sources are embedded rather than read from disk so a binary is
// self-contained: there is no deployment in which the service starts but its
// scripts are missing or, worse, are a different version than the code that
// interprets their return values.
//
//go:embed scripts/*.lua
var scriptFS embed.FS

// Script return codes shared with the Lua sources. These constants and the
// literals in the .lua files must agree; the mapping is asserted in
// scripts_test.go by parsing the Lua sources, so a change to one side that is
// not mirrored on the other fails the build rather than misrouting jobs at
// runtime.
const (
	// dequeue.lua reason codes.
	reasonOK          int64 = 0
	reasonEmpty       int64 = 1
	reasonRateLimit   int64 = 2
	reasonConcurrency int64 = 3
	reasonPaused      int64 = 4

	// complete.lua actions.
	actionAck        int64 = 0
	actionRetry      int64 = 1
	actionDead       int64 = 2
	actionRequeueNow int64 = 3

	// complete.lua results.
	resultOK       int64 = 1
	resultNoLease  int64 = 0
	resultNotOwner int64 = -1

	// requeue.lua targets.
	targetReady     int64 = 0
	targetScheduled int64 = 1
)

// scriptNames lists every Lua file, so loading is exhaustive and a newly added
// script cannot be silently forgotten.
var scriptNames = []string{
	"cancel",
	"complete",
	"dequeue",
	"enqueue",
	"extend_leases",
	"promote",
	"reap",
	"requeue",
}

// scriptSet holds the loaded scripts.
//
// go-redis's redis.Script sends EVALSHA first and transparently falls back to
// EVAL on NOSCRIPT, which matters more than it sounds: a Redis restart or a
// SCRIPT FLUSH empties the script cache, and without that fallback every
// dequeue in the fleet would start failing at once with an error most people
// have never seen. The fallback turns that into a single slow call per script.
type scriptSet struct {
	cancel       *redis.Script
	complete     *redis.Script
	dequeue      *redis.Script
	enqueue      *redis.Script
	extendLeases *redis.Script
	promote      *redis.Script
	reap         *redis.Script
	requeue      *redis.Script
}

// loadScripts reads and parses every embedded script.
func loadScripts() (*scriptSet, error) {
	sources := make(map[string]*redis.Script, len(scriptNames))

	for _, name := range scriptNames {
		src, err := scriptFS.ReadFile("scripts/" + name + ".lua")
		if err != nil {
			return nil, fmt.Errorf("read embedded script %s.lua: %w", name, err)
		}
		if len(src) == 0 {
			return nil, fmt.Errorf("embedded script %s.lua is empty", name)
		}
		sources[name] = redis.NewScript(string(src))
	}

	return &scriptSet{
		cancel:       sources["cancel"],
		complete:     sources["complete"],
		dequeue:      sources["dequeue"],
		enqueue:      sources["enqueue"],
		extendLeases: sources["extend_leases"],
		promote:      sources["promote"],
		reap:         sources["reap"],
		requeue:      sources["requeue"],
	}, nil
}

// preload pushes every script into Redis's cache at startup.
//
// Strictly optional -- EVALSHA falls back to EVAL on its own. It is done anyway
// so that a syntax error in a Lua file is discovered when the service boots,
// in one obvious log line, instead of on the first dequeue of a queue that
// happens to hit that script hours later.
func (s *scriptSet) preload(ctx context.Context, rdb redis.Scripter) error {
	all := map[string]*redis.Script{
		"cancel":        s.cancel,
		"complete":      s.complete,
		"dequeue":       s.dequeue,
		"enqueue":       s.enqueue,
		"extend_leases": s.extendLeases,
		"promote":       s.promote,
		"reap":          s.reap,
		"requeue":       s.requeue,
	}

	for name, script := range all {
		if err := script.Load(ctx, rdb).Err(); err != nil {
			return fmt.Errorf("load script %s.lua into redis: %w", name, err)
		}
	}
	return nil
}
