-- requeue.lua -- place a job back into the hot path, idempotently.
--
-- Shared by every path that returns a job to circulation without holding a
-- lease on it: the reaper after reclaiming an expired lease, the reconciler
-- rebuilding Redis from Postgres, and dead-letter replay from the admin UI.
--
-- The ZADD NX is load-bearing, not defensive habit.
--
-- The reaper, promoter, and rollup loops run on a single replica chosen by a
-- Redis lock, and a Redis lock is NOT a safe mutual-exclusion primitive: a
-- leader can be paused by GC past its lease, lose the lock, and resume acting
-- while a new leader is already running (docs/design-decisions.md covers this
-- at length). The system tolerates that only because every operation those
-- loops perform is idempotent, and this script is where most of that
-- idempotence actually lives.
--
-- NX makes a second requeue a no-op instead of a score reset. Without it, two
-- concurrent leaders would each rewrite the score of a job already sitting in
-- the ready set -- repeatedly shoving it backward or forward in its priority
-- band, and in the worst case starving it while the two loops fight. With NX,
-- the loser's write simply does nothing and the return value says so.
--
-- KEYS[1] ready sorted set
-- KEYS[2] scheduled sorted set
-- KEYS[3] envelope key for this job
-- KEYS[4] queue registry set
--
-- ARGV[1] job id
-- ARGV[2] target: 0 ready, 1 scheduled
-- ARGV[3] score (ready score, or run_at millis for scheduled)
-- ARGV[4] envelope JSON; empty leaves any existing envelope untouched
-- ARGV[5] envelope TTL in millis (0 disables)
-- ARGV[6] queue name, for the registry
--
-- Returns 1 if this call placed the job, 0 if it was already present.

local ready_key     = KEYS[1]
local scheduled_key = KEYS[2]
local job_key       = KEYS[3]
local registry_key  = KEYS[4]

local job_id   = ARGV[1]
local target   = tonumber(ARGV[2])
local score    = tonumber(ARGV[3])
local envelope = ARGV[4]
local ttl_ms   = tonumber(ARGV[5])
local queue    = ARGV[6]

local TARGET_READY     = 0
local TARGET_SCHEDULED = 1

redis.call('SADD', registry_key, queue)

if envelope ~= '' then
    redis.call('SET', job_key, envelope)
    if ttl_ms > 0 then
        redis.call('PEXPIRE', job_key, ttl_ms)
    end
end

-- A job must exist in exactly one of the two sets. Clearing the opposite set
-- keeps that invariant true even if a crash left a stale entry behind -- for
-- instance a promotion interrupted between its ZADD and its ZREM.
local added
if target == TARGET_READY then
    redis.call('ZREM', scheduled_key, job_id)
    added = redis.call('ZADD', ready_key, 'NX', score, job_id)
elseif target == TARGET_SCHEDULED then
    redis.call('ZREM', ready_key, job_id)
    added = redis.call('ZADD', scheduled_key, 'NX', score, job_id)
else
    return redis.error_reply('requeue.lua: unknown target ' .. tostring(target))
end

return added
