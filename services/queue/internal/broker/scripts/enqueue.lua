-- enqueue.lua -- admit one job to the hot path.
--
-- Called AFTER the durable row exists in Postgres. That ordering is deliberate:
-- if this script fails, the job is already durable and the reconciler will
-- place it in Redis later, so the worst case is delay. The reverse ordering
-- would allow a job to be dispatchable with no durable record -- a worker would
-- lease something that does not exist, and no operator could ever explain where
-- it came from.
--
-- Paused queues still accept enqueues. Pausing stops dispatch, not submission;
-- an operator draining a misbehaving queue during an incident wants the backlog
-- to accumulate safely, not to start rejecting callers.
--
-- KEYS[1] ready sorted set
-- KEYS[2] scheduled sorted set
-- KEYS[3] envelope key for this job
-- KEYS[4] queue registry set
--
-- ARGV[1] job id
-- ARGV[2] now, unix millis
-- ARGV[3] run_at, unix millis
-- ARGV[4] ready score (priority*1e13 + enqueue millis), precomputed in Go
-- ARGV[5] envelope JSON
-- ARGV[6] envelope TTL in millis (0 disables)
-- ARGV[7] queue name, for the registry
--
-- Returns { state, depth } where state is 0 for scheduled and 1 for ready.

local ready_key     = KEYS[1]
local scheduled_key = KEYS[2]
local job_key       = KEYS[3]
local registry_key  = KEYS[4]

local job_id   = ARGV[1]
local now_ms   = tonumber(ARGV[2])
local run_at   = tonumber(ARGV[3])
local score    = tonumber(ARGV[4])
local envelope = ARGV[5]
local ttl_ms   = tonumber(ARGV[6])
local queue    = ARGV[7]

redis.call('SET', job_key, envelope)

-- A TTL on the envelope is a leak backstop, not a correctness mechanism. If it
-- ever does expire out from under a still-queued job, dequeue returns an empty
-- payload and Go rehydrates from Postgres, so the job still runs.
if ttl_ms > 0 then
    redis.call('PEXPIRE', job_key, ttl_ms)
end

-- Let background loops discover this queue without polling Postgres.
redis.call('SADD', registry_key, queue)

if run_at > now_ms then
    redis.call('ZADD', scheduled_key, run_at, job_id)
    return { 0, redis.call('ZCARD', scheduled_key) }
end

redis.call('ZADD', ready_key, score, job_id)
return { 1, redis.call('ZCARD', ready_key) }
