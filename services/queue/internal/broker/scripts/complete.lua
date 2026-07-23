-- complete.lua -- release a lease and route the job to its next state.
--
-- Ack, retry, and dead-letter are one script rather than three because they
-- share the part that must not be got wrong: verifying the caller still owns
-- the lease before mutating anything.
--
-- That check is what makes at-least-once delivery safe. A worker can be paused
-- by a long GC or partitioned from Redis for longer than the visibility
-- timeout; the reaper then reclaims its job and another worker starts running
-- it. When the first worker comes back and reports success, it must be told no.
-- Without the ownership check it would clear a lease it no longer holds --
-- deleting the *second* worker's lease, letting the reaper hand the job to a
-- third worker, and turning one slow worker into an amplifying loop.
--
-- KEYS[1] leases sorted set
-- KEYS[2] inflight hash
-- KEYS[3] ready sorted set
-- KEYS[4] scheduled sorted set
-- KEYS[5] envelope key for this job
--
-- ARGV[1] job id
-- ARGV[2] worker id claiming the lease
-- ARGV[3] action: 0 ack, 1 retry with delay, 2 dead-letter, 3 requeue now
-- ARGV[4] score for the target set (run_at millis for retry, ready score for
--         immediate requeue); ignored for ack and dead-letter
-- ARGV[5] updated envelope JSON, written on requeue so the retry carries its
--         incremented attempt count; empty to leave the envelope untouched
-- ARGV[6] envelope TTL in millis (0 disables)
--
-- Returns 1 on success, 0 if the lease was already gone, -1 if another worker
-- holds it.

local leases_key    = KEYS[1]
local inflight_key  = KEYS[2]
local ready_key     = KEYS[3]
local scheduled_key = KEYS[4]
local job_key       = KEYS[5]

local job_id    = ARGV[1]
local worker_id = ARGV[2]
local action    = tonumber(ARGV[3])
local score     = tonumber(ARGV[4])
local envelope  = ARGV[5]
local ttl_ms    = tonumber(ARGV[6])

local ACTION_ACK         = 0
local ACTION_RETRY       = 1
local ACTION_DEAD        = 2
local ACTION_REQUEUE_NOW = 3

local RESULT_OK        = 1
local RESULT_NO_LEASE  = 0
local RESULT_NOT_OWNER = -1

local owner = redis.call('HGET', inflight_key, job_id)

-- HGET yields false when the field is absent. No lease means the reaper already
-- took this job, or it was cancelled, or this is a duplicate report.
if not owner then
    return RESULT_NO_LEASE
end

if owner ~= worker_id then
    return RESULT_NOT_OWNER
end

redis.call('ZREM', leases_key, job_id)
redis.call('HDEL', inflight_key, job_id)

if action == ACTION_ACK or action == ACTION_DEAD then
    -- Terminal. Drop the cached envelope; Postgres keeps the durable record and
    -- the dead-letter UI reads from there, so nothing is lost by evicting it.
    redis.call('DEL', job_key)
    return RESULT_OK
end

-- Requeueing. Refresh the envelope first so the retry carries its incremented
-- attempt count, then move the job. Both happen inside this script, so a worker
-- can never observe a job that is back in the ready set but still advertising
-- its previous attempt number.
if envelope ~= '' then
    redis.call('SET', job_key, envelope)
    if ttl_ms > 0 then
        redis.call('PEXPIRE', job_key, ttl_ms)
    end
end

if action == ACTION_RETRY then
    redis.call('ZADD', scheduled_key, score, job_id)
elseif action == ACTION_REQUEUE_NOW then
    -- Graceful drain: the job did not fail, the worker is shutting down. Skip
    -- backoff entirely and make it immediately dispatchable, so a rolling
    -- restart costs milliseconds instead of a full visibility timeout.
    redis.call('ZADD', ready_key, score, job_id)
else
    return redis.error_reply('complete.lua: unknown action ' .. tostring(action))
end

return RESULT_OK
