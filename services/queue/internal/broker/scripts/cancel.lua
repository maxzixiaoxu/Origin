-- cancel.lua -- remove a job from the hot path entirely.
--
-- Cancelling a job that is merely queued is trivial: drop it from whichever set
-- holds it. Cancelling one that is already *running* is the interesting case,
-- because there is no way to reach into another process and stop a goroutine.
--
-- This script does not try. It deletes the lease, and that is enough -- the
-- worker's next heartbeat asks to renew a lease that no longer exists,
-- extend_leases.lua answers "lost", and the worker cancels that job's
-- context.Context. The handler unwinds through ordinary Go cancellation within
-- one heartbeat interval.
--
-- The nice property of routing cancellation through the lease is that it reuses
-- a path already exercised constantly by reaping, rather than adding a separate
-- kill channel that would only run on the rare cancel and therefore be the
-- least-tested code in the system.
--
-- KEYS[1] ready sorted set
-- KEYS[2] scheduled sorted set
-- KEYS[3] leases sorted set
-- KEYS[4] inflight hash
-- KEYS[5] envelope key for this job
--
-- ARGV[1] job id
--
-- Returns { removed_from_ready, removed_from_scheduled, was_running, worker_id }

local ready_key     = KEYS[1]
local scheduled_key = KEYS[2]
local leases_key    = KEYS[3]
local inflight_key  = KEYS[4]
local job_key       = KEYS[5]

local job_id = ARGV[1]

local from_ready     = redis.call('ZREM', ready_key, job_id)
local from_scheduled = redis.call('ZREM', scheduled_key, job_id)

-- Capture the holder before deleting, so the caller can record who was
-- interrupted in the job's attempt history.
local worker = redis.call('HGET', inflight_key, job_id)
local was_running = 0

if worker then
    was_running = 1
    redis.call('HDEL', inflight_key, job_id)
end

-- Remove the lease regardless of whether an inflight entry existed. If a
-- previous reap was interrupted between its two writes, the lease can outlive
-- the ownership record, and leaving it behind would let the reaper resurrect a
-- job the operator explicitly cancelled.
redis.call('ZREM', leases_key, job_id)

redis.call('DEL', job_key)

return { from_ready, from_scheduled, was_running, worker or '' }
