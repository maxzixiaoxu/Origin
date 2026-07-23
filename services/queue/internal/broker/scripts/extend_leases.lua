-- extend_leases.lua -- renew every lease one worker holds on one queue.
--
-- Batched by design. A worker running 64 concurrent jobs must renew 64 leases
-- every heartbeat tick; doing that as 64 round trips would make RPC load scale
-- with in-flight jobs rather than with workers, and would put the heartbeat
-- itself on the critical path for lease expiry under load. One call per worker
-- per tick keeps renewal cost flat no matter how deep the concurrency goes.
--
-- The per-job answer matters as much as the renewal. A job whose lease was
-- reaped is already running somewhere else, and the worker still executing it
-- is doing duplicate work with a result that will be rejected at ack time. The
-- sooner it learns, the less wasted work -- so this returns a verdict for every
-- job rather than a single aggregate success.
--
-- KEYS[1] leases sorted set
-- KEYS[2] inflight hash
--
-- ARGV[1] worker id
-- ARGV[2] new expiry, unix millis
-- ARGV[3..] job ids
--
-- Returns a parallel array of 1 (extended) or 0 (lease lost).

local leases_key   = KEYS[1]
local inflight_key = KEYS[2]

local worker_id  = ARGV[1]
local expires_at = tonumber(ARGV[2])

local results = {}

for i = 3, #ARGV do
    local job_id = ARGV[i]
    local extended = 0

    -- Both conditions are required, and neither is redundant.
    --
    -- The ownership check catches the common case: the reaper reclaimed the job
    -- and another worker now owns it. The ZSCORE check catches the narrower
    -- one: the lease row is gone but the inflight entry lingers, which is the
    -- state left behind if a reap is interrupted between its two writes.
    -- Without ZSCORE, a bare ZADD would happily *recreate* a lease that the
    -- reaper had deliberately removed, resurrecting a job the system had
    -- already decided to reassign.
    if redis.call('HGET', inflight_key, job_id) == worker_id then
        if redis.call('ZSCORE', leases_key, job_id) then
            redis.call('ZADD', leases_key, expires_at, job_id)
            extended = 1
        end
    end

    results[#results + 1] = extended
end

return results
