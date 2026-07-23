-- reap.lua -- reclaim leases whose holders stopped reporting.
--
-- This is the mechanism behind the system's central claim: kill a worker
-- mid-job and nothing is lost. A leased job whose worker dies stops
-- heartbeating; its lease expiry passes; this script takes it back.
--
-- Deliberately narrow in scope. It clears the lease and returns the ids, and
-- does NOT decide what happens next. Whether an expired job is retried or
-- dead-lettered depends on its attempt count against max_attempts, and that
-- policy lives in Go where it is unit-testable and where the same decision code
-- is shared with the nack path. A Lua script that also made policy decisions
-- would be a second, silently-diverging copy of the retry rules.
--
-- The gap this opens is real and worth naming: between this script clearing a
-- lease and Go requeueing the job, the job is in neither the lease set nor the
-- ready set. If the queue service dies in that window, the job sits in Postgres
-- as 'running' with an expired lease and no Redis presence. The reconciler
-- finds exactly that shape on boot and restores it, which is why the
-- reconciler exists and why it scans on lease expiry rather than only on
-- pending status.
--
-- KEYS[1] leases sorted set
-- KEYS[2] inflight hash
--
-- ARGV[1] now, unix millis
-- ARGV[2] max leases to reclaim this tick
--
-- Returns { ids, worker_ids, remaining_expired_count }
-- worker_ids is parallel to ids so the caller can attribute each expiry to the
-- worker that dropped it -- which is what makes "which worker is dying?" an
-- answerable question rather than an aggregate mystery.

local leases_key   = KEYS[1]
local inflight_key = KEYS[2]

local now_ms = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])

local expired = redis.call('ZRANGEBYSCORE', leases_key,
    '-inf', now_ms, 'LIMIT', 0, limit)

if #expired == 0 then
    return { {}, {}, 0 }
end

local ids     = {}
local workers = {}

for i = 1, #expired do
    local job_id = expired[i]

    -- Capture the previous holder before deleting it. Lost here, it is lost for
    -- good: nothing else records which worker was holding a job when its lease
    -- died.
    local worker = redis.call('HGET', inflight_key, job_id)

    ids[i]     = job_id
    workers[i] = worker or ''

    redis.call('ZREM', leases_key, job_id)
    redis.call('HDEL', inflight_key, job_id)
end

local remaining = redis.call('ZCOUNT', leases_key, '-inf', now_ms)

return { ids, workers, remaining }
