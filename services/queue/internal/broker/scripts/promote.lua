-- promote.lua -- move scheduled jobs whose time has come into the ready set.
--
-- Runs on one elected replica every PROMOTE_INTERVAL. Because the whole batch
-- moves atomically, a job is never simultaneously absent from both sets, so a
-- concurrent dequeue can miss a job by at most one tick -- it can never lose it.
--
-- The ready score is rebuilt from the job's own run_at, NOT from the promotion
-- timestamp. That distinction is the difference between a fair queue and one
-- that quietly punishes its own operators: if the promoter falls behind -- a
-- leader election, a slow tick, a restart -- scoring by promotion time would
-- stamp every delayed job as freshly enqueued and send it to the back of its
-- priority band, behind jobs submitted while it was waiting. Scoring by run_at
-- means lateness in the promoter costs latency, never ordering.
--
-- KEYS[1] scheduled sorted set
-- KEYS[2] ready sorted set
--
-- ARGV[1] now, unix millis
-- ARGV[2] max jobs to promote this tick
-- ARGV[3] envelope key prefix
-- ARGV[4] default priority when an envelope is missing or unreadable
--
-- Returns { promoted_count, remaining_due_count }

local scheduled_key = KEYS[1]
local ready_key     = KEYS[2]

local now_ms          = tonumber(ARGV[1])
local limit           = tonumber(ARGV[2])
local job_prefix      = ARGV[3]
local default_priority = tonumber(ARGV[4])

-- Bounded per tick so one enormous backlog cannot block Redis inside a single
-- script call. Redis is single-threaded: a script that walks a million-member
-- sorted set stalls every other client for its whole duration, which would turn
-- a large scheduled backlog into a full outage. The leftover count is returned
-- so the caller can tick again immediately instead of waiting out the interval.
local due = redis.call('ZRANGEBYSCORE', scheduled_key,
    '-inf', now_ms, 'WITHSCORES', 'LIMIT', 0, limit)

if #due == 0 then
    return { 0, 0 }
end

local promoted = 0

for i = 1, #due, 2 do
    local job_id = due[i]
    local run_at = tonumber(due[i + 1])

    -- Priority lives in the envelope, so it survives a retry without needing a
    -- parallel priority index to keep in sync.
    local priority = default_priority
    local raw = redis.call('GET', job_prefix .. job_id)
    if raw then
        -- pcall because a truncated or hand-edited envelope must degrade to the
        -- default priority, not abort the batch and wedge the promoter.
        local ok, envelope = pcall(cjson.decode, raw)
        if ok and type(envelope) == 'table' and type(envelope.priority) == 'number' then
            priority = envelope.priority
        end
    end

    -- Must match jobtypes.PriorityScore in Go. Lua numbers are float64, the
    -- same type as Redis sorted-set scores, so this arithmetic is exact at the
    -- magnitudes involved.
    local score = priority * 1e13 + run_at

    redis.call('ZADD', ready_key, score, job_id)
    redis.call('ZREM', scheduled_key, job_id)
    promoted = promoted + 1
end

-- Report whether more work is already due, so a backlog drains at the caller's
-- pace rather than one batch per interval.
local remaining = redis.call('ZCOUNT', scheduled_key, '-inf', now_ms)

return { promoted, remaining }
