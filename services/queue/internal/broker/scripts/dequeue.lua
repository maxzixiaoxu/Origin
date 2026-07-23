-- dequeue.lua -- lease a batch of jobs to one worker.
--
-- This is the hot path and the correctness core of the whole system. Every
-- gate below (pause, concurrency, rate limit) and the pop-and-lease itself run
-- inside a single script, which Redis executes atomically against all other
-- clients. That atomicity is the entire reason two workers can never receive
-- the same job: there is no window between "this job was in the ready set" and
-- "this job is leased to worker X" for a second worker to observe.
--
-- Doing this with separate ZPOPMIN and ZADD round trips would be a textbook
-- race. Doing it with a distributed lock would serialise every dequeue in the
-- cluster and cap throughput at one job at a time. A Lua script is what buys
-- both correctness and parallelism.
--
-- The gates are ordered cheapest-first and most-decisive-first, so a paused or
-- saturated queue costs one or two Redis operations rather than a full pop.
--
-- KEYS[1] ready sorted set
-- KEYS[2] leases sorted set
-- KEYS[3] inflight hash (job id -> worker id)
-- KEYS[4] rate-limit token bucket hash
--
-- ARGV[1] now, unix millis
-- ARGV[2] max jobs requested
-- ARGV[3] worker id
-- ARGV[4] visibility timeout in millis
-- ARGV[5] max concurrency (0 = unlimited)
-- ARGV[6] rate limit per second (0 = unlimited)
-- ARGV[7] rate limit burst
-- ARGV[8] paused flag (0 or 1)
-- ARGV[9] envelope key prefix
--
-- Returns { reason, retry_after_ms, lease_expires_at_ms, ids, envelopes }
--   reason 0 ok, 1 empty, 2 rate limited, 3 at concurrency ceiling, 4 paused

local ready_key    = KEYS[1]
local leases_key   = KEYS[2]
local inflight_key = KEYS[3]
local rate_key     = KEYS[4]

local now_ms     = tonumber(ARGV[1])
local max_jobs   = tonumber(ARGV[2])
local worker_id  = ARGV[3]
local vis_ms     = tonumber(ARGV[4])
local max_conc   = tonumber(ARGV[5])
local rate       = tonumber(ARGV[6])
local burst      = tonumber(ARGV[7])
local paused     = tonumber(ARGV[8])
local job_prefix = ARGV[9]

local REASON_OK          = 0
local REASON_EMPTY       = 1
local REASON_RATE_LIMIT  = 2
local REASON_CONCURRENCY = 3
local REASON_PAUSED      = 4

local function nothing(reason, retry_after)
    return { reason, retry_after, 0, {}, {} }
end

if paused == 1 then
    return nothing(REASON_PAUSED, 0)
end

-- Concurrency ceiling. ZCARD of the lease set IS the live in-flight count, so
-- there is no separate counter to leak or drift when a worker dies. A counter
-- incremented on dequeue and decremented on ack would slowly overcount every
-- time a worker crashed between the two, and would eventually wedge the queue
-- shut with no way to tell why.
if max_conc > 0 then
    local room = max_conc - redis.call('ZCARD', leases_key)
    if room <= 0 then
        return nothing(REASON_CONCURRENCY, 0)
    end
    if room < max_jobs then
        max_jobs = room
    end
end

local depth = redis.call('ZCARD', ready_key)
if depth == 0 then
    return nothing(REASON_EMPTY, 0)
end
if depth < max_jobs then
    max_jobs = depth
end

-- Token bucket, refilled lazily.
--
-- Lazy refill means no background timer and no per-queue ticker: the bucket is
-- brought up to date only when someone asks for a token, using elapsed time
-- since the last request. `now` arrives from Go rather than redis.call('TIME')
-- so the script stays a pure function of its inputs -- deterministic under
-- replication and testable against a fake clock.
if rate > 0 then
    local state  = redis.call('HMGET', rate_key, 'tokens', 'ts')
    local tokens = tonumber(state[1])
    local ts     = tonumber(state[2])

    if tokens == nil or ts == nil then
        -- First use: start full, so a burst limit is available immediately
        -- rather than after a warm-up the operator did not ask for.
        tokens = burst
        ts = now_ms
    end

    local elapsed_ms = now_ms - ts
    if elapsed_ms > 0 then
        tokens = math.min(burst, tokens + (elapsed_ms / 1000.0) * rate)
    end

    if tokens < 1 then
        redis.call('HSET', rate_key, 'tokens', tokens, 'ts', now_ms)
        redis.call('PEXPIRE', rate_key, 60000)

        -- Tell the caller exactly how long until one token exists. A worker
        -- that sleeps precisely this long neither spins against Redis nor
        -- waits longer than necessary; without it the client can only guess.
        local wait_ms = math.ceil(((1 - tokens) / rate) * 1000)
        if wait_ms < 1 then
            wait_ms = 1
        end
        return nothing(REASON_RATE_LIMIT, wait_ms)
    end

    -- Spend whole tokens only. Handing out a fractional job is meaningless, and
    -- flooring here is what keeps the long-run dispatch rate at or below the
    -- configured limit rather than drifting above it.
    local grant = math.floor(tokens)
    if grant < max_jobs then
        max_jobs = grant
    end

    redis.call('HSET', rate_key, 'tokens', tokens - max_jobs, 'ts', now_ms)
    redis.call('PEXPIRE', rate_key, 60000)
end

-- Pop and lease. ZPOPMIN returns a flat { member, score, member, score, ... }.
local popped = redis.call('ZPOPMIN', ready_key, max_jobs)
if #popped == 0 then
    return nothing(REASON_EMPTY, 0)
end

local expires_at = now_ms + vis_ms
local ids       = {}
local envelopes = {}
local n = 0

for i = 1, #popped, 2 do
    local job_id = popped[i]
    n = n + 1
    ids[n] = job_id

    redis.call('ZADD', leases_key, expires_at, job_id)
    redis.call('HSET', inflight_key, job_id, worker_id)

    -- GET returns false, not nil, when the key is missing. An absent envelope
    -- is recoverable rather than fatal: Go detects the empty string and
    -- rehydrates that job from Postgres. This is what makes a Redis flush a
    -- latency event instead of a data-loss event.
    local raw = redis.call('GET', job_prefix .. job_id)
    envelopes[n] = raw or ''
end

return { REASON_OK, 0, expires_at, ids, envelopes }
