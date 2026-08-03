#!/usr/bin/env bash
#
# Kill a worker while it is holding leases, and prove no job is lost.
#
# This is the demonstration the whole lease design exists for. The sequence is:
#
#   1. Submit enough slow work that every worker is holding leases.
#   2. SIGKILL one worker container -- not SIGTERM. A graceful stop would let
#      the worker hand its jobs back, which proves nothing; the interesting
#      case is a worker that dies with no chance to clean up.
#   3. Wait for the visibility timeout to pass and the reaper to run.
#   4. Assert every submitted job still completed.
#
# Success is boring on purpose: the same number of jobs finish, just a few
# seconds later, on the surviving workers.

set -euo pipefail

QUEUE="${QUEUE:-images}"
JOBS="${JOBS:-40}"
SLEEP_MS="${SLEEP_MS:-3000}"
ADDR="${QUEUE_ADDR:-localhost:59090}"

bold() { printf '\033[1m%s\033[0m\n' "$1"; }
dim()  { printf '\033[2m%s\033[0m\n' "$1"; }

command -v docker >/dev/null || { echo "docker is required"; exit 1; }
[[ -x ./bin/jobctl ]] || { echo "run: go build -o bin/jobctl ./tools/jobctl"; exit 1; }

bold "1. Baseline"
./bin/jobctl queues -addr "$ADDR"
echo

workers=$(docker compose ps --format '{{.Name}}' | grep -- '-workerd-' || true)
[[ -n "$workers" ]] || { echo "no worker containers are running"; exit 1; }
worker_count=$(wc -l <<< "$workers")
dim "$worker_count workers running"
echo

bold "2. Submitting $JOBS slow jobs (${SLEEP_MS}ms each)"
# bench.sleep rather than image.derive: the point is to hold leases open
# predictably, and a fixed duration makes the timing of the kill reproducible.
./bin/jobctl bench -addr "$ADDR" -queue "$QUEUE" -n "$JOBS" -sleep-ms "$SLEEP_MS" -async
echo

sleep 2

bold "3. Killing a worker mid-flight (SIGKILL, no cleanup)"
victim=$(head -1 <<< "$workers")
dim "victim: $victim"
docker kill "$victim" >/dev/null
echo

bold "4. Waiting for the lease to expire and the reaper to reclaim"
./bin/jobctl queues -addr "$ADDR"
echo
dim "watching until the queue drains..."
./bin/jobctl drain -addr "$ADDR" -queue "$QUEUE" -timeout 180s
echo

bold "5. Result"
./bin/jobctl verify -addr "$ADDR" -queue "$QUEUE" -expect "$JOBS"

echo
dim "restoring the killed worker"
docker compose up -d --no-recreate workerd >/dev/null 2>&1 || true
