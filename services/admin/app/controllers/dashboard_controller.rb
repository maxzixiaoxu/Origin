# The overview page: live depth, recent throughput, and worker activity.
class DashboardController < ApplicationController
  def show
    @queues = JobQueue.alphabetical.to_a
    @depths = fetch_depths

    # Charts read the pre-aggregated rollups, never the jobs table. Plotting an
    # hour is a few dozen indexed rows instead of a scan over job history, which
    # is what keeps this page fast once the table is large.
    @stats = QueueStatMinute.since(1.hour.ago).chronological.to_a

    @recent = Job.newest_first.limit(15).to_a
    @dead_count = Job.dead.count

    # Distinct workers seen recently. Approximate by design: it answers "is
    # anything processing?" without a heartbeat table to maintain.
    @active_workers = JobAttempt
                        .where(started_at: 5.minutes.ago..)
                        .distinct
                        .count(:worker_id)
  end

  # Turbo Frame target for the live counters. Polled rather than pushed: the
  # data is a handful of integers on a two-second cadence, and a WebSocket layer
  # would add a persistent connection per viewer plus a whole subsystem to
  # operate, for a dashboard that at most a few people have open.
  def stats
    @queues = JobQueue.alphabetical.to_a
    @depths = fetch_depths
    @dead_count = Job.dead.count

    render partial: "dashboard/live_stats",
           locals: { queues: @queues, depths: @depths, dead_count: @dead_count }
  end

  private

  # Depth comes from Redis via the queue service, not from Postgres.
  #
  # Postgres knows what jobs exist; only Redis knows what is dispatchable right
  # now. Counting rows with status='pending' would also include jobs the
  # reconciler has not yet restored, and would miss the distinction between
  # ready and scheduled entirely.
  def fetch_depths
    queue_client.depth.index_by { |d| d["queue"] }
  rescue QueueClient::Error => e
    # A dashboard that renders nothing because one panel failed is worse than
    # one that renders most of itself. The view falls back to blanks.
    logger.warn("could not read live depth: #{e.message}")
    {}
  end
end
