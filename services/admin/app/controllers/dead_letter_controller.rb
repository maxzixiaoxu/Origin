# The dead-letter queue: jobs that exhausted their retries or failed
# permanently, with one-click and bulk replay.
class DeadLetterController < ApplicationController
  PER_PAGE = 50
  BULK_LIMIT = 500

  def index
    scope = Job.dead.in_queue(params[:queue].presence).order(finished_at: :desc)

    @total = scope.count
    @page = [params[:page].to_i, 1].max
    @jobs = scope.limit(PER_PAGE).offset((@page - 1) * PER_PAGE).to_a
    @queues = JobQueue.alphabetical.pluck(:name)

    # Grouping by error makes a systemic failure obvious. Two hundred dead jobs
    # with one shared message is a single broken dependency; two hundred with
    # two hundred different messages is something else entirely, and the
    # response to each is different.
    @by_error = Job.dead
                   .group(:last_error)
                   .order(Arel.sql("count(*) DESC"))
                   .limit(5)
                   .count
  end

  # Replays every dead job in a queue, bounded per click.
  #
  # The bound is deliberate. Replaying fifty thousand jobs against a dependency
  # that is still broken would refill the dead-letter queue and hammer the thing
  # that was already failing. Capping it forces the operator to observe the
  # first batch before committing to the rest.
  def bulk_retry
    queue = params[:queue].presence

    ids = Job.dead.in_queue(queue).order(:finished_at).limit(BULK_LIMIT).pluck(:id)

    if ids.empty?
      return redirect_to dead_letter_index_path(queue:),
                         notice: "Nothing to replay."
    end

    succeeded, failed = replay(ids)

    message = "Replayed #{succeeded} job#{'s' unless succeeded == 1}."
    message += " #{failed} could not be replayed." if failed.positive?
    if ids.size == BULK_LIMIT
      message += " Capped at #{BULK_LIMIT} per click; run it again for more."
    end

    redirect_to dead_letter_index_path(queue:), notice: message
  end

  private

  def replay(ids)
    succeeded = 0
    failed = 0

    ids.each do |id|
      queue_client.retry_job(id)
      succeeded += 1
    rescue QueueClient::Error => e
      # One job failing to replay must not abandon the rest of the batch.
      failed += 1
      logger.warn("could not replay #{id}: #{e.message}")
    end

    [succeeded, failed]
  end
end
