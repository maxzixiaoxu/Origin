# Browsing and inspecting jobs.
#
# Reads come straight from Postgres through read-only models; writes (cancel,
# retry) go through the Go service. That split is the whole architecture in one
# controller.
class JobsController < ApplicationController
  PER_PAGE = 50

  def index
    @filters = filter_params

    scope = Job
              .in_queue(@filters[:queue])
              .with_status(@filters[:status])
              .of_type(@filters[:type])
              .search(@filters[:q])
              .newest_first

    @total = scope.count
    @page = [@filters[:page].to_i, 1].max
    @jobs = scope.limit(PER_PAGE).offset((@page - 1) * PER_PAGE).to_a

    @queues = JobQueue.alphabetical.pluck(:name)
    @statuses = Job::STATUSES
  end

  def show
    @job = Job.find(params[:id])
    @attempts = @job.job_attempts.chronological.to_a

    # For an image job, presign the original and whatever derivative it
    # produced, so the page can show the actual before and after.
    return unless @job.image_derivative?

    payload = @job.payload_data
    @source_url = object_store.presigned_url(payload["source_key"])
    @derivative_url = object_store.presigned_url(payload["target_key"]) if @job.status == "succeeded"

    # Sibling jobs from the same upload: the other two sizes. Found by shared
    # source_key, which is what makes the detail page show the whole fan-out
    # rather than one third of it.
    @siblings = sibling_jobs(payload["source_key"])
  end

  def cancel
    queue_client.cancel(params[:id])
    redirect_to job_path(params[:id]), notice: "Cancellation requested."
  rescue QueueClient::NotFoundError
    redirect_to jobs_path, alert: "That job no longer exists."
  end

  def retry
    queue_client.retry_job(params[:id])
    redirect_back fallback_location: job_path(params[:id]),
                  notice: "Job requeued."
  rescue QueueClient::ConflictError => e
    redirect_back fallback_location: job_path(params[:id]), alert: e.message
  end

  private

  def filter_params
    {
      queue: params[:queue].presence,
      status: params[:status].presence,
      type: params[:type].presence,
      q: params[:q].presence,
      page: params[:page].presence || 1
    }
  end

  # Jobs sharing a source image.
  #
  # Uses a JSONB containment query, which Postgres can answer without a GIN
  # index at this scale because it is scoped to a single queue and a recent
  # window. If the jobs table grows past a few million rows this needs an index
  # on (payload->>'source_key') -- noted here rather than pre-optimised.
  def sibling_jobs(source_key)
    return [] if source_key.blank?

    Job.where(job_type: "image.derive")
       .where("payload->>'source_key' = ?", source_key)
       .order(:priority)
       .limit(6)
       .to_a
  end
end
