# Image upload: the demo's entry point.
#
# One upload fans out into three derivative jobs at different priorities, and
# the resulting page shows them completing in priority order.
class UploadsController < ApplicationController
  def new
    @recent = recent_uploads
  end

  def create
    result = ImageSubmission.new(store: object_store, client: queue_client)
                            .call(params[:image])

    if result.errors.any?
      redirect_to new_upload_path,
                  alert: "Some sizes could not be queued: #{result.errors.join('; ')}"
      return
    end

    # Keyed by the thumbnail job's id rather than the S3 key.
    #
    # The key contains slashes, which a path segment cannot hold without
    # escaping -- and escaping it produced URLs like /uploads/originals%252F...
    # because Rails escapes the already-escaped value again. A UUID needs no
    # encoding, and the source key is one payload lookup away.
    redirect_to upload_path(result.jobs.first[:id]),
                notice: "Queued #{result.jobs.size} derivatives."
  rescue ArgumentError => e
    redirect_to new_upload_path, alert: e.message
  rescue ObjectStore::Error => e
    redirect_to new_upload_path, alert: "Upload failed: #{e.message}"
  end

  # Shows one upload and its derivatives as they complete.
  #
  # params[:id] is any job from the upload; the source key comes from its
  # payload, and the siblings are found by that key.
  def show
    @jobs = sibling_jobs(params[:id])
    if @jobs.empty?
      redirect_to new_upload_path, alert: "No jobs found for that upload."
      return
    end

    @anchor_id = params[:id]
    @source_key = @jobs.first.payload_data["source_key"]
    @source_url = object_store.presigned_url(@source_key)
    @derivatives = @jobs.map { |job| derivative_view(job) }
  end

  # Turbo Frame target: re-renders just the derivative grid.
  #
  # Polled every second while anything is still running. Once everything is
  # terminal the view stops refreshing, so an idle page costs nothing.
  def status
    jobs = sibling_jobs(params[:id])

    render partial: "uploads/derivatives",
           locals: {
             anchor_id: params[:id],
             derivatives: jobs.map { |job| derivative_view(job) },
             pending: jobs.any? { |j| !j.terminal? }
           }
  end

  private

  # Every derivative job from the same upload, found via the anchor job's
  # source key.
  def sibling_jobs(anchor_job_id)
    anchor = Job.find_by(id: anchor_job_id)
    return [] if anchor.nil?

    source_key = anchor.payload_data["source_key"]
    return [anchor] if source_key.blank?

    Job.where(job_type: "image.derive")
       .where("payload->>'source_key' = ?", source_key)
       .order(:priority)
       .to_a
  end

  def derivative_view(job)
    payload = job.payload_data
    result = job.result_data || {}

    {
      job:,
      label: payload["label"],
      width: payload["width"],
      priority: job.priority,
      status: job.status,
      # Presign only once the derivative actually exists. Signing a URL for an
      # object that has not been written yet produces a link that 404s, which
      # looks like a broken image rather than one still being generated.
      url: (object_store.presigned_url(payload["target_key"]) if job.status == "succeeded"),
      dimensions: result["width"] && "#{result['width']}x#{result['height']}",
      bytes: result["bytes"],
      total_ms: result["total_ms"],
      resize_ms: result["resize_ms"],
      worker: result["processed_by"],
      error: job.last_error
    }
  end

  # Recent uploads, derived from job payloads since there is no uploads table.
  #
  # Go owns the schema, and adding a Rails-owned table would break that rule for
  # something the jobs themselves already record.
  def recent_uploads
    Job.where(job_type: "image.derive")
       .where(priority: 0) # one row per upload: the thumbnail job
       .newest_first
       .limit(12)
       .map { |job| { id: job.id, key: job.payload_data["source_key"], at: job.enqueued_at } }
       .reject { |u| u[:key].blank? }
  end
end
