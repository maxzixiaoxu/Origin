class ApplicationController < ActionController::Base
  before_action :set_current_request_id

  # QueueClient failures are operational conditions, not bugs. The queue service
  # restarting mid-click should show a banner, not a stack trace -- the
  # dashboard's whole job is to be usable while something else is broken.
  rescue_from QueueClient::Error, with: :handle_queue_error
  rescue_from ActiveRecord::RecordNotFound, with: :handle_not_found

  private

  def set_current_request_id
    Current.request_id = request.request_id
  end

  def queue_client
    @queue_client ||= QueueClient.new
  end

  def object_store
    @object_store ||= ObjectStore.new
  end

  def handle_queue_error(error)
    logger.error("queue service error: #{error.message}")

    respond_to do |format|
      format.html do
        redirect_back fallback_location: root_path, alert: error.message
      end
      format.json { render json: { error: error.message }, status: :bad_gateway }
    end
  end

  def handle_not_found
    respond_to do |format|
      format.html { redirect_to jobs_path, alert: "That job no longer exists." }
      format.json { render json: { error: "not found" }, status: :not_found }
    end
  end
end
