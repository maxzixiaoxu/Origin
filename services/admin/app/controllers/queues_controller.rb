# Live queue configuration.
#
# Everything here takes effect on the next dequeue: the queue service caches
# config for about a second and drops that cache on every admin write. Pausing a
# misbehaving queue during an incident is the main reason this page exists.
class QueuesController < ApplicationController
  def index
    @queues = JobQueue.alphabetical.to_a
    @depths = queue_client.depth.index_by { |d| d["queue"] }
  rescue QueueClient::Error => e
    @depths = {}
    flash.now[:alert] = e.message
  end

  def update
    queue_client.update_queue(params[:name], queue_params)
    redirect_to queues_path, notice: "#{params[:name]} updated."
  end

  def pause
    queue_client.pause(params[:name])
    redirect_to queues_path,
                notice: "#{params[:name]} paused. Submissions are still accepted; " \
                        "nothing will be dispatched."
  end

  def resume
    queue_client.resume(params[:name])
    redirect_to queues_path, notice: "#{params[:name]} resumed."
  end

  private

  # Blank fields are dropped rather than sent as nil.
  #
  # The API treats an omitted field as "leave it alone" and an explicit null as
  # "clear it". Submitting the whole form would otherwise wipe settings the
  # operator never touched -- and would create a lost-update race between two
  # people editing the same queue at once.
  def queue_params
    permitted = params.require(:queue).permit(
      :max_concurrency, :rate_limit_per_sec, :rate_limit_burst,
      :max_attempts, :visibility_timeout_sec,
      :backoff_base_ms, :backoff_cap_ms, :description
    )

    permitted.to_h.each_with_object({}) do |(key, value), out|
      next if value.blank?

      out[key] = key == "description" ? value : value.to_i
    end
  end
end
