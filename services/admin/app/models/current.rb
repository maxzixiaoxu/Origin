# Per-request state.
#
# Exists only to carry the request id down to QueueClient without threading it
# through every method signature. Rails already generates one per request; this
# makes it reachable from the service layer, so a single identifier spans the
# browser request, the Rails log, the Go service log, the job row, and the
# worker that eventually runs the job.
class Current < ActiveSupport::CurrentAttributes
  attribute :request_id
end
