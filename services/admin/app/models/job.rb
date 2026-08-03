# A row of the Go-owned `jobs` table.
#
# Read-only; see ApplicationRecord. Scopes here exist to keep the controllers
# thin and, more importantly, to keep every query aligned with an index that
# actually exists -- the schema was designed around these access patterns.
class Job < ApplicationRecord
  self.table_name = "jobs"

  has_many :job_attempts,
           -> { order(:attempt, :started_at) },
           foreign_key: :job_id,
           inverse_of: :job,
           dependent: nil

  # Mirrors the Postgres job_status enum and jobtypes.Status in Go. All three
  # must change together.
  STATUSES = %w[pending scheduled running succeeded failed dead cancelled].freeze

  TERMINAL_STATUSES = %w[succeeded dead cancelled].freeze
  ACTIVE_STATUSES   = %w[pending scheduled running].freeze

  # A job is replayable from the dead-letter UI only from these states.
  # Replaying a running job would create a second execution of work already in
  # flight.
  REPLAYABLE_STATUSES = %w[dead failed cancelled].freeze

  scope :in_queue,    ->(name) { where(queue_name: name) if name.present? }
  scope :with_status, ->(s)    { where(status: s) if s.present? }
  scope :of_type,     ->(t)    { where(job_type: t) if t.present? }

  scope :active,   -> { where(status: ACTIVE_STATUSES) }
  scope :terminal, -> { where(status: TERMINAL_STATUSES) }
  scope :dead,     -> { where(status: "dead") }

  # Matches jobs_queue_status_priority_idx.
  scope :dispatch_order, -> { order(:priority, enqueued_at: :desc) }
  scope :newest_first,   -> { order(enqueued_at: :desc) }

  # Matches jobs_finished_idx.
  scope :recently_finished, -> { terminal.order(finished_at: :desc) }

  scope :with_trace, ->(id) { where(trace_id: id) if id.present? }

  # Free-text search across the fields an operator actually knows.
  #
  # Deliberately narrow: id, type, and trace id. Searching the payload would
  # need a GIN index the schema does not carry, and would let one query scan the
  # whole table -- on the page most likely to be open during an incident.
  scope :search, ->(term) {
    next all if term.blank?

    if (uuid = term[/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i])
      where(id: uuid)
    else
      where("job_type ILIKE :t OR trace_id = :exact", t: "%#{sanitize_sql_like(term)}%", exact: term)
    end
  }

  def terminal? = TERMINAL_STATUSES.include?(status)
  def replayable? = REPLAYABLE_STATUSES.include?(status)
  def running? = status == "running"
  def dead? = status == "dead"

  def attempts_remaining
    [max_attempts - attempt, 0].max
  end

  # Time from submission to the first execution starting.
  #
  # The queue's real service-level signal. A queue can have fast handlers and
  # still be badly backed up, and only this number shows it -- execution
  # duration would look perfectly healthy the whole time.
  def queue_wait_ms
    return nil if started_at.blank? || enqueued_at.blank?

    ((started_at - enqueued_at) * 1000).round
  end

  # Total wall-clock from submission to finishing, across all attempts.
  def end_to_end_ms
    return nil if finished_at.blank? || enqueued_at.blank?

    ((finished_at - enqueued_at) * 1000).round
  end

  # The decoded handler result, or nil.
  #
  # Rescues rather than raising: a malformed result must not take down the job
  # detail page, which is exactly the page someone opens to find out why a job
  # misbehaved.
  def result_data
    return nil if result.blank?

    result.is_a?(Hash) ? result : JSON.parse(result.to_s)
  rescue JSON::ParserError
    nil
  end

  def payload_data
    return {} if payload.blank?

    payload.is_a?(Hash) ? payload : JSON.parse(payload.to_s)
  rescue JSON::ParserError
    {}
  end

  # True when this job produced an image derivative, so the view knows to render
  # a preview rather than a JSON blob.
  def image_derivative?
    job_type == "image.derive"
  end
end
