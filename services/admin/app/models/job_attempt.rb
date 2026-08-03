# One row of the Go-owned `job_attempts` table: a single execution attempt.
#
# There can legitimately be MORE THAN ONE row per attempt number. Under
# at-least-once delivery the reaper records 'lease_expired' for a worker it
# presumed dead, and that worker -- merely slow, not dead -- may then report its
# own outcome for the same attempt. Both are true observations, and the timeline
# shows both rather than hiding the more interesting one.
class JobAttempt < ApplicationRecord
  self.table_name = "job_attempts"

  belongs_to :job, inverse_of: :job_attempts

  # Mirrors jobtypes.Outcome in Go.
  OUTCOMES = %w[succeeded failed timeout lease_expired panic cancelled].freeze

  scope :chronological, -> { order(:attempt, :started_at) }
  scope :failures,      -> { where.not(outcome: "succeeded") }

  # Attempts the reaper reclaimed, i.e. the worker died mid-job. Kept queryable
  # separately from ordinary failures so "how often are workers dying?" is
  # answerable without it being confused with "how buggy are the handlers?".
  scope :lease_expired, -> { where(outcome: "lease_expired") }

  def succeeded? = outcome == "succeeded"
  def lease_expired? = outcome == "lease_expired"

  # How this attempt should read in the timeline.
  def summary
    case outcome
    when "succeeded"     then "completed"
    when "lease_expired" then "worker stopped responding; lease reclaimed"
    when "timeout"       then "exceeded the job timeout"
    when "panic"         then "handler panicked"
    when "cancelled"     then "cancelled or drained"
    else                      "failed"
    end
  end

  # Colour class for the timeline entry.
  def badge
    case outcome
    when "succeeded"                then "ok"
    when "lease_expired", "timeout" then "warn"
    else                                 "bad"
    end
  end
end
