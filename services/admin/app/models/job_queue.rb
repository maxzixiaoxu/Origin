# One row of the Go-owned `queues` table.
#
# Every setting here is live: the queue service caches configuration for about a
# second and drops that cache whenever the admin API writes, so a pause or a
# rate-limit change takes effect on the next dequeue rather than after a
# restart. That is the property that makes this page useful during an incident.
# NOT named `Queue`.
#
# Ruby defines ::Queue in core as an alias for Thread::Queue, and a model class
# named Queue gets silently shadowed by it -- `Queue.alphabetical` then fails
# with "undefined method for class Thread::Queue", which reads like an
# ActiveRecord problem and is not. Zeitwerk will not warn, because the constant
# genuinely resolves; it just resolves to the wrong thing.
class JobQueue < ApplicationRecord
  self.table_name = "queues"
  self.primary_key = "name"

  has_many :jobs, foreign_key: :queue_name, inverse_of: nil, dependent: nil

  scope :alphabetical, -> { order(:name) }

  def rate_limited?
    rate_limit_per_sec.present? && rate_limit_per_sec.positive?
  end

  def rate_limit_label
    return "unlimited" unless rate_limited?

    "#{rate_limit_per_sec}/sec (burst #{rate_limit_burst})"
  end

  def visibility_timeout
    visibility_timeout_sec.seconds
  end

  # Worst-case time between a worker dying and its job becoming available again.
  #
  # Surfaced in the UI because it is the number people actually want when
  # tuning: it is the visibility timeout plus one reaper tick, NOT the job
  # duration. Workers heartbeat every few seconds, so the timeout only needs to
  # outlast a couple of missed beats -- it does not need to cover how long a job
  # runs, which is the common misconception this label is meant to correct.
  def worst_case_recovery
    visibility_timeout_sec.seconds + Rails.configuration.x.reap_interval
  end
end
