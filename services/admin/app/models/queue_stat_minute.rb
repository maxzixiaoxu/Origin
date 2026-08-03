# One row of the Go-owned `queue_stats_minute` table: a pre-aggregated bucket.
#
# The charts read only from here and never from `jobs`. That is what keeps the
# overview page fast at ten million rows -- plotting a day of throughput is a
# few hundred indexed rows rather than a full scan of job history.
class QueueStatMinute < ApplicationRecord
  self.table_name = "queue_stats_minute"

  # Composite primary key (bucket, queue_name). Declared so ActiveRecord does
  # not assume an `id` column that does not exist.
  self.primary_key = [:bucket, :queue_name]

  scope :for_queue, ->(name) { where(queue_name: name) if name.present? }
  scope :since,     ->(time) { where(bucket: time..) }
  scope :chronological, -> { order(:bucket) }

  def total_completed
    succeeded.to_i + dead.to_i
  end

  def error_rate
    total = total_completed + failed.to_i
    return 0.0 if total.zero?

    (failed.to_i.to_f / total * 100).round(1)
  end
end
