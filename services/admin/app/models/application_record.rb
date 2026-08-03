# Base class for every model in this app. All of them are READ ONLY, and that
# is enforced here rather than left to discipline.
#
# The queue's correctness depends on Go being the only writer. A Rails
# controller that "just" flipped a job's status would bypass the Redis lease set
# entirely -- marking a job succeeded while a worker is still running it, or
# resurrecting a dead job that exists in no Redis set at all. Those bugs would
# be intermittent, hard to attribute, and would look like queue bugs rather than
# dashboard bugs.
#
# Overriding `readonly?` makes the failure immediate and unmistakable instead:
# ActiveRecord::ReadOnlyRecord, at the call site, on the developer's machine.
# Writes go through QueueClient to the Go service, which owns the transitions.
class ApplicationRecord < ActiveRecord::Base
  primary_abstract_class

  def readonly?
    true
  end

  # Closes the gap `readonly?` leaves.
  #
  # `readonly?` blocks save and update on a loaded record, but `update_all`,
  # `delete_all`, and `destroy_all` operate on the relation and never
  # instantiate anything -- so they would rewrite the jobs table without ever
  # consulting it. Raising on the relation is what makes the model genuinely
  # read-only rather than read-only-if-you-use-it-the-usual-way.
  module ReadOnlyRelation
    def update_all(*)
      raise ActiveRecord::ReadOnlyRecord, readonly_message
    end

    def delete_all(*)
      raise ActiveRecord::ReadOnlyRecord, readonly_message
    end

    def destroy_all(*)
      raise ActiveRecord::ReadOnlyRecord, readonly_message
    end

    def insert_all(*)
      raise ActiveRecord::ReadOnlyRecord, readonly_message
    end

    private

    def readonly_message
      "#{klass.name} is read-only: the Go queue service owns every write. " \
        "Use QueueClient instead."
    end
  end

  def self.all
    super.extending(ReadOnlyRelation)
  end
end
