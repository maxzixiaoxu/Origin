require_relative "boot"

require "rails"

# Frameworks are picked explicitly rather than via rails/all.
#
# Active Job is deliberately ABSENT. This application is the admin interface for
# a job queue written in Go; loading a second, unrelated job system would be
# misleading to anyone reading the code, and would hand a future contributor a
# `perform_later` that silently bypasses the entire system this app exists to
# operate.
#
# Active Storage is absent for a different reason: it owns tables and ships its
# own migrations, and Go owns the schema here. Uploads go to S3 directly.
require "active_model/railtie"
require "active_record/railtie"
require "action_controller/railtie"
require "action_view/railtie"

Bundler.require(*Rails.groups)

module Admin
  class Application < Rails::Application
    config.load_defaults 8.1

    config.autoload_lib(ignore: %w[assets tasks])

    config.generators.system_tests = nil

    # Rails runs NO migrations against this database.
    #
    # The schema is owned by Go and applied with golang-migrate; see migrations/
    # at the repository root. Rails connects to the result with read-only
    # models. Leaving schema maintenance enabled would let a stray `db:prepare`
    # -- which several Rails commands invoke implicitly -- try to load a
    # schema.rb that does not exist, and in the worst case recreate the database
    # empty underneath a running queue.
    config.active_record.maintain_test_schema = false

    # Timestamps render in UTC, matching the queue service and the database. A
    # dashboard that silently localises them turns correlating a Rails page
    # against a Go log line into timezone arithmetic, during an incident.
    config.time_zone = "UTC"

    # Trace propagation. Rails already generates X-Request-Id per request; this
    # makes it the same header the Go services read, so one identifier follows a
    # job from the browser click through Rails, the queue service, the job row,
    # and the worker that runs it.
    config.action_dispatch.rack_cache = false
  end
end
