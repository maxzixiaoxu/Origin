# Rails admin dashboard.
#
# Ruby is only ever installed in this image -- there is no local Ruby
# requirement for working on this project, and `make` wraps every Rails command
# in `docker compose run`.

FROM ruby:3.4-slim

# libpq-dev and build-essential are needed to compile the pg gem's native
# extension. They stay in the image rather than being removed in a later layer:
# this is a development and demo stack, and `bundle install` on a mounted
# Gemfile has to keep working after the image is built.
RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y \
        build-essential libpq-dev libyaml-dev git curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Gemfile first, so editing application code does not invalidate the bundle
# layer. Without this every code change reinstalls every gem.
COPY services/admin/Gemfile services/admin/Gemfile.lock* ./
RUN bundle config set --local without "" && \
    bundle install --jobs 4 --retry 3

COPY services/admin/ ./

# A dummy key so `assets:precompile` can boot the app. It is never used to
# encrypt anything: this stack stores no credentials, and the real deployment
# would supply RAILS_MASTER_KEY.
ENV RAILS_ENV=development \
    SECRET_KEY_BASE=dummy-key-for-local-development-only \
    RAILS_LOG_TO_STDOUT=true

EXPOSE 3000

# Bind to 0.0.0.0, not localhost: Puma bound to the loopback inside a container
# is unreachable from the host, which presents as a connection refused that
# looks like the app failed to start.
CMD ["bash", "-c", "bundle install --quiet && bin/rails server -b 0.0.0.0 -p 3000"]
