# Shared build for both Go services.
#
# One Dockerfile parameterised by BINARY rather than two near-identical files:
# they compile from the same module with the same flags, and keeping them in
# sync by hand is how one service quietly ends up on a different Go version.

# --- build ---------------------------------------------------------------
FROM golang:1.26-alpine AS build

# git is needed for module resolution; the rest of the toolchain is in the base.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Dependencies are copied and downloaded before the source, so editing a .go
# file does not invalidate the module cache layer. This is the difference
# between a ~5 second and a ~90 second incremental build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CMD_PATH is the full package path rather than a name to interpolate.
#
# Deriving it from the binary name would be shorter but wrong: the service
# directories are services/queue and services/worker while the binaries are
# queued and workerd, so any naming convention baked in here breaks the moment
# the two disagree. Passing the path explicitly cannot drift.
ARG CMD_PATH
ARG VERSION=dev

# CGO_ENABLED=0 produces a static binary, which is what lets the runtime stage
# be a bare Alpine with no libc compatibility concerns.
#
# -trimpath strips local filesystem paths from the binary: without it, every
# panic stack trace in production leaks the build machine's directory layout.
RUN test -n "$CMD_PATH" || (echo "CMD_PATH build arg is required" && exit 1) && \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/app \
        "./${CMD_PATH}"

# --- runtime -------------------------------------------------------------
FROM alpine:3.21

# Alpine rather than distroless, deliberately.
#
# Distroless is smaller and has less attack surface, but it contains no shell
# and no HTTP client, so a container healthcheck would have to be built into
# the binary as a special flag. Alpine is 8MB, gives compose a working
# healthcheck for free, and leaves a shell available for `docker exec` during a
# demo -- which is worth more here than the marginal size saving.
RUN apk add --no-cache ca-certificates tzdata wget && \
    adduser -D -u 10001 -h /app app

WORKDIR /app

COPY --from=build /out/app /app/app

# Non-root. A worker that runs as root has no reason to, and it is the cheapest
# hardening available.
USER 10001:10001

ENTRYPOINT ["/app/app"]
