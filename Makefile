# Distributed job queue -- developer entry points.
#
# Every target is safe to run repeatedly. Anything requiring Ruby runs inside a
# container, so no local Ruby install is needed.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE       := docker compose
MIGRATE_IMAGE := migrate/migrate:v4.18.1

# Host ports default into a 5xxxx block to avoid colliding with a Postgres or
# Redis already running on the conventional ports. See .env.example.
JOBQ_POSTGRES_PORT ?= 55432
JOBQ_REDIS_PORT    ?= 56379

# DB_URL is for tools running on the host; DB_URL_DOCKER is for containers on
# the compose network, which always reach Postgres on its internal port.
DB_URL        ?= postgres://jobq:jobq@localhost:$(JOBQ_POSTGRES_PORT)/jobq?sslmode=disable
DB_URL_DOCKER := postgres://jobq:jobq@postgres:5432/jobq?sslmode=disable
REDIS_ADDR    ?= localhost:$(JOBQ_REDIS_PORT)

# Colour helpers for readable output.
BOLD := \033[1m
DIM  := \033[2m
OFF  := \033[0m

.PHONY: help
help: ## Show available targets
	@echo -e "$(BOLD)Distributed Job Queue$(OFF)"
	@echo
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-22s\033[0m %s\n", $$1, $$2}'

# --- stack ----------------------------------------------------------------

.PHONY: up
up: ## Start the stack and apply migrations
	$(COMPOSE) up -d --wait postgres redis
	$(MAKE) migrate
	@echo -e "$(DIM)postgres :$(JOBQ_POSTGRES_PORT)   redis :$(JOBQ_REDIS_PORT)$(OFF)"

.PHONY: down
down: ## Stop the stack (volumes preserved)
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop the stack and delete all data volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

.PHONY: ps
ps: ## Show service status
	$(COMPOSE) ps

# --- database -------------------------------------------------------------

.PHONY: migrate
migrate: ## Apply all pending migrations
	$(COMPOSE) run --rm migrate

.PHONY: migrate-down
migrate-down: ## Roll back the most recent migration
	$(COMPOSE) run --rm migrate -path /migrations -database "$(DB_URL_DOCKER)" down 1

.PHONY: migrate-reset
migrate-reset: ## Roll back every migration, then re-apply
	$(COMPOSE) run --rm migrate -path /migrations -database "$(DB_URL_DOCKER)" down -all
	$(MAKE) migrate

.PHONY: migrate-new
migrate-new: ## Create a migration pair: make migrate-new NAME=add_foo
	@test -n "$(NAME)" || { echo "usage: make migrate-new NAME=add_foo"; exit 1; }
	docker run --rm -v "$(PWD)/migrations:/migrations" $(MIGRATE_IMAGE) \
		create -ext sql -dir /migrations -seq $(NAME)

.PHONY: psql
psql: ## Open a psql shell against the running database
	$(COMPOSE) exec postgres psql -U jobq -d jobq

.PHONY: redis-cli
redis-cli: ## Open a redis-cli shell against the running Redis
	$(COMPOSE) exec redis redis-cli

.PHONY: schema
schema: ## Print the applied schema
	$(COMPOSE) exec postgres psql -U jobq -d jobq -c '\d+ jobs' -c '\d+ queues'

# --- go -------------------------------------------------------------------

.PHONY: build
build: ## Compile all Go binaries into ./bin
	go build -o bin/ ./...

.PHONY: test
test: ## Run Go tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-short
test-short: ## Run only fast unit tests (skips container-backed integration)
	go test -short -race -count=1 ./...

.PHONY: cover
cover: ## Run tests and open an HTML coverage report
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...
	go tool cover -html=coverage.txt -o coverage.html
	@echo "wrote coverage.html"

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: fmt
fmt: ## Format all Go source
	gofmt -s -w $$(git ls-files '*.go' 2>/dev/null || find . -name '*.go' -not -path './vendor/*')

.PHONY: tidy
tidy: ## Tidy and verify module dependencies
	go mod tidy
	go mod verify

# --- protobuf -------------------------------------------------------------
#
# Generated stubs in gen/ are committed, so building the project needs none of
# these tools. They are only required when editing a .proto.

PROTOC_GEN_GO_VERSION      := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
BUF_VERSION                := v1.47.2
GOBIN                      := $(shell go env GOPATH)/bin

.PHONY: proto-tools
proto-tools: ## Install pinned protobuf codegen tools
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

.PHONY: proto
proto: ## Regenerate protobuf stubs into gen/
	PATH="$(PATH):$(GOBIN)" buf generate

.PHONY: proto-lint
proto-lint: ## Lint .proto files
	PATH="$(PATH):$(GOBIN)" buf lint

.PHONY: proto-check
proto-check: ## Fail if committed stubs are stale relative to the .proto files
	@$(MAKE) proto
	@if ! git diff --quiet -- gen/ 2>/dev/null; then \
		echo "ERROR: generated stubs are out of date; commit the result of 'make proto'"; \
		git diff --stat -- gen/; \
		exit 1; \
	fi
	@echo "generated stubs are up to date"

.PHONY: check
check: fmt vet test ## Format, vet, and test
