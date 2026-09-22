# Recipes are POSIX sh, not bash: GNU Make on Windows resolves SHELL to Git's
# sh.exe regardless of what this file asks for, so bashisms silently break
# there and nowhere else.
.SHELLFLAGS := -eu -c
.DEFAULT_GOAL := help

COMPOSE  := docker compose -f deploy/docker-compose.yml
SERVICES := gateway social-svc timeline-svc fanout-worker
CMDS     := $(SERVICES) migrate

ifeq ($(OS),Windows_NT)
EXE := .exe
else
EXE :=
endif

.PHONY: help
help: ## Show the available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---------------------------------------------------------------- environment

.PHONY: up
up: ## Start Postgres, Redis, NATS and Jaeger, then migrate
	$(COMPOSE) up -d --wait
	$(MAKE) migrate

.PHONY: down
down: ## Stop the stack, keeping data
	$(COMPOSE) down

.PHONY: reset
reset: ## Destroy the stack and its volumes, then start clean
	$(COMPOSE) down -v
	$(MAKE) up

.PHONY: logs
logs: ## Follow the stack logs
	$(COMPOSE) logs -f

# ----------------------------------------------------------------- migrations

# Migrations run in the container like everything else that executes a freshly
# built binary. This was native until Application Control began blocking
# ./bin/migrate after a rebuild changed its contents — the policy decides per
# binary and re-decides on every build, so "it worked yesterday" is not a
# property worth depending on.
.PHONY: migrate
migrate: ## Apply all pending migrations
	$(DEV_RUN) $(GO_IMAGE) go run ./cmd/migrate up

.PHONY: migrate-down
migrate-down: ## Roll back the most recent migration
	$(DEV_RUN) $(GO_IMAGE) go run ./cmd/migrate down

.PHONY: migrate-status
migrate-status: ## Show which migrations have been applied
	$(DEV_RUN) $(GO_IMAGE) go run ./cmd/migrate status

# --------------------------------------------------------------------- build

.PHONY: build
build: ## Build every binary into ./bin
	@mkdir -p bin
	@for cmd in $(CMDS); do \
		echo "  building $$cmd"; \
		go build -o "bin/$$cmd$(EXE)" "./cmd/$$cmd"; \
	done

.PHONY: tidy
tidy: ## Sync go.mod and go.sum
	go mod tidy

# Codegen runs in a container so the plugins are built from the versions
# pinned in go.mod and nothing has to be installed on the host. The output is
# committed, so this is rare and CI never runs it.
.PHONY: generate
generate: ## Regenerate protobuf and gRPC code from api/proto
	$(GO_IN_CONTAINER) sh scripts/generate.sh

# ---------------------------------------------------------------------- check

# The Go toolchain runs in a container for everything that has to execute a
# binary it just built. `go test` writes its test binaries to the system temp
# directory and runs them, which this machine's Application Control policy
# blocks; the race detector separately needs cgo. Both problems disappear in
# the same Linux image CI uses, which is also the platform these services
# deploy to.
#
# MSYS_NO_PATHCONV stops Git Bash rewriting container-side paths into Windows
# ones — without it, `-w /src` becomes `-w C:/Program Files/Git/src`. The
# variable is meaningless on Linux and macOS, and harmless there.
DOCKER = MSYS_NO_PATHCONV=1 docker

GO_IMAGE = golang:1.27

# Split so targets needing extra docker flags can insert them before the image
# name, which must be the last thing on the command line.
#
# The module cache is a named volume rather than a host directory: $(HOME) is
# a backslash path under Git Bash and does not survive being spliced into a
# volume spec. A named volume needs no host path at all.
GO_DOCKER_FLAGS = run --rm \
	-v "$(CURDIR):/src" \
	-v murmur-gomodcache:/go/pkg/mod \
	-w /src

GO_IN_CONTAINER = $(DOCKER) $(GO_DOCKER_FLAGS) $(GO_IMAGE)

.PHONY: test
test: ## Run the unit tests under the race detector
	$(GO_IN_CONTAINER) go test -race -short -count=1 ./...

.PHONY: test-integration
test-integration: testdb ## Run every test, including those needing Postgres, Redis and NATS
	$(DOCKER) $(GO_DOCKER_FLAGS) --network murmur_default \
		-e MURMUR_TEST_POSTGRES_DSN="postgres://murmur:murmur@postgres:5432/murmur_test?sslmode=disable" \
		-e MURMUR_TEST_REDIS_ADDR="redis:6379" \
		-e MURMUR_TEST_REDIS_DB="1" \
		-e MURMUR_TEST_NATS_URL="nats://nats:4222" \
		$(GO_IMAGE) go test -race -count=1 -timeout 15m ./...

# Integration tests get their own database so a run never destroys the graph
# in the development one. They truncate between cases, which would otherwise
# wipe whatever the seeder just spent four minutes building.
.PHONY: testdb
testdb: ## Create the integration-test database if it is missing
	@$(DOCKER) exec murmur-postgres-1 psql -U murmur -d murmur -tAc \
		"SELECT 1 FROM pg_database WHERE datname='murmur_test'" | grep -q 1 \
		|| $(DOCKER) exec murmur-postgres-1 createdb -U murmur murmur_test

.PHONY: test-race
test-race: test ## Alias for test, which already runs with -race

# The benchmark that chooses the fanout threshold. See
# docs/adr-001-fanout-threshold.md.
.PHONY: bench-threshold
bench-threshold: ## Measure the push/pull crossover against a real Redis
	$(DOCKER) $(GO_DOCKER_FLAGS) --network murmur_default \
		-e MURMUR_TEST_REDIS_ADDR="redis:6379" \
		-e MURMUR_TEST_REDIS_DB="1" \
		$(GO_IMAGE) go test -count=1 -timeout 25m \
		-run TestThresholdCrossover -bench 'PushFanout|PullRead|ReadBaseline' \
		-benchtime 300x -v ./internal/fanout/

.PHONY: cover
cover: ## Run tests and print a coverage summary
	$(GO_IN_CONTAINER) sh -c 'go test -short -count=1 -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -30'

.PHONY: lint
lint: ## Run go vet and staticcheck
	$(GO_IN_CONTAINER) sh -c 'go vet ./... && go tool staticcheck ./...'

.PHONY: fmt
fmt: ## Format the tree
	$(GO_IN_CONTAINER) gofmt -w -l .

# Note: formatting is *checked* by TestEveryGoFileIsFormatted rather than by a
# gofmt step here. The check then holds on machines where the gofmt binary
# cannot be executed, and `make test` is the single gate.
.PHONY: check
check: lint test-race ## Everything CI runs

# ------------------------------------------------------------------- services

# Services run inside the dev container, attached to the compose network.
#
# Two reasons. Windows Application Control blocks freshly built binaries from
# executing on the host, unpredictably and by content, so a host run is not
# reliable here. And containers are where these processes run from phase 8
# onward, so local behaviour matches deployed behaviour — service discovery by
# hostname included.
DEV_RUN = $(DOCKER) run --rm -i \
	-v "$(CURDIR):/src" \
	-v murmur-gomodcache:/go/pkg/mod \
	-w /src \
	--network murmur_default \
	-e POSTGRES_DSN="postgres://murmur:murmur@postgres:5432/murmur?sslmode=disable" \
	-e REDIS_ADDR="redis:6379" \
	-e NATS_URL="nats://nats:4222" \
	-e SOCIAL_ADDR="murmur-social:9081" \
	-e TIMELINE_ADDR="murmur-timeline:9082" \
	-e OTEL_EXPORTER_OTLP_ENDPOINT="jaeger:4317"

.PHONY: seed
seed: ## Seed a social graph (make seed ARGS="-users 600000 -whale-followers 500000")
	$(DEV_RUN) $(GO_IMAGE) go run ./cmd/seed $(ARGS)

SMOKE_TARGET ?= murmur-social:9081

# grpcurl is a tool dependency in go.mod, so its version is pinned alongside
# everything else rather than resolved from the network per invocation.
.PHONY: smoke
smoke: ## Drive a running social-svc end to end over gRPC (needs make run-social)
	$(DEV_RUN) $(GO_IMAGE) sh -c \
		'go build -o /tmp/grpcurl github.com/fullstorydev/grpcurl/cmd/grpcurl && \
		 GRPCURL=/tmp/grpcurl sh scripts/smoke.sh $(SMOKE_TARGET)'

.PHONY: grpcurl
grpcurl: ## Call the social service (make grpcurl ARGS="murmur-social:9081 list")
	$(DEV_RUN) $(GO_IMAGE) go tool grpcurl -plaintext $(ARGS)

# Fixed container names so `make smoke` and grpcurl can resolve a service by
# hostname on the compose network.
.PHONY: run-gateway run-social run-timeline run-fanout
run-gateway: ## Run the GraphQL edge
	$(DEV_RUN) --name murmur-gateway -p 8080:8080 -p 9080:9080 $(GO_IMAGE) go run ./cmd/gateway

run-social: ## Run the write-path service
	$(DEV_RUN) --name murmur-social -p 8081:8081 -p 9081:9081 $(GO_IMAGE) go run ./cmd/social-svc

run-timeline: ## Run the read-path service
	$(DEV_RUN) --name murmur-timeline -p 8082:8082 -p 9082:9082 $(GO_IMAGE) go run ./cmd/timeline-svc

run-fanout: ## Run the fanout worker
	$(DEV_RUN) --name murmur-fanout -p 8083:8083 $(GO_IMAGE) go run ./cmd/fanout-worker

.PHONY: clean
clean: ## Remove build output
	rm -rf bin coverage.out
