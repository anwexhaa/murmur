# Recipes are POSIX sh, not bash: GNU Make on Windows resolves SHELL to Git's
# sh.exe regardless of what this file asks for, so bashisms silently break
# there and nowhere else.
.SHELLFLAGS := -eu -c
.DEFAULT_GOAL := help

COMPOSE  := docker compose -f deploy/docker-compose.yml
PG_DSN   ?= postgres://murmur:murmur@localhost:5432/murmur?sslmode=disable
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
up: ## Start Postgres, Redis and NATS, then migrate
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

# Build-then-run rather than `go run`. `go run` executes from the system temp
# directory, which Windows Application Control blocks on some machines; a
# binary under ./bin runs everywhere. It is also faster on repeat invocations.
bin/migrate$(EXE): $(wildcard cmd/migrate/*.go) go.mod go.sum
	@mkdir -p bin
	go build -o "$@" ./cmd/migrate

.PHONY: migrate
migrate: bin/migrate$(EXE) ## Apply all pending migrations
	./bin/migrate$(EXE) -dsn "$(PG_DSN)" up

.PHONY: migrate-down
migrate-down: bin/migrate$(EXE) ## Roll back the most recent migration
	./bin/migrate$(EXE) -dsn "$(PG_DSN)" down

.PHONY: migrate-status
migrate-status: bin/migrate$(EXE) ## Show which migrations have been applied
	./bin/migrate$(EXE) -dsn "$(PG_DSN)" status

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

# ---------------------------------------------------------------------- check

.PHONY: test
test: ## Run the test suite
	go test -count=1 ./...

# The race detector needs cgo, which on Windows means installing a C toolchain.
# Running it in the same Linux image CI uses is both easier and more honest:
# races are found on the platform the services actually deploy to.
.PHONY: test-race
test-race: ## Run the test suite under the race detector (Linux container)
	docker run --rm \
		-v "$(CURDIR):/src" \
		-v "$(HOME)/go/pkg/mod:/go/pkg/mod" \
		-w /src golang:1.27 \
		go test -race -count=1 ./...

.PHONY: cover
cover: ## Run tests and print a coverage summary
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# staticcheck is a tool dependency in go.mod, so its version is pinned with
# everything else. Built to ./bin for the same reason as migrate.
bin/staticcheck$(EXE): go.mod go.sum
	@mkdir -p bin
	go build -o "$@" honnef.co/go/tools/cmd/staticcheck

.PHONY: lint
lint: bin/staticcheck$(EXE) ## Run go vet and staticcheck
	go vet ./...
	./bin/staticcheck$(EXE) ./...

.PHONY: fmt
fmt: ## Format the tree
	go fmt ./...

# Note: formatting is *checked* by TestEveryGoFileIsFormatted rather than by a
# gofmt step here. The check then holds on machines where the gofmt binary
# cannot be executed, and `make test` is the single gate.
.PHONY: check
check: lint test-race ## Everything CI runs

# ------------------------------------------------------------------- services

.PHONY: run-gateway run-social run-timeline run-fanout
run-gateway: ## Run the GraphQL edge
	@go build -o bin/gateway$(EXE) ./cmd/gateway && ./bin/gateway$(EXE)

run-social: ## Run the write-path service
	@go build -o bin/social-svc$(EXE) ./cmd/social-svc && ./bin/social-svc$(EXE)

run-timeline: ## Run the read-path service
	@go build -o bin/timeline-svc$(EXE) ./cmd/timeline-svc && ./bin/timeline-svc$(EXE)

run-fanout: ## Run the fanout worker
	@go build -o bin/fanout-worker$(EXE) ./cmd/fanout-worker && ./bin/fanout-worker$(EXE)

.PHONY: clean
clean: ## Remove build output
	rm -rf bin coverage.out
