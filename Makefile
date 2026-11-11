# Every Go command runs in the tools container (D-021); npm runs on the host in console/.
# Docker Desktop must be running.

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

COMPOSE := docker compose
# Every non-default profile, so `down` and the compose check see every service.
ALL_PROFILES := --profile tools --profile chaos --profile bench
# The chaos stack: its own Compose project, with the chaos override on top (D-042).
CHAOS_COMPOSE := $(COMPOSE) -p jobqueue-chaos -f docker-compose.yml -f docker-compose.chaos.yml
# The bench stack: its own Compose project, with the bench override on top (D-053).
BENCH_COMPOSE := $(COMPOSE) -p jobqueue-bench -f docker-compose.yml -f docker-compose.bench.yml
TOOLS := $(COMPOSE) run --rm --no-deps tools
CONSOLE_DEPS := console/node_modules/.package-lock.json

.PHONY: help up down test lint audit console ci stack-check compose-config tools-image chaos chaos-one chaos-smoke bench

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

# Local dummy settings (D-035). .env is git-ignored; it is created once from the example.
.env:
	cp .env.example .env

up: .env ## Build and start the stack; wait until every service is healthy
	$(COMPOSE) up --detach --build --wait

down: .env ## Stop and remove every project container (volumes are kept; the chaos and bench stacks' are removed)
	$(COMPOSE) $(ALL_PROFILES) down --remove-orphans
	$(CHAOS_COMPOSE) $(ALL_PROFILES) down --volumes --remove-orphans
	$(BENCH_COMPOSE) $(ALL_PROFILES) down --volumes --remove-orphans

# Rebuilt on every run so a change to tools/Dockerfile takes effect; the layer cache makes it quick.
tools-image: .env
	$(COMPOSE) $(ALL_PROFILES) build --quiet tools

# The compose check reads these files, because the tools container has no Docker CLI (D-034):
# the normal stack, the chaos stack with its override (P3-8), and the bench stack with its
# override (D-053).
compose-config: .env
	@mkdir -p .cache
	$(COMPOSE) $(ALL_PROFILES) config --format json --no-path-resolution > .cache/compose-config.json
	$(CHAOS_COMPOSE) $(ALL_PROFILES) config --format json --no-path-resolution > .cache/compose-config-chaos.json
	$(BENCH_COMPOSE) $(ALL_PROFILES) config --format json --no-path-resolution > .cache/compose-config-bench.json

test: tools-image compose-config ## Go tests with -race, next to Postgres on the Compose network
	$(COMPOSE) up --detach --wait --quiet-pull postgres
	$(TOOLS) go test -race -count=1 ./...

lint: tools-image $(CONSOLE_DEPS) ## gofmt, golangci-lint, and the console typecheck
	$(TOOLS) sh -c 'unformatted=$$(gofmt -l $$(go list -f "{{.Dir}}" ./...)); if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi'
	$(TOOLS) golangci-lint run ./...
	cd console && npm run typecheck

audit: tools-image $(CONSOLE_DEPS) ## govulncheck and npm audit
	$(TOOLS) govulncheck ./...
	cd console && npm audit --audit-level=moderate

console: $(CONSOLE_DEPS) ## Typecheck and build the console
	cd console && npm run typecheck && npm run build

$(CONSOLE_DEPS): console/package.json console/package-lock.json
	cd console && npm ci

stack-check: .env ## P1-1: make up is healthy, then make down leaves nothing running
	scripts/stack-check.sh

# Chaos (design §18). Each target runs on the separate jobqueue-chaos stack and removes it,
# volume included, when it ends (D-042). CHAOS_BASE_SEED=<n> repeats a batch (D-046).
chaos: tools-image ## P3-1: 100 seeded chaos runs, then crash recovery; results in bench/results/chaos/
	scripts/chaos.sh --runs 100 --recovery

chaos-one: tools-image ## P3-2: replay one chaos run: CHAOS_SEED=<n> make chaos-one
	@[ -n "$(CHAOS_SEED)" ] || { echo "usage: CHAOS_SEED=<n> make chaos-one" >&2; exit 2; }
	scripts/chaos.sh --seed "$(CHAOS_SEED)"

chaos-smoke: tools-image ## P3-9: the 3-run chaos smoke CI runs; results in .cache/chaos-results/
	scripts/chaos.sh --runs 3 --results /src/.cache/chaos-results

# Black Friday (design §17). It runs on the separate jobqueue-bench stack and removes it,
# volume included, when it ends (D-053). Stop `make up` first: it would compete for CPU.
bench: tools-image ## P5-6: the FIFO and DRR Black Friday runs, chart and report; results in bench/results/
	scripts/bench.sh

ci: lint test audit console stack-check ## Everything CI runs, except the chaos smoke step
