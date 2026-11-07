# Every Go command runs in the tools container (D-021); npm runs on the host in console/.
# Docker Desktop must be running.

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

COMPOSE := docker compose
# Every non-default profile, so `down` and the compose check see every service.
ALL_PROFILES := --profile tools
TOOLS := $(COMPOSE) run --rm --no-deps tools
CONSOLE_DEPS := console/node_modules/.package-lock.json

.PHONY: help up down test lint audit console ci stack-check compose-config tools-image

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

# Local dummy settings (D-035). .env is git-ignored; it is created once from the example.
.env:
	cp .env.example .env

up: .env ## Build and start the stack; wait until every service is healthy
	$(COMPOSE) up --detach --build --wait

down: .env ## Stop and remove every project container (volumes are kept)
	$(COMPOSE) $(ALL_PROFILES) down --remove-orphans

# Rebuilt on every run so a change to tools/Dockerfile takes effect; the layer cache makes it quick.
tools-image: .env
	$(COMPOSE) $(ALL_PROFILES) build --quiet tools

# The compose check reads this file, because the tools container has no Docker CLI (D-034).
compose-config: .env
	@mkdir -p .cache
	$(COMPOSE) $(ALL_PROFILES) config --format json --no-path-resolution > .cache/compose-config.json

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

ci: lint test audit console stack-check ## Everything CI runs
