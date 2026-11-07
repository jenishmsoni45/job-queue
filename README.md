# Multi-tenant durable job queue

A Go job queue on Postgres in which Shopify-style shops share workers. It delivers at least once, using leases and fenced completions, and keeps shops fair with weighted deficit round robin, rate limits and admission control. A local mock stands in for Shopify's webhooks and Admin API, and a small React console shows each shop's state.

**Status:** phase 1 (scaffold). The stack builds and starts, and every service reports healthy. The queue itself arrives in phase 2. See [`docs/phases.md`](docs/phases.md).

## Requirements

- Docker Desktop, running
- Node.js (the version in [`console/.nvmrc`](console/.nvmrc)) and npm, for the console
- `make`, `git` and `curl`

Go is not needed on the host. Every Go command runs in a tools container (`tools/Dockerfile`, D-021).

## Commands

| Command | What it does |
|---|---|
| `make up` | Build and start the stack, and wait until every service is healthy |
| `make down` | Stop and remove the project's containers (volumes are kept) |
| `make test` | Go tests with `-race`, run in the tools container next to Postgres |
| `make lint` | `gofmt`, golangci-lint, and the console typecheck |
| `make audit` | govulncheck and `npm audit` |
| `make console` | Typecheck and build the console |
| `make stack-check` | `make up`, check every service, then `make down` |
| `make ci` | Everything CI runs |

The first `make` copies `.env.example` to `.env`. Both hold local dummy values only.

## Services

`make up` starts Postgres 16, Toxiproxy, Prometheus, a one-shot `migrate`, the three `queued` roles (`api`, `dispatcher`, and two `worker` processes) and `mockshop`. Published ports listen on `127.0.0.1` only:

- `api`: <http://127.0.0.1:8080/healthz>
- `mockshop`: <http://127.0.0.1:8090/healthz>
- Prometheus: <http://127.0.0.1:9090>

## Layout

```
cmd/         queued (api, dispatcher, worker, migrate, healthcheck), mockshop, loadgen
internal/    httpserver, migrate, pgtest; store, lease, scheduler, admission, webhook (later phases)
migrations/  embedded SQL, applied in order by `queued migrate`
console/     React + TypeScript (Vite)
tests/       compose checks; invariants, adversarial, chaos (later phases)
config/      Prometheus config
tools/       the tools image
bench/       experiment configs and results (phase 5)
docs/        requirements, architecture, design, decisions, phases
```

## Docs

- [`docs/requirements.md`](docs/requirements.md): what the queue must do, and how each requirement is checked
- [`docs/architecture.md`](docs/architecture.md): diagrams
- [`docs/design.md`](docs/design.md): how it works
- [`docs/decisions.md`](docs/decisions.md): every decision and why
- [`docs/phases.md`](docs/phases.md): the build plan and acceptance criteria
