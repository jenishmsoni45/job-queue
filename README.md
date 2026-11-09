# Multi-tenant durable job queue

A Go job queue on Postgres in which Shopify-style shops share workers. It delivers at least once, using leases and fenced completions, and keeps shops fair with weighted deficit round robin, rate limits and admission control. A local mock stands in for Shopify's webhooks and Admin API, and a small React console shows each shop's state.

**Status:** phase 2 (core queue). Jobs can be enqueued and read through the API. The dispatcher claims them in FIFO order for workers that long-poll it. Workers run handlers under leases renewed by batched heartbeats, and write fenced results. The reaper requeues expired leases. Fairness, caps, rate limits, admission, dedupe and webhooks arrive in later phases. See [`docs/phases.md`](docs/phases.md).

Delivery is at-least-once: a job can run more than once, so handlers must be idempotent.

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
| `make stack-check` | `make up`, check every service and run one job through the stack, then `make down` |
| `make ci` | Everything CI runs |

The first `make` copies `.env.example` to `.env`. Both hold local dummy values only.

## Services

`make up` starts Postgres 16, Toxiproxy, Prometheus, a one-shot `migrate`, the three `queued` roles (`api`, `dispatcher`, and two `worker` processes) and `mockshop`. The `queued` roles read the shops and job kinds from [`config/shops.json`](config/shops.json); `api` seeds the database from it at startup. Settings come from the environment and are listed in [`cmd/queued/main.go`](cmd/queued/main.go). Published ports listen on `127.0.0.1` only:

- `api`: <http://127.0.0.1:8080/healthz>, plus `POST /v1/jobs` and `GET /v1/jobs/{id}` (design §11.1)
- `mockshop`: <http://127.0.0.1:8090/healthz>
- Prometheus: <http://127.0.0.1:9090>

## Layout

```
cmd/         queued (api, dispatcher, worker, migrate, healthcheck), mockshop, loadgen
internal/    api, dispatcher, queue (handler API and worker), lease, store, config, clock,
             testkinds, httpserver, migrate; pgtest and queuetest (test helpers);
             scheduler, admission, webhook (later phases)
migrations/  embedded SQL, applied in order by `queued migrate`
console/     React + TypeScript (Vite)
tests/       compose checks, invariants (Q1 basic, Q2, Q3, backlog counter), adversarial
             (skewed worker clocks), testdata; chaos (phase 3)
config/      shops and job kinds (shops.json), Prometheus config
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
