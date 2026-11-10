# Multi-tenant durable job queue

A Go job queue on Postgres in which Shopify-style shops share workers. It delivers at least once, using leases and fenced completions, and keeps shops fair with weighted deficit round robin, rate limits and admission control. A local mock stands in for Shopify's webhooks and Admin API, and a small React console shows each shop's state.

**Status:** phase 4 (tenants). Jobs can be enqueued singly or in all-or-nothing batches and read through the API, with an optional dedupe key that makes a repeated enqueue return the existing job. A shop at its backlog limit gets 429 with `Retry-After` and nothing is created. The dispatcher claims jobs for workers that long-poll it, choosing shops with weighted deficit round robin (or FIFO, by one setting), and holds each shop to its concurrency cap and token bucket. Workers run handlers under leases renewed by batched heartbeats, and write fenced results. The reaper requeues expired leases, and the pruner removes finished rows past their retention. A seeded chaos harness kills and pauses workers, restarts Postgres and breaks its connections, then checks that no job was lost. Throttle-aware retries, webhooks, the console and the Black Friday experiment arrive in later phases. See [`docs/phases.md`](docs/phases.md).

Delivery is at-least-once: a job can run more than once, so handlers must be idempotent. The idempotency helper (`internal/idem`, `run.Step`, `run.Tx()`) makes repeated runs harmless; it doesn't stop them.

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
| `make chaos` | 100 seeded chaos runs, then the crash-recovery scenario; results in `bench/results/chaos/` |
| `make chaos-one` | Replay one chaos run: `CHAOS_SEED=<n> make chaos-one` |
| `make chaos-smoke` | The 3-run chaos smoke that CI runs; results in `.cache/chaos-results/` |
| `make ci` | Everything CI runs, except the chaos smoke |

The first `make` copies `.env.example` to `.env`. Both hold local dummy values only.

### Chaos runs

The chaos targets run on a separate Compose project, `jobqueue-chaos`, with [`docker-compose.chaos.yml`](docker-compose.chaos.yml) on top of the normal file (design §18, D-042). That stack has its own Postgres volume and publishes no ports, so it never touches the `make up` stack or its data, and the targets remove it, volume included, when they end. Its workers and dispatcher reach Postgres through Toxiproxy.

The harness runs in the `chaos` container. That is the only service that mounts the Docker socket, and only under the `chaos` profile (D-025). It acts only on the `worker-1`, `worker-2` and `postgres` containers of the chaos project. Each `make chaos` starts from a fresh random seed, which it prints and records; `CHAOS_BASE_SEED=<n> make chaos` repeats a batch (D-046). A full `make chaos` takes a while and keeps Docker busy.

## Services

`make up` starts Postgres 16, Toxiproxy, Prometheus, a one-shot `migrate`, the three `queued` roles (`api`, `dispatcher`, and two `worker` processes) and `mockshop`. The `queued` roles read the shops and job kinds from [`config/shops.json`](config/shops.json); `api` seeds the database from it at startup. Settings come from the environment and are listed in [`cmd/queued/main.go`](cmd/queued/main.go); for example, `QUEUED_SCHEDULER=fifo make up` starts the dispatcher with FIFO instead of DRR. Published ports listen on `127.0.0.1` only:

- `api`: <http://127.0.0.1:8080/healthz>, plus `POST /v1/jobs` (with an optional `dedupe_key`), `POST /v1/jobs/batch` and `GET /v1/jobs/{id}` (design §11.1)
- `mockshop`: <http://127.0.0.1:8090/healthz>
- Prometheus: <http://127.0.0.1:9090>

## Layout

```
cmd/         queued (api, dispatcher, worker, migrate, healthcheck), mockshop, loadgen
internal/    api, dispatcher, queue (handler API and worker), idem (idempotency helper), lease,
             store, config, clock, testkinds, httpserver, migrate; pgtest and queuetest (test
             helpers); scheduler (DRR and FIFO), admission (the backlog limit); webhook (a later phase)
migrations/  embedded SQL, applied in order by `queued migrate`
console/     React + TypeScript (Vite)
tests/       compose checks, invariants (Q1 basic, Q2, Q3, Q4, Q5, Q8, idempotency helper,
             backlog counter), adversarial (skewed worker clocks, replay storms, two
             dispatchers), chaos (the harness, its config and Toxiproxy links), testdata
scripts/     stack check and the chaos runner
config/      shops and job kinds (shops.json), Prometheus config
tools/       the tools image
bench/       chaos results (bench/results/chaos/); experiment configs and results (phase 5)
docs/        requirements, architecture, design, decisions, phases
```

## Docs

- [`docs/requirements.md`](docs/requirements.md): what the queue must do, and how each requirement is checked
- [`docs/architecture.md`](docs/architecture.md): diagrams
- [`docs/design.md`](docs/design.md): how it works
- [`docs/decisions.md`](docs/decisions.md): every decision and why
- [`docs/phases.md`](docs/phases.md): the build plan and acceptance criteria
