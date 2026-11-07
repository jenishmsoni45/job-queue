# Requirements

This doc says what the queue must do, who it is for, and how each requirement is checked. The reasoning behind each item is in [`decisions.md`](decisions.md), cited as D-xxx. The mechanisms are in [`design.md`](design.md), and the build order is in [`phases.md`](phases.md).

**About numbers (D-005):** a value marked *target* is a goal that a named test or script measures. A value marked *setting* is a configuration default. Measured results never appear here. They live in `bench/results/`.

**Delivery semantics:** the queue is at-least-once. A job may run more than once, and handlers are written to be idempotent. Duplicate runs are made harmless by fencing (stale results are discarded) and by the idempotency helper (side effects apply once).

## 1. Users

| User | Wants | Uses |
|---|---|---|
| App developer | To enqueue background work for a shop and write handlers that are safe to run twice | Enqueue API, handler interface, idempotency helper |
| Shopify, simulated by `mockshop` | To deliver webhooks and get a fast 2xx, or a 429 that means "retry later" | Webhook receiver |
| Ops engineer | To see each shop's backlog and wait, and to retry or discard dead jobs | Console, read API, Prometheus |
| Reviewer or interviewer | To rerun the evidence behind every claim in the README | `make test`, `make chaos`, `make bench` |

## 2. User stories and acceptance criteria

Each criterion is testable. `design.md` §16 (Test plan) names the test for each one, and `phases.md` says which phase builds it.

### US-1 · Enqueue a job
As an app developer, I enqueue a job for a shop. It has a kind and a JSON payload, and optionally a run time and a dedupe key.
- **AC-1.1** A valid enqueue for a known shop returns 201 with the job ID. Once that response is sent, the job survives a crash of any process, including Postgres.
- **AC-1.2** An enqueue for an unknown shop returns 404 and creates nothing (D-019).
- **AC-1.3** An unknown job kind, an invalid payload or an oversized body returns 422 (or 413 for size) and creates nothing.
- **AC-1.4** If the shop already has the dedupe key within the dedupe window, the enqueue returns 200 with the existing job's ID and creates nothing (D-018).

### US-2 · Enqueue a batch
As the load generator, I enqueue many jobs for one shop in one call (D-030).
- **AC-2.1** A batch either creates all its new jobs or creates none.
- **AC-2.2** A batch that would push the shop past its backlog limit returns 429 with `Retry-After` and creates nothing.
- **AC-2.3** Jobs in the batch whose dedupe keys already exist are returned with their existing IDs and don't count toward the limit.
- **AC-2.4** A batch larger than the maximum batch size (setting) returns 422 and creates nothing.

### US-3 · Delayed and scheduled jobs
As an app developer, I can ask for a job to run no earlier than a given time.
- **AC-3.1** A job with `run_at` in the future is never claimed before `run_at`, measured by the database clock.
- **AC-3.2** When a worker is free, a job is claimed within one dispatcher refresh interval (setting) of `run_at`, plus the claim transaction's own time.

### US-4 · At-least-once execution with leases
As an app developer, I want every accepted job to run until it completes or is declared dead, even when workers or the database crash.
- **AC-4.1** A claim gives the worker a lease timed by the database clock. While the handler runs, the worker's batched heartbeat extends the lease (D-003).
- **AC-4.2** Completing, failing or rescheduling a job succeeds only if the job is still running with the worker's `claim_seq` and lease owner, and the lease has not expired. Otherwise the write affects 0 rows and the result is discarded (D-004, Q2).
- **AC-4.3** If a heartbeat affects 0 rows, the worker cancels the handler's context within one heartbeat interval.
- **AC-4.4** The reaper makes jobs with expired leases runnable again at once and counts a failed attempt (D-031).
- **AC-4.5** If a handler runs past its kind's hard timeout (setting), its context is cancelled and the run counts as a failed attempt.

### US-5 · Retries and dead-letter
As an app developer, I want temporary failures retried with backoff, and hopeless jobs set aside.
- **AC-5.1** After a temporary failure, the next run is delayed by a random time between 0 and min(cap, base × 2^(attempts − 1)), with the settings from D-004.
- **AC-5.2** A job whose failed attempts reach the kind's maximum goes dead, with a reason and the last error.
- **AC-5.3** A handler result of `Permanent` sends the job straight to dead.

### US-6 · Throttle-aware retries
As an app developer calling the rate-limited Admin API, I want a throttle to delay a job without using up its retries (D-004).
- **AC-6.1** A `Throttled(retryAfter)` result reschedules the job no earlier than `now() + retryAfter`, and the job's failed-attempt count is unchanged (Q7).
- **AC-6.2** After a throttle commits, no claim for that shop that starts later commits before the Retry-After time has passed.
- **AC-6.3** A job throttled more often than the throttle budget (setting) goes dead with the reason "throttle budget exhausted".

### US-7 · Idempotent side effects
As an app developer, I want a duplicate run of my handler to cause no extra side effects (D-017).
- **AC-7.1** A database effect guarded by `idem.Once` is applied at most once per job and step, including when two runs of the same job overlap.
- **AC-7.2** A database effect committed together with the fenced completion is rolled back if fencing fails.
- **AC-7.3** For a given job and step, the helper produces the same `Idempotency-Key` on every run. `mockshop` applies the first request with a key and replays its response for repeats.

### US-8 · Receive Shopify webhooks
As the simulated Shopify, I deliver `orders/create` webhooks, sometimes more than once (D-020).
- **AC-8.1** A webhook with a missing or wrong `X-Shopify-Hmac-Sha256` returns 401 and creates nothing. The HMAC is checked over the raw body with a constant-time comparison.
- **AC-8.2** A webhook delivered k times, sequentially or concurrently, creates exactly one job (Q4).
- **AC-8.3** A valid, new webhook creates one `order.tag` job and returns 200.
- **AC-8.4** At the shop's backlog limit, a new webhook returns 429 with `Retry-After`. A duplicate of an existing job still returns 200 (D-018).

### US-9 · Fair share across shops
As an ops engineer, I want one shop's flood to leave the other shops' waits nearly unchanged.
- **AC-9.1** The dispatcher picks shops with weighted deficit round robin, charging each job its kind's cost (D-013).
- **AC-9.2** Under saturation, each backlogged shop's served cost units are never below its weighted share by more than its quantum plus the largest job cost (Q6, D-026).
- **AC-9.3** The scheduler can be switched to FIFO by config, and nothing else changes. The experiment depends on this.

### US-10 · Concurrency caps and rate limits
As an ops engineer, I want each shop held to its limits.
- **AC-10.1** A shop's running jobs never exceed its `max_concurrency`, even if two dispatchers run at once (Q5, D-002).
- **AC-10.2** Claims for a shop never exceed its token bucket: rate × elapsed time + burst (setting).

### US-11 · Admission control
As an ops engineer, I want a flooding shop to have its own new work rejected, not everyone's (D-007).
- **AC-11.1** If an enqueue would push a shop's unfinished jobs past its backlog limit, it returns 429 with `Retry-After` and creates nothing (Q8).
- **AC-11.2** Rejections for one shop never reject another shop's enqueues.
- **AC-11.3** Every job rejected with 429 is counted per shop and shown in the console.

### US-12 · Console: per-shop view
As an ops engineer, I see every shop on one page that refreshes every 2 s (setting) (D-012).
- **AC-12.1** For each shop the page shows the backlog, running vs cap, p50 and p99 queue wait over the last minute, throttled-until, and jobs shed by 429.
- **AC-12.2** For each shop the page shows a sparkline of p99 queue wait over the last 10 min, in 10 s buckets.
- **AC-12.3** Shop settings are shown read-only.

### US-13 · Console: dead-letter queue
As an ops engineer, I can inspect dead jobs, then retry or discard them.
- **AC-13.1** The DLQ table lists dead, undiscarded jobs, filtered by shop, with the reason and the last error.
- **AC-13.2** Retry makes a dead job runnable with its attempt and throttle counts reset. If the shop is at its backlog limit, retry returns 429 and the job stays dead (D-032).
- **AC-13.3** "Retry all for this shop" retries jobs oldest first while they fit, and reports how many stayed dead.
- **AC-13.4** Discard keeps the job dead, sets `discarded_at`, and hides it from the DLQ table.

### US-14 · Metrics
As an ops engineer, I can scrape the queue's metrics with Prometheus.
- **AC-14.1** Every role of `queued`, and `mockshop`, serves `/metrics` in the Prometheus text format, written with the standard library (D-006).
- **AC-14.2** Prometheus in Compose scrapes every target successfully.

### US-15 · Evidence: chaos suite
As a reviewer, I can rerun the crash tests and see that no job is lost.
- **AC-15.1** `make chaos` runs 100 seeded runs (D-023) and reports every accepted job's final state.
- **AC-15.2** Any run can be replayed exactly with `CHAOS_SEED`.

### US-16 · Evidence: Black Friday experiment
As a reviewer, I can rerun the FIFO-vs-DRR experiment and get the chart and a PASS/FAIL report (D-009, D-027).
- **AC-16.1** `make bench` runs the FIFO and DRR runs with identical settings except the scheduler, and writes results, the chart and the report to `bench/results/`.
- **AC-16.2** The report shows PASS or FAIL for the Black Friday target, the baseline target and the one-effect-per-job target (§3).

### US-17 · Evidence: reproducibility and the Postgres ceiling
As a reviewer, I can check that the README's numbers still hold (D-008, D-028).
- **AC-17.1** Every number in the README is copied from a committed results file and tagged with its source.
- **AC-17.2** `make bench-check` reruns the bench, then fails if a target fails or if any README number differs from the rerun by more than 25%.
- **AC-17.3** `make bench-ceiling` raises load until claim p99 gets worse, and writes the measured ceiling to `bench/results/`.

### US-18 · Sharded queue (3+, wave 2)
As a platform engineer, I can run the queue on several nodes, and it fails over when one dies (D-016).
- **AC-18.1** Each of the 16 partitions has at most one owner at a given epoch, and a node claims only from partitions it owns at the current epoch.
- **AC-18.2** When a node dies, its partitions are taken over within one partition lease plus one rebalance scan (settings), and no job is lost.
- **AC-18.3** A node that resumes after losing its partitions can claim nothing from them.
- **AC-18.4** When a node joins or leaves, the partitions are rebalanced so that each live node owns at most ceil(16 / live nodes).

## 3. Non-functional requirements

| ID | Requirement | Value | Checked by |
|---|---|---|---|
| NFR-1 | **Q1** No accepted job is lost: each ends completed or dead. | Target: 0 lost across 100 chaos runs | `make chaos` |
| NFR-2 | **Q2** A worker whose lease expired cannot complete, fail or reschedule the job. | Target: every stale write affects 0 rows | `tests/invariants` |
| NFR-3 | **Q3** At most one live lease per job. | Target: 0 violations | `tests/invariants` (rapid, concurrent claimers) |
| NFR-4 | **Q4** A webhook delivered k times creates exactly one job. | Within the dedupe window (setting: 7 days) | `tests/invariants` |
| NFR-5 | **Q5** A shop's running jobs never exceed its `max_concurrency`. | Target: 0 samples over the cap | dispatcher property test, plus samples taken during chaos and bench runs |
| NFR-6 | **Q6** Under saturation, each backlogged shop gets at least its weighted share of cost units, within tolerance. | Tolerance: the shop's quantum + the largest job cost (D-026) | `internal/scheduler` property test (rapid) |
| NFR-7 | **Q7** A throttle reschedules without using up an attempt. | Target: `attempts` unchanged by every throttle | `tests/invariants` with a tiny bucket |
| NFR-8 | **Q8** At the backlog limit, enqueue returns 429 and creates nothing. | Target: 0 rows created by any rejected call | `tests/invariants` |
| NFR-9 | Fairness under a flood (D-027): in the DRR run, shops 2–10's p99 queue wait in every 10 s bucket of the spike stays within bound. | Target: ≤ 2 × that shop's warm-up p99 + 100 ms | `make bench` report |
| NFR-10 | Crash recovery (D-029): a crashed worker's jobs are claimed again quickly. | Target: ≤ one lease + one reaper scan (35 s with default settings) | chaos recovery scenario |
| NFR-11 | One effect per job (D-029): `mockshop` applies each job's tag mutation once, even when the handler ran more than once. | Target: every completed `order.tag` job has exactly one applied mutation, and no job has more than one | `make bench`, `make chaos` (from phase 5) |
| NFR-12 | Baseline keeps up (D-029): during the Black Friday warm-up, no shop's backlog grows. | Target: each shop's backlog stays ≤ 2 s of its arrivals (40 jobs at 20 jobs/s) throughout warm-up | `make bench` report |
| NFR-13 | Reproducibility (D-028): a rerun passes the same targets, and README numbers hold. | Target: every README number within ±25% of the rerun | `make bench-check` |
| NFR-14 | Leases and timestamps use the database clock (`now()`), never a worker's clock. | Rule | code review, plus `tests/adversarial` with skewed worker clocks |
| NFR-15 | Everything runs locally in Docker Compose, using only the PLAN stack (D-001, D-006, D-021). | Rule | Phase 1 checks on the Compose file and lockfiles |
| NFR-16 | Security: HMAC checked in constant time, published ports bound to 127.0.0.1, no real secrets in the repo, Docker socket only under the `chaos` profile (D-025). | Rule | `tests/adversarial`, a Compose config check |
| NFR-17 | Quality gates: golangci-lint is clean, Go tests run with `-race`, govulncheck and `npm audit` pass. | Rule | `make lint`, `make test`, `make audit`, CI |

Measured results that are published but have no target: throughput, per-shop p50 and p99 queue wait, end-to-end time, enqueue latency, duplicate executions, and the Postgres ceiling (`make bench-ceiling`).

## 4. Non-goals

- Exactly-once delivery or claims. The queue is at-least-once, and side effects rely on idempotent handlers.
- A custom storage engine, multi-region deployment, or any cloud or paid service.
- Console auth, multiple console pages, live editing of shop settings (D-012), and any UI library beyond React.
- A real Shopify app, or any call to real Shopify. `mockshop` simulates the webhooks and the Admin API.
- Job priorities and a global backlog cap (D-007).
- A second webhook topic or handler (D-020).
- A dispatcher that keeps claiming through a leader change; wave 1 relies on restart plus the advisory lock (D-002).
- Strict per-shop ordering. Jobs are claimed oldest-runnable-first within a shop, but retries and throttles reorder them.
- Workflows, job dependencies, cron schedules and cancellation of running jobs.
