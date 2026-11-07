# Phases

Each phase is one PR, started only when asked ("Start phase N") and run by the workflow in `CLAUDE.md`. A phase is done when every acceptance criterion below has code and a test that proves it, and when `make lint`, `make test` and `make audit` pass, along with any check the phase names.

- **Criterion IDs:** each criterion has an ID of the form `P<phase>-<n>`. It cites the requirement it serves: AC-x.y and NFR-n in [`requirements.md`](requirements.md), Q1–Q8 invariants, and D-xxx decisions.
- **Values:** every value is a setting or a target (D-005). If a target is missed, work stops and the user decides (D-027).

| Phase | Dates | Theme | Done when |
|---|---|---|---|
| 0 | before Nov 30 | Plan | No open questions; the docs are consistent (this PR) |
| 1 | Nov 30 | Scaffold | `make up` healthy; CI green |
| 2 | Dec 1–6 | Core queue | Q2 and Q3 pass; a basic Q1 run passes |
| 3 | Dec 7–13 | Crash-proof | 100 chaos runs lose 0 jobs; Q4 passes |
| 4 | Dec 14–20 | Tenants | Q5, Q6 and Q8 pass |
| 5 | Dec 21–27 | Shopify workload and Black Friday | Q7 passes; the chart exists and reproduces |
| 6 | Dec 28–Jan 3 | Console and ship | Definition of done met; `make bench` reproduces the chart |
| 7 | Mar 29–Apr 18 | Sharded queue (3+) | Failover measured; 0 lost jobs; duplicates explained |

---

## Phase 1 · Scaffold (Nov 30)

**Scope**
- The repo layout from PLAN §7, plus `config/` and `tools/`.
- **Go module and tools image:**
  - `go.mod` names the module `github.com/jenishmsoni45/job-queue` and pins the current stable Go release. The only Go dependency is pgx.
  - `tools/Dockerfile` builds from the official `golang` image, pinned by version and digest. It installs golangci-lint and govulncheck with `go install` at pinned versions (D-021).
  - Module and build caches live in named volumes.
- **Makefile:** `up`, `down`, `test`, `lint`, `audit`, `console` and `ci`. Every Go command runs in the tools image; `npm` runs on the host inside `console/`.
  - `make lint` runs golangci-lint (with a committed `.golangci.yml`), `gofmt`, and `tsc --noEmit` for the console.
  - `make audit` runs govulncheck and `npm audit --audit-level=moderate`.
- **Compose:**
  - `docker-compose.yml` defines `postgres` (official `postgres:16.x`), `toxiproxy` and `prometheus` (each from its project's own registry), plus `migrate`, `api`, `dispatcher`, `worker-1`, `worker-2` and `mockshop`.
  - Every image is pinned to an exact version and digest. Every long-running service has a healthcheck; the one-shots `migrate` and `tools` don't need one (D-033).
  - Every published port is bound to `127.0.0.1`.
  - Go services run as static binaries; `queued healthcheck` serves the healthcheck, so no shell is needed in the image.
- **Stub binaries:** `queued` with its three roles, and `mockshop`. Each serves `/healthz` and nothing else yet.
- **Migrations:** the runner described in design §3, and a first migration that creates `schema_migrations` only.
- **Config:** `.env.example` with a dummy webhook HMAC secret (`.env` is already git-ignored).
- **Console:** `console/` is Vite + React + TypeScript with a one-page placeholder. It commits `package-lock.json` and an `.npmrc` with `ignore-scripts=true`.
- **CI:** `.github/workflows/ci.yml` uses only GitHub-owned actions, pinned by commit SHA. It runs `make lint test audit` and the console typecheck and build (D-022).
- **Compose check:** a Go test that reads `docker compose config --format json` (standard-library JSON) and checks the image pins, the port bindings and Docker socket mounts.

**Acceptance criteria**
- **P1-1** `make up` brings every default-profile service to healthy in `docker compose ps`; the one-shot `migrate` instead exits 0 (D-033). `make down` leaves no project container running.
- **P1-2** `make lint`, `make test` (with `-race`) and `make audit` pass. Every Go step runs inside the tools image, and nothing is installed on the Mac (D-021).
- **P1-3** CI runs on the PR and is green.
- **P1-4** Running the migration runner twice leaves the schema unchanged. Two runners started at once apply each migration exactly once.
- **P1-5** The compose check passes (NFR-15, NFR-16):
  - every image is pinned to an exact version and digest
  - every published port is bound to `127.0.0.1`
  - no service mounts the Docker socket
  - Postgres doesn't turn off `fsync` or `synchronous_commit` (Q1 relies on them)
- **P1-6** The console typechecks and builds. `package-lock.json` is committed, and `.npmrc` sets `ignore-scripts=true`.
- **P1-7** Each `queued` role and `mockshop` answers `GET /healthz` with 200.

**Tests to add:** migration runner tests (P1-4), the compose check (P1-5), health handler tests (P1-7).

**Out of scope:** all queue logic, the schema beyond `schema_migrations`, and the `chaos` and `bench` profiles.

---

## Phase 2 · Core queue (Dec 1–6)

**Scope**
- **Schema:** `shops`, `shop_state`, `job_kinds` and `jobs`, with the indexes from design §3. Config is loaded from `config/shops.json`, validated, and seeded by `api` (D-019).
- **`api`:** `POST /v1/jobs` (single job) and `GET /v1/jobs/{id}`. The backlog counter is maintained on every transition (design §3).
- **`dispatcher`:**
  - The leader advisory lock and standby mode.
  - The FIFO scheduler and the claim transaction (design §6.3) without the cap step.
  - `POST /internal/v1/claim`, a long-poll.
  - The reaper (D-031).
- **`worker`:**
  - The handler pool, the handler registry and `run.Tx()`.
  - Batched heartbeats and the fenced results from design §8, including backoff with full jitter, the hard timeout, panic recovery and ambiguous-commit handling.
  - Test handlers behind `QUEUED_TEST_HANDLERS=1`.
- Scheduled and delayed jobs through `run_at`.

**Acceptance criteria**
- **P2-1** **Q2:** a worker held past its lease cannot complete, fail or reschedule. Its write affects 0 rows both after the reaper requeued the job and before the reaper has run (NFR-2, AC-4.2).
- **P2-2** **Q3:** a rapid property test with many concurrent claimers on the real claim SQL finds no job with two live leases (NFR-3).
- **P2-3** **Basic Q1:** an in-process test enqueues jobs and kills worker goroutines and their connections mid-run at random. Every job ends completed or dead.
- **P2-4** AC-1.1, AC-1.2 and AC-1.3: 201 with a durable job, 404 for an unknown shop, and 422 or 413 for invalid input, each creating nothing.
- **P2-5** AC-3.1 and AC-3.2: a scheduled job is not claimed before `run_at`, and it is claimable within one refresh interval after `run_at`.
- **P2-6** AC-4.1, AC-4.3, AC-4.4 and AC-4.5:
  - heartbeats extend the lease
  - a heartbeat that affects 0 rows cancels the handler within one heartbeat interval
  - an expired lease requeues the job at once and counts an attempt
  - a hard timeout counts an attempt
- **P2-7** AC-5.1, AC-5.2 and AC-5.3: backoff delays fall within the full-jitter bounds; a job goes dead at max attempts with a reason and the last error; `Permanent` sends a job straight to dead.
- **P2-8** A second dispatcher answers 503 while the leader holds the lock. It takes over after the leader's lock connection is closed.
- **P2-9** A property test: after random enqueues, claims, results and expiries, `shop_state.backlog` equals the real count of queued and running jobs.
- **P2-10** The worker never uses its own clock for lease decisions. An adversarial test skews the worker's clock and sees no change in lease outcomes (NFR-14).

**Tests to add:** `tests/invariants` (Q2, Q3, basic Q1, backlog counter), unit tests for backoff, config validation and fencing, and `tests/adversarial` (skewed clock).

**Out of scope:** dedupe, the idempotency helper, DRR, caps, token buckets, admission, throttles, webhooks, `mockshop`, metrics and the pruner.

---

## Phase 3 · Crash-proof (Dec 7–13)

**Scope**
- **Chaos harness** (design §18, D-023, D-025):
  - A `chaos` Compose profile and an override that routes workers and the dispatcher to Postgres through Toxiproxy.
  - A Docker Engine API client over the socket and a Toxiproxy client, both with `net/http`.
  - Seeded fault schedules, the checks, and the results under `bench/results/chaos/`.
- **Make targets:** `make chaos` (100 runs), `make chaos-one` (`CHAOS_SEED`), and a 3-run smoke step in CI.
- **Idempotency helper:** `idem.Once`, `run.Step` and `idem.Key` (D-017), with the `idempotency_keys` table.
- **Enqueue dedupe:** the `dedupe_keys` table, with 200 for a duplicate and handling for concurrent conflicts (D-018).
- **Pruner:** removes completed jobs, discarded jobs and dedupe keys past their retention.
- **Crash-recovery scenario:** a chaos scenario for NFR-10.

**Acceptance criteria**
- **P3-1** **Q1:** `make chaos` completes 100 seeded runs. Every accepted job ends completed or dead, so 0 are lost (NFR-1). The per-run results are written to `bench/results/chaos/`.
- **P3-2** The same `CHAOS_SEED` always produces the same fault schedule, and `make chaos-one` replays it.
- **P3-3** Each run also checks that leases never overlap (Q3), that the backlog counter matches a real count, and that each completed job's `run.Tx()` effect was applied once.
- **P3-4** **Q4 (enqueue path):** k concurrent and k sequential enqueues with one dedupe key create exactly one job, including after that job completes (AC-1.4, NFR-4).
- **P3-5** AC-7.1, AC-7.2 and AC-7.3:
  - an effect guarded by `Once` applies once when runs overlap
  - effects in `run.Tx()` roll back when fencing fails
  - `idem.Key` is stable across runs, tested against an HTTP test server that honours `Idempotency-Key`
- **P3-6** **NFR-10:** the crash-recovery scenario, using the default lease, measures the time from the kill to the last orphaned job's new claim. It meets the target, and the result is written under `bench/results/chaos/`.
- **P3-7** The pruner deletes only rows past their retention window. Rows younger than the window, and every job that isn't completed, are untouched.
- **P3-8** The compose check passes: only the chaos harness mounts the Docker socket, and only under the `chaos` profile (D-025).
- **P3-9** CI runs the 3-run chaos smoke step, and it is green.

**Tests to add:** `tests/chaos` (the harness and its scenarios), `tests/invariants` (Q4 through the enqueue path), helper tests, pruner tests, and `tests/adversarial` (replay storms on one dedupe key).

**Out of scope:** `mockshop` and the webhook receiver (phase 5). Until then, external effects are checked against an HTTP test server.

---

## Phase 4 · Tenants (Dec 14–20)

**Scope**
- **DRR:** `internal/scheduler`, standard deficit round robin with costs per kind (D-013, design §6.2), as a pure function. A config switch chooses DRR or FIFO.
- **Caps:** the claim transaction's cap step (a per-shop advisory lock and an exact running count).
- **Dispatcher refresh:** per-shop token buckets, eligibility checks and the refresh query, which already reads `throttled_until`.
- **Admission control:** a backlog check under the shop row lock, 429 with `Retry-After`, and `shed_total` (D-007).
- **Batch enqueue:** `POST /v1/jobs/batch`, all or nothing (D-030).

**Acceptance criteria**
- **P4-1** **Q5:** a rapid property test on the dispatcher model never exceeds `max_concurrency`. In an integration test with two dispatchers claiming at once, sampled running counts never exceed the cap (NFR-5, AC-10.1).
- **P4-2** **Q6:** a rapid property test with random weights, costs and claim batch sizes shows that every backlogged shop's served cost units stay within `qᵢ + C` of its weighted share after every claim (NFR-6, D-026, AC-9.2).
- **P4-3** **Q8:** at the limit, single and batch enqueues return 429 with `Retry-After` and create no jobs and no keys. A duplicate still returns 200 (NFR-8, AC-11.1, AC-2.2).
- **P4-4** AC-2.1, AC-2.3 and AC-2.4: batches are all or nothing; existing keys are reported, not counted; an oversized or self-duplicating batch is rejected with 422.
- **P4-5** AC-9.1 and AC-9.3: DRR charges each job's cost. Switching to FIFO claims the globally oldest runnable jobs and changes nothing else.
- **P4-6** AC-10.2: claims per shop never exceed `rate × elapsed + burst`.
- **P4-7** AC-11.2 and AC-11.3: one shop's rejections never reject another shop's enqueues, and `shed_total` counts every rejected job.
- **P4-8** The P2-9 backlog-counter property test also covers batches and rejections.
- **P4-9** DRR unit tests: a shop that empties leaves the list with its deficit reset, and an ineligible shop is skipped without gaining quantum.

**Tests to add:** `internal/scheduler` property and unit tests, `internal/dispatcher` property tests, `tests/invariants` (Q5, Q8), and `tests/adversarial` (two dispatchers).

**Out of scope:** `Throttled` results, webhooks, the console and metrics.

---

## Phase 5 · Shopify workload and Black Friday (Dec 21–27)

**Scope**
- **`mockshop`** (design §12, D-010):
  - The Admin API with per-shop leaky buckets, 429 with `Retry-After`, `X-Shopify-Shop-Api-Call-Limit`, `Idempotency-Key` replay, and the mutation log.
  - The webhook sender, with HMAC signing, duplicates, concurrent duplicates and retries that honour `Retry-After`. Config validation ensures the retry horizon is shorter than the dedupe window.
- **Webhook receiver:** `POST /webhooks/shopify` (design §11.1).
- **Handler:** `order.tag` (D-020).
- **Throttle-aware retries:** the `Throttled` result, the shop pause through `throttled_until` and notify, and the throttle budget (D-004).
- **`loadgen`:** open-loop, with seeded Poisson arrivals sent as one batch per tick.
- **Bench runner:** `make bench` (design §17) runs from the committed config `bench/blackfriday.json`. It writes per-run exports, `summary.json`, `blackfriday.svg` (drawn with the Go standard library) and `report.md` with PASS/FAIL.
- **Chaos with `mockshop`:** chaos runs gain `mockshop` calls, for NFR-11.
- **Optional:** the throttle-storm scenario. If it isn't built, it goes under Follow-ups.

**Acceptance criteria**
- **P5-1** **Q7:** with a `mockshop` bucket of capacity 1, every job completes, `attempts` stays at 0 through every throttle, and some jobs have `throttles > 0` (NFR-7, AC-6.1).
- **P5-2** AC-6.2 and AC-6.3: once a throttle commits, no later claim for that shop commits before Retry-After has passed; a job past its throttle budget goes dead with the reason "throttle budget exhausted".
- **P5-3** AC-8.1: a missing HMAC, a wrong HMAC, a tampered body and a wrong secret each return 401 and create nothing. The comparison uses `hmac.Equal` on the raw body.
- **P5-4** **Q4 end to end:** a webhook delivered k times, sequentially and concurrently, creates exactly one job (AC-8.2, AC-8.3). At the limit, a new webhook gets 429 and a duplicate gets 200 (AC-8.4).
- **P5-5** Unit tests of `mockshop`'s leaky bucket show the level, the leak and the `Retry-After` math agree with design §12. A repeated `Idempotency-Key` applies nothing.
- **P5-6** `make bench` writes every output listed in design §17 for both runs. The two runs differ only in the scheduler, which the report checks by diffing their effective configs.
- **P5-7** The report shows PASS for NFR-9 (Black Friday fairness), NFR-11 (one effect per job) and NFR-12 (baseline keeps up). A FAIL is a stop-and-ask, never a change to the target.
- **P5-8** A second `make bench` run on this machine also passes every target, meeting the first half of D-028.
- **P5-9** Chaos runs with `mockshop` meet NFR-11.

**Tests to add:** `tests/invariants` (Q7, Q4 through webhooks), `tests/adversarial` (HMAC tampering, oversized and malformed bodies), `mockshop` unit tests, and `loadgen` determinism tests (the same seed produces the same arrival schedule).

**Out of scope:** README numbers and the ±25% check (phase 6), the console, and metrics.

---

## Phase 6 · Console and ship (Dec 28–Jan 3)

**Scope**
- **Read API:** `GET /v1/shops`, the wait series, the DLQ list, retry, discard and retry-all, all following D-032.
- **Console** (D-012): one page that polls every 2 s, with the per-shop table, hand-drawn SVG sparklines and the DLQ table.
- **Metrics:** hand-written Prometheus metrics (design §19) and the Prometheus scrape config.
- **Bench targets:**
  - `make bench-ceiling` (D-008).
  - `make bench-check` (D-028), which reruns the bench, checks the targets, and compares every tagged README number with the rerun, failing beyond ±25%.
- **README:**
  - Every number is copied from `bench/results/` and tagged with its source file (D-005).
  - It has a section on how the project was built under the plan and review, and how it was verified.
- **Docs:**
  - `docs/postmortem.md` covers one real bug found during the build.
  - `docs/demo.md` is the 3-minute demo script, and `docs/talk.md` is the 20-minute talk outline (D-024).
  - The prior-art claims in design §22 are checked against current docs.
  - Design §20 cites the measured ceiling.

**Acceptance criteria**
- **P6-1** AC-12.1 to AC-12.3: Go tests of the read API cover every field the console shows. The console typechecks and builds.
- **P6-2** AC-13.1 to AC-13.4, including D-032: retry at the limit returns 429 and leaves the job dead; retry-all reports the number retried and the number left dead; discard hides a job and keeps it dead.
- **P6-3** AC-14.1: a test parses each process's `/metrics` output against the text-format rules. AC-14.2: a script confirms that Prometheus reports every target as up.
- **P6-4** AC-17.3: `make bench-ceiling` writes its measured ceiling to `bench/results/`, and design §20 cites that file.
- **P6-5** AC-17.1 and AC-17.2 (NFR-13): every README number is tagged with its source file, and `make bench-check` passes.
- **P6-6** `docs/postmortem.md` describes a real bug from this build, with links to the commits that introduced and fixed it.
- **P6-7** Every step in `docs/demo.md` was run and works. `docs/talk.md` exists.
- **P6-8** The definition of done in PLAN §9 is ticked, item by item, in the PR, with evidence for each.

**Tests to add:** read API tests, DLQ tests (D-032), the metrics format test, the Prometheus targets script, and the README number-tag check.

**Out of scope:** live editing of shop settings, console auth, a second topic (D-012, D-020).

---

## Phase 7 · Sharded queue (3+, Mar 29–Apr 18)

**Scope**
- **Schema:** the `nodes` and `partitions` tables, and `jobs.partition_id` (`FNV-1a(shop_id) mod 16`), with a backfill migration.
- **Nodes:** partition leases with epochs, a dispatcher per node over the partitions it owns, and epoch-fenced claims (design §23, D-016).
- **Rebalancing:** each node targets `ceil(16 / live nodes)` partitions.
- **Chaos:** the harness gains node faults (kill, pause, add and remove nodes), and it measures failover time and duplicates.

**Acceptance criteria**
- **P7-1** AC-18.1: a rapid property test on the rebalancing function never gives two owners the same partition at the same epoch, and it converges to at most `target` partitions per node.
- **P7-2** AC-18.2: after a node is killed, its partitions are claimable by others within one partition lease plus one scan. The failover time is measured and written to `bench/results/`.
- **P7-3** AC-18.3: a paused node that resumes after losing its partitions claims 0 jobs from them.
- **P7-4** AC-18.4: adding and removing nodes rebalances ownership to at most `ceil(16 / live)` per node.
- **P7-5** Chaos runs with node faults still lose 0 jobs (Q1) and keep Q2–Q5.
- **P7-6** Each duplicate execution in the failover runs is counted and attributed to its cause (lease expiry or partition handover) in the results.

**Tests to add:** rebalancing property tests, `tests/chaos` node-fault scenarios, and `tests/adversarial` (split brain with a paused node).

**Out of scope:** a database per shard, global fairness across nodes, multi-region.
