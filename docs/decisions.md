# Decisions

Every question settled for this project, with the answer and the reasoning behind it. Phase 0 entries settle the questions in PLAN.md §5 and the gaps found while reading it. Later entries record decisions made mid-phase and every change to `requirements.md`, `architecture.md` or `design.md`.

Each entry gives the question, the options considered, the decision and its consequences. Timeouts, intervals and limits in these entries are configuration defaults, not measurements.

| ID | Topic | Source | Status |
|---|---|---|---|
| D-001 | Dependency rule | User constraint | Decided |
| D-002 | Where deficit round robin runs | PLAN §5 Q1 | Decided |
| D-003 | Lease timeout, heartbeat, slow workers | PLAN §5 Q2 | Decided |
| D-004 | Throttle vs failure, retries, fencing token | PLAN §5 Q3 | Decided |
| D-005 | Numbers in docs | CLAUDE.md vs PLAN | Decided |
| D-006 | Companion libraries for PLAN tools | Follows D-001 | Decided |
| D-007 | What admission control sheds | PLAN §5 Q4 | Decided |
| D-008 | Where Postgres-as-a-queue stops scaling | PLAN §5 Q5 | Decided |
| D-009 | Black Friday experiment settings | PLAN §5 Q6 | Decided |
| D-010 | Mock Admin API rate limit | Gap: bucket vs workload | Decided |
| D-011 | Definition of queue wait | Gap: metric undefined | Decided |
| D-012 | Console pages and actions | PLAN §5 Q7 | Decided |
| D-013 | DRR cost unit | Gap: unit cost makes DRR plain WRR | Decided |
| D-014 | Process layout | Gap: chaos needs separate workers | Decided |
| D-015 | Job storage and retention | Gap: table design and pruning | Decided |
| D-016 | Partitions for the sharded queue (3+) | PLAN §5 Q8 | Decided |
| D-017 | Idempotency helper contract | Gap: helper undefined | Decided |
| D-018 | Dedupe key retention | Gap: Q4 window undefined | Decided |
| D-019 | Where shop configuration lives | Gap: config source undefined | Decided |
| D-020 | Webhook topics and job handlers | Gap: workload undefined | Decided |
| D-021 | Go toolchain | Gap: Go and golangci-lint not installed | Decided |
| D-022 | Continuous integration | Gap: private repo, limited minutes | Decided |
| D-023 | Shape of a chaos run | Gap: run length undefined | Decided |
| D-024 | Demo video and talk outline | Gap: who records | Decided |
| D-025 | How the chaos harness controls containers | Follows D-021 | Decided |
| D-026 | Q6 tolerance | Gap: "within tolerance" undefined | Decided |
| D-027 | Black Friday pass target | Gap: "nearly flat" undefined | Decided |
| D-028 | What "reproduces" means | Gap: undefined | Decided |
| D-029 | Extra non-functional targets | Gap: NFRs undefined | Decided |
| D-030 | Batch enqueue for the hot counter row | Found while writing the design | Decided |
| D-031 | When an expired job runs again | Found while writing the design | Decided |
| D-032 | DLQ retry and the backlog limit | Found while writing the design | Decided |
| D-033 | One-shot Compose services and healthchecks | Phase 1: P1-1 vs design §3 | Decided |
| D-034 | Where the compose check gets its config | Phase 1: no Docker CLI in the tools image | Decided |
| D-035 | Local Postgres credentials | Phase 1: no secrets in the repo | Decided |
| D-036 | Which database clock lease SQL reads | Phase 2: `now()` is frozen per transaction | Decided |
| D-037 | A claimed job with no registered handler | Phase 2: gap in design §8 | Decided |
| D-038 | Hard-timeout grace period | Phase 2: setting missing from design §2 | Decided |
| D-039 | What makes an enqueue request invalid | Phase 2: AC-1.3 "invalid payload" undefined | Decided |
| D-040 | When a dispatcher that lost the lock tries again | Phase 2: gap in design §9 | Decided |

---

## D-001 · Dependency rule

- **Date:** 2026-11-07 · **Phase:** 0
- **Question:** The project must use nothing third party beyond what it already plans and download nothing extra. How far does that go, given that the planned stack already needs downloads?
- **Options:**
  - A. PLAN stack only: exactly what PLAN.md lists, nothing added.
  - B. Bare minimum: drop rapid, Toxiproxy and Prometheus and write our own replacements.
  - C. Zero downloads: not workable. Go is not installed, and Go has no built-in Postgres driver.
- **Decision:** A. Allowed: the Go toolchain and standard library, the Postgres 16 image, pgx, rapid, Toxiproxy, Prometheus, React + Vite + TypeScript in `console/`, golangci-lint and govulncheck. Nothing outside that list is added in any phase.
- **Consequences:** Every design choice must fit inside that list. Other systems (Kafka, Redis, PgBouncer and similar) may appear in `design.md` only as text, for comparison or as "what we would move to". Any later need for something outside the list is a stop-and-ask.

## D-002 · Where deficit round robin runs

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** PLAN §5 Q1
- **Question:** Does a central dispatcher run deficit round robin (DRR), or does each worker schedule itself? Where does the round-robin state live?
- **Options:**
  - A. One central dispatcher. It keeps DRR state in memory and claims on behalf of workers.
  - B. Each worker schedules itself, with DRR counters in a `shops` table updated in every claim transaction.
  - C. Leaderless stride scheduling, with a virtual time per shop.
- **Decision:** A.
  - The dispatcher runs inside `queued`, and leadership is a Postgres session advisory lock.
  - Workers ask the dispatcher for work and wait for an answer. The dispatcher picks a shop with DRR and claims one of its jobs for that worker with `FOR UPDATE SKIP LOCKED`, recording the worker as lease owner.
  - The DRR state (the list of active shops and their deficit counters) lives only in the dispatcher's memory and resets to zero when leadership moves.
- **Safety rule:** The claim SQL enforces each shop's concurrency cap against the database, not against dispatcher memory. A second dispatcher during a split brain can skew fairness, but it cannot break Q3 (one live lease) or Q5 (concurrency cap).
- **Why not B:** every claim writes the same shared rows, so claims serialize. `SKIP LOCKED` makes fairness approximate, which makes Q6 hard to prove.
- **Why not C:** it is weighted fair queuing, not DRR, and fairness drifts when claimers compete.
- **Consequences:**
  - DRR is a pure function, and Q6 is a property test on that function.
  - Claims pause while leadership moves. No job is lost, because only the deficits live solely in memory.
  - Every claim goes through the dispatcher, which adds a hop and is a throughput ceiling.
  - Follow-up: dispatcher failover that keeps claiming through a leader change.

## D-003 · Lease timeout, heartbeat and slow workers

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** PLAN §5 Q2
- **Question:** What lease timeout and heartbeat interval? How does the reaper treat a slow but alive worker?
- **Options:**
  - A. 30 s lease, 10 s heartbeat.
  - B. 10 s lease, 3 s heartbeat.
- **Decision:** A.
  - Defaults: lease 30 s, heartbeat every 10 s, reaper scan every 5 s. A job kind can override them.
  - The lease is the only sign of life, so the reaper never judges a worker "slow". A slow worker that keeps heartbeating keeps its job.
  - Each job kind has a hard timeout (default 5 min). When it fires, the handler's context is cancelled and the run counts as a failed attempt. This stops a stuck handler that would otherwise heartbeat forever.
  - If a heartbeat updates 0 rows, the worker has lost its lease: it cancels the handler and discards the result.
  - Each worker process sends one batched heartbeat that covers all the leases it holds.
  - Lease times come from the database clock (`now()`). Tests shorten every interval through config.
- **Why not B:** a Postgres restart or an injected delay longer than 10 s would expire healthy leases and cause more duplicate runs.
- **Consequences:**
  - A crashed worker's job waits up to one lease plus one reaper scan before it can run again.
  - An expired lease counts as a failed attempt (PLAN §3: too many attempts → dead-letter). A job that crashes its worker on every run therefore still ends dead, which Q1 needs.

## D-004 · Throttle vs failure, retries and the fencing token

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** PLAN §5 Q3
- **Question:** How is a throttle told apart from a failure, and how is each retried?
- **Options:**
  - A. A throttle reschedules without using an attempt and pauses the whole shop. A per-shop token bucket prevents throttles up front.
  - B. A throttle reschedules only that job, with no shop pause and no token bucket.
  - C. Prevention only: a token bucket keeps calls under the limit, and a 429 is an ordinary failure.
- **Decision:** A. A handler returns one of four results:
  - **Success:** the job is completed.
  - **`Throttled(retryAfter)`** (the mock Admin API returned 429 with `Retry-After`): the job runs again at `now() + Retry-After + jitter`, and no attempt is used. The dispatcher claims nothing more for that shop until the Retry-After time has passed.
  - **`Permanent(err)`:** the job goes straight to dead.
  - **Anything else** (an error, a panic or a hard timeout): a temporary failure. The job retries with exponential backoff and full jitter (2 s base, 5 min cap), and goes dead after 10 attempts.
  - Each job can be throttle-rescheduled at most 50 times. Past that, the job goes dead with reason "throttle budget exhausted", so it can never loop forever.
  - The dispatcher also keeps a per-shop token bucket that holds claims under the shop's API rate. That is the "rate limits" in PLAN's scope.
- **Why not B:** during a throttle, every job of that shop costs a claim and an API call before it is rescheduled.
- **Why not C:** it breaks Q7.
- **Fencing token (changes PLAN §3):** Q7 means the attempt number cannot also be the fencing token, so the job keeps two counters:
  - `claim_seq` rises on every claim and is the fencing token.
  - `attempts` rises only on a failure and is the retry budget.
  - The fencing rule becomes: *a worker may complete, fail or reschedule a job only if the job is still running with the same `claim_seq` and lease owner, and its lease has not expired by the database clock. Zero rows updated means the worker lost its lease, and its result is discarded.*
  - The expiry check is required by Q2. A worker whose lease has expired must lose, even if the reaper has not yet requeued the job.

## D-005 · Numbers in docs

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** CLAUDE.md says "never put a number in README.md or docs/ unless a script in the repo produced it", while PLAN asks for non-functional requirements with numbers.
- **Options:**
  - A. The rule covers measured results only.
  - B. Config defaults are allowed, but targets are stated only in words.
  - C. No numbers in docs at all.
- **Decision:** A. The rule covers measured results: throughput, latency, lost jobs, recovery time.
  - Docs may state configuration defaults and targets, as long as each is labelled as a setting or a target.
  - Every target names the script or test that measures it.
  - A measured value appears in `README.md` or `docs/` only when it is copied from a file that a repo script wrote under `bench/results/`, and the doc names that file.
  - Estimates and predictions never appear in docs.

## D-006 · Companion libraries for PLAN tools

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Follows D-001.
- **Question:** Prometheus and Toxiproxy have Go client libraries that PLAN does not list (`client_golang`, which pulls in protobuf and other modules, and the Toxiproxy Go client). Should we use them?
- **Options:**
  - A. Standard library.
  - B. The client libraries.
- **Decision:** A.
  - `queued` and `mockshop` write the Prometheus text format themselves (counters, gauges and histograms).
  - Tests drive Toxiproxy through its HTTP API with `net/http`.
  - The Go modules are pgx (with its own dependencies) and rapid, and nothing else.

## D-007 · What admission control sheds

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** PLAN §5 Q4
- **Question:** What does admission control shed first: the noisy shop's work, low-priority work, or new work?
- **Options:**
  - A. A per-shop backlog limit only.
  - B. A, plus job priorities, with low priority shed first.
  - C. A, plus a global limit that rejects only shops over their weighted share of the backlog.
- **Decision:** A. Each shop has a backlog limit.
  - The backlog is the shop's unfinished jobs: those `queued` (runnable or scheduled, including retry backoff and throttle reschedules) and those `running`.
  - The enqueue transaction checks the limit exactly.
  - At the limit, enqueue returns 429 with `Retry-After` and creates nothing (Q8).
  - Only the shop that is over its limit sheds new work. Other shops are unaffected.
- **Consequences:**
  - There are no job priorities.
  - There is no global cap, so the most the queue can hold is the sum of the shop limits.
  - A webhook that gets a 429 is retried later by mockshop, as Shopify retries failed deliveries, and dedupe (Q4) still makes it exactly one job.
  - Follow-ups: job priorities (option B) and a fair-share global cap (option C).

## D-008 · Where Postgres-as-a-queue stops scaling

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** PLAN §5 Q5
- **Question:** Where does Postgres-as-a-queue stop scaling, and what would we move to?
- **Options:**
  - A. A written analysis, plus a benchmark that measures the limit on this machine.
  - B. The written analysis only.
- **Decision:** A.
  - `design.md` explains the pressure points: row churn and vacuum on the hot table, index bloat, `SKIP LOCKED` scanning past locked rows, connection count, WAL volume, and the write limit of a single primary.
  - It covers the fixes (pruning, partitioning, batching) and what to move to next. Systems outside D-001 appear there as text only.
  - Phase 6 adds `make bench-ceiling`, which raises load until claim p99 gets worse. `design.md` cites the ceiling only from that script's output (D-005).

## D-009 · Black Friday experiment settings

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** PLAN §5 Q6
- **Question:** What are the final Black Friday settings (shops, rates, spike, workers, handler cost) for this machine (Apple M4, 10 cores, 16 GB)?
- **Options:**
  - A. PLAN's settings with a per-shop backlog limit.
  - B. PLAN's settings with admission control off.
  - C. A scaled-down version.
- **Decision:** A. These are settings, not results:
  - **Shops:** 10 shops, weight 1 each. Each sends 20 jobs/s at a steady rate. `loadgen` generates the load open-loop: it does not slow down when jobs back up.
  - **Workers:** 16.
  - **Handler:** a 30 ms sleep, which simulates I/O rather than CPU so the worker count doesn't depend on core count, plus one mock Admin API call.
  - **Timeline:** 2 min of warm-up at the steady rate. Then shop 1 sends 2,000 jobs/s (100×) for 5 min. Then every shop is back at the steady rate until every accepted job is completed or dead.
  - **Admission:** both runs use the same per-shop backlog limit of 50,000. `loadgen` counts each 429 as shed and does not retry it.
  - **Two runs, identical except the scheduler:** FIFO (global order by runnable time) and DRR.
  - **Limits that must not bind:** per-shop concurrency caps, queue-side rate limits and mock Admin API buckets are all set high enough that they never bind in either run. If they did, they would hide the scheduler's effect.
  - **Outputs:** each shop's p99 queue wait (D-011) over time in 10 s buckets for both runs on one chart, plus throughput and shed counts. All go to `bench/results/`.

## D-010 · Mock Admin API rate limit

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. A Shopify-sized bucket would throttle normal shops at the D-009 rates and muddy the experiment.
- **Options:**
  - A. A configurable request-count leaky bucket.
  - B. A cost-based, GraphQL-style bucket.
  - C. A realistic small bucket in the main experiment.
- **Decision:** A.
  - `mockshop` keeps one request-count leaky bucket per shop, REST style. A call that finds the bucket full gets 429 with `Retry-After`.
  - Capacity and leak rate are set per shop.
  - The Black Friday config sets them so they never throttle.
  - Throttling is exercised by the Q7 integration test with a tiny bucket. An optional throttle-storm bench scenario is planned for Phase 5; if it isn't built, it goes under Follow-ups.
- **Why not B:** more mock code, and it does not match PLAN's 429 + `Retry-After`.
- **Why not C:** it mixes throttle handling with scheduling, so a result can't be attributed to either.

## D-011 · Definition of queue wait

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. PLAN publishes p50/p99 queue wait without defining it.
- **Options:**
  - A. The wait for the first claim.
  - B. The wait for every claim.
  - C. End-to-end time only.
- **Decision:** A.
  - Queue wait = the first claim time minus the moment the job first became runnable. That moment is the enqueue time, or `run_at` for a scheduled job. Both come from the database clock.
  - Only the first attempt counts.
  - End-to-end time (completed minus enqueued) is published as a second metric.
  - Retries and throttle reschedules are reported as their own counts.
- **Why not B:** it mixes backoff timing into the fairness numbers.
- **Why not C:** it includes handler time and retries, so it measures less of the scheduler.

## D-012 · Console pages and actions

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** PLAN §5 Q7
- **Options:**
  - A. A per-shop table, sparklines and the DLQ.
  - B. Minimal: the table and the DLQ only.
  - C. A, plus live editing of shop settings.
- **Decision:** A. One page, no auth, React only: no UI kit and no chart library (D-001). The page polls the read API every 2 s.
  - **Per-shop table:** backlog, running vs cap, p50/p99 queue wait over the last minute, throttled-until, and 429s shed.
  - **Sparklines:** a hand-drawn SVG sparkline of each shop's p99 queue wait over the last 10 min.
  - **Dead-letter queue:** a table filtered by shop, with retry and discard for each job and "retry all for this shop".
  - **Shop settings:** shown read-only.
- **Verification:** no JS test runner is in the stack (D-001). The console is checked with a TypeScript typecheck and a Vite build, and its behaviour is covered by Go tests of the read API it calls.
- **Follow-up:** live editing of shop settings (option C), which would need write endpoints with no auth.

## D-013 · DRR cost unit

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. If every job costs 1, deficit round robin behaves exactly like weighted round robin.
- **Options:**
  - A. A cost per job kind.
  - B. Unit cost.
  - C. Charge the measured runtime.
- **Decision:** A.
  - Each job kind declares an integer cost of at least 1 (default 1).
  - Each round, a backlogged shop's deficit grows by its weight times the base quantum.
  - The shop may claim while its deficit is at least the cost of its next job, and each claim subtracts that cost.
- **Changes Q6:** Q6 now reads *"Under saturation, each backlogged shop gets at least its weighted share of cost units, within tolerance."* With a single job kind, as in Black Friday, cost units equal claims. This PR updates the Q6 wording in `CLAUDE.md` and `PLAN.md` to match.
- **Why not B:** it is weighted round robin under another name.
- **Why not C:** the charge arrives only after the job finishes, so scheduling decisions use stale data and Q6 is harder to state exactly.

## D-014 · Process layout

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. Chaos tests kill -9 workers, so workers must run in processes separate from the dispatcher.
- **Options:**
  - A. One `queued` binary with roles.
  - B. All roles in every process.
  - C. Workers reach Postgres only through the API.
- **Decision:** A. `queued` runs in one of three roles:
  - **`api`:** enqueue, the webhook receiver, the read API, and serving the console build.
  - **`dispatcher`:** DRR, the reaper and the pruner. It holds the leader advisory lock (D-002).
  - **`worker`:** a pool of handler goroutines in one process. Workers long-poll the dispatcher over HTTP for claims, and write heartbeats and fenced results straight to Postgres.
  - `mockshop` and `loadgen` are separate binaries.
  - Compose runs 1 `api`, 1 `dispatcher` (`restart: always`) and 2 `worker` processes with 8 workers each, giving the 16 workers of D-009.
- **Why not B:** killing a "worker" would also kill an API and possibly the leader, so chaos tests would mix failure modes.
- **Why not C:** every heartbeat and result would go through an extra hop, and the API would become a second point of failure.

## D-015 · Job storage and retention

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. Chaos checks need every job ID's final state, and bench runs create many rows.
- **Options:**
  - A. One `jobs` table with pruning.
  - B. Split tables in the style of Solid Queue (`jobs` plus hot ready and claimed tables).
  - C. Delete jobs on completion and keep a slim outcome row.
- **Decision:** A.
  - All jobs live in one `jobs` table with a state column (River style).
  - Partial indexes cover the runnable jobs of each shop.
  - The pruner deletes completed jobs older than a retention window (default 24 h). Tests, chaos runs and bench runs set a longer window, so every ID can still be checked.
  - DLQ discard keeps the job dead with `discarded_at` set, and the pruner removes it after the retention window.
  - DLQ retry resets `attempts` and the throttle count and makes the job runnable again. `claim_seq` keeps rising, so fencing stays monotonic.
- **Why not B:** every state change moves rows between tables, and fencing would span two tables.
- **Why not C:** every completion becomes two writes, and the console has less history.

## D-016 · Partitions for the sharded queue (3+)

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** PLAN §5 Q8
- **Question:** How many partitions, where do partition leases live, and how does rebalancing work when a node joins or leaves?
- **Options:**
  - A. One Postgres with logical partitions.
  - B. Several Postgres shards plus a coordinator database.
  - C. Leave it for wave 2.
- **Decision:** A.
  - **Partitions:** there are 16 fixed partitions. A shop belongs to partition `hash(shop_id) mod 16`, using a stable standard-library hash.
  - **Leases:** partition leases (owner, epoch, expiry) live in a `partitions` table in the same Postgres, timed by the database clock.
  - **Dispatching:** each node runs a dispatcher for the partitions it owns. Every claim it makes checks that the node still owns that partition at the current epoch (epoch fencing).
  - **Rebalancing:** each node aims for ceil(16 / live nodes) partitions. A node over its target releases the extras, which bumps their epoch. A node under its target takes free or expired partitions.
- **Consequences:**
  - Fairness holds within each node's set of shops, not globally. `design.md` explains this.
  - This shows ownership, fencing and failover, but it does not scale storage. True per-shard databases appear in `design.md` as text only.
- **Why not B:** rebalancing would move data between databases, and fencing would span two of them.
- **Why not C:** Phase 0 would end with an open question and no 3+ layout diagram.

## D-017 · Idempotency helper contract

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. PLAN asks for a helper so that "a duplicate run of a handler has no extra side effects", without defining it.
- **Options:**
  - A. DB effects plus an external idempotency key.
  - B. DB effects only.
  - C. The external key only.
- **Decision:** A. The queue stays at-least-once. The helper guards side effects; it does not change delivery.
  - **Effects in Postgres:** `idem.Once(tx, key)` records the key in the same transaction as the effect.
    - If the handler commits the effect together with its fenced completion, a stale run fails fencing and rolls the effect back.
    - If a multi-step handler commits a step on its own, the recorded key stops a re-run from applying that step twice.
  - **Calls to the mock Admin API:** the helper derives a key from the job ID and the step name and sends it in an `Idempotency-Key` header. `mockshop` stores the first response for each (shop, key) and replays it for repeats. This is a simulation choice; the docs make no claim about which real Shopify endpoints support such a key.
  - `mockshop` counts the mutations it applied, so the published "duplicate executions vs side effects applied" number compares handler runs with applied effects.
- **Why not B:** "side effects applied" would cover only DB effects.
- **Why not C:** a handler that writes to Postgres would have no protection.

## D-018 · Dedupe key retention

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. Q4 says "a webhook delivered k times creates exactly one job" without saying for how long.
- **Options:**
  - A. A separate table with a 7-day window.
  - B. A unique column on `jobs`.
  - C. Keep keys forever.
- **Decision:** A.
  - A `dedupe_keys` table is unique on (shop, key) and written in the enqueue transaction. `X-Shopify-Webhook-Id` is the key for webhooks; other enqueues may pass their own key.
  - The dedupe check runs before the admission check. A duplicate returns the existing job's ID (200) and creates nothing, even at the backlog limit. Only an enqueue that would create a new job gets 429.
  - The pruner deletes keys older than 7 days. `mockshop`'s retry horizon must be shorter than that, and config validation enforces it, so Q4 holds for every redelivery `mockshop` makes.
- **Clarifies Q8:** "at the backlog limit, enqueue returns 429 and creates nothing" applies to enqueues that would create a job.
- **Why not B:** pruning completed jobs after 24 h would reopen dedupe, and a later redelivery would create a second job.
- **Why not C:** the table would grow without bound.

## D-019 · Where shop configuration lives

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. The claim SQL needs each shop's max concurrency in the database. YAML would need a third-party parser (D-001), so config files are JSON.
- **Options:**
  - A. A `shops` table seeded from a committed JSON file.
  - B. A, but unknown shops are created automatically with default limits.
  - C. Admin endpoints instead of a seed file.
- **Decision:** A.
  - The `shops` table holds each shop's weight, max concurrency, queue rate limit and backlog limit.
  - The `api` role upserts it at startup from a committed JSON file, `config/shops.json`. The same file also lists the job kinds with their costs, lease overrides and hard timeouts.
  - `loadgen` and `mockshop` read the same file.
  - An enqueue or webhook for an unknown shop is rejected with 404.
  - Bench and test runs supply their own JSON files.
- **Why not B:** any caller could create tenants, and a typo would become a new shop.
- **Why not C:** it adds write endpoints with no auth, and every test would have to call them first.

## D-020 · Webhook topics and job handlers

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. PLAN names no topics or handlers.
- **Options:**
  - A. One topic and one handler.
  - B. Two topics, the second with a higher DRR cost.
- **Decision:** A.
  - The `orders/create` webhook becomes a job of kind `order.tag`.
  - Its handler sleeps (30 ms in the Black Friday config), then adds a tag to the order through `mockshop`'s Admin API, using the idempotency key from D-017.
  - `loadgen` enqueues `order.tag` jobs through the enqueue API, so the experiment and the webhook path run the same handler.
  - Mixed DRR costs (D-013) are covered by the scheduler property test rather than by a second production handler.
- **Follow-up:** a second topic with a higher cost (option B).

## D-021 · Go toolchain

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. Go and golangci-lint are not installed on this Mac, and CLAUDE.md forbids installing anything.
- **Options:**
  - A. The user installs Go and golangci-lint.
  - B. Run the toolchain in Docker.
- **Decision:** B. Nothing new is installed on the Mac.
  - `make` targets run `go`, golangci-lint and govulncheck inside a local tools image built from the official `golang` image.
  - golangci-lint and govulncheck are installed inside that image with `go install` at pinned versions, which the Go checksum database verifies.
  - Module and build caches live in named Docker volumes.
  - Go tests run in a container on the Compose network, so they reach Postgres and Toxiproxy by service name.
  - The console's `npm` commands run on the host, inside `console/`. Node is already installed and is in PLAN's Install row.
- **Consequences:**
  - The first runs are slower while images build.
  - There is no local Go editor tooling.
  - CI runs the same Docker-based targets (D-022), so local runs and CI match.
  - Docker Desktop must be running for every `make` target.

## D-022 · Continuous integration

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. The GitHub repo is private, so GitHub Actions has a limited number of free minutes.
- **Options:**
  - A. GitHub Actions running a short suite.
  - B. Local CI only.
  - C. A, with the repo made public.
- **Decision:** A.
  - The workflow runs on an Ubuntu runner and uses only GitHub-owned actions, pinned by commit SHA.
  - It calls the same `make` targets as local runs (D-021): lint, `test -race` against Postgres, audit, the console typecheck and build, and a 3-run chaos smoke.
  - The full `make chaos` (D-023) and `make bench` (D-009) run locally, and their result files are committed under `bench/results/`.
  - The repo's visibility is the user's call and is unchanged.

## D-023 · Shape of a chaos run

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. Phase 3's acceptance is "100 chaos runs lose 0 jobs", but the length of a run is undefined.
- **Options:**
  - A. Short seeded runs.
  - B. Fewer, longer runs.
  - C. 100 long runs.
- **Decision:** A.
  - **Setup:** each run uses short leases (2 s lease, 0.5 s heartbeat) and enqueues 2,000 jobs across 5 shops.
  - **Faults:** a seeded random schedule of faults: kill -9 of a worker process, Postgres restarts, and Toxiproxy latency and connection resets.
  - **Check:** the harness waits until every job is completed or dead, then checks every job ID.
  - **Length:** the target is 30 s per run. This is a setting, not a measurement.
  - **Replay:** `CHAOS_SEED` replays any run exactly.
  - How the harness controls the containers is settled in D-025.

## D-024 · Demo video and talk outline

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. The definition of done includes a 3-minute demo video and a 20-minute talk outline.
- **Options:**
  - A. The user records from a script.
  - B. A GIF captured through browser automation.
- **Decision:** A.
  - In Phase 6, `docs/demo.md` gives the 3-minute script: the commands to run and what to show during the spike. The same phase adds the talk outline.
  - Phase 6 checks that every step in the script works.
  - The user records and narrates the video.

## D-025 · How the chaos harness controls containers

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Follows D-021. With no Go on the Mac, the chaos harness runs in a container, and it needs some way to kill workers and restart Postgres.
- **Options:**
  - A. Mount the Docker socket into the harness container only.
  - B. A shell script on the host injects the faults from a schedule the harness writes.
  - C. No real Postgres restart: workers become child processes of the harness, and Toxiproxy simulates outages.
- **Decision:** A.
  - Only the chaos harness container gets `/var/run/docker.sock`. It runs under its own Compose profile, `chaos`, and the queue services never get the socket.
  - The harness calls the Docker Engine API over the socket with `net/http`, with no Docker SDK (D-006). It sends SIGKILL to worker containers and restarts the Postgres container.
  - One seeded Go harness owns the whole fault schedule, so `CHAOS_SEED` replays a run exactly.
- **Trade-off:** while it runs, the harness container has full control of Docker Desktop. That is acceptable only because it is local, runs our own code, and exists only under the `chaos` profile. `design.md` records this under security.
- **Why not B:** the logic would be split between shell and Go, and runs would be harder to replay exactly.
- **Why not C:** Q1's "restart Postgres" would become "cut Postgres connections", so database crash recovery would never be exercised.

## D-026 · Q6 tolerance

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. Q6 says "within tolerance" without defining the tolerance.
- **Options:**
  - A. DRR's proven bound.
  - B. A percentage over a window of claims.
  - C. Both.
- **Decision:** A. The scheduler property test enforces this on every prefix of claims under saturation:
  - For every backlogged shop, cost units served ≥ (its weight ÷ total weight of backlogged shops) × total cost units served − (its quantum + the largest job cost).
  - The bound follows from the DRR algorithm itself, so it is exact rather than a tuned percentage.
- **Why not B:** the percentage would be arbitrary and could hide short-term unfairness.

## D-027 · Black Friday pass target

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. PLAN says the other shops' p99 wait stays "nearly flat" without defining it.
- **Options:**
  - A. Relative to warm-up, with slack.
  - B. An absolute ceiling.
  - C. A ratio against FIFO.
- **Decision:** A.
  - **Target:** in the DRR run, for shops 2–10, the p99 queue wait (D-011) in every 10 s bucket of the spike is at most 2 × that shop's warm-up p99 + 100 ms.
  - **Why the slack:** the 100 ms absorbs one DRR round when the warm-up p99 is very small.
  - **FIFO:** the FIFO run is reported, not targeted.
  - **Report:** the bench report prints PASS or FAIL for the target.
  - **If missed:** a missed target is a mid-phase surprise. Work stops and the user decides; the target is never quietly changed.
- **Why not B:** an absolute ceiling is not tied to how this machine performs at baseline.
- **Why not C:** if FIFO happened to look bad enough, a weak DRR result would still pass.

## D-028 · What "reproduces" means

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. PLAN's definition of done says `make bench` reproduces the chart and every README number. Arrival schedules are seeded, but timing still varies between runs.
- **Options:**
  - A. The targets pass, and the numbers fall within ±25%.
  - B. The targets pass, and the numbers are not checked.
- **Decision:** A.
  - A rerun of `make bench` must meet the same PASS targets.
  - A script compares each number in the README with the rerun's value. README numbers are always copied from the committed results file (D-005).
  - The script flags any number that differs from the rerun by more than 25%.
- **Why not B:** nobody would check that the published numbers still hold.

## D-029 · Extra non-functional targets

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Gap. PLAN asks for non-functional requirements with numbers. These come on top of Q1–Q8, D-026 and D-027.
- **Options:** crash recovery within one lease plus one reaper scan; one side effect per job; the baseline keeps up; enqueue p99 at most 50 ms.
- **Decision:** three targets, each measured by a repo script (D-005):
  - **Crash recovery:** a crashed worker's jobs are claimed again within one lease plus one reaper scan (35 s with the D-003 defaults). Measured from the kill to the last orphaned job's new claim, by a chaos scenario that uses the default lease.
  - **One effect per job:** `mockshop` applies each job's tag mutation once, even when the handler ran more than once. Measured from `mockshop`'s mutation log in chaos and bench runs. This is a property of the side effect, guarded by the idempotency helper (D-017). It is not a property of delivery, which stays at-least-once.
  - **Baseline keeps up:** during the Black Friday warm-up, no shop's backlog grows. Measured by `make bench`. `requirements.md` NFR-12 makes this testable: each shop's backlog stays at or below 2 s of its arrivals throughout warm-up.
- **Not adopted:** enqueue p99 at most 50 ms. It is still reported as a metric.

## D-030 · Batch enqueue for the hot counter row

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Found while writing the design. Exact admission (Q8) locks a shop's backlog counter row from the check until commit. During the spike, shop 1's enqueues all queue up on that one row.
- **Options:**
  - A. A batch endpoint, all or nothing.
  - B. A batch endpoint with per-item results.
  - C. Single enqueues only.
- **Decision:** A.
  - `POST /v1/jobs/batch` takes jobs for one shop and locks the counter row once per batch.
  - If the whole batch fits under the shop's backlog limit, every job is created. If it doesn't, the call returns 429 and creates nothing. Q8 holds per batch.
  - `loadgen` sends each tick's arrivals for a shop as one batch.
  - Webhooks stay single-job.
  - Duplicates inside a batch follow D-018: each job whose dedupe key already exists is returned with its existing ID and does not count toward the limit.
- **Why not B:** the response is more complex, and Q8 would have to be tested per item.
- **Why not C:** the spike might never reach its 100× rate.

## D-031 · When an expired job runs again

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Found while writing the design. D-003 counts an expired lease as a failed attempt but does not say when the job runs again.
- **Options:**
  - A. Runnable immediately.
  - B. After the normal retry backoff.
- **Decision:** A.
  - The reaper makes the job runnable at once and counts the attempt.
  - The crash-recovery target in D-029 (one lease plus one reaper scan) therefore needs no backoff term.
  - A job that crashes its worker on every run still goes dead after 10 attempts.
- **Why not B:** crash recovery would become one lease plus one reaper scan plus up to one backoff, so the D-029 target would have to change.

## D-032 · DLQ retry and the backlog limit

- **Date:** 2026-11-07 · **Phase:** 0 · **Source:** Found while writing the design. A retried dead job rejoins its shop's backlog.
- **Options:**
  - A. DLQ retry respects the backlog limit.
  - B. DLQ retry bypasses the limit.
- **Decision:** A.
  - A DLQ retry goes through admission. If the shop is at its backlog limit, the call returns 429 and the job stays dead.
  - "Retry all for this shop" retries as many jobs as fit, oldest first, and reports how many stayed dead.
  - The backlog limit is never exceeded.
- **Why not B:** the backlog limit would stop being a hard bound.

## D-033 · One-shot Compose services and healthchecks

- **Date:** 2026-11-07 · **Phase:** 1 · **Source:** Phase 1 asks for a healthcheck on every service and for every default-profile service to end up healthy (P1-1). Design §3 makes `migrate` a one-shot that runs and exits, and the `tools` runner that `make` uses for Go commands is not a long-running service either.
- **Options:**
  - A. Exempt the one-shots by name.
  - B. Keep `migrate` running after it migrates, serving `/healthz`.
  - C. Give the one-shots healthchecks as well, even though those checks mean nothing.
- **Decision:** A.
  - Every long-running service has a healthcheck and must become healthy.
  - `migrate` must exit 0. It doesn't restart, and the `queued` roles wait for it with `service_completed_successfully`.
  - `tools` runs only under its own `tools` profile, through `docker compose run`, and has no healthcheck.
  - The compose check names both one-shots explicitly, so the exemption can't hide a missing healthcheck. P1-1's stack check expects `running healthy` for every other default-profile service and `exited 0` for `migrate`.
- **Why not B:** it contradicts design §3 and leaves an idle process running.
- **Why not C:** the checks would meet the wording but prove nothing.

## D-034 · Where the compose check gets its config

- **Date:** 2026-11-07 · **Phase:** 1 · **Source:** The compose check (P1-5) is a Go test that reads `docker compose config --format json`, but Go tests run in the tools container, which has no Docker CLI (D-021).
- **Options:**
  - A. `make test` writes the config on the host, and the test reads the file.
  - B. Add the Docker CLI and the compose plugin to the tools image.
- **Decision:** A.
  - `make test` runs `docker compose --profile tools config --format json --no-path-resolution` on the host and writes `.cache/compose-config.json`, which is git-ignored.
  - The test parses it with `encoding/json`. It fails, rather than skips, if the file is missing.
  - `--no-path-resolution` keeps build contexts relative, so the test can find each Dockerfile in the repo and check its `FROM` lines.
- **Why not B:** an extra download outside D-001.

## D-035 · Local Postgres credentials

- **Date:** 2026-11-07 · **Phase:** 1 · **Source:** Postgres needs a password, and the repo may hold no secrets.
- **Options:**
  - A. A dummy `POSTGRES_PASSWORD` in `.env.example`, copied to `.env` by `make`.
  - B. Trust authentication, with no password.
  - C. A dummy password written straight into `docker-compose.yml`.
- **Decision:** A.
  - `.env.example` holds a dummy `POSTGRES_PASSWORD` next to the dummy webhook HMAC secret.
  - Compose requires the value with `${POSTGRES_PASSWORD:?...}`, so a missing `.env` fails loudly.
  - `make` copies `.env.example` to `.env` (git-ignored) when `.env` is missing, so `make up` works on a fresh clone and in CI.
  - Every Go process and test reads its connection settings from the standard libpq variables (`PGHOST`, `PGUSER`, `PGPASSWORD`, ...), which Compose sets.
- **Why not B:** trust authentication is a habit worth avoiding, even inside the Compose network.
- **Why not C:** it puts a credential-shaped value in a tracked file and ignores `.env.example`.

## D-036 · Which database clock lease SQL reads

- **Date:** 2026-11-08 · **Phase:** 2 · **Source:** Found while reading design §7 before coding. In Postgres, `now()` returns the start time of the current transaction. The fenced completion runs inside the handler's `run.Tx()` transaction, which can stay open for the whole handler run. `lease_expires_at > now()` would then compare with the moment the handler started, so a lease that expired mid-run would still pass, and Q2 would break.
- **Options:**
  - A. `statement_timestamp()` in every claim, heartbeat, fenced-write and reaper statement.
  - B. `clock_timestamp()` in the fenced write and the heartbeat only, keeping `now()` elsewhere.
- **Decision:** A.
  - `statement_timestamp()` is the database clock read when each statement starts. It is still the database's clock, so NFR-14 holds, and it is stable within a statement, so the reaper and claim queries can still use their indexes.
  - Every statement that sets or checks a lease, a fencing predicate, `run_at` for a retry, or a claim time uses it. Design §6.1, §6.3, §7, §8, §9 and §23 now say so (approved by the user).
  - Where `requirements.md`, `architecture.md` and `CLAUDE.md` write `now()`, they mean "the database clock", and are unchanged.
- **Why not B:** two clocks to reason about, and `clock_timestamp()` is volatile, so it can't drive an index scan.
- **Consequences:** a test holds `run.Tx()` open past the lease and checks that the completion affects 0 rows (P2-1).

## D-037 · A claimed job with no registered handler

- **Date:** 2026-11-08 · **Phase:** 2 · **Source:** Gap. Design §8 does not say what a worker does with a job whose kind has no handler, for example `order.tag` before phase 5, or a test kind when `QUEUED_TEST_HANDLERS` is off.
- **Options:**
  - A. A temporary failure.
  - B. A permanent failure.
- **Decision:** A. The job uses an attempt and backs off, with `last_error` "no handler registered for kind ...". After the kind's max attempts it goes dead with "attempts exhausted".
- **Why not B:** a job enqueued before the deploy that adds its handler would go dead at once, instead of running once the handler exists.

## D-038 · Hard-timeout grace period

- **Date:** 2026-11-08 · **Phase:** 2 · **Source:** Gap. Design §8 says the worker waits "a short grace period" after a hard timeout before it writes the failure and abandons a handler that ignores cancellation, but no setting names that period.
- **Options:**
  - A. The kind's heartbeat interval.
  - B. A new fixed setting (5 s), configurable through the environment.
- **Decision:** A. The grace period is the kind's `heartbeat_ms`.
  - Heartbeats continue during the grace period, so the lease is still live and the failure write passes fencing.
  - Short-lease test and chaos settings get a short grace period with no extra setting.
- **Why not B:** one more setting that every short-lease test would have to override.

## D-039 · What makes an enqueue request invalid

- **Date:** 2026-11-08 · **Phase:** 2 · **Source:** Gap. AC-1.3 returns 422 for "an invalid payload" without defining it, and enqueue dedupe arrives only in phase 3.
- **Options:**
  - A. Strict: `payload` must be a JSON object; malformed JSON, unknown fields and a bad `run_at` are 422; `dedupe_key` is 422 until phase 3.
  - B. Lenient: any JSON value as payload; unknown fields and `dedupe_key` ignored until phase 3.
- **Decision:** A. Design §11.1 lists the rules.
  - The whole request is validated before anything is written, so every 413, 404 and 422 creates nothing.
  - Phase 3 replaces the `dedupe_key` rejection with the dedupe path (D-018).
- **Why not B:** in phase 2, a caller's retry with a `dedupe_key` would silently create a second job.

## D-040 · When a dispatcher that lost the lock tries again

- **Date:** 2026-11-08 · **Phase:** 2 · **Source:** Found in the phase 2 self-review. Design §9 says a standby retries the leader lock every second, but not when a leader that lost its lock connection tries again. In the P2-8 test the old leader reconnected within milliseconds and won the lock straight back, so the standby never took over.
- **Options:**
  - A. After losing the lock, the old leader waits two standby retry intervals before it tries again.
  - B. No wait: the old leader reconnects at once and races the standby.
- **Decision:** A. Design §9 now says so (approved by the user).
  - A waiting standby retries within one interval, so it gets the lock before the old leader comes back.
  - The wait is a multiple of the standby retry setting, with no setting of its own.
- **Why not B:** the old leader nearly always wins the race, so a leader whose lock connection keeps dropping keeps the lock, and a healthy standby never takes over (P2-8).
- **Consequences:** with no standby running, a dispatcher that loses its lock connection leads again only after the wait, so claims pause for that long. Workers waiting on it get 503 and retry, as in any leader change (design §6.4). `TestStandbyTakesOverWhenTheLeaderConnectionCloses` covers the takeover.
