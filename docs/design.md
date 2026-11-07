# Design

This doc explains how the queue meets [`requirements.md`](requirements.md). The diagrams are in [`architecture.md`](architecture.md), and the reasoning behind each choice is in [`decisions.md`](decisions.md), cited as D-xxx. SQL here is a sketch of intent. The migrations and queries written in each phase are the real thing, and their tests are the proof.

**Numbers (D-005):** every value here is a *setting* (a configuration default) or a *target*. Measured results live only in `bench/results/`.

---

## 1. Overview

- `queued` is one Go binary that runs as `api`, `dispatcher` or `worker` (D-014).
- Postgres 16 holds all durable state: jobs, shop counters, dedupe keys and idempotency keys.
- The dispatcher is the only process that claims jobs. It is elected with an advisory lock and picks shops with weighted deficit round robin (DRR), or FIFO for the experiment (D-002).
- Workers long-poll the dispatcher for jobs, run the handlers, heartbeat their leases, and write fenced results straight to Postgres.
- `mockshop` simulates Shopify: it sends signed, sometimes-duplicated webhooks and serves a rate-limited Admin API.
- `loadgen` drives the Black Friday experiment.
- The React console reads the `api` role's read API.

Delivery is at-least-once. A job can run more than once, so correctness rests on three things:
1. **Fencing:** the write from a stale run affects 0 rows.
2. **The idempotency helper:** a side effect applies once per job and step.
3. **Dedupe on enqueue:** a redelivered webhook maps to the existing job.

## 2. Settings

Every setting lives in config, either the JSON file (§11.4) or the environment. Tests and bench runs override them.

| Setting | Default | Source |
|---|---|---|
| Lease / heartbeat interval / reaper scan | 30 s / 10 s / 5 s | D-003 |
| Hard timeout per kind | 5 min | D-003 |
| Backoff | full jitter, base 2 s, cap 5 min | D-004 |
| Max failed attempts per kind | 10 | D-004 |
| Throttle budget per kind | 50 reschedules | D-004 |
| Throttle jitter | uniform, 0–10% of Retry-After | this doc |
| `Retry-After` on a backlog 429 | 5 s (per shop: `backlog_retry_after_ms`) | this doc |
| Max batch size | 1,000 jobs | D-030 |
| Max request body | 1 MiB | this doc |
| Dispatcher refresh interval | 250 ms | this doc |
| Claim long-poll timeout | 25 s | this doc |
| DRR base quantum | the largest job cost in config | D-013 |
| Completed-job retention / discarded-job retention | 24 h / 24 h after discard | D-015 |
| Dedupe window | 7 days | D-018 |
| `mockshop` webhook retry horizon | 1 h (must be shorter than the dedupe window) | D-018 |
| Console poll / stats window / sparkline | 2 s / 1 min / 10 min in 10 s buckets | D-012 |
| Chaos lease / heartbeat | 2 s / 0.5 s | D-023 |
| Partitions (3+) | 16 | D-016 |
| Partition lease / rebalance scan (3+) | 10 s / 2 s | this doc |

## 3. Data model

```sql
-- Static, seeded from config/shops.json at api startup (D-019).
CREATE TABLE shops (
  id                     text PRIMARY KEY,             -- shop domain, e.g. shop-01.mockshop.test
  weight                 int  NOT NULL CHECK (weight > 0),
  max_concurrency        int  NOT NULL CHECK (max_concurrency > 0),
  rate_per_sec           double precision NOT NULL CHECK (rate_per_sec > 0),
  rate_burst             int  NOT NULL CHECK (rate_burst > 0),
  backlog_limit          int  NOT NULL CHECK (backlog_limit > 0),
  backlog_retry_after_ms int  NOT NULL CHECK (backlog_retry_after_ms > 0)
);

-- Hot counters, one row per shop, kept apart so the static row stays cheap to read.
CREATE TABLE shop_state (
  shop_id         text   PRIMARY KEY REFERENCES shops(id),
  backlog         int    NOT NULL DEFAULT 0 CHECK (backlog >= 0),  -- unfinished jobs
  shed_total      bigint NOT NULL DEFAULT 0,                       -- jobs rejected with 429
  throttled_until timestamptz                                      -- set by Throttled results
);

CREATE TABLE job_kinds (
  kind          text PRIMARY KEY,
  cost          int NOT NULL CHECK (cost >= 1),                     -- DRR cost (D-013)
  lease_ms      int NOT NULL, heartbeat_ms int NOT NULL, timeout_ms int NOT NULL,
  max_attempts  int NOT NULL, max_throttles int NOT NULL
);

CREATE TABLE jobs (
  id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  shop_id           text  NOT NULL REFERENCES shops(id),
  kind              text  NOT NULL REFERENCES job_kinds(kind),
  cost              int   NOT NULL,            -- copied from job_kinds at enqueue
  payload           jsonb NOT NULL,
  state             text  NOT NULL CHECK (state IN ('queued','running','completed','dead')),
  run_at            timestamptz NOT NULL,      -- claimable when run_at <= now()
  enqueued_at       timestamptz NOT NULL DEFAULT now(),
  first_runnable_at timestamptz NOT NULL,      -- greatest(enqueue time, requested run_at)
  first_claimed_at  timestamptz,               -- queue wait = first_claimed_at - first_runnable_at (D-011)
  claim_seq         int NOT NULL DEFAULT 0,    -- fencing token, +1 on every claim (D-004)
  attempts          int NOT NULL DEFAULT 0,    -- failed attempts, the retry budget
  throttles         int NOT NULL DEFAULT 0,
  lease_owner       text,
  lease_expires_at  timestamptz,
  completed_at      timestamptz,
  dead_at           timestamptz,
  dead_reason       text,
  last_error        text,                      -- truncated to 4 KiB
  discarded_at      timestamptz,
  CHECK ((state = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))
);

CREATE INDEX jobs_queued_shop  ON jobs (shop_id, run_at, id) WHERE state = 'queued';    -- DRR claims
CREATE INDEX jobs_queued_fifo  ON jobs (run_at, id)          WHERE state = 'queued';    -- FIFO claims
CREATE INDEX jobs_running_shop ON jobs (shop_id)             WHERE state = 'running';   -- cap checks
CREATE INDEX jobs_running_exp  ON jobs (lease_expires_at)    WHERE state = 'running';   -- reaper
CREATE INDEX jobs_dlq          ON jobs (shop_id, dead_at)    WHERE state = 'dead' AND discarded_at IS NULL;
CREATE INDEX jobs_done_at      ON jobs (completed_at)        WHERE state = 'completed'; -- pruner
CREATE INDEX jobs_discarded_at ON jobs (discarded_at)        WHERE discarded_at IS NOT NULL;
CREATE INDEX jobs_first_claim  ON jobs (shop_id, first_claimed_at) WHERE first_claimed_at IS NOT NULL; -- wait stats

CREATE TABLE dedupe_keys (                     -- D-018
  shop_id    text   NOT NULL REFERENCES shops(id),
  key        text   NOT NULL,                  -- X-Shopify-Webhook-Id, or a caller's key
  job_id     bigint NOT NULL,                  -- no FK: the job may be pruned before its key
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (shop_id, key)
);
CREATE INDEX dedupe_keys_created ON dedupe_keys (created_at);

CREATE TABLE idempotency_keys (                -- D-017
  job_id     bigint NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  step       text   NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, step)
);
```

- **Job states:**
  - `queued` covers both runnable and scheduled jobs. A job is runnable once `run_at <= now()`.
  - `running` means a lease is held.
  - `completed` and `dead` are terminal. A discarded job stays `dead` with `discarded_at` set.
- **Backlog counter invariant:** `shop_state.backlog` always equals the shop's jobs in `queued` or `running`. Every transition into or out of that set updates it in the same transaction: enqueue (+n), completion or death (−1), and DLQ retry (+1). A property test checks the counter against a real count after random operations.
- **Migrations:** a small runner in Go applies the embedded SQL files in order, inside an advisory lock, and records each one in `schema_migrations` (D-001: no migration tool). A one-shot `migrate` Compose service runs it before the other services start.

## 4. Processes and roles

| Process | Role | Talks to |
|---|---|---|
| `queued api` | enqueue (single and batch), webhook receiver, read API, console static files, `/metrics` | Postgres |
| `queued dispatcher` | leader lock, scheduler (DRR or FIFO), token buckets, claims, reaper, pruner, `/metrics` | Postgres |
| `queued worker` | pool of handler goroutines (8 per process in Compose), batched heartbeats, fenced results, `/metrics` | dispatcher (claims), Postgres (heartbeats and results), `mockshop` |
| `mockshop` | webhook sender, Admin API with per-shop leaky buckets, `/metrics` | `api` |
| `loadgen` | open-loop, seeded load for the bench | `api` |
| chaos harness | fault schedule and checks (`chaos` profile only) | Docker Engine API, Toxiproxy, `api`, Postgres |

Compose runs `postgres`, `toxiproxy`, `prometheus`, `migrate`, `api`, `dispatcher` (`restart: always`), `worker-1`, `worker-2`, and `mockshop`. Under the `bench` profile it adds `loadgen`; under the `chaos` profile, the chaos harness. A `tools` image runs Go, golangci-lint and govulncheck (D-021). Every published port is bound to `127.0.0.1`.

**Worker identity:** a worker's `lease_owner` is `<host>/<pid>/<slot>/<boot nonce>`. The boot nonce is random at process start, so a restarted worker never matches its predecessor's leases.

## 5. Enqueue

### 5.1 Single job (and webhook)

```text
BEGIN
  if dedupe key given:
    SELECT job_id FROM dedupe_keys WHERE shop_id = $shop AND key = $key
    found → ROLLBACK, return 200 {id, duplicate: true}            -- dedupe runs before admission (D-018)
  SELECT backlog FROM shop_state WHERE shop_id = $shop FOR UPDATE  -- serializes this shop's enqueues
  if backlog + 1 > backlog_limit:
    UPDATE shop_state SET shed_total = shed_total + 1
    COMMIT, return 429 + Retry-After                               -- no job, no key (Q8)
  INSERT INTO jobs (..., state 'queued', run_at, first_runnable_at = greatest(now(), run_at)) RETURNING id
  if dedupe key given:
    INSERT INTO dedupe_keys ... ON CONFLICT DO NOTHING
    conflict → ROLLBACK, re-read the key, return 200 {id, duplicate: true}
  UPDATE shop_state SET backlog = backlog + 1
  SELECT pg_notify('jobs_enqueued', $shop)                         -- wakes the dispatcher at commit
COMMIT, return 201 {id}
```

- The conflict branch covers a concurrent duplicate. Two requests can both pass the first `SELECT`. The row lock then serializes them, and the second hits the primary key on `dedupe_keys`.
- The webhook receiver calls this same path after its HMAC check (§11.1).

### 5.2 Batch (D-030)

- A batch holds up to the max batch size for one shop. A key that appears twice in the same batch makes the batch invalid (422).
- The API reads the existing keys first. Those jobs are reported with their existing IDs.
- It then takes the `shop_state` lock once. If `backlog + new_count > backlog_limit`, the whole batch gets 429 and nothing is created.
- Otherwise every new job and key is inserted, `backlog += new_count`, and one notify is sent.

## 6. Scheduling and claims

### 6.1 Dispatcher state

The scheduler keeps the following for each shop, in memory:
- weight
- deficit
- whether it has runnable jobs
- its earliest `run_at`
- its running count
- its token bucket
- `throttled_until`

This state is refreshed from Postgres every refresh interval, with one row per shop:

```sql
SELECT s.id, st.throttled_until, now() AS db_now,
       (SELECT count(*)    FROM jobs WHERE shop_id = s.id AND state = 'running') AS running,
       (SELECT min(run_at) FROM jobs WHERE shop_id = s.id AND state = 'queued')  AS next_run_at
FROM shops s JOIN shop_state st ON st.shop_id = s.id;
```

It is also refreshed early when a notify arrives on `jobs_enqueued` or `shop_throttled`.

**Eligible shops:** a shop is *eligible* when all four hold:
- it has a runnable job (`next_run_at <= db_now`, compared using the database's clock as returned by the refresh)
- its running count is below its cap
- it has at least one token
- it is not throttled

Memory only *hints* at eligibility. The claim transaction (§6.3) is the authority for caps.

### 6.2 Schedulers

The scheduler is an interface with two implementations, chosen by config. Changing it changes nothing else (AC-9.3).

**DRR** (`internal/scheduler`) is a pure function and has no I/O. It uses standard deficit round robin (Shreedhar and Varghese) with costs:
- Eligible shops form a round-robin list.
- When the pointer reaches a shop, its deficit grows by `weight × base quantum`.
- The shop is offered `allowance = deficit`. The claim takes jobs oldest first while their cumulative cost stays within the allowance, and the deficit drops by the cost claimed.
- If the next job's cost exceeds what is left, the shop's turn ends and the pointer moves on.
- If the claim was cut short only because fewer workers were waiting, the turn continues on the next claim.
- A shop with no runnable jobs leaves the list and its deficit resets to 0.
- An ineligible shop that still has jobs is skipped. It gets no quantum and keeps its deficit.

**Why the Q6 bound holds:** while every shop stays backlogged, a shop's deficit at the end of its turn is in [0, C), where C is the largest job cost. After k full rounds, shop i has therefore been served more than k·qᵢ − C. Total service is at most (k+1)·Q·W, where W is the total weight. So the shop's share, (wᵢ/W)·S, is at most (k+1)·qᵢ. The shortfall against its share is therefore below qᵢ + C (D-026).

**FIFO:** the claim takes the globally oldest runnable jobs by `(run_at, id)` across eligible shops. Caps, buckets and throttles still apply.

### 6.3 Claim transaction

Both schedulers claim through the same transaction. It claims up to `n` jobs, where `n` is the number of waiting workers, for one worker each:

```sql
BEGIN;
-- 1. Candidates, oldest first, skipping rows other transactions hold.
--    DRR: WHERE shop_id = $shop.  FIFO: WHERE shop_id = ANY($eligible).
SELECT id, shop_id, cost FROM jobs
 WHERE state = 'queued' AND run_at <= now() AND <shop filter>
 ORDER BY run_at, id
 LIMIT $n
 FOR UPDATE SKIP LOCKED;
-- 2. For each distinct candidate shop, in shop-ID order (so no deadlocks):
SELECT pg_advisory_xact_lock(hashtextextended('claim:' || $shop, 0));
SELECT count(*) FROM jobs WHERE shop_id = $shop AND state = 'running';   -- exact room under the cap
SELECT throttled_until FROM shop_state WHERE shop_id = $shop;            -- skip the shop if > now()
-- 3. In Go: drop throttled shops, then keep candidates in order while the shop has room,
--    and (DRR) while cumulative cost <= allowance.
-- 4. Claim the kept rows.
UPDATE jobs j
   SET state = 'running', claim_seq = j.claim_seq + 1, lease_owner = $owner_for_row,
       lease_expires_at = now() + k.lease_ms * interval '1 millisecond',
       first_claimed_at = coalesce(j.first_claimed_at, now())
  FROM job_kinds k
 WHERE j.id = ANY($kept) AND k.kind = j.kind
RETURNING j.*;
COMMIT;   -- unkept candidates' row locks are released here
```

- The advisory lock per shop is what makes Q5 hold, even if a second dispatcher is claiming during a split brain (D-002). A single leader needs it only as a safety net.
- `FOR UPDATE SKIP LOCKED` means claimers never wait on each other's rows, and two claimers can never take the same row.
- The throttle check inside the transaction makes AC-6.2 exact: no claim that starts after a throttle commits can claim for that shop before `throttled_until`.
- The dispatcher spends one token per claimed job, and `n` never exceeds the shop's whole tokens. Token buckets refill using the dispatcher's monotonic clock. They are rate limits, not leases or timestamps, so NFR-14 doesn't apply to them.

### 6.4 Handing out work

- A worker goroutine calls `POST /internal/v1/claim` and waits for an answer.
- The dispatcher queues waiting workers. When workers are waiting and some shop is eligible, it runs one claim and answers each worker with its job.
- If no job is available before the long-poll timeout, the worker gets 204 and polls again.
- A standby dispatcher answers 503. Workers retry with backoff, which covers both a leader change and a restart.

## 7. Leases, heartbeats and fencing

- **Lease:** the claim sets `lease_expires_at = now() + lease`, using the database clock (NFR-14).
- **Heartbeat:** each worker process sends one statement every heartbeat interval, covering every job it holds:

  ```sql
  UPDATE jobs j SET lease_expires_at = now() + k.lease_ms * interval '1 millisecond'
    FROM job_kinds k
   WHERE k.kind = j.kind
     AND (j.id, j.claim_seq, j.lease_owner) IN
         (SELECT * FROM unnest($ids::bigint[], $seqs::int[], $owners::text[]))
     AND j.state = 'running'
     AND j.lease_expires_at > now()            -- an expired lease cannot be revived (Q2)
  RETURNING j.id;
  ```

  A job missing from the result has lost its lease. The worker cancels that handler's context right away (AC-4.3).
- **Fenced write:** every completion, failure and reschedule ends with this predicate (D-004):

  ```sql
  ... WHERE id = $id AND claim_seq = $seq AND lease_owner = $owner
        AND state = 'running' AND lease_expires_at > now()
  ```

  - Zero rows means the lease is lost, so the transaction rolls back and the result is discarded. Effects the handler wrote through `run.Tx()` are rolled back with it.
  - A terminal transition also decrements `shop_state.backlog`, in the same transaction.
- **Heartbeats that keep failing:** if heartbeats fail (for example, Postgres is unreachable) for a full lease, the worker cancels the handler early. This uses a local monotonic timer and only saves wasted work. The worker never decides from its own clock that a lease is valid; the fenced write in the database is the only authority.
- **Ambiguous commit:** a commit can fail with an unknown outcome, for example when the connection resets during `COMMIT`.
  - **For a completion,** the worker reads the job back. `completed` at its own `claim_seq` means the write succeeded, since only the holder of that `claim_seq` can complete at it. Anything else means the worker abandons the job, its lease expires, and the job runs again.
  - **For a failure or a throttle,** the worker abandons the job without reading it. Either its write landed, or the lease expires and the reaper requeues the job. Both outcomes leave the job queued.

## 8. Results, retries and throttles

| Handler result | Transition (fenced, one transaction) |
|---|---|
| `nil` | → `completed`, `completed_at = now()`, backlog −1, `run.Tx()` effects commit |
| `queue.Throttled(d)` | `throttles + 1`. Past the budget → `dead` ("throttle budget exhausted"), backlog −1. Otherwise → `queued` with `run_at = now() + d + jitter`. `shop_state.throttled_until = greatest(coalesce(throttled_until, now()), now() + d)`, plus a notify on `shop_throttled`. `attempts` is unchanged (Q7). |
| `queue.Permanent(err)` | → `dead`, reason "permanent", backlog −1 |
| any other error, a panic, or the hard timeout | `attempts + 1`. At the max → `dead` ("attempts exhausted"), backlog −1. Otherwise → `queued` with `run_at = now() + backoff` |

- **Backoff:** the delay is a uniform random value in [0, min(cap, base × 2^(attempts − 1))). The worker draws it, and the database adds it to `now()`.
- **Hard timeout:** at the kind's timeout, the handler's context is cancelled. If the handler ignores that and is still running after a short grace period, the worker writes the failure anyway and abandons the goroutine. The abandoned goroutine is counted in a metric. Any side effects it still makes are covered by §10.
- **Panics:** a panic is recovered and stored in `last_error`, truncated.

## 9. Reaper, pruner and leader election

- **Leader:** the dispatcher holds a session advisory lock on a dedicated connection.
  - If it can't get the lock, it stays on standby and retries every second.
  - If that connection drops, it stops claiming at once and cancels in-flight claims.
  - The reaper and the pruner run only on the leader.
- **Reaper:** runs every reaper scan, repeating while a batch comes back full:

  ```sql
  WITH expired AS (
    SELECT id FROM jobs WHERE state = 'running' AND lease_expires_at <= now()
    ORDER BY lease_expires_at LIMIT 500 FOR UPDATE SKIP LOCKED)
  UPDATE jobs j
     SET attempts = j.attempts + 1, lease_owner = NULL, lease_expires_at = NULL, last_error = 'lease expired',
         state = CASE WHEN j.attempts + 1 >= k.max_attempts THEN 'dead' ELSE 'queued' END,
         run_at = now()                                   -- runnable at once (D-031)
         -- plus dead_at and dead_reason when dead
    FROM expired e, job_kinds k WHERE j.id = e.id AND k.kind = j.kind
  RETURNING j.shop_id, j.state;
  -- then, in the same transaction: backlog −1 per shop for each row that went dead
  ```

- **Pruner:** runs every minute, deleting in batches of 5,000. It removes:
  - completed jobs older than the retention window
  - discarded jobs older than the retention window after discard
  - dedupe keys older than the dedupe window

  Idempotency keys go with their job through `ON DELETE CASCADE`.

## 10. Idempotency helper (D-017)

```go
// Once records (jobID, step) in tx. It returns false if a committed transaction already recorded it.
func Once(ctx context.Context, tx pgx.Tx, jobID int64, step string) (first bool, err error)

// Key returns the Idempotency-Key header value for an external call made by this job's step.
// It is identical on every run of the same job.
func Key(jobID int64, step string) string // "job-<id>-<step>"
```

- **Effects in `run.Tx()`** commit only with the fenced completion. A stale run fails fencing, and its effects roll back, so these effects don't need `Once`.
- **Steps that commit on their own:** `run.Step(ctx, name, fn)` commits `Once(name)` together with `fn`'s writes, so a rerun skips a step that already committed.
- **External calls:** the handler sends `Key(job, step)` as `Idempotency-Key`. `mockshop` applies the first request for each key and replays its response for repeats (§12).
- **What the helper doesn't change:** delivery stays at-least-once. The helper makes repeated runs harmless; it doesn't stop them.

## 11. APIs

### 11.1 `api` role (JSON over HTTP)

| Method and path | Purpose | Responses |
|---|---|---|
| `POST /v1/jobs` | enqueue `{shop, kind, payload, run_at?, dedupe_key?}` | 201 `{id}` · 200 `{id, duplicate}` · 404 unknown shop · 413 · 422 · 429 + `Retry-After` |
| `POST /v1/jobs/batch` | enqueue `{shop, jobs: [...]}`, all or nothing (D-030) | 201 `{ids, duplicates}` · 404 · 413 · 422 · 429 + `Retry-After` |
| `GET /v1/jobs/{id}` | state, counters and timestamps of one job | 200 · 404 |
| `POST /webhooks/shopify` | the Shopify webhook receiver | 200 created or duplicate · 401 bad or missing HMAC · 404 unknown shop · 413 · 422 unknown topic · 429 + `Retry-After` |
| `GET /v1/shops` | per-shop stats for the console (AC-12.1) | 200 |
| `GET /v1/shops/{id}/wait-series` | p99 queue wait in 10 s buckets for the last 10 min | 200 · 404 |
| `GET /v1/dlq?shop=&before=&limit=` | dead jobs that aren't discarded | 200 |
| `POST /v1/dlq/{id}/retry` | retry one dead job, through admission (D-032) | 200 · 404 · 409 not dead · 429 |
| `POST /v1/dlq/{id}/discard` | discard one dead job | 200 · 404 · 409 |
| `POST /v1/dlq/retry-all?shop=` | retry oldest first while they fit | 200 `{retried, still_dead}` |
| `GET /healthz`, `GET /metrics`, `GET /` | health, metrics, console files | |

**Webhook receiver:**
1. It reads the raw body, up to the max body size.
2. It computes base64(HMAC-SHA256(secret, raw body)) and compares it with `X-Shopify-Hmac-Sha256` using `hmac.Equal`. It parses the body only after that check passes.
3. `X-Shopify-Shop-Domain` names the shop, `X-Shopify-Topic` must be `orders/create`, and `X-Shopify-Webhook-Id` becomes the dedupe key.
4. The job is kind `order.tag`, with payload `{order_id}` (D-020).

**Read API:**
- p50 and p99 use `percentile_cont` over jobs whose `first_claimed_at` is in the window, read through the `jobs_first_claim` index.
- Running and backlog counts come from `shop_state` and the cap index.

### 11.2 `dispatcher` role

| Method and path | Purpose | Responses |
|---|---|---|
| `POST /internal/v1/claim` `{owner}` | long-poll for one job | 200 job (including `claim_seq` and lease length) · 204 timeout · 503 not leader |
| `GET /healthz`, `GET /metrics` | | |

### 11.3 Handler interface (Go)

```go
type Handler func(ctx context.Context, run *queue.Run) error

// run.Job                  ID, Shop, Kind, Payload, ClaimSeq, Attempts
// run.Tx()                 a transaction committed together with the fenced completion when the handler returns nil
// run.Step(ctx, name, fn)  commits fn's writes plus idem.Once(name) in a transaction of its own
// queue.Throttled(d)       reschedule after d without using an attempt
// queue.Permanent(err)     go to dead now
```

Handlers are registered by kind. Test-only kinds (used by chaos runs and invariant tests) are compiled in but only registered when `QUEUED_TEST_HANDLERS=1`.

### 11.4 Config file (`config/shops.json`, D-019)

```json
{
  "shops": [
    {"id": "shop-01.mockshop.test", "weight": 1, "max_concurrency": 4, "rate_per_sec": 50,
     "rate_burst": 50, "backlog_limit": 10000, "backlog_retry_after_ms": 5000}
  ],
  "kinds": [
    {"kind": "order.tag", "cost": 1, "lease_ms": 30000, "heartbeat_ms": 10000, "timeout_ms": 300000,
     "max_attempts": 10, "max_throttles": 50, "handler_sleep_ms": 30}
  ]
}
```

- Each process validates the file at startup and fails fast on an error. Examples of errors: a weight of 0, a heartbeat that isn't shorter than the lease, and a `mockshop` retry horizon that isn't shorter than the dedupe window.
- `api` upserts `shops`, `shop_state` and `job_kinds` from it.
- Bench, chaos and test runs use their own files under `bench/` and `tests/`.

## 12. mockshop

`mockshop` simulates Shopify; it never calls the real thing. All of its state lives in memory, and it is never a chaos target.

- **Admin API:** `POST /admin/api/mock/shops/{shop}/orders/{order_id}/tags` with body `{tag}`.
  - **Rate limit:** each shop has a leaky bucket (D-010), with capacity and leak rate from config. A request is allowed if `level + 1 ≤ capacity`; then `level += 1`. The level drains continuously at the leak rate.
  - **When full:** the response is 429 with `Retry-After: <seconds, decimal>`, the time until one slot frees. Every response carries `X-Shopify-Shop-Api-Call-Limit: <level>/<capacity>`.
  - **Idempotency:** with an `Idempotency-Key` header, the first request is applied and its response stored per (shop, key). Repeats replay that response and apply nothing.
  - **Mutation log:** every applied mutation is appended to a log. `GET /mock/v1/mutations?shop=` returns the applied count per key, which feeds NFR-11.
- **Webhook sender:** emits `orders/create` webhooks for each shop at configured rates.
  - Each webhook gets a fresh `X-Shopify-Webhook-Id` from `crypto/rand` and is signed with the dummy secret from `.env`.
  - With a configured probability it sends the same webhook again later, and with another it sends it again concurrently.
  - On any non-2xx response or a timeout, it retries with exponential backoff, honouring `Retry-After`, until the retry horizon.

## 13. Invariants Q1–Q8

| # | Invariant | Mechanism | Test |
|---|---|---|---|
| Q1 | No accepted job is lost: each ends completed or dead. | The enqueue commits before 201; Postgres keeps its durable defaults (`fsync` and `synchronous_commit` on); leases expire and the reaper requeues; attempts are bounded, so every job ends | `tests/chaos`: 100 seeded runs (§18); basic in-process version in phase 2 |
| Q2 | A worker whose lease expired cannot complete, fail or reschedule the job. | The fenced predicate checks `claim_seq`, the owner, the state and `lease_expires_at > now()`; heartbeats can't revive an expired lease | `tests/invariants`: hold a handler past its lease, let a second worker claim and complete, release the first; its write affects 0 rows. Also: lease expired but not yet reaped; the write still affects 0 rows |
| Q3 | At most one live lease per job. | `FOR UPDATE SKIP LOCKED` plus the state check on claim; `claim_seq` is monotonic | `tests/invariants` (rapid): many concurrent claimers on the real claim SQL, no dispatcher; each (job, `claim_seq`) has one owner, and lease intervals of one job never overlap by the DB clock |
| Q4 | A webhook delivered k times creates exactly one job. | Dedupe keys in the enqueue transaction, unique on (shop, key), checked before admission | `tests/invariants`: k concurrent and k sequential deliveries of one webhook ID, including across the job's completion |
| Q5 | A shop's running jobs never exceed `max_concurrency`. | Per-shop advisory lock plus an exact running count inside the claim transaction | dispatcher property test; two dispatchers claiming at once; running counts sampled during chaos and bench runs |
| Q6 | Under saturation, each backlogged shop gets at least its weighted share of cost units, within tolerance. | Standard DRR with costs (§6.2); tolerance qᵢ + C (D-026) | `internal/scheduler` (rapid): random weights, costs and batch sizes; the bound checked after every claim |
| Q7 | A throttle reschedules without using up an attempt. | `Throttled` touches `throttles`, never `attempts`; `claim_seq` is the separate fencing token | `tests/invariants`: `mockshop` bucket of capacity 1; every job completes with `attempts = 0` and `throttles > 0` for some |
| Q8 | At the backlog limit, enqueue returns 429 and creates nothing. | Shop row lock plus a counter check before any insert; batches are all or nothing | `tests/invariants`: fill to the limit; the next single, batch and webhook enqueues get 429 with no new jobs or keys; a duplicate still gets 200 |

## 14. Failure modes

| Failure | What happens | Outcome |
|---|---|---|
| Worker killed (kill -9) | Its leases stop being renewed and expire; the reaper requeues the jobs (attempt +1, runnable at once) | Jobs rerun within one lease plus one reaper scan (NFR-10); duplicate side effects are guarded by §10 |
| Worker paused (GC, SIGSTOP, partition) | Its lease expires; another worker claims the job; the paused worker's heartbeat and fenced write affect 0 rows | Its result is discarded and its `run.Tx()` effects roll back (Q2) |
| Postgres restart | Open transactions abort, and every process reconnects with backoff; leases whose expiry passed during the outage get reaped | No accepted job is lost (Q1); some jobs run twice |
| Latency or connection resets (Toxiproxy) | Heartbeats slow down; a reset during commit is ambiguous | Ambiguous commits are resolved by reading the job back (§7); a lease is lost only if heartbeats miss a whole lease |
| Dispatcher dies | Claims stop; workers retry; Compose restarts the dispatcher; deficits reset to 0 | Nothing is lost; fairness restarts from zero (D-002) |
| Two dispatchers at once | Both claim | Q3 holds (SKIP LOCKED) and Q5 holds (advisory lock); only fairness skews |
| `api` dies | Enqueues fail; callers retry, made safe by dedupe keys; `mockshop` retries webhooks | Nothing accepted is lost |
| `mockshop` down | Handler calls fail as temporary errors | Backoff and retries; jobs go dead if it stays down past max attempts |
| Admin API throttles | `Throttled` result; the shop pauses until Retry-After | No attempts are used up (Q7); throttle budget bounds the loop |
| Poison job (always fails, or crashes its worker) | Every failure or expired lease counts an attempt | Dead after max attempts |
| Handler ignores cancellation | Hard timeout plus grace, then the worker writes the failure and abandons the goroutine | The job retries; stray effects are guarded by §10 |
| Skewed worker clock | Leases and timestamps come from `now()` | No effect on correctness (NFR-14) |

## 15. Security

- **Webhooks:** the HMAC is checked over the raw body in constant time, before parsing. Bodies are size-limited. The secret comes from the environment; `.env.example` holds a dummy value, and `.env` is git-ignored.
- **Network exposure:** every published port is bound to `127.0.0.1`. The console and the read and DLQ APIs have no auth (a non-goal), which is acceptable only because nothing listens beyond localhost. A Phase 1 check fails if any port in the Compose file is published without `127.0.0.1`.
- **Docker socket:** only the chaos harness container gets it, under the `chaos` profile (D-025). That container has full control of Docker Desktop while it runs. The queue services never get the socket.
- **SQL:** every query is parameterized through pgx. Nothing shells out.
- **Dependencies:** only the PLAN stack (D-001, D-006), pinned in `go.sum`, `package-lock.json` and the Compose file. The console's `.npmrc` sets `ignore-scripts=true`. `make audit` runs govulncheck and `npm audit`.

## 16. Test plan

| Layer | Where | What | Tools | Phase |
|---|---|---|---|---|
| Unit | `internal/*` | backoff bounds, token bucket, config validation, HMAC, metrics text format, Retry-After parsing | `go test -race` | 2–6 |
| Property | `internal/scheduler` | Q6 bound; deficit reset when a shop empties; ineligible shops skipped | rapid | 4 |
| Property | `internal/dispatcher` | Q5 against a model of claims and completions | rapid | 4 |
| Invariant | `tests/invariants` | Q2, Q3, Q4, Q5, Q7, Q8, against real Postgres in Compose | `go test -race`, rapid | 2–5 |
| Property | `tests/invariants` | backlog counter equals a real count after random enqueues, claims, results and DLQ actions | rapid | 4 |
| Adversarial | `tests/adversarial` | tampered or missing HMAC, oversized bodies, malformed JSON, replay storms, two dispatchers, skewed worker clocks, handlers that ignore cancellation | `go test -race` | 3–6 |
| Chaos | `tests/chaos` | Q1 over 100 seeded runs; crash recovery (NFR-10); one effect per job (NFR-11, from phase 5) | harness (§18) | 3, 5 |
| Bench | `bench/` | Black Friday (NFR-9, NFR-12), the Postgres ceiling, the ±25% check (NFR-13) | `loadgen`, bench runner | 5–6 |
| Console | `console/` | typecheck and production build; behaviour through Go tests of the read API (D-012) | `tsc`, Vite | 6 |

`make test` runs unit, property, invariant and adversarial tests, all with `-race`. `make chaos` and `make bench` are separate. CI runs `make lint test audit`, the console checks, and a 3-run chaos smoke (D-022).

## 17. Black Friday experiment (D-009, D-027)

- **Hypothesis:** under a 100× flood from one shop, DRR keeps the other shops' p99 queue wait near their warm-up level, and FIFO doesn't.
- **Settings:**
  - 10 shops, weight 1 each, 20 jobs/s each.
  - 16 workers.
  - The `order.tag` handler sleeps 30 ms, then makes one `mockshop` call.
  - 2 min warm-up. Then shop 1 sends 2,000 jobs/s for 5 min. Then every shop is back at the steady rate until every accepted job is done.
  - The backlog limit is 50,000 per shop.
  - Caps, token buckets and `mockshop` buckets are set so they never bind. Both runs use one config file, `bench/blackfriday.json`; the scheduler is the only difference.
- **Load:**
  - `loadgen` is open-loop: Poisson arrivals per shop from a seeded random generator.
  - It sends each tick's arrivals for a shop as one batch, and counts 429s as shed without retrying them.
  - The arrival schedule is identical in both runs.
- **Procedure:** for each scheduler, `make bench` does the following:
  1. It starts from a fresh database.
  2. It runs the timeline and waits until every accepted job is terminal.
  3. It exports per-job timestamps from Postgres and the `mockshop` mutation counts.
- **Outputs:** everything lands in `bench/results/<timestamp>/`:
  - per-run raw data
  - `summary.json`
  - `blackfriday.svg`, drawn by a Go program using only the standard library: each shop's p99 queue wait in 10 s buckets over time, FIFO and DRR side by side
  - `report.md`, with PASS/FAIL for NFR-9, NFR-11 and NFR-12, plus throughput, p50/p99, end-to-end time and shed counts
- **Validity:**
  - Everything shares one machine, including Docker Desktop's VM, so absolute numbers are machine-specific and the report records the machine.
  - The FIFO run claims in batches exactly as DRR does, so FIFO isn't handicapped by claim overhead.
  - A missed target is a mid-phase surprise: work stops and the user decides. The target is never quietly changed (D-027).
- **Throttle storm (optional, D-010):** the same setup with a small `mockshop` bucket for shop 1, to show throttle handling. If it isn't built in phase 5, it is listed under follow-ups.

## 18. Chaos harness (D-023, D-025)

- **One run:**
  1. Bring up the stack with short leases. In the chaos profile, workers and the dispatcher reach Postgres through Toxiproxy.
  2. Enqueue 2,000 jobs across 5 shops with dedupe keys, so retried enqueues stay safe and every accepted ID is known. The jobs use a test kind that sleeps randomly and writes a `run.Tx()` effect; from phase 5 they also call `mockshop`.
  3. Run a seeded fault schedule. Each step is one of: kill -9 a worker container (it restarts); pause and unpause a worker past its lease; restart Postgres; or add Toxiproxy latency, timeouts or `reset_peer` on the Postgres links.
  4. Wait until every accepted job is terminal, or until the run timeout (setting: 120 s), which counts as a failure.
- **Checks:**
  - Every accepted ID is completed or dead (Q1).
  - No job has two overlapping leases (Q3), checked from claim records.
  - The backlog counter matches a real count.
  - Each completed job's `run.Tx()` effect was applied once.
  - From phase 5, `mockshop`'s applied mutations equal one per completed job (NFR-11).
- **Output and replay:** each run writes its seed, fault schedule and check results to `bench/results/chaos/`. `CHAOS_SEED=<n> make chaos-one` replays a single run. Chaos runs use a 2 s lease, and the target is 30 s per run (both settings).
- **Crash-recovery scenario (NFR-10):** this runs with the default 30 s lease and measures the time from the kill to the last orphaned job's new claim.
- **Docker control:** the harness talks to the Docker Engine API over `/var/run/docker.sock` with `net/http`, and drives Toxiproxy's HTTP API (D-006).

## 19. Metrics

The text format is written by hand (D-006). Each process serves `/metrics`.

- `jobs_enqueued_total{shop}`, `jobs_shed_total{shop}`, `jobs_duplicate_total{shop}`
- `jobs_claimed_total{shop}`, `jobs_completed_total{shop}`, `jobs_failed_total{shop,reason}`, `jobs_dead_total{shop,reason}`, `jobs_throttled_total{shop}`
- `queue_wait_seconds{shop}` (histogram), `job_run_seconds{kind}` (histogram)
- `shop_backlog{shop}`, `shop_running{shop}` (gauges, from the dispatcher's refresh)
- `lease_lost_total`, `reaper_requeued_total`, `heartbeat_seconds` (histogram), `abandoned_handlers_total`
- `dispatcher_is_leader`, `claim_batch_size` (histogram), `claim_seconds` (histogram)
- From `mockshop`: `mock_api_requests_total{shop,status}`, `mock_api_mutations_applied_total{shop}`, `mock_api_idempotent_replays_total{shop}`, `mock_webhooks_sent_total{shop,kind}`

## 20. Where Postgres-as-a-queue stops scaling (D-008)

- **Row churn:**
  - Every job is several updates (claim, heartbeats, result), and each update writes a new row version.
  - Autovacuum has to keep up. If it falls behind, the table and its partial indexes bloat, and `SKIP LOCKED` scans read more dead or locked entries before finding a free one.
  - Mitigations: batched heartbeats, pruning, and, if needed, a partitioned `jobs` table so old partitions are dropped rather than deleted.
- **Hot rows:** a shop's `shop_state` row is updated by every enqueue and every terminal result for that shop, and its lock serializes them. Batching (D-030) reduces that for enqueues.
- **Single primary:** all writes and all WAL go through one server, and the claim path is a single dispatcher. Read replicas don't help claims.
- **Connections:** every worker process holds connections. At many processes, a pooler becomes necessary; pooling is outside the PLAN stack, so it appears here as text only (D-001).
- **O(shops) refresh:** the dispatcher refresh is one query per refresh interval with one row per shop. At very large shop counts, it needs push-based eligibility instead.
- **What to move to:**
  - A partitioned log (a Kafka-style system) for ingest and per-tenant ordering, with Postgres kept for state.
  - Or Postgres sharded by tenant, as in phase 7 and Shopify's pods.
  - Both appear here as text only.
- **Measured ceiling:** `make bench-ceiling` (phase 6) raises load until claim p99 gets worse. This section cites the ceiling only from that script's output.

## 21. Alternatives rejected

| Alternative | Why not |
|---|---|
| Wrap Sidekiq, Solid Queue or River | The point is to own the delivery semantics (PLAN §1) |
| Exactly-once delivery | Not achievable once handlers call external systems; at-least-once plus fencing plus idempotent side effects is honest (non-goal) |
| Workers schedule themselves with DRR state in the database | Hot shared rows serialize claims, and fairness becomes approximate (D-002) |
| Leaderless stride scheduling | It is weighted fair queuing, not DRR, and drifts when claimers compete (D-002) |
| Strict priority between shops | It starves lower-priority shops; DRR bounds unfairness, and each decision is O(1) |
| Unit cost | It is weighted round robin under another name (D-013) |
| Split ready/claimed tables (Solid Queue style) | Every transition moves rows, and fencing spans two tables (D-015) |
| Dedupe key as a unique column on `jobs` | Pruning would reopen dedupe (D-018) |
| Shorter leases (10 s) | Postgres restarts and injected latency would expire healthy leases (D-003) |
| Redis, Kafka, a pooler, or Prometheus and Toxiproxy client libraries | Outside the PLAN stack (D-001, D-006) |
| A migration tool or a YAML config | Third-party; the Go standard library and JSON are enough (D-001, D-019) |

## 22. Prior art

These descriptions come from each project's public documentation. Phase 6 checks them again before the README cites them.

- **Solid Queue** (the default job backend in new Rails 8 apps):
  - It is database-backed and polls with `FOR UPDATE SKIP LOCKED`.
  - It keeps ready, scheduled and claimed executions in separate tables.
  - Its supervisor tracks process liveness with heartbeats and releases a dead process's claimed jobs.
  - It offers per-key concurrency limits and priorities.
  - It has no weighted fair share across tenants. Liveness is tracked per process, not per job lease with a fencing token.
- **River** (Go, Postgres):
  - It claims with `SKIP LOCKED`, and its enqueue can join the caller's own transaction.
  - It has unique jobs, and a leader-elected maintenance process rescues stuck jobs, schedules jobs and prunes old ones.
  - It uses LISTEN/NOTIFY to wake producers.
  - The open-source version has no per-tenant weighted fair scheduling.
- **Sidekiq** (Ruby, Redis):
  - In the open-source version, a job a process was running when it crashed can be lost. Reliable fetch is a paid feature.
  - Its weighted queues pick queues at random by weight, so per-tenant fairness needs a queue per tenant.
  - Rate limiting and unique jobs are paid features.
- **This queue's edge:**
  - per-job leases with fenced completions
  - weighted DRR across tenants, with a proven bound
  - per-tenant admission control
  - throttle-aware retries that don't use up attempts
  - an experiment that measures fairness against FIFO

## 23. Sharded queue (3+, D-016)

```sql
CREATE TABLE nodes      (id text PRIMARY KEY, heartbeat_at timestamptz NOT NULL);
CREATE TABLE partitions (id smallint PRIMARY KEY CHECK (id BETWEEN 0 AND 15),
                         owner text, epoch bigint NOT NULL DEFAULT 0, lease_expires_at timestamptz);
ALTER TABLE jobs ADD COLUMN partition_id smallint NOT NULL;  -- set at enqueue: FNV-1a(shop_id) mod 16
```

- **Node:** a node is one dispatcher plus its workers. Workers long-poll their own node's dispatcher.
- **Leases:** a node renews its partition leases every rebalance scan.
  - Taking a free or expired partition sets `owner = me`, `epoch = epoch + 1` and `lease_expires_at = now() + partition lease`, all in one conditional update.
  - Releasing a partition clears `owner` and bumps the epoch.
- **Epoch fencing on claims:** a node's claim transaction adds `AND EXISTS (SELECT 1 FROM partitions p WHERE p.id = jobs.partition_id AND p.owner = $node AND p.epoch = $epoch AND p.lease_expires_at > now())`.
  - A node that lost a partition (its lease expired, or the partition was taken at a higher epoch) can claim nothing from it.
  - Jobs it had already claimed are still guarded by job leases and fencing (§7).
- **Rebalancing:** each scan, a node counts live nodes, meaning those whose heartbeat is within one partition lease, and computes `target = ceil(16 / live)`.
  - A node over its target releases partitions until it is at the target.
  - A node under its target takes free or expired partitions.
  - The partitions table settles to at most `target` per node.
- **Fairness:** DRR runs per node, over the shops in that node's partitions. It holds within a node, not globally, and this is reported as such.
- **Failover tests:**
  - kill a node; its partitions move within one partition lease plus one scan (AC-18.2)
  - pause a node past its lease, then resume it; its claims are rejected (AC-18.3)
  - add and remove nodes; check the ownership counts (AC-18.4)
  - every chaos check (Q1–Q5) still passes

  Duplicates are counted and explained.
- **Not covered:** storage doesn't scale here; that would need a database per shard (text only).

## 24. Risks to watch

| Risk | Signal | Response |
|---|---|---|
| Shop 1's `shop_state` row bottlenecks enqueues or completions during the spike | `loadgen` reports a lower accepted rate than offered without 429s; claim or completion latency climbs | Measure first; then batch the decrements on completion. This is a design change, so stop and ask |
| A single dispatcher can't keep up with the claim rate | `claim_seconds` p99 climbs while workers sit idle | Larger claim batches; then stop and ask |
| Docker Desktop's disk and network overhead on macOS | Bench numbers vary between runs | The ±25% check (NFR-13) catches it; the report records the machine |
| The DRR bound in D-026 fails in the property test | rapid shrinks to a counterexample | Stop and ask; never loosen the bound quietly |
