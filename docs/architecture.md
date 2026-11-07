# Architecture

The diagrams for the queue. [`design.md`](design.md) gives the mechanisms behind each one, and [`decisions.md`](decisions.md) gives the reasons (D-xxx). GitHub renders every diagram here as Mermaid.

## 1. Components

```mermaid
flowchart LR
  subgraph sim["mockshop: simulated Shopify, never the real one"]
    WHS["Webhook sender<br/>HMAC-signed, duplicates, retries"]
    ADM["Admin API<br/>leaky bucket per shop<br/>Idempotency-Key replay"]
  end
  LG["loadgen<br/>open-loop, seeded<br/>(bench profile)"]

  subgraph q["queued: one binary, three roles"]
    API["api<br/>enqueue, batch enqueue<br/>webhook receiver<br/>read API, console files"]
    DSP["dispatcher (leader)<br/>DRR or FIFO, caps<br/>token buckets, claims<br/>reaper, pruner"]
    WK["worker x2 processes<br/>8 handler goroutines each<br/>batched heartbeats"]
  end

  PG[("Postgres 16<br/>jobs, shop_state<br/>dedupe_keys, idempotency_keys")]
  CON["Console<br/>React, one page"]
  PROM["Prometheus"]

  WHS -- "POST /webhooks/shopify" --> API
  LG -- "POST /v1/jobs/batch" --> API
  API -- "enqueue txn: dedupe, admission" --> PG
  WK -- "long-poll claim" --> DSP
  DSP -- "claim txn: SKIP LOCKED, cap check" --> PG
  WK -- "heartbeats, fenced results" --> PG
  WK -- "order.tag API call" --> ADM
  CON -- "poll every 2 s" --> API
  PROM -. "scrape /metrics" .-> API
  PROM -.-> DSP
  PROM -.-> WK
  PROM -.-> ADM
```

## 2. Deployment (Docker Compose)

Every published port is bound to `127.0.0.1` (NFR-16). The `tools` image runs Go, golangci-lint and govulncheck, because nothing is installed on the Mac (D-021).

```mermaid
flowchart TB
  subgraph dflt["default profile: make up"]
    MIG["migrate<br/>one-shot"] --> PG[("postgres:16")]
    API["queued api"] --> PG
    DSP["queued dispatcher<br/>restart: always"] --> PG
    W1["queued worker-1"] --> DSP
    W2["queued worker-2"] --> DSP
    W1 --> PG
    W2 --> PG
    MS["mockshop"] --> API
    TP["toxiproxy"]
    PR["prometheus"]
  end
  subgraph bench["bench profile: make bench"]
    LG["loadgen"] --> API
  end
  subgraph chaos["chaos profile: make chaos"]
    CH["chaos harness<br/>Docker socket mounted<br/>only here (D-025)"]
  end
  subgraph tools["tools image: make lint, test, audit"]
    T["golang + golangci-lint + govulncheck<br/>pinned versions"]
  end
  CH -. "kill -9, pause, restart" .-> W1
  CH -. "restart" .-> PG
  CH -. "toxics via HTTP API" .-> TP
  TP -. "chaos profile: workers and dispatcher reach Postgres through Toxiproxy" .-> PG
```

## 3. Job lifecycle

`queued` covers both runnable jobs (`run_at` ≤ `now()`) and scheduled ones. `claim_seq` rises on every claim, while `attempts` rises only on failures and expired leases (D-004).

```mermaid
stateDiagram-v2
  [*] --> queued: enqueue (201)
  queued --> running: claim, claim_seq + 1, lease set
  running --> running: heartbeat extends lease
  running --> completed: fenced complete
  running --> queued: temporary failure, attempts + 1, backoff
  running --> queued: Throttled, throttles + 1, attempts unchanged
  running --> queued: lease expired (reaper), attempts + 1, runnable at once
  running --> dead: Permanent error
  running --> dead: attempts or throttle budget exhausted
  dead --> queued: DLQ retry, admission checked
  dead --> dead: DLQ discard sets discarded_at
  completed --> [*]: pruned after retention
  dead --> [*]: pruned after discard plus retention
```

## 4. Claim, heartbeat and a lost lease

This shows Q2 and the fencing rule. Every time comes from Postgres's `now()`.

```mermaid
sequenceDiagram
  autonumber
  participant A as Worker A
  participant D as Dispatcher
  participant P as Postgres
  participant B as Worker B

  A->>D: POST /internal/v1/claim (long-poll)
  D->>P: claim txn: SKIP LOCKED, cap check, claim_seq 6 to 7, owner A, lease = now() + 30 s
  D-->>A: job 42, claim_seq 7
  loop every 10 s while the handler runs
    A->>P: heartbeat (42, seq 7, owner A) where lease not expired
    P-->>A: 1 row, lease = now() + 30 s
  end
  Note over A: A stalls (GC pause, SIGSTOP, network partition)
  Note over P: lease_expires_at passes on the DB clock
  D->>P: reaper: expired, so state queued, attempts + 1, run_at = now()
  B->>D: POST /internal/v1/claim
  D->>P: claim txn: claim_seq 7 to 8, owner B
  D-->>B: job 42, claim_seq 8
  B->>P: fenced complete (42, seq 8, owner B, lease live)
  P-->>B: 1 row, completed
  Note over A: A resumes
  A->>P: fenced complete (42, seq 7, owner A)
  P-->>A: 0 rows
  Note over A: lease lost: roll back run.Tx() effects, discard the result
```

A second path ends the same way. If A's heartbeat returns 0 rows while A is still running, A cancels the handler's context at once and never writes a result (AC-4.3).

## 5. Webhook path

This shows HMAC, dedupe before admission, and the 429 at the backlog limit (Q4, Q8, D-018).

```mermaid
sequenceDiagram
  autonumber
  participant M as mockshop
  participant R as api (webhook receiver)
  participant P as Postgres

  M->>R: POST /webhooks/shopify (X-Shopify-Webhook-Id W, HMAC)
  R->>R: HMAC-SHA256 over raw body, constant-time compare
  alt bad or missing HMAC
    R-->>M: 401 (nothing created)
  else valid
    R->>P: BEGIN, look up dedupe key (shop, W)
    alt key exists
      R->>P: ROLLBACK
      R-->>M: 200 existing job id (even at the limit)
    else new
      R->>P: lock shop_state row
      alt backlog at limit
        R->>P: shed_total + 1, COMMIT
        R-->>M: 429 Retry-After (no job, no key)
        Note over M: retries later, until its horizon (shorter than the 7-day window)
      else room
        R->>P: insert job, insert key, backlog + 1, notify, COMMIT
        R-->>M: 200 new job id
      end
    end
  end
```

## 6. One dispatcher decision

```mermaid
flowchart TD
  W["workers waiting: n"] --> E{"any eligible shop?<br/>runnable, below cap,<br/>has token, not throttled"}
  E -- no --> WAIT["wait for notify or refresh<br/>(204 at long-poll timeout)"]
  E -- yes --> S{"scheduler"}
  S -- DRR --> D1["next shop in round robin<br/>new turn: deficit += weight x quantum"]
  S -- FIFO --> F1["all eligible shops,<br/>oldest runnable first"]
  D1 --> C["claim txn<br/>(a) up to n candidates, SKIP LOCKED<br/>(b) advisory lock per shop, exact running count, throttle check<br/>(c) keep while room under cap, DRR also while cost within deficit<br/>(d) mark running, claim_seq + 1, lease"]
  F1 --> C
  C --> H["answer one worker per claimed job<br/>DRR: deficit -= cost, spend tokens"]
  H --> T{"DRR turn over?"}
  T -- "next cost > deficit" --> NEXT["advance pointer"]
  T -- "shop empty" --> RESET["remove shop, deficit = 0"]
  T -- "fewer workers than jobs" --> STAY["keep turn"]
```

## 7. Data model

```mermaid
erDiagram
  shops ||--|| shop_state : "hot counters"
  shops ||--o{ jobs : "owns"
  job_kinds ||--o{ jobs : "kind"
  shops ||--o{ dedupe_keys : "owns"
  jobs ||--o{ idempotency_keys : "steps"

  shops {
    text id PK "shop domain"
    int weight
    int max_concurrency
    float rate_per_sec
    int rate_burst
    int backlog_limit
    int backlog_retry_after_ms
  }
  shop_state {
    text shop_id PK
    int backlog "queued plus running"
    bigint shed_total
    timestamptz throttled_until
  }
  job_kinds {
    text kind PK
    int cost "DRR cost"
    int lease_ms
    int heartbeat_ms
    int timeout_ms
    int max_attempts
    int max_throttles
  }
  jobs {
    bigint id PK
    text shop_id FK
    text kind FK
    int cost
    jsonb payload
    text state "queued, running, completed, dead"
    timestamptz run_at
    timestamptz first_runnable_at
    timestamptz first_claimed_at
    int claim_seq "fencing token"
    int attempts "retry budget"
    int throttles
    text lease_owner
    timestamptz lease_expires_at
    timestamptz completed_at
    timestamptz dead_at
    timestamptz discarded_at
  }
  dedupe_keys {
    text shop_id PK
    text key PK
    bigint job_id "no FK, may outlive the job"
    timestamptz created_at
  }
  idempotency_keys {
    bigint job_id PK
    text step PK
    timestamptz created_at
  }
```

## 8. Sharded queue (3+, wave 2)

All 16 logical partitions live in one Postgres. A node is one dispatcher plus its workers, and it owns its partitions through leases that carry an epoch (D-016).

```mermaid
flowchart TB
  subgraph nodes["nodes: dispatcher + workers each"]
    N1["node 1<br/>owns P0 to P5 at their epochs"]
    N2["node 2<br/>owns P6 to P10"]
    N3["node 3<br/>owns P11 to P15"]
  end
  subgraph pg["Postgres 16"]
    PT["partitions<br/>id, owner, epoch, lease_expires_at"]
    NT["nodes<br/>id, heartbeat_at"]
    J["jobs<br/>partition_id = FNV-1a(shop_id) mod 16"]
  end
  N1 -- "renew leases every scan" --> PT
  N2 -- "renew leases every scan" --> PT
  N3 -- "renew leases every scan" --> PT
  N1 -- "heartbeat" --> NT
  N1 -- "claim only where partition owned at my epoch" --> J
  N2 -- "claim" --> J
  N3 -- "claim" --> J
```

Failover and split brain:

```mermaid
sequenceDiagram
  autonumber
  participant N3 as Node 3
  participant P as Postgres (partitions)
  participant N1 as Node 1

  N3->>P: renew P11 to P15 (epoch e)
  Note over N3: N3 pauses (or dies)
  Note over P: P11 to P15 leases expire on the DB clock
  N1->>P: scan: live nodes = 2, target = 8, take expired P11 and P12 (epoch e + 1)
  Note over N1: node 2 takes P13 to P15 the same way, so each node owns 8
  N1->>P: claim from P11 where owner = N1 and epoch = e + 1
  P-->>N1: jobs
  Note over N3: N3 resumes, still believing it owns P11 at epoch e
  N3->>P: claim from P11 where owner = N3 and epoch = e
  P-->>N3: 0 rows (epoch fencing)
  N3->>P: renew P11 at epoch e
  P-->>N3: 0 rows, so N3 drops P11 and rebalances
```
