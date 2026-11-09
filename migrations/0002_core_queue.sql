-- The core queue (design §3): shops and their hot counters, job kinds, and jobs.
-- shops, shop_state and job_kinds are seeded from config/shops.json by the api
-- role (D-019). Lease and claim times come from the database clock (D-036).

CREATE TABLE shops (
  id                     text PRIMARY KEY CHECK (id <> ''),      -- shop domain, e.g. shop-01.mockshop.test
  weight                 int  NOT NULL CHECK (weight > 0),
  max_concurrency        int  NOT NULL CHECK (max_concurrency > 0),
  rate_per_sec           double precision NOT NULL CHECK (rate_per_sec > 0),
  rate_burst             int  NOT NULL CHECK (rate_burst > 0),
  backlog_limit          int  NOT NULL CHECK (backlog_limit > 0),
  backlog_retry_after_ms int  NOT NULL CHECK (backlog_retry_after_ms > 0)
);

-- Hot counters, one row per shop, kept apart so the static row stays cheap to read.
CREATE TABLE shop_state (
  shop_id         text   PRIMARY KEY REFERENCES shops (id),
  backlog         int    NOT NULL DEFAULT 0 CHECK (backlog >= 0),  -- jobs queued or running
  shed_total      bigint NOT NULL DEFAULT 0,                       -- jobs rejected with 429 (phase 4)
  throttled_until timestamptz                                      -- set by Throttled results (phase 5)
);

CREATE TABLE job_kinds (
  kind          text PRIMARY KEY CHECK (kind <> ''),
  cost          int  NOT NULL CHECK (cost >= 1),                    -- DRR cost (D-013)
  lease_ms      int  NOT NULL CHECK (lease_ms > 0),
  heartbeat_ms  int  NOT NULL CHECK (heartbeat_ms > 0 AND heartbeat_ms < lease_ms),
  timeout_ms    int  NOT NULL CHECK (timeout_ms > 0),
  max_attempts  int  NOT NULL CHECK (max_attempts >= 1),
  max_throttles int  NOT NULL CHECK (max_throttles >= 0)
);

CREATE TABLE jobs (
  id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  shop_id           text  NOT NULL REFERENCES shops (id),
  kind              text  NOT NULL REFERENCES job_kinds (kind),
  cost              int   NOT NULL CHECK (cost >= 1),   -- copied from job_kinds at enqueue
  payload           jsonb NOT NULL,
  state             text  NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'dead')),
  run_at            timestamptz NOT NULL,               -- claimable when run_at <= the database clock
  enqueued_at       timestamptz NOT NULL DEFAULT now(),
  first_runnable_at timestamptz NOT NULL,               -- greatest(enqueue time, requested run_at)
  first_claimed_at  timestamptz,                        -- queue wait = first_claimed_at - first_runnable_at (D-011)
  claim_seq         int  NOT NULL DEFAULT 0 CHECK (claim_seq >= 0),  -- fencing token, +1 on every claim (D-004)
  attempts          int  NOT NULL DEFAULT 0 CHECK (attempts >= 0),   -- failed attempts, the retry budget
  throttles         int  NOT NULL DEFAULT 0 CHECK (throttles >= 0),
  lease_owner       text,
  lease_expires_at  timestamptz,
  completed_at      timestamptz,
  dead_at           timestamptz,
  dead_reason       text,
  last_error        text,                               -- truncated to 4 KiB by the worker
  discarded_at      timestamptz,
  CHECK ((state = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
  CHECK ((state = 'completed') = (completed_at IS NOT NULL)),
  CHECK ((state = 'dead') = (dead_at IS NOT NULL AND dead_reason IS NOT NULL)),
  CHECK (discarded_at IS NULL OR state = 'dead')
);

CREATE INDEX jobs_queued_shop  ON jobs (shop_id, run_at, id) WHERE state = 'queued';    -- DRR claims
CREATE INDEX jobs_queued_fifo  ON jobs (run_at, id)          WHERE state = 'queued';    -- FIFO claims
CREATE INDEX jobs_running_shop ON jobs (shop_id)             WHERE state = 'running';   -- cap checks
CREATE INDEX jobs_running_exp  ON jobs (lease_expires_at)    WHERE state = 'running';   -- reaper
CREATE INDEX jobs_dlq          ON jobs (shop_id, dead_at)    WHERE state = 'dead' AND discarded_at IS NULL;
CREATE INDEX jobs_done_at      ON jobs (completed_at)        WHERE state = 'completed'; -- pruner
CREATE INDEX jobs_discarded_at ON jobs (discarded_at)        WHERE discarded_at IS NOT NULL;
CREATE INDEX jobs_first_claim  ON jobs (shop_id, first_claimed_at) WHERE first_claimed_at IS NOT NULL; -- wait stats
