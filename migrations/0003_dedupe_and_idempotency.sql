-- Enqueue dedupe keys (D-018) and the idempotency helper's step keys (D-017),
-- design §3, §5.1 and §10.

-- One row per (shop, key) seen within the dedupe window. The pruner deletes rows
-- older than the window (design §9). job_id has no foreign key: the pruner may
-- delete a completed job before its key, and the key must still dedupe.
CREATE TABLE dedupe_keys (
  shop_id    text   NOT NULL REFERENCES shops (id),
  key        text   NOT NULL CHECK (key <> ''),          -- X-Shopify-Webhook-Id, or a caller's key
  job_id     bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (shop_id, key)
);
CREATE INDEX dedupe_keys_created ON dedupe_keys (created_at);  -- pruner

-- One row per (job, step) whose effect has committed (idem.Once). The keys go
-- with their job when the pruner deletes it.
CREATE TABLE idempotency_keys (
  job_id     bigint NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  step       text   NOT NULL CHECK (step <> ''),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, step)
);
