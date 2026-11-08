-- The runner's own ledger: one row per applied migration, written in the same
-- transaction as the migration itself (internal/migrate).
CREATE TABLE schema_migrations (
  version    bigint      PRIMARY KEY CHECK (version > 0),
  name       text        NOT NULL,
  checksum   text        NOT NULL,                 -- hex SHA-256 of the file as applied
  applied_at timestamptz NOT NULL DEFAULT now()
);
