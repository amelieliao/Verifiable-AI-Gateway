# FILE: vault/sql/001_init.sql
CREATE TABLE IF NOT EXISTS receipts (
  tenant   TEXT   NOT NULL,
  head_hex TEXT   PRIMARY KEY,
  prev     TEXT   NOT NULL,
  ts_ns    BIGINT NOT NULL,
  ctx      JSONB  NOT NULL,
  body     JSONB  NOT NULL
);
CREATE TABLE IF NOT EXISTS tenant_state (
  tenant    TEXT PRIMARY KEY,
  last_head TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_receipts_tenant_ts ON receipts (tenant, ts_ns DESC);
