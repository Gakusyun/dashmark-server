-- DashMark 云同步 · D1 表结构（与 Go 版 SQLite 一致）
CREATE TABLE IF NOT EXISTS sync_data (
  id           TEXT PRIMARY KEY,
  verifier     TEXT NOT NULL,
  payload      BLOB NOT NULL,
  payload_hash TEXT NOT NULL,
  updated_at   INTEGER NOT NULL
);
