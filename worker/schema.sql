-- DashMark 云同步 · D1 表结构（与 Go 版 SQLite 一致）
CREATE TABLE IF NOT EXISTS sync_data (
  id            TEXT PRIMARY KEY,
  verifier      TEXT NOT NULL,
  payload       BLOB NOT NULL,
  payload_hash  TEXT NOT NULL,
  updated_at    INTEGER NOT NULL,
  last_accessed INTEGER NOT NULL DEFAULT 0  -- 最后访问时间（Unix 毫秒），惰性清理依据
);

-- 惰性清理任务元数据（key = 'asset_clean' 记录上次清理时间）
CREATE TABLE IF NOT EXISTS job_meta (
  key           TEXT PRIMARY KEY,
  last_clean_at INTEGER NOT NULL
);
INSERT OR IGNORE INTO job_meta(key, last_clean_at) VALUES('asset_clean', 0);

-- 老库迁移：补 last_accessed 列（已存在时本语句失败，deploy.sh 会忽略整批错误，
-- 新库直接跳过）；随后用 updated_at 回填，避免存量数据被误判过期
ALTER TABLE sync_data ADD COLUMN last_accessed INTEGER NOT NULL DEFAULT 0;
UPDATE sync_data SET last_accessed = updated_at WHERE last_accessed = 0;
