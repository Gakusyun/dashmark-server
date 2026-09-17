/**
 * DashMark 云同步 · Cloudflare Worker 实现（D1 存储）
 *
 * 只存储加密后的 gzip 数据包，接口契约见仓库根 README.md。
 */

const MAX_BODY = 1 * 1024 * 1024; // 1 MiB

// 尽力而为的内存限流：每 IP 每 60 秒
const RATE_WINDOW_MS = 60_000;
const RATE_READ = 60;
const RATE_WRITE = 10;
/** @type {Map<string, { count: number, reset: number }>} */
const rateBuckets = new Map();

// ==================== 惰性清理（垃圾回收） ====================

const CLEAN_INTERVAL_MS = 24 * 3600_000; // 清理冷却：24h 内不重复清理，减少 D1 写消耗
const EXPIRE_MS = 30 * 24 * 3600_000; // 过期阈值：1 个月未访问即删除
const MAX_ROWS = 50; // 最大保留条数，超出淘汰最久未访问
const ACCESS_REFRESH_MS = 20 * 3600_000; // 访问刷新冷却：20h 内重复读取不更新 last_accessed

// 每个隔离实例只尝试一次迁移（老库补 last_accessed 列并回填）
let schemaReady = false;
async function ensureSchema(db) {
  if (schemaReady) return;
  schemaReady = true;
  try {
    await db.prepare('SELECT last_accessed FROM sync_data LIMIT 1').first();
  } catch {
    try {
      await db.batch([
        db.prepare('ALTER TABLE sync_data ADD COLUMN last_accessed INTEGER NOT NULL DEFAULT 0'),
        db.prepare('UPDATE sync_data SET last_accessed = updated_at WHERE last_accessed = 0'),
      ]);
    } catch {
      /* 并发迁移或已存在，忽略 */
    }
  }
}

/**
 * 惰性清理：每次请求进入时尝试执行，24h 冷却期内直接跳过。
 * 任何失败都不影响业务请求。
 * @param {D1Database} db
 */
async function lazyClean(db) {
  try {
    await ensureSchema(db);
    const now = Date.now();
    const meta = await db.prepare("SELECT last_clean_at FROM job_meta WHERE key = 'asset_clean'").first();
    if (!meta) {
      await db.prepare("INSERT OR IGNORE INTO job_meta(key, last_clean_at) VALUES('asset_clean', 0)").run();
    } else if (now - meta.last_clean_at < CLEAN_INTERVAL_MS) {
      return; // 冷却期内，跳过
    }
    await db.batch([
      // ① 删除超过 1 个月未访问的记录
      db.prepare('DELETE FROM sync_data WHERE last_accessed < ?').bind(now - EXPIRE_MS),
      // ② 记录总数超过上限时，淘汰最久未访问的条目
      db.prepare(
        'DELETE FROM sync_data WHERE id NOT IN (SELECT id FROM sync_data ORDER BY last_accessed DESC LIMIT ?)'
      ).bind(MAX_ROWS),
      db.prepare("UPDATE job_meta SET last_clean_at = ? WHERE key = 'asset_clean'").bind(now),
    ]);
  } catch {
    /* 清理失败静默忽略，下次请求再试 */
  }
}

function rateLimit(ip, isWrite) {
  const now = Date.now();
  const entry = rateBuckets.get(ip);
  const limit = isWrite ? RATE_WRITE : RATE_READ;
  if (!entry || now > entry.reset) {
    if (rateBuckets.size > 10_000) rateBuckets.clear();
    rateBuckets.set(ip, { count: 1, reset: now + RATE_WINDOW_MS });
    return true;
  }
  if (entry.count >= limit) return false;
  entry.count += 1;
  return true;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
}

export default {
  /**
   * @param {Request} request
   * @param {{ DB: D1Database }} env
   */
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/sync\/([^/]+)(\/meta)?$/);
    if (!match) return json({ error: 'not found' }, 404);

    const id = decodeURIComponent(match[1]);
    const isMeta = Boolean(match[2]);
    const ip =
      request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For') || 'unknown';

    // ---------- GET /sync/:id/meta ----------
    if (isMeta && request.method === 'GET') {
      if (!rateLimit(ip, false)) return json({ error: 'rate limited' }, 429);
      lazyClean(env.DB); // 不 await，不阻塞响应
      const row = await env.DB.prepare('SELECT updated_at FROM sync_data WHERE id = ?')
        .bind(id)
        .first();
      if (!row) return json({ error: 'not found' }, 404);
      return json({ updatedAt: row.updated_at });
    }

    // ---------- GET /sync/:id ----------
    if (request.method === 'GET') {
      if (!rateLimit(ip, false)) return json({ error: 'rate limited' }, 429);
      lazyClean(env.DB); // 不 await，不阻塞响应
      const row = await env.DB.prepare(
        'SELECT verifier, payload, updated_at, last_accessed FROM sync_data WHERE id = ?'
      )
        .bind(id)
        .first();
      if (!row) return json({ error: 'not found' }, 404);
      const verifier = request.headers.get('X-Dashmark-Verifier') || '';
      if (row.verifier !== verifier) return json({ error: 'verifier mismatch' }, 403);
      // 访问时间惰性更新：20h 内重复读取不写库，减少 D1 写消耗
      if (Date.now() - row.last_accessed > ACCESS_REFRESH_MS) {
        env.DB.prepare('UPDATE sync_data SET last_accessed = ? WHERE id = ?').bind(Date.now(), id).run();
      }
      return new Response(row.payload, {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'x-updated-at': String(row.updated_at),
        },
      });
    }

    // ---------- PUT /sync/:id ----------
    if (request.method === 'PUT') {
      if (!rateLimit(ip, true)) return json({ error: 'rate limited' }, 429);
      lazyClean(env.DB); // 不 await，不阻塞响应
      const verifier = request.headers.get('X-Dashmark-Verifier') || '';
      if (!id || !verifier) return json({ error: 'missing id or verifier' }, 400);

      const body = await request.arrayBuffer();
      if (body.byteLength === 0) return json({ error: 'empty body' }, 400);
      if (body.byteLength > MAX_BODY) return json({ error: 'too large' }, 413);

      // 优先使用客户端声明的明文哈希（密文带随机 IV，每次不同，
      // 对密文做哈希无法判定内容一致）；缺失时退化为对请求体做哈希
      let payloadHash = request.headers.get('X-Dashmark-Hash') || '';
      if (!/^[0-9a-f]{64}$/.test(payloadHash)) {
        const digest = await crypto.subtle.digest('SHA-256', body);
        payloadHash = [...new Uint8Array(digest)]
          .map((b) => b.toString(16).padStart(2, '0'))
          .join('');
      }

      const existing = await env.DB.prepare(
        'SELECT verifier, payload_hash, updated_at FROM sync_data WHERE id = ?'
      )
        .bind(id)
        .first();

      if (existing) {
        if (existing.verifier !== verifier) return json({ error: 'id taken' }, 403);
        if (existing.payload_hash === payloadHash) {
          return json({ updatedAt: existing.updated_at, unchanged: true });
        }
      }

      const now = Date.now();
      await env.DB.prepare(
        `INSERT INTO sync_data (id, verifier, payload, payload_hash, updated_at, last_accessed)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           verifier = excluded.verifier,
           payload = excluded.payload,
           payload_hash = excluded.payload_hash,
           updated_at = excluded.updated_at,
           last_accessed = excluded.last_accessed`
      )
        .bind(id, verifier, new Uint8Array(body), payloadHash, now, now)
        .run();

      return json({ updatedAt: now });
    }

    return json({ error: 'method not allowed' }, 405);
  },
};
