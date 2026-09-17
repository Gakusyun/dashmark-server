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
      const row = await env.DB.prepare('SELECT updated_at FROM sync_data WHERE id = ?')
        .bind(id)
        .first();
      if (!row) return json({ error: 'not found' }, 404);
      return json({ updatedAt: row.updated_at });
    }

    // ---------- GET /sync/:id ----------
    if (request.method === 'GET') {
      if (!rateLimit(ip, false)) return json({ error: 'rate limited' }, 429);
      const row = await env.DB.prepare(
        'SELECT verifier, payload, updated_at FROM sync_data WHERE id = ?'
      )
        .bind(id)
        .first();
      if (!row) return json({ error: 'not found' }, 404);
      const verifier = request.headers.get('X-Dashmark-Verifier') || '';
      if (row.verifier !== verifier) return json({ error: 'verifier mismatch' }, 403);
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
        `INSERT INTO sync_data (id, verifier, payload, payload_hash, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           verifier = excluded.verifier,
           payload = excluded.payload,
           payload_hash = excluded.payload_hash,
           updated_at = excluded.updated_at`
      )
        .bind(id, verifier, new Uint8Array(body), payloadHash, now)
        .run();

      return json({ updatedAt: now });
    }

    return json({ error: 'method not allowed' }, 405);
  },
};
