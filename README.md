# dashmark-server

[DashMark](https://github.com/Gakusyun/Dashmark) 的云同步后端。服务器只存储**加密后的 gzip 数据包**，看不到用户的任何明文数据。

同一套 HTTP API 提供两种实现：

| 目录 | 运行时 | 存储 | 适用场景 |
|---|---|---|---|
| `worker/` | Cloudflare Workers | D1（SQLite） | 官方公用部署 / 一键 fork 自部署 |
| `go/` | Go（单二进制） | SQLite（纯 Go 驱动，无 CGO） | 自有云服务器自部署，内存占用极低 |

### 相关环境变量

| 位置 | 变量 | 说明 |
|---|---|---|
| 前端构建 | `VITE_SYNC_SERVER` | 官方同步服务器地址，注入后替代前端内置默认值 |
| `worker/deploy.sh` | `D1_DATABASE_ID` | D1 数据库 ID（必需） |

同步服务器内置惰性清理（过期删除 + 50 条上限 + 读写冷却），详见下文「数据生命周期」。
| `worker/deploy.sh` | `D1_DATABASE_NAME` / `WORKER_NAME` | 数据库名 / Worker 名（可选） |

## 加密模型

客户端（浏览器）在本地完成全部加密：

1. 用户输入 **同步 ID** 与 **口令**（两者均由用户自定，忘记即数据永久丢失）。
2. 使用 PBKDF2-SHA256（150000 次迭代）派生两个密钥，盐为确定性字符串：
   - `AES 密钥 = PBKDF2(口令, 盐 = ID + ":enc")` — 用于 AES-256-GCM 加解密，**永不离开浏览器**；
   - `口令校验值 = PBKDF2(口令, 盐 = ID + ":ver")` — 十六进制字符串，随请求发送，服务器存它做"认领"比对。
3. 上传内容：`JSON 整包 → gzip → AES-256-GCM（密文格式：12 字节 IV + 密文 + 16 字节认证标签）`。

相同 ID + 口令在任何设备上派生出相同密钥，即可互相同步。

## 表结构（D1 与 SQLite 一致）

```sql
CREATE TABLE IF NOT EXISTS sync_data (
  id            TEXT PRIMARY KEY,   -- 同步 ID
  verifier      TEXT NOT NULL,      -- PBKDF2 口令校验值（十六进制）
  payload       BLOB NOT NULL,      -- 加密后的 gzip 数据
  payload_hash  TEXT NOT NULL,      -- payload 的 SHA-256（十六进制），内容一致时不写入
  updated_at    INTEGER NOT NULL,   -- 最后修改时间（Unix 毫秒）
  last_accessed INTEGER NOT NULL DEFAULT 0  -- 最后访问时间（Unix 毫秒），惰性清理依据
);

CREATE TABLE IF NOT EXISTS job_meta (
  key           TEXT PRIMARY KEY,
  last_clean_at INTEGER NOT NULL     -- 惰性清理上次执行时间
);
```

## 数据生命周期（惰性清理）

无定时任务，每次接口请求进入时尝试触发清理（失败静默忽略，不影响业务）：

- **清理冷却**：距上次清理不足 24 小时则跳过，避免频繁删除消耗 D1 写额度；
- **过期删除**：删除 `last_accessed` 超过 **1 个月** 的记录；
- **数量上限**：总记录数超过 **50 条** 时，淘汰最久未访问的条目；
- **访问刷新**：读取成功且距上次访问超过 **20 小时** 才更新 `last_accessed`（20 小时内重复读取不写库）；修改内容（PUT）时强制刷新。

风险说明：若服务长期无任何请求，过期记录会留在库内；但单条仅 KB 级、总量远低于 D1 免费 5GB 上限，不会产生费用，下次任意请求即一次性清完。

## HTTP API

校验值通过请求头传递：`X-Dashmark-Verifier: <十六进制校验值>`。

### `GET /sync/:id/meta`

查询该 ID 的最后修改时间。不需要校验值。

- `200` → `{"updatedAt": 1700000000000}`
- `404` → 该 ID 尚未上传过

### `GET /sync/:id`

下载加密数据包。

- `200` → 响应体为二进制密文，`X-Updated-At: <毫秒时间戳>` 响应头携带修改时间
- `403` → 校验值不匹配
- `404` → ID 不存在

### `PUT /sync/:id`

上传加密数据包（请求体为二进制密文，上限 1 MiB）。

- 首次上传：写入并绑定校验值（"认领"该 ID）→ `200` `{"updatedAt": ...}`
- 已存在且校验值不匹配 → `403` `{"error": "id taken"}`
- 已存在且 `payload_hash` 与请求体一致 → 不写入，`200` `{"updatedAt": <原值>, "unchanged": true}`
- 正常覆盖 → `200` `{"updatedAt": <新值>}`
- 超过大小限制 → `413`

### 限流

两种实现均为**实例内存计数**（尽力而为）：每 IP 每 60 秒最多 10 次写入、60 次读取，超出返回 `429`。不引入额外组件，重启或换实例即重置。

## 部署

### Cloudflare Worker

```bash
cd worker
chmod +x deploy.sh
npx wrangler d1 create dashmark-sync   # 复制返回的 database_id
D1_DATABASE_ID=<database_id> ./deploy.sh
```

可选环境变量：`D1_DATABASE_NAME`（默认 `dashmark-sync`）、`WORKER_NAME`（默认 `dashmark-sync`）、`SKIP_MIGRATION=1`（跳过建表）。

### Go

```bash
cd go
go build -o dashmark-server .
./dashmark-server -addr :8787 -db ./sync.db
```

数据目录只有一个 SQLite 文件，备份即拷贝。建议反代（Nginx/Caddy）加 HTTPS 后对外暴露。
