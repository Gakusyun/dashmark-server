# AGENTS.md

本文件供 AI 编码代理（以及人类贡献者）了解本仓库的结构与约定。

## 项目概述

dashmark-server 是 [DashMark](https://github.com/Gakusyun/Dashmark) 的云同步后端。
服务器只存储**加密后的 gzip 数据包**，不接触任何明文数据。同一套 HTTP API 有两种实现：

| 目录 | 运行时 | 存储 |
|---|---|---|
| `worker/` | Cloudflare Workers | D1（SQLite） |
| `go/` | Go 单二进制 | SQLite（纯 Go 驱动，无 CGO） |

## 目录结构

```
go/
  main.go        # Go 实现：net/http + database/sql (modernc.org/sqlite)
  go.mod / go.sum
worker/
  src/index.js   # Cloudflare Worker 实现（ES Module 语法）
  wrangler.toml  # wrangler 配置（D1 绑定）
  schema.sql     # D1 表结构（与 Go 版 SQLite 一致）
  deploy.sh      # 部署脚本，支持 D1_DATABASE_ID 等环境变量注入
```

## 关键约定

- **两种实现必须保持 API 与行为一致**：修改任一实现（路由、状态码、错误消息、
  响应格式）时，必须同步修改另一个。
- **表结构以 `worker/schema.sql` 为准**，`go/main.go` 中的建表语句需与之保持一致。
- 加密在客户端完成；服务端仅存 `verifier`（PBKDF2 口令校验值）用于"认领"比对，
  严禁在服务端实现任何解密逻辑。
- `payload_hash` 用于内容去重：内容一致时不重复写入。
- Go 代码：单文件 `main.go`，标准库 + `modernc.org/sqlite`，避免引入新依赖。
- Worker 代码：纯 JavaScript（无构建步骤），依赖 wrangler 部署。

## 常用命令

```bash
# Go 版
cd go && go build ./... && go vet ./...
go run main.go            # 默认监听 :8787，可用 PORT 覆盖

# Worker 版
cd worker
npx wrangler dev          # 本地开发
./deploy.sh               # 部署（读取 D1_DATABASE_ID 等环境变量）
```

## 提交规范

使用 Conventional Commits（`feat:`、`fix:`、`docs:` 等），中文描述。
