#!/usr/bin/env bash
# DashMark 云同步 Worker 部署脚本
#
# 用法：
#   D1_DATABASE_ID=xxxx ./deploy.sh
#
# 可选环境变量：
#   D1_DATABASE_NAME   D1 数据库名称（默认 dashmark-sync）
#   WORKER_NAME        Worker 名称（默认 dashmark-sync）
#
# wrangler.toml 不支持读取环境变量，因此这里先注入再执行 wrangler deploy。
set -euo pipefail
cd "$(dirname "$0")"

: "${D1_DATABASE_ID:?请先设置 D1_DATABASE_ID（npx wrangler d1 create 后返回的 database_id）}"
D1_DATABASE_NAME="${D1_DATABASE_NAME:-dashmark-sync}"
WORKER_NAME="${WORKER_NAME:-dashmark-sync}"

cp wrangler.toml wrangler.toml.bak
trap 'mv wrangler.toml.bak wrangler.toml' EXIT

sed -i \
  -e "s|^name = .*|name = \"${WORKER_NAME}\"|" \
  -e "s|database_name = .*|database_name = \"${D1_DATABASE_NAME}\"|" \
  -e "s|database_id = .*|database_id = \"${D1_DATABASE_ID}\"|" \
  wrangler.toml

if [ "${SKIP_MIGRATION:-0}" != "1" ]; then
  echo ">> 初始化 D1 表结构..."
  npx wrangler d1 execute "${D1_DATABASE_NAME}" --remote --file=./schema.sql -y || \
    echo ">> 表结构可能已存在，跳过（如失败请手动执行 schema.sql）"
fi

echo ">> 部署..."
npx wrangler deploy

echo ">> 完成。请把 Worker 地址填入前端 VITE_SYNC_SERVER。"
