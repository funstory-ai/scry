#!/usr/bin/env bash
# P3-3: backup SQLite index → destroy data dir → restore → start scryd → smoke (Admin + Search).
# Dependencies: grpcurl, openssl; optional jq (otherwise python3 for trivial JSON parse).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROTO_IMPORT="${ROOT}/proto"
PROTO_FILE="scry/v1/workspace.proto"
SCRYD_BIN="${SCRYD_BIN:-${ROOT}/target/debug/scryd}"
VACUUM_BACKUP="${ROOT}/ops/backup/vacuum_backup.sh"
RESTORE_DB="${ROOT}/ops/recovery/restore_from_backup.sh"

if [[ ! -x "$(command -v grpcurl || true)" ]]; then
  echo "grpcurl is required" >&2
  exit 1
fi

if [[ ! -f "${SCRYD_BIN}" ]]; then
  echo "scryd not found at ${SCRYD_BIN}; run: cargo build -p scryd" >&2
  exit 1
fi

if [[ ! -f "${ROOT}/proto/${PROTO_FILE}" ]]; then
  echo "missing proto ${ROOT}/proto/${PROTO_FILE}" >&2
  exit 1
fi

free_tcp_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

grpc() {
  grpcurl -import-path "${PROTO_IMPORT}" -proto "${PROTO_FILE}" "$@"
}

STAGING="$(mktemp -d "${TMPDIR:-/tmp}/scry-restore-drill.XXXXXX")"
cleanup() {
  if [[ -n "${SCRYD_PID:-}" ]]; then
    kill "${SCRYD_PID}" 2>/dev/null || true
    wait "${SCRYD_PID}" 2>/dev/null || true
  fi
  rm -rf "${STAGING}"
}
trap cleanup EXIT

CONTENT="${STAGING}/content"
INDEX_ROOT="${STAGING}/index"
CATALOG_DB="${INDEX_ROOT}/_catalog.db"
BACKUP_DIR="${STAGING}/backup"
mkdir -p "${CONTENT}" "${INDEX_ROOT}" "${BACKUP_DIR}"

PORT="$(free_tcp_port)"
ADDR="127.0.0.1:${PORT}"
SECRET="$(openssl rand -hex 16)"

start_scryd() {
  SCRYD_DEV_MODE=1 \
    SCRYD_AUTH_SECRET="${SECRET}" \
    SCRYD_ALLOW_LOCAL_NO_AUTH=1 \
    SCRYD_CONTENT_ROOT="${CONTENT}" \
    SCRYD_INDEX_ROOT="${INDEX_ROOT}" \
    SCRYD_GRPC_ADDR="${ADDR}" \
    SCRYD_EMBED_PROVIDER=mock \
    SCRYD_CATALOG_BACKEND=sqlite \
    "${SCRYD_BIN}" &
  SCRYD_PID=$!
}

wait_grpc() {
  for _ in $(seq 1 80); do
    if grpc -plaintext "${ADDR}" list >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "scryd did not become ready on ${ADDR}" >&2
  return 1
}

stop_scryd() {
  if [[ -n "${SCRYD_PID:-}" ]]; then
    kill "${SCRYD_PID}" 2>/dev/null || true
    wait "${SCRYD_PID}" 2>/dev/null || true
  fi
  SCRYD_PID=
}

mint_admin_token() {
  SCRYD_AUTH_SECRET="${SECRET}" "${SCRYD_BIN}" token mint \
    --kind admin \
    --scope "admin.workspace.create admin.workspace.list admin.workspace.stats" \
    --ttl-secs 3600 \
    --secret "${SECRET}" | tr -d '\n'
}

mint_workspace_token() {
  local ws="$1"
  SCRYD_AUTH_SECRET="${SECRET}" "${SCRYD_BIN}" token mint \
    --kind workspace \
    --workspace-id "${ws}" \
    --scope "workspace.access" \
    --ttl-secs 3600 \
    --secret "${SECRET}" | tr -d '\n'
}

start_scryd
wait_grpc

ADMIN_TOKEN="$(mint_admin_token)"
CREATE_JSON='{"name":"restore-drill","config":{"embedding_model":"","embedding_dim":32,"max_file_size":0}}'
CREATE_OUT="$(grpc -plaintext -H "authorization: Bearer ${ADMIN_TOKEN}" -d "${CREATE_JSON}" "${ADDR}" scry.v1.Admin/CreateWorkspace)"
if command -v jq >/dev/null 2>&1; then
  WS_ID="$(printf '%s' "${CREATE_OUT}" | jq -r .workspaceId)"
  ROOT_ID="$(printf '%s' "${CREATE_OUT}" | jq -r .rootNodeId)"
else
  WS_ID="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["workspaceId"])' "${CREATE_OUT}")"
  ROOT_ID="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["rootNodeId"])' "${CREATE_OUT}")"
fi

WS_TOKEN="$(mint_workspace_token "${WS_ID}")"

# UTF-8 phrase for hybrid search smoke
PHRASE="restore-drill-unique-phrase-9f3c"
B64_CONTENT="$(printf '%s' "${PHRASE}" | base64 -w0 2>/dev/null || printf '%s' "${PHRASE}" | base64)"
PUT_JSON="$(printf '{"workspace_id":"%s","put_file_path":{"parent_node_id":"%s","name":"drill.md"},"content":"%s","if_version":0,"mode":420}' "${WS_ID}" "${ROOT_ID}" "${B64_CONTENT}")"
grpc -plaintext -H "authorization: Bearer ${WS_TOKEN}" -d "${PUT_JSON}" "${ADDR}" scry.v1.Files/PutFile >/dev/null

search_hits() {
  grpc -plaintext -H "authorization: Bearer ${WS_TOKEN}" -d "{\"workspace_id\":\"${WS_ID}\",\"query\":\"${PHRASE}\",\"mode\":3,\"limit\":10}" "${ADDR}" scry.v1.Search/Search
}

FOUND=0
for _ in $(seq 1 60); do
  OUT="$(search_hits || true)"
  if printf '%s' "${OUT}" | grep -q 'drill.md'; then
    FOUND=1
    break
  fi
  sleep 0.1
done
if [[ "${FOUND}" != 1 ]]; then
  echo "pre-backup search smoke failed" >&2
  exit 1
fi

LIST_OUT="$(grpc -plaintext -H "authorization: Bearer ${ADMIN_TOKEN}" -d '{"limit":50,"cursor":""}' "${ADDR}" scry.v1.Admin/ListWorkspaces)"
if ! printf '%s' "${LIST_OUT}" | grep -q "${WS_ID}"; then
  echo "admin list smoke failed before backup" >&2
  exit 1
fi

stop_scryd

WS_INDEX="${INDEX_ROOT}/${WS_ID}/index.db"
bash "${VACUUM_BACKUP}" "${WS_INDEX}" "${BACKUP_DIR}/${WS_ID}.bak.db"
bash "${VACUUM_BACKUP}" "${CATALOG_DB}" "${BACKUP_DIR}/_catalog.bak.db"
tar -C "${STAGING}" -cf "${STAGING}/content.tar" "$(basename "${CONTENT}")"

rm -rf "${INDEX_ROOT}" "${CONTENT}"
mkdir -p "${INDEX_ROOT}/${WS_ID}" "${CONTENT}"

bash "${RESTORE_DB}" "${BACKUP_DIR}/${WS_ID}.bak.db" "${WS_INDEX}" --force
bash "${RESTORE_DB}" "${BACKUP_DIR}/_catalog.bak.db" "${CATALOG_DB}" --force
tar -C "${STAGING}" -xf "${STAGING}/content.tar"

start_scryd
wait_grpc

ADMIN_TOKEN="$(mint_admin_token)"
LIST_OUT="$(grpc -plaintext -H "authorization: Bearer ${ADMIN_TOKEN}" -d '{"limit":50,"cursor":""}' "${ADDR}" scry.v1.Admin/ListWorkspaces)"
if ! printf '%s' "${LIST_OUT}" | grep -q "${WS_ID}"; then
  echo "admin list smoke failed after restore" >&2
  exit 1
fi

WS_TOKEN="$(mint_workspace_token "${WS_ID}")"
FOUND=0
for _ in $(seq 1 60); do
  OUT="$(search_hits || true)"
  if printf '%s' "${OUT}" | grep -q 'drill.md'; then
    FOUND=1
    break
  fi
  sleep 0.1
done
if [[ "${FOUND}" != 1 ]]; then
  echo "post-restore search smoke failed" >&2
  exit 1
fi

echo "restore_drill: OK (${ADDR})"
