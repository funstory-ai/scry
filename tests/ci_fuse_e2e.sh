#!/usr/bin/env bash
# CI helper: build scryd + scry-fuse (fuse), seed workspace, run tests/fuse_smoke.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

if [[ ! -e /dev/fuse ]]; then
  echo "skip fuse e2e: /dev/fuse not present"
  exit 0
fi

export PATH="${ROOT}/target/debug:${PATH}"

cargo build -p scryd -q
cargo build -p scry-fuse --features fuse -q

if ! command -v grpcurl >/dev/null 2>&1; then
  echo "grpcurl required for fuse CI seed" >&2
  exit 1
fi

STAGING="$(mktemp -d "${TMPDIR:-/tmp}/scry-fuse-ci.XXXXXX")"
cleanup() {
  if [[ -n "${SCRYD_PID:-}" ]]; then
    kill "${SCRYD_PID}" 2>/dev/null || true
    wait "${SCRYD_PID}" 2>/dev/null || true
  fi
  if [[ -n "${MOUNT:-}" ]]; then
    fusermount3 -u "${MOUNT}" 2>/dev/null || true
  fi
  rm -rf "${STAGING}"
}
trap cleanup EXIT

CONTENT="${STAGING}/content"
INDEX_ROOT="${STAGING}/index"
MOUNT="${STAGING}/mnt"
mkdir -p "${CONTENT}" "${INDEX_ROOT}" "${MOUNT}"

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
ADDR="127.0.0.1:${PORT}"
SECRET="fuse-ci-secret-not-default-$(python3 -c 'import secrets; print(secrets.token_hex(8))')"

SCRYD_DEV_MODE=1 \
  SCRYD_AUTH_SECRET="${SECRET}" \
  SCRYD_ALLOW_LOCAL_NO_AUTH=1 \
  SCRYD_CONTENT_ROOT="${CONTENT}" \
  SCRYD_INDEX_ROOT="${INDEX_ROOT}" \
  SCRYD_GRPC_ADDR="${ADDR}" \
  SCRYD_EMBED_PROVIDER=mock \
  SCRYD_CATALOG_BACKEND=sqlite \
  scryd &
SCRYD_PID=$!

PROTO_IMPORT="${ROOT}/proto"
PROTO_FILE="scry/v1/workspace.proto"

for _ in $(seq 1 80); do
  if grpcurl -import-path "${PROTO_IMPORT}" -proto "${PROTO_FILE}" -plaintext "${ADDR}" list >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done

ADMIN_TOKEN="$(SCRYD_AUTH_SECRET="${SECRET}" scryd token mint \
  --kind admin \
  --scope "admin.workspace.create admin.workspace.list" \
  --ttl-secs 3600 \
  --secret "${SECRET}" | tr -d '\n')"

CREATE_JSON='{"name":"fuse-ci","config":{"embedding_model":"","embedding_dim":32,"max_file_size":0}}'
CREATE_OUT="$(grpcurl -import-path "${PROTO_IMPORT}" -proto "${PROTO_FILE}" -plaintext \
  -H "authorization: Bearer ${ADMIN_TOKEN}" \
  -d "${CREATE_JSON}" \
  "${ADDR}" scry.v1.Admin/CreateWorkspace)"
WS_ID="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["workspaceId"])' "${CREATE_OUT}")"

WS_TOKEN="$(SCRYD_AUTH_SECRET="${SECRET}" scryd token mint \
  --kind workspace \
  --workspace-id "${WS_ID}" \
  --scope "workspace.access" \
  --ttl-secs 3600 \
  --secret "${SECRET}" | tr -d '\n')"

export SCRY_FUSE_GRPC_ADDR="${ADDR}"
export SCRY_FUSE_WORKSPACE_ID="${WS_ID}"
export SCRY_FUSE_TOKEN="${WS_TOKEN}"
export SCRY_FUSE_MOUNT="${MOUNT}"
export SCRY_FUSE_BIN="${ROOT}/target/debug/scry-fuse"
# Unprivileged CI: skip kernel default_permissions so the test uid can create files.
export SCRY_FUSE_USER_MOUNT=1

bash "${ROOT}/tests/fuse_smoke.sh"

echo "fuse e2e: OK"
