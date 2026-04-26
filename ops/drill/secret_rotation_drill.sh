#!/usr/bin/env bash
# P3-4: documents + validates the three-step auth secret rotation (S1 → S2).
# This script does not mutate production; it prints the checklist and optionally
# exercises token verification against a throwaway scryd when RUN_LOCAL=1.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRYD_BIN="${SCRYD_BIN:-${ROOT}/target/debug/scryd}"
PROTO_IMPORT="${ROOT}/proto"
PROTO_FILE="scry/v1/workspace.proto"

echo "=== Secret rotation drill (auth HS256) ==="
echo "Phase A — current only: SCRYD_AUTH_SECRET=S1, SCRYD_AUTH_PREVIOUS_SECRETS empty"
echo "Phase B — overlap window: SCRYD_AUTH_SECRET=S2, SCRYD_AUTH_PREVIOUS_SECRETS=S1 (comma-separated list allowed)"
echo "Phase C — retire old: SCRYD_AUTH_SECRET=S2, SCRYD_AUTH_PREVIOUS_SECRETS empty"
echo ""
echo "Between phases: restart every scryd instance after updating env / systemd unit / k8s secret."
echo "Clients minted under S1 remain valid through Phase B only."
echo ""

if [[ "${RUN_LOCAL:-0}" != "1" ]]; then
  echo "Set RUN_LOCAL=1 to run a local throwaway verification (requires built scryd)."
  exit 0
fi

if [[ ! -f "${SCRYD_BIN}" ]]; then
  echo "scryd not found at ${SCRYD_BIN}; run: cargo build -p scryd" >&2
  exit 1
fi

S1="drill-s1-$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
S2="drill-s2-$(python3 -c 'import secrets; print(secrets.token_hex(6))')"

STAGING="$(mktemp -d "${TMPDIR:-/tmp}/scry-secret-drill.XXXXXX")"
cleanup() {
  if [[ -n "${PID:-}" ]]; then
    kill "${PID}" 2>/dev/null || true
    wait "${PID}" 2>/dev/null || true
  fi
  rm -rf "${STAGING}"
}
trap cleanup EXIT

CONTENT="${STAGING}/c"
INDEX_ROOT="${STAGING}/i"
mkdir -p "${CONTENT}" "${INDEX_ROOT}"
PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
ADDR="127.0.0.1:${PORT}"

run_phase() {
  local active="$1"
  local prev="$2"
  kill "${PID:-}" 2>/dev/null || true
  wait "${PID:-}" 2>/dev/null || true
  PID=
  SCRYD_DEV_MODE=1 \
    SCRYD_AUTH_SECRET="${active}" \
    SCRYD_AUTH_PREVIOUS_SECRETS="${prev}" \
    SCRYD_ALLOW_LOCAL_NO_AUTH=1 \
    SCRYD_CONTENT_ROOT="${CONTENT}" \
    SCRYD_INDEX_ROOT="${INDEX_ROOT}" \
    SCRYD_GRPC_ADDR="${ADDR}" \
    SCRYD_EMBED_PROVIDER=mock \
    SCRYD_CATALOG_BACKEND=sqlite \
    "${SCRYD_BIN}" &
  PID=$!
  for _ in $(seq 1 80); do
    if grpcurl -import-path "${PROTO_IMPORT}" -proto "${PROTO_FILE}" -plaintext "${ADDR}" list >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "scryd failed to listen on ${ADDR}" >&2
  exit 1
}

if ! command -v grpcurl >/dev/null 2>&1; then
  echo "grpcurl required for RUN_LOCAL=1" >&2
  exit 1
fi

run_phase "${S1}" ""
T_S1="$(SCRYD_AUTH_SECRET="${S1}" "${SCRYD_BIN}" token mint --kind admin --scope "admin.workspace.list" --ttl-secs 600 --secret "${S1}" | tr -d '\n')"

run_phase "${S2}" "${S1}"
if ! grpcurl -import-path "${PROTO_IMPORT}" -proto "${PROTO_FILE}" -plaintext \
  -H "authorization: Bearer ${T_S1}" -d '{"limit":1,"cursor":""}' "${ADDR}" scry.v1.Admin/ListWorkspaces >/dev/null; then
  echo "expected S1-signed admin token to work in overlap phase" >&2
  exit 1
fi

run_phase "${S2}" ""
if grpcurl -import-path "${PROTO_IMPORT}" -proto "${PROTO_FILE}" -plaintext \
  -H "authorization: Bearer ${T_S1}" -d '{"limit":1,"cursor":""}' "${ADDR}" scry.v1.Admin/ListWorkspaces >/dev/null 2>&1; then
  echo "expected S1-signed token to fail after previous secret removed" >&2
  exit 1
fi

echo "secret_rotation_drill: OK (local token lifecycle)"
