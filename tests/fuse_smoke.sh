#!/usr/bin/env bash
# FUSE smoke: requires scryd reachable at SCRY_FUSE_GRPC_ADDR, workspace at SCRY_FUSE_WORKSPACE_ID,
# bearer token at SCRY_FUSE_TOKEN (or file SCRY_FUSE_TOKEN_FILE), and empty mount dir SCRY_FUSE_MOUNT.
# Set SCRY_FUSE_USER_MOUNT=1 for unprivileged CI (passes scry-fuse --user-mount).
set -euo pipefail

: "${SCRY_FUSE_GRPC_ADDR:?set SCRY_FUSE_GRPC_ADDR (e.g. 127.0.0.1:50051)}"
: "${SCRY_FUSE_WORKSPACE_ID:?set SCRY_FUSE_WORKSPACE_ID}"
: "${SCRY_FUSE_MOUNT:?set SCRY_FUSE_MOUNT to an empty directory}"

if [[ -n "${SCRY_FUSE_TOKEN_FILE:-}" ]]; then
  TOKEN_FILE=(--auth-token-file "${SCRY_FUSE_TOKEN_FILE}")
elif [[ -n "${SCRY_FUSE_TOKEN:-}" ]]; then
  TOKEN_FILE=(--auth-token "${SCRY_FUSE_TOKEN}")
else
  echo "set SCRY_FUSE_TOKEN or SCRY_FUSE_TOKEN_FILE" >&2
  exit 1
fi

FUSE_BIN="${SCRY_FUSE_BIN:-$(command -v scry-fuse || true)}"
if [[ -z "${FUSE_BIN}" || ! -x "${FUSE_BIN}" ]]; then
  echo "scry-fuse not found; set SCRY_FUSE_BIN to the binary path" >&2
  exit 1
fi

USER_MOUNT=()
if [[ "${SCRY_FUSE_USER_MOUNT:-}" == "1" ]]; then
  USER_MOUNT=(--user-mount)
fi

cleanup() {
  # After the FUSE process exits, `mountpoint` can be a false negative; always try unmount.
  fusermount3 -u "${SCRY_FUSE_MOUNT}" 2>/dev/null || true
}
trap cleanup EXIT

rm -f "${SCRY_FUSE_MOUNT}/.fuse_smoke.txt"
"${FUSE_BIN}" \
  --server-addr "${SCRY_FUSE_GRPC_ADDR}" \
  "${TOKEN_FILE[@]}" \
  --workspace-id "${SCRY_FUSE_WORKSPACE_ID}" \
  --mountpoint "${SCRY_FUSE_MOUNT}" \
  "${USER_MOUNT[@]}" \
  &
FUSE_PID=$!

for _ in $(seq 1 50); do
  if mountpoint -q "${SCRY_FUSE_MOUNT}" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if ! mountpoint -q "${SCRY_FUSE_MOUNT}" 2>/dev/null; then
  echo "mount did not become ready" >&2
  kill "${FUSE_PID}" 2>/dev/null || true
  wait "${FUSE_PID}" 2>/dev/null || true
  exit 1
fi

echo "fuse smoke $(date -Iseconds)" >"${SCRY_FUSE_MOUNT}/.fuse_smoke.txt"
CONTENT=$(cat "${SCRY_FUSE_MOUNT}/.fuse_smoke.txt")
[[ "${CONTENT}" == fuse\ smoke* ]]

if command -v rg >/dev/null 2>&1; then
  rg -n "fuse smoke" "${SCRY_FUSE_MOUNT}/.fuse_smoke.txt"
else
  grep -n "fuse smoke" "${SCRY_FUSE_MOUNT}/.fuse_smoke.txt"
fi

kill "${FUSE_PID}"
wait "${FUSE_PID}" 2>/dev/null || true
cleanup
trap - EXIT
