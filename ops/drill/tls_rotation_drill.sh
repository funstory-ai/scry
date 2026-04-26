#!/usr/bin/env bash
# P3-4: TLS / mTLS rotation checklist. Does not generate real certs unless RUN_LOCAL=1.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "=== TLS rotation drill (gRPC over TCP) ==="
echo "1) Prepare new server certificate + key (same CN/SAN as clients expect, or update clients)."
echo "2) If using client mTLS: prepare new client CA bundle for SCRYD_TLS_CLIENT_CA_PATH."
echo "3) Install new key material alongside old (different paths), update config to point at NEW paths."
echo "4) For each scryd instance: send SIGTERM, wait for graceful shutdown (transport.graceful_shutdown_timeout_secs), start with new paths."
echo "5) Verify clients reconnect with new trust store / client cert as needed."
echo "6) Retire old PEM files from disk once all instances and clients moved."
echo ""
echo "Env vars involved: SCRYD_TLS_CERT_PATH, SCRYD_TLS_KEY_PATH, SCRYD_TLS_CLIENT_CA_PATH (optional)."
echo "See docs/runbooks/tls-rotation.md for ordering details."
echo ""

if [[ "${RUN_LOCAL:-0}" != "1" ]]; then
  echo "Set RUN_LOCAL=1 to generate throwaway certs with openssl (no scryd start)."
  exit 0
fi

OUT="$(mktemp -d "${TMPDIR:-/tmp}/scry-tls-drill.XXXXXX")"

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "${OUT}/server.key" -out "${OUT}/server.crt" \
  -subj "/CN=localhost" -days 1 >/dev/null 2>&1

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "${OUT}/client.key" -out "${OUT}/client.crt" \
  -subj "/CN=scry-client" -days 1 >/dev/null 2>&1

cp "${OUT}/client.crt" "${OUT}/client-ca.crt"

echo "Generated sample PEMs under ${OUT} (self-signed, for local experiments only)."
echo "Example server env: SCRYD_TLS_CERT_PATH=${OUT}/server.crt SCRYD_TLS_KEY_PATH=${OUT}/server.key"
echo "Example fuse client: scry-fuse --tls --ca-cert ${OUT}/server.crt --client-cert ${OUT}/client.crt --client-key ${OUT}/client.key ..."
echo "When finished: rm -rf ${OUT}"
