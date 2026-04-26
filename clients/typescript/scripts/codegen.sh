#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "${ROOT}/../.." && pwd)"
PROTO_DIR="${REPO}/proto"
OUT="${ROOT}/src/gen"
mkdir -p "${OUT}"
PROTOC="$(npm root)/grpc-tools/bin/protoc"
PLUGIN="$(npm root)/.bin/protoc-gen-ts_proto"
"${PROTOC}" \
  -I"${PROTO_DIR}" \
  --plugin=protoc-gen-ts_proto="${PLUGIN}" \
  --ts_proto_out="${OUT}" \
  --ts_proto_opt=esModuleInterop=true,outputServices=grpc-js,stringEnums=true,useOptionals=messages,forceLong=bigint \
  "${PROTO_DIR}/scry/v1/workspace.proto"
