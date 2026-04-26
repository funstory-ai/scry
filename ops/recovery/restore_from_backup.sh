#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: $0 <backup_db_path> <target_db_path> [--force]" >&2
  exit 1
fi

BACKUP_DB="$1"
TARGET_DB="$2"
FORCE_FLAG="${3:-}"

if [[ ! -f "$BACKUP_DB" ]]; then
  echo "backup database file not found: $BACKUP_DB" >&2
  exit 1
fi

if [[ "$FORCE_FLAG" != "" && "$FORCE_FLAG" != "--force" ]]; then
  echo "unknown third argument: $FORCE_FLAG (expected --force)" >&2
  exit 1
fi

if [[ -f "$TARGET_DB" && "$FORCE_FLAG" != "--force" ]]; then
  echo "target exists: $TARGET_DB (pass --force to overwrite)" >&2
  exit 1
fi

if [[ "$FORCE_FLAG" == "--force" ]]; then
  rm -f "$TARGET_DB" "${TARGET_DB}-wal" "${TARGET_DB}-shm"
fi

mkdir -p "$(dirname "$TARGET_DB")"
cp "$BACKUP_DB" "$TARGET_DB"

echo "restored backup to $TARGET_DB"
