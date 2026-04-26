#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <source-index.db> <backup-output.db>" >&2
  exit 1
fi

SOURCE_DB="$1"
BACKUP_DB="$2"

if [[ ! -f "$SOURCE_DB" ]]; then
  echo "source database not found: $SOURCE_DB" >&2
  exit 1
fi

mkdir -p "$(dirname "$BACKUP_DB")"

sqlite3 "$SOURCE_DB" "VACUUM INTO '$BACKUP_DB';"
echo "backup created: $BACKUP_DB"

if command -v sha256sum >/dev/null 2>&1; then
  echo -n "sha256 "
  sha256sum "$BACKUP_DB" | awk '{print $1}'
elif command -v shasum >/dev/null 2>&1; then
  echo -n "sha256 "
  shasum -a 256 "$BACKUP_DB" | awk '{print $1}'
else
  echo "checksum: skipped (install sha256sum or shasum)" >&2
fi
