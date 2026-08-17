#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ " ${*:-} " == *" --help "* || " ${*:-} " == *" -h "* ]]; then
  exec "$PYTHON_BIN" "$REPO_ROOT/scripts/archive_updater.py" --help
fi

cd "$REPO_ROOT"
git pull --ff-only

exec "$PYTHON_BIN" scripts/archive_updater.py \
  --contests all \
  --last 1 \
  --publish \
  "$@"
