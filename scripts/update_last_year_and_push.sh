#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
PYTHON_EXPLICIT="${PYTHON_BIN:+yes}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ " ${*:-} " == *" --help "* || " ${*:-} " == *" -h "* ]]; then
  exec "$PYTHON_BIN" "$REPO_ROOT/scripts/archive_updater.py" --help
fi

if [[ -z "$PYTHON_EXPLICIT" ]] && ! "$PYTHON_BIN" -c \
  'import ssl; ssl.create_default_context()' >/dev/null 2>&1; then
  for candidate in python3.15 python3.14 python3.13 python3.12 python3.11 python3.10; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c \
      'import ssl; ssl.create_default_context()' >/dev/null 2>&1; then
      PYTHON_BIN="$candidate"
      echo "Default Python HTTPS support is unavailable; using $PYTHON_BIN." >&2
      break
    fi
  done
fi

"$PYTHON_BIN" - <<'PY'
import sys

try:
    import ssl
    ssl.create_default_context()
except (ImportError, OSError) as exc:
    print(
        f"Updater cannot use HTTPS with {sys.executable}: {exc}\n"
        "Repair this Python installation or set PYTHON_BIN to an interpreter "
        "with working SSL support.",
        file=sys.stderr,
    )
    raise SystemExit(1)
PY

cd "$REPO_ROOT"
git pull --ff-only

exec "$PYTHON_BIN" scripts/archive_updater.py \
  --contests all \
  --last 1 \
  --publish \
  "$@"
