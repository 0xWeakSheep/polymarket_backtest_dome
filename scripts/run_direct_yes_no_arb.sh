#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -d ".venv" ]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

if [[ -z "${DOME_API_KEY:-}" ]]; then
  echo "Missing DOME_API_KEY" >&2
  exit 1
fi

export PYTHONUNBUFFERED=1

exec python3 -u -m src.research.direct_yes_no_arb.analyze_direct_arb --resume "$@"
