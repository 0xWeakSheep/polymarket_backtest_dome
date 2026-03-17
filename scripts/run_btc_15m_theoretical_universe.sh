#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -d ".venv" ]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

export PYTHONUNBUFFERED=1

ARGS=()
if [ -f "data/processed/btc_15m_theoretical_value/universe/progress.json" ]; then
  ARGS+=(--resume)
fi

exec python3 -u -m src.research.btc_15m_theoretical_value.fetch_market_universe "${ARGS[@]}" "$@"
