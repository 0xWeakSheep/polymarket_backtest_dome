#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -d ".venv" ]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

export PYTHONUNBUFFERED=1

exec python3 -u -m src.research.btc_5m_volatility.compute_pre_5m_volatility --resume "$@"
