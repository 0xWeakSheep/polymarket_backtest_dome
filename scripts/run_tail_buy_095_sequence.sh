#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs

if [[ -z "${DOME_API_KEY:-}" ]]; then
  echo "Missing DOME_API_KEY" >&2
  exit 1
fi

exec python3 -u -m src.research.tail_buy_095_sequence.analyze_sequence --threshold 0.95 --market-limit 17391 --resume "$@"
