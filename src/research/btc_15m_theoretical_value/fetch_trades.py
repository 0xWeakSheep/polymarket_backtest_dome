"""Run: python3 -m src.research.btc_15m_theoretical_value.fetch_trades --resume"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, Iterable, List

from src.api.dome import DomeAPIError, DomeClient
from src.research.btc_5m_arrival.analyze_arrival import append_jsonl, load_json, write_json
from src.research.btc_15m_theoretical_value.logic import (
    choose_up_down_labels,
    collapse_trade_mirrors,
    normalize_trade_price_to_probability,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch deduplicated tick-level trades for BTC 15m markets.")
    parser.add_argument(
        "--universe-jsonl",
        default="data/processed/btc_15m_theoretical_value/universe/markets.jsonl",
    )
    parser.add_argument(
        "--trades-dir",
        default="data/processed/btc_15m_theoretical_value/trades/by_market",
    )
    parser.add_argument(
        "--completed-jsonl",
        default="data/processed/btc_15m_theoretical_value/trades/completed_markets.jsonl",
    )
    parser.add_argument(
        "--progress-json",
        default="data/processed/btc_15m_theoretical_value/trades/progress.json",
    )
    parser.add_argument(
        "--summary-json",
        default="data/processed/btc_15m_theoretical_value/trades/summary.json",
    )
    parser.add_argument(
        "--failed-jsonl",
        default="data/processed/btc_15m_theoretical_value/trades/failed_markets.jsonl",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-markets", type=int, default=0)
    return parser.parse_args()


def load_jsonl(path: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def load_completed_markets(path: Path) -> set[str]:
    return {str(row.get("condition_id") or "") for row in load_jsonl(path) if row.get("condition_id")}


def write_jsonl(path: Path, rows: Iterable[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def build_progress(
    *,
    processed_markets: int,
    failed_markets: int,
    raw_order_count: int,
    deduped_trade_count: int,
    last_condition_id: str | None,
) -> dict[str, object]:
    return {
        "processed_markets": processed_markets,
        "failed_markets": failed_markets,
        "raw_order_count": raw_order_count,
        "deduped_trade_count": deduped_trade_count,
        "last_condition_id": last_condition_id,
    }


def build_summary(
    *,
    processed_markets: int,
    failed_markets: int,
    raw_order_count: int,
    deduped_trade_count: int,
) -> dict[str, object]:
    return {
        "strategy_name": "btc_15m_theoretical_value_trades",
        "processed_markets": processed_markets,
        "failed_markets": failed_markets,
        "raw_order_count": raw_order_count,
        "deduped_trade_count": deduped_trade_count,
        "generated_at": int(time.time()),
    }


def normalize_market_trades(market: Dict[str, object], order_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    up_label, down_label = choose_up_down_labels(market)
    if not up_label or not down_label:
        return []
    deduped_rows = collapse_trade_mirrors(order_rows)
    normalized_rows: List[Dict[str, object]] = []
    for row in deduped_rows:
        try:
            price = float(row.get("price") or 0.0)
            shares_normalized = float(row.get("shares_normalized") or 0.0)
            timestamp = int(row.get("timestamp") or 0)
            block_number = int(row.get("block_number") or 0)
            log_index = int(row.get("log_index") or 0)
        except (TypeError, ValueError):
            continue
        probability = normalize_trade_price_to_probability(
            str(row.get("token_label") or ""),
            price,
            up_label=up_label,
            down_label=down_label,
        )
        if probability is None or timestamp <= 0 or shares_normalized <= 0:
            continue
        normalized_rows.append(
            {
                "condition_id": str(row.get("condition_id") or ""),
                "market_slug": str(row.get("market_slug") or ""),
                "title": str(row.get("title") or ""),
                "timestamp": timestamp,
                "block_number": block_number,
                "log_index": log_index,
                "tx_hash": str(row.get("tx_hash") or ""),
                "order_hash": str(row.get("order_hash") or ""),
                "token_id": str(row.get("token_id") or ""),
                "token_label": str(row.get("token_label") or ""),
                "side": str(row.get("side") or ""),
                "price": price,
                "shares_normalized": shares_normalized,
                "p_up": probability,
                "end_time": market.get("end_time"),
            }
        )
    normalized_rows.sort(
        key=lambda row: (
            int(row["timestamp"]),
            int(row["block_number"]),
            int(row["log_index"]),
            str(row["tx_hash"]),
        )
    )
    return normalized_rows


def main() -> None:
    args = parse_args()
    universe_rows = load_jsonl(Path(args.universe_jsonl))
    trades_dir = Path(args.trades_dir)
    completed_jsonl = Path(args.completed_jsonl)
    progress_json = Path(args.progress_json)
    summary_json = Path(args.summary_json)
    failed_jsonl = Path(args.failed_jsonl)

    state = load_json(progress_json) if args.resume else {}
    completed_markets = load_completed_markets(completed_jsonl) if args.resume else set()
    processed_markets = int(state.get("processed_markets") or 0)
    failed_markets = int(state.get("failed_markets") or 0)
    raw_order_count = int(state.get("raw_order_count") or 0)
    deduped_trade_count = int(state.get("deduped_trade_count") or 0)
    last_condition_id_raw = state.get("last_condition_id")
    last_condition_id = str(last_condition_id_raw) if last_condition_id_raw else None

    now_ts = int(time.time())
    client = DomeClient()

    for market in universe_rows:
        condition_id = str(market.get("condition_id") or "")
        if not condition_id:
            continue
        market_end_time = int(market.get("end_time") or 0)
        is_closed = market_end_time and market_end_time <= now_ts
        if args.resume and is_closed and condition_id in completed_markets:
            continue
        if args.max_markets and processed_markets >= args.max_markets:
            break

        market_slug = str(market.get("market_slug") or "")
        output_path = trades_dir / f"{condition_id}.jsonl"
        try:
            start_time = int(market.get("start_time") or 0)
            end_time = max(min(market_end_time or now_ts, now_ts), start_time)
            print("fetching trades", condition_id, market_slug, f"window={start_time}->{end_time}")
            order_rows: List[Dict[str, object]] = []
            for page in client.iter_order_pages(
                params={
                    "condition_id": condition_id,
                    "start_time": start_time,
                    "end_time": end_time,
                },
                limit=1000,
            ):
                items = page["items"]
                for row in items:
                    if isinstance(row, dict):
                        order_rows.append(row)
            raw_order_count += len(order_rows)
            normalized_rows = normalize_market_trades(market, order_rows)
            deduped_trade_count += len(normalized_rows)
            write_jsonl(output_path, normalized_rows)
            print(
                "market trades done",
                condition_id,
                f"raw_orders={len(order_rows)}",
                f"deduped_ticks={len(normalized_rows)}",
            )
            if is_closed and condition_id not in completed_markets:
                append_jsonl(
                    completed_jsonl,
                    {
                        "condition_id": condition_id,
                        "market_slug": market_slug,
                        "trade_count": len(normalized_rows),
                        "first_timestamp": normalized_rows[0]["timestamp"] if normalized_rows else None,
                        "last_timestamp": normalized_rows[-1]["timestamp"] if normalized_rows else None,
                    },
                )
                completed_markets.add(condition_id)
            processed_markets += 1
            last_condition_id = condition_id
        except DomeAPIError as exc:
            failed_markets += 1
            append_jsonl(
                failed_jsonl,
                {
                    "condition_id": condition_id,
                    "market_slug": market_slug,
                    "error": str(exc),
                },
            )

        write_json(
            progress_json,
            build_progress(
                processed_markets=processed_markets,
                failed_markets=failed_markets,
                raw_order_count=raw_order_count,
                deduped_trade_count=deduped_trade_count,
                last_condition_id=last_condition_id,
            ),
        )
        write_json(
            summary_json,
            build_summary(
                processed_markets=processed_markets,
                failed_markets=failed_markets,
                raw_order_count=raw_order_count,
                deduped_trade_count=deduped_trade_count,
            ),
        )

    write_json(
        summary_json,
        build_summary(
            processed_markets=processed_markets,
            failed_markets=failed_markets,
            raw_order_count=raw_order_count,
            deduped_trade_count=deduped_trade_count,
        ),
    )
    print("trade fetch completed", f"processed_markets={processed_markets}", f"deduped_ticks={deduped_trade_count}")


if __name__ == "__main__":
    main()
