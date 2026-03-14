"""Run: python3 -m src.research.btc_5m_volatility.compute_pre_5m_volatility --resume"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from src.research.btc_5m_arrival.analyze_arrival import (
    append_jsonl,
    load_condition_ids,
    load_json,
    write_json,
)


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
ONE_MINUTE_SECONDS = 60
FIVE_MINUTE_SECONDS = 300
BINANCE_LIMIT = 1000
MAX_RETRIES = 5


@dataclass
class SpotCandle:
    open_time: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    close_time: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record BTC spot pre-window volatility for each selected 5m market. "
            "Outputs are isolated from existing btc_5m_arrival files."
        )
    )
    parser.add_argument(
        "--selected-markets-jsonl",
        default="data/raw/btc_5m_arrival/markets/selected_markets_complex.jsonl",
        help="Universe of selected BTC 5m markets to analyze.",
    )
    parser.add_argument(
        "--volatility-jsonl",
        default="data/processed/btc_5m_volatility/market_volatility.jsonl",
        help="Path to append per-market volatility rows.",
    )
    parser.add_argument(
        "--progress-json",
        default="data/processed/btc_5m_volatility/progress.json",
        help="Path to write progress for resume.",
    )
    parser.add_argument(
        "--summary-json",
        default="data/processed/btc_5m_volatility/summary.json",
        help="Path to write aggregate summary.",
    )
    parser.add_argument(
        "--failed-jsonl",
        default="data/processed/btc_5m_volatility/failed_markets.jsonl",
        help="Path to append failed market rows.",
    )
    parser.add_argument("--symbol", default="BTCUSDT", help="Spot symbol to fetch from Binance.")
    parser.add_argument("--resume", action="store_true", help="Resume from existing volatility rows.")
    parser.add_argument(
        "--max-markets",
        type=int,
        default=0,
        help="Optional debug cap. 0 means process all selected markets.",
    )
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


def parse_slug_timestamp(market_slug: str) -> int:
    return int(market_slug.rsplit("-", 1)[-1])


def build_summary(
    *,
    symbol: str,
    selected_market_count: int,
    processed_markets: int,
    failed_markets: int,
    skipped_existing_markets: int,
    fetched_spot_candles: int,
    min_market_ts: Optional[int],
    max_market_ts: Optional[int],
) -> Dict[str, object]:
    return {
        "symbol": symbol,
        "selected_market_count": selected_market_count,
        "processed_markets": processed_markets,
        "failed_markets": failed_markets,
        "skipped_existing_markets": skipped_existing_markets,
        "fetched_spot_candles": fetched_spot_candles,
        "min_market_window_start_ts": min_market_ts,
        "max_market_window_start_ts": max_market_ts,
    }


def fetch_klines(
    *,
    symbol: str,
    start_time: int,
    end_time: int,
) -> Dict[int, SpotCandle]:
    candles_by_open_time: Dict[int, SpotCandle] = {}
    start_ms = start_time * 1000
    end_ms = end_time * 1000

    while start_ms <= end_ms:
        params = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "interval": "1m",
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": BINANCE_LIMIT,
            }
        )
        url = f"{BINANCE_KLINES_URL}?{params}"
        payload = None

        for attempt in range(MAX_RETRIES):
            try:
                request = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 polymarket-backtest-volatility",
                        "Accept": "application/json",
                    },
                )
                with urllib.request.urlopen(request, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except Exception:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(min(2 ** attempt, 10))

        if not isinstance(payload, list) or not payload:
            break

        last_open_time = None
        for item in payload:
            if not isinstance(item, list) or len(item) < 7:
                continue
            open_time = int(item[0] // 1000)
            candle = SpotCandle(
                open_time=open_time,
                open_price=float(item[1]),
                high_price=float(item[2]),
                low_price=float(item[3]),
                close_price=float(item[4]),
                close_time=int(item[6] // 1000),
            )
            candles_by_open_time[open_time] = candle
            last_open_time = open_time

        if last_open_time is None:
            break

        next_start_ms = (last_open_time + ONE_MINUTE_SECONDS) * 1000
        if next_start_ms <= start_ms:
            break
        start_ms = next_start_ms

        if len(payload) < BINANCE_LIMIT:
            break

        time.sleep(0.2)

    return candles_by_open_time


def compute_volatility_row(
    market: Dict[str, object],
    candles_by_open_time: Dict[int, SpotCandle],
    *,
    symbol: str,
) -> Dict[str, object]:
    market_slug = str(market.get("market_slug") or "")
    market_window_start_ts = parse_slug_timestamp(market_slug)
    pre_window_start_ts = market_window_start_ts - FIVE_MINUTE_SECONDS
    required_open_times = [pre_window_start_ts + offset for offset in range(0, FIVE_MINUTE_SECONDS, ONE_MINUTE_SECONDS)]

    candles: List[SpotCandle] = []
    for open_time in required_open_times:
        candle = candles_by_open_time.get(open_time)
        if candle is None:
            raise ValueError(f"missing_spot_candle:{open_time}")
        candles.append(candle)

    close_prices = [candle.close_price for candle in candles]
    returns = [
        (close_prices[index] / close_prices[index - 1]) - 1.0
        for index in range(1, len(close_prices))
        if close_prices[index - 1] > 0
    ]
    first_open = candles[0].open_price
    max_high = max(candle.high_price for candle in candles)
    min_low = min(candle.low_price for candle in candles)
    last_close = candles[-1].close_price

    return_std = statistics.pstdev(returns) if len(returns) >= 2 else 0.0
    abs_return_sum = sum(abs(value) for value in returns)
    realized_vol = math.sqrt(sum(value * value for value in returns))
    range_pct = ((max_high - min_low) / first_open) if first_open else 0.0
    net_move_pct = ((last_close - first_open) / first_open) if first_open else 0.0

    return {
        "condition_id": str(market.get("condition_id") or ""),
        "market_slug": market_slug,
        "event_slug": str(market.get("event_slug") or ""),
        "title": str(market.get("title") or ""),
        "market_start_time": market.get("start_time"),
        "market_end_time": market.get("end_time"),
        "market_window_start_ts": market_window_start_ts,
        "market_window_end_ts": market_window_start_ts + FIVE_MINUTE_SECONDS,
        "pre_window_start_ts": pre_window_start_ts,
        "pre_window_end_ts": market_window_start_ts,
        "volatility_window_minutes": 5,
        "spot_symbol": symbol,
        "source": "binance_spot_1m",
        "return_std_1m_5m": round(return_std, 10),
        "abs_return_sum_1m_5m": round(abs_return_sum, 10),
        "realized_vol_1m_5m": round(realized_vol, 10),
        "range_pct_5m": round(range_pct, 10),
        "net_move_pct_5m": round(net_move_pct, 10),
        "first_open_price": round(first_open, 6),
        "last_close_price": round(last_close, 6),
        "max_high_price": round(max_high, 6),
        "min_low_price": round(min_low, 6),
        "sample_candle_count": len(candles),
    }


def main() -> None:
    args = parse_args()
    selected_markets_jsonl = Path(args.selected_markets_jsonl)
    volatility_jsonl = Path(args.volatility_jsonl)
    progress_json = Path(args.progress_json)
    summary_json = Path(args.summary_json)
    failed_jsonl = Path(args.failed_jsonl)

    state = load_json(progress_json) if args.resume else {}
    seen_condition_ids: Set[str] = load_condition_ids(volatility_jsonl) if args.resume else set()
    selected_rows = load_jsonl(selected_markets_jsonl)
    unique_markets = {
        str(row.get("condition_id") or ""): row
        for row in selected_rows
        if str(row.get("condition_id") or "").strip()
    }
    ordered_markets = sorted(
        unique_markets.values(),
        key=lambda row: parse_slug_timestamp(str(row.get("market_slug") or "")),
    )

    if args.max_markets:
        ordered_markets = ordered_markets[: args.max_markets]

    selected_market_count = len(ordered_markets)
    skipped_existing_markets = 0
    processed_markets = int(state.get("processed_markets") or 0) if args.resume else 0
    failed_markets = int(state.get("failed_markets") or 0) if args.resume else 0

    pending_markets = [row for row in ordered_markets if str(row.get("condition_id") or "") not in seen_condition_ids]
    skipped_existing_markets = selected_market_count - len(pending_markets)

    if not pending_markets:
        write_json(
            progress_json,
            {
                "symbol": args.symbol,
                "selected_market_count": selected_market_count,
                "processed_markets": len(seen_condition_ids),
                "failed_markets": int(state.get("failed_markets") or 0) if args.resume else 0,
                "skipped_existing_markets": skipped_existing_markets,
                "fetched_spot_candles": int(state.get("fetched_spot_candles") or 0) if args.resume else 0,
                "last_completed_condition_id": state.get("last_completed_condition_id"),
            },
        )
        write_json(
            summary_json,
            build_summary(
                symbol=args.symbol,
                selected_market_count=selected_market_count,
                processed_markets=len(seen_condition_ids),
                failed_markets=int(state.get("failed_markets") or 0) if args.resume else 0,
                skipped_existing_markets=skipped_existing_markets,
                fetched_spot_candles=int(state.get("fetched_spot_candles") or 0) if args.resume else 0,
                min_market_ts=parse_slug_timestamp(str(ordered_markets[0].get("market_slug") or "")),
                max_market_ts=parse_slug_timestamp(str(ordered_markets[-1].get("market_slug") or "")),
            ),
        )
        print("No pending markets for btc_5m_volatility; exiting without refetching spot candles")
        return

    if not ordered_markets:
        write_json(
            summary_json,
            build_summary(
                symbol=args.symbol,
                selected_market_count=0,
                processed_markets=0,
                failed_markets=0,
                skipped_existing_markets=0,
                fetched_spot_candles=0,
                min_market_ts=None,
                max_market_ts=None,
            ),
        )
        return

    min_market_ts = parse_slug_timestamp(str(ordered_markets[0].get("market_slug") or ""))
    max_market_ts = parse_slug_timestamp(str(ordered_markets[-1].get("market_slug") or ""))
    spot_fetch_start = min_market_ts - FIVE_MINUTE_SECONDS
    spot_fetch_end = max_market_ts - ONE_MINUTE_SECONDS

    print(
        f"Fetching {args.symbol} 1m spot candles from {spot_fetch_start} to {spot_fetch_end} "
        f"for {selected_market_count} selected markets"
    )
    candles_by_open_time = fetch_klines(symbol=args.symbol, start_time=spot_fetch_start, end_time=spot_fetch_end)
    fetched_spot_candles = len(candles_by_open_time)

    for index, market in enumerate(pending_markets, start=1):
        condition_id = str(market.get("condition_id") or "")
        try:
            row = compute_volatility_row(market, candles_by_open_time, symbol=args.symbol)
            append_jsonl(volatility_jsonl, row)
            processed_markets += 1
            seen_condition_ids.add(condition_id)
        except Exception as exc:
            failed_markets += 1
            append_jsonl(
                failed_jsonl,
                {
                    "condition_id": condition_id,
                    "market_slug": str(market.get("market_slug") or ""),
                    "error": str(exc),
                },
            )

        if index % 100 == 0 or index == len(pending_markets):
            write_json(
                progress_json,
                {
                    "symbol": args.symbol,
                    "selected_market_count": selected_market_count,
                    "processed_markets": processed_markets,
                    "failed_markets": failed_markets,
                    "skipped_existing_markets": skipped_existing_markets,
                    "fetched_spot_candles": fetched_spot_candles,
                    "last_completed_condition_id": condition_id,
                },
            )
            write_json(
                summary_json,
                build_summary(
                    symbol=args.symbol,
                    selected_market_count=selected_market_count,
                    processed_markets=processed_markets,
                    failed_markets=failed_markets,
                    skipped_existing_markets=skipped_existing_markets,
                    fetched_spot_candles=fetched_spot_candles,
                    min_market_ts=min_market_ts,
                    max_market_ts=max_market_ts,
                ),
            )
            print(
                f"Processed volatility markets {index}/{len(pending_markets)} | "
                f"success={processed_markets} failed={failed_markets}"
            )

    print(f"Volatility rows: {volatility_jsonl}")
    print(f"Summary: {summary_json}")


if __name__ == "__main__":
    main()
