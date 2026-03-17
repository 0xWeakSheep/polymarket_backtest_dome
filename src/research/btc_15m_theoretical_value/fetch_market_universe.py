"""Run: python3 -m src.research.btc_15m_theoretical_value.fetch_market_universe --resume"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from src.api.dome import DomeAPIError, DomeClient
from src.research.btc_5m_arrival.analyze_arrival import append_jsonl, load_condition_ids, load_json, write_json
from src.research.btc_15m_theoretical_value.logic import (
    FIFTEEN_MINUTE_SECONDS,
    align_timestamp_to_step,
    build_market_slugs,
    extract_binary_tokens,
    market_is_btc_fifteen_minute,
)


DEFAULT_SCAN_START_TIMESTAMP = 1704067200  # 2024-01-01 00:00:00 UTC


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch the full BTC 15m market universe from the earliest recurring slug to now."
    )
    parser.add_argument("--slug-prefix", default="btc-updown-15m")
    parser.add_argument("--scan-start-timestamp", type=int, default=DEFAULT_SCAN_START_TIMESTAMP)
    parser.add_argument("--disable-auto-discovery", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument(
        "--universe-jsonl",
        default="data/processed/btc_15m_theoretical_value/universe/markets.jsonl",
    )
    parser.add_argument(
        "--progress-json",
        default="data/processed/btc_15m_theoretical_value/universe/progress.json",
    )
    parser.add_argument(
        "--summary-json",
        default="data/processed/btc_15m_theoretical_value/universe/summary.json",
    )
    parser.add_argument(
        "--failed-jsonl",
        default="data/processed/btc_15m_theoretical_value/universe/failed_batches.jsonl",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-markets", type=int, default=0)
    return parser.parse_args()


def build_progress(
    *,
    slug_prefix: str,
    scan_start_timestamp: int,
    next_slug_timestamp: int,
    requested_slug_count: int,
    fetched_market_count: int,
    selected_market_count: int,
    failed_batches: int,
    first_market_timestamp: int | None,
    last_condition_id: str | None,
) -> dict[str, object]:
    return {
        "slug_prefix": slug_prefix,
        "scan_start_timestamp": scan_start_timestamp,
        "next_slug_timestamp": next_slug_timestamp,
        "requested_slug_count": requested_slug_count,
        "fetched_market_count": fetched_market_count,
        "selected_market_count": selected_market_count,
        "failed_batches": failed_batches,
        "first_market_timestamp": first_market_timestamp,
        "last_condition_id": last_condition_id,
    }


def fetch_matching_markets(client: DomeClient, slug_batch: list[str]) -> list[dict[str, object]]:
    payload = client._request_json(
        "/polymarket/markets",
        params={"market_slug": slug_batch, "limit": len(slug_batch)},
    )
    markets = payload.get("markets", [])
    if not isinstance(markets, list):
        return []
    return [market for market in markets if isinstance(market, dict) and market_is_btc_fifteen_minute(market)]


def discover_scan_start_timestamp(
    *,
    client: DomeClient,
    slug_prefix: str,
    lower_bound_timestamp: int,
    aligned_now: int,
    batch_size: int,
) -> int:
    step = FIFTEEN_MINUTE_SECONDS
    current_end = aligned_now
    first_hit_timestamp: int | None = None
    empty_batches_after_hit = 0

    while current_end >= lower_bound_timestamp:
        batch_start = max(lower_bound_timestamp, current_end - step * (batch_size - 1))
        slug_batch = build_market_slugs(
            slug_prefix=slug_prefix,
            start_timestamp=batch_start,
            step_seconds=step,
            batch_size=batch_size,
            end_timestamp=current_end,
        )
        if not slug_batch:
            break
        markets = fetch_matching_markets(client, slug_batch)
        if markets:
            batch_first_timestamp = min(int(market.get("end_time") or current_end) for market in markets)
            first_hit_timestamp = batch_first_timestamp if first_hit_timestamp is None else min(first_hit_timestamp, batch_first_timestamp)
            empty_batches_after_hit = 0
        elif first_hit_timestamp is not None:
            empty_batches_after_hit += 1
            if empty_batches_after_hit >= 32:
                break
        current_end = batch_start - step

    if first_hit_timestamp is None:
        return aligned_now
    return align_timestamp_to_step(first_hit_timestamp, step)


def main() -> None:
    args = parse_args()
    progress_json = Path(args.progress_json)
    universe_jsonl = Path(args.universe_jsonl)
    summary_json = Path(args.summary_json)
    failed_jsonl = Path(args.failed_jsonl)

    scan_start_timestamp = align_timestamp_to_step(args.scan_start_timestamp, FIFTEEN_MINUTE_SECONDS)
    aligned_now = align_timestamp_to_step(int(time.time()), FIFTEEN_MINUTE_SECONDS)
    state = load_json(progress_json) if args.resume else {}
    client = DomeClient()
    if args.resume or args.disable_auto_discovery:
        next_slug_timestamp = int(state.get("next_slug_timestamp") or scan_start_timestamp)
    else:
        next_slug_timestamp = discover_scan_start_timestamp(
            client=client,
            slug_prefix=args.slug_prefix,
            lower_bound_timestamp=scan_start_timestamp,
            aligned_now=aligned_now,
            batch_size=args.batch_size,
        )
    requested_slug_count = int(state.get("requested_slug_count") or 0)
    fetched_market_count = int(state.get("fetched_market_count") or 0)
    selected_market_count = int(state.get("selected_market_count") or 0)
    failed_batches = int(state.get("failed_batches") or 0)
    first_market_timestamp_raw = state.get("first_market_timestamp")
    first_market_timestamp = int(first_market_timestamp_raw) if first_market_timestamp_raw else None
    last_condition_id_raw = state.get("last_condition_id")
    last_condition_id = str(last_condition_id_raw) if last_condition_id_raw else None

    seen_condition_ids = load_condition_ids(universe_jsonl) if args.resume else set()
    while next_slug_timestamp <= aligned_now:
        if args.max_markets and selected_market_count >= args.max_markets:
            break

        slug_batch = build_market_slugs(
            slug_prefix=args.slug_prefix,
            start_timestamp=next_slug_timestamp,
            step_seconds=FIFTEEN_MINUTE_SECONDS,
            batch_size=args.batch_size,
            end_timestamp=aligned_now,
        )
        if not slug_batch:
            break

        requested_slug_count += len(slug_batch)
        try:
            markets = fetch_matching_markets(client, slug_batch)
        except DomeAPIError as exc:
            failed_batches += 1
            append_jsonl(
                failed_jsonl,
                {
                    "slug_batch_start": slug_batch[0],
                    "slug_batch_end": slug_batch[-1],
                    "error": str(exc),
                },
            )
            next_slug_timestamp += FIFTEEN_MINUTE_SECONDS * len(slug_batch)
            write_json(
                progress_json,
                build_progress(
                    slug_prefix=args.slug_prefix,
                    scan_start_timestamp=scan_start_timestamp,
                    next_slug_timestamp=next_slug_timestamp,
                    requested_slug_count=requested_slug_count,
                    fetched_market_count=fetched_market_count,
                    selected_market_count=selected_market_count,
                    failed_batches=failed_batches,
                    first_market_timestamp=first_market_timestamp,
                    last_condition_id=last_condition_id,
                ),
            )
            continue

        fetched_market_count += len(markets)

        for market in markets:
            condition_id = str(market.get("condition_id") or "").strip()
            if not condition_id or condition_id in seen_condition_ids:
                continue
            tokens = extract_binary_tokens(market)
            if tokens is None:
                continue
            market_end_time = int(market.get("end_time") or 0)
            if first_market_timestamp is None or (market_end_time and market_end_time < first_market_timestamp):
                first_market_timestamp = market_end_time
            append_jsonl(
                universe_jsonl,
                {
                    "condition_id": condition_id,
                    "market_slug": str(market.get("market_slug") or ""),
                    "event_slug": str(market.get("event_slug") or ""),
                    "title": str(market.get("title") or ""),
                    "start_time": market.get("start_time"),
                    "end_time": market.get("end_time"),
                    "close_time": market.get("close_time"),
                    "status": str(market.get("status") or ""),
                    "volume_total": market.get("volume_total"),
                    **tokens,
                },
            )
            seen_condition_ids.add(condition_id)
            selected_market_count += 1
            last_condition_id = condition_id
            if args.max_markets and selected_market_count >= args.max_markets:
                break

        next_slug_timestamp += FIFTEEN_MINUTE_SECONDS * len(slug_batch)
        print(
            "universe batch done",
            slug_batch[0],
            slug_batch[-1],
            f"matching={len(markets)}",
            f"selected={selected_market_count}",
        )
        write_json(
            progress_json,
            build_progress(
                slug_prefix=args.slug_prefix,
                scan_start_timestamp=scan_start_timestamp,
                next_slug_timestamp=next_slug_timestamp,
                requested_slug_count=requested_slug_count,
                fetched_market_count=fetched_market_count,
                selected_market_count=selected_market_count,
                failed_batches=failed_batches,
                first_market_timestamp=first_market_timestamp,
                last_condition_id=last_condition_id,
            ),
        )

    write_json(
        summary_json,
        {
            "strategy_name": "btc_15m_theoretical_value_universe",
            "scan_start_timestamp": scan_start_timestamp,
            "requested_slug_count": requested_slug_count,
            "fetched_market_count": fetched_market_count,
            "selected_market_count": selected_market_count,
            "failed_batches": failed_batches,
            "first_market_timestamp": first_market_timestamp,
            "generated_at": int(time.time()),
        },
    )
    print("universe fetch completed", f"selected_markets={selected_market_count}")


if __name__ == "__main__":
    main()
