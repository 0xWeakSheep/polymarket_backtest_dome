"""Run: python3 -m src.research.btc_5m_arrival_complex.analyze_threshold_misses --resume"""

import argparse
import json
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from src.api.dome import DomeAPIError, DomeClient
from src.research.btc_5m_arrival.analyze_arrival import (
    align_timestamp_to_step,
    append_jsonl,
    build_market_slugs,
    iter_candle_payloads,
    load_condition_ids,
    load_json,
    write_json,
)
from src.research.btc_5m_arrival.logic import (
    ArrivalHit,
    analyze_market_arrival,
    build_thresholds,
    format_threshold,
    market_is_btc_five_minute,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record per-threshold BTC five-minute markets where Up/Down failed to reach the target level. "
            "Outputs are isolated from the original btc_5m_arrival summary files."
        )
    )
    parser.add_argument("--slug-prefix", default="btc-updown-5m")
    parser.add_argument("--first-slug-timestamp", type=int, default=1770932400)
    parser.add_argument("--step-seconds", type=int, default=300)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--min-threshold", type=float, default=0.52)
    parser.add_argument("--max-threshold", type=float, default=0.58)
    parser.add_argument("--step", type=float, default=0.01)
    parser.add_argument(
        "--selected-markets-jsonl",
        default="data/raw/btc_5m_arrival/markets/selected_markets_complex.jsonl",
        help="Path to append selected BTC five-minute market metadata for this complex pass.",
    )
    parser.add_argument(
        "--progress-json",
        default="data/processed/btc_5m_arrival/complex_progress.json",
        help="Path to write running progress and resume state for the complex pass.",
    )
    parser.add_argument(
        "--summary-json",
        default="data/processed/btc_5m_arrival/complex_summary.json",
        help="Path to write miss counts and rates by threshold for the complex pass.",
    )
    parser.add_argument(
        "--failed-markets-jsonl",
        default="data/processed/btc_5m_arrival/complex_failed_markets.jsonl",
        help="Path to append matched markets that failed due to API errors or invalid timestamps.",
    )
    parser.add_argument(
        "--threshold-output-root",
        default="data/processed/btc_5m_arrival",
        help="Root directory containing 0.52/0.53/... subdirectories for threshold-level miss files.",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from saved progress and existing outputs.")
    parser.add_argument(
        "--max-markets",
        type=int,
        default=0,
        help="Optional cap for debugging. 0 means process all matched BTC five-minute markets.",
    )
    return parser.parse_args()


def build_progress(
    *,
    thresholds: List[float],
    slug_prefix: str,
    first_slug_timestamp: int,
    next_slug_timestamp: int,
    requested_slug_count: int,
    fetched_candidate_markets: int,
    matched_target_markets: int,
    processed_markets: int,
    failed_markets: int,
    last_completed_condition_id: Optional[str],
) -> Dict[str, object]:
    return {
        "thresholds": [format_threshold(value) for value in thresholds],
        "slug_prefix": slug_prefix,
        "first_slug_timestamp": first_slug_timestamp,
        "next_slug_timestamp": next_slug_timestamp,
        "requested_slug_count": requested_slug_count,
        "fetched_candidate_markets": fetched_candidate_markets,
        "matched_target_markets": matched_target_markets,
        "processed_markets": processed_markets,
        "failed_markets": failed_markets,
        "last_completed_condition_id": last_completed_condition_id,
    }


def build_summary(
    *,
    thresholds: List[float],
    processed_markets: int,
    up_miss_counts: Dict[str, int],
    down_miss_counts: Dict[str, int],
) -> Dict[str, object]:
    denominator = processed_markets if processed_markets > 0 else 0
    up_miss_rates: Dict[str, float] = {}
    down_miss_rates: Dict[str, float] = {}

    for threshold in thresholds:
        key = format_threshold(threshold)
        up_count = up_miss_counts.get(key, 0)
        down_count = down_miss_counts.get(key, 0)
        up_miss_rates[key] = round(up_count / denominator, 6) if denominator else 0.0
        down_miss_rates[key] = round(down_count / denominator, 6) if denominator else 0.0

    return {
        "processed_markets": processed_markets,
        "up_miss_count_by_threshold": up_miss_counts,
        "down_miss_count_by_threshold": down_miss_counts,
        "up_miss_rate_by_threshold": up_miss_rates,
        "down_miss_rate_by_threshold": down_miss_rates,
    }


def load_threshold_seen_ids(
    threshold_output_root: Path,
    threshold_keys: List[str],
    *,
    resume: bool,
) -> Dict[Tuple[str, str], Set[str]]:
    seen: Dict[Tuple[str, str], Set[str]] = {}
    for threshold_key in threshold_keys:
        for side in ("up", "down"):
            path = threshold_output_root / threshold_key / f"{side}_misses.jsonl"
            seen[(threshold_key, side)] = load_condition_ids(path) if resume else set()
    return seen


def load_threshold_counts(
    threshold_output_root: Path,
    threshold_keys: List[str],
    *,
    resume: bool,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    if not resume:
        return ({key: 0 for key in threshold_keys}, {key: 0 for key in threshold_keys})

    up_counts: Dict[str, int] = {}
    down_counts: Dict[str, int] = {}
    for threshold_key in threshold_keys:
        up_path = threshold_output_root / threshold_key / "up_misses.jsonl"
        down_path = threshold_output_root / threshold_key / "down_misses.jsonl"
        up_counts[threshold_key] = len(load_condition_ids(up_path))
        down_counts[threshold_key] = len(load_condition_ids(down_path))
    return up_counts, down_counts


def build_threshold_miss_row(hit: ArrivalHit, threshold_key: str, side: str) -> Dict[str, object]:
    return {
        "condition_id": hit.condition_id,
        "market_slug": hit.market_slug,
        "event_slug": hit.event_slug,
        "title": hit.title,
        "start_time": hit.start_time,
        "end_time": hit.end_time,
        "threshold": threshold_key,
        "side": side,
        "outcome_a_label": hit.outcome_a_label,
        "outcome_b_label": hit.outcome_b_label,
        "outcome_a_max_price": hit.outcome_a_max_price,
        "outcome_b_max_price": hit.outcome_b_max_price,
        "outcome_a_hit_levels": hit.outcome_a_hit_levels,
        "outcome_b_hit_levels": hit.outcome_b_hit_levels,
    }


def append_threshold_misses(
    *,
    hit: ArrivalHit,
    threshold_keys: List[str],
    threshold_output_root: Path,
    seen_threshold_ids: Dict[Tuple[str, str], Set[str]],
    up_miss_counts: Dict[str, int],
    down_miss_counts: Dict[str, int],
) -> None:
    side_to_max_price = {
        hit.outcome_a_label.lower(): hit.outcome_a_max_price,
        hit.outcome_b_label.lower(): hit.outcome_b_max_price,
    }

    for threshold_key in threshold_keys:
        threshold_value = float(threshold_key)
        for side in ("up", "down"):
            side_max_price = side_to_max_price.get(side)
            if side_max_price is None or side_max_price >= threshold_value:
                continue

            seen_ids = seen_threshold_ids[(threshold_key, side)]
            if hit.condition_id in seen_ids:
                continue

            path = threshold_output_root / threshold_key / f"{side}_misses.jsonl"
            append_jsonl(path, build_threshold_miss_row(hit, threshold_key, side))
            seen_ids.add(hit.condition_id)

            if side == "up":
                up_miss_counts[threshold_key] = up_miss_counts.get(threshold_key, 0) + 1
            else:
                down_miss_counts[threshold_key] = down_miss_counts.get(threshold_key, 0) + 1


def main() -> None:
    args = parse_args()
    thresholds = build_thresholds(args.min_threshold, args.max_threshold, args.step)
    threshold_keys = [format_threshold(value) for value in thresholds]

    selected_markets_jsonl = Path(args.selected_markets_jsonl)
    progress_json = Path(args.progress_json)
    summary_json = Path(args.summary_json)
    failed_markets_jsonl = Path(args.failed_markets_jsonl)
    threshold_output_root = Path(args.threshold_output_root)

    aligned_now = align_timestamp_to_step(int(time.time()), args.step_seconds)
    state = load_json(progress_json) if args.resume else {}
    next_slug_timestamp = int(state.get("next_slug_timestamp") or args.first_slug_timestamp)
    requested_slug_count = int(state.get("requested_slug_count") or 0)
    fetched_candidate_markets = int(state.get("fetched_candidate_markets") or 0)
    matched_target_markets = int(state.get("matched_target_markets") or 0)
    processed_markets = int(state.get("processed_markets") or 0)
    failed_markets = int(state.get("failed_markets") or 0)
    last_completed_condition_id = state.get("last_completed_condition_id")
    if last_completed_condition_id is not None:
        last_completed_condition_id = str(last_completed_condition_id)

    seen_selected_ids = load_condition_ids(selected_markets_jsonl) if args.resume else set()
    seen_threshold_ids = load_threshold_seen_ids(threshold_output_root, threshold_keys, resume=args.resume)
    up_miss_counts, down_miss_counts = load_threshold_counts(
        threshold_output_root,
        threshold_keys,
        resume=args.resume,
    )

    client = DomeClient()
    analyzed_in_this_run = 0

    write_json(
        progress_json,
        build_progress(
            thresholds=thresholds,
            slug_prefix=args.slug_prefix,
            first_slug_timestamp=args.first_slug_timestamp,
            next_slug_timestamp=next_slug_timestamp,
            requested_slug_count=requested_slug_count,
            fetched_candidate_markets=fetched_candidate_markets,
            matched_target_markets=matched_target_markets,
            processed_markets=processed_markets,
            failed_markets=failed_markets,
            last_completed_condition_id=last_completed_condition_id,
        ),
    )
    write_json(
        summary_json,
        build_summary(
            thresholds=thresholds,
            processed_markets=processed_markets,
            up_miss_counts=up_miss_counts,
            down_miss_counts=down_miss_counts,
        ),
    )

    while next_slug_timestamp <= aligned_now:
        slug_batch = build_market_slugs(
            slug_prefix=args.slug_prefix,
            start_timestamp=next_slug_timestamp,
            step_seconds=args.step_seconds,
            batch_size=args.batch_size,
            end_timestamp=aligned_now,
        )
        if not slug_batch:
            break

        requested_slug_count += len(slug_batch)

        try:
            payload = client._request_json(
                "/polymarket/markets",
                params={
                    "market_slug": slug_batch,
                    "status": "closed",
                    "limit": len(slug_batch),
                },
            )
        except DomeAPIError as exc:
            append_jsonl(
                failed_markets_jsonl,
                {
                    "slug_batch_start": slug_batch[0],
                    "slug_batch_end": slug_batch[-1],
                    "error": str(exc),
                },
            )
            next_slug_timestamp += args.step_seconds * len(slug_batch)
            failed_markets += 1
            write_json(
                progress_json,
                build_progress(
                    thresholds=thresholds,
                    slug_prefix=args.slug_prefix,
                    first_slug_timestamp=args.first_slug_timestamp,
                    next_slug_timestamp=next_slug_timestamp,
                    requested_slug_count=requested_slug_count,
                    fetched_candidate_markets=fetched_candidate_markets,
                    matched_target_markets=matched_target_markets,
                    processed_markets=processed_markets,
                    failed_markets=failed_markets,
                    last_completed_condition_id=last_completed_condition_id,
                ),
            )
            continue

        markets = payload.get("markets", [])
        if not isinstance(markets, list):
            markets = []
        fetched_candidate_markets += len(markets)

        for market in markets:
            if not isinstance(market, dict) or not market_is_btc_five_minute(market):
                continue

            condition_id = str(market.get("condition_id") or "").strip()
            if not condition_id:
                continue

            matched_target_markets += 1
            analyzed_in_this_run += 1

            selected_row = {
                "condition_id": condition_id,
                "market_slug": str(market.get("market_slug") or ""),
                "event_slug": str(market.get("event_slug") or ""),
                "title": str(market.get("title") or ""),
                "start_time": market.get("start_time"),
                "end_time": market.get("end_time"),
                "outcome_a_label": str((market.get("side_a") or {}).get("label") or ""),
                "outcome_b_label": str((market.get("side_b") or {}).get("label") or ""),
            }
            if condition_id not in seen_selected_ids:
                append_jsonl(selected_markets_jsonl, selected_row)
                seen_selected_ids.add(condition_id)

            start_time_raw = market.get("start_time")
            end_time_raw = market.get("end_time")
            try:
                start_time = int(start_time_raw)
                end_time = int(end_time_raw)
            except (TypeError, ValueError):
                failed_markets += 1
                processed_markets += 1
                last_completed_condition_id = condition_id
                append_jsonl(
                    failed_markets_jsonl,
                    {
                        "condition_id": condition_id,
                        "market_slug": str(market.get("market_slug") or ""),
                        "title": str(market.get("title") or ""),
                        "error": "Invalid start_time or end_time",
                    },
                )
                continue

            try:
                hit = analyze_market_arrival(
                    market,
                    iter_candle_payloads(
                        client,
                        condition_id=condition_id,
                        start_time=start_time,
                        end_time=end_time,
                    ),
                    thresholds,
                )
            except DomeAPIError as exc:
                failed_markets += 1
                processed_markets += 1
                last_completed_condition_id = condition_id
                append_jsonl(
                    failed_markets_jsonl,
                    {
                        "condition_id": condition_id,
                        "market_slug": str(market.get("market_slug") or ""),
                        "title": str(market.get("title") or ""),
                        "error": str(exc),
                    },
                )
                continue

            if hit is None:
                continue

            processed_markets += 1
            last_completed_condition_id = condition_id
            append_threshold_misses(
                hit=hit,
                threshold_keys=threshold_keys,
                threshold_output_root=threshold_output_root,
                seen_threshold_ids=seen_threshold_ids,
                up_miss_counts=up_miss_counts,
                down_miss_counts=down_miss_counts,
            )

            write_json(
                progress_json,
                build_progress(
                    thresholds=thresholds,
                    slug_prefix=args.slug_prefix,
                    first_slug_timestamp=args.first_slug_timestamp,
                    next_slug_timestamp=next_slug_timestamp,
                    requested_slug_count=requested_slug_count,
                    fetched_candidate_markets=fetched_candidate_markets,
                    matched_target_markets=matched_target_markets,
                    processed_markets=processed_markets,
                    failed_markets=failed_markets,
                    last_completed_condition_id=last_completed_condition_id,
                ),
            )
            write_json(
                summary_json,
                build_summary(
                    thresholds=thresholds,
                    processed_markets=processed_markets,
                    up_miss_counts=up_miss_counts,
                    down_miss_counts=down_miss_counts,
                ),
            )

            if processed_markets % 25 == 0:
                print(
                    f"Processed {processed_markets} BTC 5m markets | "
                    f"Up misses@0.52 {up_miss_counts.get('0.52', 0)} | "
                    f"Down misses@0.52 {down_miss_counts.get('0.52', 0)}"
                )

            if args.max_markets and analyzed_in_this_run >= args.max_markets:
                break

        next_slug_timestamp += args.step_seconds * len(slug_batch)
        write_json(
            progress_json,
            build_progress(
                thresholds=thresholds,
                slug_prefix=args.slug_prefix,
                first_slug_timestamp=args.first_slug_timestamp,
                next_slug_timestamp=next_slug_timestamp,
                requested_slug_count=requested_slug_count,
                fetched_candidate_markets=fetched_candidate_markets,
                matched_target_markets=matched_target_markets,
                processed_markets=processed_markets,
                failed_markets=failed_markets,
                last_completed_condition_id=last_completed_condition_id,
            ),
        )

        if args.max_markets and analyzed_in_this_run >= args.max_markets:
            break

    write_json(
        summary_json,
        build_summary(
            thresholds=thresholds,
            processed_markets=processed_markets,
            up_miss_counts=up_miss_counts,
            down_miss_counts=down_miss_counts,
        ),
    )
    print(f"Completed complex BTC 5m miss analysis for {processed_markets} markets.")


if __name__ == "__main__":
    main()
