import argparse
import json
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from src.api.dome import DomeAPIError, DomeClient
from src.research.btc_5m_arrival.logic import (
    ArrivalHit,
    MAX_ONE_MINUTE_RANGE_SECONDS,
    ONE_MINUTE_CANDLE_INTERVAL,
    analyze_market_arrival,
    build_thresholds,
    format_threshold,
    market_is_btc_five_minute,
)


def append_jsonl(path: Path, row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_condition_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()

    condition_ids: Set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            condition_id = str(row.get("condition_id") or "").strip()
            if condition_id:
                condition_ids.add(condition_id)
    return condition_ids


def align_timestamp_to_step(timestamp: int, step_seconds: int) -> int:
    return timestamp - (timestamp % step_seconds)


def build_market_slugs(
    *,
    slug_prefix: str,
    start_timestamp: int,
    step_seconds: int,
    batch_size: int,
    end_timestamp: int,
) -> List[str]:
    slugs: List[str] = []
    current_timestamp = start_timestamp
    while current_timestamp <= end_timestamp and len(slugs) < batch_size:
        slugs.append(f"{slug_prefix}-{current_timestamp}")
        current_timestamp += step_seconds
    return slugs


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
    hit_markets: int,
    no_arrival_markets: int,
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
        "hit_markets": hit_markets,
        "no_arrival_markets": no_arrival_markets,
        "failed_markets": failed_markets,
        "last_completed_condition_id": last_completed_condition_id,
    }


def build_summary(
    *,
    thresholds: List[float],
    slug_prefix: str,
    first_slug_timestamp: int,
    requested_slug_count: int,
    fetched_candidate_markets: int,
    matched_target_markets: int,
    processed_markets: int,
    hit_markets: int,
    no_arrival_markets: int,
    failed_markets: int,
    up_hits_by_threshold: Dict[str, int],
    down_hits_by_threshold: Dict[str, int],
) -> Dict[str, object]:
    up_rates: Dict[str, float] = {}
    down_rates: Dict[str, float] = {}
    denominator = processed_markets if processed_markets > 0 else 0

    for threshold in thresholds:
        key = format_threshold(threshold)
        up_count = up_hits_by_threshold.get(key, 0)
        down_count = down_hits_by_threshold.get(key, 0)
        up_rates[key] = round(up_count / denominator, 6) if denominator else 0.0
        down_rates[key] = round(down_count / denominator, 6) if denominator else 0.0

    return {
        "slug_prefix": slug_prefix,
        "first_slug_timestamp": first_slug_timestamp,
        "requested_slug_count": requested_slug_count,
        "fetched_candidate_markets": fetched_candidate_markets,
        "matched_target_markets": matched_target_markets,
        "analyzed_markets": processed_markets,
        "processed_markets": processed_markets,
        "arrival_markets": hit_markets,
        "no_arrival_markets": no_arrival_markets,
        "failed_markets": failed_markets,
        "up_arrival_count_by_threshold": up_hits_by_threshold,
        "down_arrival_count_by_threshold": down_hits_by_threshold,
        "up_arrival_rate_by_threshold": up_rates,
        "down_arrival_rate_by_threshold": down_rates,
    }


def update_directional_counts(
    hit: ArrivalHit,
    up_hits_by_threshold: Dict[str, int],
    down_hits_by_threshold: Dict[str, int],
) -> None:
    if hit.outcome_a_label.lower() == "up":
        for level in set(hit.outcome_a_hit_levels):
            up_hits_by_threshold[level] = up_hits_by_threshold.get(level, 0) + 1
    if hit.outcome_a_label.lower() == "down":
        for level in set(hit.outcome_a_hit_levels):
            down_hits_by_threshold[level] = down_hits_by_threshold.get(level, 0) + 1
    if hit.outcome_b_label.lower() == "up":
        for level in set(hit.outcome_b_hit_levels):
            up_hits_by_threshold[level] = up_hits_by_threshold.get(level, 0) + 1
    if hit.outcome_b_label.lower() == "down":
        for level in set(hit.outcome_b_hit_levels):
            down_hits_by_threshold[level] = down_hits_by_threshold.get(level, 0) + 1


def iter_candle_payloads(
    client: DomeClient,
    *,
    condition_id: str,
    start_time: int,
    end_time: int,
) -> Iterable[Dict[str, object]]:
    chunk_start = start_time
    while chunk_start <= end_time:
        chunk_end = min(chunk_start + MAX_ONE_MINUTE_RANGE_SECONDS - 1, end_time)
        payload = client.get_candlesticks(
            condition_id,
            start_time=chunk_start,
            end_time=chunk_end,
            interval=ONE_MINUTE_CANDLE_INTERVAL,
        )
        candlesticks = payload.get("candlesticks", [])
        if isinstance(candlesticks, list):
            for item in candlesticks:
                if not isinstance(item, list) or len(item) != 2:
                    continue
                candles, token_meta = item
                if not isinstance(token_meta, dict):
                    continue
                yield {
                    "token_id": token_meta.get("token_id"),
                    "side": token_meta.get("side"),
                    "candles": candles if isinstance(candles, list) else [],
                }
        chunk_start = chunk_end + 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze arrival rates for BTC five-minute Up/Down markets using arithmetic slug generation."
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
        default="data/raw/btc_5m_arrival/markets/selected_markets.jsonl",
        help="Path to append selected BTC five-minute market metadata.",
    )
    parser.add_argument(
        "--misses-jsonl",
        default="data/processed/btc_5m_arrival/misses.jsonl",
        help="Path to append markets where neither side hit any configured threshold.",
    )
    parser.add_argument(
        "--progress-json",
        default="data/processed/btc_5m_arrival/progress.json",
        help="Path to write running progress and resume state.",
    )
    parser.add_argument(
        "--summary-json",
        default="data/processed/btc_5m_arrival/summary.json",
        help="Path to write aggregated threshold counts and rates.",
    )
    parser.add_argument(
        "--failed-markets-jsonl",
        default="data/processed/btc_5m_arrival/failed_markets.jsonl",
        help="Path to append matched markets that failed due to API errors or timeouts.",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from saved progress and existing outputs.")
    parser.add_argument(
        "--max-markets",
        type=int,
        default=0,
        help="Optional cap for debugging. 0 means process all matched BTC five-minute markets.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    thresholds = build_thresholds(args.min_threshold, args.max_threshold, args.step)

    selected_markets_jsonl = Path(args.selected_markets_jsonl)
    misses_jsonl = Path(args.misses_jsonl)
    progress_json = Path(args.progress_json)
    summary_json = Path(args.summary_json)
    failed_markets_jsonl = Path(args.failed_markets_jsonl)

    aligned_now = align_timestamp_to_step(int(time.time()), args.step_seconds)
    state = load_json(progress_json) if args.resume else {}
    next_slug_timestamp = int(state.get("next_slug_timestamp") or args.first_slug_timestamp)
    requested_slug_count = int(state.get("requested_slug_count") or 0)
    fetched_candidate_markets = int(state.get("fetched_candidate_markets") or 0)
    matched_target_markets = int(state.get("matched_target_markets") or 0)
    processed_markets = int(state.get("processed_markets") or 0)
    hit_markets = int(state.get("hit_markets") or 0)
    no_arrival_markets = int(state.get("no_arrival_markets") or 0)
    failed_markets = int(state.get("failed_markets") or 0)
    last_completed_condition_id = state.get("last_completed_condition_id")
    if last_completed_condition_id is not None:
        last_completed_condition_id = str(last_completed_condition_id)

    summary_state = load_json(summary_json) if args.resume else {}
    up_hits_by_threshold = {
        format_threshold(value): int((summary_state.get("up_arrival_count_by_threshold") or {}).get(format_threshold(value), 0))
        for value in thresholds
    }
    down_hits_by_threshold = {
        format_threshold(value): int((summary_state.get("down_arrival_count_by_threshold") or {}).get(format_threshold(value), 0))
        for value in thresholds
    }

    seen_selected_ids = load_condition_ids(selected_markets_jsonl) if args.resume else set()
    seen_miss_ids = load_condition_ids(misses_jsonl) if args.resume else set()

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
            hit_markets=hit_markets,
            no_arrival_markets=no_arrival_markets,
            failed_markets=failed_markets,
            last_completed_condition_id=last_completed_condition_id,
        ),
    )
    write_json(
        summary_json,
        build_summary(
            thresholds=thresholds,
            slug_prefix=args.slug_prefix,
            first_slug_timestamp=args.first_slug_timestamp,
            requested_slug_count=requested_slug_count,
            fetched_candidate_markets=fetched_candidate_markets,
            matched_target_markets=matched_target_markets,
            processed_markets=processed_markets,
            hit_markets=hit_markets,
            no_arrival_markets=no_arrival_markets,
            failed_markets=failed_markets,
            up_hits_by_threshold=up_hits_by_threshold,
            down_hits_by_threshold=down_hits_by_threshold,
        ),
    )

    print(
        f"BTC 5m market source: slug_prefix={args.slug_prefix}, "
        f"first_slug_timestamp={args.first_slug_timestamp}, step_seconds={args.step_seconds}"
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
                    hit_markets=hit_markets,
                    no_arrival_markets=no_arrival_markets,
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
            if not isinstance(market, dict):
                continue
            if not market_is_btc_five_minute(market):
                continue

            condition_id = str(market.get("condition_id") or "").strip()
            if not condition_id:
                continue

            matched_target_markets += 1
            analyzed_in_this_run += 1
            print(f"Processing BTC 5m market {processed_markets + 1}: {condition_id}")

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
                append_jsonl(
                    failed_markets_jsonl,
                    {
                        "condition_id": condition_id,
                        "market_slug": str(market.get("market_slug") or ""),
                        "title": str(market.get("title") or ""),
                        "error": "Invalid start_time or end_time",
                    },
                )
                processed_markets += 1
                last_completed_condition_id = condition_id
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
                append_jsonl(
                    failed_markets_jsonl,
                    {
                        "condition_id": condition_id,
                        "market_slug": str(market.get("market_slug") or ""),
                        "title": str(market.get("title") or ""),
                        "error": str(exc),
                    },
                )
                processed_markets += 1
                last_completed_condition_id = condition_id
                continue

            if hit is None:
                continue

            processed_markets += 1
            last_completed_condition_id = condition_id

            if hit.has_any_hit():
                update_directional_counts(hit, up_hits_by_threshold, down_hits_by_threshold)
                hit_markets += 1
            else:
                no_arrival_markets += 1
                if condition_id not in seen_miss_ids:
                    append_jsonl(misses_jsonl, hit.to_dict())
                    seen_miss_ids.add(condition_id)

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
                    hit_markets=hit_markets,
                    no_arrival_markets=no_arrival_markets,
                    failed_markets=failed_markets,
                    last_completed_condition_id=last_completed_condition_id,
                ),
            )
            write_json(
                summary_json,
                build_summary(
                    thresholds=thresholds,
                    slug_prefix=args.slug_prefix,
                    first_slug_timestamp=args.first_slug_timestamp,
                    requested_slug_count=requested_slug_count,
                    fetched_candidate_markets=fetched_candidate_markets,
                    matched_target_markets=matched_target_markets,
                    processed_markets=processed_markets,
                    hit_markets=hit_markets,
                    no_arrival_markets=no_arrival_markets,
                    failed_markets=failed_markets,
                    up_hits_by_threshold=up_hits_by_threshold,
                    down_hits_by_threshold=down_hits_by_threshold,
                ),
            )

            if processed_markets % 25 == 0:
                print(
                    f"Processed {processed_markets} matched BTC 5m markets | "
                    f"hits {hit_markets} | misses {no_arrival_markets} | requested slugs {requested_slug_count}"
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
                hit_markets=hit_markets,
                no_arrival_markets=no_arrival_markets,
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
            slug_prefix=args.slug_prefix,
            first_slug_timestamp=args.first_slug_timestamp,
            requested_slug_count=requested_slug_count,
            fetched_candidate_markets=fetched_candidate_markets,
            matched_target_markets=matched_target_markets,
            processed_markets=processed_markets,
            hit_markets=hit_markets,
            no_arrival_markets=no_arrival_markets,
            failed_markets=failed_markets,
            up_hits_by_threshold=up_hits_by_threshold,
            down_hits_by_threshold=down_hits_by_threshold,
        ),
    )

    print(
        json.dumps(
            build_summary(
                thresholds=thresholds,
                slug_prefix=args.slug_prefix,
                first_slug_timestamp=args.first_slug_timestamp,
                requested_slug_count=requested_slug_count,
                fetched_candidate_markets=fetched_candidate_markets,
                matched_target_markets=matched_target_markets,
                processed_markets=processed_markets,
                hit_markets=hit_markets,
                no_arrival_markets=no_arrival_markets,
                failed_markets=failed_markets,
                up_hits_by_threshold=up_hits_by_threshold,
                down_hits_by_threshold=down_hits_by_threshold,
            ),
            indent=2,
            ensure_ascii=True,
        )
    )
    print(f"Progress: {progress_json}")
    print(f"Summary: {summary_json}")
    print(f"Misses: {misses_jsonl}")
    print(f"Selected markets: {selected_markets_jsonl}")
    print(f"Failures: {failed_markets_jsonl}")


if __name__ == "__main__":
    main()
