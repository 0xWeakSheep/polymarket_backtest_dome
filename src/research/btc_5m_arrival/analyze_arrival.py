import argparse
import json
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


def build_progress(
    *,
    thresholds: List[float],
    scanned_closed_markets: int,
    matched_markets: int,
    processed_markets: int,
    hit_markets: int,
    failed_markets: int,
    markets_page_pagination_key: Optional[str],
    last_completed_condition_id: Optional[str],
) -> Dict[str, object]:
    return {
        "thresholds": [format_threshold(value) for value in thresholds],
        "scanned_closed_markets": scanned_closed_markets,
        "matched_markets": matched_markets,
        "processed_markets": processed_markets,
        "hit_markets": hit_markets,
        "failed_markets": failed_markets,
        "markets_page_pagination_key": markets_page_pagination_key,
        "last_completed_condition_id": last_completed_condition_id,
    }


def build_summary(
    *,
    thresholds: List[float],
    scanned_closed_markets: int,
    matched_markets: int,
    processed_markets: int,
    hit_markets: int,
    failed_markets: int,
    yes_hits_by_threshold: Dict[str, int],
    no_hits_by_threshold: Dict[str, int],
) -> Dict[str, object]:
    yes_rates: Dict[str, float] = {}
    no_rates: Dict[str, float] = {}
    combined_hits: Dict[str, int] = {}
    combined_rates: Dict[str, float] = {}
    denominator = processed_markets if processed_markets > 0 else 0

    for threshold in thresholds:
        key = format_threshold(threshold)
        yes_count = yes_hits_by_threshold.get(key, 0)
        no_count = no_hits_by_threshold.get(key, 0)
        combined_count = yes_count + no_count
        yes_rates[key] = round(yes_count / denominator, 6) if denominator else 0.0
        no_rates[key] = round(no_count / denominator, 6) if denominator else 0.0
        combined_hits[key] = combined_count
        combined_rates[key] = round(combined_count / denominator, 6) if denominator else 0.0

    return {
        "scanned_closed_markets": scanned_closed_markets,
        "matched_markets": matched_markets,
        "processed_markets": processed_markets,
        "hit_markets": hit_markets,
        "failed_markets": failed_markets,
        "combined_hits_by_threshold": combined_hits,
        "combined_rate_by_threshold": combined_rates,
        "yes_hits_by_threshold": yes_hits_by_threshold,
        "no_hits_by_threshold": no_hits_by_threshold,
        "yes_rate_by_threshold": yes_rates,
        "no_rate_by_threshold": no_rates,
    }


def update_counts_from_hit(
    hit: ArrivalHit,
    yes_hits_by_threshold: Dict[str, int],
    no_hits_by_threshold: Dict[str, int],
) -> None:
    for level in hit.yes_hit_levels:
        yes_hits_by_threshold[level] = yes_hits_by_threshold.get(level, 0) + 1
    for level in hit.no_hit_levels:
        no_hits_by_threshold[level] = no_hits_by_threshold.get(level, 0) + 1


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
        description="Analyze arrival rates for BTC five-minute Yes/No markets using 1-minute candlesticks."
    )
    parser.add_argument("--min-threshold", type=float, default=0.52)
    parser.add_argument("--max-threshold", type=float, default=0.58)
    parser.add_argument("--step", type=float, default=0.01)
    parser.add_argument(
        "--selected-markets-jsonl",
        default="data/raw/btc_5m_arrival/markets/selected_markets.jsonl",
        help="Path to append selected BTC five-minute market metadata.",
    )
    parser.add_argument(
        "--hits-jsonl",
        default="data/processed/btc_5m_arrival/hits.jsonl",
        help="Path to append markets where Yes or No hit any configured threshold.",
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
    hits_jsonl = Path(args.hits_jsonl)
    progress_json = Path(args.progress_json)
    summary_json = Path(args.summary_json)
    failed_markets_jsonl = Path(args.failed_markets_jsonl)

    state = load_json(progress_json) if args.resume else {}
    scanned_closed_markets = int(state.get("scanned_closed_markets") or 0)
    matched_markets = int(state.get("matched_markets") or 0)
    processed_markets = int(state.get("processed_markets") or 0)
    hit_markets = int(state.get("hit_markets") or 0)
    failed_markets = int(state.get("failed_markets") or 0)
    start_market_page_key = state.get("markets_page_pagination_key")
    if start_market_page_key is not None:
        start_market_page_key = str(start_market_page_key)
    last_completed_condition_id = state.get("last_completed_condition_id")
    if last_completed_condition_id is not None:
        last_completed_condition_id = str(last_completed_condition_id)

    summary_state = load_json(summary_json) if args.resume else {}
    yes_hits_by_threshold = {
        format_threshold(value): int((summary_state.get("yes_hits_by_threshold") or {}).get(format_threshold(value), 0))
        for value in thresholds
    }
    no_hits_by_threshold = {
        format_threshold(value): int((summary_state.get("no_hits_by_threshold") or {}).get(format_threshold(value), 0))
        for value in thresholds
    }

    seen_selected_ids = load_condition_ids(selected_markets_jsonl) if args.resume else set()
    seen_hit_ids = load_condition_ids(hits_jsonl) if args.resume else set()

    client = DomeClient()
    analyzed_in_this_run = 0
    should_skip_completed_in_first_page = args.resume and last_completed_condition_id is not None

    write_json(
        progress_json,
        build_progress(
            thresholds=thresholds,
            scanned_closed_markets=scanned_closed_markets,
            matched_markets=matched_markets,
            processed_markets=processed_markets,
            hit_markets=hit_markets,
            failed_markets=failed_markets,
            markets_page_pagination_key=start_market_page_key,
            last_completed_condition_id=last_completed_condition_id,
        ),
    )
    write_json(
        summary_json,
        build_summary(
            thresholds=thresholds,
            scanned_closed_markets=scanned_closed_markets,
            matched_markets=matched_markets,
            processed_markets=processed_markets,
            hit_markets=hit_markets,
            failed_markets=failed_markets,
            yes_hits_by_threshold=yes_hits_by_threshold,
            no_hits_by_threshold=no_hits_by_threshold,
        ),
    )

    for page in client.iter_closed_market_pages(start_pagination_key=start_market_page_key):
        page_pagination_key = page["page_pagination_key"]
        if page_pagination_key is not None:
            page_pagination_key = str(page_pagination_key)

        for market in page["items"]:
            if not isinstance(market, dict):
                continue

            if should_skip_completed_in_first_page:
                condition_id = str(market.get("condition_id") or "").strip()
                if condition_id == last_completed_condition_id:
                    should_skip_completed_in_first_page = False
                continue

            scanned_closed_markets += 1
            if not market_is_btc_five_minute(market):
                continue

            condition_id = str(market.get("condition_id") or "").strip()
            if not condition_id:
                continue

            matched_markets += 1
            analyzed_in_this_run += 1
            print(f"Processing BTC 5m market {processed_markets + 1}: {condition_id}")

            selected_row = {
                "condition_id": condition_id,
                "market_slug": str(market.get("market_slug") or ""),
                "event_slug": str(market.get("event_slug") or ""),
                "title": str(market.get("title") or ""),
                "start_time": market.get("start_time"),
                "end_time": market.get("end_time"),
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
                write_json(
                    progress_json,
                    build_progress(
                        thresholds=thresholds,
                        scanned_closed_markets=scanned_closed_markets,
                        matched_markets=matched_markets,
                        processed_markets=processed_markets,
                        hit_markets=hit_markets,
                        failed_markets=failed_markets,
                        markets_page_pagination_key=page_pagination_key,
                        last_completed_condition_id=last_completed_condition_id,
                    ),
                )
                write_json(
                    summary_json,
                    build_summary(
                        thresholds=thresholds,
                        scanned_closed_markets=scanned_closed_markets,
                        matched_markets=matched_markets,
                        processed_markets=processed_markets,
                        hit_markets=hit_markets,
                        failed_markets=failed_markets,
                        yes_hits_by_threshold=yes_hits_by_threshold,
                        no_hits_by_threshold=no_hits_by_threshold,
                    ),
                )
                print(f"Failed BTC 5m market {condition_id}: {exc}")
                if args.max_markets and analyzed_in_this_run >= args.max_markets:
                    break
                continue

            processed_markets += 1
            last_completed_condition_id = condition_id

            if hit is not None and condition_id not in seen_hit_ids:
                append_jsonl(hits_jsonl, hit.to_dict())
                update_counts_from_hit(hit, yes_hits_by_threshold, no_hits_by_threshold)
                seen_hit_ids.add(condition_id)
                hit_markets += 1

            write_json(
                progress_json,
                build_progress(
                    thresholds=thresholds,
                    scanned_closed_markets=scanned_closed_markets,
                    matched_markets=matched_markets,
                    processed_markets=processed_markets,
                    hit_markets=hit_markets,
                    failed_markets=failed_markets,
                    markets_page_pagination_key=page_pagination_key,
                    last_completed_condition_id=last_completed_condition_id,
                ),
            )
            write_json(
                summary_json,
                build_summary(
                    thresholds=thresholds,
                    scanned_closed_markets=scanned_closed_markets,
                    matched_markets=matched_markets,
                    processed_markets=processed_markets,
                    hit_markets=hit_markets,
                    failed_markets=failed_markets,
                    yes_hits_by_threshold=yes_hits_by_threshold,
                    no_hits_by_threshold=no_hits_by_threshold,
                ),
            )

            if processed_markets % 25 == 0:
                print(
                    f"Processed {processed_markets} matched BTC 5m markets | "
                    f"hits {hit_markets} | scanned closed markets {scanned_closed_markets}"
                )

            if args.max_markets and analyzed_in_this_run >= args.max_markets:
                break

        write_json(
            progress_json,
            build_progress(
                thresholds=thresholds,
                scanned_closed_markets=scanned_closed_markets,
                matched_markets=matched_markets,
                processed_markets=processed_markets,
                hit_markets=hit_markets,
                failed_markets=failed_markets,
                markets_page_pagination_key=page["next_pagination_key"],
                last_completed_condition_id=last_completed_condition_id,
            ),
        )

        if args.max_markets and analyzed_in_this_run >= args.max_markets:
            break

        should_skip_completed_in_first_page = False

    print(
        json.dumps(
            build_summary(
                thresholds=thresholds,
                scanned_closed_markets=scanned_closed_markets,
                matched_markets=matched_markets,
                processed_markets=processed_markets,
                hit_markets=hit_markets,
                failed_markets=failed_markets,
                yes_hits_by_threshold=yes_hits_by_threshold,
                no_hits_by_threshold=no_hits_by_threshold,
            ),
            indent=2,
            ensure_ascii=True,
        )
    )
    print(f"Progress: {progress_json}")
    print(f"Summary: {summary_json}")
    print(f"Hits: {hits_jsonl}")
    print(f"Selected markets: {selected_markets_jsonl}")
    print(f"Failures: {failed_markets_jsonl}")


if __name__ == "__main__":
    main()
