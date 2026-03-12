import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from src.api.dome import DomeAPIError, DomeClient
from src.research.tail_reversal.logic import (
    MAX_DAILY_RANGE_SECONDS,
    RECOMMENDED_CANDLE_CHUNK_SECONDS,
    ReversalCandidate,
    analyze_market_with_candles,
)


def append_jsonl(path: Path, row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def load_reversal_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()

    ids: Set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            condition_id = str(row.get("condition_id") or "").strip()
            if condition_id:
                ids.add(condition_id)
    return ids


def load_state(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def build_progress(
    *,
    threshold: float,
    processed_markets: int,
    reversal_count: int,
    markets_page_pagination_key: Optional[str],
    last_completed_condition_id: Optional[str],
) -> Dict[str, object]:
    return {
        "threshold": threshold,
        "processed_markets": processed_markets,
        "reversal_count": reversal_count,
        "markets_page_pagination_key": markets_page_pagination_key,
        "last_completed_condition_id": last_completed_condition_id,
    }


def iter_candle_payloads(
    client: DomeClient,
    *,
    condition_id: str,
    start_time: int,
    end_time: int,
) -> Iterable[Dict[str, object]]:
    chunk_start = start_time
    while chunk_start <= end_time:
        chunk_end = min(
            chunk_start + min(MAX_DAILY_RANGE_SECONDS, RECOMMENDED_CANDLE_CHUNK_SECONDS) - 1,
            end_time,
        )
        payload = client.get_candlesticks(
            condition_id,
            start_time=chunk_start,
            end_time=chunk_end,
            interval=1440,
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
        description="Analyze how often the losing side touched a threshold using market metadata and candlesticks."
    )
    parser.add_argument("--threshold", type=float, default=0.95, help="Threshold to test, for example 0.95")
    parser.add_argument(
        "--reversals-jsonl",
        default="data/processed/tail_reversal_095_reversals.jsonl",
        help="Path to append only reversal market details.",
    )
    parser.add_argument(
        "--progress-json",
        default="data/processed/tail_reversal_095_progress.json",
        help="Path to write running progress and resume state.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the saved progress and existing reversal file.",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=0,
        help="Optional cap for debugging. 0 means analyze all closed markets.",
    )
    parser.add_argument(
        "--failed-markets-jsonl",
        default="data/processed/tail_reversal_095_failed_markets.jsonl",
        help="Path to append markets that failed due to API errors or timeouts.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    threshold = args.threshold
    reversals_jsonl = Path(args.reversals_jsonl)
    progress_json = Path(args.progress_json)
    failed_markets_jsonl = Path(args.failed_markets_jsonl)

    state = load_state(progress_json) if args.resume else {}
    processed_markets = int(state.get("processed_markets") or 0)
    reversal_count = int(state.get("reversal_count") or 0)
    start_market_page_key = state.get("markets_page_pagination_key")
    if start_market_page_key is not None:
        start_market_page_key = str(start_market_page_key)
    last_completed_condition_id = state.get("last_completed_condition_id")
    if last_completed_condition_id is not None:
        last_completed_condition_id = str(last_completed_condition_id)

    seen_reversal_ids = load_reversal_ids(reversals_jsonl) if args.resume else set()

    client = DomeClient()
    analyzed_in_this_run = 0

    write_json(
        progress_json,
        build_progress(
            threshold=threshold,
            processed_markets=processed_markets,
            reversal_count=reversal_count,
            markets_page_pagination_key=start_market_page_key,
            last_completed_condition_id=last_completed_condition_id,
        ),
    )

    for page in client.iter_closed_market_pages(start_pagination_key=start_market_page_key):
        page_pagination_key = page["page_pagination_key"]
        if page_pagination_key is not None:
            page_pagination_key = str(page_pagination_key)

        for market in page["items"]:
            if not isinstance(market, dict):
                continue

            condition_id = str(market.get("condition_id") or "").strip()
            if not condition_id:
                continue

            print(f"Processing market {processed_markets + 1}: {condition_id}")

            start_time_raw = market.get("start_time")
            end_time_raw = market.get("end_time")
            try:
                start_time = int(start_time_raw)
                end_time = int(end_time_raw)
            except (TypeError, ValueError):
                continue

            try:
                reversal = analyze_market_with_candles(
                    market,
                    iter_candle_payloads(
                        client,
                        condition_id=condition_id,
                        start_time=start_time,
                        end_time=end_time,
                    ),
                    threshold=threshold,
                )
            except DomeAPIError as exc:
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
                analyzed_in_this_run += 1
                last_completed_condition_id = condition_id
                write_json(
                    progress_json,
                    build_progress(
                        threshold=threshold,
                        processed_markets=processed_markets,
                        reversal_count=reversal_count,
                        markets_page_pagination_key=page_pagination_key,
                        last_completed_condition_id=last_completed_condition_id,
                    ),
                )
                print(f"Failed market {condition_id}: {exc}")
                if args.max_markets and analyzed_in_this_run >= args.max_markets:
                    break
                continue

            processed_markets += 1
            analyzed_in_this_run += 1
            last_completed_condition_id = condition_id

            if reversal is not None and condition_id not in seen_reversal_ids:
                append_jsonl(reversals_jsonl, reversal.to_dict())
                reversal_count += 1
                seen_reversal_ids.add(condition_id)

            write_json(
                progress_json,
                build_progress(
                    threshold=threshold,
                    processed_markets=processed_markets,
                    reversal_count=reversal_count,
                    markets_page_pagination_key=page_pagination_key,
                    last_completed_condition_id=last_completed_condition_id,
                ),
            )

            if processed_markets % 25 == 0:
                print(
                    f"Processed {processed_markets} markets | reversals {reversal_count} | "
                    f"threshold={threshold}"
                )

            if args.max_markets and analyzed_in_this_run >= args.max_markets:
                break

        write_json(
            progress_json,
            build_progress(
                threshold=threshold,
                processed_markets=processed_markets,
                reversal_count=reversal_count,
                markets_page_pagination_key=page["next_pagination_key"],
                last_completed_condition_id=last_completed_condition_id,
            ),
        )

        if args.max_markets and analyzed_in_this_run >= args.max_markets:
            break

    print(
        json.dumps(
            build_progress(
                threshold=threshold,
                processed_markets=processed_markets,
                reversal_count=reversal_count,
                markets_page_pagination_key=None,
                last_completed_condition_id=last_completed_condition_id,
            ),
            indent=2,
            ensure_ascii=True,
        )
    )
    print(f"Progress: {progress_json}")
    print(f"Reversals: {reversals_jsonl}")
    print(f"Failures: {failed_markets_jsonl}")


if __name__ == "__main__":
    main()
