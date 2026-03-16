import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, Optional, Set

from src.api.dome import DomeAPIError, DomeClient
from src.research.tail_buy_095_sequence.logic import (
    MAX_DAILY_RANGE_SECONDS,
    RECOMMENDED_CANDLE_CHUNK_SECONDS,
    TailBuySequenceRecord,
    find_first_threshold_trigger,
)


def append_jsonl(path: Path, row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def load_seen_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    seen: Set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            condition_id = str(row.get("condition_id") or "").strip()
            if condition_id:
                seen.add(condition_id)
    return seen


def load_reversals(path: Path) -> Dict[str, dict]:
    reversals: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            condition_id = str(row.get("condition_id") or "").strip()
            if condition_id:
                reversals[condition_id] = row
    return reversals


def build_progress(
    *,
    threshold: float,
    market_limit: int,
    processed_markets: int,
    triggered_markets: int,
    success_count: int,
    failure_count: int,
    missing_trigger_markets: int,
    failed_markets: int,
    markets_page_pagination_key: Optional[str],
    last_completed_condition_id: Optional[str],
    current_market_slug: Optional[str],
) -> Dict[str, object]:
    return {
        "threshold": threshold,
        "market_limit": market_limit,
        "processed_markets": processed_markets,
        "triggered_markets": triggered_markets,
        "success_count": success_count,
        "failure_count": failure_count,
        "missing_trigger_markets": missing_trigger_markets,
        "failed_markets": failed_markets,
        "markets_page_pagination_key": markets_page_pagination_key,
        "last_completed_condition_id": last_completed_condition_id,
        "current_market_slug": current_market_slug,
    }


def build_summary(
    *,
    threshold: float,
    market_limit: int,
    processed_markets: int,
    triggered_markets: int,
    success_count: int,
    failure_count: int,
    missing_trigger_markets: int,
    failed_markets: int,
) -> Dict[str, object]:
    return {
        "strategy_name": "tail_buy_095_sequence",
        "threshold": threshold,
        "market_limit": market_limit,
        "processed_markets": processed_markets,
        "triggered_markets": triggered_markets,
        "success_count": success_count,
        "failure_count": failure_count,
        "missing_trigger_markets": missing_trigger_markets,
        "failed_markets": failed_markets,
        "trigger_rate": round((triggered_markets / processed_markets) if processed_markets else 0.0, 6),
        "success_rate": round((success_count / triggered_markets) if triggered_markets else 0.0, 6),
        "failure_rate": round((failure_count / triggered_markets) if triggered_markets else 0.0, 6),
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


def reversal_to_record(row: dict, threshold: float) -> TailBuySequenceRecord:
    return TailBuySequenceRecord(
        condition_id=str(row.get("condition_id") or ""),
        market_slug=str(row.get("market_slug") or ""),
        title=str(row.get("title") or ""),
        threshold=threshold,
        trigger_timestamp=int(row.get("trigger_timestamp") or 0),
        trigger_side=str(row.get("losing_side") or ""),
        trigger_token_id=str(row.get("losing_token_id") or ""),
        trigger_price=threshold,
        observed_max_price=float(row.get("losing_max_price") or threshold),
        market_start_time=int(row.get("market_start_time")) if row.get("market_start_time") is not None else None,
        market_end_time=int(row.get("market_end_time")) if row.get("market_end_time") is not None else None,
        outcome="failure",
        source="reversal_file",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild the full 0.95 buy-side trade sequence.")
    parser.add_argument("--threshold", type=float, default=0.95)
    parser.add_argument("--market-limit", type=int, default=17391)
    parser.add_argument(
        "--reversals-jsonl",
        default="data/processed/tail_reversal_095_reversals.jsonl",
    )
    parser.add_argument(
        "--output-root",
        default="data/processed/tail_buy_095_sequence",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-markets", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    threshold = args.threshold
    market_limit = args.market_limit
    reversals = load_reversals(Path(args.reversals_jsonl))
    output_root = Path(args.output_root)
    all_entries_jsonl = output_root / "all_entries.jsonl"
    success_entries_jsonl = output_root / "successful_entries.jsonl"
    failure_entries_jsonl = output_root / "failed_entries.jsonl"
    missing_entries_jsonl = output_root / "missing_trigger_markets.jsonl"
    failed_markets_jsonl = output_root / "failed_markets.jsonl"
    progress_json = output_root / "progress.json"
    summary_json = output_root / "summary.json"

    state = load_json(progress_json) if args.resume else {}
    processed_markets = int(state.get("processed_markets") or 0)
    triggered_markets = int(state.get("triggered_markets") or 0)
    success_count = int(state.get("success_count") or 0)
    failure_count = int(state.get("failure_count") or 0)
    missing_trigger_markets = int(state.get("missing_trigger_markets") or 0)
    failed_markets = int(state.get("failed_markets") or 0)
    start_market_page_key = state.get("markets_page_pagination_key")
    if start_market_page_key is not None:
        start_market_page_key = str(start_market_page_key)
    last_completed_condition_id = state.get("last_completed_condition_id")
    if last_completed_condition_id is not None:
        last_completed_condition_id = str(last_completed_condition_id)

    seen_ids = load_seen_ids(all_entries_jsonl) if args.resume else set()
    client = DomeClient()
    analyzed_in_this_run = 0

    write_json(
        progress_json,
        build_progress(
            threshold=threshold,
            market_limit=market_limit,
            processed_markets=processed_markets,
            triggered_markets=triggered_markets,
            success_count=success_count,
            failure_count=failure_count,
            missing_trigger_markets=missing_trigger_markets,
            failed_markets=failed_markets,
            markets_page_pagination_key=start_market_page_key,
            last_completed_condition_id=last_completed_condition_id,
            current_market_slug=None,
        ),
    )

    for page in client.iter_closed_market_pages(start_pagination_key=start_market_page_key):
        page_pagination_key = page["page_pagination_key"]
        if page_pagination_key is not None:
            page_pagination_key = str(page_pagination_key)

        for market in page["items"]:
            if not isinstance(market, dict):
                continue
            if processed_markets >= market_limit:
                break

            condition_id = str(market.get("condition_id") or "").strip()
            market_slug = str(market.get("market_slug") or "").strip()
            if not condition_id:
                continue

            write_json(
                progress_json,
                build_progress(
                    threshold=threshold,
                    market_limit=market_limit,
                    processed_markets=processed_markets,
                    triggered_markets=triggered_markets,
                    success_count=success_count,
                    failure_count=failure_count,
                    missing_trigger_markets=missing_trigger_markets,
                    failed_markets=failed_markets,
                    markets_page_pagination_key=page_pagination_key,
                    last_completed_condition_id=last_completed_condition_id,
                    current_market_slug=market_slug,
                ),
            )

            print(f"Processing market {processed_markets + 1}: {condition_id}")
            processed_markets += 1
            analyzed_in_this_run += 1
            last_completed_condition_id = condition_id

            if condition_id in seen_ids:
                continue

            if condition_id in reversals:
                record = reversal_to_record(reversals[condition_id], threshold)
                row = record.to_dict()
                append_jsonl(all_entries_jsonl, row)
                append_jsonl(failure_entries_jsonl, row)
                failure_count += 1
                triggered_markets += 1
                seen_ids.add(condition_id)
            else:
                start_time_raw = market.get("start_time")
                end_time_raw = market.get("end_time")
                try:
                    start_time = int(start_time_raw)
                    end_time = int(end_time_raw)
                except (TypeError, ValueError):
                    append_jsonl(
                        failed_markets_jsonl,
                        {
                            "condition_id": condition_id,
                            "market_slug": market_slug,
                            "title": str(market.get("title") or ""),
                            "error": "missing_start_or_end_time",
                        },
                    )
                    failed_markets += 1
                    continue

                try:
                    record = find_first_threshold_trigger(
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
                            "market_slug": market_slug,
                            "title": str(market.get("title") or ""),
                            "error": str(exc),
                        },
                    )
                    failed_markets += 1
                    record = None

                if record is None:
                    missing_trigger_markets += 1
                    append_jsonl(
                        missing_entries_jsonl,
                        {
                            "condition_id": condition_id,
                            "market_slug": market_slug,
                            "title": str(market.get("title") or ""),
                            "market_start_time": start_time,
                            "market_end_time": end_time,
                        },
                    )
                else:
                    row = record.to_dict()
                    append_jsonl(all_entries_jsonl, row)
                    append_jsonl(success_entries_jsonl, row)
                    success_count += 1
                    triggered_markets += 1
                    seen_ids.add(condition_id)

            write_json(
                progress_json,
                build_progress(
                    threshold=threshold,
                    market_limit=market_limit,
                    processed_markets=processed_markets,
                    triggered_markets=triggered_markets,
                    success_count=success_count,
                    failure_count=failure_count,
                    missing_trigger_markets=missing_trigger_markets,
                    failed_markets=failed_markets,
                    markets_page_pagination_key=page_pagination_key,
                    last_completed_condition_id=last_completed_condition_id,
                    current_market_slug=None,
                ),
            )
            write_json(
                summary_json,
                build_summary(
                    threshold=threshold,
                    market_limit=market_limit,
                    processed_markets=processed_markets,
                    triggered_markets=triggered_markets,
                    success_count=success_count,
                    failure_count=failure_count,
                    missing_trigger_markets=missing_trigger_markets,
                    failed_markets=failed_markets,
                ),
            )

            if processed_markets % 25 == 0:
                print(
                    f"Processed {processed_markets} markets | triggered {triggered_markets} | "
                    f"success {success_count} | failure {failure_count} | missing {missing_trigger_markets}"
                )

            if args.max_markets and analyzed_in_this_run >= args.max_markets:
                break

        write_json(
            progress_json,
            build_progress(
                threshold=threshold,
                market_limit=market_limit,
                processed_markets=processed_markets,
                triggered_markets=triggered_markets,
                success_count=success_count,
                failure_count=failure_count,
                missing_trigger_markets=missing_trigger_markets,
                failed_markets=failed_markets,
                markets_page_pagination_key=page["next_pagination_key"],
                last_completed_condition_id=last_completed_condition_id,
                current_market_slug=None,
            ),
        )

        if processed_markets >= market_limit:
            break
        if args.max_markets and analyzed_in_this_run >= args.max_markets:
            break

    write_json(
        summary_json,
        build_summary(
            threshold=threshold,
            market_limit=market_limit,
            processed_markets=processed_markets,
            triggered_markets=triggered_markets,
            success_count=success_count,
            failure_count=failure_count,
            missing_trigger_markets=missing_trigger_markets,
            failed_markets=failed_markets,
        ),
    )


if __name__ == "__main__":
    main()
