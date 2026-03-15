import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from src.api.dome import DomeAPIError, DomeClient
from src.research.direct_yes_no_arb.logic import (
    ORDERBOOK_HISTORY_START_MS,
    analyze_direct_arb,
    build_orderbook_snapshots,
    extract_yes_no_token_ids,
    market_is_yes_no_binary,
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


def load_ids(path: Path, key: str) -> Set[str]:
    if not path.exists():
        return set()

    values: Set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            value = str(row.get(key) or "").strip()
            if value:
                values.add(value)
    return values


def iter_orderbook_snapshots(
    client: DomeClient,
    *,
    token_id: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int,
) -> Iterable[Dict[str, object]]:
    pagination_key: Optional[str] = None
    while True:
        params: Dict[str, object] = {
            "token_id": token_id,
            "start_time": start_time_ms,
            "end_time": end_time_ms,
            "limit": limit,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

        payload = client._request_json("/polymarket/orderbooks", params=params)
        snapshots = payload.get("snapshots", [])
        if isinstance(snapshots, list):
            for snapshot in snapshots:
                if isinstance(snapshot, dict):
                    yield snapshot

        pagination = payload.get("pagination") or {}
        if not isinstance(pagination, dict) or not pagination.get("has_more"):
            break
        pagination_key = str(pagination.get("pagination_key") or pagination.get("paginationKey") or "").strip()
        if not pagination_key:
            break


def build_progress(
    *,
    requested_pages: int,
    fetched_closed_markets: int,
    matched_yes_no_markets: int,
    processed_markets: int,
    opportunity_markets: int,
    opportunity_count: int,
    no_opportunity_markets: int,
    failed_markets: int,
    skipped_pre_orderbook_markets: int,
    next_pagination_key: Optional[str],
    last_completed_condition_id: Optional[str],
    current_condition_id: Optional[str],
    current_market_slug: Optional[str],
) -> Dict[str, object]:
    return {
        "strategy_name": "direct_yes_no_lt_1_orderbook",
        "price_field": "best_ask",
        "arb_condition": "yes_ask + no_ask < 1.0",
        "market_scope": "closed yes/no markets with orderbook history coverage",
        "orderbook_history_start_ms": ORDERBOOK_HISTORY_START_MS,
        "requested_pages": requested_pages,
        "fetched_closed_markets": fetched_closed_markets,
        "matched_yes_no_markets": matched_yes_no_markets,
        "processed_markets": processed_markets,
        "opportunity_markets": opportunity_markets,
        "opportunity_count": opportunity_count,
        "no_opportunity_markets": no_opportunity_markets,
        "failed_markets": failed_markets,
        "skipped_pre_orderbook_markets": skipped_pre_orderbook_markets,
        "next_pagination_key": next_pagination_key,
        "last_completed_condition_id": last_completed_condition_id,
        "current_condition_id": current_condition_id,
        "current_market_slug": current_market_slug,
    }


def build_summary(
    *,
    requested_pages: int,
    fetched_closed_markets: int,
    matched_yes_no_markets: int,
    processed_markets: int,
    opportunity_markets: int,
    opportunity_count: int,
    no_opportunity_markets: int,
    failed_markets: int,
    skipped_pre_orderbook_markets: int,
) -> Dict[str, object]:
    opportunity_market_rate = round(opportunity_markets / processed_markets, 6) if processed_markets else 0.0
    return {
        "strategy_name": "direct_yes_no_lt_1_orderbook",
        "price_field": "best_ask",
        "arb_condition": "yes_ask + no_ask < 1.0",
        "market_scope": "closed yes/no markets with orderbook history coverage",
        "orderbook_history_start_ms": ORDERBOOK_HISTORY_START_MS,
        "requested_pages": requested_pages,
        "fetched_closed_markets": fetched_closed_markets,
        "matched_yes_no_markets": matched_yes_no_markets,
        "processed_markets": processed_markets,
        "opportunity_markets": opportunity_markets,
        "opportunity_count": opportunity_count,
        "opportunity_market_rate": opportunity_market_rate,
        "no_opportunity_markets": no_opportunity_markets,
        "failed_markets": failed_markets,
        "skipped_pre_orderbook_markets": skipped_pre_orderbook_markets,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backtest direct yes/no arbitrage using historical orderbook best asks only. "
            "The scan starts from the first timestamp with Dome orderbook history support."
        )
    )
    parser.add_argument("--progress-json", default="data/processed/direct_yes_no_arb/progress.json")
    parser.add_argument("--summary-json", default="data/processed/direct_yes_no_arb/summary.json")
    parser.add_argument("--sample-records-jsonl", default="data/processed/direct_yes_no_arb/sample_records.jsonl")
    parser.add_argument("--opportunity-records-jsonl", default="data/processed/direct_yes_no_arb/opportunity_records.jsonl")
    parser.add_argument("--failed-markets-jsonl", default="data/processed/direct_yes_no_arb/failed_markets.jsonl")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--page-limit", type=int, default=100)
    parser.add_argument("--snapshot-limit", type=int, default=200)
    parser.add_argument("--max-markets", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    progress_json = Path(args.progress_json)
    summary_json = Path(args.summary_json)
    sample_records_jsonl = Path(args.sample_records_jsonl)
    opportunity_records_jsonl = Path(args.opportunity_records_jsonl)
    failed_markets_jsonl = Path(args.failed_markets_jsonl)

    state = load_json(progress_json) if args.resume else {}
    requested_pages = int(state.get("requested_pages") or 0)
    fetched_closed_markets = int(state.get("fetched_closed_markets") or 0)
    matched_yes_no_markets = int(state.get("matched_yes_no_markets") or 0)
    processed_markets = int(state.get("processed_markets") or 0)
    opportunity_markets = int(state.get("opportunity_markets") or 0)
    opportunity_count = int(state.get("opportunity_count") or 0)
    no_opportunity_markets = int(state.get("no_opportunity_markets") or 0)
    failed_markets = int(state.get("failed_markets") or 0)
    skipped_pre_orderbook_markets = int(state.get("skipped_pre_orderbook_markets") or 0)
    next_pagination_key = state.get("next_pagination_key")
    if next_pagination_key is not None:
        next_pagination_key = str(next_pagination_key)
    last_completed_condition_id = state.get("last_completed_condition_id")
    if last_completed_condition_id is not None:
        last_completed_condition_id = str(last_completed_condition_id)
    current_condition_id = state.get("current_condition_id")
    if current_condition_id is not None:
        current_condition_id = str(current_condition_id)
    current_market_slug = state.get("current_market_slug")
    if current_market_slug is not None:
        current_market_slug = str(current_market_slug)

    seen_samples = load_ids(sample_records_jsonl, "condition_id") if args.resume else set()
    seen_opportunities = load_ids(opportunity_records_jsonl, "opportunity_key") if args.resume else set()
    analyzed_in_this_run = 0
    client = DomeClient()

    write_json(
        progress_json,
        build_progress(
            requested_pages=requested_pages,
            fetched_closed_markets=fetched_closed_markets,
            matched_yes_no_markets=matched_yes_no_markets,
            processed_markets=processed_markets,
            opportunity_markets=opportunity_markets,
            opportunity_count=opportunity_count,
            no_opportunity_markets=no_opportunity_markets,
            failed_markets=failed_markets,
            skipped_pre_orderbook_markets=skipped_pre_orderbook_markets,
            next_pagination_key=next_pagination_key,
            last_completed_condition_id=last_completed_condition_id,
            current_condition_id=current_condition_id,
            current_market_slug=current_market_slug,
        ),
    )
    write_json(
        summary_json,
        build_summary(
            requested_pages=requested_pages,
            fetched_closed_markets=fetched_closed_markets,
            matched_yes_no_markets=matched_yes_no_markets,
            processed_markets=processed_markets,
            opportunity_markets=opportunity_markets,
            opportunity_count=opportunity_count,
            no_opportunity_markets=no_opportunity_markets,
            failed_markets=failed_markets,
            skipped_pre_orderbook_markets=skipped_pre_orderbook_markets,
        ),
    )

    print("Scanning closed yes/no markets with historical orderbook best asks only")

    try:
        page_iter = client.iter_closed_market_pages(start_pagination_key=next_pagination_key)
        for page in page_iter:
            requested_pages += 1
            markets = page.get("items") or []
            if not isinstance(markets, list):
                markets = []
            fetched_closed_markets += len(markets)
            print(
                f"Processing closed market page {requested_pages} | "
                f"page_key={page.get('page_pagination_key') or '<initial>'} | markets={len(markets)}"
            )

            for market in markets:
                if not isinstance(market, dict) or not market_is_yes_no_binary(market):
                    continue

                condition_id = str(market.get("condition_id") or "").strip()
                if not condition_id or condition_id in seen_samples:
                    continue

                token_ids = extract_yes_no_token_ids(market)
                if token_ids is None:
                    continue

                try:
                    start_time = int(market.get("start_time"))
                    end_time = int(market.get("end_time"))
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
                    last_completed_condition_id = condition_id
                    continue

                analysis_start_ms = max(start_time * 1000, ORDERBOOK_HISTORY_START_MS)
                analysis_end_ms = end_time * 1000
                matched_yes_no_markets += 1

                if analysis_end_ms < ORDERBOOK_HISTORY_START_MS or analysis_start_ms >= analysis_end_ms:
                    skipped_pre_orderbook_markets += 1
                    last_completed_condition_id = condition_id
                    continue

                analyzed_in_this_run += 1
                current_condition_id = condition_id
                current_market_slug = str(market.get("market_slug") or "")
                print(
                    f"Processing orderbook market {processed_markets + 1}: "
                    f"{condition_id} | {current_market_slug}"
                )
                write_json(
                    progress_json,
                    build_progress(
                        requested_pages=requested_pages,
                        fetched_closed_markets=fetched_closed_markets,
                        matched_yes_no_markets=matched_yes_no_markets,
                        processed_markets=processed_markets,
                        opportunity_markets=opportunity_markets,
                        opportunity_count=opportunity_count,
                        no_opportunity_markets=no_opportunity_markets,
                        failed_markets=failed_markets,
                        skipped_pre_orderbook_markets=skipped_pre_orderbook_markets,
                        next_pagination_key=next_pagination_key,
                        last_completed_condition_id=last_completed_condition_id,
                        current_condition_id=current_condition_id,
                        current_market_slug=current_market_slug,
                    ),
                )

                yes_token_id, no_token_id = token_ids
                try:
                    yes_snapshots = build_orderbook_snapshots(
                        yes_token_id,
                        iter_orderbook_snapshots(
                            client,
                            token_id=yes_token_id,
                            start_time_ms=analysis_start_ms,
                            end_time_ms=analysis_end_ms,
                            limit=args.snapshot_limit,
                        ),
                    )
                    no_snapshots = build_orderbook_snapshots(
                        no_token_id,
                        iter_orderbook_snapshots(
                            client,
                            token_id=no_token_id,
                            start_time_ms=analysis_start_ms,
                            end_time_ms=analysis_end_ms,
                            limit=args.snapshot_limit,
                        ),
                    )
                    sample, opportunities = analyze_direct_arb(
                        market,
                        yes_snapshots=yes_snapshots,
                        no_snapshots=no_snapshots,
                        analysis_start_ms=analysis_start_ms,
                        analysis_end_ms=analysis_end_ms,
                    )
                except (DomeAPIError, Exception) as exc:
                    failed_markets += 1
                    append_jsonl(
                        failed_markets_jsonl,
                        {
                            "condition_id": condition_id,
                            "market_slug": current_market_slug,
                            "title": str(market.get("title") or ""),
                            "error": str(exc),
                        },
                    )
                    last_completed_condition_id = condition_id
                    current_condition_id = None
                    current_market_slug = None
                    continue

                if sample is None:
                    continue

                append_jsonl(sample_records_jsonl, sample.to_dict())
                seen_samples.add(condition_id)
                processed_markets += 1
                last_completed_condition_id = condition_id
                current_condition_id = None
                current_market_slug = None

                if opportunities:
                    opportunity_markets += 1
                    opportunity_count += len(opportunities)
                    for record in opportunities:
                        if record.opportunity_key in seen_opportunities:
                            continue
                        append_jsonl(opportunity_records_jsonl, record.to_dict())
                        seen_opportunities.add(record.opportunity_key)
                else:
                    no_opportunity_markets += 1

                write_json(
                    progress_json,
                    build_progress(
                        requested_pages=requested_pages,
                        fetched_closed_markets=fetched_closed_markets,
                        matched_yes_no_markets=matched_yes_no_markets,
                        processed_markets=processed_markets,
                        opportunity_markets=opportunity_markets,
                        opportunity_count=opportunity_count,
                        no_opportunity_markets=no_opportunity_markets,
                        failed_markets=failed_markets,
                        skipped_pre_orderbook_markets=skipped_pre_orderbook_markets,
                        next_pagination_key=next_pagination_key,
                        last_completed_condition_id=last_completed_condition_id,
                        current_condition_id=current_condition_id,
                        current_market_slug=current_market_slug,
                    ),
                )
                write_json(
                    summary_json,
                    build_summary(
                        requested_pages=requested_pages,
                        fetched_closed_markets=fetched_closed_markets,
                        matched_yes_no_markets=matched_yes_no_markets,
                        processed_markets=processed_markets,
                        opportunity_markets=opportunity_markets,
                        opportunity_count=opportunity_count,
                        no_opportunity_markets=no_opportunity_markets,
                        failed_markets=failed_markets,
                        skipped_pre_orderbook_markets=skipped_pre_orderbook_markets,
                    ),
                )

                if processed_markets % 10 == 0:
                    print(
                        f"Processed {processed_markets} orderbook-backed yes/no markets | "
                        f"opportunity_markets={opportunity_markets} | opportunity_count={opportunity_count} | "
                        f"failed={failed_markets}"
                    )

                if args.max_markets and analyzed_in_this_run >= args.max_markets:
                    break

            next_pagination_key = page.get("next_pagination_key")
            if next_pagination_key is not None:
                next_pagination_key = str(next_pagination_key)

            write_json(
                progress_json,
                build_progress(
                    requested_pages=requested_pages,
                    fetched_closed_markets=fetched_closed_markets,
                    matched_yes_no_markets=matched_yes_no_markets,
                    processed_markets=processed_markets,
                    opportunity_markets=opportunity_markets,
                    opportunity_count=opportunity_count,
                    no_opportunity_markets=no_opportunity_markets,
                    failed_markets=failed_markets,
                    skipped_pre_orderbook_markets=skipped_pre_orderbook_markets,
                    next_pagination_key=next_pagination_key,
                    last_completed_condition_id=last_completed_condition_id,
                    current_condition_id=current_condition_id,
                    current_market_slug=current_market_slug,
                ),
            )

            if args.max_markets and analyzed_in_this_run >= args.max_markets:
                break
    except (DomeAPIError, Exception) as exc:
        failed_markets += 1
        append_jsonl(
            failed_markets_jsonl,
            {
                "page_error": True,
                "next_pagination_key": next_pagination_key,
                "error": str(exc),
            },
        )

    write_json(
        progress_json,
        build_progress(
            requested_pages=requested_pages,
            fetched_closed_markets=fetched_closed_markets,
            matched_yes_no_markets=matched_yes_no_markets,
            processed_markets=processed_markets,
            opportunity_markets=opportunity_markets,
            opportunity_count=opportunity_count,
            no_opportunity_markets=no_opportunity_markets,
            failed_markets=failed_markets,
            skipped_pre_orderbook_markets=skipped_pre_orderbook_markets,
            next_pagination_key=next_pagination_key,
            last_completed_condition_id=last_completed_condition_id,
            current_condition_id=current_condition_id,
            current_market_slug=current_market_slug,
        ),
    )
    write_json(
        summary_json,
        build_summary(
            requested_pages=requested_pages,
            fetched_closed_markets=fetched_closed_markets,
            matched_yes_no_markets=matched_yes_no_markets,
            processed_markets=processed_markets,
            opportunity_markets=opportunity_markets,
            opportunity_count=opportunity_count,
            no_opportunity_markets=no_opportunity_markets,
            failed_markets=failed_markets,
            skipped_pre_orderbook_markets=skipped_pre_orderbook_markets,
        ),
    )

    print(
        f"Completed orderbook yes/no scan | processed_markets={processed_markets} | "
        f"opportunity_markets={opportunity_markets} | opportunity_count={opportunity_count}"
    )


if __name__ == "__main__":
    main()
