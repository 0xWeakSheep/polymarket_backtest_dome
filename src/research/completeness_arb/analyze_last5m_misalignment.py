import argparse
import json
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from src.api.dome import DomeAPIError, DomeClient
from src.research.completeness_arb.logic import (
    FIFTEEN_MINUTE_STEP_SECONDS,
    MAX_ONE_MINUTE_RANGE_SECONDS,
    ONE_MINUTE_CANDLE_INTERVAL,
    SampleRecord,
    align_timestamp_to_step,
    analyze_path_misalignment,
    build_market_slugs,
    derive_child_five_minute_slugs,
    derive_path_pattern,
    market_is_btc_updown,
    resolve_market_outcome,
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


def parse_optional_int(value: object) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


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


def build_progress(
    *,
    fifteen_slug_prefix: str,
    five_slug_prefix: str,
    first_fifteen_slug_timestamp: int,
    next_fifteen_slug_timestamp: int,
    requested_15m_slugs: int,
    fetched_15m_markets: int,
    processed_15m_markets: int,
    eligible_up_up_samples: int,
    eligible_down_down_samples: int,
    hit_up_up_samples: int,
    hit_down_down_samples: int,
    no_hit_up_up_samples: int,
    no_hit_down_down_samples: int,
    failed_markets: int,
    last_completed_condition_id: Optional[str],
) -> Dict[str, object]:
    return {
        "fifteen_slug_prefix": fifteen_slug_prefix,
        "five_slug_prefix": five_slug_prefix,
        "first_fifteen_slug_timestamp": first_fifteen_slug_timestamp,
        "next_fifteen_slug_timestamp": next_fifteen_slug_timestamp,
        "requested_15m_slugs": requested_15m_slugs,
        "fetched_15m_markets": fetched_15m_markets,
        "processed_15m_markets": processed_15m_markets,
        "eligible_up_up_samples": eligible_up_up_samples,
        "eligible_down_down_samples": eligible_down_down_samples,
        "hit_up_up_samples": hit_up_up_samples,
        "hit_down_down_samples": hit_down_down_samples,
        "no_hit_up_up_samples": no_hit_up_up_samples,
        "no_hit_down_down_samples": no_hit_down_down_samples,
        "failed_markets": failed_markets,
        "last_completed_condition_id": last_completed_condition_id,
    }


def build_summary(
    *,
    fifteen_slug_prefix: str,
    five_slug_prefix: str,
    first_fifteen_slug_timestamp: int,
    requested_15m_slugs: int,
    fetched_15m_markets: int,
    processed_15m_markets: int,
    eligible_up_up_samples: int,
    eligible_down_down_samples: int,
    hit_up_up_samples: int,
    hit_down_down_samples: int,
    no_hit_up_up_samples: int,
    no_hit_down_down_samples: int,
    failed_markets: int,
) -> Dict[str, object]:
    eligible_total = eligible_up_up_samples + eligible_down_down_samples
    hit_total = hit_up_up_samples + hit_down_down_samples
    return {
        "strategy_name": "btc_15m_last5m_misalignment",
        "price_field": "close_dollars",
        "arb_condition": "15m_target_side + last5m_opposite_side < 1.0",
        "fifteen_slug_prefix": fifteen_slug_prefix,
        "five_slug_prefix": five_slug_prefix,
        "first_fifteen_slug_timestamp": first_fifteen_slug_timestamp,
        "requested_15m_slugs": requested_15m_slugs,
        "fetched_15m_markets": fetched_15m_markets,
        "processed_15m_markets": processed_15m_markets,
        "eligible_up_up_samples": eligible_up_up_samples,
        "hit_up_up_samples": hit_up_up_samples,
        "hit_rate_up_up": round(hit_up_up_samples / eligible_up_up_samples, 6) if eligible_up_up_samples else 0.0,
        "eligible_down_down_samples": eligible_down_down_samples,
        "hit_down_down_samples": hit_down_down_samples,
        "hit_rate_down_down": round(hit_down_down_samples / eligible_down_down_samples, 6) if eligible_down_down_samples else 0.0,
        "no_hit_up_up_samples": no_hit_up_up_samples,
        "no_hit_down_down_samples": no_hit_down_down_samples,
        "eligible_total": eligible_total,
        "hit_total": hit_total,
        "hit_rate_total": round(hit_total / eligible_total, 6) if eligible_total else 0.0,
        "failed_markets": failed_markets,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze cross-tenor completeness arbitrage for BTC 15m markets against the last 5m market. "
            "Only samples where the first two 5m markets resolve in the same direction enter the denominator."
        )
    )
    parser.add_argument("--fifteen-slug-prefix", default="btc-updown-15m")
    parser.add_argument("--five-slug-prefix", default="btc-updown-5m")
    parser.add_argument("--first-five-slug-timestamp", type=int, default=1770932400)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument(
        "--progress-json",
        default="data/processed/completeness_arb/btc_15m_last5m_misalignment/progress.json",
    )
    parser.add_argument(
        "--summary-json",
        default="data/processed/completeness_arb/btc_15m_last5m_misalignment/summary.json",
    )
    parser.add_argument(
        "--sample-records-jsonl",
        default="data/processed/completeness_arb/btc_15m_last5m_misalignment/sample_records.jsonl",
    )
    parser.add_argument(
        "--opportunity-records-jsonl",
        default="data/processed/completeness_arb/btc_15m_last5m_misalignment/opportunity_records.jsonl",
    )
    parser.add_argument(
        "--miss-records-jsonl",
        default="data/processed/completeness_arb/btc_15m_last5m_misalignment/miss_records.jsonl",
    )
    parser.add_argument(
        "--failed-markets-jsonl",
        default="data/processed/completeness_arb/btc_15m_last5m_misalignment/failed_markets.jsonl",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-markets", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    first_fifteen_slug_timestamp = align_timestamp_to_step(args.first_five_slug_timestamp, FIFTEEN_MINUTE_STEP_SECONDS)
    aligned_now = align_timestamp_to_step(int(time.time()), FIFTEEN_MINUTE_STEP_SECONDS)

    progress_json = Path(args.progress_json)
    summary_json = Path(args.summary_json)
    sample_records_jsonl = Path(args.sample_records_jsonl)
    opportunity_records_jsonl = Path(args.opportunity_records_jsonl)
    miss_records_jsonl = Path(args.miss_records_jsonl)
    failed_markets_jsonl = Path(args.failed_markets_jsonl)

    state = load_json(progress_json) if args.resume else {}
    next_fifteen_slug_timestamp = int(state.get("next_fifteen_slug_timestamp") or first_fifteen_slug_timestamp)
    requested_15m_slugs = int(state.get("requested_15m_slugs") or 0)
    fetched_15m_markets = int(state.get("fetched_15m_markets") or 0)
    processed_15m_markets = int(state.get("processed_15m_markets") or 0)
    eligible_up_up_samples = int(state.get("eligible_up_up_samples") or 0)
    eligible_down_down_samples = int(state.get("eligible_down_down_samples") or 0)
    hit_up_up_samples = int(state.get("hit_up_up_samples") or 0)
    hit_down_down_samples = int(state.get("hit_down_down_samples") or 0)
    no_hit_up_up_samples = int(state.get("no_hit_up_up_samples") or 0)
    no_hit_down_down_samples = int(state.get("no_hit_down_down_samples") or 0)
    failed_markets = int(state.get("failed_markets") or 0)
    last_completed_condition_id = state.get("last_completed_condition_id")
    if last_completed_condition_id is not None:
        last_completed_condition_id = str(last_completed_condition_id)

    seen_samples = load_ids(sample_records_jsonl, "fifteen_condition_id") if args.resume else set()
    seen_misses = load_ids(miss_records_jsonl, "fifteen_condition_id") if args.resume else set()
    seen_opportunity_keys = load_ids(opportunity_records_jsonl, "opportunity_key") if args.resume else set()

    client = DomeClient()
    analyzed_in_this_run = 0

    write_json(
        progress_json,
        build_progress(
            fifteen_slug_prefix=args.fifteen_slug_prefix,
            five_slug_prefix=args.five_slug_prefix,
            first_fifteen_slug_timestamp=first_fifteen_slug_timestamp,
            next_fifteen_slug_timestamp=next_fifteen_slug_timestamp,
            requested_15m_slugs=requested_15m_slugs,
            fetched_15m_markets=fetched_15m_markets,
            processed_15m_markets=processed_15m_markets,
            eligible_up_up_samples=eligible_up_up_samples,
            eligible_down_down_samples=eligible_down_down_samples,
            hit_up_up_samples=hit_up_up_samples,
            hit_down_down_samples=hit_down_down_samples,
            no_hit_up_up_samples=no_hit_up_up_samples,
            no_hit_down_down_samples=no_hit_down_down_samples,
            failed_markets=failed_markets,
            last_completed_condition_id=last_completed_condition_id,
        ),
    )
    write_json(
        summary_json,
        build_summary(
            fifteen_slug_prefix=args.fifteen_slug_prefix,
            five_slug_prefix=args.five_slug_prefix,
            first_fifteen_slug_timestamp=first_fifteen_slug_timestamp,
            requested_15m_slugs=requested_15m_slugs,
            fetched_15m_markets=fetched_15m_markets,
            processed_15m_markets=processed_15m_markets,
            eligible_up_up_samples=eligible_up_up_samples,
            eligible_down_down_samples=eligible_down_down_samples,
            hit_up_up_samples=hit_up_up_samples,
            hit_down_down_samples=hit_down_down_samples,
            no_hit_up_up_samples=no_hit_up_up_samples,
            no_hit_down_down_samples=no_hit_down_down_samples,
            failed_markets=failed_markets,
        ),
    )

    while next_fifteen_slug_timestamp <= aligned_now:
        slug_batch = build_market_slugs(
            slug_prefix=args.fifteen_slug_prefix,
            start_timestamp=next_fifteen_slug_timestamp,
            step_seconds=FIFTEEN_MINUTE_STEP_SECONDS,
            batch_size=args.batch_size,
            end_timestamp=aligned_now,
        )
        if not slug_batch:
            break

        requested_15m_slugs += len(slug_batch)
        try:
            payload = client._request_json(
                "/polymarket/markets",
                params={"market_slug": slug_batch, "status": "closed", "limit": len(slug_batch)},
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
            failed_markets += 1
            next_fifteen_slug_timestamp += FIFTEEN_MINUTE_STEP_SECONDS * len(slug_batch)
            write_json(
                progress_json,
                build_progress(
                    fifteen_slug_prefix=args.fifteen_slug_prefix,
                    five_slug_prefix=args.five_slug_prefix,
                    first_fifteen_slug_timestamp=first_fifteen_slug_timestamp,
                    next_fifteen_slug_timestamp=next_fifteen_slug_timestamp,
                    requested_15m_slugs=requested_15m_slugs,
                    fetched_15m_markets=fetched_15m_markets,
                    processed_15m_markets=processed_15m_markets,
                    eligible_up_up_samples=eligible_up_up_samples,
                    eligible_down_down_samples=eligible_down_down_samples,
                    hit_up_up_samples=hit_up_up_samples,
                    hit_down_down_samples=hit_down_down_samples,
                    no_hit_up_up_samples=no_hit_up_up_samples,
                    no_hit_down_down_samples=no_hit_down_down_samples,
                    failed_markets=failed_markets,
                    last_completed_condition_id=last_completed_condition_id,
                ),
            )
            continue

        markets = payload.get("markets", [])
        if not isinstance(markets, list):
            markets = []
        fetched_15m_markets += len(markets)

        for fifteen_market in markets:
            if not isinstance(fifteen_market, dict):
                continue
            if not market_is_btc_updown(fifteen_market, minutes=15):
                continue

            fifteen_condition_id = str(fifteen_market.get("condition_id") or "").strip()
            fifteen_market_slug = str(fifteen_market.get("market_slug") or "").strip()
            if not fifteen_condition_id or fifteen_condition_id in seen_samples:
                continue

            analyzed_in_this_run += 1
            print(f"Processing completeness arb market {processed_15m_markets + 1}: {fifteen_condition_id}")

            try:
                fifteen_slug_timestamp = int(fifteen_market_slug.rsplit("-", 1)[-1])
            except (TypeError, ValueError):
                failed_markets += 1
                append_jsonl(
                    failed_markets_jsonl,
                    {
                        "fifteen_condition_id": fifteen_condition_id,
                        "fifteen_market_slug": fifteen_market_slug,
                        "error": "invalid_15m_slug_timestamp",
                    },
                )
                continue

            five_1_slug, five_2_slug, five_3_slug = derive_child_five_minute_slugs(
                fifteen_slug_timestamp,
                five_slug_prefix=args.five_slug_prefix,
            )

            try:
                linked_payload = client._request_json(
                    "/polymarket/markets",
                    params={
                        "market_slug": [five_1_slug, five_2_slug, five_3_slug],
                        "status": "closed",
                        "limit": 3,
                    },
                )
            except DomeAPIError as exc:
                failed_markets += 1
                append_jsonl(
                    failed_markets_jsonl,
                    {
                        "fifteen_condition_id": fifteen_condition_id,
                        "fifteen_market_slug": fifteen_market_slug,
                        "error": str(exc),
                    },
                )
                continue

            linked_markets = linked_payload.get("markets", [])
            if not isinstance(linked_markets, list):
                linked_markets = []
            linked_by_slug = {
                str(item.get("market_slug") or ""): item
                for item in linked_markets
                if isinstance(item, dict) and market_is_btc_updown(item, minutes=5)
            }

            five_1_market = linked_by_slug.get(five_1_slug)
            five_2_market = linked_by_slug.get(five_2_slug)
            five_3_market = linked_by_slug.get(five_3_slug)

            base_sample = SampleRecord(
                fifteen_condition_id=fifteen_condition_id,
                fifteen_market_slug=fifteen_market_slug,
                fifteen_title=str(fifteen_market.get("title") or ""),
                fifteen_start_ts=parse_optional_int(fifteen_market.get("start_time")),
                fifteen_end_ts=parse_optional_int(fifteen_market.get("end_time")),
                five_1_slug=five_1_slug,
                five_2_slug=five_2_slug,
                five_3_slug=five_3_slug,
                five_1_outcome=None,
                five_2_outcome=None,
                path_pattern=None,
                eligible=False,
                direction_checked=None,
                hit=False,
                best_price_sum=None,
                best_edge=None,
                first_hit_ts=None,
                reason=None,
            )

            if five_1_market is None or five_2_market is None or five_3_market is None:
                base_sample.reason = "missing_5m_link"
                append_jsonl(sample_records_jsonl, base_sample.to_dict())
                seen_samples.add(fifteen_condition_id)
                processed_15m_markets += 1
                last_completed_condition_id = fifteen_condition_id
                continue

            five_1_outcome = resolve_market_outcome(five_1_market)
            five_2_outcome = resolve_market_outcome(five_2_market)
            path_pattern, direction_checked = derive_path_pattern(five_1_outcome, five_2_outcome)

            base_sample.five_1_outcome = five_1_outcome
            base_sample.five_2_outcome = five_2_outcome
            base_sample.path_pattern = path_pattern
            base_sample.direction_checked = direction_checked

            if path_pattern is None:
                base_sample.reason = "first_two_5m_not_same_direction"
                append_jsonl(sample_records_jsonl, base_sample.to_dict())
                seen_samples.add(fifteen_condition_id)
                processed_15m_markets += 1
                last_completed_condition_id = fifteen_condition_id
                continue

            if path_pattern == "up_up":
                eligible_up_up_samples += 1
            else:
                eligible_down_down_samples += 1

            try:
                third_start = int(five_3_market.get("start_time"))
                third_end = int(five_3_market.get("end_time"))
            except (TypeError, ValueError):
                failed_markets += 1
                base_sample.eligible = True
                base_sample.reason = "invalid_third_window"
                append_jsonl(sample_records_jsonl, base_sample.to_dict())
                seen_samples.add(fifteen_condition_id)
                processed_15m_markets += 1
                last_completed_condition_id = fifteen_condition_id
                continue

            try:
                analyzed_sample, opportunities = analyze_path_misalignment(
                    fifteen_market=fifteen_market,
                    third_five_market=five_3_market,
                    path_pattern=path_pattern,
                    fifteen_candle_payloads=iter_candle_payloads(
                        client,
                        condition_id=fifteen_condition_id,
                        start_time=third_start,
                        end_time=third_end,
                    ),
                    third_five_candle_payloads=iter_candle_payloads(
                        client,
                        condition_id=str(five_3_market.get("condition_id") or ""),
                        start_time=third_start,
                        end_time=third_end,
                    ),
                )
            except DomeAPIError as exc:
                failed_markets += 1
                append_jsonl(
                    failed_markets_jsonl,
                    {
                        "fifteen_condition_id": fifteen_condition_id,
                        "fifteen_market_slug": fifteen_market_slug,
                        "error": str(exc),
                    },
                )
                continue

            if analyzed_sample is None:
                failed_markets += 1
                append_jsonl(
                    failed_markets_jsonl,
                    {
                        "fifteen_condition_id": fifteen_condition_id,
                        "fifteen_market_slug": fifteen_market_slug,
                        "error": "analysis_returned_none",
                    },
                )
                continue

            analyzed_sample.five_1_slug = five_1_slug
            analyzed_sample.five_2_slug = five_2_slug
            analyzed_sample.five_3_slug = five_3_slug
            analyzed_sample.five_1_outcome = five_1_outcome
            analyzed_sample.five_2_outcome = five_2_outcome
            append_jsonl(sample_records_jsonl, analyzed_sample.to_dict())
            seen_samples.add(fifteen_condition_id)

            if opportunities:
                if path_pattern == "up_up":
                    hit_up_up_samples += 1
                else:
                    hit_down_down_samples += 1
                for opportunity in opportunities:
                    row = opportunity.to_dict()
                    opportunity_key = f"{row['fifteen_condition_id']}:{row['trigger_ts']}:{row['path_pattern']}"
                    if opportunity_key in seen_opportunity_keys:
                        continue
                    row["opportunity_key"] = opportunity_key
                    append_jsonl(opportunity_records_jsonl, row)
                    seen_opportunity_keys.add(opportunity_key)
            else:
                if path_pattern == "up_up":
                    no_hit_up_up_samples += 1
                else:
                    no_hit_down_down_samples += 1
                if fifteen_condition_id not in seen_misses:
                    append_jsonl(miss_records_jsonl, analyzed_sample.to_dict())
                    seen_misses.add(fifteen_condition_id)

            processed_15m_markets += 1
            last_completed_condition_id = fifteen_condition_id

            write_json(
                progress_json,
                build_progress(
                    fifteen_slug_prefix=args.fifteen_slug_prefix,
                    five_slug_prefix=args.five_slug_prefix,
                    first_fifteen_slug_timestamp=first_fifteen_slug_timestamp,
                    next_fifteen_slug_timestamp=next_fifteen_slug_timestamp,
                    requested_15m_slugs=requested_15m_slugs,
                    fetched_15m_markets=fetched_15m_markets,
                    processed_15m_markets=processed_15m_markets,
                    eligible_up_up_samples=eligible_up_up_samples,
                    eligible_down_down_samples=eligible_down_down_samples,
                    hit_up_up_samples=hit_up_up_samples,
                    hit_down_down_samples=hit_down_down_samples,
                    no_hit_up_up_samples=no_hit_up_up_samples,
                    no_hit_down_down_samples=no_hit_down_down_samples,
                    failed_markets=failed_markets,
                    last_completed_condition_id=last_completed_condition_id,
                ),
            )
            write_json(
                summary_json,
                build_summary(
                    fifteen_slug_prefix=args.fifteen_slug_prefix,
                    five_slug_prefix=args.five_slug_prefix,
                    first_fifteen_slug_timestamp=first_fifteen_slug_timestamp,
                    requested_15m_slugs=requested_15m_slugs,
                    fetched_15m_markets=fetched_15m_markets,
                    processed_15m_markets=processed_15m_markets,
                    eligible_up_up_samples=eligible_up_up_samples,
                    eligible_down_down_samples=eligible_down_down_samples,
                    hit_up_up_samples=hit_up_up_samples,
                    hit_down_down_samples=hit_down_down_samples,
                    no_hit_up_up_samples=no_hit_up_up_samples,
                    no_hit_down_down_samples=no_hit_down_down_samples,
                    failed_markets=failed_markets,
                ),
            )

            if processed_15m_markets % 25 == 0:
                print(
                    f"Processed {processed_15m_markets} 15m markets | "
                    f"eligible up_up={eligible_up_up_samples}, down_down={eligible_down_down_samples} | "
                    f"hits up_up={hit_up_up_samples}, down_down={hit_down_down_samples}"
                )

            if args.max_markets and analyzed_in_this_run >= args.max_markets:
                break

        next_fifteen_slug_timestamp += FIFTEEN_MINUTE_STEP_SECONDS * len(slug_batch)
        write_json(
            progress_json,
            build_progress(
                fifteen_slug_prefix=args.fifteen_slug_prefix,
                five_slug_prefix=args.five_slug_prefix,
                first_fifteen_slug_timestamp=first_fifteen_slug_timestamp,
                next_fifteen_slug_timestamp=next_fifteen_slug_timestamp,
                requested_15m_slugs=requested_15m_slugs,
                fetched_15m_markets=fetched_15m_markets,
                processed_15m_markets=processed_15m_markets,
                eligible_up_up_samples=eligible_up_up_samples,
                eligible_down_down_samples=eligible_down_down_samples,
                hit_up_up_samples=hit_up_up_samples,
                hit_down_down_samples=hit_down_down_samples,
                no_hit_up_up_samples=no_hit_up_up_samples,
                no_hit_down_down_samples=no_hit_down_down_samples,
                failed_markets=failed_markets,
                last_completed_condition_id=last_completed_condition_id,
            ),
        )
        write_json(
            summary_json,
            build_summary(
                fifteen_slug_prefix=args.fifteen_slug_prefix,
                five_slug_prefix=args.five_slug_prefix,
                first_fifteen_slug_timestamp=first_fifteen_slug_timestamp,
                requested_15m_slugs=requested_15m_slugs,
                fetched_15m_markets=fetched_15m_markets,
                processed_15m_markets=processed_15m_markets,
                eligible_up_up_samples=eligible_up_up_samples,
                eligible_down_down_samples=eligible_down_down_samples,
                hit_up_up_samples=hit_up_up_samples,
                hit_down_down_samples=hit_down_down_samples,
                no_hit_up_up_samples=no_hit_up_up_samples,
                no_hit_down_down_samples=no_hit_down_down_samples,
                failed_markets=failed_markets,
            ),
        )

        if args.max_markets and analyzed_in_this_run >= args.max_markets:
            break

    final_summary = build_summary(
        fifteen_slug_prefix=args.fifteen_slug_prefix,
        five_slug_prefix=args.five_slug_prefix,
        first_fifteen_slug_timestamp=first_fifteen_slug_timestamp,
        requested_15m_slugs=requested_15m_slugs,
        fetched_15m_markets=fetched_15m_markets,
        processed_15m_markets=processed_15m_markets,
        eligible_up_up_samples=eligible_up_up_samples,
        eligible_down_down_samples=eligible_down_down_samples,
        hit_up_up_samples=hit_up_up_samples,
        hit_down_down_samples=hit_down_down_samples,
        no_hit_up_up_samples=no_hit_up_up_samples,
        no_hit_down_down_samples=no_hit_down_down_samples,
        failed_markets=failed_markets,
    )
    write_json(summary_json, final_summary)
    print(json.dumps(final_summary, indent=2, ensure_ascii=True))
    print(f"Progress: {progress_json}")
    print(f"Summary: {summary_json}")
    print(f"Samples: {sample_records_jsonl}")
    print(f"Opportunities: {opportunity_records_jsonl}")
    print(f"Misses: {miss_records_jsonl}")
    print(f"Failures: {failed_markets_jsonl}")


if __name__ == "__main__":
    main()
