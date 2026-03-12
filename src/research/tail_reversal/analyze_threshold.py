import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Set

from src.api.dome import DomeClient
from src.research.tail_reversal.logic import MarketAnalysis, analyze_market


def load_existing_results(results_path: Path) -> List[Dict[str, object]]:
    if not results_path.exists():
        return []

    rows: List[Dict[str, object]] = []
    with results_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def append_result(results_path: Path, row: Dict[str, object]) -> None:
    results_path.parent.mkdir(parents=True, exist_ok=True)
    with results_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def write_csv(rows: List[Dict[str, object]], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        csv_path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_summary(rows: List[Dict[str, object]], threshold: float) -> Dict[str, object]:
    resolved_markets = len(rows)
    any_touch = sum(1 for row in rows if row["any_side_touched_threshold"])
    reversals = sum(1 for row in rows if row["losing_side_touched_threshold"])
    with_trades = sum(1 for row in rows if int(row["trade_count"]) > 0)

    return {
        "threshold": threshold,
        "resolved_markets_analyzed": resolved_markets,
        "markets_with_trades": with_trades,
        "markets_where_any_side_touched_threshold": any_touch,
        "reversal_count": reversals,
        "reversal_rate_overall": (reversals / resolved_markets) if resolved_markets else None,
        "reversal_rate_given_any_side_touched_threshold": (reversals / any_touch) if any_touch else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze how often a closed Polymarket market reversed after touching a threshold price."
    )
    parser.add_argument("--threshold", type=float, default=0.95, help="Threshold to test, for example 0.95")
    parser.add_argument(
        "--results-jsonl",
        default="data/processed/tail_reversal_095_results.jsonl",
        help="Path to append per-market analysis results.",
    )
    parser.add_argument(
        "--results-csv",
        default="data/processed/tail_reversal_095_results.csv",
        help="Path to write the final CSV export.",
    )
    parser.add_argument(
        "--summary-json",
        default="reports/tables/tail_reversal_095_summary.json",
        help="Path to write the final summary JSON.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from existing jsonl results and skip already analyzed condition_ids.",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=0,
        help="Optional cap for debugging. 0 means analyze all closed markets.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    threshold = args.threshold
    results_jsonl = Path(args.results_jsonl)
    results_csv = Path(args.results_csv)
    summary_json = Path(args.summary_json)

    existing_rows = load_existing_results(results_jsonl) if args.resume else []
    processed_condition_ids: Set[str] = {str(row["condition_id"]) for row in existing_rows}
    all_rows = list(existing_rows)

    client = DomeClient()
    analyzed_count = 0

    for market in client.iter_closed_markets():
        condition_id = str(market.get("condition_id") or "").strip()
        if not condition_id or condition_id in processed_condition_ids:
            continue

        analysis: MarketAnalysis | None = analyze_market(
            market=market,
            orders=client.iter_orders_for_condition(condition_id),
            threshold=threshold,
        )
        if analysis is None:
            continue

        row = analysis.to_dict()
        append_result(results_jsonl, row)
        all_rows.append(row)
        processed_condition_ids.add(condition_id)
        analyzed_count += 1

        total_done = len(all_rows)
        if total_done % 25 == 0:
            print(
                f"Processed {total_done} markets | threshold={threshold} | current reversals="
                f"{sum(1 for item in all_rows if item['losing_side_touched_threshold'])}"
            )

        if args.max_markets and analyzed_count >= args.max_markets:
            break

    write_csv(all_rows, results_csv)

    summary = build_summary(all_rows, threshold)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=True))
    print(f"Per-market results: {results_jsonl}")
    print(f"CSV export: {results_csv}")
    print(f"Summary: {summary_json}")


if __name__ == "__main__":
    main()

