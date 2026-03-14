"""Run: python3 data/chart/script/btc_5m_volatility_vs_arrival/plot_volatility_vs_arrival.py"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parents[4]
VOLATILITY_JSONL = ROOT_DIR / "data/processed/btc_5m_volatility/market_volatility.jsonl"
SELECTED_MARKETS_JSONL = ROOT_DIR / "data/raw/btc_5m_arrival/markets/selected_markets_complex.jsonl"
ARRIVAL_ROOT = ROOT_DIR / "data/processed/btc_5m_arrival"
PROCESSED_OUTPUT_DIR = ROOT_DIR / "data/processed/btc_5m_volatility"
CHART_OUTPUT_DIR = ROOT_DIR / "data/chart/result/btc_5m_volatility_vs_arrival"
THRESHOLDS = ["0.52", "0.53", "0.54", "0.55", "0.56", "0.57", "0.58"]
METRICS = [
    "return_std_1m_5m",
    "abs_return_sum_1m_5m",
    "realized_vol_1m_5m",
    "range_pct_5m",
    "net_move_pct_5m",
]
FOCUS_METRIC = "return_std_1m_5m"


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


def rank_values(values: List[float]) -> List[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    position = 0
    while position < len(indexed):
        next_position = position + 1
        while next_position < len(indexed) and indexed[next_position][1] == indexed[position][1]:
            next_position += 1
        average_rank = (position + next_position - 1) / 2 + 1
        for inner_position in range(position, next_position):
            original_index = indexed[inner_position][0]
            ranks[original_index] = average_rank
        position = next_position
    return ranks


def pearson_corr(x_values: List[float], y_values: List[float]) -> float:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return 0.0
    x_mean = sum(x_values) / len(x_values)
    y_mean = sum(y_values) / len(y_values)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
    x_var = sum((x - x_mean) ** 2 for x in x_values)
    y_var = sum((y - y_mean) ** 2 for y in y_values)
    if x_var <= 0 or y_var <= 0:
        return 0.0
    return numerator / math.sqrt(x_var * y_var)


def spearman_corr(x_values: List[float], y_values: List[float]) -> float:
    return pearson_corr(rank_values(x_values), rank_values(y_values))


def load_universe_condition_ids() -> set[str]:
    return {
        str(row.get("condition_id") or "")
        for row in load_jsonl(SELECTED_MARKETS_JSONL)
        if str(row.get("condition_id") or "").strip()
    }


def load_miss_sets(universe_condition_ids: set[str]) -> Dict[str, Dict[str, set[str]]]:
    miss_sets: Dict[str, Dict[str, set[str]]] = {}
    for threshold in THRESHOLDS:
        miss_sets[threshold] = {}
        for side in ["up", "down"]:
            miss_path = ARRIVAL_ROOT / threshold / f"{side}_misses.jsonl"
            miss_sets[threshold][side] = {
                str(row.get("condition_id") or "")
                for row in load_jsonl(miss_path)
                if str(row.get("condition_id") or "") in universe_condition_ids
            }
    return miss_sets


def build_analysis_rows() -> List[Dict[str, object]]:
    universe_condition_ids = load_universe_condition_ids()
    volatility_rows = {
        str(row.get("condition_id") or ""): row
        for row in load_jsonl(VOLATILITY_JSONL)
        if str(row.get("condition_id") or "") in universe_condition_ids
    }
    miss_sets = load_miss_sets(universe_condition_ids)

    analysis_rows: List[Dict[str, object]] = []
    for condition_id, row in sorted(
        volatility_rows.items(),
        key=lambda item: int(item[1].get("market_window_start_ts") or 0),
    ):
        output_row = {
            "condition_id": condition_id,
            "market_slug": row.get("market_slug"),
            "market_window_start_ts": row.get("market_window_start_ts"),
        }
        for metric in METRICS:
            output_row[metric] = float(row.get(metric) or 0.0)
        for threshold in THRESHOLDS:
            output_row[f"up_arrived_{threshold}"] = 0 if condition_id in miss_sets[threshold]["up"] else 1
            output_row[f"down_arrived_{threshold}"] = 0 if condition_id in miss_sets[threshold]["down"] else 1
        analysis_rows.append(output_row)
    return analysis_rows


def write_analysis_csv(rows: List[Dict[str, object]]) -> None:
    PROCESSED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_OUTPUT_DIR / "volatility_arrival_analysis.csv"
    fieldnames = ["condition_id", "market_slug", "market_window_start_ts", *METRICS]
    for threshold in THRESHOLDS:
        fieldnames.append(f"up_arrived_{threshold}")
        fieldnames.append(f"down_arrived_{threshold}")

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_correlation_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    correlation_rows: List[Dict[str, object]] = []
    for metric in METRICS:
        x_values = [float(row[metric]) for row in rows]
        for side in ["up", "down"]:
            for threshold in THRESHOLDS:
                y_values = [float(row[f"{side}_arrived_{threshold}"]) for row in rows]
                correlation_rows.append(
                    {
                        "metric": metric,
                        "side": side,
                        "threshold": threshold,
                        "sample_count": len(rows),
                        "pearson_corr": round(pearson_corr(x_values, y_values), 6),
                        "spearman_corr": round(spearman_corr(x_values, y_values), 6),
                    }
                )
    return correlation_rows


def write_correlation_csv(rows: List[Dict[str, object]]) -> None:
    output_path = PROCESSED_OUTPUT_DIR / "correlation_summary.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["metric", "side", "threshold", "sample_count", "pearson_corr", "spearman_corr"],
        )
        writer.writeheader()
        writer.writerows(rows)

    top_positive = sorted(rows, key=lambda row: (row["spearman_corr"], row["pearson_corr"]), reverse=True)
    top_path = PROCESSED_OUTPUT_DIR / "top_positive_correlations.csv"
    with top_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["metric", "side", "threshold", "sample_count", "pearson_corr", "spearman_corr"],
        )
        writer.writeheader()
        writer.writerows(top_positive[:20])


def split_into_quantile_bins(rows: List[Dict[str, object]], metric: str, bin_count: int = 10) -> List[List[Dict[str, object]]]:
    ordered_rows = sorted(rows, key=lambda row: float(row[metric]))
    if not ordered_rows:
        return []
    bins: List[List[Dict[str, object]]] = []
    for index in range(bin_count):
        start = index * len(ordered_rows) // bin_count
        end = (index + 1) * len(ordered_rows) // bin_count
        if start == end:
            continue
        bins.append(ordered_rows[start:end])
    return bins


def plot_quantile_arrival_lines(rows: List[Dict[str, object]], *, side: str) -> None:
    CHART_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    quantile_bins = split_into_quantile_bins(rows, FOCUS_METRIC, bin_count=10)
    if not quantile_bins:
        return

    x_labels = [f"Q{index + 1}" for index in range(len(quantile_bins))]
    fig, ax = plt.subplots(figsize=(12, 6))
    for threshold in THRESHOLDS:
        rates = []
        for group in quantile_bins:
            hit_rate = sum(float(row[f"{side}_arrived_{threshold}"]) for row in group) / len(group)
            rates.append(hit_rate)
        ax.plot(x_labels, rates, marker="o", linewidth=1.8, label=threshold)

    ax.set_title(f"Polymarket Arrival vs Pre-5m Volatility Quantiles ({side.capitalize()})")
    ax.set_xlabel(f"{FOCUS_METRIC} Quantile Bucket")
    ax.set_ylabel("Arrival Rate")
    ax.set_ylim(0, 1)
    ax.grid(linestyle="--", alpha=0.3)
    ax.legend(title="Threshold", ncols=min(4, len(THRESHOLDS)))
    fig.tight_layout()
    fig.savefig(CHART_OUTPUT_DIR / f"{side}_arrival_rate_by_{FOCUS_METRIC}_quantile.png", dpi=150)
    plt.close(fig)


def plot_correlation_heatmap(correlation_rows: List[Dict[str, object]], *, side: str) -> None:
    CHART_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    matrix = []
    for metric in METRICS:
        row_values = []
        for threshold in THRESHOLDS:
            match = next(
                item
                for item in correlation_rows
                if item["metric"] == metric and item["threshold"] == threshold and item["side"] == side
            )
            row_values.append(float(match["spearman_corr"]))
        matrix.append(row_values)

    fig, ax = plt.subplots(figsize=(10, 5.5))
    image = ax.imshow(matrix, aspect="auto", cmap="RdYlGn", vmin=-1, vmax=1)
    ax.set_title(f"Polymarket Arrival / Volatility Spearman Heatmap ({side.capitalize()})")
    ax.set_xlabel("Arrival Threshold")
    ax.set_ylabel("Volatility Metric")
    ax.set_xticks(range(len(THRESHOLDS)))
    ax.set_xticklabels(THRESHOLDS)
    ax.set_yticks(range(len(METRICS)))
    ax.set_yticklabels(METRICS)

    for row_index, metric in enumerate(METRICS):
        for column_index, threshold in enumerate(THRESHOLDS):
            ax.text(column_index, row_index, f"{matrix[row_index][column_index]:.2f}", ha="center", va="center", fontsize=8)

    colorbar = fig.colorbar(image, ax=ax)
    colorbar.set_label("Spearman Correlation")
    fig.tight_layout()
    fig.savefig(CHART_OUTPUT_DIR / f"{side}_correlation_heatmap.png", dpi=150)
    plt.close(fig)


def main() -> None:
    rows = build_analysis_rows()
    if not rows:
        raise SystemExit("No volatility rows available. Run the btc_5m_volatility research job first.")

    write_analysis_csv(rows)
    correlation_rows = build_correlation_rows(rows)
    write_correlation_csv(correlation_rows)
    plot_quantile_arrival_lines(rows, side="up")
    plot_quantile_arrival_lines(rows, side="down")
    plot_correlation_heatmap(correlation_rows, side="up")
    plot_correlation_heatmap(correlation_rows, side="down")


if __name__ == "__main__":
    main()
