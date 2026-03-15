"""Run: python3 data/chart/script/btc_5m_volatility_precision_scan/plot_volatility_precision_scan.py"""

from __future__ import annotations

import csv
import statistics
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parents[4]
ANALYSIS_CSV = ROOT_DIR / "data/processed/btc_5m_volatility/volatility_arrival_analysis.csv"
PROCESSED_OUTPUT_DIR = ROOT_DIR / "data/processed/btc_5m_volatility_precision_scan"
CHART_OUTPUT_DIR = ROOT_DIR / "data/chart/result/btc_5m_volatility_precision_scan"
THRESHOLDS = ["0.52", "0.53", "0.54", "0.55", "0.56", "0.57", "0.58"]
CORE_METRICS = ["range_pct_5m", "return_std_1m_5m"]
QUANTILE_BUCKETS = 30
SWEEP_PERCENTILES = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
METRIC_LABELS = {
    "range_pct_5m": "Volatility Metric: 5m High-Low Range (%)",
    "return_std_1m_5m": "Volatility Metric: 1m Return Std Dev over Prior 5m",
}


def load_rows() -> List[Dict[str, float | str]]:
    with ANALYSIS_CSV.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows: List[Dict[str, float | str]] = []
        for row in reader:
            parsed: Dict[str, float | str] = {
                "condition_id": row["condition_id"],
                "market_slug": row["market_slug"],
                "market_window_start_ts": row["market_window_start_ts"],
            }
            for metric in ["range_pct_5m", "return_std_1m_5m", "abs_return_sum_1m_5m", "realized_vol_1m_5m", "net_move_pct_5m"]:
                parsed[metric] = float(row[metric] or 0.0)
            for threshold in THRESHOLDS:
                parsed[f"up_arrived_{threshold}"] = float(row[f"up_arrived_{threshold}"] or 0.0)
                parsed[f"down_arrived_{threshold}"] = float(row[f"down_arrived_{threshold}"] or 0.0)
            rows.append(parsed)
    return rows


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = p * (len(sorted_values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def quantile_groups(rows: List[Dict[str, float | str]], metric: str, bucket_count: int) -> List[List[Dict[str, float | str]]]:
    ordered = sorted(rows, key=lambda row: float(row[metric]))
    groups: List[List[Dict[str, float | str]]] = []
    for bucket in range(bucket_count):
        start = bucket * len(ordered) // bucket_count
        end = (bucket + 1) * len(ordered) // bucket_count
        if start == end:
            continue
        groups.append(ordered[start:end])
    return groups


def write_distribution_summary(rows: List[Dict[str, float | str]]) -> None:
    PROCESSED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_OUTPUT_DIR / "metric_distribution_summary.csv"
    fieldnames = ["metric", "count", "min", "p10", "p25", "median", "p75", "p90", "max", "mean", "stdev"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for metric in CORE_METRICS:
            values = sorted(float(row[metric]) for row in rows)
            writer.writerow(
                {
                    "metric": metric,
                    "count": len(values),
                    "min": f"{values[0]:.10f}",
                    "p10": f"{percentile(values, 0.10):.10f}",
                    "p25": f"{percentile(values, 0.25):.10f}",
                    "median": f"{percentile(values, 0.50):.10f}",
                    "p75": f"{percentile(values, 0.75):.10f}",
                    "p90": f"{percentile(values, 0.90):.10f}",
                    "max": f"{values[-1]:.10f}",
                    "mean": f"{statistics.fmean(values):.10f}",
                    "stdev": f"{statistics.pstdev(values):.10f}",
                }
            )


def write_arrived_vs_missed_summary(rows: List[Dict[str, float | str]], *, side: str) -> None:
    output_path = PROCESSED_OUTPUT_DIR / f"{side}_arrived_vs_missed_summary.csv"
    fieldnames = [
        "threshold",
        "metric",
        "arrived_count",
        "missed_count",
        "arrived_mean",
        "missed_mean",
        "arrived_median",
        "missed_median",
        "mean_gap",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for threshold in THRESHOLDS:
            target_key = f"{side}_arrived_{threshold}"
            for metric in CORE_METRICS:
                arrived = [float(row[metric]) for row in rows if float(row[target_key]) == 1.0]
                missed = [float(row[metric]) for row in rows if float(row[target_key]) == 0.0]
                writer.writerow(
                    {
                        "threshold": threshold,
                        "metric": metric,
                        "arrived_count": len(arrived),
                        "missed_count": len(missed),
                        "arrived_mean": f"{statistics.fmean(arrived):.10f}" if arrived else "0.0000000000",
                        "missed_mean": f"{statistics.fmean(missed):.10f}" if missed else "0.0000000000",
                        "arrived_median": f"{statistics.median(arrived):.10f}" if arrived else "0.0000000000",
                        "missed_median": f"{statistics.median(missed):.10f}" if missed else "0.0000000000",
                        "mean_gap": f"{(statistics.fmean(arrived) - statistics.fmean(missed)):.10f}" if arrived and missed else "0.0000000000",
                    }
                )


def write_quantile_rates(rows: List[Dict[str, float | str]], *, side: str) -> None:
    output_path = PROCESSED_OUTPUT_DIR / f"{side}_fine_quantile_rates.csv"
    fieldnames = ["metric", "bucket", "bucket_label", "bucket_min", "bucket_max", "threshold", "sample_count", "arrival_rate"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for metric in CORE_METRICS:
            groups = quantile_groups(rows, metric, QUANTILE_BUCKETS)
            for bucket_index, group in enumerate(groups, start=1):
                bucket_values = [float(row[metric]) for row in group]
                for threshold in THRESHOLDS:
                    target_key = f"{side}_arrived_{threshold}"
                    arrival_rate = sum(float(row[target_key]) for row in group) / len(group)
                    writer.writerow(
                        {
                            "metric": metric,
                            "bucket": bucket_index,
                            "bucket_label": f"Q{bucket_index}",
                            "bucket_min": f"{min(bucket_values):.10f}",
                            "bucket_max": f"{max(bucket_values):.10f}",
                            "threshold": threshold,
                            "sample_count": len(group),
                            "arrival_rate": f"{arrival_rate:.6f}",
                        }
                    )


def write_threshold_sweep(rows: List[Dict[str, float | str]], *, side: str) -> None:
    output_path = PROCESSED_OUTPUT_DIR / f"{side}_threshold_sweep.csv"
    fieldnames = ["metric", "cutoff_percentile", "cutoff_value", "threshold", "sample_count", "arrival_rate_above_cutoff"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for metric in CORE_METRICS:
            sorted_values = sorted(float(row[metric]) for row in rows)
            for cutoff_p in SWEEP_PERCENTILES:
                cutoff_value = percentile(sorted_values, cutoff_p)
                filtered = [row for row in rows if float(row[metric]) >= cutoff_value]
                for threshold in THRESHOLDS:
                    target_key = f"{side}_arrived_{threshold}"
                    arrival_rate = sum(float(row[target_key]) for row in filtered) / len(filtered) if filtered else 0.0
                    writer.writerow(
                        {
                            "metric": metric,
                            "cutoff_percentile": f"{cutoff_p:.2f}",
                            "cutoff_value": f"{cutoff_value:.10f}",
                            "threshold": threshold,
                            "sample_count": len(filtered),
                            "arrival_rate_above_cutoff": f"{arrival_rate:.6f}",
                        }
                    )


def plot_distributions(rows: List[Dict[str, float | str]]) -> None:
    CHART_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    for axis, metric in zip(axes, CORE_METRICS):
        values = [float(row[metric]) for row in rows]
        axis.hist(values, bins=60, color="#457b9d", alpha=0.8)
        axis.set_title(f"{metric} Distribution")
        axis.set_xlabel(metric)
        axis.set_ylabel("Market Count")
        axis.ticklabel_format(axis="x", style="plain", useOffset=False)
        axis.grid(axis="y", linestyle="--", alpha=0.3)
    fig.suptitle("Pre-5m Volatility Metric Distribution")
    fig.tight_layout()
    fig.savefig(CHART_OUTPUT_DIR / "metric_distributions.png", dpi=150)
    plt.close(fig)


def plot_quantile_lines(rows: List[Dict[str, float | str]], *, side: str, metric: str) -> None:
    CHART_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    groups = quantile_groups(rows, metric, QUANTILE_BUCKETS)
    labels = [f"Q{index + 1}" for index in range(len(groups))]
    fig, ax = plt.subplots(figsize=(14, 6))
    for threshold in THRESHOLDS:
        target_key = f"{side}_arrived_{threshold}"
        rates = [sum(float(row[target_key]) for row in group) / len(group) for group in groups]
        ax.plot(labels, rates, marker="o", linewidth=1.5, markersize=3, label=threshold)
    ax.set_title(f"{side.capitalize()} Arrival Rate by Fine {metric} Quantiles")
    ax.set_xlabel(f"{metric} Quantile Bucket")
    ax.set_ylabel("Arrival Rate")
    ax.set_ylim(0, 1)
    ax.tick_params(axis="x", rotation=45)
    ax.grid(linestyle="--", alpha=0.3)
    ax.legend(title="Threshold", ncols=min(4, len(THRESHOLDS)))
    fig.tight_layout()
    fig.savefig(CHART_OUTPUT_DIR / f"{side}_{metric}_fine_quantile_lines.png", dpi=150)
    plt.close(fig)


def plot_threshold_quantile_line(
    rows: List[Dict[str, float | str]],
    *,
    side: str,
    metric: str,
    threshold: str,
) -> None:
    threshold_dir = CHART_OUTPUT_DIR / threshold
    threshold_dir.mkdir(parents=True, exist_ok=True)
    groups = quantile_groups(rows, metric, QUANTILE_BUCKETS)
    labels = [f"Q{index + 1}" for index in range(len(groups))]
    target_key = f"{side}_arrived_{threshold}"
    rates = [sum(float(row[target_key]) for row in group) / len(group) for group in groups]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(labels, rates, marker="o", linewidth=1.8, markersize=3.5, color="#2a9d8f")
    max_index = max(range(len(rates)), key=lambda index: rates[index])
    ax.scatter([labels[max_index]], [rates[max_index]], color="#e63946", zorder=3)
    ax.annotate(
        f"{rates[max_index]:.4f}",
        (labels[max_index], rates[max_index]),
        textcoords="offset points",
        xytext=(0, 10),
        ha="center",
        fontsize=9,
        color="#e63946",
    )
    ax.set_title(f"{side.capitalize()} {threshold} Arrival Rate by {metric} Quantiles")
    ax.set_xlabel(f"{METRIC_LABELS[metric]} Quantile Bucket")
    ax.set_ylabel("Arrival Rate")
    ax.set_ylim(0, 1)
    ax.tick_params(axis="x", rotation=45)
    ax.grid(linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(threshold_dir / f"{side}_{metric}_quantile.png", dpi=150)
    plt.close(fig)


def plot_threshold_sweep(rows: List[Dict[str, float | str]], *, side: str, metric: str) -> None:
    CHART_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sorted_values = sorted(float(row[metric]) for row in rows)
    fig, ax = plt.subplots(figsize=(12, 6))
    x_labels: List[str] = []
    for cutoff_p in SWEEP_PERCENTILES:
        cutoff_value = percentile(sorted_values, cutoff_p)
        x_labels.append(f"P{int(cutoff_p * 100)}\n{cutoff_value:.6f}")
    for threshold in THRESHOLDS:
        target_key = f"{side}_arrived_{threshold}"
        rates = []
        for cutoff_p in SWEEP_PERCENTILES:
            cutoff_value = percentile(sorted_values, cutoff_p)
            filtered = [row for row in rows if float(row[metric]) >= cutoff_value]
            arrival_rate = sum(float(row[target_key]) for row in filtered) / len(filtered) if filtered else 0.0
            rates.append(arrival_rate)
        ax.plot(x_labels, rates, marker="o", linewidth=1.6, label=threshold)
    ax.set_title(f"{side.capitalize()} Arrival Rate Above {metric} Cutoffs")
    ax.set_xlabel("Minimum Metric Cutoff")
    ax.set_ylabel("Arrival Rate")
    ax.set_ylim(0, 1)
    ax.grid(linestyle="--", alpha=0.3)
    ax.legend(title="Threshold", ncols=min(4, len(THRESHOLDS)))
    fig.tight_layout()
    fig.savefig(CHART_OUTPUT_DIR / f"{side}_{metric}_threshold_sweep.png", dpi=150)
    plt.close(fig)


def plot_threshold_specific_sweep(
    rows: List[Dict[str, float | str]],
    *,
    side: str,
    metric: str,
    threshold: str,
) -> None:
    threshold_dir = CHART_OUTPUT_DIR / threshold
    threshold_dir.mkdir(parents=True, exist_ok=True)
    sorted_values = sorted(float(row[metric]) for row in rows)
    x_labels: List[str] = []
    rates = []
    target_key = f"{side}_arrived_{threshold}"
    for cutoff_p in SWEEP_PERCENTILES:
        cutoff_value = percentile(sorted_values, cutoff_p)
        filtered = [row for row in rows if float(row[metric]) >= cutoff_value]
        arrival_rate = sum(float(row[target_key]) for row in filtered) / len(filtered) if filtered else 0.0
        rates.append(arrival_rate)
        x_labels.append(f"P{int(cutoff_p * 100)}\n{cutoff_value:.6f}")

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x_labels, rates, marker="o", linewidth=1.8, color="#457b9d")
    max_index = max(range(len(rates)), key=lambda index: rates[index])
    ax.scatter([x_labels[max_index]], [rates[max_index]], color="#e63946", zorder=3)
    ax.annotate(
        f"{rates[max_index]:.4f}",
        (x_labels[max_index], rates[max_index]),
        textcoords="offset points",
        xytext=(0, 10),
        ha="center",
        fontsize=9,
        color="#e63946",
    )
    ax.set_title(f"{side.capitalize()} {threshold} Arrival Rate Above {metric} Cutoffs")
    ax.set_xlabel(f"Volatility Cutoff ({METRIC_LABELS[metric]})")
    ax.set_ylabel("Arrival Rate")
    ax.set_ylim(0, 1)
    ax.grid(linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(threshold_dir / f"{side}_{metric}_threshold_sweep.png", dpi=150)
    plt.close(fig)


def main() -> None:
    rows = load_rows()
    if not rows:
        raise SystemExit("No rows found in volatility_arrival_analysis.csv")

    write_distribution_summary(rows)
    plot_distributions(rows)

    for side in ["up", "down"]:
        write_arrived_vs_missed_summary(rows, side=side)
        write_quantile_rates(rows, side=side)
        write_threshold_sweep(rows, side=side)
        for metric in CORE_METRICS:
            plot_quantile_lines(rows, side=side, metric=metric)
            plot_threshold_sweep(rows, side=side, metric=metric)
            for threshold in THRESHOLDS:
                plot_threshold_quantile_line(rows, side=side, metric=metric, threshold=threshold)
                plot_threshold_specific_sweep(rows, side=side, metric=metric, threshold=threshold)


if __name__ == "__main__":
    main()
