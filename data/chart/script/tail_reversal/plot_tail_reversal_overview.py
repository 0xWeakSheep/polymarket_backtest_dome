"""Run: python3 data/chart/script/tail_reversal/plot_tail_reversal_overview.py"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parents[4]
REVERSALS_PATH = ROOT_DIR / "data/processed/tail_reversal_095_reversals.jsonl"
OUTPUT_DIR = ROOT_DIR / "data/chart/result/tail_reversal"


def load_reversals() -> list[dict]:
    records: list[dict] = []
    with REVERSALS_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def format_month_label(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m")


def plot_time_to_close(records: list[dict]) -> None:
    hours_to_close = [
        (record["market_end_time"] - record["trigger_timestamp"]) / 3600
        for record in records
    ]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(hours_to_close, bins=12, color="#2E86AB", edgecolor="white")
    ax.set_title(f"Tail Reversal Trigger Time Before Close\nSource: {len(records)} reversal markets")
    ax.set_xlabel("Hours Before Market Close")
    ax.set_ylabel("Reversal Count")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "time_to_close_hist.png", dpi=150)
    plt.close(fig)


def plot_reversal_count_by_month(records: list[dict]) -> None:
    counts_by_month: dict[str, int] = defaultdict(int)
    for record in records:
        counts_by_month[format_month_label(record["trigger_timestamp"])] += 1

    months = sorted(counts_by_month)
    counts = [counts_by_month[month] for month in months]
    month_dates = [datetime.strptime(month, "%Y-%m").replace(tzinfo=timezone.utc) for month in months]

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.bar(month_dates, counts, width=20, color="#F18F01")
    ax.set_title(f"Tail Reversal Count by Month\nSource: {len(records)} reversal markets")
    ax.set_xlabel("Trigger Month")
    ax.set_ylabel("Reversal Count")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "reversal_count_by_month.png", dpi=150)
    plt.close(fig)


def plot_losing_side_distribution(records: list[dict]) -> None:
    side_counts = Counter(record["losing_side"] for record in records)
    labels = sorted(side_counts)
    counts = [side_counts[label] for label in labels]

    fig, ax = plt.subplots(figsize=(8, 6))
    bars = ax.bar(labels, counts, color=["#C73E1D", "#3A86FF"][: len(labels)])
    ax.set_title(f"Losing Side Distribution\nSource: {len(records)} reversal markets")
    ax.set_xlabel("Losing Side")
    ax.set_ylabel("Reversal Count")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.bar_label(bars, padding=3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "losing_side_distribution.png", dpi=150)
    plt.close(fig)


def plot_duration_vs_price(records: list[dict]) -> None:
    durations_days = [
        (record["market_end_time"] - record["market_start_time"]) / 86400
        for record in records
    ]
    max_prices = [record["losing_max_price"] for record in records]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(durations_days, max_prices, alpha=0.75, color="#6A994E", edgecolors="white", linewidths=0.5)
    ax.set_title(f"Market Duration vs. Tail Reversal Severity\nSource: {len(records)} reversal markets")
    ax.set_xlabel("Market Duration (Days)")
    ax.set_ylabel("Losing Side Max Price")
    ax.set_ylim(0.94, 1.005)
    ax.grid(linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "duration_vs_price.png", dpi=150)
    plt.close(fig)


def main() -> None:
    records = load_reversals()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    plot_time_to_close(records)
    plot_reversal_count_by_month(records)
    plot_losing_side_distribution(records)
    plot_duration_vs_price(records)

    print(f"saved charts to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
