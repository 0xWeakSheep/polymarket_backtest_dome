"""Run: python3 data/chart/script/completeness_arb/plot_btc_15m_last5m_misalignment.py"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parents[4]
DATA_DIR = ROOT_DIR / "data/processed/completeness_arb/btc_15m_last5m_misalignment"
OUTPUT_DIR = ROOT_DIR / "data/chart/result/completeness_arb/btc_15m_last5m_misalignment"
TITLE_PREFIX = "Polymarket Cross-Market Arbitrage"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def utc_day(timestamp: int | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")


def plot_funnel(summary: dict) -> None:
    processed = int(summary["processed_15m_markets"])
    eligible = int(summary["eligible_total"])
    hits = int(summary["hit_total"])

    labels = ["Processed", "Eligible", "Opportunity"]
    values = [processed, eligible, hits]
    colors = ["#8ecae6", "#ffb703", "#219ebc"]

    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.bar(labels, values, color=colors, width=0.6)
    ax.set_title(f"{TITLE_PREFIX}: BTC 15m / Last 5m Opportunity Funnel")
    ax.set_ylabel("Window Count")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.bar_label(
        bars,
        labels=[
            f"{processed}",
            f"{eligible}\n({eligible / processed:.2%} of processed)" if processed else "0",
            f"{hits}\n({hits / processed:.2%} of processed)" if processed else "0",
        ],
        padding=4,
        fontsize=9,
    )
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "overall_funnel.png", dpi=150)
    plt.close(fig)


def plot_hit_rate_by_pattern(summary: dict) -> None:
    patterns = ["up_up", "down_down"]
    eligible_values = [int(summary["eligible_up_up_samples"]), int(summary["eligible_down_down_samples"])]
    hit_values = [int(summary["hit_up_up_samples"]), int(summary["hit_down_down_samples"])]
    miss_values = [int(summary["no_hit_up_up_samples"]), int(summary["no_hit_down_down_samples"])]

    positions = range(len(patterns))

    fig, ax = plt.subplots(figsize=(9, 6))
    hit_bars = ax.bar(positions, hit_values, label="Opportunity", color="#2a9d8f", width=0.55)
    miss_bars = ax.bar(positions, miss_values, bottom=hit_values, label="No Opportunity", color="#e76f51", width=0.55)

    ax.set_title(f"{TITLE_PREFIX}: Opportunity Count by Path Pattern")
    ax.set_ylabel("Eligible Window Count")
    ax.set_xticks(list(positions), patterns)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend()

    hit_labels = [
        f"{hit}\n({hit / eligible:.2%})" if eligible else "0"
        for hit, eligible in zip(hit_values, eligible_values)
    ]
    miss_labels = [f"{miss}" for miss in miss_values]
    ax.bar_label(hit_bars, labels=hit_labels, label_type="center", color="white", fontsize=9)
    ax.bar_label(miss_bars, labels=miss_labels, label_type="center", color="white", fontsize=9)

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "hit_rate_by_pattern.png", dpi=150)
    plt.close(fig)


def plot_edge_distribution(opportunity_rows: list[dict]) -> None:
    grouped_edges: dict[str, list[float]] = defaultdict(list)
    for row in opportunity_rows:
        grouped_edges[str(row["path_pattern"])].append(float(row["edge"]))

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = {"up_up": "#1d3557", "down_down": "#e63946"}
    bins = 20
    for pattern in ["up_up", "down_down"]:
        edges = grouped_edges.get(pattern, [])
        if not edges:
            continue
        ax.hist(edges, bins=bins, alpha=0.6, label=f"{pattern} ({len(edges)})", color=colors[pattern])

    ax.set_title(f"{TITLE_PREFIX}: Opportunity Edge Distribution")
    ax.set_xlabel("Edge = 1.0 - price_sum")
    ax.set_ylabel("Window Count")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "edge_distribution.png", dpi=150)
    plt.close(fig)


def plot_daily_opportunity_windows(sample_rows: list[dict], opportunity_rows: list[dict]) -> None:
    processed_by_day = Counter()
    eligible_by_day = Counter()
    hit_by_day = Counter()

    for row in sample_rows:
        day = utc_day(row.get("fifteen_end_ts"))
        if day is None:
            continue
        processed_by_day[day] += 1
        if bool(row.get("eligible")):
            eligible_by_day[day] += 1

    for row in opportunity_rows:
        day = utc_day(row.get("trigger_ts"))
        if day is None:
            continue
        hit_by_day[day] += 1

    ordered_days = sorted(set(processed_by_day) | set(hit_by_day))
    dates = [datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc) for day in ordered_days]
    hit_counts = [hit_by_day.get(day, 0) for day in ordered_days]
    hit_rates = [
        (hit_by_day.get(day, 0) / eligible_by_day[day]) if eligible_by_day.get(day, 0) else 0.0
        for day in ordered_days
    ]

    fig, ax = plt.subplots(figsize=(14, 6))
    bars = ax.bar(dates, hit_counts, width=0.8, color="#457b9d", label="Opportunity Windows")
    ax.set_title(f"{TITLE_PREFIX}: Daily Opportunity Windows and Eligible Hit Rate")
    ax.set_xlabel("UTC Day")
    ax.set_ylabel("Opportunity Window Count")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    rate_ax = ax.twinx()
    rate_ax.plot(dates, hit_rates, color="#f4a261", marker="o", linewidth=1.8, label="Hit Rate Within Eligible")
    rate_ax.set_ylabel("Eligible Hit Rate")
    rate_ax.set_ylim(0, min(1.0, max(hit_rates) * 1.15 if hit_rates else 1.0))

    if len(hit_counts) <= 20:
        ax.bar_label(bars, labels=[str(value) for value in hit_counts], padding=3, fontsize=8)

    lines, labels = ax.get_legend_handles_labels()
    rate_lines, rate_labels = rate_ax.get_legend_handles_labels()
    ax.legend(lines + rate_lines, labels + rate_labels, loc="upper left")

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "daily_opportunity_windows.png", dpi=150)
    plt.close(fig)


def write_chart_summary(summary: dict, sample_rows: list[dict], opportunity_rows: list[dict]) -> None:
    processed = int(summary["processed_15m_markets"])
    eligible = int(summary["eligible_total"])
    hits = int(summary["hit_total"])
    best_row = max(opportunity_rows, key=lambda row: float(row["edge"]), default=None)

    processed_by_day = Counter()
    hit_by_day = Counter()
    for row in sample_rows:
        day = utc_day(row.get("fifteen_end_ts"))
        if day is not None:
            processed_by_day[day] += 1
    for row in opportunity_rows:
        day = utc_day(row.get("trigger_ts"))
        if day is not None:
            hit_by_day[day] += 1

    peak_day = None
    peak_count = 0
    if hit_by_day:
        peak_day, peak_count = max(hit_by_day.items(), key=lambda item: item[1])

    payload = {
        "processed_total": processed,
        "eligible_total": eligible,
        "opportunity_total": hits,
        "opportunity_rate_of_processed": round(hits / processed, 6) if processed else 0.0,
        "opportunity_rate_of_eligible": round(hits / eligible, 6) if eligible else 0.0,
        "eligible_breakdown": {
            "up_up": int(summary["eligible_up_up_samples"]),
            "down_down": int(summary["eligible_down_down_samples"]),
        },
        "opportunity_breakdown": {
            "up_up": int(summary["hit_up_up_samples"]),
            "down_down": int(summary["hit_down_down_samples"]),
        },
        "peak_opportunity_day_utc": peak_day,
        "peak_opportunity_count": peak_count,
        "best_edge_record": best_row,
    }

    with (OUTPUT_DIR / "chart_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary = load_json(DATA_DIR / "summary.json")
    sample_rows = load_jsonl(DATA_DIR / "sample_records.jsonl")
    opportunity_rows = load_jsonl(DATA_DIR / "opportunity_records.jsonl")

    plot_funnel(summary)
    plot_hit_rate_by_pattern(summary)
    plot_edge_distribution(opportunity_rows)
    plot_daily_opportunity_windows(sample_rows, opportunity_rows)
    write_chart_summary(summary, sample_rows, opportunity_rows)


if __name__ == "__main__":
    main()
