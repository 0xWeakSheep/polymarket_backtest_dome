"""Run: python3 data/chart/script/btc_5m_arrival_complex/plot_daily_miss_rates.py"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parents[4]
SELECTED_MARKETS_PATH = ROOT_DIR / "data/raw/btc_5m_arrival/markets/selected_markets_complex.jsonl"
MISSES_ROOT = ROOT_DIR / "data/processed/btc_5m_arrival"
OUTPUT_DIR = ROOT_DIR / "data/chart/result/btc_5m_arrival_complex"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot daily miss rates for BTC 5m arrival thresholds.")
    parser.add_argument(
        "--thresholds",
        nargs="+",
        default=["0.52", "0.53", "0.54", "0.55", "0.56", "0.57", "0.58"],
        help="Threshold directories to include.",
    )
    parser.add_argument(
        "--focus-threshold",
        default="0.53",
        help="Threshold to highlight in the top-day chart.",
    )
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def timestamp_to_day(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")


def load_daily_market_totals() -> tuple[list[str], dict[str, int], int]:
    selected_rows = load_jsonl(SELECTED_MARKETS_PATH)
    unique_rows = {str(row["condition_id"]): row for row in selected_rows}
    market_count_by_day = Counter(
        timestamp_to_day(int(row["end_time"]))
        for row in unique_rows.values()
    )
    all_days = sorted(market_count_by_day)
    return all_days, dict(market_count_by_day), len(unique_rows)


def load_daily_miss_rates(
    thresholds: list[str],
    side: str,
    market_count_by_day: dict[str, int],
) -> dict[str, list[float]]:
    rates_by_threshold: dict[str, list[float]] = {}
    ordered_days = sorted(market_count_by_day)

    for threshold in thresholds:
        miss_path = MISSES_ROOT / threshold / f"{side}_misses.jsonl"
        daily_miss_counts = Counter()
        if miss_path.exists():
            for row in load_jsonl(miss_path):
                daily_miss_counts[timestamp_to_day(int(row["end_time"]))] += 1

        rates_by_threshold[threshold] = [
            daily_miss_counts.get(day, 0) / market_count_by_day[day]
            for day in ordered_days
        ]

    return rates_by_threshold


def plot_daily_lines(
    *,
    days: list[str],
    rates_by_threshold: dict[str, list[float]],
    side: str,
    total_markets: int,
) -> None:
    fig, ax = plt.subplots(figsize=(14, 6))
    dates = [datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc) for day in days]

    for threshold, rates in rates_by_threshold.items():
        ax.plot(dates, rates, marker="o", linewidth=1.8, markersize=3.5, label=threshold)

    ax.set_title(
        f"Daily {side.capitalize()} Miss Rate by Threshold\n"
        f"Source: {total_markets} BTC 5m markets, grouped by UTC day"
    )
    ax.set_xlabel("Day")
    ax.set_ylabel("Miss Rate")
    ax.set_ylim(0, max(max(rates) for rates in rates_by_threshold.values()) * 1.15 if rates_by_threshold else 1)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.tick_params(axis="x", rotation=45)
    ax.grid(linestyle="--", alpha=0.3)
    ax.legend(title="Threshold", ncols=min(4, len(rates_by_threshold)))
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / f"daily_{side}_miss_rate_lines.png", dpi=150)
    plt.close(fig)


def plot_daily_heatmap(
    *,
    days: list[str],
    rates_by_threshold: dict[str, list[float]],
    side: str,
    total_markets: int,
) -> None:
    thresholds = list(rates_by_threshold.keys())
    matrix = [rates_by_threshold[threshold] for threshold in thresholds]

    fig, ax = plt.subplots(figsize=(14, 6))
    image = ax.imshow(matrix, aspect="auto", cmap="YlOrRd", interpolation="nearest")
    ax.set_title(
        f"Daily {side.capitalize()} Miss Rate Heatmap\n"
        f"Source: {total_markets} BTC 5m markets, grouped by UTC day"
    )
    ax.set_xlabel("Day")
    ax.set_ylabel("Threshold")
    ax.set_yticks(range(len(thresholds)))
    ax.set_yticklabels(thresholds)

    tick_positions = list(range(0, len(days), max(1, len(days) // 8)))
    if tick_positions[-1] != len(days) - 1:
        tick_positions.append(len(days) - 1)
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([days[index] for index in tick_positions], rotation=45, ha="right")

    colorbar = fig.colorbar(image, ax=ax)
    colorbar.set_label("Miss Rate")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / f"daily_{side}_miss_rate_heatmap.png", dpi=150)
    plt.close(fig)


def plot_focus_threshold_top_days(
    *,
    days: list[str],
    focus_threshold: str,
    up_rates_by_threshold: dict[str, list[float]],
    down_rates_by_threshold: dict[str, list[float]],
    total_markets: int,
) -> None:
    if focus_threshold not in up_rates_by_threshold or focus_threshold not in down_rates_by_threshold:
        raise ValueError(f"focus threshold {focus_threshold} not found in loaded thresholds")

    rows = []
    for index, day in enumerate(days):
        rows.append(
            {
                "day": day,
                "up_rate": up_rates_by_threshold[focus_threshold][index],
                "down_rate": down_rates_by_threshold[focus_threshold][index],
                "combined_rate": (
                    up_rates_by_threshold[focus_threshold][index] + down_rates_by_threshold[focus_threshold][index]
                )
                / 2,
            }
        )

    top_rows = sorted(rows, key=lambda item: item["combined_rate"], reverse=True)[:10]
    labels = [row["day"] for row in top_rows]
    up_rates = [row["up_rate"] for row in top_rows]
    down_rates = [row["down_rate"] for row in top_rows]
    positions = list(range(len(top_rows)))
    bar_width = 0.38

    fig, ax = plt.subplots(figsize=(12, 6))
    up_bars = ax.bar([position - bar_width / 2 for position in positions], up_rates, width=bar_width, label="Up")
    down_bars = ax.bar([position + bar_width / 2 for position in positions], down_rates, width=bar_width, label="Down")

    ax.set_title(
        f"Top Daily Miss-Rate Days at Threshold {focus_threshold}\n"
        f"Source: {total_markets} BTC 5m markets, ranked by average of Up/Down miss rate"
    )
    ax.set_xlabel("UTC Day")
    ax.set_ylabel("Miss Rate")
    ax.set_xticks(positions, labels, rotation=45, ha="right")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend()
    ax.bar_label(up_bars, labels=[f"{value:.2%}" for value in up_rates], padding=3, fontsize=8)
    ax.bar_label(down_bars, labels=[f"{value:.2%}" for value in down_rates], padding=3, fontsize=8)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / f"top_daily_miss_rate_{focus_threshold.replace('.', '_')}.png", dpi=150)
    plt.close(fig)


def write_peak_summary(
    *,
    days: list[str],
    thresholds: list[str],
    up_rates_by_threshold: dict[str, list[float]],
    down_rates_by_threshold: dict[str, list[float]],
) -> None:
    summary: dict[str, dict[str, object]] = {}
    for threshold in thresholds:
        up_rates = up_rates_by_threshold[threshold]
        down_rates = down_rates_by_threshold[threshold]
        up_index = max(range(len(days)), key=lambda index: up_rates[index])
        down_index = max(range(len(days)), key=lambda index: down_rates[index])
        summary[threshold] = {
            "up_peak_day": days[up_index],
            "up_peak_rate": round(up_rates[up_index], 6),
            "down_peak_day": days[down_index],
            "down_peak_rate": round(down_rates[down_index], 6),
        }

    path = OUTPUT_DIR / "daily_peak_summary.json"
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    days, market_count_by_day, total_markets = load_daily_market_totals()
    thresholds = sorted(args.thresholds, key=float)
    up_rates_by_threshold = load_daily_miss_rates(thresholds, "up", market_count_by_day)
    down_rates_by_threshold = load_daily_miss_rates(thresholds, "down", market_count_by_day)

    plot_daily_lines(days=days, rates_by_threshold=up_rates_by_threshold, side="up", total_markets=total_markets)
    plot_daily_lines(days=days, rates_by_threshold=down_rates_by_threshold, side="down", total_markets=total_markets)
    plot_daily_heatmap(days=days, rates_by_threshold=up_rates_by_threshold, side="up", total_markets=total_markets)
    plot_daily_heatmap(days=days, rates_by_threshold=down_rates_by_threshold, side="down", total_markets=total_markets)
    plot_focus_threshold_top_days(
        days=days,
        focus_threshold=args.focus_threshold,
        up_rates_by_threshold=up_rates_by_threshold,
        down_rates_by_threshold=down_rates_by_threshold,
        total_markets=total_markets,
    )
    write_peak_summary(
        days=days,
        thresholds=thresholds,
        up_rates_by_threshold=up_rates_by_threshold,
        down_rates_by_threshold=down_rates_by_threshold,
    )

    print(f"saved charts to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
