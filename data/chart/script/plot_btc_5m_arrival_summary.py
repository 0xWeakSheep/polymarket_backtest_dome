from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parents[3]
SUMMARY_PATH = ROOT_DIR / "data/processed/btc_5m_arrival/summary.json"
OUTPUT_PATH = ROOT_DIR / "data/chart/result/btc_5m_arrival_summary_rates.png"


def main() -> None:
    with SUMMARY_PATH.open("r", encoding="utf-8") as handle:
        summary = json.load(handle)

    market_count = summary["processed_markets"]
    thresholds = list(summary["up_arrival_rate_by_threshold"].keys())
    up_rates = [summary["up_arrival_rate_by_threshold"][threshold] for threshold in thresholds]
    down_rates = [summary["down_arrival_rate_by_threshold"][threshold] for threshold in thresholds]

    x_positions = list(range(len(thresholds)))
    bar_width = 0.38

    fig, ax = plt.subplots(figsize=(11, 6))
    up_bars = ax.bar(
        [position - bar_width / 2 for position in x_positions],
        up_rates,
        width=bar_width,
        label="Up",
        color="#2E86AB",
    )
    down_bars = ax.bar(
        [position + bar_width / 2 for position in x_positions],
        down_rates,
        width=bar_width,
        label="Down",
        color="#F18F01",
    )

    ax.set_xticks(x_positions, thresholds)
    ax.set_xlabel("Threshold")
    ax.set_ylabel("Arrival Rate")
    ax.set_ylim(0, 1.02)
    ax.set_title(f"BTC 5M Arrival Rates by Threshold\nSource: {market_count} markets")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    ax.bar_label(up_bars, labels=[f"{rate:.2%}" for rate in up_rates], padding=3, fontsize=9)
    ax.bar_label(down_bars, labels=[f"{rate:.2%}" for rate in down_rates], padding=3, fontsize=9)

    fig.tight_layout()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT_PATH, dpi=150)
    plt.close(fig)

    print(f"saved chart to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
