"""Run: python3 data/chart/script/tail_buy_095_sequence/plot_tail_buy_095_sequence.py"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parents[4]
ENTRIES_PATH = ROOT_DIR / "data/processed/tail_buy_095_sequence/all_entries.jsonl"
SUMMARY_PATH = ROOT_DIR / "data/processed/tail_buy_095_sequence/summary.json"
OUTPUT_DIR = ROOT_DIR / "data/chart/result/tail_buy_095_sequence"

INITIAL_CAPITAL = 1000.0
POSITION_SIZE = 10.0
MAX_POSITIONS = 100
ENTRY_PRICE = 0.95
ANALYSIS_CUTOFF = datetime(2026, 3, 16, tzinfo=timezone.utc)


@dataclass
class Trade:
    trigger_timestamp: int
    market_end_time: int
    outcome: str
    payout: float


def load_entries() -> list[dict]:
    rows: list[dict] = []
    with ENTRIES_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return sorted(rows, key=lambda row: (int(row["trigger_timestamp"]), int(row["market_end_time"] or 0)))


def filter_settled_entries(entries: list[dict]) -> tuple[list[dict], int]:
    cutoff_ts = int(ANALYSIS_CUTOFF.timestamp())
    filtered = [
        entry
        for entry in entries
        if entry.get("market_end_time") is not None and int(entry["market_end_time"]) <= cutoff_ts
    ]
    return filtered, len(entries) - len(filtered)


def load_summary() -> dict:
    if not SUMMARY_PATH.exists():
        return {}
    return json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))


def simulate(entries: list[dict]) -> tuple[list[tuple[int, float]], list[tuple[int, float]], dict]:
    events: list[tuple[int, str, dict]] = []
    for entry in entries:
        events.append((int(entry["trigger_timestamp"]), "entry", entry))
        events.append((int(entry["market_end_time"]), "exit", entry))
    events.sort(key=lambda item: (item[0], 0 if item[1] == "exit" else 1))

    cash = INITIAL_CAPITAL
    open_positions: dict[str, Trade] = {}
    equity_points: list[tuple[int, float]] = []
    total_profit = 0.0
    skipped_for_capacity = 0
    executed_trades = 0
    max_open_positions = 0
    settled_outcomes: list[str] = []
    entry_order_outcomes: list[str] = []

    for timestamp, event_type, entry in events:
        condition_id = str(entry["condition_id"])
        if event_type == "exit":
            trade = open_positions.pop(condition_id, None)
            if trade is not None:
                cash += trade.payout
                total_profit += trade.payout - POSITION_SIZE
                settled_outcomes.append(trade.outcome)
        else:
            if len(open_positions) >= MAX_POSITIONS or cash < POSITION_SIZE:
                skipped_for_capacity += 1
            else:
                payout = (POSITION_SIZE / ENTRY_PRICE) if str(entry["outcome"]) == "success" else 0.0
                open_positions[condition_id] = Trade(
                    trigger_timestamp=int(entry["trigger_timestamp"]),
                    market_end_time=int(entry["market_end_time"]),
                    outcome=str(entry["outcome"]),
                    payout=payout,
                )
                cash -= POSITION_SIZE
                executed_trades += 1
                entry_order_outcomes.append(str(entry["outcome"]))
                max_open_positions = max(max_open_positions, len(open_positions))

        equity = cash + sum(POSITION_SIZE for _ in open_positions.values())
        equity_points.append((timestamp, equity))

    drawdown_points: list[tuple[int, float]] = []
    running_peak = equity_points[0][1] if equity_points else INITIAL_CAPITAL
    max_drawdown = 0.0
    for timestamp, equity in equity_points:
        running_peak = max(running_peak, equity)
        drawdown = 0.0 if running_peak == 0 else (running_peak - equity) / running_peak
        max_drawdown = max(max_drawdown, drawdown)
        drawdown_points.append((timestamp, drawdown))

    longest_failure_streak = 0
    current_failure_streak = 0
    for outcome in entry_order_outcomes:
        if outcome == "failure":
            current_failure_streak += 1
            longest_failure_streak = max(longest_failure_streak, current_failure_streak)
        else:
            current_failure_streak = 0

    summary = {
        "initial_capital": INITIAL_CAPITAL,
        "position_size": POSITION_SIZE,
        "max_positions": MAX_POSITIONS,
        "entry_price": ENTRY_PRICE,
        "executed_trades": executed_trades,
        "settled_trades": len(settled_outcomes),
        "skipped_for_capacity": skipped_for_capacity,
        "final_equity": round(equity_points[-1][1], 6) if equity_points else INITIAL_CAPITAL,
        "total_profit": round(total_profit, 6),
        "max_drawdown": round(max_drawdown, 6),
        "max_open_positions": max_open_positions,
        "longest_failure_streak": longest_failure_streak,
    }
    return equity_points, drawdown_points, summary


def to_datetimes(points: list[tuple[int, float]]) -> tuple[list[datetime], list[float]]:
    dates = [datetime.fromtimestamp(timestamp, tz=timezone.utc) for timestamp, _ in points]
    values = [value for _, value in points]
    return dates, values


def plot_capital_curve(points: list[tuple[int, float]], summary: dict) -> None:
    dates, values = to_datetimes(points)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(dates, values, color="#1D3557", linewidth=2)
    ax.set_title(
        "Tail 0.95 Buy-Side Capital Curve\n"
        f"Initial ${INITIAL_CAPITAL:.0f}, Size ${POSITION_SIZE:.0f}, Entry {ENTRY_PRICE:.2f}"
    )
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Account Value (USD)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.tick_params(axis="x", rotation=45)
    ax.grid(linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "capital_curve.png", dpi=150)
    plt.close(fig)


def plot_drawdown_curve(points: list[tuple[int, float]], summary: dict) -> None:
    dates, values = to_datetimes(points)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.fill_between(dates, values, color="#C1121F", alpha=0.35)
    ax.plot(dates, values, color="#C1121F", linewidth=1.5)
    ax.set_title(f"Tail 0.95 Buy-Side Drawdown Curve\nMax Drawdown: {summary['max_drawdown']:.2%}")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Drawdown")
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.yaxis.set_major_formatter(lambda value, _pos: f"{value:.0%}")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "drawdown_curve.png", dpi=150)
    plt.close(fig)


def plot_outcome_bar(entries: list[dict]) -> None:
    success_count = sum(1 for entry in entries if entry["outcome"] == "success")
    failure_count = sum(1 for entry in entries if entry["outcome"] == "failure")
    labels = ["Win", "Loss"]
    counts = [success_count, failure_count]

    fig, ax = plt.subplots(figsize=(8, 6))
    bars = ax.bar(labels, counts, color=["#2A9D8F", "#E63946"])
    ax.set_title(f"Tail 0.95 Buy-Side Outcome Count\nSource: {len(entries)} trades")
    ax.set_xlabel("Outcome")
    ax.set_ylabel("Trade Count")
    ax.bar_label(bars, padding=3)
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "outcome_count.png", dpi=150)
    plt.close(fig)


def plot_monthly_loss_rate(entries: list[dict]) -> None:
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"success": 0, "failure": 0})
    for entry in entries:
        month = datetime.fromtimestamp(int(entry["trigger_timestamp"]), tz=timezone.utc).strftime("%Y-%m")
        counts[month][str(entry["outcome"])] += 1

    months = sorted(counts)
    month_dates = [datetime.strptime(month, "%Y-%m").replace(tzinfo=timezone.utc) for month in months]
    loss_rates = []
    for month in months:
        total = counts[month]["success"] + counts[month]["failure"]
        loss_rates.append((counts[month]["failure"] / total) if total else 0.0)

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(month_dates, loss_rates, color="#E63946", marker="o", linewidth=2)
    ax.set_title("Tail 0.95 Buy-Side Monthly Loss Rate")
    ax.set_xlabel("Trigger Month")
    ax.set_ylabel("Loss Rate")
    ax.set_ylim(0, 1)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.tick_params(axis="x", rotation=45)
    ax.grid(linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "monthly_loss_rate.png", dpi=150)
    plt.close(fig)


def save_chart_summary(strategy_summary: dict, simulation_summary: dict) -> None:
    payload = {
        "strategy_summary": strategy_summary,
        "simulation_summary": simulation_summary,
        "assumption": (
            "Treat the 17,391-market study universe as the full trade set. Buy any side that reaches 0.95, "
            "use a fixed $0.95 entry price, settle at $1.00 on non-reversal markets, and lose the full stake on reversal markets."
        ),
        "analysis_cutoff_utc": ANALYSIS_CUTOFF.isoformat(),
    }
    (OUTPUT_DIR / "chart_summary.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    entries = load_entries()
    if not entries:
        raise RuntimeError("No tail buy 0.95 sequence records found.")
    entries, excluded_future_settlements = filter_settled_entries(entries)
    if not entries:
        raise RuntimeError("No settled tail buy 0.95 records remain after cutoff filtering.")
    strategy_summary = load_summary()
    strategy_summary["excluded_future_settlement_markets"] = excluded_future_settlements
    strategy_summary["analysis_cutoff_utc"] = ANALYSIS_CUTOFF.isoformat()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    equity_points, drawdown_points, simulation_summary = simulate(entries)
    plot_capital_curve(equity_points, simulation_summary)
    plot_drawdown_curve(drawdown_points, simulation_summary)
    plot_outcome_bar(entries)
    plot_monthly_loss_rate(entries)
    save_chart_summary(strategy_summary, simulation_summary)
    print(f"saved charts to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
