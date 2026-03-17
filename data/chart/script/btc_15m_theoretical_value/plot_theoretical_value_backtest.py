"""Run: python3 data/chart/script/btc_15m_theoretical_value/plot_theoretical_value_backtest.py"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parents[4]


def parse_args() -> argparse.Namespace:
    backtest_dir = ROOT_DIR / "data/processed/btc_15m_theoretical_value/backtest"
    parser = argparse.ArgumentParser(description="Plot second-level BTC 15m theoretical value backtest outputs.")
    parser.add_argument("--rows-csv", default=str(backtest_dir / "second_rows.csv"))
    parser.add_argument("--metrics-csv", default=str(backtest_dir / "model_metrics.csv"))
    parser.add_argument("--expiry-accuracy-csv", default=str(backtest_dir / "expiry_accuracy.csv"))
    parser.add_argument("--absolute-time-accuracy-csv", default=str(backtest_dir / "absolute_time_accuracy.csv"))
    parser.add_argument("--summary-json", default=str(backtest_dir / "summary.json"))
    parser.add_argument("--output-dir", default=str(ROOT_DIR / "data/chart/result/btc_15m_theoretical_value"))
    return parser.parse_args()


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_summary(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def parse_float(row: dict[str, str], key: str) -> float:
    try:
        return float(row.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def parse_int(row: dict[str, str], key: str) -> int:
    try:
        return int(float(row.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def ensure_output_dirs(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    external_dir = path / "external"
    external_dir.mkdir(parents=True, exist_ok=True)
    return external_dir


def plot_theory_vs_actual(rows: list[dict[str, str]], summary: dict[str, object], output_dir: Path) -> None:
    condition_id = str((summary.get("example_markets") or {}).get("swing_market_condition_id") or "")
    market_rows = [row for row in rows if str(row.get("condition_id") or "") == condition_id]
    if not market_rows:
        return
    market_rows.sort(key=lambda row: parse_int(row, "timestamp"))
    market_rows = market_rows[:300]
    times = [datetime.fromtimestamp(parse_int(row, "timestamp"), tz=timezone.utc) for row in market_rows]
    actual = [parse_float(row, "target_rv_300s") for row in market_rows]
    theory = [parse_float(row, "pred_theory_x_300s") for row in market_rows]
    baseline = [parse_float(row, "pred_x_roll_300s") for row in market_rows]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(times, actual, linewidth=1.4, label="Realized RV_x 300s")
    ax.plot(times, theory, linewidth=1.4, label="Theory 300s")
    ax.plot(times, baseline, linewidth=1.2, label="x-roll 300s")
    ax.set_title("Second-Level Theory vs Realized RV_x")
    ax.set_xlabel("UTC Time")
    ax.set_ylabel("Variance")
    ax.grid(linestyle="--", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "theory_vs_actual_rv_300s.png", dpi=150)
    plt.close(fig)


def plot_model_comparison(metrics: list[dict[str, str]], output_dir: Path) -> None:
    filtered = [
        row
        for row in metrics
        if row.get("horizon") == "300s" and row.get("bucket") == "all" and row.get("model") in {"theory_x", "x_roll", "p_roll"}
    ]
    if not filtered:
        return
    models = [str(row["model"]) for row in filtered]
    values = [parse_float(row, "qlike") for row in filtered]
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(models, values, color=["#d1495b", "#2d6a4f", "#577590"])
    ax.set_title("Second-Level QLIKE Comparison (300s)")
    ax.set_ylabel("QLIKE")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.bar_label(bars, labels=[f"{value:.4f}" for value in values], padding=3, fontsize=9)
    fig.tight_layout()
    fig.savefig(output_dir / "qlike_model_comparison_300s.png", dpi=150)
    plt.close(fig)


def plot_external_actual_vs_implied_expiry(expiry_rows: list[dict[str, str]], external_dir: Path) -> None:
    if not expiry_rows:
        return
    xs = [parse_int(row, "seconds_to_expiry") for row in expiry_rows]
    actual = [parse_float(row, "actual_price_next_second") for row in expiry_rows]
    implied = [parse_float(row, "model_implied_price_next_second") for row in expiry_rows]
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(xs, actual, linewidth=1.4, label="Actual next-second price")
    ax.plot(xs, implied, linewidth=1.4, label="Model-implied next-second price")
    ax.set_title("Actual vs Model-Implied Price by Seconds to Expiry")
    ax.set_xlabel("Seconds to Expiry")
    ax.set_ylabel("Price")
    ax.grid(linestyle="--", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(external_dir / "actual_vs_model_implied_price_by_expiry.png", dpi=150)
    plt.close(fig)


def plot_external_expiry_accuracy(expiry_rows: list[dict[str, str]], external_dir: Path) -> None:
    if not expiry_rows:
        return
    xs = [parse_int(row, "seconds_to_expiry") for row in expiry_rows]
    weighted_accuracy = [parse_float(row, "value_weighted_accuracy") for row in expiry_rows]
    directional_accuracy = [parse_float(row, "directional_accuracy") for row in expiry_rows]
    mean_abs_error = [parse_float(row, "mean_abs_price_error") for row in expiry_rows]
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(xs, weighted_accuracy, linewidth=1.5, label="Value-weighted accuracy", color="#1d3557")
    ax1.plot(xs, directional_accuracy, linewidth=1.4, label="Directional accuracy", color="#2a9d8f")
    ax1.set_xlabel("Seconds to Expiry")
    ax1.set_ylabel("Accuracy")
    ax1.grid(linestyle="--", alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(xs, mean_abs_error, linewidth=1.2, label="Mean abs price error", color="#e76f51")
    ax2.set_ylabel("Mean abs price error")
    lines = ax1.get_lines() + ax2.get_lines()
    labels = [line.get_label() for line in lines]
    ax1.legend(lines, labels, loc="best")
    ax1.set_title("Value-Weighted Accuracy by Seconds to Expiry")
    fig.tight_layout()
    fig.savefig(external_dir / "value_weighted_accuracy_by_expiry.png", dpi=150)
    plt.close(fig)


def plot_external_actual_vs_implied_absolute_time(absolute_rows: list[dict[str, str]], external_dir: Path) -> None:
    if not absolute_rows:
        return
    times = [datetime.fromtimestamp(parse_int(row, "bin_timestamp"), tz=timezone.utc) for row in absolute_rows]
    actual = [parse_float(row, "actual_price_next_second") for row in absolute_rows]
    implied = [parse_float(row, "model_implied_price_next_second") for row in absolute_rows]
    fig, ax = plt.subplots(figsize=(13, 5))
    ax.plot(times, actual, linewidth=1.4, label="Actual next-second price")
    ax.plot(times, implied, linewidth=1.4, label="Model-implied next-second price")
    ax.set_title("Actual vs Model-Implied Price Across Full Backtest Time")
    ax.set_xlabel("UTC Time")
    ax.set_ylabel("Price")
    ax.grid(linestyle="--", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(external_dir / "actual_vs_model_implied_price_absolute_time.png", dpi=150)
    plt.close(fig)


def plot_external_absolute_accuracy(absolute_rows: list[dict[str, str]], external_dir: Path) -> None:
    if not absolute_rows:
        return
    times = [datetime.fromtimestamp(parse_int(row, "bin_timestamp"), tz=timezone.utc) for row in absolute_rows]
    weighted_accuracy = [parse_float(row, "value_weighted_accuracy") for row in absolute_rows]
    directional_accuracy = [parse_float(row, "directional_accuracy") for row in absolute_rows]
    mean_abs_error = [parse_float(row, "mean_abs_price_error") for row in absolute_rows]
    fig, ax1 = plt.subplots(figsize=(13, 5))
    ax1.plot(times, weighted_accuracy, linewidth=1.5, label="Value-weighted accuracy", color="#264653")
    ax1.plot(times, directional_accuracy, linewidth=1.4, label="Directional accuracy", color="#2a9d8f")
    ax1.set_xlabel("UTC Time")
    ax1.set_ylabel("Accuracy")
    ax1.grid(linestyle="--", alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(times, mean_abs_error, linewidth=1.2, label="Mean abs price error", color="#e76f51")
    ax2.set_ylabel("Mean abs price error")
    lines = ax1.get_lines() + ax2.get_lines()
    labels = [line.get_label() for line in lines]
    ax1.legend(lines, labels, loc="best")
    ax1.set_title("Value-Weighted Accuracy Across Full Backtest Time")
    fig.tight_layout()
    fig.savefig(external_dir / "value_weighted_accuracy_absolute_time.png", dpi=150)
    plt.close(fig)


def write_external_readme(external_dir: Path) -> None:
    content = """# BTC 15m Second-Level External Charts

## `actual_vs_model_implied_price_by_expiry.png`

- 横轴是距离到期还剩多少秒。
- 纵轴是价格/概率。
- 蓝线是秒级回测中下一秒真实价格的均值。
- 橙线是模型给出的下一秒隐含价格均值。

## `value_weighted_accuracy_by_expiry.png`

- 横轴是距离到期还剩多少秒。
- 深蓝线是价值加权准确率。
- 绿线是纯方向准确率。
- 红线是预测价和下一秒真实价的平均绝对误差。

## `actual_vs_model_implied_price_absolute_time.png`

- 横轴是整个回测期间的绝对时间。
- 纵轴是价格/概率。
- 两条线分别是该时间分箱内的真实下一秒价格均值和模型隐含下一秒价格均值。

## `value_weighted_accuracy_absolute_time.png`

- 横轴是整个回测期间的绝对时间。
- 深蓝线是该时间分箱内的价值加权准确率。
- 绿线是方向准确率。
- 红线是平均绝对价格误差。
"""
    (external_dir / "README.md").write_text(content, encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    external_dir = ensure_output_dirs(output_dir)

    rows = load_csv(Path(args.rows_csv))
    metrics = load_csv(Path(args.metrics_csv))
    expiry_rows = load_csv(Path(args.expiry_accuracy_csv))
    absolute_rows = load_csv(Path(args.absolute_time_accuracy_csv))
    summary = load_summary(Path(args.summary_json))

    plot_theory_vs_actual(rows, summary, output_dir)
    plot_model_comparison(metrics, output_dir)
    plot_external_actual_vs_implied_expiry(expiry_rows, external_dir)
    plot_external_expiry_accuracy(expiry_rows, external_dir)
    plot_external_actual_vs_implied_absolute_time(absolute_rows, external_dir)
    plot_external_absolute_accuracy(absolute_rows, external_dir)
    write_external_readme(external_dir)


if __name__ == "__main__":
    main()
