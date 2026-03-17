"""Run: python3 -m src.research.btc_15m_theoretical_value.run_backtest"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from src.research.btc_15m_theoretical_value.logic import (
    EPSILON,
    classify_probability_bucket,
    logit,
    logistic,
    mean,
    qlike,
    theory_variance_forecast,
)

FIFTEEN_MINUTE_SECONDS = 15 * 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run second-level BTC 15m theoretical value backtest.")
    parser.add_argument(
        "--universe-jsonl",
        default="data/processed/btc_15m_theoretical_value/universe/markets.jsonl",
    )
    parser.add_argument(
        "--trades-dir",
        default="data/processed/btc_15m_theoretical_value/trades/by_market",
    )
    parser.add_argument(
        "--rows-csv",
        default="data/processed/btc_15m_theoretical_value/backtest/second_rows.csv",
    )
    parser.add_argument(
        "--metrics-csv",
        default="data/processed/btc_15m_theoretical_value/backtest/model_metrics.csv",
    )
    parser.add_argument(
        "--expiry-accuracy-csv",
        default="data/processed/btc_15m_theoretical_value/backtest/expiry_accuracy.csv",
    )
    parser.add_argument(
        "--absolute-time-accuracy-csv",
        default="data/processed/btc_15m_theoretical_value/backtest/absolute_time_accuracy.csv",
    )
    parser.add_argument(
        "--summary-json",
        default="data/processed/btc_15m_theoretical_value/backtest/summary.json",
    )
    parser.add_argument("--jump-threshold", type=float, default=2.5)
    parser.add_argument("--history-window-seconds", type=int, default=120)
    parser.add_argument("--min-history-seconds", type=int, default=10)
    parser.add_argument("--min-future-seconds", type=int, default=10)
    parser.add_argument("--absolute-time-bin-seconds", type=int, default=900)
    parser.add_argument("--max-markets", type=int, default=0)
    return parser.parse_args()


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


def load_trade_files(trades_dir: Path) -> Dict[str, List[Dict[str, object]]]:
    trade_map: Dict[str, List[Dict[str, object]]] = {}
    if not trades_dir.exists():
        return trade_map
    for path in sorted(trades_dir.glob("*.jsonl")):
        trade_map[path.stem] = load_jsonl(path)
    return trade_map


def parse_float(row: Dict[str, object], key: str) -> float:
    try:
        return float(row.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def parse_int(row: Dict[str, object], key: str) -> int:
    try:
        return int(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def infer_event_window_start(market: Dict[str, object]) -> int:
    market_slug = str(market.get("market_slug") or "")
    slug_parts = market_slug.split("-")
    slug_timestamp = 0
    if slug_parts:
        try:
            slug_timestamp = int(slug_parts[-1])
        except ValueError:
            slug_timestamp = 0
    end_time = parse_int(market, "end_time")
    end_based_start = max(end_time - FIFTEEN_MINUTE_SECONDS, 0)
    if slug_timestamp > 0:
        return max(min(slug_timestamp, end_time), end_based_start)
    return end_based_start


def aggregate_trades_to_seconds(
    market: Dict[str, object],
    market_trades: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    if not market_trades:
        return []
    event_window_start = infer_event_window_start(market)
    end_time = parse_int(market, "end_time")
    grouped: Dict[int, List[Dict[str, object]]] = defaultdict(list)
    for trade in market_trades:
        timestamp = parse_int(trade, "timestamp")
        if timestamp <= 0 or timestamp < event_window_start:
            continue
        if end_time > 0 and timestamp > end_time:
            continue
        grouped[timestamp].append(trade)

    seconds: List[Dict[str, object]] = []
    prev_probability = None
    prev_x = None
    for timestamp in sorted(grouped.keys()):
        bucket = grouped[timestamp]
        total_size = sum(max(parse_float(item, "shares_normalized"), 0.0) for item in bucket)
        if total_size <= 0:
            prices = [parse_float(item, "p_up") for item in bucket]
            probability = prices[-1] if prices else 0.5
        else:
            probability = sum(parse_float(item, "p_up") * max(parse_float(item, "shares_normalized"), 0.0) for item in bucket) / total_size
        x_value = logit(probability)
        if prev_probability is None:
            p_ret = 0.0
            x_ret = 0.0
        else:
            p_ret = probability - prev_probability
            x_ret = x_value - float(prev_x)
        seconds.append(
            {
                "condition_id": str(market.get("condition_id") or ""),
                "market_slug": str(market.get("market_slug") or bucket[-1].get("market_slug") or ""),
                "title": str(market.get("title") or bucket[-1].get("title") or ""),
                "event_window_start": event_window_start,
                "end_time": end_time,
                "timestamp": timestamp,
                "p_up": probability,
                "x_value": x_value,
                "trade_count": len(bucket),
                "shares_normalized": total_size,
                "p_ret": p_ret,
                "x_ret": x_ret,
                "dt_seconds": 1.0,
            }
        )
        prev_probability = probability
        prev_x = x_value
    return seconds


def future_window_stats(
    market_rows: List[Dict[str, object]],
    *,
    index: int,
    horizon_seconds: int,
) -> Dict[str, float]:
    end_index = min(index + horizon_seconds, len(market_rows) - 1)
    future_rows = market_rows[index + 1 : end_index + 1]
    x_components = [float(row["x_ret"]) ** 2 for row in future_rows]
    p_components = [float(row["p_ret"]) ** 2 for row in future_rows]
    rv_x = sum(x_components)
    rv_p = sum(p_components)
    realized_jump_share = (max(x_components) / rv_x) if rv_x > 0 and x_components else 0.0
    current_probability = float(market_rows[index]["p_up"])
    realized_corridor = rv_x if 0.25 <= current_probability <= 0.75 else 0.0
    future_probabilities = [float(row["p_up"]) for row in future_rows]
    realized_first_passage_up_90 = 1.0 if future_probabilities and max(future_probabilities) >= 0.9 else 0.0
    realized_first_passage_down_10 = 1.0 if future_probabilities and min(future_probabilities) <= 0.1 else 0.0
    return {
        "target_rv": rv_x,
        "target_p_var": rv_p,
        "realized_jump_share": realized_jump_share,
        "realized_corridor": realized_corridor,
        "realized_first_passage_up_90": realized_first_passage_up_90,
        "realized_first_passage_down_10": realized_first_passage_down_10,
        "future_second_count": float(len(future_rows)),
    }


def build_second_rows(
    universe_rows: List[Dict[str, object]],
    trade_map: Dict[str, List[Dict[str, object]]],
    *,
    history_window_seconds: int,
    min_history_seconds: int,
    min_future_seconds: int,
    jump_threshold: float,
    max_markets: int,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    processed_markets = 0
    for market in universe_rows:
        condition_id = str(market.get("condition_id") or "")
        market_trades = trade_map.get(condition_id, [])
        second_rows = aggregate_trades_to_seconds(market, market_trades)
        if len(second_rows) < max(min_history_seconds + 1, 2):
            continue
        processed_markets += 1
        if max_markets and processed_markets > max_markets:
            break

        for index in range(1, len(second_rows) - 1):
            row = second_rows[index]
            hist_start = max(0, index - history_window_seconds + 1)
            hist_slice = second_rows[hist_start : index + 1]
            if len(hist_slice) < min_history_seconds:
                continue
            hist_x = [float(item["x_ret"]) for item in hist_slice]
            hist_p = [float(item["p_ret"]) for item in hist_slice]
            hist_dt = [1.0 for _ in hist_slice]

            probability = float(row["p_up"])
            x_value = float(row["x_value"])
            theory_60 = theory_variance_forecast(
                probability=probability,
                x_returns=hist_x,
                dt_seconds=hist_dt,
                horizon_seconds=60,
                jump_threshold=jump_threshold,
            )
            theory_300 = theory_variance_forecast(
                probability=probability,
                x_returns=hist_x,
                dt_seconds=hist_dt,
                horizon_seconds=300,
                jump_threshold=jump_threshold,
            )
            total_hist_seconds = float(len(hist_slice))
            delta_scale = max((probability * (1.0 - probability)) ** 2, EPSILON)
            baseline_x_var_per_second = max(sum(value * value for value in hist_x) / total_hist_seconds, EPSILON)
            baseline_x_60 = max(60.0 * baseline_x_var_per_second, EPSILON)
            baseline_x_300 = max(300.0 * baseline_x_var_per_second, EPSILON)
            baseline_p_var_per_second = max(sum(value * value for value in hist_p) / total_hist_seconds, EPSILON)
            baseline_p_60 = max(60.0 * baseline_p_var_per_second / delta_scale, EPSILON)
            baseline_p_300 = max(300.0 * baseline_p_var_per_second / delta_scale, EPSILON)

            target_60 = future_window_stats(second_rows, index=index, horizon_seconds=60)
            target_300 = future_window_stats(second_rows, index=index, horizon_seconds=300)
            if target_60["future_second_count"] < min_future_seconds or target_300["future_second_count"] < min_future_seconds:
                continue
            next_row = second_rows[index + 1]
            model_implied_price_next_second = logistic(x_value + float(theory_60["drift_per_second"]))
            actual_price_next_second = float(next_row["p_up"])
            predicted_move_next_second = model_implied_price_next_second - probability
            actual_move_next_second = actual_price_next_second - probability
            price_abs_error_next_second = abs(model_implied_price_next_second - actual_price_next_second)
            direction_match_next_second = 1.0 if predicted_move_next_second * actual_move_next_second > 0 else 0.0
            price_fit_weight_next_second = max(0.0, 1.0 - price_abs_error_next_second)
            weighted_direction_score_next_second = direction_match_next_second * price_fit_weight_next_second
            seconds_to_expiry = max(parse_int(row, "end_time") - parse_int(row, "timestamp"), 0)

            rows.append(
                {
                    "condition_id": condition_id,
                    "market_slug": str(row["market_slug"]),
                    "title": str(row["title"]),
                    "timestamp": parse_int(row, "timestamp"),
                    "event_window_start": parse_int(row, "event_window_start"),
                    "end_time": parse_int(row, "end_time"),
                    "seconds_since_event_start": max(parse_int(row, "timestamp") - parse_int(row, "event_window_start"), 0),
                    "seconds_to_expiry": seconds_to_expiry,
                    "minutes_to_expiry": seconds_to_expiry / 60.0,
                    "bucket": classify_probability_bucket(probability),
                    "p_up": probability,
                    "x_value": x_value,
                    "trade_count_second": int(row["trade_count"]),
                    "shares_normalized_second": float(row["shares_normalized"]),
                    "effective_history_seconds": len(hist_slice),
                    "p_ret_second": float(row["p_ret"]),
                    "x_ret_second": float(row["x_ret"]),
                    "theoretical_event_value": float(theory_300["theoretical_event_value"]),
                    "drift_per_second": float(theory_300["drift_per_second"]),
                    "diffusion_var_per_second": float(theory_300["diffusion_var_per_second"]),
                    "jump_var_per_second": float(theory_300["jump_var_per_second"]),
                    "jump_intensity_per_second": float(theory_300["jump_intensity_per_second"]),
                    "avg_jump_sq": float(theory_300["avg_jump_sq"]),
                    "model_implied_price_next_second": model_implied_price_next_second,
                    "actual_price_next_second": actual_price_next_second,
                    "predicted_move_next_second": predicted_move_next_second,
                    "actual_move_next_second": actual_move_next_second,
                    "price_abs_error_next_second": price_abs_error_next_second,
                    "direction_match_next_second": direction_match_next_second,
                    "price_fit_weight_next_second": price_fit_weight_next_second,
                    "weighted_direction_score_next_second": weighted_direction_score_next_second,
                    "pred_theory_x_60s": float(theory_60["predicted_total_variance_x"]),
                    "pred_theory_x_300s": float(theory_300["predicted_total_variance_x"]),
                    "pred_theory_x_diffusion_300s": float(theory_300["predicted_diffusion_component_x"]),
                    "pred_theory_x_jump_300s": float(theory_300["predicted_jump_component_x"]),
                    "pred_theory_jump_share_x": float(theory_300["predicted_jump_share_x"]),
                    "pred_theory_p_60s": float(theory_60["predicted_total_variance_p"]),
                    "pred_theory_p_300s": float(theory_300["predicted_total_variance_p"]),
                    "pred_theory_p_diffusion_300s": float(theory_300["predicted_diffusion_component_p"]),
                    "pred_theory_p_jump_300s": float(theory_300["predicted_jump_component_p"]),
                    "pred_theory_jump_share_p": float(theory_300["predicted_jump_share_p"]),
                    "pred_corridor_x_300s": float(theory_300["predicted_corridor_variance_x"]),
                    "pred_first_passage_up_90_300s": float(theory_300["predicted_first_passage_up_90"]),
                    "pred_first_passage_down_10_300s": float(theory_300["predicted_first_passage_down_10"]),
                    "pred_x_roll_60s": baseline_x_60,
                    "pred_x_roll_300s": baseline_x_300,
                    "pred_p_roll_60s": baseline_p_60,
                    "pred_p_roll_300s": baseline_p_300,
                    "target_rv_60s": float(target_60["target_rv"]),
                    "target_rv_300s": float(target_300["target_rv"]),
                    "target_p_var_60s": float(target_60["target_p_var"]),
                    "target_p_var_300s": float(target_300["target_p_var"]),
                    "realized_jump_share_300s": float(target_300["realized_jump_share"]),
                    "realized_corridor_variance_300s": float(target_300["realized_corridor"]),
                    "realized_first_passage_up_90_300s": float(target_300["realized_first_passage_up_90"]),
                    "realized_first_passage_down_10_300s": float(target_300["realized_first_passage_down_10"]),
                    "future_second_count_60s": float(target_60["future_second_count"]),
                    "future_second_count_300s": float(target_300["future_second_count"]),
                }
            )
    return rows


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_metrics(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    groups: Dict[tuple[str, str, str], List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        bucket = str(row["bucket"])
        for bucket_key in (bucket, "all"):
            groups[("theory_x", "60s", bucket_key)].append(
                {"pred": float(row["pred_theory_x_60s"]), "target": float(row["target_rv_60s"])}
            )
            groups[("theory_x", "300s", bucket_key)].append(
                {"pred": float(row["pred_theory_x_300s"]), "target": float(row["target_rv_300s"])}
            )
            groups[("x_roll", "60s", bucket_key)].append(
                {"pred": float(row["pred_x_roll_60s"]), "target": float(row["target_rv_60s"])}
            )
            groups[("x_roll", "300s", bucket_key)].append(
                {"pred": float(row["pred_x_roll_300s"]), "target": float(row["target_rv_300s"])}
            )
            groups[("p_roll", "60s", bucket_key)].append(
                {"pred": float(row["pred_p_roll_60s"]), "target": float(row["target_rv_60s"])}
            )
            groups[("p_roll", "300s", bucket_key)].append(
                {"pred": float(row["pred_p_roll_300s"]), "target": float(row["target_rv_300s"])}
            )
    metrics: List[Dict[str, object]] = []
    for (model, horizon, bucket), items in sorted(groups.items()):
        mse = mean((item["pred"] - item["target"]) ** 2 for item in items)
        mae = mean(abs(item["pred"] - item["target"]) for item in items)
        qlike_value = mean(qlike(item["target"], item["pred"]) for item in items)
        logmse = mean((math.log(max(item["pred"], EPSILON)) - math.log(max(item["target"], EPSILON))) ** 2 for item in items)
        metrics.append(
            {
                "model": model,
                "horizon": horizon,
                "bucket": bucket,
                "sample_count": len(items),
                "mse": mse,
                "mae": mae,
                "qlike": qlike_value,
                "logmse": logmse,
            }
        )
    return metrics


def aggregate_expiry_accuracy(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    grouped: Dict[int, List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        bucket = int(row["seconds_to_expiry"])
        grouped[bucket].append(row)
    results: List[Dict[str, object]] = []
    for seconds_to_expiry in sorted(grouped.keys()):
        items = grouped[seconds_to_expiry]
        results.append(
            {
                "seconds_to_expiry": seconds_to_expiry,
                "minutes_to_expiry": seconds_to_expiry / 60.0,
                "sample_count": len(items),
                "actual_price_next_second": mean(float(item["actual_price_next_second"]) for item in items),
                "model_implied_price_next_second": mean(float(item["model_implied_price_next_second"]) for item in items),
                "value_weighted_accuracy": mean(float(item["weighted_direction_score_next_second"]) for item in items),
                "directional_accuracy": mean(float(item["direction_match_next_second"]) for item in items),
                "mean_abs_price_error": mean(float(item["price_abs_error_next_second"]) for item in items),
            }
        )
    return results


def aggregate_absolute_time_accuracy(rows: List[Dict[str, object]], *, bin_seconds: int) -> List[Dict[str, object]]:
    grouped: Dict[int, List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        timestamp = int(row["timestamp"])
        bin_timestamp = timestamp - (timestamp % bin_seconds)
        grouped[bin_timestamp].append(row)
    results: List[Dict[str, object]] = []
    for bin_timestamp in sorted(grouped.keys()):
        items = grouped[bin_timestamp]
        results.append(
            {
                "bin_timestamp": bin_timestamp,
                "sample_count": len(items),
                "actual_price_next_second": mean(float(item["actual_price_next_second"]) for item in items),
                "model_implied_price_next_second": mean(float(item["model_implied_price_next_second"]) for item in items),
                "value_weighted_accuracy": mean(float(item["weighted_direction_score_next_second"]) for item in items),
                "directional_accuracy": mean(float(item["direction_match_next_second"]) for item in items),
                "mean_abs_price_error": mean(float(item["price_abs_error_next_second"]) for item in items),
            }
        )
    return results


def choose_example_markets(rows: List[Dict[str, object]]) -> Dict[str, str]:
    grouped: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["condition_id"])].append(row)
    swing_market = ""
    boundary_market = ""
    swing_score = -1.0
    boundary_score = -1.0
    for condition_id, items in grouped.items():
        probabilities = [float(item["p_up"]) for item in items]
        spread = max(probabilities) - min(probabilities)
        boundary_hits = sum(1 for value in probabilities if value <= 0.1 or value >= 0.9)
        if spread > swing_score:
            swing_score = spread
            swing_market = condition_id
        if boundary_hits > boundary_score:
            boundary_score = float(boundary_hits)
            boundary_market = condition_id
    return {
        "swing_market_condition_id": swing_market,
        "boundary_market_condition_id": boundary_market,
    }


def write_summary(
    path: Path,
    *,
    rows: List[Dict[str, object]],
    metrics: List[Dict[str, object]],
    expiry_accuracy: List[Dict[str, object]],
    absolute_time_accuracy: List[Dict[str, object]],
) -> None:
    summary = {
        "strategy_name": "btc_15m_theoretical_value_second_backtest",
        "granularity": "1s",
        "sample_row_count": len(rows),
        "market_count": len({str(row["condition_id"]) for row in rows}),
        "example_markets": choose_example_markets(rows),
        "first_timestamp": min((int(row["timestamp"]) for row in rows), default=None),
        "last_timestamp": max((int(row["timestamp"]) for row in rows), default=None),
        "mean_value_weighted_accuracy": mean(float(row["weighted_direction_score_next_second"]) for row in rows),
        "mean_directional_accuracy": mean(float(row["direction_match_next_second"]) for row in rows),
        "mean_abs_price_error": mean(float(row["price_abs_error_next_second"]) for row in rows),
        "mean_predicted_jump_share": mean(float(row["pred_theory_jump_share_x"]) for row in rows),
        "mean_realized_jump_share": mean(float(row["realized_jump_share_300s"]) for row in rows),
        "best_qlike_300s_model": min(
            (
                {"model": row["model"], "qlike": row["qlike"]}
                for row in metrics
                if row["horizon"] == "300s" and row["bucket"] == "all"
            ),
            key=lambda item: float(item["qlike"]),
            default=None,
        ),
        "expiry_curve_points": len(expiry_accuracy),
        "absolute_time_curve_points": len(absolute_time_accuracy),
        "generated_at": int(time.time()),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    universe_rows = load_jsonl(Path(args.universe_jsonl))
    trade_map = load_trade_files(Path(args.trades_dir))
    rows = build_second_rows(
        universe_rows,
        trade_map,
        history_window_seconds=args.history_window_seconds,
        min_history_seconds=args.min_history_seconds,
        min_future_seconds=args.min_future_seconds,
        jump_threshold=args.jump_threshold,
        max_markets=args.max_markets,
    )
    metrics = build_metrics(rows)
    expiry_accuracy = aggregate_expiry_accuracy(rows)
    absolute_time_accuracy = aggregate_absolute_time_accuracy(rows, bin_seconds=args.absolute_time_bin_seconds)

    write_csv(Path(args.rows_csv), rows)
    write_csv(Path(args.metrics_csv), metrics)
    write_csv(Path(args.expiry_accuracy_csv), expiry_accuracy)
    write_csv(Path(args.absolute_time_accuracy_csv), absolute_time_accuracy)
    write_summary(
        Path(args.summary_json),
        rows=rows,
        metrics=metrics,
        expiry_accuracy=expiry_accuracy,
        absolute_time_accuracy=absolute_time_accuracy,
    )


if __name__ == "__main__":
    main()
