from __future__ import annotations

import math
from typing import Dict, Iterable, List, Optional, Sequence


FIFTEEN_MINUTE_SECONDS = 900
EPSILON = 1e-5
SUPPORTED_BINARY_LABEL_SETS = ({"yes", "no"}, {"up", "down"})


def normalize_label(value: object) -> str:
    return str(value or "").strip().lower()


def align_timestamp_to_step(timestamp: int, step_seconds: int) -> int:
    return timestamp - (timestamp % step_seconds)


def build_market_slugs(
    *,
    slug_prefix: str,
    start_timestamp: int,
    step_seconds: int,
    batch_size: int,
    end_timestamp: int,
) -> List[str]:
    slugs: List[str] = []
    current_timestamp = start_timestamp
    while current_timestamp <= end_timestamp and len(slugs) < batch_size:
        slugs.append(f"{slug_prefix}-{current_timestamp}")
        current_timestamp += step_seconds
    return slugs


def market_is_btc_fifteen_minute(market: Dict[str, object]) -> bool:
    title = str(market.get("title") or "").lower()
    market_slug = str(market.get("market_slug") or "").lower()
    event_slug = str(market.get("event_slug") or "").lower()
    combined = " ".join([title, market_slug, event_slug])
    if not market_slug.startswith("btc-updown-15m-"):
        return False
    if "bitcoin" not in combined and "btc" not in combined:
        return False

    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return False

    labels = {normalize_label(side_a.get("label")), normalize_label(side_b.get("label"))}
    return labels in SUPPORTED_BINARY_LABEL_SETS


def extract_binary_tokens(market: Dict[str, object]) -> Optional[Dict[str, str]]:
    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return None

    label_a = str(side_a.get("label") or "").strip()
    label_b = str(side_b.get("label") or "").strip()
    token_a = str(side_a.get("id") or "").strip()
    token_b = str(side_b.get("id") or "").strip()
    if not label_a or not label_b or not token_a or not token_b:
        return None
    if {label_a.lower(), label_b.lower()} not in SUPPORTED_BINARY_LABEL_SETS:
        return None
    return {
        "outcome_a_label": label_a,
        "outcome_b_label": label_b,
        "outcome_a_token_id": token_a,
        "outcome_b_token_id": token_b,
    }


def clamp_probability(value: float, epsilon: float = EPSILON) -> float:
    return min(max(value, epsilon), 1.0 - epsilon)


def logit(probability: float, epsilon: float = EPSILON) -> float:
    clipped = clamp_probability(probability, epsilon)
    return math.log(clipped / (1.0 - clipped))


def logistic(x_value: float) -> float:
    return 1.0 / (1.0 + math.exp(-x_value))


def sigmoid_prime(x_value: float) -> float:
    probability = logistic(x_value)
    return probability * (1.0 - probability)


def sigmoid_second(x_value: float) -> float:
    probability = logistic(x_value)
    return probability * (1.0 - probability) * (1.0 - 2.0 * probability)


def qlike(actual: float, predicted: float, epsilon: float = 1e-12) -> float:
    actual_value = max(actual, epsilon)
    predicted_value = max(predicted, epsilon)
    ratio = actual_value / predicted_value
    return ratio - math.log(ratio) - 1.0


def mean(values: Iterable[float]) -> float:
    items = list(values)
    if not items:
        return 0.0
    return sum(items) / len(items)


def classify_probability_bucket(probability: float) -> str:
    if probability <= 0.1 or probability >= 0.9:
        return "boundary"
    if 0.25 <= probability <= 0.75:
        return "core"
    return "middle"


def choose_up_down_labels(market: Dict[str, object]) -> tuple[str | None, str | None]:
    labels = [str(market.get("outcome_a_label") or ""), str(market.get("outcome_b_label") or "")]
    up_label = None
    down_label = None
    for label in labels:
        lowered = label.lower()
        if lowered in {"up", "yes"}:
            up_label = label
        if lowered in {"down", "no"}:
            down_label = label
    return up_label, down_label


def normalize_trade_price_to_probability(
    token_label: str,
    price: float,
    *,
    up_label: str,
    down_label: str,
) -> Optional[float]:
    normalized_label = normalize_label(token_label)
    normalized_up = normalize_label(up_label)
    normalized_down = normalize_label(down_label)
    if normalized_label == normalized_up:
        return clamp_probability(price)
    if normalized_label == normalized_down:
        return clamp_probability(1.0 - price)
    return None


def collapse_trade_mirrors(order_rows: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
    collapsed: List[Dict[str, object]] = []
    seen: set[tuple[object, ...]] = set()
    sorted_rows = sorted(
        order_rows,
        key=lambda row: (
            int(row.get("timestamp") or 0),
            int(row.get("block_number") or 0),
            int(row.get("log_index") or 0),
            str(row.get("tx_hash") or ""),
            str(row.get("order_hash") or ""),
        ),
    )
    for row in sorted_rows:
        signature = (
            str(row.get("condition_id") or ""),
            str(row.get("token_id") or ""),
            int(row.get("timestamp") or 0),
            int(row.get("block_number") or 0),
            str(row.get("tx_hash") or ""),
            float(row.get("price") or 0.0),
            float(row.get("shares_normalized") or 0.0),
        )
        if signature in seen:
            continue
        seen.add(signature)
        collapsed.append(dict(row))
    return collapsed


def estimate_diffusion_and_jump(
    x_returns: Sequence[float],
    dt_seconds: Sequence[float],
    *,
    jump_threshold: float = 2.5,
) -> Dict[str, float | List[float]]:
    if len(x_returns) < 2 or len(x_returns) != len(dt_seconds):
        return {
            "diffusion_var_per_second": 0.0,
            "jump_var_per_second": 0.0,
            "jump_intensity_per_second": 0.0,
            "avg_jump_sq": 0.0,
            "expected_dt_seconds": 1.0,
            "empirical_jump_sizes": [],
        }

    normalized_sq = []
    for value, dt_value in zip(x_returns, dt_seconds):
        normalized_sq.append((value * value) / max(dt_value, 1.0))
    local_scale = math.sqrt(max(mean(normalized_sq), 1e-12))

    jumps: List[float] = []
    continuous_sq_per_second: List[float] = []
    total_time = 0.0
    jump_time = 0.0
    continuous_time = 0.0
    for value, dt_value in zip(x_returns, dt_seconds):
        scaled_move = abs(value) / math.sqrt(max(dt_value, 1.0))
        total_time += max(dt_value, 1.0)
        if scaled_move >= jump_threshold * local_scale:
            jumps.append(value)
            jump_time += max(dt_value, 1.0)
        else:
            continuous_sq_per_second.append((value * value) / max(dt_value, 1.0))
            continuous_time += max(dt_value, 1.0)

    diffusion_var_per_second = mean(continuous_sq_per_second)
    avg_jump_sq = mean(value * value for value in jumps)
    jump_intensity = len(jumps) / max(total_time, 1.0)
    jump_var_per_second = jump_intensity * avg_jump_sq
    expected_dt_seconds = max(mean(dt_seconds), 1.0)

    return {
        "diffusion_var_per_second": diffusion_var_per_second,
        "jump_var_per_second": jump_var_per_second,
        "jump_intensity_per_second": jump_intensity,
        "avg_jump_sq": avg_jump_sq,
        "expected_dt_seconds": expected_dt_seconds,
        "empirical_jump_sizes": jumps,
    }


def truncation_function(z_value: float) -> float:
    return z_value if abs(z_value) <= 1.0 else 0.0


def jump_compensation_term(x_value: float, jump_sizes: Sequence[float], jump_intensity_per_second: float) -> float:
    if not jump_sizes or jump_intensity_per_second <= 0:
        return 0.0
    s_x = logistic(x_value)
    s_prime_x = sigmoid_prime(x_value)
    if abs(s_prime_x) <= 1e-12:
        return 0.0
    average_term = mean(
        logistic(x_value + jump_size) - s_x - s_prime_x * truncation_function(jump_size)
        for jump_size in jump_sizes
    )
    return jump_intensity_per_second * average_term / s_prime_x


def martingale_constrained_drift(
    x_value: float,
    diffusion_var_per_second: float,
    jump_sizes: Sequence[float],
    jump_intensity_per_second: float,
) -> float:
    s_prime_x = sigmoid_prime(x_value)
    if abs(s_prime_x) <= 1e-12:
        return 0.0
    diffusion_term = 0.5 * sigmoid_second(x_value) * diffusion_var_per_second / s_prime_x
    jump_term = jump_compensation_term(x_value, jump_sizes, jump_intensity_per_second)
    return -(diffusion_term + jump_term)


def estimate_p_diffusion_variance(probability: float, diffusion_var_per_second: float) -> float:
    clipped = clamp_probability(probability)
    return (clipped * (1.0 - clipped)) ** 2 * diffusion_var_per_second


def estimate_p_jump_variance(x_value: float, jump_sizes: Sequence[float], jump_intensity_per_second: float) -> float:
    if not jump_sizes or jump_intensity_per_second <= 0:
        return 0.0
    base_probability = logistic(x_value)
    average_jump_sq = mean((logistic(x_value + jump_size) - base_probability) ** 2 for jump_size in jump_sizes)
    return jump_intensity_per_second * average_jump_sq


def corridor_indicator(probability: float, lower: float = 0.25, upper: float = 0.75) -> float:
    return 1.0 if lower <= probability <= upper else 0.0


def first_passage_probability(
    probability: float,
    x_value: float,
    diffusion_var_per_second: float,
    jump_sizes: Sequence[float],
    jump_intensity_per_second: float,
    *,
    threshold_probability: float,
    horizon_seconds: int,
) -> float:
    threshold_x = logit(threshold_probability)
    direction = 1.0 if threshold_x >= x_value else -1.0
    distance = abs(threshold_x - x_value)
    if distance <= 0:
        return 1.0
    diffusion_scale = math.sqrt(max(diffusion_var_per_second * horizon_seconds, 1e-12))
    diffusion_hit = math.exp(-(distance ** 2) / max(2.0 * diffusion_scale ** 2, 1e-12))
    jump_hit_probability = 0.0
    if jump_sizes and jump_intensity_per_second > 0:
        hit_count = sum(1 for jump_size in jump_sizes if direction * jump_size >= distance)
        jump_hit_probability = jump_intensity_per_second * horizon_seconds * (hit_count / len(jump_sizes))
    return min(max(diffusion_hit + jump_hit_probability, 0.0), 1.0)


def theory_variance_forecast(
    *,
    probability: float,
    x_returns: Sequence[float],
    dt_seconds: Sequence[float],
    horizon_seconds: int,
    jump_threshold: float = 2.5,
) -> Dict[str, float]:
    components = estimate_diffusion_and_jump(x_returns, dt_seconds, jump_threshold=jump_threshold)
    x_value = logit(probability)
    diffusion_var_per_second = float(components["diffusion_var_per_second"])
    jump_var_per_second = float(components["jump_var_per_second"])
    jump_intensity_per_second = float(components["jump_intensity_per_second"])
    empirical_jump_sizes = list(components["empirical_jump_sizes"])
    expected_dt_seconds = float(components["expected_dt_seconds"])

    x_diffusion_component = horizon_seconds * diffusion_var_per_second
    x_jump_component = horizon_seconds * jump_var_per_second
    total_var_x = x_diffusion_component + x_jump_component
    jump_share_x = (x_jump_component / total_var_x) if total_var_x > 0 else 0.0

    p_diffusion_component = horizon_seconds * estimate_p_diffusion_variance(probability, diffusion_var_per_second)
    p_jump_component = horizon_seconds * estimate_p_jump_variance(
        x_value,
        empirical_jump_sizes,
        jump_intensity_per_second,
    )
    total_var_p = p_diffusion_component + p_jump_component
    jump_share_p = (p_jump_component / total_var_p) if total_var_p > 0 else 0.0

    drift_per_second = martingale_constrained_drift(
        x_value,
        diffusion_var_per_second,
        empirical_jump_sizes,
        jump_intensity_per_second,
    )
    corridor_x = corridor_indicator(probability) * total_var_x
    first_passage_up_90 = first_passage_probability(
        probability,
        x_value,
        diffusion_var_per_second,
        empirical_jump_sizes,
        jump_intensity_per_second,
        threshold_probability=0.9,
        horizon_seconds=horizon_seconds,
    )
    first_passage_down_10 = first_passage_probability(
        probability,
        x_value,
        diffusion_var_per_second,
        empirical_jump_sizes,
        jump_intensity_per_second,
        threshold_probability=0.1,
        horizon_seconds=horizon_seconds,
    )

    return {
        "diffusion_var_per_second": diffusion_var_per_second,
        "jump_var_per_second": jump_var_per_second,
        "jump_intensity_per_second": jump_intensity_per_second,
        "avg_jump_sq": float(components["avg_jump_sq"]),
        "expected_dt_seconds": expected_dt_seconds,
        "theoretical_event_value": probability,
        "drift_per_second": drift_per_second,
        "predicted_total_variance_x": total_var_x,
        "predicted_diffusion_component_x": x_diffusion_component,
        "predicted_jump_component_x": x_jump_component,
        "predicted_jump_share_x": jump_share_x,
        "predicted_total_variance_p": total_var_p,
        "predicted_diffusion_component_p": p_diffusion_component,
        "predicted_jump_component_p": p_jump_component,
        "predicted_jump_share_p": jump_share_p,
        "predicted_corridor_variance_x": corridor_x,
        "predicted_first_passage_up_90": first_passage_up_90,
        "predicted_first_passage_down_10": first_passage_down_10,
    }
