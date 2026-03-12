from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


SECONDS_PER_MINUTE = 60
MAX_FIVE_MINUTE_DURATION_SECONDS = 10 * SECONDS_PER_MINUTE
ONE_MINUTE_CANDLE_INTERVAL = 1
MAX_ONE_MINUTE_RANGE_SECONDS = 7 * 24 * 60 * 60
SUPPORTED_BINARY_LABEL_SETS = ({"yes", "no"}, {"up", "down"})


@dataclass
class ArrivalHit:
    condition_id: str
    market_slug: str
    event_slug: str
    title: str
    start_time: Optional[int]
    end_time: Optional[int]
    outcome_a_label: str
    outcome_b_label: str
    outcome_a_token_id: str
    outcome_b_token_id: str
    outcome_a_max_price: float
    outcome_b_max_price: float
    outcome_a_hit_levels: List[str]
    outcome_b_hit_levels: List[str]

    def has_any_hit(self) -> bool:
        return bool(self.outcome_a_hit_levels or self.outcome_b_hit_levels)

    def to_dict(self) -> Dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "event_slug": self.event_slug,
            "title": self.title,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "outcome_a_label": self.outcome_a_label,
            "outcome_b_label": self.outcome_b_label,
            "outcome_a_token_id": self.outcome_a_token_id,
            "outcome_b_token_id": self.outcome_b_token_id,
            "outcome_a_max_price": self.outcome_a_max_price,
            "outcome_b_max_price": self.outcome_b_max_price,
            "outcome_a_hit_levels": self.outcome_a_hit_levels,
            "outcome_b_hit_levels": self.outcome_b_hit_levels,
        }


def build_thresholds(start: float = 0.52, end: float = 0.58, step: float = 0.01) -> List[float]:
    if step <= 0:
        raise ValueError("step must be greater than 0")
    if end < start:
        raise ValueError("end must be greater than or equal to start")

    thresholds: List[float] = []
    current = start
    while current <= end + 1e-9:
        thresholds.append(round(current, 2))
        current += step
    return thresholds


def format_threshold(value: float) -> str:
    return f"{value:.2f}"


def market_is_btc_five_minute(market: Dict[str, object]) -> bool:
    title = str(market.get("title") or "").lower()
    market_slug = str(market.get("market_slug") or "").lower()
    event_slug = str(market.get("event_slug") or "").lower()
    combined_text = " ".join([title, market_slug, event_slug])

    if not market_slug.startswith("btc-updown-5m-"):
        return False

    if "bitcoin" not in combined_text and "btc" not in combined_text:
        return False

    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return False

    side_a_label = str(side_a.get("label") or "").strip().lower()
    side_b_label = str(side_b.get("label") or "").strip().lower()
    if {side_a_label, side_b_label} not in SUPPORTED_BINARY_LABEL_SETS:
        return False

    return True


def extract_binary_tokens(market: Dict[str, object]) -> Optional[Dict[str, str]]:
    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return None

    side_a_label = str(side_a.get("label") or "").strip()
    side_b_label = str(side_b.get("label") or "").strip()
    side_a_token = str(side_a.get("id") or "").strip()
    side_b_token = str(side_b.get("id") or "").strip()

    if not side_a_token or not side_b_token:
        return None
    if {side_a_label.lower(), side_b_label.lower()} not in SUPPORTED_BINARY_LABEL_SETS:
        return None

    return {
        "outcome_a_label": side_a_label,
        "outcome_b_label": side_b_label,
        "outcome_a_token_id": side_a_token,
        "outcome_b_token_id": side_b_token,
    }


def analyze_market_arrival(
    market: Dict[str, object],
    candle_payloads: Iterable[Dict[str, object]],
    thresholds: List[float],
) -> Optional[ArrivalHit]:
    tokens = extract_binary_tokens(market)
    if tokens is None:
        return None

    outcome_a_max_price = 0.0
    outcome_b_max_price = 0.0

    for candle_payload in candle_payloads:
        token_id = str(candle_payload.get("token_id") or "").strip()
        candles = candle_payload.get("candles") or []
        if not isinstance(candles, list):
            continue

        current_max = 0.0
        for candle in candles:
            if not isinstance(candle, dict):
                continue
            price = candle.get("price") or {}
            if not isinstance(price, dict):
                continue
            try:
                high_price = float(price.get("high_dollars") or 0.0)
            except (TypeError, ValueError):
                continue
            if high_price > current_max:
                current_max = high_price

        if token_id == tokens["outcome_a_token_id"] and current_max > outcome_a_max_price:
            outcome_a_max_price = current_max
        if token_id == tokens["outcome_b_token_id"] and current_max > outcome_b_max_price:
            outcome_b_max_price = current_max

    outcome_a_hit_levels = [format_threshold(level) for level in thresholds if outcome_a_max_price >= level]
    outcome_b_hit_levels = [format_threshold(level) for level in thresholds if outcome_b_max_price >= level]

    start_time_raw = market.get("start_time")
    end_time_raw = market.get("end_time")
    try:
        start_time = int(start_time_raw) if start_time_raw is not None else None
    except (TypeError, ValueError):
        start_time = None
    try:
        end_time = int(end_time_raw) if end_time_raw is not None else None
    except (TypeError, ValueError):
        end_time = None

    return ArrivalHit(
        condition_id=str(market.get("condition_id") or ""),
        market_slug=str(market.get("market_slug") or ""),
        event_slug=str(market.get("event_slug") or ""),
        title=str(market.get("title") or ""),
        start_time=start_time,
        end_time=end_time,
        outcome_a_label=tokens["outcome_a_label"],
        outcome_b_label=tokens["outcome_b_label"],
        outcome_a_token_id=tokens["outcome_a_token_id"],
        outcome_b_token_id=tokens["outcome_b_token_id"],
        outcome_a_max_price=outcome_a_max_price,
        outcome_b_max_price=outcome_b_max_price,
        outcome_a_hit_levels=outcome_a_hit_levels,
        outcome_b_hit_levels=outcome_b_hit_levels,
    )
