from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


SECONDS_PER_MINUTE = 60
FIVE_MINUTE_INTERVAL = 5 * SECONDS_PER_MINUTE
MAX_FIVE_MINUTE_DURATION_SECONDS = 10 * SECONDS_PER_MINUTE
ONE_MINUTE_CANDLE_INTERVAL = 1
MAX_ONE_MINUTE_RANGE_SECONDS = 7 * 24 * 60 * 60


@dataclass
class ArrivalHit:
    condition_id: str
    market_slug: str
    event_slug: str
    title: str
    start_time: Optional[int]
    end_time: Optional[int]
    yes_token_id: str
    no_token_id: str
    yes_max_price: float
    no_max_price: float
    yes_hit_levels: List[str]
    no_hit_levels: List[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "event_slug": self.event_slug,
            "title": self.title,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "yes_token_id": self.yes_token_id,
            "no_token_id": self.no_token_id,
            "yes_max_price": self.yes_max_price,
            "no_max_price": self.no_max_price,
            "yes_hit_levels": self.yes_hit_levels,
            "no_hit_levels": self.no_hit_levels,
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

    is_btc = "bitcoin" in combined_text or "btc" in combined_text
    if not is_btc:
        return False

    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return False

    side_a_label = str(side_a.get("label") or "").strip().lower()
    side_b_label = str(side_b.get("label") or "").strip().lower()
    if {side_a_label, side_b_label} != {"yes", "no"}:
        return False

    start_time_raw = market.get("start_time")
    end_time_raw = market.get("end_time")
    try:
        start_time = int(start_time_raw)
        end_time = int(end_time_raw)
    except (TypeError, ValueError):
        return False

    duration = end_time - start_time
    if duration <= 0:
        return False

    return duration <= MAX_FIVE_MINUTE_DURATION_SECONDS


def extract_yes_no_tokens(market: Dict[str, object]) -> Optional[Dict[str, str]]:
    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return None

    side_a_label = str(side_a.get("label") or "").strip().lower()
    side_b_label = str(side_b.get("label") or "").strip().lower()
    side_a_token = str(side_a.get("id") or "").strip()
    side_b_token = str(side_b.get("id") or "").strip()

    if not side_a_token or not side_b_token:
        return None

    if side_a_label == "yes" and side_b_label == "no":
        return {"yes_token_id": side_a_token, "no_token_id": side_b_token}
    if side_a_label == "no" and side_b_label == "yes":
        return {"yes_token_id": side_b_token, "no_token_id": side_a_token}
    return None


def analyze_market_arrival(
    market: Dict[str, object],
    candle_payloads: Iterable[Dict[str, object]],
    thresholds: List[float],
) -> Optional[ArrivalHit]:
    tokens = extract_yes_no_tokens(market)
    if tokens is None:
        return None

    yes_max_price = 0.0
    no_max_price = 0.0

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

        if token_id == tokens["yes_token_id"] and current_max > yes_max_price:
            yes_max_price = current_max
        if token_id == tokens["no_token_id"] and current_max > no_max_price:
            no_max_price = current_max

    yes_hit_levels = [format_threshold(level) for level in thresholds if yes_max_price >= level]
    no_hit_levels = [format_threshold(level) for level in thresholds if no_max_price >= level]

    if not yes_hit_levels and not no_hit_levels:
        return None

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
        yes_token_id=tokens["yes_token_id"],
        no_token_id=tokens["no_token_id"],
        yes_max_price=yes_max_price,
        no_max_price=no_max_price,
        yes_hit_levels=yes_hit_levels,
        no_hit_levels=no_hit_levels,
    )
