from dataclasses import dataclass
from typing import Dict, Iterable, Optional


SECONDS_PER_DAY = 24 * 60 * 60
MAX_DAILY_RANGE_SECONDS = 365 * SECONDS_PER_DAY
RECOMMENDED_CANDLE_CHUNK_SECONDS = 7 * SECONDS_PER_DAY


@dataclass
class ReversalCandidate:
    condition_id: str
    market_slug: str
    title: str
    threshold: float
    winning_side: str
    losing_side: str
    losing_token_id: str
    losing_max_price: float
    trigger_timestamp: Optional[int]
    market_start_time: Optional[int]
    market_end_time: Optional[int]

    def to_dict(self) -> Dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "title": self.title,
            "threshold": self.threshold,
            "winning_side": self.winning_side,
            "losing_side": self.losing_side,
            "losing_token_id": self.losing_token_id,
            "losing_max_price": self.losing_max_price,
            "trigger_timestamp": self.trigger_timestamp,
            "market_start_time": self.market_start_time,
            "market_end_time": self.market_end_time,
        }


def _normalize_label(value: object) -> str:
    return str(value or "").strip().lower()


def resolve_market_sides(market: Dict[str, object]) -> Optional[Dict[str, str]]:
    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return None

    side_a_label = str(side_a.get("label") or "").strip()
    side_b_label = str(side_b.get("label") or "").strip()
    side_a_token_id = str(side_a.get("id") or "").strip()
    side_b_token_id = str(side_b.get("id") or "").strip()
    winning_side_raw = market.get("winning_side") or {}

    winning_label = ""
    winning_token_id = ""
    if isinstance(winning_side_raw, dict):
        winning_label = str(winning_side_raw.get("label") or "").strip()
        winning_token_id = str(winning_side_raw.get("id") or "").strip()
    else:
        winning_label = str(winning_side_raw or "").strip()

    if not side_a_label or not side_b_label or not side_a_token_id or not side_b_token_id:
        return None

    winning_norm = _normalize_label(winning_label)
    side_a_norm = _normalize_label(side_a_label)
    side_b_norm = _normalize_label(side_b_label)

    if winning_norm == side_a_norm or winning_token_id == side_a_token_id:
        return {
            "winning_side": side_a_label,
            "winning_token_id": side_a_token_id,
            "losing_side": side_b_label,
            "losing_token_id": side_b_token_id,
        }
    if winning_norm == side_b_norm or winning_token_id == side_b_token_id:
        return {
            "winning_side": side_b_label,
            "winning_token_id": side_b_token_id,
            "losing_side": side_a_label,
            "losing_token_id": side_a_token_id,
        }
    return None


def analyze_market_with_candles(
    market: Dict[str, object],
    candle_payloads: Iterable[Dict[str, object]],
    threshold: float,
) -> Optional[ReversalCandidate]:
    sides = resolve_market_sides(market)
    if sides is None:
        return None

    losing_token_id = sides["losing_token_id"]
    losing_max_price = 0.0
    trigger_timestamp: Optional[int] = None

    for candle_payload in candle_payloads:
        token_id = str(candle_payload.get("token_id") or "").strip()
        if token_id != losing_token_id:
            continue

        candles = candle_payload.get("candles") or []
        if not isinstance(candles, list):
            continue

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

            if high_price > losing_max_price:
                losing_max_price = high_price

            if high_price >= threshold and trigger_timestamp is None:
                raw_ts = candle.get("end_period_ts")
                try:
                    trigger_timestamp = int(raw_ts) if raw_ts is not None else None
                except (TypeError, ValueError):
                    trigger_timestamp = None

    if losing_max_price < threshold:
        return None

    start_time_raw = market.get("start_time")
    end_time_raw = market.get("end_time")
    try:
        market_start_time = int(start_time_raw) if start_time_raw is not None else None
    except (TypeError, ValueError):
        market_start_time = None
    try:
        market_end_time = int(end_time_raw) if end_time_raw is not None else None
    except (TypeError, ValueError):
        market_end_time = None

    return ReversalCandidate(
        condition_id=str(market.get("condition_id") or ""),
        market_slug=str(market.get("market_slug") or ""),
        title=str(market.get("title") or ""),
        threshold=threshold,
        winning_side=sides["winning_side"],
        losing_side=sides["losing_side"],
        losing_token_id=losing_token_id,
        losing_max_price=losing_max_price,
        trigger_timestamp=trigger_timestamp,
        market_start_time=market_start_time,
        market_end_time=market_end_time,
    )
