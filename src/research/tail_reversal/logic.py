from dataclasses import dataclass
from typing import Dict, Iterable, Optional


@dataclass
class MarketAnalysis:
    condition_id: str
    market_slug: str
    title: str
    winning_side: str
    losing_side: str
    threshold: float
    side_a_label: str
    side_b_label: str
    side_a_token_id: str
    side_b_token_id: str
    side_a_max_price: float
    side_b_max_price: float
    losing_max_price: float
    any_side_touched_threshold: bool
    losing_side_touched_threshold: bool
    trade_count: int
    first_trade_timestamp: Optional[int]
    last_trade_timestamp: Optional[int]

    def to_dict(self) -> Dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "title": self.title,
            "winning_side": self.winning_side,
            "losing_side": self.losing_side,
            "threshold": self.threshold,
            "side_a_label": self.side_a_label,
            "side_b_label": self.side_b_label,
            "side_a_token_id": self.side_a_token_id,
            "side_b_token_id": self.side_b_token_id,
            "side_a_max_price": self.side_a_max_price,
            "side_b_max_price": self.side_b_max_price,
            "losing_max_price": self.losing_max_price,
            "any_side_touched_threshold": self.any_side_touched_threshold,
            "losing_side_touched_threshold": self.losing_side_touched_threshold,
            "trade_count": self.trade_count,
            "first_trade_timestamp": self.first_trade_timestamp,
            "last_trade_timestamp": self.last_trade_timestamp,
        }


def _normalize_label(value: object) -> str:
    return str(value or "").strip().lower()


def analyze_market(
    market: Dict[str, object],
    orders: Iterable[Dict[str, object]],
    threshold: float,
) -> Optional[MarketAnalysis]:
    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return None

    side_a_label = str(side_a.get("label") or "").strip()
    side_b_label = str(side_b.get("label") or "").strip()
    side_a_token_id = str(side_a.get("id") or "").strip()
    side_b_token_id = str(side_b.get("id") or "").strip()
    winning_side = str(market.get("winning_side") or "").strip()

    if not side_a_label or not side_b_label or not side_a_token_id or not side_b_token_id or not winning_side:
        return None

    winning_norm = _normalize_label(winning_side)
    side_a_norm = _normalize_label(side_a_label)
    side_b_norm = _normalize_label(side_b_label)

    if winning_norm == side_a_norm:
        losing_side = side_b_label
        losing_token_id = side_b_token_id
    elif winning_norm == side_b_norm:
        losing_side = side_a_label
        losing_token_id = side_a_token_id
    else:
        return None

    max_prices = {
        side_a_token_id: 0.0,
        side_b_token_id: 0.0,
    }
    trade_count = 0
    first_trade_timestamp: Optional[int] = None
    last_trade_timestamp: Optional[int] = None

    for order in orders:
        token_id = str(order.get("token_id") or "").strip()
        if token_id not in max_prices:
            continue

        try:
            price = float(order.get("price") or 0.0)
        except (TypeError, ValueError):
            continue

        timestamp_raw = order.get("timestamp")
        try:
            timestamp = int(timestamp_raw) if timestamp_raw is not None else None
        except (TypeError, ValueError):
            timestamp = None

        trade_count += 1
        max_prices[token_id] = max(max_prices[token_id], price)

        if timestamp is not None:
            if first_trade_timestamp is None or timestamp < first_trade_timestamp:
                first_trade_timestamp = timestamp
            if last_trade_timestamp is None or timestamp > last_trade_timestamp:
                last_trade_timestamp = timestamp

    side_a_max_price = max_prices[side_a_token_id]
    side_b_max_price = max_prices[side_b_token_id]
    losing_max_price = max_prices[losing_token_id]

    return MarketAnalysis(
        condition_id=str(market.get("condition_id") or ""),
        market_slug=str(market.get("market_slug") or ""),
        title=str(market.get("title") or ""),
        winning_side=winning_side,
        losing_side=losing_side,
        threshold=threshold,
        side_a_label=side_a_label,
        side_b_label=side_b_label,
        side_a_token_id=side_a_token_id,
        side_b_token_id=side_b_token_id,
        side_a_max_price=side_a_max_price,
        side_b_max_price=side_b_max_price,
        losing_max_price=losing_max_price,
        any_side_touched_threshold=max(side_a_max_price, side_b_max_price) >= threshold,
        losing_side_touched_threshold=losing_max_price >= threshold,
        trade_count=trade_count,
        first_trade_timestamp=first_trade_timestamp,
        last_trade_timestamp=last_trade_timestamp,
    )

