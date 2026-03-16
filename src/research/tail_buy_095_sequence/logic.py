from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from src.research.tail_reversal.logic import (
    MAX_DAILY_RANGE_SECONDS,
    RECOMMENDED_CANDLE_CHUNK_SECONDS,
)


@dataclass
class TailBuySequenceRecord:
    condition_id: str
    market_slug: str
    title: str
    threshold: float
    trigger_timestamp: int
    trigger_side: str
    trigger_token_id: str
    trigger_price: float
    observed_max_price: float
    market_start_time: Optional[int]
    market_end_time: Optional[int]
    outcome: str
    source: str

    def to_dict(self) -> Dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "title": self.title,
            "threshold": self.threshold,
            "trigger_timestamp": self.trigger_timestamp,
            "trigger_side": self.trigger_side,
            "trigger_token_id": self.trigger_token_id,
            "trigger_price": self.trigger_price,
            "observed_max_price": self.observed_max_price,
            "market_start_time": self.market_start_time,
            "market_end_time": self.market_end_time,
            "outcome": self.outcome,
            "source": self.source,
        }


def find_first_threshold_trigger(
    market: Dict[str, object],
    candle_payloads: Iterable[Dict[str, object]],
    *,
    threshold: float,
) -> Optional[TailBuySequenceRecord]:
    first_trigger: Optional[dict] = None

    for candle_payload in candle_payloads:
        token_id = str(candle_payload.get("token_id") or "").strip()
        side = str(candle_payload.get("side") or "").strip()
        candles = candle_payload.get("candles") or []
        if not token_id or not side or not isinstance(candles, list):
            continue

        token_max = 0.0
        token_first_ts: Optional[int] = None
        token_first_price: Optional[float] = None
        for candle in candles:
            if not isinstance(candle, dict):
                continue
            raw_ts = candle.get("end_period_ts")
            try:
                candle_ts = int(raw_ts)
            except (TypeError, ValueError):
                continue
            price = candle.get("price") or {}
            if not isinstance(price, dict):
                continue
            try:
                high_price = float(price.get("high_dollars") or 0.0)
            except (TypeError, ValueError):
                continue
            token_max = max(token_max, high_price)
            if high_price >= threshold and token_first_ts is None:
                token_first_ts = candle_ts
                token_first_price = high_price

        if token_first_ts is None or token_first_price is None:
            continue

        row = {
            "trigger_timestamp": token_first_ts,
            "trigger_side": side,
            "trigger_token_id": token_id,
            "observed_max_price": token_max,
        }
        if first_trigger is None:
            first_trigger = row
            continue
        if row["trigger_timestamp"] < first_trigger["trigger_timestamp"]:
            first_trigger = row

    if first_trigger is None:
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

    return TailBuySequenceRecord(
        condition_id=str(market.get("condition_id") or ""),
        market_slug=str(market.get("market_slug") or ""),
        title=str(market.get("title") or ""),
        threshold=threshold,
        trigger_timestamp=int(first_trigger["trigger_timestamp"]),
        trigger_side=str(first_trigger["trigger_side"]),
        trigger_token_id=str(first_trigger["trigger_token_id"]),
        trigger_price=threshold,
        observed_max_price=float(first_trigger["observed_max_price"]),
        market_start_time=market_start_time,
        market_end_time=market_end_time,
        outcome="success",
        source="market_scan",
    )


__all__ = [
    "MAX_DAILY_RANGE_SECONDS",
    "RECOMMENDED_CANDLE_CHUNK_SECONDS",
    "TailBuySequenceRecord",
    "find_first_threshold_trigger",
]
