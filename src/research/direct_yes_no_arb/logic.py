from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


ARB_THRESHOLD = 1.0
ORDERBOOK_HISTORY_START_MS = 1760400000000  # 2025-10-14T00:00:00Z


@dataclass
class OrderbookSnapshot:
    token_id: str
    timestamp_ms: int
    best_ask: Optional[float]


@dataclass
class MarketSample:
    condition_id: str
    market_slug: str
    title: str
    start_time: Optional[int]
    end_time: Optional[int]
    analysis_start_ms: int
    analysis_end_ms: int
    hit: bool
    hit_count: int
    first_hit_ts_ms: Optional[int]
    best_price_sum: Optional[float]
    best_edge: Optional[float]
    reason: Optional[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "title": self.title,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "analysis_start_ms": self.analysis_start_ms,
            "analysis_end_ms": self.analysis_end_ms,
            "hit": self.hit,
            "hit_count": self.hit_count,
            "first_hit_ts_ms": self.first_hit_ts_ms,
            "best_price_sum": self.best_price_sum,
            "best_edge": self.best_edge,
            "reason": self.reason,
        }


@dataclass
class OpportunityRecord:
    opportunity_key: str
    condition_id: str
    market_slug: str
    title: str
    trigger_ts_ms: int
    yes_ask: float
    no_ask: float
    price_sum: float
    edge: float
    source_side: str

    def to_dict(self) -> Dict[str, object]:
        return {
            "opportunity_key": self.opportunity_key,
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "title": self.title,
            "trigger_ts_ms": self.trigger_ts_ms,
            "yes_ask": self.yes_ask,
            "no_ask": self.no_ask,
            "price_sum": self.price_sum,
            "edge": self.edge,
            "source_side": self.source_side,
        }


def normalize_label(value: object) -> str:
    return str(value or "").strip().lower()


def market_is_yes_no_binary(market: Dict[str, object]) -> bool:
    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    if not isinstance(side_a, dict) or not isinstance(side_b, dict):
        return False

    side_a_id = str(side_a.get("id") or "").strip()
    side_b_id = str(side_b.get("id") or "").strip()
    labels = {normalize_label(side_a.get("label")), normalize_label(side_b.get("label"))}
    return bool(side_a_id and side_b_id and labels == {"yes", "no"})


def extract_yes_no_token_ids(market: Dict[str, object]) -> Optional[Tuple[str, str]]:
    if not market_is_yes_no_binary(market):
        return None

    side_a = market.get("side_a") or {}
    side_b = market.get("side_b") or {}
    side_a_label = normalize_label(side_a.get("label"))
    side_b_label = normalize_label(side_b.get("label"))
    side_a_id = str(side_a.get("id") or "").strip()
    side_b_id = str(side_b.get("id") or "").strip()

    if side_a_label == "yes" and side_b_label == "no":
        return (side_a_id, side_b_id)
    if side_a_label == "no" and side_b_label == "yes":
        return (side_b_id, side_a_id)
    return None


def extract_best_ask(snapshot: Dict[str, object]) -> Optional[float]:
    asks = snapshot.get("asks") or []
    if not isinstance(asks, list) or not asks:
        return None

    best_level = asks[0]
    if not isinstance(best_level, dict):
        return None

    try:
        value = float(best_level.get("price"))
    except (TypeError, ValueError):
        return None
    return value if value > 0.0 else None


def build_orderbook_snapshots(
    token_id: str,
    raw_snapshots: Iterable[Dict[str, object]],
) -> List[OrderbookSnapshot]:
    snapshots: List[OrderbookSnapshot] = []
    for raw in raw_snapshots:
        if not isinstance(raw, dict):
            continue
        try:
            timestamp_ms = int(raw.get("timestamp"))
        except (TypeError, ValueError):
            continue
        best_ask = extract_best_ask(raw)
        snapshots.append(
            OrderbookSnapshot(
                token_id=token_id,
                timestamp_ms=timestamp_ms,
                best_ask=best_ask,
            )
        )
    snapshots.sort(key=lambda item: item.timestamp_ms)
    return snapshots


def analyze_direct_arb(
    market: Dict[str, object],
    *,
    yes_snapshots: Sequence[OrderbookSnapshot],
    no_snapshots: Sequence[OrderbookSnapshot],
    analysis_start_ms: int,
    analysis_end_ms: int,
) -> Tuple[Optional[MarketSample], List[OpportunityRecord]]:
    token_ids = extract_yes_no_token_ids(market)
    if token_ids is None:
        return (None, [])

    condition_id = str(market.get("condition_id") or "")
    market_slug = str(market.get("market_slug") or "")
    title = str(market.get("title") or "")

    try:
        start_time = int(market.get("start_time")) if market.get("start_time") is not None else None
        end_time = int(market.get("end_time")) if market.get("end_time") is not None else None
    except (TypeError, ValueError):
        start_time = None
        end_time = None

    yes_events = [(item.timestamp_ms, "yes", item.best_ask) for item in yes_snapshots]
    no_events = [(item.timestamp_ms, "no", item.best_ask) for item in no_snapshots]
    events = sorted(yes_events + no_events, key=lambda item: (item[0], item[1]))

    if not events:
        sample = MarketSample(
            condition_id=condition_id,
            market_slug=market_slug,
            title=title,
            start_time=start_time,
            end_time=end_time,
            analysis_start_ms=analysis_start_ms,
            analysis_end_ms=analysis_end_ms,
            hit=False,
            hit_count=0,
            first_hit_ts_ms=None,
            best_price_sum=None,
            best_edge=None,
            reason="missing_orderbook_history",
        )
        return (sample, [])

    current_yes_ask: Optional[float] = None
    current_no_ask: Optional[float] = None
    best_price_sum: Optional[float] = None
    best_edge: Optional[float] = None
    first_hit_ts_ms: Optional[int] = None
    opportunities: List[OpportunityRecord] = []

    for timestamp_ms, source_side, best_ask in events:
        if timestamp_ms < analysis_start_ms or timestamp_ms > analysis_end_ms:
            continue
        if source_side == "yes":
            current_yes_ask = best_ask
        else:
            current_no_ask = best_ask

        if current_yes_ask is None or current_no_ask is None:
            continue

        price_sum = round(current_yes_ask + current_no_ask, 6)
        edge = round(ARB_THRESHOLD - price_sum, 6)

        if best_price_sum is None or price_sum < best_price_sum:
            best_price_sum = price_sum
        if best_edge is None or edge > best_edge:
            best_edge = edge

        if price_sum < ARB_THRESHOLD:
            if first_hit_ts_ms is None:
                first_hit_ts_ms = timestamp_ms
            opportunities.append(
                OpportunityRecord(
                    opportunity_key=f"{condition_id}:{timestamp_ms}",
                    condition_id=condition_id,
                    market_slug=market_slug,
                    title=title,
                    trigger_ts_ms=timestamp_ms,
                    yes_ask=current_yes_ask,
                    no_ask=current_no_ask,
                    price_sum=price_sum,
                    edge=edge,
                    source_side=source_side,
                )
            )

    sample = MarketSample(
        condition_id=condition_id,
        market_slug=market_slug,
        title=title,
        start_time=start_time,
        end_time=end_time,
        analysis_start_ms=analysis_start_ms,
        analysis_end_ms=analysis_end_ms,
        hit=bool(opportunities),
        hit_count=len(opportunities),
        first_hit_ts_ms=first_hit_ts_ms,
        best_price_sum=best_price_sum,
        best_edge=best_edge,
        reason=None if opportunities else "no_direct_ask_arb_observed",
    )
    return (sample, opportunities)
