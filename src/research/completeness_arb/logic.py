from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


FIVE_MINUTE_STEP_SECONDS = 300
FIFTEEN_MINUTE_STEP_SECONDS = 900
ONE_MINUTE_CANDLE_INTERVAL = 1
MAX_ONE_MINUTE_RANGE_SECONDS = 7 * 24 * 60 * 60
ARB_THRESHOLD = 1.0
SUPPORTED_BINARY_LABEL_SETS = ({"yes", "no"}, {"up", "down"})


@dataclass
class SampleRecord:
    fifteen_condition_id: str
    fifteen_market_slug: str
    fifteen_title: str
    fifteen_start_ts: Optional[int]
    fifteen_end_ts: Optional[int]
    five_1_slug: str
    five_2_slug: str
    five_3_slug: str
    five_1_outcome: Optional[str]
    five_2_outcome: Optional[str]
    path_pattern: Optional[str]
    eligible: bool
    direction_checked: Optional[str]
    hit: bool
    best_price_sum: Optional[float]
    best_edge: Optional[float]
    first_hit_ts: Optional[int]
    reason: Optional[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "fifteen_condition_id": self.fifteen_condition_id,
            "fifteen_market_slug": self.fifteen_market_slug,
            "fifteen_title": self.fifteen_title,
            "fifteen_start_ts": self.fifteen_start_ts,
            "fifteen_end_ts": self.fifteen_end_ts,
            "five_1_slug": self.five_1_slug,
            "five_2_slug": self.five_2_slug,
            "five_3_slug": self.five_3_slug,
            "five_1_outcome": self.five_1_outcome,
            "five_2_outcome": self.five_2_outcome,
            "path_pattern": self.path_pattern,
            "eligible": self.eligible,
            "direction_checked": self.direction_checked,
            "hit": self.hit,
            "best_price_sum": self.best_price_sum,
            "best_edge": self.best_edge,
            "first_hit_ts": self.first_hit_ts,
            "reason": self.reason,
        }


@dataclass
class OpportunityRecord:
    fifteen_condition_id: str
    fifteen_market_slug: str
    five_3_slug: str
    path_pattern: str
    trigger_ts: int
    fifteen_side: str
    last5m_side: str
    fifteen_price: float
    last5m_price: float
    price_sum: float
    edge: float

    def to_dict(self) -> Dict[str, object]:
        return {
            "fifteen_condition_id": self.fifteen_condition_id,
            "fifteen_market_slug": self.fifteen_market_slug,
            "five_3_slug": self.five_3_slug,
            "path_pattern": self.path_pattern,
            "trigger_ts": self.trigger_ts,
            "fifteen_side": self.fifteen_side,
            "last5m_side": self.last5m_side,
            "fifteen_price": self.fifteen_price,
            "last5m_price": self.last5m_price,
            "price_sum": self.price_sum,
            "edge": self.edge,
        }


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


def market_is_btc_updown(market: Dict[str, object], *, minutes: int) -> bool:
    title = str(market.get("title") or "").lower()
    market_slug = str(market.get("market_slug") or "").lower()
    event_slug = str(market.get("event_slug") or "").lower()
    combined = " ".join([title, market_slug, event_slug])

    if not market_slug.startswith(f"btc-updown-{minutes}m-"):
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

    outcome_a_label = str(side_a.get("label") or "").strip()
    outcome_b_label = str(side_b.get("label") or "").strip()
    outcome_a_token_id = str(side_a.get("id") or "").strip()
    outcome_b_token_id = str(side_b.get("id") or "").strip()

    if not outcome_a_token_id or not outcome_b_token_id:
        return None
    if {normalize_label(outcome_a_label), normalize_label(outcome_b_label)} not in SUPPORTED_BINARY_LABEL_SETS:
        return None

    return {
        "outcome_a_label": outcome_a_label,
        "outcome_b_label": outcome_b_label,
        "outcome_a_token_id": outcome_a_token_id,
        "outcome_b_token_id": outcome_b_token_id,
    }


def resolve_market_outcome(market: Dict[str, object]) -> Optional[str]:
    tokens = extract_binary_tokens(market)
    if tokens is None:
        return None

    winning_side_raw = market.get("winning_side") or {}
    winning_label = ""
    winning_token_id = ""
    if isinstance(winning_side_raw, dict):
        winning_label = str(winning_side_raw.get("label") or "").strip()
        winning_token_id = str(winning_side_raw.get("id") or "").strip()
    else:
        winning_label = str(winning_side_raw or "").strip()

    if normalize_label(winning_label) == normalize_label(tokens["outcome_a_label"]) or winning_token_id == tokens["outcome_a_token_id"]:
        return tokens["outcome_a_label"]
    if normalize_label(winning_label) == normalize_label(tokens["outcome_b_label"]) or winning_token_id == tokens["outcome_b_token_id"]:
        return tokens["outcome_b_label"]
    return None


def derive_path_pattern(first_outcome: Optional[str], second_outcome: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    first = normalize_label(first_outcome)
    second = normalize_label(second_outcome)
    if first == "up" and second == "up":
        return ("up_up", "15m_up_plus_last5m_down")
    if first == "down" and second == "down":
        return ("down_down", "15m_down_plus_last5m_up")
    return (None, None)


def derive_child_five_minute_slugs(fifteen_timestamp: int, *, five_slug_prefix: str) -> Tuple[str, str, str]:
    return (
        f"{five_slug_prefix}-{fifteen_timestamp}",
        f"{five_slug_prefix}-{fifteen_timestamp + FIVE_MINUTE_STEP_SECONDS}",
        f"{five_slug_prefix}-{fifteen_timestamp + 2 * FIVE_MINUTE_STEP_SECONDS}",
    )


def build_price_series(
    market: Dict[str, object],
    candle_payloads: Iterable[Dict[str, object]],
    *,
    price_field: str = "close_dollars",
) -> Optional[Dict[str, Dict[int, float]]]:
    tokens = extract_binary_tokens(market)
    if tokens is None:
        return None

    token_to_label = {
        tokens["outcome_a_token_id"]: tokens["outcome_a_label"],
        tokens["outcome_b_token_id"]: tokens["outcome_b_label"],
    }
    series: Dict[str, Dict[int, float]] = {
        tokens["outcome_a_label"]: {},
        tokens["outcome_b_label"]: {},
    }

    for candle_payload in candle_payloads:
        token_id = str(candle_payload.get("token_id") or "").strip()
        label = token_to_label.get(token_id)
        if label is None:
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
            raw_ts = candle.get("end_period_ts")
            try:
                end_ts = int(raw_ts)
                value = float(price.get(price_field) or 0.0)
            except (TypeError, ValueError):
                continue
            series[label][end_ts] = value

    return series


def analyze_path_misalignment(
    *,
    fifteen_market: Dict[str, object],
    third_five_market: Dict[str, object],
    path_pattern: str,
    fifteen_candle_payloads: Iterable[Dict[str, object]],
    third_five_candle_payloads: Iterable[Dict[str, object]],
) -> Tuple[Optional[SampleRecord], List[OpportunityRecord]]:
    fifteen_series = build_price_series(fifteen_market, fifteen_candle_payloads)
    third_series = build_price_series(third_five_market, third_five_candle_payloads)
    if fifteen_series is None or third_series is None:
        return (None, [])

    fifteen_slug = str(fifteen_market.get("market_slug") or "")
    third_slug = str(third_five_market.get("market_slug") or "")
    condition_id = str(fifteen_market.get("condition_id") or "")
    title = str(fifteen_market.get("title") or "")
    try:
        start_ts = int(fifteen_market.get("start_time")) if fifteen_market.get("start_time") is not None else None
        end_ts = int(fifteen_market.get("end_time")) if fifteen_market.get("end_time") is not None else None
    except (TypeError, ValueError):
        start_ts = None
        end_ts = None

    third_start_raw = third_five_market.get("start_time")
    third_end_raw = third_five_market.get("end_time")
    try:
        third_start = int(third_start_raw)
        third_end = int(third_end_raw)
    except (TypeError, ValueError):
        third_start = None
        third_end = None

    if third_start is None or third_end is None:
        sample = SampleRecord(
            fifteen_condition_id=condition_id,
            fifteen_market_slug=fifteen_slug,
            fifteen_title=title,
            fifteen_start_ts=start_ts,
            fifteen_end_ts=end_ts,
            five_1_slug="",
            five_2_slug="",
            five_3_slug=third_slug,
            five_1_outcome=None,
            five_2_outcome=None,
            path_pattern=path_pattern,
            eligible=True,
            direction_checked="15m_up_plus_last5m_down" if path_pattern == "up_up" else "15m_down_plus_last5m_up",
            hit=False,
            best_price_sum=None,
            best_edge=None,
            first_hit_ts=None,
            reason="invalid_third_window",
        )
        return (sample, [])

    if path_pattern == "up_up":
        fifteen_side = "Up"
        last5m_side = "Down"
        direction_checked = "15m_up_plus_last5m_down"
    else:
        fifteen_side = "Down"
        last5m_side = "Up"
        direction_checked = "15m_down_plus_last5m_up"

    fifteen_side_series = fifteen_series.get(fifteen_side, {})
    third_side_series = third_series.get(last5m_side, {})
    common_timestamps = sorted(
        ts
        for ts in set(fifteen_side_series.keys()) & set(third_side_series.keys())
        if third_start <= ts <= third_end
    )

    if not common_timestamps:
        sample = SampleRecord(
            fifteen_condition_id=condition_id,
            fifteen_market_slug=fifteen_slug,
            fifteen_title=title,
            fifteen_start_ts=start_ts,
            fifteen_end_ts=end_ts,
            five_1_slug="",
            five_2_slug="",
            five_3_slug=third_slug,
            five_1_outcome=None,
            five_2_outcome=None,
            path_pattern=path_pattern,
            eligible=True,
            direction_checked=direction_checked,
            hit=False,
            best_price_sum=None,
            best_edge=None,
            first_hit_ts=None,
            reason="missing_overlap_prices",
        )
        return (sample, [])

    opportunities: List[OpportunityRecord] = []
    best_price_sum: Optional[float] = None
    best_edge: Optional[float] = None
    first_hit_ts: Optional[int] = None
    best_opportunity: Optional[OpportunityRecord] = None

    for ts in common_timestamps:
        fifteen_price = fifteen_side_series[ts]
        last5m_price = third_side_series[ts]
        price_sum = round(fifteen_price + last5m_price, 6)
        edge = round(ARB_THRESHOLD - price_sum, 6)

        if best_price_sum is None or price_sum < best_price_sum:
            best_price_sum = price_sum
        if best_edge is None or edge > best_edge:
            best_edge = edge

        if price_sum < ARB_THRESHOLD:
            if first_hit_ts is None:
                first_hit_ts = ts
            candidate = OpportunityRecord(
                fifteen_condition_id=condition_id,
                fifteen_market_slug=fifteen_slug,
                five_3_slug=third_slug,
                path_pattern=path_pattern,
                trigger_ts=ts,
                fifteen_side=fifteen_side,
                last5m_side=last5m_side,
                fifteen_price=fifteen_price,
                last5m_price=last5m_price,
                price_sum=price_sum,
                edge=edge,
            )
            if (
                best_opportunity is None
                or candidate.edge > best_opportunity.edge
                or (
                    candidate.edge == best_opportunity.edge
                    and candidate.trigger_ts < best_opportunity.trigger_ts
                )
            ):
                best_opportunity = candidate

    if best_opportunity is not None:
        opportunities.append(best_opportunity)

    sample = SampleRecord(
        fifteen_condition_id=condition_id,
        fifteen_market_slug=fifteen_slug,
        fifteen_title=title,
        fifteen_start_ts=start_ts,
        fifteen_end_ts=end_ts,
        five_1_slug="",
        five_2_slug="",
        five_3_slug=third_slug,
        five_1_outcome=None,
        five_2_outcome=None,
        path_pattern=path_pattern,
        eligible=True,
        direction_checked=direction_checked,
        hit=bool(opportunities),
        best_price_sum=best_price_sum,
        best_edge=best_edge,
        first_hit_ts=first_hit_ts,
        reason=None if opportunities else "no_arb_observed",
    )
    return (sample, opportunities)
