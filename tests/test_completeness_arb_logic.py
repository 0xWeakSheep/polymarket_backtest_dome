import unittest

from src.research.completeness_arb.logic import (
    analyze_path_misalignment,
    derive_child_five_minute_slugs,
    derive_path_pattern,
    market_is_btc_updown,
    resolve_market_outcome,
)


class CompletenessArbLogicTest(unittest.TestCase):
    def test_market_filter_matches_btc_15m_market(self) -> None:
        market = {
            "title": "Bitcoin Up or Down - November 7, 7:00AM-7:15AM ET",
            "market_slug": "btc-updown-15m-1770931800",
            "event_slug": "btc-updown-15m-1770931800",
            "side_a": {"id": "up", "label": "Up"},
            "side_b": {"id": "down", "label": "Down"},
        }
        self.assertTrue(market_is_btc_updown(market, minutes=15))
        self.assertFalse(market_is_btc_updown(market, minutes=5))

    def test_resolve_market_outcome_from_winning_side(self) -> None:
        market = {
            "side_a": {"id": "up", "label": "Up"},
            "side_b": {"id": "down", "label": "Down"},
            "winning_side": {"id": "up", "label": "Up"},
        }
        self.assertEqual(resolve_market_outcome(market), "Up")

    def test_derive_path_pattern(self) -> None:
        self.assertEqual(derive_path_pattern("Up", "Up"), ("up_up", "15m_up_plus_last5m_down"))
        self.assertEqual(derive_path_pattern("Down", "Down"), ("down_down", "15m_down_plus_last5m_up"))
        self.assertEqual(derive_path_pattern("Up", "Down"), (None, None))

    def test_child_five_minute_slugs(self) -> None:
        self.assertEqual(
            derive_child_five_minute_slugs(1770931800, five_slug_prefix="btc-updown-5m"),
            (
                "btc-updown-5m-1770931800",
                "btc-updown-5m-1770932100",
                "btc-updown-5m-1770932400",
            ),
        )

    def test_analyze_path_misalignment_hits_for_up_up_path(self) -> None:
        fifteen_market = {
            "condition_id": "f15",
            "market_slug": "btc-updown-15m-1770931800",
            "title": "15m market",
            "start_time": 1770931800,
            "end_time": 1770932700,
            "side_a": {"id": "up15", "label": "Up"},
            "side_b": {"id": "down15", "label": "Down"},
        }
        third_market = {
            "condition_id": "f5-3",
            "market_slug": "btc-updown-5m-1770932400",
            "title": "last 5m market",
            "start_time": 1770932400,
            "end_time": 1770932700,
            "side_a": {"id": "up5", "label": "Up"},
            "side_b": {"id": "down5", "label": "Down"},
        }
        fifteen_candles = [
            {"token_id": "up15", "candles": [{"end_period_ts": 1770932460, "price": {"close_dollars": "0.41"}}]},
            {"token_id": "down15", "candles": [{"end_period_ts": 1770932460, "price": {"close_dollars": "0.59"}}]},
        ]
        third_candles = [
            {"token_id": "up5", "candles": [{"end_period_ts": 1770932460, "price": {"close_dollars": "0.70"}}]},
            {"token_id": "down5", "candles": [{"end_period_ts": 1770932460, "price": {"close_dollars": "0.48"}}]},
        ]

        sample, opportunities = analyze_path_misalignment(
            fifteen_market=fifteen_market,
            third_five_market=third_market,
            path_pattern="up_up",
            fifteen_candle_payloads=fifteen_candles,
            third_five_candle_payloads=third_candles,
        )

        assert sample is not None
        self.assertTrue(sample.hit)
        self.assertEqual(sample.first_hit_ts, 1770932460)
        self.assertEqual(sample.best_price_sum, 0.89)
        self.assertEqual(sample.best_edge, 0.11)
        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0].fifteen_side, "Up")
        self.assertEqual(opportunities[0].last5m_side, "Down")

    def test_analyze_path_misalignment_records_no_hit(self) -> None:
        fifteen_market = {
            "condition_id": "f15",
            "market_slug": "btc-updown-15m-1770931800",
            "title": "15m market",
            "start_time": 1770931800,
            "end_time": 1770932700,
            "side_a": {"id": "up15", "label": "Up"},
            "side_b": {"id": "down15", "label": "Down"},
        }
        third_market = {
            "condition_id": "f5-3",
            "market_slug": "btc-updown-5m-1770932400",
            "title": "last 5m market",
            "start_time": 1770932400,
            "end_time": 1770932700,
            "side_a": {"id": "up5", "label": "Up"},
            "side_b": {"id": "down5", "label": "Down"},
        }
        fifteen_candles = [
            {"token_id": "down15", "candles": [{"end_period_ts": 1770932460, "price": {"close_dollars": "0.61"}}]},
            {"token_id": "up15", "candles": [{"end_period_ts": 1770932460, "price": {"close_dollars": "0.39"}}]},
        ]
        third_candles = [
            {"token_id": "up5", "candles": [{"end_period_ts": 1770932460, "price": {"close_dollars": "0.43"}}]},
            {"token_id": "down5", "candles": [{"end_period_ts": 1770932460, "price": {"close_dollars": "0.57"}}]},
        ]

        sample, opportunities = analyze_path_misalignment(
            fifteen_market=fifteen_market,
            third_five_market=third_market,
            path_pattern="down_down",
            fifteen_candle_payloads=fifteen_candles,
            third_five_candle_payloads=third_candles,
        )

        assert sample is not None
        self.assertFalse(sample.hit)
        self.assertEqual(sample.best_price_sum, 1.04)
        self.assertEqual(sample.best_edge, -0.04)
        self.assertEqual(sample.reason, "no_arb_observed")
        self.assertEqual(opportunities, [])


if __name__ == "__main__":
    unittest.main()
