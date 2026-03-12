import unittest

from src.research.btc_5m_arrival.logic import (
    analyze_market_arrival,
    build_thresholds,
    market_is_btc_five_minute,
)


class BTC5MArrivalLogicTest(unittest.TestCase):
    def test_market_filter_matches_btc_yes_no_short_duration(self) -> None:
        market = {
            "title": "Will Bitcoin be above $90,000 in 5 minutes?",
            "market_slug": "bitcoin-5m-test",
            "event_slug": "bitcoin-price",
            "start_time": 100,
            "end_time": 400,
            "side_a": {"id": "token_yes", "label": "Yes"},
            "side_b": {"id": "token_no", "label": "No"},
        }

        self.assertTrue(market_is_btc_five_minute(market))

    def test_market_filter_rejects_long_duration_bitcoin_market(self) -> None:
        market = {
            "title": "Will Bitcoin hit $100,000 this month?",
            "market_slug": "bitcoin-monthly-test",
            "event_slug": "bitcoin-price",
            "start_time": 100,
            "end_time": 100 + 86400,
            "side_a": {"id": "token_yes", "label": "Yes"},
            "side_b": {"id": "token_no", "label": "No"},
        }

        self.assertFalse(market_is_btc_five_minute(market))

    def test_arrival_counts_threshold_hits_for_both_sides(self) -> None:
        market = {
            "condition_id": "condition-1",
            "market_slug": "bitcoin-5m-test",
            "event_slug": "bitcoin-price",
            "title": "Will Bitcoin be above $90,000 in 5 minutes?",
            "start_time": 100,
            "end_time": 400,
            "side_a": {"id": "token_yes", "label": "Yes"},
            "side_b": {"id": "token_no", "label": "No"},
        }
        candle_payloads = [
            {
                "token_id": "token_yes",
                "candles": [
                    {"price": {"high_dollars": "0.5810"}},
                ],
            },
            {
                "token_id": "token_no",
                "candles": [
                    {"price": {"high_dollars": "0.5490"}},
                ],
            },
        ]

        result = analyze_market_arrival(market, candle_payloads, build_thresholds(0.52, 0.58, 0.01))

        assert result is not None
        self.assertEqual(result.yes_hit_levels, ["0.52", "0.53", "0.54", "0.55", "0.56", "0.57", "0.58"])
        self.assertEqual(result.no_hit_levels, ["0.52", "0.53", "0.54"])

    def test_arrival_returns_none_when_no_threshold_reached(self) -> None:
        market = {
            "condition_id": "condition-2",
            "market_slug": "bitcoin-5m-test-2",
            "event_slug": "bitcoin-price",
            "title": "Will Bitcoin be above $91,000 in 5 minutes?",
            "start_time": 100,
            "end_time": 400,
            "side_a": {"id": "token_yes", "label": "Yes"},
            "side_b": {"id": "token_no", "label": "No"},
        }
        candle_payloads = [
            {
                "token_id": "token_yes",
                "candles": [
                    {"price": {"high_dollars": "0.5100"}},
                ],
            },
            {
                "token_id": "token_no",
                "candles": [
                    {"price": {"high_dollars": "0.5000"}},
                ],
            },
        ]

        result = analyze_market_arrival(market, candle_payloads, build_thresholds(0.52, 0.58, 0.01))

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
