import unittest

from src.research.btc_5m_arrival.logic import (
    analyze_market_arrival,
    build_thresholds,
    market_is_btc_five_minute,
)


class BTC5MArrivalLogicTest(unittest.TestCase):
    def test_market_filter_matches_btc_updown_short_duration(self) -> None:
        market = {
            "title": "Bitcoin Up or Down - November 7, 7:00AM-7:05AM ET",
            "market_slug": "btc-updown-5m-test",
            "event_slug": "bitcoin-price",
            "start_time": 100,
            "end_time": 400,
            "tags": ["Bitcoin", "Recurring", "Up or Down", "5M"],
            "side_a": {"id": "token_up", "label": "Up"},
            "side_b": {"id": "token_down", "label": "Down"},
        }

        self.assertTrue(market_is_btc_five_minute(market))

    def test_market_filter_rejects_long_duration_bitcoin_market(self) -> None:
        market = {
            "title": "Will Bitcoin hit $100,000 this month?",
            "market_slug": "bitcoin-monthly-test",
            "event_slug": "bitcoin-price",
            "start_time": 100,
            "end_time": 100 + 86400,
            "side_a": {"id": "token_up", "label": "Up"},
            "side_b": {"id": "token_down", "label": "Down"},
        }

        self.assertFalse(market_is_btc_five_minute(market))

    def test_market_filter_ignores_long_market_lifetime_for_real_5m_slug(self) -> None:
        market = {
            "title": "Bitcoin Up or Down - February 12, 4:40PM-4:45PM ET",
            "market_slug": "btc-updown-5m-1770932400",
            "event_slug": "btc-updown-5m-1770932400",
            "start_time": 1770866035,
            "end_time": 1770932700,
            "tags": ["crypto prices", "up or down", "recurring", "crypto", "bitcoin", "5m"],
            "side_a": {"id": "token_up", "label": "Up"},
            "side_b": {"id": "token_down", "label": "Down"},
        }

        self.assertTrue(market_is_btc_five_minute(market))

    def test_arrival_counts_threshold_hits_for_both_sides(self) -> None:
        market = {
            "condition_id": "condition-1",
            "market_slug": "btc-updown-5m-test",
            "event_slug": "bitcoin-price",
            "title": "Bitcoin Up or Down - November 7, 7:00AM-7:05AM ET",
            "start_time": 100,
            "end_time": 400,
            "side_a": {"id": "token_up", "label": "Up"},
            "side_b": {"id": "token_down", "label": "Down"},
        }
        candle_payloads = [
            {
                "token_id": "token_up",
                "candles": [
                    {"price": {"high_dollars": "0.5810"}},
                ],
            },
            {
                "token_id": "token_down",
                "candles": [
                    {"price": {"high_dollars": "0.5490"}},
                ],
            },
        ]

        result = analyze_market_arrival(market, candle_payloads, build_thresholds(0.52, 0.58, 0.01))

        assert result is not None
        self.assertEqual(result.outcome_a_label, "Up")
        self.assertEqual(result.outcome_b_label, "Down")
        self.assertEqual(
            result.outcome_a_hit_levels,
            ["0.52", "0.53", "0.54", "0.55", "0.56", "0.57", "0.58"],
        )
        self.assertEqual(result.outcome_b_hit_levels, ["0.52", "0.53", "0.54"])

    def test_arrival_returns_empty_hits_when_no_threshold_reached(self) -> None:
        market = {
            "condition_id": "condition-2",
            "market_slug": "btc-updown-5m-test-2",
            "event_slug": "bitcoin-price",
            "title": "Bitcoin Up or Down - November 7, 7:05AM-7:10AM ET",
            "start_time": 100,
            "end_time": 400,
            "side_a": {"id": "token_up", "label": "Up"},
            "side_b": {"id": "token_down", "label": "Down"},
        }
        candle_payloads = [
            {
                "token_id": "token_up",
                "candles": [
                    {"price": {"high_dollars": "0.5100"}},
                ],
            },
            {
                "token_id": "token_down",
                "candles": [
                    {"price": {"high_dollars": "0.5000"}},
                ],
            },
        ]

        result = analyze_market_arrival(market, candle_payloads, build_thresholds(0.52, 0.58, 0.01))

        assert result is not None
        self.assertEqual(result.outcome_a_hit_levels, [])
        self.assertEqual(result.outcome_b_hit_levels, [])


if __name__ == "__main__":
    unittest.main()
