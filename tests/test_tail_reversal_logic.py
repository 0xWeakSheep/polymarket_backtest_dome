import unittest

from src.research.tail_reversal.logic import analyze_market_with_candles


class TailReversalLogicTest(unittest.TestCase):
    def test_losing_side_candle_high_counts_as_reversal(self) -> None:
        market = {
            "condition_id": "condition-1",
            "market_slug": "market-1",
            "title": "Example market",
            "winning_side": {"id": "token_no", "label": "No"},
            "side_a": {"id": "token_yes", "label": "Yes"},
            "side_b": {"id": "token_no", "label": "No"},
            "start_time": 10,
            "end_time": 100,
        }
        candle_payloads = [
            {
                "token_id": "token_yes",
                "candles": [
                    {"end_period_ts": 50, "price": {"high": 96, "high_dollars": "0.9600"}},
                ],
            },
            {
                "token_id": "token_no",
                "candles": [
                    {"end_period_ts": 50, "price": {"high": 12, "high_dollars": "0.1200"}},
                ],
            },
        ]

        result = analyze_market_with_candles(market, candle_payloads, threshold=0.95)

        assert result is not None
        self.assertEqual(result.losing_side, "Yes")
        self.assertEqual(result.losing_max_price, 0.96)
        self.assertEqual(result.trigger_timestamp, 50)

    def test_winning_side_touch_does_not_count_as_reversal(self) -> None:
        market = {
            "condition_id": "condition-2",
            "market_slug": "market-2",
            "title": "Example market 2",
            "winning_side": {"id": "token_yes", "label": "Yes"},
            "side_a": {"id": "token_yes", "label": "Yes"},
            "side_b": {"id": "token_no", "label": "No"},
            "start_time": 10,
            "end_time": 100,
        }
        candle_payloads = [
            {
                "token_id": "token_yes",
                "candles": [
                    {"end_period_ts": 50, "price": {"high": 98, "high_dollars": "0.9800"}},
                ],
            },
            {
                "token_id": "token_no",
                "candles": [
                    {"end_period_ts": 50, "price": {"high": 3, "high_dollars": "0.0300"}},
                ],
            },
        ]

        result = analyze_market_with_candles(market, candle_payloads, threshold=0.95)

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
