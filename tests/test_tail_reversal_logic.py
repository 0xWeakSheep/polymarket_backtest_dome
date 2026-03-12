import unittest

from src.research.tail_reversal.logic import analyze_market


class TailReversalLogicTest(unittest.TestCase):
    def test_losing_side_touching_threshold_counts_as_reversal(self) -> None:
        market = {
            "condition_id": "condition-1",
            "market_slug": "market-1",
            "title": "Example market",
            "winning_side": "No",
            "side_a": {"id": "token_yes", "label": "Yes"},
            "side_b": {"id": "token_no", "label": "No"},
        }
        orders = [
            {"token_id": "token_yes", "price": 0.96, "timestamp": 100},
            {"token_id": "token_no", "price": 0.07, "timestamp": 110},
        ]

        result = analyze_market(market, orders, threshold=0.95)

        assert result is not None
        self.assertTrue(result.any_side_touched_threshold)
        self.assertTrue(result.losing_side_touched_threshold)
        self.assertEqual(result.losing_side, "Yes")
        self.assertEqual(result.trade_count, 2)

    def test_winning_side_touching_threshold_is_not_reversal(self) -> None:
        market = {
            "condition_id": "condition-2",
            "market_slug": "market-2",
            "title": "Example market 2",
            "winning_side": "Yes",
            "side_a": {"id": "token_yes", "label": "Yes"},
            "side_b": {"id": "token_no", "label": "No"},
        }
        orders = [
            {"token_id": "token_yes", "price": 0.98, "timestamp": 100},
            {"token_id": "token_no", "price": 0.02, "timestamp": 101},
        ]

        result = analyze_market(market, orders, threshold=0.95)

        assert result is not None
        self.assertTrue(result.any_side_touched_threshold)
        self.assertFalse(result.losing_side_touched_threshold)
        self.assertEqual(result.losing_side, "No")


if __name__ == "__main__":
    unittest.main()
