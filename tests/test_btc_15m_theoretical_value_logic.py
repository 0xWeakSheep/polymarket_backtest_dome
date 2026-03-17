import unittest

from src.research.btc_15m_theoretical_value.logic import (
    classify_probability_bucket,
    collapse_trade_mirrors,
    extract_binary_tokens,
    logit,
    market_is_btc_fifteen_minute,
    martingale_constrained_drift,
    normalize_trade_price_to_probability,
    sigmoid_prime,
    sigmoid_second,
    theory_variance_forecast,
)


class BTC15MTheoreticalValueLogicTest(unittest.TestCase):
    def test_market_filter_matches_btc_15m(self) -> None:
        market = {
            "title": "Bitcoin Up or Down - November 7, 7:00AM-7:15AM ET",
            "market_slug": "btc-updown-15m-1770931800",
            "event_slug": "btc-updown-15m-1770931800",
            "side_a": {"id": "up", "label": "Up"},
            "side_b": {"id": "down", "label": "Down"},
        }
        self.assertTrue(market_is_btc_fifteen_minute(market))

    def test_extract_binary_tokens(self) -> None:
        market = {
            "side_a": {"id": "up", "label": "Up"},
            "side_b": {"id": "down", "label": "Down"},
        }
        tokens = extract_binary_tokens(market)
        self.assertIsNotNone(tokens)
        assert tokens is not None
        self.assertEqual(tokens["outcome_a_token_id"], "up")
        self.assertEqual(tokens["outcome_b_label"], "Down")

    def test_logit_is_zero_at_half(self) -> None:
        self.assertAlmostEqual(logit(0.5), 0.0)

    def test_sigmoid_derivatives(self) -> None:
        self.assertAlmostEqual(sigmoid_prime(0.0), 0.25)
        self.assertAlmostEqual(sigmoid_second(0.0), 0.0)

    def test_martingale_drift_returns_finite_value(self) -> None:
        drift = martingale_constrained_drift(logit(0.8), 0.0002, [0.2, -0.1], 0.01)
        self.assertTrue(abs(drift) < 1)

    def test_trade_probability_normalization(self) -> None:
        self.assertAlmostEqual(
            normalize_trade_price_to_probability("Up", 0.63, up_label="Up", down_label="Down") or 0.0,
            0.63,
        )
        self.assertAlmostEqual(
            normalize_trade_price_to_probability("Down", 0.37, up_label="Up", down_label="Down") or 0.0,
            0.63,
        )

    def test_collapse_trade_mirrors_removes_dual_sides(self) -> None:
        rows = [
            {
                "condition_id": "c1",
                "token_id": "t1",
                "timestamp": 100,
                "block_number": 1,
                "log_index": 10,
                "tx_hash": "0xabc",
                "price": 0.55,
                "shares_normalized": 10,
                "side": "BUY",
                "order_hash": "1",
            },
            {
                "condition_id": "c1",
                "token_id": "t1",
                "timestamp": 100,
                "block_number": 1,
                "log_index": 11,
                "tx_hash": "0xabc",
                "price": 0.55,
                "shares_normalized": 10,
                "side": "SELL",
                "order_hash": "2",
            },
        ]
        self.assertEqual(len(collapse_trade_mirrors(rows)), 1)

    def test_theory_variance_forecast_contains_jump_component(self) -> None:
        forecast = theory_variance_forecast(
            probability=0.55,
            x_returns=[0.01, -0.02, 0.015, 0.30, -0.01, 0.02],
            dt_seconds=[5, 8, 7, 10, 6, 9],
            horizon_seconds=300,
            jump_threshold=2.0,
        )
        self.assertGreaterEqual(forecast["predicted_total_variance_x"], 0.0)
        self.assertGreaterEqual(forecast["predicted_jump_share_x"], 0.0)
        self.assertGreaterEqual(forecast["predicted_total_variance_p"], 0.0)
        self.assertGreaterEqual(forecast["predicted_first_passage_up_90"], 0.0)

    def test_probability_bucket(self) -> None:
        self.assertEqual(classify_probability_bucket(0.05), "boundary")
        self.assertEqual(classify_probability_bucket(0.5), "core")
        self.assertEqual(classify_probability_bucket(0.2), "middle")


if __name__ == "__main__":
    unittest.main()
