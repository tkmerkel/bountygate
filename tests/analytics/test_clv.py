"""
Unit tests for bountygate.analytics.clv
"""

import math
import pytest

from bountygate.analytics import clv


# ---------------------------------------------------------------------------
# clv_from_fair
# ---------------------------------------------------------------------------

class TestClvFromFair:
    def test_market_moved_in_favour_positive(self):
        # bet fair prob = 0.50, closing fair prob = 0.55 (market moved toward bettor)
        # clv = 0.55/0.50 - 1 = 0.10  → positive = good
        val = clv.clv_from_fair(0.50, 0.55)
        assert math.isclose(val, 0.10, abs_tol=1e-9)

    def test_market_moved_against_negative(self):
        # bet fair prob = 0.55, closing fair prob = 0.50 (market moved away)
        # clv = 0.50/0.55 - 1 ≈ -0.0909 → negative = bad
        val = clv.clv_from_fair(0.55, 0.50)
        assert val < 0

    def test_no_movement_zero(self):
        val = clv.clv_from_fair(0.52, 0.52)
        assert math.isclose(val, 0.0, abs_tol=1e-9)

    def test_formula_is_closing_over_bet_minus_one(self):
        bet_fp = 0.48
        close_fp = 0.51
        expected = close_fp / bet_fp - 1.0
        assert math.isclose(clv.clv_from_fair(bet_fp, close_fp), expected, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# clv_from_price
# ---------------------------------------------------------------------------

class TestClvFromPrice:
    def test_higher_price_at_bet_time_positive(self):
        # Got 2.10, closed at 1.95 → beat_close → positive CLV
        val = clv.clv_from_price(2.10, 1.95)
        assert val > 0

    def test_lower_price_at_bet_time_negative(self):
        # Got 1.80, closed at 2.00 → missed the closing move → negative
        val = clv.clv_from_price(1.80, 2.00)
        assert val < 0

    def test_same_price_zero(self):
        val = clv.clv_from_price(1.91, 1.91)
        assert math.isclose(val, 0.0, abs_tol=1e-9)

    def test_known_value(self):
        # bet=2.0, close=2.0 → 0; bet=2.1, close=2.0 → 0.05
        val = clv.clv_from_price(2.1, 2.0)
        assert math.isclose(val, 0.05, abs_tol=1e-9)

    def test_formula_is_bet_over_closing_minus_one(self):
        bet_d = 2.05
        close_d = 1.95
        expected = bet_d / close_d - 1.0
        assert math.isclose(clv.clv_from_price(bet_d, close_d), expected, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# beat_close
# ---------------------------------------------------------------------------

class TestBeatClose:
    def test_closing_higher_prob_true(self):
        # Market assigned higher prob at close → bettor beat the close
        assert clv.beat_close(0.50, 0.55) is True

    def test_closing_lower_prob_false(self):
        # Market moved the other way
        assert clv.beat_close(0.55, 0.50) is False

    def test_equal_prob_false(self):
        # Exactly equal: did NOT beat (strict >)
        assert clv.beat_close(0.52, 0.52) is False

    def test_large_move_true(self):
        assert clv.beat_close(0.40, 0.70) is True

    def test_small_move_true(self):
        assert clv.beat_close(0.499, 0.501) is True

    def test_sign_consistency_with_clv_from_fair(self):
        # When beat_close is True, clv_from_fair should be positive
        bet_fp, close_fp = 0.48, 0.52
        assert clv.beat_close(bet_fp, close_fp) is True
        assert clv.clv_from_fair(bet_fp, close_fp) > 0

        # When beat_close is False, clv_from_fair should be negative or zero
        bet_fp2, close_fp2 = 0.52, 0.48
        assert clv.beat_close(bet_fp2, close_fp2) is False
        assert clv.clv_from_fair(bet_fp2, close_fp2) < 0


# ---------------------------------------------------------------------------
# compute_clv (DataFrame wrapper) — minimal smoke test
# ---------------------------------------------------------------------------

pd = pytest.importorskip("pandas", reason="pandas not installed")


class TestComputeClv:
    def _make_frames(self):
        tracked = pd.DataFrame([{
            "bg_event_id": "aaa",
            "market_key": "totals",
            "player_name": "none",
            "side": "over",
            "line": 7.5,
            "bet_price_decimal": 2.10,
            "bet_fair_prob": 0.50,
        }])
        closing = pd.DataFrame([{
            "bg_event_id": "aaa",
            "market_key": "totals",
            "player_name": "none",
            "side": "over",
            "line": 7.5,
            "closing_price_decimal": 1.95,
            "closing_fair_prob": 0.54,
        }])
        return tracked, closing

    def test_returns_one_row(self):
        tracked, closing = self._make_frames()
        result = clv.compute_clv(tracked, closing)
        assert len(result) == 1

    def test_clv_pct_is_positive_beat_close(self):
        tracked, closing = self._make_frames()
        result = clv.compute_clv(tracked, closing)
        # closing_fair_prob (0.54) > bet_fair_prob (0.50) → beat close → positive clv_pct
        assert result.iloc[0]["clv_pct"] > 0

    def test_beat_close_column_is_true(self):
        tracked, closing = self._make_frames()
        result = clv.compute_clv(tracked, closing)
        assert bool(result.iloc[0]["beat_close"]) is True

    def test_clv_hash_column_present(self):
        tracked, closing = self._make_frames()
        result = clv.compute_clv(tracked, closing)
        assert "clv_hash" in result.columns
        assert isinstance(result.iloc[0]["clv_hash"], str)

    def test_no_join_match_empty(self):
        tracked = pd.DataFrame([{
            "bg_event_id": "aaa",
            "market_key": "totals",
            "player_name": "none",
            "side": "over",
            "line": 7.5,
            "bet_price_decimal": 2.10,
            "bet_fair_prob": 0.50,
        }])
        closing = pd.DataFrame([{
            "bg_event_id": "bbb",  # different event → no match
            "market_key": "totals",
            "player_name": "none",
            "side": "over",
            "line": 7.5,
            "closing_price_decimal": 1.95,
            "closing_fair_prob": 0.54,
        }])
        result = clv.compute_clv(tracked, closing)
        assert len(result) == 0
