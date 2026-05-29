"""
Unit tests for bountygate.analytics.kelly
"""

import math
import pytest

from bountygate.analytics import kelly


# ---------------------------------------------------------------------------
# kelly_fraction
# ---------------------------------------------------------------------------

class TestKellyFraction:
    def test_known_value(self):
        # kelly_fraction(0.55, 2.0) = (0.55*2 - 1) / (2 - 1) = 0.10 / 1 = 0.10
        assert math.isclose(kelly.kelly_fraction(0.55, 2.0), 0.10, abs_tol=1e-9)

    def test_even_odds_fair_prob_zero_fraction(self):
        # Fair price → zero edge → zero Kelly
        assert math.isclose(kelly.kelly_fraction(0.5, 2.0), 0.0, abs_tol=1e-9)

    def test_negative_edge_clamps_to_zero(self):
        # Negative-EV bet should return 0, not negative
        result = kelly.kelly_fraction(0.40, 2.0)
        assert result == 0.0

    def test_negative_edge_explicit(self):
        # 0.45 * 2.0 - 1 = -0.10  →  clamp to 0
        assert kelly.kelly_fraction(0.45, 2.0) == 0.0

    def test_high_prob_large_fraction(self):
        # 0.75 * 1.5 - 1 = 0.125; denom = 0.5 → fraction = 0.25
        assert math.isclose(kelly.kelly_fraction(0.75, 1.5), 0.25, abs_tol=1e-9)

    def test_denominator_zero_or_negative_returns_zero(self):
        # decimal_odds = 1.0 → denominator = 0 → should not crash
        result = kelly.kelly_fraction(0.9, 1.0)
        assert result == 0.0

    def test_result_non_negative(self):
        for p, d in [(0.3, 1.5), (0.01, 2.0), (0.5, 1.01)]:
            assert kelly.kelly_fraction(p, d) >= 0.0


# ---------------------------------------------------------------------------
# quarter_kelly
# ---------------------------------------------------------------------------

class TestQuarterKelly:
    def test_is_quarter_of_full(self):
        for p, d in [(0.55, 2.0), (0.60, 1.8), (0.70, 1.5)]:
            full = kelly.kelly_fraction(p, d)
            quarter = kelly.quarter_kelly(p, d)
            assert math.isclose(quarter, 0.25 * full, abs_tol=1e-12)

    def test_zero_edge_gives_zero(self):
        assert math.isclose(kelly.quarter_kelly(0.5, 2.0), 0.0, abs_tol=1e-12)


# ---------------------------------------------------------------------------
# capped_stake
# ---------------------------------------------------------------------------

class TestCappedStake:
    def test_small_kelly_below_cap(self):
        # fraction = 0.01, cap = 0.02, bankroll = 1000 → stake = 10
        stake = kelly.capped_stake(0.01, 1000.0, cap_frac=0.02)
        assert math.isclose(stake, 10.0, abs_tol=1e-9)

    def test_large_kelly_capped(self):
        # fraction = 0.10, cap = 0.02, bankroll = 1000 → stake = 20 (capped)
        stake = kelly.capped_stake(0.10, 1000.0, cap_frac=0.02)
        assert math.isclose(stake, 20.0, abs_tol=1e-9)

    def test_exactly_at_cap(self):
        stake = kelly.capped_stake(0.02, 1000.0, cap_frac=0.02)
        assert math.isclose(stake, 20.0, abs_tol=1e-9)

    def test_negative_fraction_clamps_to_zero(self):
        stake = kelly.capped_stake(-0.05, 1000.0)
        assert stake == 0.0

    def test_zero_bankroll(self):
        stake = kelly.capped_stake(0.10, 0.0)
        assert stake == 0.0

    def test_default_cap_is_two_percent(self):
        # Default cap_frac=0.02: big fraction → stake = 2% of bankroll
        stake = kelly.capped_stake(0.50, 500.0)
        assert math.isclose(stake, 500.0 * 0.02, abs_tol=1e-9)

    def test_respects_custom_cap(self):
        stake = kelly.capped_stake(0.10, 1000.0, cap_frac=0.05)
        assert math.isclose(stake, 50.0, abs_tol=1e-9)
