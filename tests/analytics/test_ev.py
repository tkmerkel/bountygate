"""
Unit tests for bountygate.analytics.ev
"""

import math
import pytest

from bountygate.analytics import ev


# ---------------------------------------------------------------------------
# edge
# ---------------------------------------------------------------------------

class TestEdge:
    def test_known_value(self):
        # edge(0.55, 2.0) = 0.55 * 2.0 - 1 = 0.10
        assert math.isclose(ev.edge(0.55, 2.0), 0.10, abs_tol=1e-9)

    def test_even_money_fair_price_zero_edge(self):
        # fair_prob 0.5 at decimal 2.0 → edge = 0
        assert math.isclose(ev.edge(0.5, 2.0), 0.0, abs_tol=1e-9)

    def test_negative_edge(self):
        # Priced worse than fair: fair_prob=0.5, decimal=1.8 → 0.5*1.8-1=-0.1
        assert math.isclose(ev.edge(0.5, 1.8), -0.1, abs_tol=1e-9)

    def test_positive_edge(self):
        assert ev.edge(0.60, 2.0) > 0

    def test_zero_prob_returns_negative_one(self):
        assert math.isclose(ev.edge(0.0, 2.0), -1.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# edge_pct
# ---------------------------------------------------------------------------

class TestEdgePct:
    def test_matches_edge_times_100(self):
        for fp, d in [(0.55, 2.0), (0.5, 1.9), (0.60, 2.1)]:
            assert math.isclose(ev.edge_pct(fp, d), ev.edge(fp, d) * 100, abs_tol=1e-9)

    def test_known_value(self):
        # 10% edge
        assert math.isclose(ev.edge_pct(0.55, 2.0), 10.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# no_vig_price
# ---------------------------------------------------------------------------

class TestNoVigPrice:
    def test_half_prob_gives_two(self):
        assert math.isclose(ev.no_vig_price(0.5), 2.0, abs_tol=1e-9)

    def test_one_third_prob_gives_three(self):
        assert math.isclose(ev.no_vig_price(1.0 / 3.0), 3.0, abs_tol=1e-6)

    def test_roundtrip_with_implied_prob(self):
        # no_vig_price(1/d) should return d
        from bountygate.analytics.devig import implied_prob
        d = 1.8
        p = implied_prob(d)
        assert math.isclose(ev.no_vig_price(p), d, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# is_actionable
# ---------------------------------------------------------------------------

class TestIsActionable:
    def test_above_threshold_true(self):
        assert ev.is_actionable(0.03) is True

    def test_exactly_threshold_true(self):
        # >= threshold: 0.025 should be actionable
        assert ev.is_actionable(0.025) is True

    def test_just_below_threshold_false(self):
        assert ev.is_actionable(0.0249) is False

    def test_negative_edge_false(self):
        assert ev.is_actionable(-0.05) is False

    def test_zero_edge_false(self):
        assert ev.is_actionable(0.0) is False

    def test_custom_threshold(self):
        assert ev.is_actionable(0.04, threshold=0.05) is False
        assert ev.is_actionable(0.06, threshold=0.05) is True

    def test_large_edge_true(self):
        assert ev.is_actionable(0.20) is True
