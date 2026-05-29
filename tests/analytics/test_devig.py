"""
Unit tests for bountygate.analytics.devig
"""

import math
import pytest

from bountygate.analytics import devig


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# -110 American  →  decimal 1.909090...  →  raw implied 0.52381...
# A -110/-110 book has overround = 2 * (1/1.909090) - 1 ≈ 0.04762
DECIMAL_110 = 10 / 5.25  # exact fraction for -110 American = 1.90476...
# Use the commonly-cited approximation: American -110 → decimal 1.9091
D110 = 1.9091
# For a perfectly symmetric book both sides are identical, so fair prob = 0.5

# A lopsided market:  Over at 1.10  (heavy favourite), Under at 7.0
D_FAV = 1.10
D_DOG = 7.0


# ---------------------------------------------------------------------------
# implied_prob
# ---------------------------------------------------------------------------

class TestImpliedProb:
    def test_even_money(self):
        assert math.isclose(devig.implied_prob(2.0), 0.5, rel_tol=1e-9)

    def test_110(self):
        # 1 / 1.9091 ≈ 0.52381
        p = devig.implied_prob(D110)
        assert 0.52 < p < 0.54

    def test_invalid_zero_raises(self):
        with pytest.raises((ValueError, ZeroDivisionError)):
            devig.implied_prob(0)

    def test_invalid_negative_raises(self):
        with pytest.raises(ValueError):
            devig.implied_prob(-1.5)


# ---------------------------------------------------------------------------
# multiplicative_devig
# ---------------------------------------------------------------------------

class TestMultiplicativeDevig:
    def test_symmetric_110_gives_half(self):
        p = devig.implied_prob(D110)
        fo, fu = devig.multiplicative_devig(p, p)
        assert math.isclose(fo, 0.5, abs_tol=1e-6)
        assert math.isclose(fu, 0.5, abs_tol=1e-6)

    def test_pair_sums_to_one(self):
        p_o = devig.implied_prob(D110)
        p_u = devig.implied_prob(D110)
        fo, fu = devig.multiplicative_devig(p_o, p_u)
        assert math.isclose(fo + fu, 1.0, abs_tol=1e-9)

    def test_lopsided_sum_to_one(self):
        po = devig.implied_prob(D_FAV)
        pu = devig.implied_prob(D_DOG)
        fo, fu = devig.multiplicative_devig(po, pu)
        assert math.isclose(fo + fu, 1.0, abs_tol=1e-9)

    def test_lopsided_fav_has_higher_prob(self):
        po = devig.implied_prob(D_FAV)
        pu = devig.implied_prob(D_DOG)
        fo, fu = devig.multiplicative_devig(po, pu)
        assert fo > fu


# ---------------------------------------------------------------------------
# power_devig
# ---------------------------------------------------------------------------

class TestPowerDevig:
    def test_symmetric_110_gives_half(self):
        p = devig.implied_prob(D110)
        fo, fu = devig.power_devig(p, p)
        assert math.isclose(fo, 0.5, abs_tol=1e-5)
        assert math.isclose(fu, 0.5, abs_tol=1e-5)

    def test_pair_sums_to_one(self):
        p = devig.implied_prob(D110)
        fo, fu = devig.power_devig(p, p)
        assert math.isclose(fo + fu, 1.0, abs_tol=1e-9)

    def test_lopsided_sum_to_one(self):
        po = devig.implied_prob(D_FAV)
        pu = devig.implied_prob(D_DOG)
        fo, fu = devig.power_devig(po, pu)
        assert math.isclose(fo + fu, 1.0, abs_tol=1e-9)

    def test_lopsided_fav_has_higher_prob(self):
        po = devig.implied_prob(D_FAV)
        pu = devig.implied_prob(D_DOG)
        fo, fu = devig.power_devig(po, pu)
        assert fo > fu


# ---------------------------------------------------------------------------
# shin_devig
# ---------------------------------------------------------------------------

class TestShinDevig:
    def test_symmetric_110_gives_half(self):
        p = devig.implied_prob(D110)
        fo, fu = devig.shin_devig(p, p)
        assert math.isclose(fo, 0.5, abs_tol=1e-5)
        assert math.isclose(fu, 0.5, abs_tol=1e-5)

    def test_pair_sums_to_one(self):
        p = devig.implied_prob(D110)
        fo, fu = devig.shin_devig(p, p)
        assert math.isclose(fo + fu, 1.0, abs_tol=1e-9)

    def test_lopsided_sum_to_one(self):
        po = devig.implied_prob(D_FAV)
        pu = devig.implied_prob(D_DOG)
        fo, fu = devig.shin_devig(po, pu)
        assert math.isclose(fo + fu, 1.0, abs_tol=1e-9)

    def test_lopsided_fav_has_higher_prob(self):
        po = devig.implied_prob(D_FAV)
        pu = devig.implied_prob(D_DOG)
        fo, fu = devig.shin_devig(po, pu)
        assert fo > fu


# ---------------------------------------------------------------------------
# devig_all
# ---------------------------------------------------------------------------

class TestDevigAll:
    def test_symmetric_110_all_methods_near_half(self):
        result = devig.devig_all(D110, D110)
        assert result is not None
        for key in ("fair_prob_over_mult", "fair_prob_over_power", "fair_prob_over_shin"):
            assert math.isclose(result[key], 0.5, abs_tol=1e-4), f"{key} = {result[key]}"
        for key in ("fair_prob_under_mult", "fair_prob_under_power", "fair_prob_under_shin"):
            assert math.isclose(result[key], 0.5, abs_tol=1e-4), f"{key} = {result[key]}"

    def test_symmetric_pairs_each_sum_to_one(self):
        result = devig.devig_all(D110, D110)
        assert result is not None
        assert math.isclose(result["fair_prob_over_mult"] + result["fair_prob_under_mult"], 1.0, abs_tol=1e-9)
        assert math.isclose(result["fair_prob_over_power"] + result["fair_prob_under_power"], 1.0, abs_tol=1e-9)
        assert math.isclose(result["fair_prob_over_shin"] + result["fair_prob_under_shin"], 1.0, abs_tol=1e-9)

    def test_lopsided_over_prob_higher(self):
        # Over at 1.10 is the heavy favourite
        result = devig.devig_all(D_FAV, D_DOG)
        assert result is not None
        assert result["fair_prob_over_mult"] > result["fair_prob_under_mult"]
        assert result["fair_prob_over_power"] > result["fair_prob_under_power"]
        assert result["fair_prob_over_shin"] > result["fair_prob_under_shin"]

    def test_lopsided_pairs_sum_to_one(self):
        result = devig.devig_all(D_FAV, D_DOG)
        assert result is not None
        assert math.isclose(result["fair_prob_over_mult"] + result["fair_prob_under_mult"], 1.0, abs_tol=1e-9)
        assert math.isclose(result["fair_prob_over_power"] + result["fair_prob_under_power"], 1.0, abs_tol=1e-9)
        assert math.isclose(result["fair_prob_over_shin"] + result["fair_prob_under_shin"], 1.0, abs_tol=1e-9)

    def test_none_over_odds_returns_none(self):
        assert devig.devig_all(None, D110) is None

    def test_none_under_odds_returns_none(self):
        assert devig.devig_all(D110, None) is None

    def test_odds_at_one_returns_none(self):
        # <= 1.0 is invalid (no-return or prop-bet placeholder)
        assert devig.devig_all(1.0, D110) is None
        assert devig.devig_all(D110, 1.0) is None

    def test_odds_below_one_returns_none(self):
        assert devig.devig_all(0.5, D110) is None

    def test_overround_positive_for_vig_market(self):
        result = devig.devig_all(D110, D110)
        assert result is not None
        assert result["overround"] > 0

    def test_method_disagreement_in_result(self):
        result = devig.devig_all(D110, D110)
        assert result is not None
        assert "method_disagreement" in result
        assert "disagreement_flag" in result

    def test_symmetric_110_low_disagreement(self):
        # All three methods should agree closely for a symmetric market
        result = devig.devig_all(D110, D110)
        assert result is not None
        assert result["method_disagreement"] < 0.01

    def test_disagreement_flag_false_for_small_spread(self):
        result = devig.devig_all(D110, D110)
        assert result is not None
        assert result["disagreement_flag"] is False


# ---------------------------------------------------------------------------
# method_disagreement
# ---------------------------------------------------------------------------

class TestMethodDisagreement:
    def test_identical_estimates_zero(self):
        assert devig.method_disagreement([0.5, 0.5, 0.5]) == 0.0

    def test_spread_correct(self):
        val = devig.method_disagreement([0.48, 0.50, 0.52])
        assert math.isclose(val, 0.04, abs_tol=1e-9)

    def test_single_estimate_zero(self):
        assert devig.method_disagreement([0.5]) == 0.0

    def test_empty_list_zero(self):
        assert devig.method_disagreement([]) == 0.0

    def test_none_values_skipped(self):
        # None values should be filtered out
        val = devig.method_disagreement([0.48, None, 0.52])
        assert math.isclose(val, 0.04, abs_tol=1e-9)

    def test_disagreement_flag_threshold(self):
        # > 0.02 should trigger flag; devig_all uses this threshold
        big_spread = devig.method_disagreement([0.40, 0.43])
        assert big_spread > 0.02
        small_spread = devig.method_disagreement([0.500, 0.501])
        assert small_spread < 0.02
