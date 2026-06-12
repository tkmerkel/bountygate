"""Tests for bountygate.arb.fees — Kalshi taker fee + book-x-venue ROI."""
from __future__ import annotations

import math

import pytest

from bountygate.arb.fees import (
    book_venue_roi,
    kalshi_taker_fee,
    venue_effective_cost,
)


# ---------------------------------------------------------------------------
# kalshi_taker_fee
# ---------------------------------------------------------------------------
def test_fee_at_half_is_two_cents():
    # 0.07 * 1 * 0.5 * 0.5 = 0.0175 -> ceil to cent = 0.02
    assert kalshi_taker_fee(0.50) == pytest.approx(0.02)


def test_fee_tiny_price_ceils_to_one_cent():
    # 0.07 * 1 * 0.01 * 0.99 = 0.000693 -> ceil to cent = 0.01
    assert kalshi_taker_fee(0.01) == pytest.approx(0.01)


def test_fee_high_price_ceils_to_one_cent():
    # symmetric: 0.07 * 0.99 * 0.01 = 0.000693 -> 0.01
    assert kalshi_taker_fee(0.99) == pytest.approx(0.01)


def test_fee_is_symmetric_in_price():
    assert kalshi_taker_fee(0.30) == kalshi_taker_fee(0.70)


def test_fee_exact_cent_multiple_no_overshoot():
    # Pick contracts so raw lands exactly on a cent boundary -> no ceil bump.
    # raw = 0.07 * C * 0.5 * 0.5 = 0.0175 * C. C=40 -> 0.70 exactly.
    fee = kalshi_taker_fee(0.50, contracts=40)
    assert fee == pytest.approx(0.70)
    # confirm raw was already a cent multiple (no ceiling applied beyond it)
    raw = 0.07 * 40 * 0.5 * 0.5
    assert math.isclose(raw, 0.70)


def test_fee_scales_with_contracts_then_ceils_once():
    # 0.07 * 10 * 0.5 * 0.5 = 0.175 -> ceil to cent = 0.18 (0.175 ceils up)
    assert kalshi_taker_fee(0.50, contracts=10) == pytest.approx(0.18)


def test_fee_explicit_rate_param():
    # rate 0.035: 0.035 * 0.5 * 0.5 = 0.00875 -> ceil = 0.01
    assert kalshi_taker_fee(0.50, rate=0.035) == pytest.approx(0.01)


def test_fee_env_override(monkeypatch):
    monkeypatch.setenv("BG_KALSHI_FEE_RATE", "0.14")
    # 0.14 * 0.5 * 0.5 = 0.035 -> ceil = 0.035 -> exact cent = 0.04? 0.035 ceils to 0.04
    assert kalshi_taker_fee(0.50) == pytest.approx(0.04)


def test_explicit_rate_wins_over_env(monkeypatch):
    monkeypatch.setenv("BG_KALSHI_FEE_RATE", "0.99")
    # explicit rate=0.07 should be used, ignoring env
    assert kalshi_taker_fee(0.50, rate=0.07) == pytest.approx(0.02)


# ---------------------------------------------------------------------------
# venue_effective_cost
# ---------------------------------------------------------------------------
def test_kalshi_effective_cost_adds_fee():
    # price 0.45, fee(0.45)=ceil(0.07*0.45*0.55=0.017325)=0.02 -> 0.47
    assert venue_effective_cost(0.45, "kalshi") == pytest.approx(0.47)


def test_polymarket_effective_cost_no_fee():
    assert venue_effective_cost(0.45, "polymarket") == pytest.approx(0.45)


def test_unknown_venue_no_fee():
    assert venue_effective_cost(0.45, "something_else") == pytest.approx(0.45)


# ---------------------------------------------------------------------------
# book_venue_roi
# ---------------------------------------------------------------------------
def test_book_venue_roi_known_kalshi_case():
    # book 2.10 (side A) + kalshi ask 0.45 (side B)
    # t_pre = 1/2.10 + 0.45 = 0.476190... + 0.45 = 0.926190...
    # roi_pre = 1/0.926190 - 1 ~= +0.07969
    # fee(0.45) = 0.02 -> t_fee = 0.476190 + 0.47 = 0.946190
    # roi_fee = 1/0.946190 - 1 ~= +0.05688
    roi_pre, roi_fee = book_venue_roi(2.10, 0.45, "kalshi")
    assert roi_pre == pytest.approx(0.07969, abs=1e-4)
    assert roi_fee == pytest.approx(0.05688, abs=1e-4)
    assert roi_fee < roi_pre


def test_book_venue_roi_polymarket_pre_equals_fee():
    roi_pre, roi_fee = book_venue_roi(2.10, 0.45, "polymarket")
    assert roi_pre == pytest.approx(roi_fee)


def test_book_venue_roi_fee_flips_positive_to_negative():
    # Choose a marginal pre-fee arb that the Kalshi fee erases.
    # book 2.0 (1/decimal = 0.50), kalshi ask 0.49.
    # t_pre = 0.50 + 0.49 = 0.99 -> roi_pre = 1/0.99 - 1 = +0.0101 (positive arb)
    # fee(0.49) = ceil(0.07*0.49*0.51=0.0174930)=0.02 -> t_fee = 0.50 + 0.51 = 1.01
    # roi_fee = 1/1.01 - 1 = -0.0099 (negative)
    roi_pre, roi_fee = book_venue_roi(2.0, 0.49, "kalshi")
    assert roi_pre > 0
    assert roi_fee < 0


def test_book_venue_roi_returns_tuple_of_floats():
    out = book_venue_roi(2.10, 0.45, "kalshi")
    assert isinstance(out, tuple) and len(out) == 2
    assert all(isinstance(x, float) for x in out)
