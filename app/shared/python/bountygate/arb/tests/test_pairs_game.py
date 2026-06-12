"""Tests for bountygate.arb.pairs.pair_game_lines (book x book game lines)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from bountygate.arb.pairs import pair_game_lines

UTC = timezone.utc
COMMENCE = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
CAPTURED = datetime(2026, 6, 30, 0, 0, tzinfo=UTC)  # 24h before commence


def _row(**kw):
    base = {
        "event_id": "evt1",
        "sport_key": "baseball_mlb",
        "home_team": "Home",
        "away_team": "Away",
        "commence_time": COMMENCE,
        "market_type": "h2h",
        "bookmaker": "draftkings",
        "outcome_name": "Home",
        "point": None,
        "decimal_price": 2.10,
        "captured_at": CAPTURED,
    }
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# h2h
# ---------------------------------------------------------------------------
def test_h2h_cross_book_arb():
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10),
        _row(bookmaker="fanduel", outcome_name="Away", decimal_price=2.10),
    ]
    opps = pair_game_lines(rows)
    assert len(opps) == 1
    o = opps[0]
    # overround = 2/2.1 = 0.952381; payout = 100/overround = 105; roi = 0.05
    assert o["roi"] == pytest.approx(0.05)
    assert o["payout"] == pytest.approx(105.0)
    assert o["arb_ev"] == pytest.approx(5.0)
    assert o["fee_adjusted_roi"] == o["roi"]
    assert o["kind"] == "game"
    assert o["pairing"] == "book_book"
    assert o["market_segment"] == "h2h"
    assert o["player_name"] is None
    assert o["line"] is None
    assert o["pairing_type"] is None
    # symmetric dedupe ordering: draftkings < fanduel
    assert o["leg_a_source"] == "draftkings"
    assert o["leg_b_source"] == "fanduel"


def test_h2h_no_arb_when_overround_above_one():
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=1.90),
        _row(bookmaker="fanduel", outcome_name="Away", decimal_price=1.90),
    ]
    # overround = 2/1.9 = 1.0526 > 1 -> no arb
    assert pair_game_lines(rows) == []


def test_h2h_same_book_no_pair():
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10),
        _row(bookmaker="draftkings", outcome_name="Away", decimal_price=2.10),
    ]
    assert pair_game_lines(rows) == []


def test_h2h_same_outcome_no_pair():
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10),
        _row(bookmaker="fanduel", outcome_name="Home", decimal_price=2.10),
    ]
    assert pair_game_lines(rows) == []


# ---------------------------------------------------------------------------
# spreads
# ---------------------------------------------------------------------------
def test_spreads_mirrored_arb():
    rows = [
        _row(market_type="spreads", bookmaker="draftkings", outcome_name="Home",
             point=-3.5, decimal_price=2.10),
        _row(market_type="spreads", bookmaker="fanduel", outcome_name="Away",
             point=3.5, decimal_price=2.10),
    ]
    opps = pair_game_lines(rows)
    assert len(opps) == 1
    o = opps[0]
    assert o["market_segment"] == "spreads"
    assert o["line"] is None  # leg points carry the spread
    # leg points preserved (ordered draftkings < fanduel)
    assert o["leg_a_point"] == -3.5
    assert o["leg_b_point"] == 3.5
    assert o["roi"] == pytest.approx(0.05)


def test_spreads_non_mirrored_no_pair():
    rows = [
        _row(market_type="spreads", bookmaker="draftkings", outcome_name="Home",
             point=-3.5, decimal_price=2.10),
        _row(market_type="spreads", bookmaker="fanduel", outcome_name="Away",
             point=4.5, decimal_price=2.10),
    ]
    assert pair_game_lines(rows) == []


def test_spreads_point_none_dropped():
    rows = [
        _row(market_type="spreads", bookmaker="draftkings", outcome_name="Home",
             point=None, decimal_price=2.10),
        _row(market_type="spreads", bookmaker="fanduel", outcome_name="Away",
             point=None, decimal_price=2.10),
    ]
    assert pair_game_lines(rows) == []


# ---------------------------------------------------------------------------
# totals
# ---------------------------------------------------------------------------
def test_totals_same_point_arb():
    rows = [
        _row(market_type="totals", bookmaker="draftkings", outcome_name="Over",
             point=8.5, decimal_price=2.10),
        _row(market_type="totals", bookmaker="fanduel", outcome_name="Under",
             point=8.5, decimal_price=2.10),
    ]
    opps = pair_game_lines(rows)
    assert len(opps) == 1
    o = opps[0]
    assert o["market_segment"] == "totals"
    assert o["line"] == 8.5  # totals point == line
    assert o["roi"] == pytest.approx(0.05)


def test_totals_different_points_no_pair():
    rows = [
        _row(market_type="totals", bookmaker="draftkings", outcome_name="Over",
             point=8.5, decimal_price=2.10),
        _row(market_type="totals", bookmaker="fanduel", outcome_name="Under",
             point=9.5, decimal_price=2.10),
    ]
    assert pair_game_lines(rows) == []


def test_totals_outcome_defense_non_over_under():
    # Rows mislabeled with Yes/No outcomes should never pair as totals.
    rows = [
        _row(market_type="totals", bookmaker="draftkings", outcome_name="Yes",
             point=8.5, decimal_price=2.10),
        _row(market_type="totals", bookmaker="fanduel", outcome_name="No",
             point=8.5, decimal_price=2.10),
    ]
    assert pair_game_lines(rows) == []


# ---------------------------------------------------------------------------
# dedupe + filters
# ---------------------------------------------------------------------------
def test_symmetric_input_order_yields_one():
    a = _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10)
    b = _row(bookmaker="fanduel", outcome_name="Away", decimal_price=2.10)
    assert len(pair_game_lines([a, b])) == 1
    assert len(pair_game_lines([b, a])) == 1
    # same hash regardless of input order
    assert (
        pair_game_lines([a, b])[0]["opportunity_hash"]
        == pair_game_lines([b, a])[0]["opportunity_hash"]
    )


def test_duplicate_identical_rows_one_opp():
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10),
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10),  # dup
        _row(bookmaker="fanduel", outcome_name="Away", decimal_price=2.10),
        _row(bookmaker="fanduel", outcome_name="Away", decimal_price=2.10),  # dup
    ]
    # Cartesian would make multiple pairs with identical hashes -> dedupe to one.
    opps = pair_game_lines(rows)
    assert len(opps) == 1


def test_min_roi_floor_filters():
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10),
        _row(bookmaker="fanduel", outcome_name="Away", decimal_price=2.10),
    ]
    # The 5% opp is filtered when min_roi exceeds it.
    assert pair_game_lines(rows, min_roi=0.06) == []
    assert len(pair_game_lines(rows, min_roi=0.04)) == 1


def test_in_play_dropped():
    # commence before capture -> hours_until_commence <= 0 -> dropped
    past_commence = CAPTURED - timedelta(hours=1)
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10,
             commence_time=past_commence),
        _row(bookmaker="fanduel", outcome_name="Away", decimal_price=2.10,
             commence_time=past_commence),
    ]
    assert pair_game_lines(rows) == []


def test_price_none_or_zero_dropped():
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=None),
        _row(bookmaker="fanduel", outcome_name="Away", decimal_price=2.10),
        _row(bookmaker="betmgm", outcome_name="Away", decimal_price=0.0),
    ]
    assert pair_game_lines(rows) == []


def test_empty_input():
    assert pair_game_lines([]) == []


def test_spreads_pickem_zero_vs_neg_zero():
    """Pick'em spread: point 0.0 (book A) vs -0.0 (book B).

    0.0 == -0.0 in Python floats, and -(-0.0) == 0.0, so the mirroring check
    ``a.point == -b.point`` should pass and exactly one opportunity is produced.
    """
    rows = [
        _row(market_type="spreads", bookmaker="betmgm", outcome_name="Home",
             point=0.0, decimal_price=2.10),
        _row(market_type="spreads", bookmaker="fanduel", outcome_name="Away",
             point=-0.0, decimal_price=2.10),
    ]
    opps = pair_game_lines(rows)
    assert len(opps) == 1
    o = opps[0]
    assert o["market_segment"] == "spreads"
    assert o["roi"] == pytest.approx(0.05)


def test_hours_until_commence_uses_latest_capture():
    later_capture = CAPTURED + timedelta(hours=6)
    rows = [
        _row(bookmaker="draftkings", outcome_name="Home", decimal_price=2.10,
             captured_at=CAPTURED),
        _row(bookmaker="fanduel", outcome_name="Away", decimal_price=2.10,
             captured_at=later_capture),
    ]
    o = pair_game_lines(rows)[0]
    # commence is 24h after CAPTURED; latest capture is 6h later -> 18h
    assert o["hours_until_commence"] == pytest.approx(18.0)
