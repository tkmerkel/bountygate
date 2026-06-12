"""Tests for bountygate.arb.pairs.pair_props (book x book player props)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from bountygate.arb.pairs import pair_props

UTC = timezone.utc
COMMENCE = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
CAPTURED = datetime(2026, 6, 30, 0, 0, tzinfo=UTC)  # 24h before commence


def _row(**kw):
    base = {
        "event_id": "evt1",
        "sport_key": "basketball_nba",
        "home_team": "Home",
        "away_team": "Away",
        "commence_time": COMMENCE,
        "market_key": "player_points",
        "player_name": "Player One",
        "line": 25.5,
        "side": "under",
        "bookmaker": "fanduel",
        "decimal_price": 2.10,
        "captured_at": CAPTURED,
    }
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# basic std_std arb
# ---------------------------------------------------------------------------
def test_std_std_arb():
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10),
    ]
    opps = pair_props(rows)
    assert len(opps) == 1
    o = opps[0]
    assert o["roi"] == pytest.approx(0.05)
    assert o["payout"] == pytest.approx(105.0)
    assert o["kind"] == "prop"
    assert o["pairing"] == "book_book"
    assert o["market_segment"] == "player_points"
    assert o["player_name"] == "Player One"
    assert o["line"] == 25.5
    assert o["pairing_type"] == "std_std"
    assert o["leg_a_outcome"] == "under"
    assert o["leg_a_source"] == "fanduel"
    assert o["leg_b_outcome"] == "over"
    assert o["leg_b_source"] == "betmgm"
    assert o["details"] == {
        "under_market_key": "player_points",
        "over_market_key": "player_points",
    }
    assert o["fee_adjusted_roi"] == o["roi"]


def test_same_book_no_pair():
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10),
        _row(side="over", bookmaker="fanduel", decimal_price=2.10),
    ]
    assert pair_props(rows) == []


def test_different_line_no_pair():
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10, line=25.5),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10, line=26.5),
    ]
    assert pair_props(rows) == []


def test_blacklist_dropped():
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10,
             market_key="pitcher_strikeouts"),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10,
             market_key="pitcher_strikeouts"),
    ]
    assert pair_props(rows) == []


def test_price_none_or_zero_dropped():
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=None),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10),
    ]
    assert pair_props(rows) == []


def test_line_none_dropped():
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10, line=None),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10, line=None),
    ]
    assert pair_props(rows) == []


# ---------------------------------------------------------------------------
# canonical-market join across std/alt
# ---------------------------------------------------------------------------
def test_alt_std_canonical_join():
    # alternate under x standard over join on canonical market.
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10,
             market_key="player_points_alternate"),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10,
             market_key="player_points"),
    ]
    opps = pair_props(rows)
    assert len(opps) == 1
    o = opps[0]
    assert o["market_segment"] == "player_points"
    assert o["pairing_type"] == "alt_std"
    assert o["details"] == {
        "under_market_key": "player_points_alternate",
        "over_market_key": "player_points",
    }


def test_std_alt_hash_differs_from_alt_std():
    std_alt = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10,
             market_key="player_points"),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10,
             market_key="player_points_alternate"),
    ]
    alt_std = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10,
             market_key="player_points_alternate"),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10,
             market_key="player_points"),
    ]
    o_sa = pair_props(std_alt)[0]
    o_as = pair_props(alt_std)[0]
    assert o_sa["pairing_type"] == "std_alt"
    assert o_as["pairing_type"] == "alt_std"
    # Hash must distinguish the two pairings (market keys encoded in legs).
    assert o_sa["opportunity_hash"] != o_as["opportunity_hash"]


# ---------------------------------------------------------------------------
# roi floor + dedupe + hash determinism
# ---------------------------------------------------------------------------
def test_negative_roi_dropped_at_default():
    # overround = 2/1.90 = 1.0526 > 1 -> negative roi -> dropped at min_roi=0
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=1.90),
        _row(side="over", bookmaker="betmgm", decimal_price=1.90),
    ]
    assert pair_props(rows) == []


def test_negative_roi_kept_with_low_min_roi():
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=1.90),
        _row(side="over", bookmaker="betmgm", decimal_price=1.90),
    ]
    opps = pair_props(rows, min_roi=-1.0)
    assert len(opps) == 1
    assert opps[0]["roi"] < 0


def test_in_play_dropped():
    past_commence = CAPTURED - timedelta(hours=1)
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10,
             commence_time=past_commence),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10,
             commence_time=past_commence),
    ]
    assert pair_props(rows) == []


def test_duplicate_identical_rows_one_opp():
    rows = [
        _row(side="under", bookmaker="fanduel", decimal_price=2.10),
        _row(side="under", bookmaker="fanduel", decimal_price=2.10),  # dup
        _row(side="over", bookmaker="betmgm", decimal_price=2.10),
        _row(side="over", bookmaker="betmgm", decimal_price=2.10),  # dup
    ]
    assert len(pair_props(rows)) == 1


def test_hash_stable_across_input_order():
    u = _row(side="under", bookmaker="fanduel", decimal_price=2.10)
    o = _row(side="over", bookmaker="betmgm", decimal_price=2.10)
    h1 = pair_props([u, o])[0]["opportunity_hash"]
    h2 = pair_props([o, u])[0]["opportunity_hash"]
    assert h1 == h2


def test_empty_input():
    assert pair_props([]) == []
