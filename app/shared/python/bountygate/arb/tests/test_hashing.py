"""Tests for bountygate.arb.hashing — pairing type + opportunity hash."""
from __future__ import annotations

from bountygate.arb.hashing import derive_pairing_type, opportunity_hash


# ---------------------------------------------------------------------------
# derive_pairing_type truth table
# ---------------------------------------------------------------------------
def test_pairing_std_std():
    assert derive_pairing_type("player_points", "player_points") == "std_std"


def test_pairing_std_alt():
    assert derive_pairing_type("player_points", "player_points_alternate") == "std_alt"


def test_pairing_alt_std():
    assert derive_pairing_type("player_points_alternate", "player_points") == "alt_std"


def test_pairing_alt_alt():
    assert (
        derive_pairing_type("player_points_alternate", "player_points_alternate")
        == "alt_alt"
    )


def test_pairing_substring_does_not_count():
    # "_alternate" must be a suffix, not just a substring
    assert derive_pairing_type("player_alternate_points", "player_points") == "std_std"


# ---------------------------------------------------------------------------
# opportunity_hash
# ---------------------------------------------------------------------------
def _legs_a():
    return [
        ("draftkings", "under", 25.5, 0.523456),
        ("kalshi", "over", 25.5, 0.461234),
    ]


def test_hash_is_hex_sha256():
    h = opportunity_hash(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="player_points",
        legs=_legs_a(),
    )
    assert isinstance(h, str)
    assert len(h) == 64
    int(h, 16)  # parses as hex


def test_hash_stable_across_calls():
    kw = dict(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="player_points",
        legs=_legs_a(),
    )
    assert opportunity_hash(**kw) == opportunity_hash(**kw)


def test_hash_symmetric_leg_order():
    legs = _legs_a()
    swapped = list(reversed(legs))
    base = dict(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="player_points",
    )
    assert opportunity_hash(legs=legs, **base) == opportunity_hash(
        legs=swapped, **base
    )


def test_hash_price_change_1e6_differs():
    base = dict(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="player_points",
    )
    legs1 = [
        ("draftkings", "under", 25.5, 0.523456),
        ("kalshi", "over", 25.5, 0.461234),
    ]
    legs2 = [
        ("draftkings", "under", 25.5, 0.523457),  # +1e-6
        ("kalshi", "over", 25.5, 0.461234),
    ]
    assert opportunity_hash(legs=legs1, **base) != opportunity_hash(legs=legs2, **base)


def test_hash_none_point_differs_from_zero():
    base = dict(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="moneyline",
    )
    legs_none = [
        ("draftkings", "home", None, 0.50),
        ("kalshi", "away", None, 0.50),
    ]
    legs_zero = [
        ("draftkings", "home", 0.0, 0.50),
        ("kalshi", "away", 0.0, 0.50),
    ]
    assert opportunity_hash(legs=legs_none, **base) != opportunity_hash(
        legs=legs_zero, **base
    )


def test_hash_none_line_differs_from_zero():
    base = dict(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="player_points",
        legs=_legs_a(),
    )
    assert opportunity_hash(line=None, **base) != opportunity_hash(line=0.0, **base)


def test_hash_player_name_change_differs():
    base = dict(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="player_points",
        legs=_legs_a(),
    )
    assert opportunity_hash(player_name="A", **base) != opportunity_hash(
        player_name="B", **base
    )


def test_hash_field_changes_differ():
    base = dict(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="player_points",
        legs=_legs_a(),
    )
    h0 = opportunity_hash(**base)
    assert opportunity_hash(**{**base, "kind": "venue_venue"}) != h0
    assert opportunity_hash(**{**base, "pairing": "std_alt"}) != h0
    assert opportunity_hash(**{**base, "event_id": "evt2"}) != h0
    assert opportunity_hash(**{**base, "market_segment": "player_assists"}) != h0


def test_hash_line_precision_3f():
    # lines differing below the 3rd decimal collapse to the same hash
    base = dict(
        kind="book_venue",
        pairing="std_std",
        event_id="evt1",
        market_segment="player_points",
        legs=_legs_a(),
    )
    assert opportunity_hash(line=25.5000, **base) == opportunity_hash(
        line=25.50004, **base
    )
