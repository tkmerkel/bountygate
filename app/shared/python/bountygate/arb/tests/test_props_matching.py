"""Tests for bountygate.arb.props_matching (title parsing + book x venue prop pairs)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from bountygate.arb.props_matching import (
    normalize_player,
    parse_prop_title,
    pair_book_venue_props,
)

UTC = timezone.utc
COMMENCE = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
CAPTURED = datetime(2026, 6, 30, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# normalize_player
# ---------------------------------------------------------------------------
def test_normalize_strips_accents_and_punct():
    assert normalize_player("Nikola Jokić") == "nikola jokic"
    assert normalize_player("D'Angelo Russell") == "d angelo russell"
    assert normalize_player("  LeBron   James ") == "lebron james"


# ---------------------------------------------------------------------------
# parse_prop_title
# ---------------------------------------------------------------------------
def test_parse_points():
    r = parse_prop_title("Will LeBron James score 30+ points?", "basketball_nba")
    assert r == {"player_name": "lebron james", "market_segment": "player_points",
                 "line": 29.5}


def test_parse_accents_stripped():
    r = parse_prop_title("Will Nikola Jokić record 12+ rebounds?", "basketball_nba")
    assert r["player_name"] == "nikola jokic"
    assert r["market_segment"] == "player_rebounds"
    assert r["line"] == 11.5


def test_parse_non_will_returns_none():
    assert parse_prop_title("LeBron James 30+ points", "basketball_nba") is None


def test_parse_two_stat_ambiguous_returns_none():
    # both points AND assists patterns match -> ambiguous -> None.
    r = parse_prop_title(
        "Will Luka Doncic record 30+ points and 10+ assists?", "basketball_nba"
    )
    assert r is None


def test_parse_unknown_stat_returns_none():
    assert parse_prop_title("Will LeBron James record 5+ steals?", "basketball_nba") is None


def test_parse_long_player_segment_rejected():
    # implausibly long player segment (>4 words after filler strip).
    assert parse_prop_title(
        "Will the starting point guard for the Lakers score 30+ points?",
        "basketball_nba",
    ) is None


def test_parse_assists():
    r = parse_prop_title("Will Trae Young record 11+ assists?", "basketball_nba")
    assert r == {"player_name": "trae young", "market_segment": "player_assists",
                 "line": 10.5}


def test_parse_threes():
    r = parse_prop_title("Will Stephen Curry make 5+ three-pointers?", "basketball_nba")
    assert r["market_segment"] == "player_threes"
    assert r["line"] == 4.5


def test_parse_nhl_shots():
    r = parse_prop_title("Will Auston Matthews record 4+ shots on goal?", "icehockey_nhl")
    assert r == {"player_name": "auston matthews",
                 "market_segment": "player_shots_on_goal", "line": 3.5}


def test_parse_wnba_reuses_nba_dict():
    r = parse_prop_title("Will A'ja Wilson score 25+ points?", "basketball_wnba")
    assert r["market_segment"] == "player_points"
    assert r["player_name"] == "a ja wilson"


def test_parse_mlb_empty_dict_returns_none():
    assert parse_prop_title("Will Aaron Judge hit a home run?", "baseball_mlb") is None


def test_parse_first_half_qualifier_returns_none():
    # Trailing "in the first half" is NOT a full-game prop -> must not parse.
    assert parse_prop_title(
        "Will LeBron James score 30+ points in the first half?", "basketball_nba"
    ) is None


def test_parse_quarter_qualifier_returns_none():
    # Trailing "in Q1" is a partial-game scope -> must not parse.
    assert parse_prop_title(
        "Will Jayson Tatum record 10+ rebounds in Q1?", "basketball_nba"
    ) is None


def test_parse_tonight_tail_allowed():
    # "tonight" is a harmless whitelisted tail -> still parses.
    r = parse_prop_title(
        "Will LeBron James score 30+ points tonight?", "basketball_nba"
    )
    assert r == {"player_name": "lebron james", "market_segment": "player_points",
                 "line": 29.5}


def test_parse_team_total_leading_the_returns_none():
    # Team totals like "Will the Lakers score 110+ points?" -> leading-the guard.
    assert parse_prop_title(
        "Will the Lakers score 110+ points?", "basketball_nba"
    ) is None


# ---------------------------------------------------------------------------
# pair_book_venue_props
# ---------------------------------------------------------------------------
def _book(**kw):
    base = {
        "event_id": "evt1",
        "sport_key": "basketball_nba",
        "home_team": "Lakers",
        "away_team": "Warriors",
        "commence_time": COMMENCE,
        "market_key": "player_points",
        "player_name": "LeBron James",
        "line": 29.5,
        "side": "under",
        "bookmaker": "draftkings",
        "decimal_price": 2.10,
        "captured_at": CAPTURED,
    }
    base.update(kw)
    return base


def _venue(**kw):
    base = {
        "market_id": "kmkt1",
        "venue_key": "kalshi",
        "event_id": "evt1",
        "sport_key": "basketball_nba",
        "home_team": "Lakers",
        "away_team": "Warriors",
        "commence_time": COMMENCE,
        "outcome_name": "Yes",
        "ask": 0.45,
        "captured_at": CAPTURED,
        "title": "Will LeBron James score 30+ points?",
    }
    base.update(kw)
    return base


def test_yes_pairs_with_under():
    books = [_book(side="under", line=29.5, decimal_price=2.10)]
    venues = [_venue(outcome_name="Yes", ask=0.45)]
    opps, stats = pair_book_venue_props(books, venues)
    assert len(opps) == 1
    o = opps[0]
    assert o["kind"] == "prop"
    assert o["pairing"] == "book_venue"
    assert o["market_segment"] == "player_points"
    assert o["player_name"] == "LeBron James"  # book spelling, original case
    assert o["line"] == 29.5
    assert o["leg_a_outcome"] == "under"
    assert o["leg_b_outcome"] == "yes"
    assert o["leg_b_source"] == "kalshi"
    assert o["fee_adjusted_roi"] == pytest.approx(0.05687, abs=1e-4)
    assert o["details"]["title"] == "Will LeBron James score 30+ points?"
    assert o["details"]["threshold"] == 30
    assert stats["matched"] == 1
    assert stats["paired"] == 1


def test_no_pairs_with_over():
    books = [_book(side="over", line=29.5, decimal_price=2.10)]
    venues = [_venue(outcome_name="No", ask=0.45)]
    opps, stats = pair_book_venue_props(books, venues)
    assert len(opps) == 1
    o = opps[0]
    assert o["leg_a_outcome"] == "over"
    assert o["leg_b_outcome"] == "no"


def test_player_mismatch_unmatched_player():
    books = [_book(player_name="Anthony Davis", side="under", line=29.5)]
    venues = [_venue(outcome_name="Yes")]  # LeBron James
    opps, stats = pair_book_venue_props(books, venues)
    assert opps == []
    assert stats["unmatched_player"] == 1


def test_line_mismatch_unmatched_line():
    # book under at 28.5 vs parsed 29.5.
    books = [_book(side="under", line=28.5)]
    venues = [_venue(outcome_name="Yes")]
    opps, stats = pair_book_venue_props(books, venues)
    assert opps == []
    assert stats["unmatched_line"] == 1


def test_alternate_market_key_canonicalizes():
    books = [_book(market_key="player_points_alternate", side="under", line=29.5)]
    venues = [_venue(outcome_name="Yes", ask=0.45)]
    opps, stats = pair_book_venue_props(books, venues)
    assert len(opps) == 1
    assert opps[0]["market_segment"] == "player_points"
    assert stats["matched"] == 1


def test_unparsed_title_counted():
    books = [_book()]
    venues = [_venue(title="Random non-prop question?")]
    opps, stats = pair_book_venue_props(books, venues)
    assert opps == []
    assert stats["unparsed"] == 1
    assert stats["parsed"] == 0


def test_ambiguous_title_counted():
    books = [_book()]
    venues = [_venue(
        title="Will Luka Doncic record 30+ points and 10+ assists?",
        outcome_name="Yes",
    )]
    opps, stats = pair_book_venue_props(books, venues)
    assert opps == []
    assert stats["ambiguous"] == 1


def test_event_mismatch_no_pair():
    books = [_book(event_id="evt1")]
    venues = [_venue(event_id="evt_other", outcome_name="Yes")]
    opps, stats = pair_book_venue_props(books, venues)
    assert opps == []


def test_dedupe_identical():
    books = [_book(), _book()]
    venues = [_venue(), _venue()]
    opps, stats = pair_book_venue_props(books, venues)
    assert len(opps) == 1
