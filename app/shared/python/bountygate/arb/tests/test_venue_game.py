"""Tests for bountygate.arb.venue.pair_book_venue_game (book x venue game lines)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from bountygate.arb.venue import pair_book_venue_game

UTC = timezone.utc
COMMENCE = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
CAPTURED = datetime(2026, 6, 30, 0, 0, tzinfo=UTC)  # 24h before commence


def _book(**kw):
    base = {
        "event_id": "evt1",
        "sport_key": "baseball_mlb",
        "home_team": "Los Angeles Dodgers",
        "away_team": "St. Louis Cardinals",
        "commence_time": COMMENCE,
        "market_type": "h2h",
        "bookmaker": "draftkings",
        "outcome_name": "Los Angeles Dodgers",
        "point": None,
        "decimal_price": 2.10,
        "captured_at": CAPTURED,
    }
    base.update(kw)
    return base


def _venue(**kw):
    base = {
        "market_id": "mkt1",
        "venue_key": "kalshi",
        "event_id": "evt1",
        "sport_key": "baseball_mlb",
        "home_team": "Los Angeles Dodgers",
        "away_team": "St. Louis Cardinals",
        "commence_time": COMMENCE,
        "outcome_name": "St. Louis Cardinals",
        "ask": 0.45,
        "captured_at": CAPTURED,
        "title": "Will the Cardinals beat the Dodgers?",
    }
    base.update(kw)
    return base


def test_known_case_clears_with_fee():
    # book 2.10 on home (Dodgers) x kalshi ask 0.45 on away (Cardinals).
    books = [_book(bookmaker="draftkings", outcome_name="Los Angeles Dodgers",
                   decimal_price=2.10)]
    venues = [_venue(outcome_name="St. Louis Cardinals", ask=0.45)]
    opps, stats = pair_book_venue_game(books, venues)
    assert len(opps) == 1
    o = opps[0]
    assert o["fee_adjusted_roi"] == pytest.approx(0.05687, abs=1e-4)
    assert o["roi"] == pytest.approx(0.07969, abs=1e-4)
    assert o["kind"] == "game"
    assert o["pairing"] == "book_venue"
    assert o["market_segment"] == "h2h"
    assert o["leg_a_kind"] == "book"
    assert o["leg_a_source"] == "draftkings"
    assert o["leg_b_kind"] == "venue"
    assert o["leg_b_source"] == "kalshi"
    assert o["leg_b_price"] == 0.45
    assert o["details"]["market_id"] == "mkt1"
    assert o["details"]["fee_inputs"]["fee"] == pytest.approx(0.02)
    # stakes normalize to base_wager total
    assert o["leg_a_stake"] + o["leg_b_stake"] == pytest.approx(100.0)
    assert stats["paired"] == 1


def test_fee_flips_marginal_dropped_at_zero():
    # book 2.0 home x kalshi ask 0.49 away: pre +0.0101, fee -0.0099.
    books = [_book(decimal_price=2.0)]
    venues = [_venue(ask=0.49)]
    opps, stats = pair_book_venue_game(books, venues, min_roi=0.0)
    assert opps == []
    assert stats["paired"] == 0


def test_fee_flips_marginal_kept_with_negative_floor():
    books = [_book(decimal_price=2.0)]
    venues = [_venue(ask=0.49)]
    opps, stats = pair_book_venue_game(books, venues, min_roi=-0.02)
    assert len(opps) == 1
    o = opps[0]
    assert o["roi"] == pytest.approx(0.0101, abs=1e-4)
    assert o["fee_adjusted_roi"] == pytest.approx(-0.0099, abs=1e-4)


def test_outcome_resolving_neither_team_no_match():
    books = [_book()]
    venues = [_venue(outcome_name="New York Yankees")]  # not in this game
    opps, stats = pair_book_venue_game(books, venues)
    assert opps == []
    assert stats["no_team_match"] == 1
    assert stats["paired"] == 0


def test_ask_none_missing_ask():
    books = [_book()]
    venues = [_venue(ask=None)]
    opps, stats = pair_book_venue_game(books, venues)
    assert opps == []
    assert stats["missing_ask"] == 1


def test_ask_one_dropped():
    books = [_book()]
    venues = [_venue(ask=1.0)]
    opps, stats = pair_book_venue_game(books, venues)
    assert opps == []
    assert stats["paired"] == 0
    assert stats["missing_ask"] == 0


def test_both_directions_generated():
    # book home + venue away  AND  book away + venue home.
    books = [
        _book(bookmaker="draftkings", outcome_name="Los Angeles Dodgers",
              decimal_price=2.10),
        _book(bookmaker="fanduel", outcome_name="St. Louis Cardinals",
              decimal_price=2.10),
    ]
    venues = [
        _venue(market_id="mkt_away", outcome_name="St. Louis Cardinals", ask=0.45),
        _venue(market_id="mkt_home", outcome_name="Los Angeles Dodgers", ask=0.45),
    ]
    opps, stats = pair_book_venue_game(books, venues)
    # book home(DK) x venue away  and  book away(FD) x venue home  -> 2 opps.
    assert len(opps) == 2
    assert stats["paired"] == 2
    sources = {(o["leg_a_source"], o["leg_b_outcome"]) for o in opps}
    assert ("draftkings", "St. Louis Cardinals") in sources
    assert ("fanduel", "Los Angeles Dodgers") in sources


def test_hash_dedupe():
    books = [_book(), _book()]  # identical duplicates
    venues = [_venue(), _venue()]
    opps, stats = pair_book_venue_game(books, venues)
    assert len(opps) == 1


def test_same_team_no_pair():
    # venue on home, book also on home -> no opposing pair.
    books = [_book(outcome_name="Los Angeles Dodgers")]
    venues = [_venue(outcome_name="Los Angeles Dodgers")]  # same side
    opps, stats = pair_book_venue_game(books, venues)
    assert opps == []


def test_in_play_dropped():
    past = CAPTURED - timedelta(hours=1)
    books = [_book(commence_time=past)]
    venues = [_venue(commence_time=past)]
    opps, stats = pair_book_venue_game(books, venues)
    assert opps == []


def test_nhl_containment_fallback():
    # NHL has no alias file -> case-insensitive containment fallback.
    books = [_book(
        sport_key="icehockey_nhl",
        home_team="Boston Bruins",
        away_team="Toronto Maple Leafs",
        outcome_name="Boston Bruins",
        decimal_price=2.10,
    )]
    venues = [_venue(
        sport_key="icehockey_nhl",
        home_team="Boston Bruins",
        away_team="Toronto Maple Leafs",
        outcome_name="Toronto Maple Leafs",
        ask=0.45,
    )]
    opps, stats = pair_book_venue_game(books, venues)
    assert len(opps) == 1
    assert stats["paired"] == 1


def test_empty_inputs():
    assert pair_book_venue_game([], []) == ([], {
        "paired": 0, "no_team_match": 0, "ambiguous": 0,
        "stale_skipped": 0, "missing_ask": 0,
    })
