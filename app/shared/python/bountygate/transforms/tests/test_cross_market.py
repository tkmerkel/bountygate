from datetime import datetime, timezone

from bountygate.transforms.marts.cross_market import assemble_rows, sportsbook_side_probs


def test_sportsbook_side_probs_pinnacle_devig():
    by_book = {"pinnacle": {"LAD": 1.5, "STL": 2.6}}
    p = sportsbook_side_probs(by_book, "LAD", "STL")
    assert round(p["LAD"] + p["STL"], 6) == 1.0
    assert p["LAD"] > p["STL"]


def test_sportsbook_side_probs_consensus_fallback():
    by_book = {"fanduel": {"LAD": 1.5, "STL": 2.6}, "draftkings": {"LAD": 1.52, "STL": 2.55}}
    p = sportsbook_side_probs(by_book, "LAD", "STL")
    assert p is not None and 0 < p["LAD"] < 1


def test_sportsbook_side_probs_none_when_no_two_way():
    assert sportsbook_side_probs({"fanduel": {"LAD": 1.5}}, "LAD", "STL") is None


def test_assemble_two_rows_per_game_and_spread():
    games = [{
        "sport": "MLB", "date": datetime(2026, 5, 3, tzinfo=timezone.utc),
        "home": "STL", "away": "LAD",
        "sportsbook": {"LAD": 0.60, "STL": 0.40},
        "kalshi": {"LAD": 0.58},
        "polymarket": {"LAD": 0.62, "STL": 0.38},
    }]
    by_key = {r["question_key"]: r for r in assemble_rows(games)}
    lad = by_key["mlb:2026-05-03:LAD@STL:LAD"]
    assert lad["kalshi_prob"] == 0.58
    assert lad["polymarket_prob"] == 0.62
    assert lad["sportsbook_consensus_prob"] == 0.60
    assert round(lad["max_spread"], 2) == 0.04  # 0.62 - 0.58
    stl = by_key["mlb:2026-05-03:LAD@STL:STL"]
    assert stl["kalshi_prob"] is None           # only the LAD-side Kalshi market was linked
    assert round(stl["max_spread"], 2) == 0.02  # 0.40 - 0.38


def test_one_venue_side_dropped():
    games = [{
        "sport": "NBA", "date": datetime(2026, 6, 9, tzinfo=timezone.utc),
        "home": "BOS", "away": "NYK",
        "sportsbook": {}, "kalshi": {"BOS": 0.7}, "polymarket": {},
    }]
    assert assemble_rows(games) == []
