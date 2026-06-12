"""Tests for the player-prop parser (pure, no I/O)."""
from bountygate.transforms.parsers.props import parse_player_prop


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

_STD_PAYLOAD = {
    "event_id": "evt-prop-1",
    "sport_key": "basketball_nba",
    "commence_time": "2026-06-15T19:00:00Z",
    "home_team": "Boston Celtics",
    "away_team": "Miami Heat",
    "market": "player_points",
    "bookmaker": "draftkings",
    "outcomes": [
        {"name": "Over",  "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
        {"name": "Under", "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
    ],
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_std_market_over_under_both_parsed():
    out = parse_player_prop(_STD_PAYLOAD)
    props = out["props"]
    assert len(props) == 2
    sides = {p["side"] for p in props}
    assert sides == {"over", "under"}


def test_prop_row_fields_correct():
    out = parse_player_prop(_STD_PAYLOAD)
    over = next(p for p in out["props"] if p["side"] == "over")
    assert over["player_name"] == "Jayson Tatum"
    assert over["line"] == 27.5
    assert over["bookmaker"] == "draftkings"
    assert over["decimal_price"] == 1.91
    assert over["market_key"] == "player_points"


def test_alternate_market_key_preserved_verbatim():
    payload = {**_STD_PAYLOAD, "market": "player_points_alternate"}
    out = parse_player_prop(payload)
    assert all(p["market_key"] == "player_points_alternate" for p in out["props"])


def test_yes_tile_outcome_dropped():
    """Outcomes whose name is 'Yes' (yes-only tiles) must be dropped."""
    payload = {
        **_STD_PAYLOAD,
        "outcomes": [
            {"name": "Yes", "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
            {"name": "Over",  "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
            {"name": "Under", "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
        ],
    }
    out = parse_player_prop(payload)
    assert len(out["props"]) == 2
    assert all(p["side"] in ("over", "under") for p in out["props"])


def test_outcome_missing_description_dropped():
    """Outcomes with empty/None description (no player name) must be dropped."""
    payload = {
        **_STD_PAYLOAD,
        "outcomes": [
            {"name": "Over",  "description": "",   "point": 27.5, "price": 1.91},
            {"name": "Under", "description": None, "point": 27.5, "price": 1.91},
        ],
    }
    out = parse_player_prop(payload)
    assert out["props"] == []


def test_outcome_with_point_but_wrong_name_dropped():
    """Outcomes with a point but name that is neither Over nor Under are dropped."""
    payload = {
        **_STD_PAYLOAD,
        "outcomes": [
            {"name": "Draw", "description": "Jayson Tatum", "point": 27.5, "price": 3.0},
            {"name": "Over", "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
        ],
    }
    out = parse_player_prop(payload)
    assert len(out["props"]) == 1
    assert out["props"][0]["side"] == "over"


def test_lineless_outcome_dropped():
    """Outcomes with no point value must be dropped."""
    payload = {
        **_STD_PAYLOAD,
        "outcomes": [
            {"name": "Over",  "description": "Jayson Tatum", "point": None, "price": 1.91},
            {"name": "Under", "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
        ],
    }
    out = parse_player_prop(payload)
    assert len(out["props"]) == 1
    assert out["props"][0]["side"] == "under"


def test_event_dict_has_five_keys_matching_parse_odds_line():
    """Event dict must have exactly the same five keys as parse_odds_line."""
    from bountygate.transforms.parsers.odds import parse_odds_line

    odds_payload = {
        "event_id": "evt-prop-1", "sport_key": "basketball_nba",
        "commence_time": "2026-06-15T19:00:00Z",
        "home_team": "Boston Celtics", "away_team": "Miami Heat",
        "market": "h2h", "bookmaker": "draftkings",
        "outcomes": [],
    }
    prop_event = parse_player_prop(_STD_PAYLOAD)["event"]
    odds_event = parse_odds_line(odds_payload)["event"]
    assert set(prop_event.keys()) == set(odds_event.keys())
    # Values also match for the shared event
    for k in odds_event:
        assert prop_event[k] == odds_event[k], f"mismatch on key {k!r}"


def test_case_insensitive_over_under():
    """Over/Under matching is case-insensitive."""
    payload = {
        **_STD_PAYLOAD,
        "outcomes": [
            {"name": "OVER",  "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
            {"name": "under", "description": "Jayson Tatum", "point": 27.5, "price": 1.91},
        ],
    }
    out = parse_player_prop(payload)
    assert len(out["props"]) == 2
    sides = {p["side"] for p in out["props"]}
    assert sides == {"over", "under"}
