from bountygate.transforms.matching.aliases import (
    canonical_team, load_aliases, sport_for_odds, sport_for_series, teams_in_title,
)


def test_roster_counts():
    a = load_aliases()
    assert len(a["NFL"]) == 32
    assert len(a["NBA"]) == 30
    assert len(a["MLB"]) == 30


def test_canonical_resolves_full_name_abbrev_nickname():
    assert canonical_team("Dallas Cowboys", "NFL") == "DAL"
    assert canonical_team("DAL", "NFL") == "DAL"
    assert canonical_team("Cowboys", "NFL") == "DAL"
    assert canonical_team("Los Angeles Dodgers", "MLB") == "LAD"
    assert canonical_team("LAD", "MLB") == "LAD"


def test_canonical_miss_returns_none():
    assert canonical_team("Toronto Maple Leafs", "NFL") is None
    assert canonical_team("", "NBA") is None


def test_sport_lookups():
    assert sport_for_series("KXNFLGAME") == "NFL"
    assert sport_for_odds("baseball_mlb") == "MLB"
    assert sport_for_series("KXNHLGAME") is None


def test_teams_in_title_first_mention_order():
    res = teams_in_title("Will the Cowboys beat the Giants?")
    assert res["NFL"] == ["DAL", "NYG"]


def test_teams_in_title_single_team_only():
    res = teams_in_title("Will the Celtics three-peat?")
    assert res["NBA"] == ["BOS"]
