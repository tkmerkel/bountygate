"""Unit tests for bountygate.enrichment.match.

The player-id logic here MUST stay byte-for-byte identical to
bg_dimensional_model._normalize_player_name / _player_id so that box-score
stats join straight to dim_player.
"""

import uuid

from bountygate.enrichment import match


class TestNormalizePlayerName:
    def test_basic_lower_strip(self):
        assert match.normalize_player_name("  Aaron   Judge  ") == "aaron judge"

    def test_strips_punctuation_not_in_alnum_space(self):
        # matches dim model: _PUNCT_RE = [^a-z0-9 ]+ removes accents & periods
        assert match.normalize_player_name("Ronald Acuña Jr.") == "ronald acua jr"

    def test_none_returns_empty(self):
        assert match.normalize_player_name(None) == ""

    def test_collapses_internal_spaces(self):
        assert match.normalize_player_name("Mike   Trout") == "mike trout"

    def test_non_space_whitespace_is_stripped_like_dim_model(self):
        # dim model contract: _PUNCT_RE=[^a-z0-9 ]+ preserves only a literal
        # space, so tabs/newlines are removed (not collapsed to a space).
        assert match.normalize_player_name("Mike\tTrout\n") == "miketrout"


class TestPlayerId:
    def test_deterministic_uuid5_matches_dim_model_contract(self):
        expected = uuid.uuid5(uuid.NAMESPACE_URL, "baseball_mlb|aaron judge").hex
        assert match.player_id("baseball_mlb", "aaron judge") == expected

    def test_normalizes_before_hashing(self):
        # player_id should accept a raw name and normalize it identically
        from_raw = match.player_id_for_name("baseball_mlb", "Aaron Judge")
        from_norm = match.player_id("baseball_mlb", "aaron judge")
        assert from_raw == from_norm


class TestNormalizeTeamName:
    def test_lower_strip(self):
        assert match.normalize_team_name("  New York Yankees ") == "new york yankees"

    def test_strips_punctuation(self):
        assert match.normalize_team_name("St. Louis Cardinals") == "st louis cardinals"


class TestMatchGameToEvent:
    def _events(self):
        return [
            {
                "bg_event_id": "evt_yanks_redsox",
                "sport_key": "baseball_mlb",
                "home_team_name": "Boston Red Sox",
                "away_team_name": "New York Yankees",
                "commence_at_utc": "2026-05-28T23:10:00+00:00",
            },
            {
                "bg_event_id": "evt_dodgers_pads",
                "sport_key": "baseball_mlb",
                "home_team_name": "Los Angeles Dodgers",
                "away_team_name": "San Diego Padres",
                "commence_at_utc": "2026-05-29T02:10:00+00:00",
            },
        ]

    def test_exact_home_away_same_day(self):
        got = match.match_game_to_event(
            "baseball_mlb", "Boston Red Sox", "New York Yankees",
            "2026-05-28", self._events(),
        )
        assert got == "evt_yanks_redsox"

    def test_swapped_home_away_still_matches(self):
        # source feed may list teams in the opposite order
        got = match.match_game_to_event(
            "baseball_mlb", "New York Yankees", "Boston Red Sox",
            "2026-05-28", self._events(),
        )
        assert got == "evt_yanks_redsox"

    def test_matches_within_one_day_tolerance(self):
        # event commences 2026-05-29 UTC; a feed dating it 2026-05-28 (local) still matches
        got = match.match_game_to_event(
            "baseball_mlb", "Los Angeles Dodgers", "San Diego Padres",
            "2026-05-28", self._events(),
        )
        assert got == "evt_dodgers_pads"

    def test_wrong_sport_no_match(self):
        got = match.match_game_to_event(
            "icehockey_nhl", "Boston Red Sox", "New York Yankees",
            "2026-05-28", self._events(),
        )
        assert got is None

    def test_unknown_teams_no_match(self):
        got = match.match_game_to_event(
            "baseball_mlb", "Chicago Cubs", "Miami Marlins",
            "2026-05-28", self._events(),
        )
        assert got is None

    def test_far_date_no_match(self):
        got = match.match_game_to_event(
            "baseball_mlb", "Boston Red Sox", "New York Yankees",
            "2026-06-15", self._events(),
        )
        assert got is None
