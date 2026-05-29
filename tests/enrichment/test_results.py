"""Unit tests for bountygate.enrichment.results — game-result, schedule, and
injury parsers, asserted against real-API fixtures."""

from bountygate.enrichment import match, results


class TestParseEspnScoreboard:
    def test_parses_final_game(self, load_fixture):
        rows = results.parse_espn_scoreboard(
            load_fixture("espn_mlb_scoreboard.json"), "baseball_mlb"
        )
        assert rows
        # event[0]: Angels (away) 7 @ Tigers (home) 1, Final
        g = next(
            r for r in rows
            if r["home_team_name"] == "Detroit Tigers"
            and r["away_team_name"] == "Los Angeles Angels"
        )
        assert g["home_score"] == 1
        assert g["away_score"] == 7
        assert g["completed"] is True
        assert g["sport_key"] == "baseball_mlb"

    def test_every_row_has_event_fields(self, load_fixture):
        rows = results.parse_espn_scoreboard(
            load_fixture("espn_nba_scoreboard.json"), "basketball_nba"
        )
        for r in rows:
            assert set(r) >= {
                "home_team_name", "away_team_name", "home_score",
                "away_score", "completed", "sport_key", "commence_at_utc",
            }


class TestParseMlbSchedule:
    def test_parses_final_game_with_scores(self, load_fixture):
        rows = results.parse_mlb_schedule(load_fixture("mlb_schedule.json"))
        g = next(r for r in rows if r["game_pk"] == 824272)
        assert g["away_team_name"] == "Los Angeles Angels"
        assert g["home_team_name"] == "Detroit Tigers"
        assert g["away_score"] == 7
        assert g["home_score"] == 1
        assert g["final"] is True


class TestParseNhlSchedule:
    def test_builds_full_team_names_and_scores(self, load_fixture):
        rows = results.parse_nhl_schedule(load_fixture("nhl_schedule.json"))
        g = next(r for r in rows if r["game_id"] == 2025030324)
        assert g["away_team_name"] == "Colorado Avalanche"
        assert g["home_team_name"] == "Vegas Golden Knights"
        assert g["away_score"] == 1
        assert g["home_score"] == 2
        assert g["final"] is True


class TestParseEspnInjuries:
    def test_parses_injury_rows(self, load_fixture):
        rows = results.parse_espn_injuries(
            load_fixture("espn_mlb_injuries.json"), "baseball_mlb"
        )
        assert rows
        arenado = next(r for r in rows if r["player_name"] == "Nolan Arenado")
        assert arenado["status"] == "Day-To-Day"
        assert arenado["team_name"] == "Arizona Diamondbacks"
        assert arenado["sport_key"] == "baseball_mlb"
        assert arenado["source"] == "espn"
        # player_id must follow the dim-model contract
        assert arenado["player_id"] == match.player_id_for_name(
            "baseball_mlb", "Nolan Arenado"
        )

    def test_description_includes_injury_type(self, load_fixture):
        rows = results.parse_espn_injuries(
            load_fixture("espn_mlb_injuries.json"), "baseball_mlb"
        )
        arenado = next(r for r in rows if r["player_name"] == "Nolan Arenado")
        assert "Groin" in (arenado["description"] or "")
