"""Unit tests for bountygate.enrichment.statmap — prop settlement + box-score
stat extraction, asserted against real-API fixtures.

stat_key uses the BASE market_key vocabulary (e.g. batter_hits,
pitcher_strikeouts, player_goals) so fact_player_stat_result.stat_key joins to
the base_market_key of an EV pick in the settlement mart.
"""

from bountygate.enrichment import match, statmap


class TestSettle:
    def test_over_wins_when_value_above_line(self):
        assert statmap.settle(2, "over", 1.5) == "win"

    def test_over_loses_when_value_below_line(self):
        assert statmap.settle(1, "over", 1.5) == "loss"

    def test_over_pushes_on_whole_line(self):
        assert statmap.settle(2, "over", 2.0) == "push"

    def test_under_wins_when_value_below_line(self):
        assert statmap.settle(1, "under", 1.5) == "win"

    def test_under_loses_when_value_above_line(self):
        assert statmap.settle(3, "under", 1.5) == "loss"

    def test_under_pushes_on_whole_line(self):
        assert statmap.settle(2, "under", 2.0) == "push"

    def test_missing_value_is_unknown(self):
        assert statmap.settle(None, "over", 1.5) == "unknown"

    def test_missing_line_is_unknown(self):
        assert statmap.settle(2, "over", None) == "unknown"

    def test_unknown_side_is_unknown(self):
        assert statmap.settle(2, "yes", 1.5) == "unknown"


class TestExtractMlb:
    def test_returns_rows_with_canonical_player_id(self, load_fixture):
        box = load_fixture("mlb_boxscore.json")
        rows = statmap.extract_mlb_player_stats(box)
        assert rows, "expected stat rows from MLB boxscore"
        sample = rows[0]
        assert set(sample) >= {
            "player_name", "player_id", "sport_key", "stat_key", "stat_value",
        }
        # player_id must match the dim-model contract for joinability
        assert sample["player_id"] == match.player_id_for_name(
            "baseball_mlb", sample["player_name"]
        )

    def _stat(self, rows, name, stat_key):
        for r in rows:
            if r["player_name"] == name and r["stat_key"] == stat_key:
                return r["stat_value"]
        return None

    def test_batter_stats_from_fixture(self, load_fixture):
        rows = statmap.extract_mlb_player_stats(load_fixture("mlb_boxscore.json"))
        # Adam Frazier: 0-2 with 2 K (per fixture summary)
        assert self._stat(rows, "Adam Frazier", "batter_hits") == 0
        assert self._stat(rows, "Adam Frazier", "batter_strikeouts") == 2
        assert self._stat(rows, "Adam Frazier", "batter_total_bases") == 0

    def test_pitcher_stats_from_fixture(self, load_fixture):
        rows = statmap.extract_mlb_player_stats(load_fixture("mlb_boxscore.json"))
        # Grayson Rodriguez: 5.0 IP, ER, 5 K, 2 BB, W (per fixture summary)
        assert self._stat(rows, "Grayson Rodriguez", "pitcher_strikeouts") == 5
        assert self._stat(rows, "Grayson Rodriguez", "pitcher_walks") == 2
        assert self._stat(rows, "Grayson Rodriguez", "pitcher_earned_runs") == 1
        assert self._stat(rows, "Grayson Rodriguez", "pitcher_record_a_win") == 1

    def test_derived_singles(self, load_fixture):
        rows = statmap.extract_mlb_player_stats(load_fixture("mlb_boxscore.json"))
        # singles = hits - doubles - triples - home_runs; never negative
        for r in rows:
            if r["stat_key"] == "batter_singles":
                assert r["stat_value"] >= 0


class TestExtractNhl:
    def test_returns_skater_stats(self, load_fixture):
        box = load_fixture("nhl_boxscore.json")
        rows = statmap.extract_nhl_player_stats(box)
        assert rows, "expected stat rows from NHL boxscore"
        keys = {r["stat_key"] for r in rows}
        assert "player_goals" in keys
        assert "player_assists" in keys
        assert "player_shots_on_goal" in keys

    def test_rows_carry_sport_and_player_id(self, load_fixture):
        rows = statmap.extract_nhl_player_stats(load_fixture("nhl_boxscore.json"))
        sample = rows[0]
        assert sample["sport_key"] == "icehockey_nhl"
        assert sample["player_id"] == match.player_id_for_name(
            "icehockey_nhl", sample["player_name"]
        )
