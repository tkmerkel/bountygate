"""Unit tests for bountygate.enrichment.clients — URL builders (the testable,
network-free part of the HTTP layer)."""

from datetime import date

import pytest

from bountygate.enrichment import clients


class TestEspnUrls:
    def test_scoreboard_url(self):
        url = clients.build_espn_scoreboard_url("baseball_mlb", date(2026, 5, 28))
        assert "baseball/mlb/scoreboard" in url
        assert "dates=20260528" in url

    def test_injuries_url(self):
        url = clients.build_espn_injuries_url("icehockey_nhl")
        assert "hockey/nhl/injuries" in url

    def test_unknown_sport_raises(self):
        with pytest.raises(KeyError):
            clients.build_espn_scoreboard_url("quidditch_pro", date(2026, 5, 28))


class TestMlbUrls:
    def test_schedule_url(self):
        url = clients.build_mlb_schedule_url(date(2026, 5, 28))
        assert "statsapi.mlb.com" in url
        assert "sportId=1" in url
        assert "date=2026-05-28" in url

    def test_boxscore_url(self):
        url = clients.build_mlb_boxscore_url(824272)
        assert "game/824272/boxscore" in url


class TestNhlUrls:
    def test_schedule_url(self):
        url = clients.build_nhl_schedule_url("2026-05-28")
        assert "api-web.nhle.com/v1/schedule/2026-05-28" in url

    def test_boxscore_url(self):
        url = clients.build_nhl_boxscore_url(2025030324)
        assert "gamecenter/2025030324/boxscore" in url
