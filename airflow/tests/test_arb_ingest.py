import json
from datetime import datetime, timezone
from pathlib import Path

from bg_arb_pipeline_lib.ingest import normalize_odds_response

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class TestNormalizeOddsResponse:
    def test_std_only_produces_four_rows(self):
        rows = normalize_odds_response(
            _load("the_odds_api_std_only.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        assert len(rows) == 4

    def test_std_only_row_shape(self):
        rows = normalize_odds_response(
            _load("the_odds_api_std_only.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        fd_under = next(
            r for r in rows
            if r["bookmaker_key"] == "fanduel" and r["side"] == "under"
        )
        assert fd_under["event_id"] == "evt_test_001"
        assert fd_under["sport_title"] == "NBA"
        assert fd_under["player_name"] == "Dennis Schroder"
        assert fd_under["market_key"] == "player_assists"
        assert fd_under["line"] == 2.5
        assert fd_under["price"] == 2.00
        assert fd_under["home_team"] == "Cleveland Cavaliers"
        assert fd_under["away_team"] == "Detroit Pistons"

    def test_side_is_lowercase(self):
        rows = normalize_odds_response(
            _load("the_odds_api_std_only.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        for row in rows:
            assert row["side"] in ("under", "over")

    def test_alt_rich_produces_nine_rows(self):
        rows = normalize_odds_response(
            _load("the_odds_api_alt_rich.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        assert len(rows) == 9

    def test_alt_market_keys_preserved(self):
        rows = normalize_odds_response(
            _load("the_odds_api_alt_rich.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        alt_rows = [r for r in rows if r["market_key"].endswith("_alternate")]
        assert len(alt_rows) == 5

    def test_fanduel_alt_under_captured(self):
        rows = normalize_odds_response(
            _load("the_odds_api_alt_rich.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        fd_alt_under = [
            r for r in rows
            if r["bookmaker_key"] == "fanduel"
               and r["market_key"] == "player_assists_alternate"
               and r["side"] == "under"
        ]
        assert len(fd_alt_under) == 2

    def test_fetched_at_utc_stamped_consistently(self):
        ts = datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc)
        rows = normalize_odds_response(
            _load("the_odds_api_std_only.json"),
            fetched_at_utc=ts,
        )
        for row in rows:
            assert row["fetched_at_utc"] == ts
