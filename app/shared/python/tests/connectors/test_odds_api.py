import json
from datetime import datetime, timezone
from pathlib import Path

from bountygate.connectors.odds_api import OddsApiConnector, _window_params

FIX = Path(__file__).parent.parent / "fixtures" / "connectors" / "odds_event.json"
FIX_PROPS = Path(__file__).parent.parent / "fixtures" / "connectors" / "odds_event_props.json"


def test_normalize_event_emits_one_record_per_book_market():
    raw = json.loads(FIX.read_text())
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)

    records = OddsApiConnector().normalize_event(raw, captured_at=ts)

    assert len(records) == 1
    r = records[0]
    assert r.source == "the_odds_api"
    assert r.source_key == "evt_123:h2h:fanduel"
    assert r.record_type == "odds_line"
    assert r.payload["bookmaker"] == "fanduel"
    assert r.payload["market"] == "h2h"
    assert r.payload["home_team"] == "Boston Celtics"
    assert r.payload["outcomes"] == [
        {"name": "Boston Celtics", "price": 1.8},
        {"name": "New York Knicks", "price": 2.1},
    ]


import pytest
import requests


class _FakeResp:
    def __init__(self, status_code):
        self.status_code = status_code


class _FakeSession:
    def __init__(self, exc):
        self._exc = exc

    def get(self, *a, **k):
        raise self._exc


def _http_error(status):
    e = requests.HTTPError(f"{status} error")
    e.response = _FakeResp(status)
    return e


def test_fetch_snapshots_raises_on_auth_error(monkeypatch):
    conn = OddsApiConnector(sport_keys={"NBA": "basketball_nba"})
    monkeypatch.setattr("bountygate.connectors.odds_api.requests.Session", lambda: _FakeSession(_http_error(401)))
    with pytest.raises(requests.HTTPError):
        conn.fetch_snapshots()


def test_fetch_snapshots_degrades_on_transient_error(monkeypatch):
    conn = OddsApiConnector(sport_keys={"NBA": "basketball_nba"})
    monkeypatch.setattr("bountygate.connectors.odds_api.requests.Session", lambda: _FakeSession(_http_error(500)))
    assert conn.fetch_snapshots() == []  # non-auth error degrades to empty, no raise


# ---------------------------------------------------------------------------
# Props-mode normalize_event: record_type + outcomes pass-through
# ---------------------------------------------------------------------------

def test_normalize_event_props_stamps_player_prop_and_passes_outcomes():
    raw = json.loads(FIX_PROPS.read_text())
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)

    conn = OddsApiConnector(record_type="player_prop")
    records = conn.normalize_event(raw, captured_at=ts)

    assert len(records) == 1
    r = records[0]
    assert r.record_type == "player_prop"
    assert r.source == "the_odds_api"
    assert r.source_key == "evt_999:player_points:fanduel"
    assert r.payload["market"] == "player_points"
    # Outcomes pass through whole: description + point intact.
    assert r.payload["outcomes"] == [
        {"name": "Over", "description": "LeBron James", "price": 1.9, "point": 28.5},
        {"name": "Under", "description": "LeBron James", "price": 1.9, "point": 28.5},
    ]


def test_normalize_event_default_record_type_is_odds_line():
    raw = json.loads(FIX.read_text())
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)

    records = OddsApiConnector().normalize_event(raw, captured_at=ts)

    assert records[0].record_type == "odds_line"


# ---------------------------------------------------------------------------
# _window_params: exact YYYY-MM-DDTHH:MM:SSZ formatting, to = from + window
# ---------------------------------------------------------------------------

def test_window_params_formatting_and_span():
    now = datetime(2026, 6, 12, 9, 30, 15, 123456, tzinfo=timezone.utc)
    params = _window_params(24, now)

    # Exact format: no microseconds, no offset, literal trailing Z.
    assert params["commenceTimeFrom"] == "2026-06-12T09:30:15Z"
    assert params["commenceTimeTo"] == "2026-06-13T09:30:15Z"


def test_window_params_naive_now_treated_as_utc():
    now = datetime(2026, 6, 12, 0, 0, 0)  # naive
    params = _window_params(48, now)
    assert params["commenceTimeFrom"] == "2026-06-12T00:00:00Z"
    assert params["commenceTimeTo"] == "2026-06-14T00:00:00Z"


# ---------------------------------------------------------------------------
# Per-sport market selection + fetch_snapshots sport skipping (no HTTP)
# ---------------------------------------------------------------------------

def test_event_odds_uses_joined_markets_for_present_sport(monkeypatch):
    by_sport = {"basketball_nba": ("player_points", "player_assists")}
    conn = OddsApiConnector(markets_by_sport=by_sport)

    captured = {}

    class _RecordingResp:
        headers = {}

        def raise_for_status(self):
            pass

        def json(self):
            return {"id": "evt_1", "sport_key": "basketball_nba", "bookmakers": []}

    class _RecordingSession:
        def get(self, url, params=None, timeout=None):
            captured["params"] = params
            return _RecordingResp()

    conn._event_odds(_RecordingSession(), "basketball_nba", "evt_1")
    assert captured["params"]["markets"] == "player_points,player_assists"


def test_fetch_snapshots_skips_sports_absent_from_markets_by_sport(monkeypatch):
    # Two sports in sport_keys; only one has markets_by_sport coverage.
    conn = OddsApiConnector(
        sport_keys={"NBA": "basketball_nba", "MLB": "baseball_mlb"},
        markets_by_sport={"basketball_nba": ("player_points",)},
    )

    listed = []
    odds_calls = []

    def fake_list(session, sport_key):
        listed.append(sport_key)
        return [{"id": "evt_1"}]

    def fake_odds(session, sport_key, event_id):
        odds_calls.append(sport_key)
        return {"id": event_id, "sport_key": sport_key, "bookmakers": []}

    monkeypatch.setattr(conn, "_list_events", fake_list)
    monkeypatch.setattr(conn, "_event_odds", fake_odds)

    conn.fetch_snapshots()

    # baseball_mlb has no markets_by_sport entry -> skipped, no /events call.
    assert listed == ["basketball_nba"]
    assert odds_calls == ["basketball_nba"]
