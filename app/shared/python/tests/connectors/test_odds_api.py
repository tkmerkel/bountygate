import json
from datetime import datetime, timezone
from pathlib import Path

from bountygate.connectors.odds_api import OddsApiConnector

FIX = Path(__file__).parent.parent / "fixtures" / "connectors" / "odds_event.json"


def test_normalize_event_emits_one_record_per_book_market():
    raw = json.loads(FIX.read_text())
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)

    records = OddsApiConnector.normalize_event(raw, captured_at=ts)

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
