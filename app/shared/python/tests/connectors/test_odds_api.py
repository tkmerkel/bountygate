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
