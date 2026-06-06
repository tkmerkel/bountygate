import json
from datetime import datetime, timezone
from pathlib import Path

from bountygate.connectors.kalshi import KalshiConnector

FIX = Path(__file__).parent.parent / "fixtures" / "connectors" / "kalshi_events.json"


def test_normalize_emits_market_rawrecords():
    raw = json.loads(FIX.read_text())
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)

    records = KalshiConnector.normalize(raw, series_ticker="KXNFLGAME", captured_at=ts)

    assert len(records) == 1
    r = records[0]
    assert r.source == "kalshi"
    assert r.source_key == "KXNFLGAME-26SEP07DALNYG-DAL"
    assert r.record_type == "market"
    assert r.captured_at == ts
    assert r.payload["yes_bid"] == 0.52
    assert r.payload["no_ask"] == 0.48
    assert r.payload["series_ticker"] == "KXNFLGAME"
    assert r.payload["open_interest"] == 1234.0
