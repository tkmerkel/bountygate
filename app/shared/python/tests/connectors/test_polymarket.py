import json
from datetime import datetime, timezone
from pathlib import Path

from bountygate.connectors.polymarket import PolymarketConnector

FIX = Path(__file__).parent.parent / "fixtures" / "connectors" / "polymarket_markets.json"


def test_normalize_emits_market_rawrecords_with_parsed_outcomes():
    raw = json.loads(FIX.read_text())
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)

    records = PolymarketConnector.normalize(raw, captured_at=ts)

    assert len(records) == 1
    r = records[0]
    assert r.source == "polymarket"
    assert r.source_key == "0xabc123"
    assert r.record_type == "market"
    assert r.payload["outcomes"] == ["Yes", "No"]
    assert r.payload["outcome_prices"] == [0.42, 0.58]
    assert r.payload["volume"] == 12345.67
    assert r.payload["question"].startswith("Will it rain")


def test_normalize_skips_markets_without_condition_id():
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)
    assert PolymarketConnector.normalize([{"question": "no id"}], captured_at=ts) == []
