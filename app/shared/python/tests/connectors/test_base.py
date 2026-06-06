from datetime import datetime, timezone

import pytest

from bountygate.connectors.base import Connector, RawRecord


def test_rawrecord_holds_fields():
    r = RawRecord(
        source="kalshi",
        source_key="KX-1",
        record_type="market",
        captured_at=datetime(2026, 6, 5, tzinfo=timezone.utc),
        payload={"a": 1},
    )
    assert r.source == "kalshi"
    assert r.source_key == "KX-1"
    assert r.record_type == "market"
    assert r.payload["a"] == 1


def test_connector_is_abstract():
    with pytest.raises(TypeError):
        Connector()  # abstract: fetch_snapshots not implemented
