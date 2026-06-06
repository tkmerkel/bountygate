from datetime import datetime, timezone

from sqlalchemy import MetaData, create_engine, select

from bountygate.connectors.base import RawRecord
from bountygate.connectors.landing import _rows_from_records, land_raw, raw_table


def _rec(key):
    return RawRecord(
        source="kalshi",
        source_key=key,
        record_type="market",
        captured_at=datetime(2026, 6, 5, 12, 0, tzinfo=timezone.utc),
        payload={"price": 0.5, "key": key},
    )


def test_rows_from_records_maps_columns():
    rows = _rows_from_records([_rec("A"), _rec("B")])
    assert [r["source_key"] for r in rows] == ["A", "B"]
    assert rows[0]["source"] == "kalshi"
    assert rows[0]["record_type"] == "market"
    assert rows[0]["payload"] == {"price": 0.5, "key": "A"}


def test_land_raw_inserts_rows_and_roundtrips_payload():
    engine = create_engine("sqlite://")
    md = MetaData()
    table = raw_table(md)
    md.create_all(engine)

    n = land_raw([_rec("A"), _rec("B")], engine=engine)
    assert n == 2

    with engine.connect() as conn:
        got = conn.execute(select(table.c.source_key, table.c.payload).order_by(table.c.source_key)).all()
    assert [g[0] for g in got] == ["A", "B"]
    assert got[0][1] == {"price": 0.5, "key": "A"}  # JSON round-trips as dict


def test_land_raw_empty_is_noop():
    engine = create_engine("sqlite://")
    md = MetaData()
    raw_table(md)
    md.create_all(engine)
    assert land_raw([], engine=engine) == 0
