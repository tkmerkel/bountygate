from __future__ import annotations

from typing import Iterable, Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    Integer,
    MetaData,
    Table,
    Text,
    TIMESTAMP,
    func,
    insert,
)
from sqlalchemy.engine import Engine

from bountygate.connectors.base import RawRecord

RAW_TABLE = "raw_market_snapshots"


def raw_table(metadata: MetaData) -> Table:
    """Canonical Table definition for the raw landing table. Uses the generic
    JSON type so the same definition works on Postgres (JSONB-compatible) and
    SQLite (tests). The migration in db/migrations declares the column as JSONB."""
    return Table(
        RAW_TABLE,
        metadata,
        Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
        Column("source", Text, nullable=False),
        Column("source_key", Text, nullable=False),
        Column("record_type", Text, nullable=False),
        Column("captured_at", TIMESTAMP(timezone=True), nullable=False),
        Column("payload", JSON, nullable=False),
        Column("ingested_at", TIMESTAMP(timezone=True), server_default=func.now()),
    )


def _rows_from_records(records: Iterable[RawRecord]) -> list[dict]:
    return [
        {
            "source": r.source,
            "source_key": r.source_key,
            "record_type": r.record_type,
            "captured_at": r.captured_at,
            "payload": r.payload,
        }
        for r in records
    ]


def land_raw(records: Iterable[RawRecord], engine: Optional[Engine] = None) -> int:
    """Bulk-insert RawRecords into raw_market_snapshots. Returns rows written.
    Source-agnostic: every connector emits RawRecords, this is the only writer."""
    rows = _rows_from_records(records)
    if not rows:
        return 0
    own_engine = False
    if engine is None:
        from sqlalchemy import create_engine
        from bountygate.utils import db_connection as dbc

        engine = create_engine(dbc.DATABASE_URL)
        own_engine = True
    try:
        md = MetaData()
        table = raw_table(md)
        with engine.begin() as conn:
            conn.execute(insert(table), rows)
        return len(rows)
    finally:
        if own_engine:
            engine.dispose()
