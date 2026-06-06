"""Fast bulk DB helpers for the arb pipeline.

The shared bountygate.utils.db_connection.insert_data uses row-by-row
SQLAlchemy executemany, which is fine for 100s of rows but takes minutes
for 1000s. The arb stage table is 7000+ rows per run. Use Postgres COPY
(via psycopg2 cursor.copy_expert) which is 50-100x faster.
"""
from __future__ import annotations

import io
import logging
import os
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text

LOGGER = logging.getLogger(__name__)


def _resolve_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        # Match bountygate.utils.db_connection's module-level fallback.
        from bountygate.utils.db_connection import DATABASE_URL as _DB
        url = _DB
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return url


# Connection args passed to every engine:
#   - keepalives_* tells the kernel to send TCP keepalives every 30s; after 3
#     missed keepalives RDS notices the peer is gone and releases locks /
#     idle-in-transaction state. Default tcp_keepalive_time is 2hr, way too
#     long for an interactive pipeline.
#   - statement_timeout caps any single SQL statement at 5min so a stuck
#     TRUNCATE or COPY can't hold a lock forever.
#   - lock_timeout fails fast (60s) if we can't acquire the table lock, so
#     a stuck prior run doesn't make the current run hang indefinitely.
_CONNECT_ARGS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
    "options": "-c statement_timeout=300000 -c lock_timeout=60000",
}


def _make_engine():
    return create_engine(_resolve_database_url(), connect_args=_CONNECT_ARGS)


def _df_to_csv_buffer(df: pd.DataFrame, columns: Iterable[str]) -> io.StringIO:
    """Serialize DataFrame to a CSV in-memory buffer for COPY.

    Datetimes are normalized to UTC and stripped of tz info so they fit the
    `timestamp without time zone` columns in our schema. The bot's universe
    is UTC-only; the schema decision predates this code.
    """
    out = df[list(columns)].copy()
    # Force-convert any tz-aware datetime columns to naive UTC.
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            series = pd.to_datetime(out[col], utc=True, errors="coerce")
            out[col] = series.dt.tz_localize(None)
    buf = io.StringIO()
    out.to_csv(
        buf,
        index=False,
        header=False,
        na_rep="",
        date_format="%Y-%m-%d %H:%M:%S",
    )
    buf.seek(0)
    return buf


def bulk_replace(df: pd.DataFrame, table_name: str) -> int:
    """TRUNCATE the table, then bulk-load df via Postgres COPY.

    Returns the number of rows written.

    Pre-conditions:
      - The table already exists (we don't CREATE it here — that's a migration).
      - df.columns is a subset of the table's columns.
    """
    if df is None or df.empty:
        engine = _make_engine()
        try:
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        finally:
            engine.dispose()
        return 0

    columns = list(df.columns)
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    copy_sql = (
        f'COPY {table_name} ({cols_sql}) '
        f"FROM STDIN WITH (FORMAT CSV, NULL '')"
    )

    engine = _make_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            # Use the raw psycopg2 cursor for COPY — SQLAlchemy doesn't wrap it.
            raw = conn.connection.connection  # SQLAlchemy -> DBAPI conn
            with raw.cursor() as cur:
                buf = _df_to_csv_buffer(df, columns)
                cur.copy_expert(copy_sql, buf)
        LOGGER.info("[bulk_replace] %s: wrote %d rows via COPY", table_name, len(df))
        return len(df)
    finally:
        engine.dispose()


def bulk_append_new(df: pd.DataFrame, table_name: str, key_column: str) -> int:
    """Append rows whose key_column value isn't already in the table.

    Used by the history task. ON CONFLICT DO NOTHING via a staging round-trip:
      1. COPY df into a temp table.
      2. INSERT into target SELECT FROM temp WHERE NOT EXISTS in target.

    Returns the number of rows actually appended.
    """
    if df is None or df.empty:
        return 0

    columns = list(df.columns)
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    tmp_table = f"_tmp_{table_name}_stage"

    engine = _make_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {tmp_table}"))
            conn.execute(text(
                f"CREATE TEMP TABLE {tmp_table} (LIKE {table_name} INCLUDING DEFAULTS) "
                f"ON COMMIT DROP"
            ))
            raw = conn.connection.connection
            with raw.cursor() as cur:
                buf = _df_to_csv_buffer(df, columns)
                cur.copy_expert(
                    f'COPY {tmp_table} ({cols_sql}) '
                    f"FROM STDIN WITH (FORMAT CSV, NULL '')",
                    buf,
                )
            result = conn.execute(text(
                f'INSERT INTO {table_name} ({cols_sql}) '
                f'SELECT {cols_sql} FROM {tmp_table} '
                f'WHERE NOT EXISTS ('
                f'  SELECT 1 FROM {table_name} t '
                f'  WHERE t.{key_column} = {tmp_table}.{key_column}'
                f')'
            ))
            inserted = result.rowcount or 0
        LOGGER.info("[bulk_append_new] %s: appended %d new rows", table_name, inserted)
        return inserted
    finally:
        engine.dispose()
