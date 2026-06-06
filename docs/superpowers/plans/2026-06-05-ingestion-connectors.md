# Ingestion Connectors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build read-only GET connectors for Kalshi, Polymarket, and The Odds API behind a uniform interface, land their data into a single append-only `raw_market_snapshots` table, and run them on a 5-minute Airflow (3.2 + triggerer) heartbeat.

**Architecture:** A `bountygate.connectors` package in the shared lib. Each connector splits **I/O** (`fetch_snapshots`) from a **pure** `normalize(...)` staticmethod that emits uniform `RawRecord`s — so normalization is fixture-tested with no network. `land_raw()` is the single source-agnostic writer. One TaskFlow DAG per source emits a `raw_market_snapshots` dataset asset. The obsolete arb-era migrations are archived so the fresh DB doesn't resurrect the old schema.

**Tech Stack:** Python 3.12, SQLAlchemy Core, `requests`, `kalshi_python_sync` (runtime only), Apache Airflow 3.2, Docker Compose, pytest.

**Spec:** `docs/superpowers/specs/2026-06-05-ingestion-connectors-design.md`

---

## File Structure

| Path | Responsibility | Status |
|---|---|---|
| `app/shared/python/bountygate/connectors/__init__.py` | Package exports | Create |
| `app/shared/python/bountygate/connectors/base.py` | `RawRecord` dataclass + `Connector` ABC | Create |
| `app/shared/python/bountygate/connectors/landing.py` | `raw_table()`, `_rows_from_records()`, `land_raw()` | Create |
| `app/shared/python/bountygate/connectors/kalshi.py` | `KalshiConnector` (read-only) | Create |
| `app/shared/python/bountygate/connectors/polymarket.py` | `PolymarketConnector` | Create |
| `app/shared/python/bountygate/connectors/odds_api.py` | `OddsApiConnector` | Create |
| `app/shared/python/bountygate/connectors/registry.py` | `CONNECTORS` dict | Create |
| `app/shared/python/tests/conftest.py` | put `bountygate` on `sys.path` for tests | Create |
| `app/shared/python/tests/connectors/test_*.py` | unit tests | Create |
| `app/shared/python/tests/fixtures/connectors/*.json` | recorded API samples | Create |
| `db/migrations/_archive_pre_pivot/` | obsolete 001–010 arb-schema migrations | Move |
| `db/migrations/001_raw_market_snapshots.sql` | fresh-baseline raw table | Create |
| `airflow/dags/ingest_kalshi.py`,`ingest_polymarket.py`,`ingest_odds.py` | poller DAGs | Create |
| `airflow/tests/test_ingest_dags_import.py` | DAG-import smoke | Create |
| `airflow/Dockerfile`,`airflow/requirements.txt`,`airflow/docker-compose.yml` | 3.2 + triggerer | Modify |
| repo `.env`, `private_key.pem` | Kalshi creds (gitignored) | Modify/Create |

**Test command (connectors):** `cd app/shared/python && python -m pytest tests/connectors -v`

---

## Task 1: Connector base — `RawRecord` + `Connector` ABC

**Files:**
- Create: `app/shared/python/bountygate/connectors/__init__.py`, `base.py`
- Create: `app/shared/python/tests/conftest.py`, `app/shared/python/tests/connectors/test_base.py`

- [ ] **Step 1: Add the test conftest** (so `import bountygate` resolves without a global install)

Create `app/shared/python/tests/conftest.py`:

```python
import os
import sys

# app/shared/python (the dir that contains the `bountygate` package)
_PKG_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)
```

- [ ] **Step 2: Write the failing test**

Create `app/shared/python/tests/connectors/test_base.py`:

```python
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_base.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.connectors'`.

- [ ] **Step 4: Create the package + base module**

Create `app/shared/python/bountygate/connectors/__init__.py`:

```python
from bountygate.connectors.base import Connector, RawRecord

__all__ = ["Connector", "RawRecord"]
```

Create `app/shared/python/bountygate/connectors/base.py`:

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class RawRecord:
    """A single normalized snapshot from a source, ready for raw landing."""

    source: str          # 'kalshi' | 'polymarket' | 'the_odds_api'
    source_key: str      # natural id: ticker / condition_id / f"{event_id}:{market}:{book}"
    record_type: str     # 'market' | 'orderbook' | 'odds_line'
    captured_at: datetime  # fetch time, UTC, tz-aware
    payload: dict[str, Any]


class Connector(ABC):
    """Uniform read-only source interface. Subclasses set `source` and implement
    `fetch_snapshots`. Keep network I/O in `fetch_snapshots`; keep parsing in a
    pure `normalize(...)` staticmethod so it can be fixture-tested."""

    source: str = ""

    @abstractmethod
    def fetch_snapshots(self) -> list[RawRecord]:
        """Fetch current data from the source and return normalized RawRecords."""
        raise NotImplementedError
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_base.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/connectors/__init__.py app/shared/python/bountygate/connectors/base.py app/shared/python/tests/conftest.py app/shared/python/tests/connectors/test_base.py
git commit -m "feat(connectors): RawRecord + Connector ABC"
```

---

## Task 2: Raw landing writer — `land_raw()`

**Files:**
- Create: `app/shared/python/bountygate/connectors/landing.py`
- Create: `app/shared/python/tests/connectors/test_landing.py`

- [ ] **Step 1: Write the failing test** (uses in-memory SQLite — no Postgres needed)

Create `app/shared/python/tests/connectors/test_landing.py`:

```python
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_landing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.connectors.landing'`.

- [ ] **Step 3: Write `landing.py`**

Create `app/shared/python/bountygate/connectors/landing.py`:

```python
from __future__ import annotations

from typing import Iterable, Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
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
        Column("id", BigInteger, primary_key=True, autoincrement=True),
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_landing.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/connectors/landing.py app/shared/python/tests/connectors/test_landing.py
git commit -m "feat(connectors): land_raw() source-agnostic raw writer"
```

---

## Task 3: Fresh migration baseline + raw table

**Context:** The decommission reset dropped `schema_migrations`, so `migrate.py up` would re-run the obsolete arb-era migrations 001–010 and resurrect the old schema. Archive them (they live in tag `arb-execution-final`) and make the raw table the new baseline `001`.

**Files:**
- Move: `db/migrations/001..010*.sql` → `db/migrations/_archive_pre_pivot/`
- Create: `db/migrations/001_raw_market_snapshots.sql`

- [ ] **Step 1: Archive the obsolete migrations**

```bash
cd /c/Users/tkmer/bountygate
mkdir -p db/migrations/_archive_pre_pivot
git mv db/migrations/001_add_commence_at_utc.sql db/migrations/002_team_reference_and_aliases.sql db/migrations/003_bg_unified_lines_normalized_view.sql db/migrations/004_dq_metrics.sql db/migrations/005_bg_arb_pipeline_tables.sql db/migrations/006_dashboard_state.sql db/migrations/007_bg_game_arb_pipeline_tables.sql db/migrations/008_dim_tables.sql db/migrations/009_fact_odds_snapshot.sql db/migrations/010_fact_closing_line_and_results.sql db/migrations/_archive_pre_pivot/
echo "remaining top-level migrations:"; ls db/migrations/*.sql 2>/dev/null || echo "(none yet)"
```

- [ ] **Step 2: Create the new baseline migration**

Create `db/migrations/001_raw_market_snapshots.sql`:

```sql
-- Fresh baseline for the analytics-aggregator pivot.
-- Append-only raw landing for all marketplace connectors (Kalshi/Polymarket/Odds).
CREATE TABLE IF NOT EXISTS raw_market_snapshots (
  id           bigserial   PRIMARY KEY,
  source       text        NOT NULL,   -- 'kalshi' | 'polymarket' | 'the_odds_api'
  source_key   text        NOT NULL,   -- ticker / condition_id / event+market+book
  record_type  text        NOT NULL,   -- 'market' | 'orderbook' | 'odds_line'
  captured_at  timestamptz NOT NULL,
  payload      jsonb       NOT NULL,
  ingested_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_snap_source_time ON raw_market_snapshots (source, captured_at);
CREATE INDEX IF NOT EXISTS ix_raw_snap_source_key  ON raw_market_snapshots (source, source_key, captured_at);
```

- [ ] **Step 3: Apply the migration to the (empty) database**

```bash
cd /c/Users/tkmer/bountygate
python scripts/migrate.py status
python scripts/migrate.py up
```
Expected: `status` lists only `001_raw_market_snapshots PENDING`; `up` prints `Applied 001_raw_market_snapshots`.

- [ ] **Step 4: Verify the table exists** (Heroku CLI's pg plugin is broken — use Docker, per the project memory)

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "\d raw_market_snapshots" 2>&1 | head -20
```
Expected: the table definition prints with columns `id, source, source_key, record_type, captured_at, payload, ingested_at`.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add db/migrations/
git commit -m "feat(db): archive arb-era migrations; baseline 001 raw_market_snapshots"
```

---

## Task 4: KalshiConnector (read-only)

> **Scope note (deviation from spec §3.1):** v1 emits `record_type='market'` only. The spec also
> listed `orderbook` and `settlements` records — these are deliberately deferred to a fast follow
> (orderbook = one extra API call *per market*; settlements feed backtest, which is a later spec).
> Markets satisfy the success criteria; the connector's `normalize` stays easy to extend.

**Files:**
- Create: `app/shared/python/bountygate/connectors/kalshi.py`
- Create: `app/shared/python/tests/fixtures/connectors/kalshi_events.json`, `tests/connectors/test_kalshi.py`

- [ ] **Step 1: Create the fixture**

Create `app/shared/python/tests/fixtures/connectors/kalshi_events.json`:

```json
{
  "events": [
    {
      "event_ticker": "KXNFLGAME-26SEP07DALNYG",
      "markets": [
        {
          "ticker": "KXNFLGAME-26SEP07DALNYG-DAL",
          "event_ticker": "KXNFLGAME-26SEP07DALNYG",
          "title": "Will the Cowboys win?",
          "yes_sub_title": "Dallas C",
          "no_sub_title": "New York G",
          "yes_bid_dollars": "0.52",
          "yes_ask_dollars": "0.55",
          "no_bid_dollars": "0.45",
          "no_ask_dollars": "0.48",
          "open_interest_fp": "1234",
          "liquidity_dollars": "500.0",
          "status": "active"
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Write the failing test** (targets the pure `normalize` — no SDK, no network)

Create `app/shared/python/tests/connectors/test_kalshi.py`:

```python
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_kalshi.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.connectors.kalshi'`.

- [ ] **Step 4: Write `kalshi.py`**

Create `app/shared/python/bountygate/connectors/kalshi.py`:

```python
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from bountygate.connectors.base import Connector, RawRecord

SERIES_BY_SPORT = {"NFL": "KXNFLGAME", "NBA": "KXNBAGAME", "MLB": "KXMLBGAME"}


def _to_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class KalshiConnector(Connector):
    """Read-only Kalshi market data. Migrated from kalshi/dags/utils/kalshi_client.py,
    dropping all execution methods. Keeps the raw-HTTP-body bypass (the SDK's pydantic
    models are stale vs. the live API)."""

    source = "kalshi"

    def __init__(self, series_by_sport: dict | None = None):
        self.series_by_sport = series_by_sport or SERIES_BY_SPORT

    @staticmethod
    def normalize(raw: dict, series_ticker: str, captured_at: datetime) -> list[RawRecord]:
        """Pure: raw get_events body -> RawRecords. No I/O."""
        records: list[RawRecord] = []
        for event in (raw.get("events") or []):
            for m in (event.get("markets") or []):
                ticker = m.get("ticker")
                if not ticker:
                    continue
                yes_bid = _to_float(m.get("yes_bid_dollars"))
                yes_ask = _to_float(m.get("yes_ask_dollars"))
                no_bid = _to_float(m.get("no_bid_dollars"))
                no_ask = _to_float(m.get("no_ask_dollars"))
                payload = {
                    "ticker": ticker,
                    "event_ticker": m.get("event_ticker"),
                    "series_ticker": series_ticker,
                    "title": m.get("title"),
                    "yes_sub_title": m.get("yes_sub_title"),
                    "no_sub_title": m.get("no_sub_title"),
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "no_bid": no_bid,
                    "no_ask": no_ask,
                    "open_interest": _to_float(m.get("open_interest_fp")) or m.get("open_interest"),
                    "liquidity_dollars": _to_float(m.get("liquidity_dollars")),
                    "status": m.get("status"),
                }
                records.append(
                    RawRecord(
                        source="kalshi",
                        source_key=ticker,
                        record_type="market",
                        captured_at=captured_at,
                        payload=payload,
                    )
                )
        return records

    def _client(self):
        """Build the authenticated Kalshi SDK client (RSA-signed). Lazy import so
        unit tests of normalize() don't require kalshi_python_sync."""
        from kalshi_python_sync import Configuration, KalshiClient

        host = "https://api.elections.kalshi.com/trade-api/v2"
        with open(os.environ["KALSHI_PRIVATE_KEY_PATH"], "r") as f:
            private_key_pem = f.read()
        config = Configuration(host=host)
        config.api_key_id = os.environ["KALSHI_API_KEY_ID"]
        config.private_key_pem = private_key_pem
        return KalshiClient(config)

    def _fetch_raw(self, client, series_ticker: str) -> dict:
        resp = client.get_events_without_preload_content(
            series_ticker=series_ticker, status="open", with_nested_markets=True
        )
        return json.loads(resp.data)

    def fetch_snapshots(self) -> list[RawRecord]:
        client = self._client()
        out: list[RawRecord] = []
        captured_at = datetime.now(timezone.utc)
        for series_ticker in self.series_by_sport.values():
            try:
                raw = self._fetch_raw(client, series_ticker)
            except Exception as e:  # one series failing shouldn't sink the run
                print(f"[kalshi] fetch failed for {series_ticker}: {e}")
                continue
            out.extend(self.normalize(raw, series_ticker, captured_at))
        return out
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_kalshi.py -v`
Expected: PASS (1 passed).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/connectors/kalshi.py app/shared/python/tests/connectors/test_kalshi.py app/shared/python/tests/fixtures/connectors/kalshi_events.json
git commit -m "feat(connectors): KalshiConnector (read-only markets)"
```

---

## Task 5: PolymarketConnector

**Files:**
- Create: `app/shared/python/bountygate/connectors/polymarket.py`
- Create: `app/shared/python/tests/fixtures/connectors/polymarket_markets.json`, `tests/connectors/test_polymarket.py`

- [ ] **Step 1: Create the fixture** (Gamma `/markets` shape; `outcomes`/`outcomePrices` arrive as JSON strings)

Create `app/shared/python/tests/fixtures/connectors/polymarket_markets.json`:

```json
[
  {
    "conditionId": "0xabc123",
    "question": "Will it rain in NYC on June 6?",
    "slug": "rain-nyc-june-6",
    "outcomes": "[\"Yes\", \"No\"]",
    "outcomePrices": "[\"0.42\", \"0.58\"]",
    "volume": "12345.67",
    "liquidity": "8900.12",
    "endDate": "2026-06-06T23:59:59Z",
    "active": true,
    "closed": false
  }
]
```

- [ ] **Step 2: Write the failing test**

Create `app/shared/python/tests/connectors/test_polymarket.py`:

```python
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
    assert r.payload["outcomes"] == ["Yes", "No"]          # JSON-string parsed to list
    assert r.payload["outcome_prices"] == [0.42, 0.58]     # parsed to floats
    assert r.payload["volume"] == 12345.67
    assert r.payload["question"].startswith("Will it rain")


def test_normalize_skips_markets_without_condition_id():
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)
    assert PolymarketConnector.normalize([{"question": "no id"}], captured_at=ts) == []
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_polymarket.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.connectors.polymarket'`.

- [ ] **Step 4: Write `polymarket.py`**

Create `app/shared/python/bountygate/connectors/polymarket.py`:

```python
from __future__ import annotations

import json
from datetime import datetime, timezone

import requests

from bountygate.connectors.base import Connector, RawRecord

GAMMA_BASE = "https://gamma-api.polymarket.com"


def _maybe_json(value):
    """Gamma returns `outcomes`/`outcomePrices` as JSON-encoded strings."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class PolymarketConnector(Connector):
    """Read-only Polymarket market data via the public Gamma API (no auth)."""

    source = "polymarket"

    def __init__(self, gamma_base: str = GAMMA_BASE, page_limit: int = 500, max_pages: int = 20):
        self.gamma_base = gamma_base
        self.page_limit = page_limit
        self.max_pages = max_pages

    @staticmethod
    def normalize(raw_markets: list, captured_at: datetime) -> list[RawRecord]:
        """Pure: list of Gamma market dicts -> RawRecords."""
        records: list[RawRecord] = []
        for m in raw_markets or []:
            cond = m.get("conditionId")
            if not cond:
                continue
            prices = _maybe_json(m.get("outcomePrices"))
            if isinstance(prices, list):
                prices = [_to_float(p) for p in prices]
            payload = {
                "condition_id": cond,
                "question": m.get("question"),
                "slug": m.get("slug"),
                "outcomes": _maybe_json(m.get("outcomes")),
                "outcome_prices": prices,
                "volume": _to_float(m.get("volume")),
                "liquidity": _to_float(m.get("liquidity")),
                "end_date": m.get("endDate"),
                "active": m.get("active"),
                "closed": m.get("closed"),
            }
            records.append(
                RawRecord(
                    source="polymarket",
                    source_key=cond,
                    record_type="market",
                    captured_at=captured_at,
                    payload=payload,
                )
            )
        return records

    def _fetch_raw(self) -> list:
        """Page active, non-closed markets from Gamma."""
        out: list = []
        session = requests.Session()
        for page in range(self.max_pages):
            params = {
                "active": "true",
                "closed": "false",
                "limit": self.page_limit,
                "offset": page * self.page_limit,
            }
            resp = session.get(f"{self.gamma_base}/markets", params=params, timeout=30)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            out.extend(batch)
            if len(batch) < self.page_limit:
                break
        return out

    def fetch_snapshots(self) -> list[RawRecord]:
        captured_at = datetime.now(timezone.utc)
        return self.normalize(self._fetch_raw(), captured_at)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_polymarket.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/connectors/polymarket.py app/shared/python/tests/connectors/test_polymarket.py app/shared/python/tests/fixtures/connectors/polymarket_markets.json
git commit -m "feat(connectors): PolymarketConnector (Gamma read API)"
```

---

## Task 6: OddsApiConnector

**Files:**
- Create: `app/shared/python/bountygate/connectors/odds_api.py`
- Create: `app/shared/python/tests/fixtures/connectors/odds_event.json`, `tests/connectors/test_odds_api.py`

- [ ] **Step 1: Create the fixture** (The Odds API `/events/{id}/odds` shape)

Create `app/shared/python/tests/fixtures/connectors/odds_event.json`:

```json
{
  "id": "evt_123",
  "sport_key": "basketball_nba",
  "commence_time": "2026-06-06T23:40:00Z",
  "home_team": "Boston Celtics",
  "away_team": "New York Knicks",
  "bookmakers": [
    {
      "key": "fanduel",
      "last_update": "2026-06-05T12:00:00Z",
      "markets": [
        {
          "key": "h2h",
          "outcomes": [
            {"name": "Boston Celtics", "price": 1.8},
            {"name": "New York Knicks", "price": 2.1}
          ]
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Write the failing test**

Create `app/shared/python/tests/connectors/test_odds_api.py`:

```python
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_odds_api.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.connectors.odds_api'`.

- [ ] **Step 4: Write `odds_api.py`**

Create `app/shared/python/bountygate/connectors/odds_api.py`:

```python
from __future__ import annotations

import os
from datetime import datetime, timezone

import requests

from bountygate.connectors.base import Connector, RawRecord

BASE_URL = "https://api.the-odds-api.com/v4/sports"
SPORT_KEYS = {"NFL": "americanfootball_nfl", "NBA": "basketball_nba", "MLB": "baseball_mlb"}


class OddsApiConnector(Connector):
    """Read-only sportsbook odds via The Odds API v4. Credit-aware: list events in a
    commence window first, then request odds per event. Key from ODDS_API_KEY env."""

    source = "the_odds_api"

    def __init__(self, sport_keys: dict | None = None, markets: str = "h2h", regions: str = "us"):
        self.api_key = os.getenv("ODDS_API_KEY")
        self.sport_keys = sport_keys or SPORT_KEYS
        self.markets = markets
        self.regions = regions

    @staticmethod
    def normalize_event(event: dict, captured_at: datetime) -> list[RawRecord]:
        """Pure: one /events/{id}/odds payload -> one RawRecord per (book, market)."""
        records: list[RawRecord] = []
        event_id = event.get("id")
        if not event_id:
            return records
        common = {
            "event_id": event_id,
            "sport_key": event.get("sport_key"),
            "commence_time": event.get("commence_time"),
            "home_team": event.get("home_team"),
            "away_team": event.get("away_team"),
        }
        for book in event.get("bookmakers") or []:
            book_key = book.get("key")
            for market in book.get("markets") or []:
                market_key = market.get("key")
                if not book_key or not market_key:
                    continue
                payload = {
                    **common,
                    "bookmaker": book_key,
                    "market": market_key,
                    "last_update": book.get("last_update"),
                    "outcomes": market.get("outcomes") or [],
                }
                records.append(
                    RawRecord(
                        source="the_odds_api",
                        source_key=f"{event_id}:{market_key}:{book_key}",
                        record_type="odds_line",
                        captured_at=captured_at,
                        payload=payload,
                    )
                )
        return records

    def _list_events(self, session, sport_key: str) -> list:
        resp = session.get(
            f"{BASE_URL}/{sport_key}/events", params={"apiKey": self.api_key}, timeout=30
        )
        resp.raise_for_status()
        return resp.json()

    def _event_odds(self, session, sport_key: str, event_id: str) -> dict:
        resp = session.get(
            f"{BASE_URL}/{sport_key}/events/{event_id}/odds",
            params={
                "apiKey": self.api_key,
                "regions": self.regions,
                "markets": self.markets,
                "oddsFormat": "decimal",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_snapshots(self) -> list[RawRecord]:
        captured_at = datetime.now(timezone.utc)
        out: list[RawRecord] = []
        session = requests.Session()
        for sport_key in self.sport_keys.values():
            try:
                events = self._list_events(session, sport_key)
            except Exception as e:
                print(f"[odds] list events failed for {sport_key}: {e}")
                continue
            for ev in events:
                try:
                    odds = self._event_odds(session, sport_key, ev["id"])
                except Exception as e:
                    print(f"[odds] odds fetch failed for {ev.get('id')}: {e}")
                    continue
                out.extend(self.normalize_event(odds, captured_at))
        return out
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_odds_api.py -v`
Expected: PASS (1 passed).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/connectors/odds_api.py app/shared/python/tests/connectors/test_odds_api.py app/shared/python/tests/fixtures/connectors/odds_event.json
git commit -m "feat(connectors): OddsApiConnector (The Odds API v4, credit-aware)"
```

---

## Task 7: Connector registry

**Files:**
- Create: `app/shared/python/bountygate/connectors/registry.py`
- Create: `app/shared/python/tests/connectors/test_registry.py`

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/tests/connectors/test_registry.py`:

```python
from bountygate.connectors.base import Connector
from bountygate.connectors.registry import CONNECTORS, get_connector


def test_registry_has_three_sources():
    assert set(CONNECTORS) == {"kalshi", "polymarket", "the_odds_api"}


def test_registry_values_are_connectors():
    for source, conn in CONNECTORS.items():
        assert isinstance(conn, Connector)
        assert conn.source == source


def test_get_connector_returns_instance():
    assert get_connector("polymarket").source == "polymarket"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.connectors.registry'`.

- [ ] **Step 3: Write `registry.py`**

Create `app/shared/python/bountygate/connectors/registry.py`:

```python
from __future__ import annotations

from bountygate.connectors.base import Connector
from bountygate.connectors.kalshi import KalshiConnector
from bountygate.connectors.odds_api import OddsApiConnector
from bountygate.connectors.polymarket import PolymarketConnector

CONNECTORS: dict[str, Connector] = {
    KalshiConnector.source: KalshiConnector(),
    PolymarketConnector.source: PolymarketConnector(),
    OddsApiConnector.source: OddsApiConnector(),
}


def get_connector(source: str) -> Connector:
    return CONNECTORS[source]
```

Note: `KalshiConnector()`/`OddsApiConnector()` constructors do no network/credential I/O (auth happens lazily in `fetch_snapshots`), so importing the registry is safe in tests and at DAG parse time.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors/test_registry.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Run the full connector suite**

Run: `cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors -v`
Expected: all green (base 2, landing 3, kalshi 1, polymarket 2, odds 1, registry 3 = 12 passed).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/connectors/registry.py app/shared/python/tests/connectors/test_registry.py
git commit -m "feat(connectors): CONNECTORS registry"
```

---

## Task 8: Airflow 3.2 upgrade + triggerer service

**Files:**
- Modify: `airflow/Dockerfile`, `airflow/requirements.txt`, `airflow/docker-compose.yml`

- [ ] **Step 1: Bump the base image**

In `airflow/Dockerfile`, change line 2 from `FROM apache/airflow:3.0.6` to:

```dockerfile
FROM apache/airflow:3.2.0
```

- [ ] **Step 2: Bump the Airflow pin + add the Kalshi SDK**

In `airflow/requirements.txt`, change `apache-airflow==3.0.6` to `apache-airflow==3.2.0`. Then append the Kalshi SDK — **use the exact pin from the Kalshi repo** so it matches the working client:

```bash
cd /c/Users/tkmer/bountygate
grep -i 'kalshi' /c/Users/tkmer/kalshi/requirements.txt
```
Add that exact line (e.g. `kalshi-python-sync==<version>`) to `airflow/requirements.txt`.

- [ ] **Step 3: Add the triggerer service**

In `airflow/docker-compose.yml`, add a new service after `airflow-scheduler` (before `airflow-init`), mirroring the existing service blocks:

```yaml
  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    mem_limit: 512m
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
```

- [ ] **Step 4: Rebuild and start the stack**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose build 2>&1 | tail -15
docker compose up -d 2>&1 | tail -15
```
Expected: build succeeds; containers start including `airflow-triggerer`.

- [ ] **Step 5: Verify version 3.2 + triggerer healthy**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler airflow version 2>&1 | tail -1
docker compose ps 2>&1 | grep -E 'triggerer|scheduler|apiserver'
```
Expected: `airflow version` prints `3.2.0`; `docker compose ps` shows `airflow-triggerer` running/healthy. Open <http://localhost:8080> — the red "triggerer not running" banner is gone.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add airflow/Dockerfile airflow/requirements.txt airflow/docker-compose.yml
git commit -m "chore(airflow): upgrade 3.0.6 -> 3.2.0 + add triggerer service"
```

---

## Task 9: Kalshi credentials into bountygate

**Files:**
- Create: `private_key.pem` (gitignored), modify repo `.env`, `airflow/docker-compose.yml`, `.gitignore`

- [ ] **Step 1: Copy the working Kalshi private key into the repo root** (gitignored — never commit)

```bash
cd /c/Users/tkmer/bountygate
cp /c/Users/tkmer/kalshi/private_key.pem ./private_key.pem
grep -qxF 'private_key.pem' .gitignore || echo 'private_key.pem' >> .gitignore
git check-ignore private_key.pem && echo "OK: gitignored"
```
Expected: `OK: gitignored`.

- [ ] **Step 2: Add Kalshi env vars to the repo-root `.env`** (read the values from the Kalshi repo's `.env`)

```bash
cd /c/Users/tkmer/bountygate
grep -E '^KALSHI_API_KEY_ID=' /c/Users/tkmer/kalshi/.env
```
Append to `bountygate/.env` (use the real key id from the line above):

```
KALSHI_ENV=production
KALSHI_API_KEY_ID=<value from kalshi/.env>
KALSHI_PRIVATE_KEY_PATH=/opt/airflow/private_key.pem
```

- [ ] **Step 3: Mount the PEM read-only into the Airflow containers**

In `airflow/docker-compose.yml`, under the `x-airflow-common` `volumes:` list (around line 105), add:

```yaml
    # Kalshi private key (read-only) for the read-only Kalshi connector
    - ../private_key.pem:/opt/airflow/private_key.pem:ro
```

- [ ] **Step 4: Restart and verify the connector can authenticate inside a container**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose up -d 2>&1 | tail -5
docker compose run --rm airflow-scheduler python -c "import os; from bountygate.connectors.kalshi import KalshiConnector; c=KalshiConnector(); n=len(c.fetch_snapshots()); print('kalshi markets fetched:', n)" 2>&1 | tail -5
```
Expected: prints `kalshi markets fetched: <N>` with N ≥ 0 and no auth error. (N may be 0 out of season — a clean run with no exception is success.)

- [ ] **Step 5: Commit** (compose only — PEM and .env are gitignored)

```bash
cd /c/Users/tkmer/bountygate
git add airflow/docker-compose.yml .gitignore
git commit -m "chore(airflow): mount Kalshi private key for the read-only connector"
```

---

## Task 10: Ingestion poller DAGs + import smoke test

**Files:**
- Create: `airflow/dags/ingest_kalshi.py`, `ingest_polymarket.py`, `ingest_odds.py`
- Create: `airflow/tests/test_ingest_dags_import.py`

- [ ] **Step 1: Write the failing DAG-import smoke test**

Create `airflow/tests/test_ingest_dags_import.py`:

```python
import importlib.util
from pathlib import Path

import pytest

DAGS = Path(__file__).parent.parent / "dags"
INGEST_DAGS = ["ingest_kalshi.py", "ingest_polymarket.py", "ingest_odds.py"]


@pytest.mark.parametrize("filename", INGEST_DAGS)
def test_ingest_dag_imports(filename):
    path = DAGS / filename
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "dag"), f"{filename} must define a top-level `dag`"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate/airflow && python -m pytest tests/test_ingest_dags_import.py -v`
Expected: FAIL — files don't exist yet (or `dag` attribute missing).

- [ ] **Step 3: Write the three poller DAGs**

Create `airflow/dags/ingest_kalshi.py`:

```python
"""Read-only Kalshi ingestion poller. Fetches market snapshots every 5 min and
lands them in raw_market_snapshots. Emits the raw asset for downstream normalization."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.connectors.landing import RAW_TABLE, land_raw
from bountygate.connectors.registry import get_connector

RAW_ASSET = Asset(f"postgres://{RAW_TABLE}")


@dag(
    dag_id="ingest_kalshi",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["ingest", "kalshi"],
)
def ingest_kalshi():
    @task(outlets=[RAW_ASSET])
    def fetch_and_land() -> int:
        records = get_connector("kalshi").fetch_snapshots()
        n = land_raw(records)
        print(f"[ingest_kalshi] landed {n} records")
        return n

    fetch_and_land()


dag = ingest_kalshi()
```

Create `airflow/dags/ingest_polymarket.py` (identical structure; swap source + dag_id + tags):

```python
"""Read-only Polymarket ingestion poller (Gamma API). Every 5 min -> raw_market_snapshots."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.connectors.landing import RAW_TABLE, land_raw
from bountygate.connectors.registry import get_connector

RAW_ASSET = Asset(f"postgres://{RAW_TABLE}")


@dag(
    dag_id="ingest_polymarket",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["ingest", "polymarket"],
)
def ingest_polymarket():
    @task(outlets=[RAW_ASSET])
    def fetch_and_land() -> int:
        records = get_connector("polymarket").fetch_snapshots()
        n = land_raw(records)
        print(f"[ingest_polymarket] landed {n} records")
        return n

    fetch_and_land()


dag = ingest_polymarket()
```

Create `airflow/dags/ingest_odds.py` (same; source `the_odds_api`):

```python
"""Read-only The Odds API ingestion poller. Every 5 min -> raw_market_snapshots."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.connectors.landing import RAW_TABLE, land_raw
from bountygate.connectors.registry import get_connector

RAW_ASSET = Asset(f"postgres://{RAW_TABLE}")


@dag(
    dag_id="ingest_odds",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["ingest", "the_odds_api"],
)
def ingest_odds():
    @task(outlets=[RAW_ASSET])
    def fetch_and_land() -> int:
        records = get_connector("the_odds_api").fetch_snapshots()
        n = land_raw(records)
        print(f"[ingest_odds] landed {n} records")
        return n

    fetch_and_land()


dag = ingest_odds()
```

- [ ] **Step 4: Run the smoke test to verify it passes**

The test needs the Airflow SDK + shared lib on the path. Run it inside a container (host may lack `airflow.sdk`):

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm -v "$(pwd)/tests:/opt/airflow/project_tests" airflow-scheduler \
  python -m pytest /opt/airflow/project_tests/test_ingest_dags_import.py -v 2>&1 | tail -20
```
Expected: 3 passed (each DAG imports and exposes a top-level `dag`).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add airflow/dags/ingest_kalshi.py airflow/dags/ingest_polymarket.py airflow/dags/ingest_odds.py airflow/tests/test_ingest_dags_import.py
git commit -m "feat(dags): read-only ingestion pollers (kalshi/polymarket/odds), 5-min cadence"
```

---

## Task 11: End-to-end verification

**Files:** none (verification only).

- [ ] **Step 1: Confirm the DAGs are registered in Airflow**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler airflow dags list 2>&1 | grep -E 'ingest_kalshi|ingest_polymarket|ingest_odds'
```
Expected: all three `ingest_*` DAGs listed.

- [ ] **Step 2: Trigger one run of each and wait for completion**

```bash
cd /c/Users/tkmer/bountygate/airflow
for d in ingest_polymarket ingest_odds ingest_kalshi; do
  echo "=== $d ==="; docker compose run --rm airflow-scheduler airflow dags test "$d" 2>&1 | tail -4
done
```
Expected: each `dags test` run finishes with state `success` and prints a `landed N records` line.

- [ ] **Step 3: Verify rows landed per source** (Docker psql — Heroku pg CLI is broken)

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c \
  "SELECT source, count(*) FROM raw_market_snapshots GROUP BY source ORDER BY source;" 2>&1
```
Expected: a count for `polymarket` and `the_odds_api` (≥1 each); `kalshi` may be 0 out of season — a successful run with no error is acceptable for Kalshi.

- [ ] **Step 4: Full connector test suite green**

```bash
cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest tests/connectors -q 2>&1 | tail -3
```
Expected: `12 passed`.

- [ ] **Step 5: Report completion**

Summarize against the spec's §8 success criteria: package exists with all 6 modules + 3-entry registry; migration applied; connectors green on fixtures; the three DAGs import, are scheduled every 5 min, and land rows; Airflow 3.2 up with a healthy triggerer (no red banner).
