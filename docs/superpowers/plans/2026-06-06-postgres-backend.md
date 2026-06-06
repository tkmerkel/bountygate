# Postgres Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Postgres backend — extensions, partitioned + retained time-series, the normalized/marts schema contract (empty tables), and a thin read-only FastAPI API — so the future Airflow transforms and the frontend have a stable foundation.

**Architecture:** Ordered SQL migrations (`db/migrations/00N_*.sql`) applied via Docker `psql` (host lacks psycopg2) and recorded in `schema_migrations`. `pg_partman` v5 manages daily `RANGE(captured_at)` partitions; a daily Airflow DAG runs maintenance (pg_cron is allowlist-blocked on Heroku Postgres). A small FastAPI app (`app/web/`) serves read-only JSON over the marts, tested with `TestClient` against seeded SQLite.

**Tech Stack:** PostgreSQL 16 (Heroku), pg_partman 5.2.4, pg_trgm, pg_stat_statements, SQL migrations, Apache Airflow 3.2, FastAPI + SQLAlchemy Core, pytest.

**Spec:** `docs/superpowers/specs/2026-06-06-postgres-backend-design.md`

---

## Environment notes for the implementer
- **Apply migrations with Docker** (the host hermes venv has no psycopg2/pip). Pattern used throughout:
  ```bash
  cd /c/Users/tkmer/bountygate
  export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
  MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig \
    postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/<file>.sql
  docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" \
    -c "INSERT INTO schema_migrations(version) VALUES ('<version>') ON CONFLICT DO NOTHING;"
  ```
  (`schema_migrations` already exists; `migrate.py` works only where psycopg2 is present — Airflow image / Heroku.)
- **The three `ingest_*` DAGs are paused** — no concurrent writers to `raw_market_snapshots`.
- **Web tests run on host:** `cd /c/Users/tkmer/bountygate && python -m pytest app/web/tests -v` (host has fastapi/httpx/sqlalchemy; tests use SQLite, no DB needed).
- Current migrations: only `db/migrations/001_raw_market_snapshots.sql`. New files are `002`–`006`.

---

## File Structure

| Path | Responsibility | Status |
|---|---|---|
| `db/migrations/002_extensions.sql` | enable pg_partman/pg_trgm/pg_stat_statements + `partman` schema | Create |
| `db/migrations/003_partition_raw.sql` | recreate `raw_market_snapshots` partitioned + pg_partman + retention | Create |
| `db/migrations/004_history_tables.sql` | `price_history` + `sportsbook_odds_history` partitioned | Create |
| `db/migrations/005_normalized.sql` | venues/markets/outcomes/events/links + seed venues | Create |
| `db/migrations/006_marts.sql` | the three mart tables | Create |
| `airflow/dags/partman_maintenance.py` | daily `partman.run_maintenance_proc()` | Create |
| `app/web/__init__.py`, `app/web/db.py`, `app/web/main.py` | FastAPI app + engine provider | Create |
| `app/web/routers/{markets,edges,cross_market,history}.py` | one router per resource | Create |
| `app/web/tests/conftest.py`, `app/web/tests/test_*.py` | TestClient tests | Create |
| `Procfile`, `requirements.txt` | Heroku web slug | Create/Modify |

---

## Task 1: Extensions migration

**Files:** Create `db/migrations/002_extensions.sql`

- [ ] **Step 1: Write the migration**

Create `db/migrations/002_extensions.sql`:

```sql
-- Extensions for the analytics-aggregator backend.
-- pg_cron is intentionally absent: Heroku Postgres's rds.allowed_extensions blocks it;
-- partition maintenance runs from an Airflow DAG instead.
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

- [ ] **Step 2: Apply it**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/002_extensions.sql
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('002_extensions') ON CONFLICT DO NOTHING;"
```
Expected: `CREATE SCHEMA` / `CREATE EXTENSION` × 3, no error.

- [ ] **Step 3: Verify**

```bash
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "\dx" 2>&1 | grep -E "pg_partman|pg_trgm|pg_stat_statements"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select extversion from pg_extension where extname='pg_partman';"
```
Expected: all three extensions listed; pg_partman version `5.2.4`.

- [ ] **Step 4: Commit**

```bash
git add db/migrations/002_extensions.sql
git commit -m "feat(db): enable pg_partman, pg_trgm, pg_stat_statements"
```

---

## Task 2: Partition `raw_market_snapshots`

**Files:** Create `db/migrations/003_partition_raw.sql`

The ingest DAGs are paused, so recreating the table is safe. pg_partman v5 `create_parent` creates a default partition automatically, so copying existing rows can't miss a partition.

- [ ] **Step 1: Record the current row count (for the post-copy check)**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select count(*) from raw_market_snapshots;"
```
Note the number (call it N).

- [ ] **Step 2: Write the migration**

Create `db/migrations/003_partition_raw.sql`:

```sql
-- Recreate raw_market_snapshots as a RANGE(captured_at) partitioned table, managed by pg_partman.
ALTER TABLE raw_market_snapshots RENAME TO raw_market_snapshots_pre_partition;

CREATE TABLE raw_market_snapshots (
  id           bigint GENERATED BY DEFAULT AS IDENTITY,
  source       text        NOT NULL,
  source_key   text        NOT NULL,
  record_type  text        NOT NULL,
  captured_at  timestamptz NOT NULL,
  payload      jsonb       NOT NULL,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id, captured_at)
) PARTITION BY RANGE (captured_at);

CREATE INDEX ix_raw_snap_source_time ON raw_market_snapshots (source, captured_at);
CREATE INDEX ix_raw_snap_source_key  ON raw_market_snapshots (source, source_key, captured_at);
CREATE INDEX brin_raw_snap_captured  ON raw_market_snapshots USING brin (captured_at);

SELECT partman.create_parent(
  p_parent_table := 'public.raw_market_snapshots',
  p_control      := 'captured_at',
  p_interval     := '1 day',
  p_type         := 'range',
  p_premake      := 4
);

UPDATE partman.part_config
SET retention = '90 days', retention_keep_table = false
WHERE parent_table = 'public.raw_market_snapshots';

-- Copy existing rows (new ids regenerate; raw is a firehose with no inbound FKs).
INSERT INTO raw_market_snapshots (source, source_key, record_type, captured_at, payload, ingested_at)
SELECT source, source_key, record_type, captured_at, payload, ingested_at
FROM raw_market_snapshots_pre_partition;

DROP TABLE raw_market_snapshots_pre_partition;
```

- [ ] **Step 3: Apply it**

```bash
MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/003_partition_raw.sql
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('003_partition_raw') ON CONFLICT DO NOTHING;"
```
Expected: completes with no error (the final line is `DROP TABLE`). If `create_parent` errors on an argument name, run `docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "\df partman.create_parent"` to confirm the v5 signature and adjust — but 5.2.4 matches the call above.

- [ ] **Step 4: Verify row count preserved + partitions exist**

```bash
echo "new count:"; docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select count(*) from raw_market_snapshots;"
echo "is partitioned:"; docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select relkind from pg_class where relname='raw_market_snapshots';"
echo "partitions:"; docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select count(*) from pg_inherits where inhparent='raw_market_snapshots'::regclass;"
```
Expected: count == N (from Step 1); `relkind` = `p` (partitioned); ≥1 partition.

- [ ] **Step 5: Commit**

```bash
git add db/migrations/003_partition_raw.sql
git commit -m "feat(db): partition raw_market_snapshots by captured_at (pg_partman, 90d retention)"
```

---

## Task 3: History tables

**Files:** Create `db/migrations/004_history_tables.sql`

- [ ] **Step 1: Write the migration**

Create `db/migrations/004_history_tables.sql`:

```sql
-- Append-only analytical time-series, partitioned by captured_at (2y retention).
CREATE TABLE price_history (
  market_id   uuid        NOT NULL,
  outcome_id  uuid        NOT NULL,
  captured_at timestamptz NOT NULL,
  price       numeric,
  bid         numeric,
  ask         numeric,
  volume      numeric,
  liquidity   numeric
) PARTITION BY RANGE (captured_at);
CREATE INDEX ix_price_hist_outcome ON price_history (outcome_id, captured_at);
CREATE INDEX brin_price_hist_captured ON price_history USING brin (captured_at);

CREATE TABLE sportsbook_odds_history (
  event_id      uuid        NOT NULL,
  market_type   text        NOT NULL,
  bookmaker     text        NOT NULL,
  outcome_name  text        NOT NULL,
  captured_at   timestamptz NOT NULL,
  decimal_price numeric
) PARTITION BY RANGE (captured_at);
CREATE INDEX ix_sb_odds_event ON sportsbook_odds_history (event_id, captured_at);
CREATE INDEX brin_sb_odds_captured ON sportsbook_odds_history USING brin (captured_at);

SELECT partman.create_parent(p_parent_table := 'public.price_history',
  p_control := 'captured_at', p_interval := '1 day', p_type := 'range', p_premake := 4);
SELECT partman.create_parent(p_parent_table := 'public.sportsbook_odds_history',
  p_control := 'captured_at', p_interval := '1 day', p_type := 'range', p_premake := 4);

UPDATE partman.part_config SET retention = '2 years', retention_keep_table = false
WHERE parent_table IN ('public.price_history', 'public.sportsbook_odds_history');
```

- [ ] **Step 2: Apply it**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/004_history_tables.sql
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('004_history_tables') ON CONFLICT DO NOTHING;"
```
Expected: no error.

- [ ] **Step 3: Verify both are partitioned and in part_config**

```bash
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select relname, relkind from pg_class where relname in ('price_history','sportsbook_odds_history');"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select parent_table, retention from partman.part_config order by 1;"
```
Expected: both `relkind=p`; `part_config` lists all three parents (raw + the two history) with retentions.

- [ ] **Step 4: Commit**

```bash
git add db/migrations/004_history_tables.sql
git commit -m "feat(db): partitioned price_history + sportsbook_odds_history (2y retention)"
```

---

## Task 4: Normalized schema

**Files:** Create `db/migrations/005_normalized.sql`

- [ ] **Step 1: Write the migration**

Create `db/migrations/005_normalized.sql`:

```sql
-- Normalized cross-venue contract (populated later by Airflow transforms).
CREATE TABLE venues (
  venue_key text PRIMARY KEY,
  kind      text NOT NULL          -- 'prediction' | 'sportsbook'
);
INSERT INTO venues (venue_key, kind) VALUES
  ('kalshi', 'prediction'),
  ('polymarket', 'prediction'),
  ('the_odds_api', 'sportsbook')
ON CONFLICT DO NOTHING;

CREATE TABLE markets (
  market_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  venue_key       text NOT NULL REFERENCES venues(venue_key),
  external_id     text NOT NULL,
  title           text,
  category        text,
  status          text,
  open_time       timestamptz,
  close_time      timestamptz,
  resolved_outcome text,
  resolution_time timestamptz,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (venue_key, external_id)
);

CREATE TABLE market_outcomes (
  outcome_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  market_id    uuid NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
  outcome_name text NOT NULL,
  outcome_index int,
  last_price   numeric,
  last_seen    timestamptz,
  UNIQUE (market_id, outcome_name)
);

CREATE TABLE sports_events (
  event_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_event_id text UNIQUE NOT NULL,
  sport_key       text,
  commence_time   timestamptz,
  home_team       text,
  away_team       text
);

CREATE TABLE market_event_links (
  market_id  uuid NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
  event_id   uuid NOT NULL REFERENCES sports_events(event_id) ON DELETE CASCADE,
  confidence numeric,
  method     text,
  UNIQUE (market_id, event_id)
);
```

- [ ] **Step 2: Apply it**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/005_normalized.sql
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('005_normalized') ON CONFLICT DO NOTHING;"
```
Expected: no error.

- [ ] **Step 3: Verify tables + venue seed**

```bash
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select tablename from pg_tables where schemaname='public' and tablename in ('venues','markets','market_outcomes','sports_events','market_event_links') order by 1;"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select venue_key, kind from venues order by 1;"
```
Expected: all 5 tables; 3 venue rows (kalshi/polymarket/the_odds_api).

- [ ] **Step 4: Commit**

```bash
git add db/migrations/005_normalized.sql
git commit -m "feat(db): normalized cross-venue schema (venues/markets/outcomes/events/links)"
```

---

## Task 5: Marts schema

**Files:** Create `db/migrations/006_marts.sql`

- [ ] **Step 1: Write the migration**

Create `db/migrations/006_marts.sql`:

```sql
-- Read-only product marts (populated later by Airflow transforms).
CREATE TABLE mart_cross_market_prices (
  question_key              text,
  captured_at               timestamptz,
  kalshi_prob               numeric,
  polymarket_prob           numeric,
  sportsbook_consensus_prob numeric,
  max_spread                numeric
);
CREATE INDEX ix_mart_xmkt_question ON mart_cross_market_prices (question_key, captured_at);

CREATE TABLE mart_edge_signals (
  signal_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  detected_at    timestamptz,
  venue_key      text,
  market_id      uuid,
  outcome_id     uuid,
  signal_type    text,             -- 'arb' | 'ev'
  fair_prob      numeric,
  venue_price    numeric,
  edge           numeric,
  kelly_fraction numeric
);
CREATE INDEX ix_mart_edge_detected ON mart_edge_signals (detected_at);

CREATE TABLE mart_market_history (
  market_id       uuid,
  resolved_outcome text,
  resolution_time timestamptz,
  predicted_prob  numeric,
  realized        boolean,
  clv             numeric
);
CREATE INDEX ix_mart_hist_market ON mart_market_history (market_id);
```

- [ ] **Step 2: Apply it**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/006_marts.sql
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('006_marts') ON CONFLICT DO NOTHING;"
```
Expected: no error.

- [ ] **Step 3: Verify**

```bash
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select tablename from pg_tables where schemaname='public' and tablename like 'mart_%' order by 1;"
```
Expected: `mart_cross_market_prices`, `mart_edge_signals`, `mart_market_history`.

- [ ] **Step 4: Commit**

```bash
git add db/migrations/006_marts.sql
git commit -m "feat(db): read-only product marts (cross_market_prices/edge_signals/market_history)"
```

---

## Task 6: Airflow pg_partman maintenance DAG

**Files:** Create `airflow/dags/partman_maintenance.py`

- [ ] **Step 1: Write the DAG**

Create `airflow/dags/partman_maintenance.py`:

```python
"""Daily pg_partman maintenance: create upcoming partitions and drop expired ones
(retention). Runs from Airflow because pg_cron is not allowed on Heroku Postgres."""
from __future__ import annotations

import os

import pendulum
from airflow.sdk import dag, task
from sqlalchemy import create_engine, text


@dag(
    dag_id="partman_maintenance",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["maintenance", "db"],
)
def partman_maintenance():
    @task
    def run_maintenance() -> None:
        url = os.environ["DATABASE_URL"]
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg2://", 1)
        engine = create_engine(url)
        try:
            # AUTOCOMMIT: run_maintenance_proc commits internally; it must not run
            # inside an outer transaction block.
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text("CALL partman.run_maintenance_proc()"))
            print("[partman_maintenance] run_maintenance_proc completed")
        finally:
            engine.dispose()

    run_maintenance()


dag = partman_maintenance()
```

- [ ] **Step 2: Syntax-check + DAG-import check in the Airflow container**

```bash
cd /c/Users/tkmer/bountygate
python -m py_compile airflow/dags/partman_maintenance.py && echo "syntax OK"
cd airflow && docker compose run --rm airflow-scheduler airflow dags list-import-errors 2>&1 | grep -i "partman_maintenance" || echo "no import errors for partman_maintenance"
docker compose run --rm airflow-scheduler airflow dags list 2>&1 | grep partman_maintenance
```
Expected: `syntax OK`; no import error; `partman_maintenance` listed.

- [ ] **Step 3: Run it once to confirm maintenance fires**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler airflow dags test partman_maintenance 2>&1 | grep -iE "run_maintenance_proc completed|state=success|state=failed" | tail -4
```
Expected: `run_maintenance_proc completed` and `state=success`.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add airflow/dags/partman_maintenance.py
git commit -m "feat(dags): daily pg_partman maintenance DAG"
```

---

## Task 7: FastAPI app — engine, /health, /markets (TDD)

> **Intentional deviation from spec §7:** `db.py` reads `DATABASE_URL` from `os.environ` directly
> rather than importing `bountygate.utils.db_connection`. This keeps the Heroku web slug lean — importing
> the shared lib would pull pandas/numpy/scikit-learn into the web dyno for no benefit (the API needs
> only a connection string). Same env var, decoupled module.


**Files:** Create `app/web/__init__.py`, `app/web/db.py`, `app/web/main.py`, `app/web/routers/__init__.py`, `app/web/routers/markets.py`, `app/web/tests/conftest.py`, `app/web/tests/test_markets.py`

- [ ] **Step 1: Write the failing test**

Create `app/web/tests/conftest.py`:

```python
import os
import sys

# repo root on path so `import app.web...` resolves (app/ is a PEP 420 namespace pkg)
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
```

Create `app/web/tests/test_markets.py`:

```python
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.web.db import get_engine
from app.web.main import app


def _seed():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE markets (market_id text, venue_key text, external_id text, "
            "title text, category text, status text, open_time text, close_time text, "
            "resolved_outcome text, resolution_time text, updated_at text)"
        ))
        conn.execute(text(
            "INSERT INTO markets (market_id, venue_key, external_id, title, status) "
            "VALUES ('m1','kalshi','KX-1','Will X happen?','active')"
        ))
    return engine


def test_health_ok():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_markets_returns_rows_and_filters_by_venue():
    engine = _seed()
    app.dependency_overrides[get_engine] = lambda: engine
    try:
        client = TestClient(app)
        r = client.get("/markets")
        assert r.status_code == 200
        body = r.json()
        assert len(body) == 1 and body[0]["external_id"] == "KX-1"

        assert client.get("/markets", params={"venue": "polymarket"}).json() == []
        assert len(client.get("/markets", params={"venue": "kalshi"}).json()) == 1
    finally:
        app.dependency_overrides.clear()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/web/tests/test_markets.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.web.db'`.

- [ ] **Step 3: Write the app**

Create `app/web/__init__.py` (empty file).
Create `app/web/routers/__init__.py` (empty file).

Create `app/web/db.py`:

```python
from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_engine: Engine | None = None


def get_engine() -> Engine:
    """Process-wide engine over DATABASE_URL. Overridden in tests via
    FastAPI dependency_overrides."""
    global _engine
    if _engine is None:
        url = os.environ["DATABASE_URL"]
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg2://", 1)
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine
```

Create `app/web/routers/markets.py`:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = ("market_id, venue_key, external_id, title, category, status, "
         "open_time, close_time, resolved_outcome, resolution_time, updated_at")


@router.get("/markets")
def list_markets(
    venue: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    where, params = [], {"lim": limit, "off": offset}
    if venue:
        where.append("venue_key = :venue")
        params["venue"] = venue
    if status:
        where.append("status = :status")
        params["status"] = status
    sql = f"SELECT {_COLS} FROM markets"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
```

Create `app/web/main.py`:

```python
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.web.routers import markets

app = FastAPI(title="bountygate read API", description="Read-only prediction-market analytics")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(markets.router)
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/web/tests/test_markets.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add app/web/__init__.py app/web/db.py app/web/main.py app/web/routers/__init__.py app/web/routers/markets.py app/web/tests/conftest.py app/web/tests/test_markets.py
git commit -m "feat(web): FastAPI read API skeleton + /health + /markets"
```

---

## Task 8: Remaining endpoints — /markets/{id}/history, /edges, /cross-market, /history (TDD)

**Files:** Create `app/web/routers/{edges,cross_market,history}.py`, add a price-history route to `markets.py`; Create `app/web/tests/test_endpoints.py`; Modify `app/web/main.py`

- [ ] **Step 1: Write the failing test**

Create `app/web/tests/test_endpoints.py`:

```python
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.web.db import get_engine
from app.web.main import app


def _engine_with(ddl, insert):
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text(insert))
    return engine


def _use(engine):
    app.dependency_overrides[get_engine] = lambda: engine
    return TestClient(app)


def test_edges_filters_by_signal_type():
    engine = _engine_with(
        "CREATE TABLE mart_edge_signals (signal_id text, detected_at text, venue_key text, "
        "market_id text, outcome_id text, signal_type text, fair_prob real, venue_price real, "
        "edge real, kelly_fraction real)",
        "INSERT INTO mart_edge_signals (signal_id, detected_at, venue_key, signal_type, edge) "
        "VALUES ('s1','2026-06-06','kalshi','ev',0.05)",
    )
    try:
        client = _use(engine)
        assert len(client.get("/edges").json()) == 1
        assert client.get("/edges", params={"signal_type": "arb"}).json() == []
        assert len(client.get("/edges", params={"signal_type": "ev"}).json()) == 1
    finally:
        app.dependency_overrides.clear()


def test_cross_market_returns_rows():
    engine = _engine_with(
        "CREATE TABLE mart_cross_market_prices (question_key text, captured_at text, "
        "kalshi_prob real, polymarket_prob real, sportsbook_consensus_prob real, max_spread real)",
        "INSERT INTO mart_cross_market_prices (question_key, kalshi_prob) VALUES ('q1',0.4)",
    )
    try:
        client = _use(engine)
        body = client.get("/cross-market").json()
        assert len(body) == 1 and body[0]["question_key"] == "q1"
    finally:
        app.dependency_overrides.clear()


def test_history_returns_rows():
    engine = _engine_with(
        "CREATE TABLE mart_market_history (market_id text, resolved_outcome text, "
        "resolution_time text, predicted_prob real, realized integer, clv real)",
        "INSERT INTO mart_market_history (market_id, resolved_outcome) VALUES ('m1','yes')",
    )
    try:
        client = _use(engine)
        body = client.get("/history").json()
        assert len(body) == 1 and body[0]["market_id"] == "m1"
    finally:
        app.dependency_overrides.clear()


def test_market_price_history():
    engine = _engine_with(
        "CREATE TABLE price_history (market_id text, outcome_id text, captured_at text, "
        "price real, bid real, ask real, volume real, liquidity real)",
        "INSERT INTO price_history (market_id, outcome_id, captured_at, price) "
        "VALUES ('m1','o1','2026-06-06',0.5)",
    )
    try:
        client = _use(engine)
        body = client.get("/markets/m1/history").json()
        assert len(body) == 1 and body[0]["price"] == 0.5
        assert client.get("/markets/other/history").json() == []
    finally:
        app.dependency_overrides.clear()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/web/tests/test_endpoints.py -v`
Expected: FAIL — `404` (routes not defined) / import error for the new routers.

- [ ] **Step 3: Write the routers**

Create `app/web/routers/edges.py`:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = ("signal_id, detected_at, venue_key, market_id, outcome_id, signal_type, "
         "fair_prob, venue_price, edge, kelly_fraction")


@router.get("/edges")
def list_edges(
    signal_type: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    sql = f"SELECT {_COLS} FROM mart_edge_signals"
    params = {"lim": limit, "off": offset}
    if signal_type:
        sql += " WHERE signal_type = :st"
        params["st"] = signal_type
    sql += " ORDER BY detected_at DESC LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
```

Create `app/web/routers/cross_market.py`:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = ("question_key, captured_at, kalshi_prob, polymarket_prob, "
         "sportsbook_consensus_prob, max_spread")


@router.get("/cross-market")
def list_cross_market(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    sql = f"SELECT {_COLS} FROM mart_cross_market_prices LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"lim": limit, "off": offset}).mappings().all()
    return [dict(r) for r in rows]
```

Create `app/web/routers/history.py`:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = "market_id, resolved_outcome, resolution_time, predicted_prob, realized, clv"


@router.get("/history")
def list_history(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    sql = f"SELECT {_COLS} FROM mart_market_history LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"lim": limit, "off": offset}).mappings().all()
    return [dict(r) for r in rows]
```

Add the price-history route to `app/web/routers/markets.py` (append at the end of the file):

```python
_PRICE_COLS = "market_id, outcome_id, captured_at, price, bid, ask, volume, liquidity"


@router.get("/markets/{market_id}/history")
def market_price_history(
    market_id: str,
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    sql = (f"SELECT {_PRICE_COLS} FROM price_history WHERE market_id = :mid "
           "ORDER BY captured_at DESC LIMIT :lim OFFSET :off")
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"mid": market_id, "lim": limit, "off": offset}).mappings().all()
    return [dict(r) for r in rows]
```

Wire the new routers in `app/web/main.py` — replace the import + include block:

```python
from app.web.routers import cross_market, edges, history, markets
```
and after `app.include_router(markets.router)` add:
```python
app.include_router(edges.router)
app.include_router(cross_market.router)
app.include_router(history.router)
```

- [ ] **Step 4: Run all web tests to verify they pass**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/web/tests -v`
Expected: PASS (2 from test_markets + 4 from test_endpoints = 6 passed).

- [ ] **Step 5: Commit**

```bash
git add app/web/routers/edges.py app/web/routers/cross_market.py app/web/routers/history.py app/web/routers/markets.py app/web/main.py app/web/tests/test_endpoints.py
git commit -m "feat(web): /edges, /cross-market, /history, /markets/{id}/history endpoints"
```

---

## Task 9: Heroku slug config + local boot check

**Files:** Create `Procfile`; Modify `requirements.txt`, `.slugignore`

- [ ] **Step 1: Write the Procfile**

Create `Procfile`:

```
web: uvicorn app.web.main:app --host 0.0.0.0 --port $PORT
```

- [ ] **Step 2: Set the web slug requirements**

Overwrite `requirements.txt`:

```text
fastapi>=0.115
uvicorn[standard]>=0.32
SQLAlchemy>=2.0
psycopg2-binary>=2.9
```

- [ ] **Step 3: Ensure the slug includes app/web and excludes the heavy dirs**

Read `.slugignore`; make sure it excludes `airflow`, `db`, `docs`, `app/shared`, `scripts`, `traces`, `logs` (the web app needs only `app/web`), and does NOT exclude `app/web`. If `.slugignore` is missing or stale, write it as:

```text
airflow
app/shared
db
docs
scripts
traces
logs
audit_logs
.playwright-mcp
.superpowers
.agents
```

- [ ] **Step 4: Verify the app imports and /health works without a DB**

```bash
cd /c/Users/tkmer/bountygate
python -c "import app.web.main; print('import OK')"
python -c "from fastapi.testclient import TestClient; from app.web.main import app; r=TestClient(app).get('/health'); print(r.status_code, r.json())"
```
Expected: `import OK`; `200 {'status': 'ok'}`.

- [ ] **Step 5: Commit**

```bash
git add Procfile requirements.txt .slugignore
git commit -m "chore(web): Heroku Procfile + slim web requirements + slugignore"
```

---

## Task 10: Deploy to Heroku + verify live

**Files:** none (deploy + verification).

> The Heroku CLI's `ps`/`pg` plugins are broken locally, but `git push heroku main` (slug build) and the **Dashboard** still work. Scaling the dyno is the one manual step.

- [ ] **Step 1: Confirm the heroku git remote exists**

```bash
cd /c/Users/tkmer/bountygate
git remote -v | grep heroku
```
Expected: a `heroku  https://git.heroku.com/bountygate.git` remote. (If absent, STOP and ask the user to add it: `heroku git:remote -a bountygate` — or skip deploy and defer to the frontend spec.)

- [ ] **Step 2: Deploy**

```bash
cd /c/Users/tkmer/bountygate
git push heroku main 2>&1 | tail -25
```
Expected: a successful slug build ending with `Verifying deploy... done` and a release. If the build fails on a dependency, read the error and fix `requirements.txt`.

- [ ] **Step 3: Scale the web dyno to 1 (manual — CLI is broken)**

Ask the user to set the **web** dyno to **1** in the Heroku Dashboard:
**[bountygate → Resources](https://dashboard.heroku.com/apps/bountygate/resources)** → set `web` to 1 → Confirm.

- [ ] **Step 4: Verify the live API responds (after scale-up)**

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://bountygate-<your-domain>.herokuapp.com/health
curl -s https://bountygate-<your-domain>.herokuapp.com/edges
```
(Get the exact app URL from the Dashboard.) Expected: `/health` → `200`; `/edges` → `[]` (empty marts — the expected end state until the transform spec populates them).

---

## Task 11: Final verification

**Files:** none.

- [ ] **Step 1: Migrations all recorded**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select version from schema_migrations order by 1;"
```
Expected: `001`…`006` all present.

- [ ] **Step 2: Extensions + partitioning + part_config**

```bash
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "\dx" 2>&1 | grep -E "pg_partman|pg_trgm|pg_stat_statements"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select parent_table, partition_interval, retention from partman.part_config order by 1;"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select relname from pg_class where relkind='p' order by 1;"
```
Expected: 3 extensions; 3 part_config rows (raw + 2 history); 3 partitioned parents.

- [ ] **Step 3: All contract tables exist (empty except venues)**

```bash
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select tablename from pg_tables where schemaname='public' order by 1;"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select count(*) from venues;"
```
Expected: venues/markets/market_outcomes/sports_events/market_event_links + 3 mart_* + price_history + sportsbook_odds_history + raw_market_snapshots + schema_migrations; `venues` has 3 rows.

- [ ] **Step 4: Web tests green + DAG present**

```bash
cd /c/Users/tkmer/bountygate && python -m pytest app/web/tests -q 2>&1 | tail -2
cd airflow && docker compose run --rm airflow-scheduler airflow dags list 2>&1 | grep partman_maintenance
```
Expected: `6 passed`; `partman_maintenance` listed.

- [ ] **Step 5: Report completion**

Summarize against the spec's §9 success criteria: extensions enabled; raw partitioned (rows intact) + history tables partitioned + maintenance DAG runs; all normalized + marts tables exist; FastAPI app serves valid JSON (empty arrays over empty marts) and is deployed. Note the empty-marts state is expected until the transform spec.
