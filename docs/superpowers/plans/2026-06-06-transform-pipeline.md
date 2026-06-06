# Transform Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Airflow transform pipeline that turns the raw firehose into the normalized tables and the two sportsbook marts, so the live read API serves real data.

**Architecture:** A pure, Airflow-free package `bountygate/transforms/` (parsers + normalize + mart builders) wrapped by two thin Asset-triggered DAGs (`normalize` → `build_marts`). Parsers are stdlib-only (host-testable); mart math reuses the pandas analytics-lib primitives (container-tested); DB writes use SQLAlchemy Core `ON CONFLICT`; polars handles in-memory reshaping/latest-snapshot selection.

**Tech Stack:** Python 3.11, Apache Airflow 3.2 (Asset scheduling, `airflow.sdk`), polars, pandas (analytics lib), SQLAlchemy Core + psycopg2, PostgreSQL 16 (pg_partman), pytest, Docker.

**Spec:** `docs/superpowers/specs/2026-06-06-transform-pipeline-design.md`

---

## Environment notes for the implementer

- **Repo root:** `C:\Users\tkmer\bountygate` (Bash paths `/c/Users/tkmer/bountygate`). `cd` there first; the Bash cwd resets between some calls, so prefer absolute `cd` at the top of each command block.
- **The package lives at** `app/shared/python/bountygate/` and is importable as `bountygate.*`. In the Airflow container it is mounted **read-only** at `/opt/bountygate-shared/python` and already on `PYTHONPATH`.
- **Two test environments:**
  - **Host** (only stdlib + sqlalchemy): run *pure parser tests* with
    `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/transforms/tests/test_parsers.py -v`
    (a `conftest.py` puts the package on `sys.path`).
  - **Container** (has pandas/psycopg2/polars/pytest after Task 2): run *mart-math tests* with
    `cd /c/Users/tkmer/bountygate/airflow && docker compose run --rm -e PYTHONDONTWRITEBYTECODE=1 airflow-scheduler python -m pytest /opt/bountygate-shared/python/bountygate/transforms/tests/<file> -v`
  - **End-to-end** DAG checks use `docker compose run --rm airflow-scheduler airflow dags test <dag_id>` (writes to the **live** bountygate Postgres via `DATABASE_URL`, which is set in the container — normalize/marts data is derived and regenerable, so this is safe).
- **Apply migrations with Docker** (host lacks psycopg2):
  ```bash
  cd /c/Users/tkmer/bountygate
  export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
  MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/<file>.sql
  docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('<version>') ON CONFLICT DO NOTHING;"
  ```
  Record the migration **only after** the file applies cleanly.
- **Current migrations:** `001`–`006` applied. New files are `007`, `008`.
- The three `ingest_*` DAGs are **paused**; `raw_market_snapshots` currently holds ~690 rows.
- **Pinnacle's Odds API bookmaker key is `pinnacle`** (lives in the `eu` region).
- **Analytics primitives** (all in `bountygate.analytics`, pandas-based modules — import them inside the container only):
  - `devig.implied_prob(decimal_odds) -> float` (= `1/decimal`)
  - `devig.multiplicative_devig(p_over, p_under) -> (fair_over, fair_under)` (takes **implied probs**)
  - `consensus.no_vig_consensus(over_odds_list, under_odds_list) -> (fair_over, fair_under) | None` (takes **decimal odds**; `None`/`<=1.0` = missing quote)
  - `ev.edge(fair_prob, soft_decimal_odds) -> float`; `ev.is_actionable(edge, threshold=0.025) -> bool`
  - `kelly.quarter_kelly(p, decimal_odds) -> float`
  - `clv.clv_from_fair(bet_fair_prob, closing_fair_prob) -> float`

---

## File Structure

| Path | Responsibility | Status |
|---|---|---|
| `app/shared/python/bountygate/connectors/registry.py` | flip odds connector to `regions="us,eu"` | Modify |
| `airflow/requirements.txt` | add `polars`, `pytest` | Modify |
| `db/migrations/007_extend_edge_signals.sql` | sportsbook columns on `mart_edge_signals` | Create |
| `db/migrations/008_transform_state.sql` | normalize watermark table | Create |
| `airflow/dags/_archive_pre_pivot/` | destination for legacy `bg_*` DAGs | Create (move) |
| `app/shared/python/bountygate/transforms/__init__.py` | package marker | Create |
| `app/shared/python/bountygate/transforms/parsers/{__init__,kalshi,polymarket,odds}.py` | pure `payload -> rows` per source | Create |
| `app/shared/python/bountygate/transforms/normalize.py` | parse → upsert dims → append time-series + watermark | Create |
| `app/shared/python/bountygate/transforms/marts/{__init__,edge_signals,market_history}.py` | pure mart math | Create |
| `app/shared/python/bountygate/transforms/tests/{conftest,test_parsers,test_edge_signals,test_market_history}.py` | tests | Create |
| `airflow/dags/normalize.py` | Asset-triggered normalize DAG | Create |
| `airflow/dags/build_marts.py` | Asset-triggered marts DAG | Create |

---

## Task 1: Flip odds ingestion to `regions="us,eu"`

**Files:** Modify `app/shared/python/bountygate/connectors/registry.py`; Test `app/shared/python/bountygate/connectors/tests/test_registry_regions.py` (create)

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/bountygate/connectors/tests/test_registry_regions.py`:

```python
import os
import sys

_PKG_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

from bountygate.connectors.registry import get_connector


def test_odds_connector_requests_us_and_eu_regions():
    conn = get_connector("the_odds_api")
    assert conn.regions == "us,eu"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/connectors/tests/test_registry_regions.py -v`
Expected: FAIL — `assert 'us' == 'us,eu'`.

- [ ] **Step 3: Make the change**

In `app/shared/python/bountygate/connectors/registry.py`, change the odds entry:

```python
    OddsApiConnector.source: OddsApiConnector(regions="us,eu"),
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/connectors/tests/test_registry_regions.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/connectors/registry.py app/shared/python/bountygate/connectors/tests/test_registry_regions.py
git commit -m "feat(ingest): request us,eu regions so Pinnacle lands"
```

---

## Task 2: Add polars + pytest to the Airflow image

**Files:** Modify `airflow/requirements.txt`

- [ ] **Step 1: Add the dependencies**

In `airflow/requirements.txt`, under the `# Data processing` section (after `pandas`/`numpy`), add:

```text
polars
```

and add a new section at the end:

```text
# Testing (transform pipeline unit/integration tests run in-container)
pytest
```

- [ ] **Step 2: Rebuild the image**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose build airflow-scheduler 2>&1 | tail -15
```
Expected: build completes; `Successfully built`/`naming to ...` with no pip resolution error.

- [ ] **Step 3: Verify polars + pytest import in the container**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler python -c "import polars, pytest, pandas; print('polars', polars.__version__, 'pytest', pytest.__version__)"
```
Expected: prints versions, no ImportError.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add airflow/requirements.txt
git commit -m "build(airflow): add polars + pytest to the image"
```

---

## Task 3: Migrations 007 (edge-signals columns) + 008 (transform_state)

**Files:** Create `db/migrations/007_extend_edge_signals.sql`, `db/migrations/008_transform_state.sql`

- [ ] **Step 1: Write migration 007**

Create `db/migrations/007_extend_edge_signals.sql`:

```sql
-- Sportsbook signals need event/bookmaker dimensions the Spec-2 contract lacked.
-- Additive + nullable: market_id/outcome_id stay for future prediction-market signals.
ALTER TABLE mart_edge_signals ADD COLUMN IF NOT EXISTS event_id     uuid;
ALTER TABLE mart_edge_signals ADD COLUMN IF NOT EXISTS bookmaker    text;
ALTER TABLE mart_edge_signals ADD COLUMN IF NOT EXISTS market_type  text;
ALTER TABLE mart_edge_signals ADD COLUMN IF NOT EXISTS outcome_name text;
CREATE INDEX IF NOT EXISTS ix_mart_edge_event ON mart_edge_signals (event_id);
```

- [ ] **Step 2: Write migration 008**

Create `db/migrations/008_transform_state.sql`:

```sql
-- Watermark store so the normalize DAG processes only new raw rows.
CREATE TABLE IF NOT EXISTS transform_state (
  name      text PRIMARY KEY,
  watermark timestamptz NOT NULL
);
```

- [ ] **Step 3: Apply both**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
for v in 007_extend_edge_signals 008_transform_state; do
  if MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/$v.sql 2>&1; then
    docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('$v') ON CONFLICT DO NOTHING;"
  else echo "FAILED: $v"; break; fi
done
```
Expected: `ALTER TABLE`×4 + `CREATE INDEX`; `CREATE TABLE`; two `INSERT 0 1`.

- [ ] **Step 4: Verify**

```bash
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select column_name from information_schema.columns where table_name='mart_edge_signals' and column_name in ('event_id','bookmaker','market_type','outcome_name') order by 1;"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select tablename from pg_tables where tablename='transform_state';"
```
Expected: the 4 columns; `transform_state`.

- [ ] **Step 5: Commit**

```bash
git add db/migrations/007_extend_edge_signals.sql db/migrations/008_transform_state.sql
git commit -m "feat(db): extend mart_edge_signals + add transform_state watermark"
```

---

## Task 4: Archive legacy bg_* DAGs

**Files:** Move legacy DAG files into `airflow/dags/_archive_pre_pivot/`

- [ ] **Step 1: Move the legacy files out of the dags folder**

```bash
cd /c/Users/tkmer/bountygate/airflow/dags
mkdir -p _archive_pre_pivot
git mv bg_analysis_sheets_dag.py bg_arb_pipeline.py bg_arbitrage_player_props.py \
  bg_arbitrage_sheets_dag.py bg_closing_line.py bg_dimensional_model.py \
  bg_game_arb_pipeline.py bg_injuries.py bg_marts.py bg_methodology.py \
  bg_normalization.py bg_results.py bg_unified_analysis_dag.py bg_unified_dag.py \
  bg_weather.py bg_analytics_lib bg_arb_pipeline_lib bg_game_arb_pipeline_lib \
  update_underdog_outlier_analysis.py prepared_sql service_account.json \
  _archive_pre_pivot/
```
(If `git mv` reports a path is untracked, use plain `mv <that path> _archive_pre_pivot/` for it, then continue.)

- [ ] **Step 2: Stop Airflow from loading the archive folder**

Create `airflow/dags/_archive_pre_pivot/.airflowignore`:

```text
*
```
(`.airflowignore` patterns are regexes matched against paths; `*` excludes everything in this folder from DAG parsing.)

- [ ] **Step 3: Verify no import errors and only the intended DAGs remain**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler airflow dags list-import-errors 2>&1 | tail -20
docker compose run --rm airflow-scheduler airflow dags list 2>&1 | grep -E "ingest_|partman_maintenance|bg_" || true
```
Expected: "No data found" / no import errors; the `bg_*` DAGs are **gone** from `dags list`; `ingest_*` and `partman_maintenance` remain.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add -A airflow/dags
git commit -m "chore(dags): archive legacy bg_* arb pipeline out of the scheduler"
```

---

## Task 5: transforms package + Kalshi parser (TDD)

**Files:** Create `app/shared/python/bountygate/transforms/__init__.py`, `transforms/parsers/__init__.py`, `transforms/parsers/kalshi.py`, `transforms/tests/__init__.py`, `transforms/tests/conftest.py`, `transforms/tests/test_parsers.py`

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/bountygate/transforms/tests/conftest.py`:

```python
import os
import sys

# app/shared/python on sys.path so `import bountygate.transforms...` resolves on host.
_PKG_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)
```

Create `app/shared/python/bountygate/transforms/tests/test_parsers.py`:

```python
from bountygate.transforms.parsers.kalshi import parse_kalshi


def test_parse_kalshi_market_outcomes_and_prices():
    payload = {
        "title": "Will Kansas City win?",
        "ticker": "KXNFLGAME-26SEP14DENKC-KC",
        "series_ticker": "KXNFLGAME",
        "status": "active",
        "yes_bid": 0.53, "yes_ask": 0.64,
        "no_bid": 0.36, "no_ask": 0.47,
        "open_interest": 213.41, "liquidity_dollars": 0.0,
    }
    out = parse_kalshi(payload)
    assert out["market"]["venue_key"] == "kalshi"
    assert out["market"]["external_id"] == "KXNFLGAME-26SEP14DENKC-KC"
    assert out["market"]["title"] == "Will Kansas City win?"
    assert out["market"]["category"] == "KXNFLGAME"
    assert out["market"]["status"] == "active"
    names = [o["outcome_name"] for o in out["outcomes"]]
    assert names == ["Yes", "No"]
    yes = next(o for o in out["outcomes"] if o["outcome_name"] == "Yes")
    assert abs(yes["last_price"] - 0.585) < 1e-9   # (0.53+0.64)/2
    yes_price = next(p for p in out["prices"] if p["outcome_name"] == "Yes")
    assert yes_price["bid"] == 0.53 and yes_price["ask"] == 0.64
    assert abs(yes_price["price"] - 0.585) < 1e-9
    assert yes_price["volume"] == 213.41


def test_parse_kalshi_missing_quote_yields_none_price():
    payload = {"ticker": "KX-X", "title": "t", "series_ticker": "S", "status": "active",
               "yes_bid": None, "yes_ask": None, "no_bid": 0.4, "no_ask": 0.5}
    out = parse_kalshi(payload)
    yes = next(o for o in out["outcomes"] if o["outcome_name"] == "Yes")
    assert yes["last_price"] is None
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/transforms/tests/test_parsers.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms'`.

- [ ] **Step 3: Write the package + parser**

Create `app/shared/python/bountygate/transforms/__init__.py` (empty).
Create `app/shared/python/bountygate/transforms/parsers/__init__.py` (empty).
Create `app/shared/python/bountygate/transforms/tests/__init__.py` (empty).

Create `app/shared/python/bountygate/transforms/parsers/kalshi.py`:

```python
"""Pure parser: a Kalshi `market` raw payload -> normalized rows (no I/O)."""
from __future__ import annotations


def _mid(bid, ask):
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2


def parse_kalshi(payload: dict) -> dict:
    """Return {'market': {...}, 'outcomes': [...], 'prices': [...]} for one Kalshi market."""
    ext = payload.get("ticker")
    market = {
        "venue_key": "kalshi",
        "external_id": ext,
        "title": payload.get("title"),
        "category": payload.get("series_ticker"),
        "status": payload.get("status"),
        "open_time": None,
        "close_time": None,
        "resolved_outcome": None,
        "resolution_time": None,
    }
    yes_mid = _mid(payload.get("yes_bid"), payload.get("yes_ask"))
    no_mid = _mid(payload.get("no_bid"), payload.get("no_ask"))
    volume = payload.get("open_interest")
    liquidity = payload.get("liquidity_dollars")
    outcomes = [
        {"outcome_name": "Yes", "outcome_index": 0, "last_price": yes_mid},
        {"outcome_name": "No", "outcome_index": 1, "last_price": no_mid},
    ]
    prices = [
        {"outcome_name": "Yes", "price": yes_mid, "bid": payload.get("yes_bid"),
         "ask": payload.get("yes_ask"), "volume": volume, "liquidity": liquidity},
        {"outcome_name": "No", "price": no_mid, "bid": payload.get("no_bid"),
         "ask": payload.get("no_ask"), "volume": volume, "liquidity": liquidity},
    ]
    return {"market": market, "outcomes": outcomes, "prices": prices}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/transforms/tests/test_parsers.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/__init__.py app/shared/python/bountygate/transforms/parsers/ app/shared/python/bountygate/transforms/tests/
git commit -m "feat(transforms): package scaffold + Kalshi parser"
```

---

## Task 6: Polymarket parser (TDD)

**Files:** Create `app/shared/python/bountygate/transforms/parsers/polymarket.py`; Modify `app/shared/python/bountygate/transforms/tests/test_parsers.py`

- [ ] **Step 1: Add the failing test**

Append to `app/shared/python/bountygate/transforms/tests/test_parsers.py`:

```python
from bountygate.transforms.parsers.polymarket import parse_polymarket


def test_parse_polymarket_zips_outcomes_and_prices():
    payload = {
        "condition_id": "0xabc", "question": "New Rihanna Album before GTA VI?",
        "slug": "rihanna", "active": True, "closed": False,
        "volume": 818640.13, "liquidity": 19582.37, "end_date": "2026-07-31T12:00:00Z",
        "outcomes": ["Yes", "No"], "outcome_prices": [0.545, 0.455],
    }
    out = parse_polymarket(payload)
    assert out["market"]["venue_key"] == "polymarket"
    assert out["market"]["external_id"] == "0xabc"
    assert out["market"]["title"] == "New Rihanna Album before GTA VI?"
    assert out["market"]["status"] == "active"
    assert out["market"]["close_time"] == "2026-07-31T12:00:00Z"
    yes = next(o for o in out["outcomes"] if o["outcome_name"] == "Yes")
    assert yes["outcome_index"] == 0 and yes["last_price"] == 0.545
    yes_price = next(p for p in out["prices"] if p["outcome_name"] == "Yes")
    assert yes_price["price"] == 0.545 and yes_price["volume"] == 818640.13
    assert yes_price["liquidity"] == 19582.37


def test_parse_polymarket_closed_status():
    payload = {"condition_id": "0xd", "question": "q", "active": False, "closed": True,
               "outcomes": ["Yes", "No"], "outcome_prices": [1.0, 0.0]}
    out = parse_polymarket(payload)
    assert out["market"]["status"] == "closed"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/transforms/tests/test_parsers.py -k polymarket -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms.parsers.polymarket'`.

- [ ] **Step 3: Write the parser**

Create `app/shared/python/bountygate/transforms/parsers/polymarket.py`:

```python
"""Pure parser: a Polymarket `market` raw payload -> normalized rows (no I/O)."""
from __future__ import annotations


def _status(payload: dict) -> str | None:
    if payload.get("closed"):
        return "closed"
    if payload.get("active"):
        return "active"
    return None


def parse_polymarket(payload: dict) -> dict:
    """Return {'market': {...}, 'outcomes': [...], 'prices': [...]} for one Polymarket market."""
    market = {
        "venue_key": "polymarket",
        "external_id": payload.get("condition_id"),
        "title": payload.get("question"),
        "category": None,
        "status": _status(payload),
        "open_time": None,
        "close_time": payload.get("end_date"),
        "resolved_outcome": None,
        "resolution_time": None,
    }
    volume = payload.get("volume")
    liquidity = payload.get("liquidity")
    names = payload.get("outcomes") or []
    prices_in = payload.get("outcome_prices") or []
    outcomes, prices = [], []
    for idx, name in enumerate(names):
        price = prices_in[idx] if idx < len(prices_in) else None
        outcomes.append({"outcome_name": name, "outcome_index": idx, "last_price": price})
        prices.append({"outcome_name": name, "price": price, "bid": None, "ask": None,
                       "volume": volume, "liquidity": liquidity})
    return {"market": market, "outcomes": outcomes, "prices": prices}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/transforms/tests/test_parsers.py -v`
Expected: PASS (4 passed total).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/parsers/polymarket.py app/shared/python/bountygate/transforms/tests/test_parsers.py
git commit -m "feat(transforms): Polymarket parser"
```

---

## Task 7: Odds parser (TDD)

**Files:** Create `app/shared/python/bountygate/transforms/parsers/odds.py`; Modify `app/shared/python/bountygate/transforms/tests/test_parsers.py`

- [ ] **Step 1: Add the failing test**

Append to `app/shared/python/bountygate/transforms/tests/test_parsers.py`:

```python
from bountygate.transforms.parsers.odds import parse_odds_line


def test_parse_odds_line_event_and_odds():
    payload = {
        "event_id": "evt1", "sport_key": "baseball_mlb",
        "home_team": "San Diego Padres", "away_team": "New York Mets",
        "commence_time": "2026-06-07T02:11:00Z", "market": "h2h", "bookmaker": "pinnacle",
        "outcomes": [{"name": "New York Mets", "price": 1.82},
                     {"name": "San Diego Padres", "price": 2.04}],
    }
    out = parse_odds_line(payload)
    assert out["event"]["source_event_id"] == "evt1"
    assert out["event"]["sport_key"] == "baseball_mlb"
    assert out["event"]["home_team"] == "San Diego Padres"
    assert len(out["odds"]) == 2
    mets = next(o for o in out["odds"] if o["outcome_name"] == "New York Mets")
    assert mets["bookmaker"] == "pinnacle" and mets["market_type"] == "h2h"
    assert mets["decimal_price"] == 1.82
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/transforms/tests/test_parsers.py -k odds -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms.parsers.odds'`.

- [ ] **Step 3: Write the parser**

Create `app/shared/python/bountygate/transforms/parsers/odds.py`:

```python
"""Pure parser: a The-Odds-API `odds_line` raw payload -> event + odds rows (no I/O)."""
from __future__ import annotations


def parse_odds_line(payload: dict) -> dict:
    """Return {'event': {...}, 'odds': [...]} for one bookmaker's line on one market."""
    event = {
        "source_event_id": payload.get("event_id"),
        "sport_key": payload.get("sport_key"),
        "commence_time": payload.get("commence_time"),
        "home_team": payload.get("home_team"),
        "away_team": payload.get("away_team"),
    }
    market_type = payload.get("market")
    bookmaker = payload.get("bookmaker")
    odds = []
    for o in payload.get("outcomes") or []:
        odds.append({
            "market_type": market_type,
            "bookmaker": bookmaker,
            "outcome_name": o.get("name"),
            "decimal_price": o.get("price"),
        })
    return {"event": event, "odds": odds}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cd /c/Users/tkmer/bountygate && python -m pytest app/shared/python/bountygate/transforms/tests/test_parsers.py -v`
Expected: PASS (5 passed total).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/parsers/odds.py app/shared/python/bountygate/transforms/tests/test_parsers.py
git commit -m "feat(transforms): The Odds API parser"
```

---

## Task 8: `mart_edge_signals` math (TDD, container)

**Files:** Create `app/shared/python/bountygate/transforms/marts/__init__.py`, `transforms/marts/edge_signals.py`, `transforms/tests/test_edge_signals.py`

This is pure math reusing the analytics primitives. It runs **in the container** (needs the pandas-based lib).

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/bountygate/transforms/tests/test_edge_signals.py`:

```python
from bountygate.transforms.marts.edge_signals import compute_edge_signals


def _row(event_id, book, name, price, mt="h2h"):
    return {"event_id": event_id, "market_type": mt, "bookmaker": book,
            "outcome_name": name, "decimal_price": price}


def test_pinnacle_anchored_ev_signal():
    # Pinnacle symmetric 1.9091/1.9091 -> fair 0.5/0.5. Soft book offers 2.2 on Mets -> +EV.
    rows = [
        _row("e1", "pinnacle", "Mets", 1.9091), _row("e1", "pinnacle", "Padres", 1.9091),
        _row("e1", "softbook", "Mets", 2.20),  _row("e1", "softbook", "Padres", 1.74),
    ]
    out = compute_edge_signals(rows, threshold=0.025)
    ev = [s for s in out if s["signal_type"] == "ev"]
    mets_ev = next(s for s in ev if s["bookmaker"] == "softbook" and s["outcome_name"] == "Mets")
    assert abs(mets_ev["fair_prob"] - 0.5) < 1e-3
    assert mets_ev["venue_price"] == 2.20
    assert mets_ev["edge"] > 0.025          # 0.5*2.2 - 1 = 0.10
    assert mets_ev["kelly_fraction"] > 0
    # Pinnacle itself is never a soft/EV book
    assert all(s["bookmaker"] != "pinnacle" for s in ev)


def test_consensus_fallback_when_no_pinnacle():
    # No Pinnacle -> consensus fair (~0.5 each). bookA at 2.10/2.10 yields +EV vs that fair.
    rows = [
        _row("e2", "bookA", "Over", 2.10), _row("e2", "bookA", "Under", 2.10),
        _row("e2", "bookB", "Over", 1.95), _row("e2", "bookB", "Under", 1.95),
    ]
    out = compute_edge_signals(rows, threshold=0.0)
    assert out, "expected signals via consensus fallback"
    assert all(0.0 <= s["fair_prob"] <= 1.0 for s in out)


def test_arb_signal_detected():
    # 1/2.10 + 1/2.10 = 0.952 < 1 -> guaranteed-profit arb across the two outcomes.
    rows = [
        _row("e3", "pinnacle", "A", 1.90), _row("e3", "pinnacle", "B", 1.90),
        _row("e3", "bookX", "A", 2.10),    _row("e3", "bookY", "B", 2.10),
    ]
    out = compute_edge_signals(rows, threshold=0.025)
    arbs = [s for s in out if s["signal_type"] == "arb"]
    assert arbs, "expected an arb signal"
    assert all(s["edge"] > 0 for s in arbs)


def test_skips_non_two_way_markets():
    rows = [_row("e4", "pinnacle", "A", 2.0), _row("e4", "pinnacle", "B", 3.0),
            _row("e4", "pinnacle", "C", 4.0)]
    assert compute_edge_signals(rows) == []
```

- [ ] **Step 2: Run it to verify it fails**

Run:
```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm -e PYTHONDONTWRITEBYTECODE=1 airflow-scheduler python -m pytest /opt/bountygate-shared/python/bountygate/transforms/tests/test_edge_signals.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms.marts'`.

- [ ] **Step 3: Write the mart math**

Create `app/shared/python/bountygate/transforms/marts/__init__.py` (empty).

Create `app/shared/python/bountygate/transforms/marts/edge_signals.py`:

```python
"""Pure computation of sportsbook EV + arb signals from latest odds rows.

Input rows: dicts with keys event_id, market_type, bookmaker, outcome_name,
decimal_price (one row per book/outcome, already reduced to the latest snapshot).
Output: list of signal dicts ready to insert into mart_edge_signals.
Reuses the venue-agnostic analytics primitives.
"""
from __future__ import annotations

from collections import defaultdict

from bountygate.analytics.consensus import no_vig_consensus
from bountygate.analytics.devig import implied_prob, multiplicative_devig
from bountygate.analytics.ev import edge as ev_edge
from bountygate.analytics.ev import is_actionable
from bountygate.analytics.kelly import quarter_kelly

PINNACLE = "pinnacle"


def _fair_probs(group_by_book: dict, names: list[str]):
    """Return {outcome_name: fair_prob} via Pinnacle no-vig, else multi-book consensus.

    names is the ordered two-way pair [name0, name1]. group_by_book maps
    bookmaker -> {outcome_name: decimal_price}.
    """
    n0, n1 = names
    pin = group_by_book.get(PINNACLE)
    if pin and pin.get(n0) and pin.get(n1):
        p0 = implied_prob(pin[n0])
        p1 = implied_prob(pin[n1])
        f0, f1 = multiplicative_devig(p0, p1)
        return {n0: f0, n1: f1}
    over_odds, under_odds = [], []
    for book, prices in group_by_book.items():
        if book == PINNACLE:
            continue
        over_odds.append(prices.get(n0))
        under_odds.append(prices.get(n1))
    consensus = no_vig_consensus(over_odds, under_odds)
    if consensus is None:
        return None
    return {n0: consensus[0], n1: consensus[1]}


def compute_edge_signals(rows: list[dict], *, threshold: float = 0.025) -> list[dict]:
    # group rows by (event_id, market_type) -> book -> {outcome_name: price}
    groups: dict[tuple, dict] = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        price = r.get("decimal_price")
        if not price or price <= 1.0:
            continue
        groups[(r["event_id"], r["market_type"])][r["bookmaker"]][r["outcome_name"]] = price

    signals: list[dict] = []
    for (event_id, market_type), by_book in groups.items():
        names = sorted({n for prices in by_book.values() for n in prices})
        if len(names) != 2:          # two-way markets only (v1)
            continue
        fair = _fair_probs(by_book, names)
        if fair is None:
            continue

        # EV: each non-Pinnacle book/outcome vs the fair prob
        for book, prices in by_book.items():
            if book == PINNACLE:
                continue
            for name in names:
                price = prices.get(name)
                if not price:
                    continue
                e = ev_edge(fair[name], price)
                if is_actionable(e, threshold):
                    signals.append({
                        "event_id": event_id, "market_type": market_type,
                        "bookmaker": book, "outcome_name": name, "signal_type": "ev",
                        "fair_prob": fair[name], "venue_price": price,
                        "edge": e, "kelly_fraction": quarter_kelly(fair[name], price),
                    })

        # Arb: best price per outcome across ALL books; profit if inverse-sum < 1
        best = {}
        for name in names:
            candidates = [(prices[name], book) for book, prices in by_book.items() if prices.get(name)]
            if candidates:
                best[name] = max(candidates)        # (price, book)
        if len(best) == 2:
            inv_sum = sum(1.0 / best[n][0] for n in names)
            if inv_sum < 1.0:
                margin = 1.0 - inv_sum
                for name in names:
                    price, book = best[name]
                    signals.append({
                        "event_id": event_id, "market_type": market_type,
                        "bookmaker": book, "outcome_name": name, "signal_type": "arb",
                        "fair_prob": fair[name], "venue_price": price,
                        "edge": margin, "kelly_fraction": None,
                    })
    return signals
```

- [ ] **Step 4: Run it to verify it passes**

Run:
```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm -e PYTHONDONTWRITEBYTECODE=1 airflow-scheduler python -m pytest /opt/bountygate-shared/python/bountygate/transforms/tests/test_edge_signals.py -v
```
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/marts/__init__.py app/shared/python/bountygate/transforms/marts/edge_signals.py app/shared/python/bountygate/transforms/tests/test_edge_signals.py
git commit -m "feat(transforms): sportsbook EV + arb edge-signal math"
```

---

## Task 9: `mart_market_history` math (TDD, container)

**Files:** Create `app/shared/python/bountygate/transforms/marts/market_history.py`, `transforms/tests/test_market_history.py`

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/bountygate/transforms/tests/test_market_history.py`:

```python
from bountygate.transforms.marts.market_history import compute_market_history


def test_clv_and_realized_for_resolved_market():
    market = {
        "market_id": "m1", "resolved_outcome": "Yes",
        "close_time": "2026-06-06T12:00:00Z", "resolution_time": "2026-06-06T12:00:00Z",
        "tracked_outcome": "Yes",
    }
    # price points (captured_at, price) for the tracked outcome
    points = [
        ("2026-06-06T09:00:00Z", 0.40),   # >= 1h before close -> predicted
        ("2026-06-06T11:30:00Z", 0.55),   # < 1h before close
        ("2026-06-06T11:59:00Z", 0.60),   # closing (last before close)
    ]
    out = compute_market_history([market], {"m1": points})
    assert len(out) == 1
    row = out[0]
    assert row["market_id"] == "m1"
    assert row["predicted_prob"] == 0.40
    assert row["realized"] is True
    # clv_from_fair(0.40, 0.60) = 0.60/0.40 - 1 = 0.5
    assert abs(row["clv"] - 0.5) < 1e-9


def test_skips_market_with_no_prior_horizon_point():
    market = {"market_id": "m2", "resolved_outcome": "No", "close_time": "2026-06-06T12:00:00Z",
              "resolution_time": "2026-06-06T12:00:00Z", "tracked_outcome": "Yes"}
    points = [("2026-06-06T11:40:00Z", 0.7)]   # only inside the 1h horizon
    out = compute_market_history([market], {"m2": points})
    assert out == []
```

- [ ] **Step 2: Run it to verify it fails**

Run:
```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm -e PYTHONDONTWRITEBYTECODE=1 airflow-scheduler python -m pytest /opt/bountygate-shared/python/bountygate/transforms/tests/test_market_history.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms.marts.market_history'`.

- [ ] **Step 3: Write the mart math**

Create `app/shared/python/bountygate/transforms/marts/market_history.py`:

```python
"""Pure computation of resolved-market calibration / CLV rows for mart_market_history.

Inputs:
  resolved_markets: list of dicts with market_id, resolved_outcome, close_time,
      resolution_time, tracked_outcome (the outcome whose price series we track).
  prices_by_market: {market_id: [(captured_at_iso, price), ...]} for the tracked outcome.
Output: list of mart_market_history row dicts.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from bountygate.analytics.clv import clv_from_fair

_HORIZON = timedelta(hours=1)


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def compute_market_history(resolved_markets: list[dict], prices_by_market: dict) -> list[dict]:
    out: list[dict] = []
    for m in resolved_markets:
        points = sorted(
            ((_parse(ts), price) for ts, price in prices_by_market.get(m["market_id"], [])),
            key=lambda x: x[0],
        )
        if not points:
            continue
        close = _parse(m["close_time"])
        prior = [p for p in points if p[0] <= close - _HORIZON]
        before_close = [p for p in points if p[0] <= close]
        if not prior or not before_close:
            continue
        predicted_prob = prior[-1][1]            # last point >= 1h before close
        closing_prob = before_close[-1][1]       # last point before close
        out.append({
            "market_id": m["market_id"],
            "resolved_outcome": m.get("resolved_outcome"),
            "resolution_time": m.get("resolution_time"),
            "predicted_prob": predicted_prob,
            "realized": m.get("resolved_outcome") == m.get("tracked_outcome"),
            "clv": clv_from_fair(predicted_prob, closing_prob),
        })
    return out
```

- [ ] **Step 4: Run it to verify it passes**

Run:
```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm -e PYTHONDONTWRITEBYTECODE=1 airflow-scheduler python -m pytest /opt/bountygate-shared/python/bountygate/transforms/tests/test_market_history.py -v
```
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/marts/market_history.py app/shared/python/bountygate/transforms/tests/test_market_history.py
git commit -m "feat(transforms): resolved-market CLV / calibration math"
```

---

## Task 10: `normalize.py` writer + DAG (end-to-end against live DB)

**Files:** Create `app/shared/python/bountygate/transforms/normalize.py`, `airflow/dags/normalize.py`

`normalize.py` is the I/O orchestrator: read new raw rows (since watermark) → parse → upsert dimensions → read back the `outcome_id` map → append idempotent time-series → advance the watermark. It is verified end-to-end via `airflow dags test` (host can't import psycopg2/pandas). Uses SQLAlchemy Core + the Postgres dialect's `insert(...).on_conflict_do_*`.

- [ ] **Step 1: Write `normalize.py`**

Create `app/shared/python/bountygate/transforms/normalize.py`:

```python
"""Raw -> normalized. Reads new raw_market_snapshots rows since the stored watermark,
parses by source, upserts dimension tables, and appends the partitioned time-series
idempotently. I/O lives here; the per-source row shaping lives in parsers/."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from bountygate.transforms.parsers.kalshi import parse_kalshi
from bountygate.transforms.parsers.polymarket import parse_polymarket
from bountygate.transforms.parsers.odds import parse_odds_line

WATERMARK_NAME = "normalize"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def run_normalize() -> dict:
    """Process all raw rows newer than the watermark. Returns a counts summary."""
    engine = _engine()
    try:
        with engine.begin() as conn:
            wm = conn.execute(
                text("SELECT watermark FROM transform_state WHERE name = :n"),
                {"n": WATERMARK_NAME},
            ).scalar() or _EPOCH
            raw = conn.execute(text(
                "SELECT source, captured_at, payload FROM raw_market_snapshots "
                "WHERE captured_at > :wm ORDER BY captured_at"), {"wm": wm}).mappings().all()

            counts = {"markets": 0, "outcomes": 0, "prices": 0, "events": 0, "odds": 0}
            max_ts = wm
            for row in raw:
                src, captured_at, payload = row["source"], row["captured_at"], row["payload"]
                if captured_at and captured_at > max_ts:
                    max_ts = captured_at
                if src in ("kalshi", "polymarket"):
                    parsed = parse_kalshi(payload) if src == "kalshi" else parse_polymarket(payload)
                    counts["markets"] += _upsert_market(conn, parsed["market"])
                    oid_map = _upsert_outcomes(conn, parsed["market"], parsed["outcomes"])
                    counts["outcomes"] += len(oid_map)
                    counts["prices"] += _append_prices(conn, oid_map, parsed["prices"], captured_at)
                elif src == "the_odds_api":
                    parsed = parse_odds_line(payload)
                    eid = _upsert_event(conn, parsed["event"])
                    counts["events"] += 1
                    counts["odds"] += _append_odds(conn, eid, parsed["odds"], captured_at)

            if raw:
                conn.execute(text(
                    "INSERT INTO transform_state(name, watermark) VALUES (:n, :w) "
                    "ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark"),
                    {"n": WATERMARK_NAME, "w": max_ts})
        return counts
    finally:
        engine.dispose()


def _upsert_market(conn, m: dict) -> int:
    conn.execute(text(
        "INSERT INTO markets (venue_key, external_id, title, category, status, "
        "  open_time, close_time, resolved_outcome, resolution_time, updated_at) "
        "VALUES (:venue_key, :external_id, :title, :category, :status, "
        "  :open_time, :close_time, :resolved_outcome, :resolution_time, now()) "
        "ON CONFLICT (venue_key, external_id) DO UPDATE SET "
        "  title=EXCLUDED.title, category=EXCLUDED.category, status=EXCLUDED.status, "
        "  close_time=EXCLUDED.close_time, resolved_outcome=EXCLUDED.resolved_outcome, "
        "  resolution_time=EXCLUDED.resolution_time, updated_at=now()"), m)
    return 1


def _upsert_outcomes(conn, market: dict, outcomes: list[dict]) -> dict:
    """Upsert outcomes; return {outcome_name: outcome_id}."""
    oid_map = {}
    for o in outcomes:
        conn.execute(text(
            "INSERT INTO market_outcomes (market_id, outcome_name, outcome_index, last_price, last_seen) "
            "SELECT m.market_id, :outcome_name, :outcome_index, :last_price, now() "
            "FROM markets m WHERE m.venue_key=:venue_key AND m.external_id=:external_id "
            "ON CONFLICT (market_id, outcome_name) DO UPDATE SET "
            "  last_price=EXCLUDED.last_price, last_seen=now()"),
            {**o, "venue_key": market["venue_key"], "external_id": market["external_id"]})
        oid = conn.execute(text(
            "SELECT o.outcome_id FROM market_outcomes o JOIN markets m ON m.market_id=o.market_id "
            "WHERE m.venue_key=:venue_key AND m.external_id=:external_id AND o.outcome_name=:outcome_name"),
            {"venue_key": market["venue_key"], "external_id": market["external_id"],
             "outcome_name": o["outcome_name"]}).scalar()
        oid_map[o["outcome_name"]] = oid
    return oid_map


def _append_prices(conn, oid_map: dict, prices: list[dict], captured_at) -> int:
    n = 0
    for p in prices:
        oid = oid_map.get(p["outcome_name"])
        if oid is None:
            continue
        res = conn.execute(text(
            "INSERT INTO price_history (market_id, outcome_id, captured_at, price, bid, ask, volume, liquidity) "
            "SELECT o.market_id, :oid, :captured_at, :price, :bid, :ask, :volume, :liquidity "
            "FROM market_outcomes o WHERE o.outcome_id = :oid "
            "ON CONFLICT (outcome_id, captured_at) DO NOTHING"),
            {"oid": oid, "captured_at": captured_at, "price": p["price"], "bid": p["bid"],
             "ask": p["ask"], "volume": p["volume"], "liquidity": p["liquidity"]})
        n += res.rowcount or 0
    return n


def _upsert_event(conn, e: dict):
    return conn.execute(text(
        "INSERT INTO sports_events (source_event_id, sport_key, commence_time, home_team, away_team) "
        "VALUES (:source_event_id, :sport_key, :commence_time, :home_team, :away_team) "
        "ON CONFLICT (source_event_id) DO UPDATE SET "
        "  sport_key=EXCLUDED.sport_key, commence_time=EXCLUDED.commence_time, "
        "  home_team=EXCLUDED.home_team, away_team=EXCLUDED.away_team "
        "RETURNING event_id"), e).scalar()


def _append_odds(conn, event_id, odds: list[dict], captured_at) -> int:
    n = 0
    for o in odds:
        res = conn.execute(text(
            "INSERT INTO sportsbook_odds_history "
            "  (event_id, market_type, bookmaker, outcome_name, captured_at, decimal_price) "
            "VALUES (:event_id, :market_type, :bookmaker, :outcome_name, :captured_at, :decimal_price) "
            "ON CONFLICT (event_id, market_type, bookmaker, outcome_name, captured_at) DO NOTHING"),
            {"event_id": event_id, "captured_at": captured_at, **o})
        n += res.rowcount or 0
    return n
```

- [ ] **Step 2: Add the idempotency unique indexes (migration 009)**

The `ON CONFLICT` targets above need matching unique constraints on the partitioned tables. Create `db/migrations/009_history_unique.sql`:

```sql
-- Unique keys backing the idempotent ON CONFLICT appends in normalize.
-- On partitioned tables the unique index must include the partition key (captured_at).
CREATE UNIQUE INDEX IF NOT EXISTS uq_price_hist_outcome_time
  ON price_history (outcome_id, captured_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sb_odds_natural
  ON sportsbook_odds_history (event_id, market_type, bookmaker, outcome_name, captured_at);
```

Apply it:
```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
if MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/009_history_unique.sql 2>&1; then
  docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('009_history_unique') ON CONFLICT DO NOTHING;"
else echo FAILED; fi
```
Expected: `CREATE INDEX` ×2 (pg_partman propagates the unique index to existing + future partitions).

- [ ] **Step 3: Write the normalize DAG**

Create `airflow/dags/normalize.py`:

```python
"""raw_market_snapshots -> normalized tables. Asset-triggered by the raw firehose."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.transforms.normalize import run_normalize

RAW_ASSET = Asset(name="raw_market_snapshots")
NORMALIZED_ASSETS = [
    Asset(name="markets"), Asset(name="market_outcomes"), Asset(name="price_history"),
    Asset(name="sports_events"), Asset(name="sportsbook_odds_history"),
]


@dag(
    dag_id="normalize",
    schedule=[RAW_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["transform", "normalize"],
)
def normalize():
    @task(outlets=NORMALIZED_ASSETS)
    def normalize_raw() -> dict:
        counts = run_normalize()
        print(f"[normalize] {counts}")
        return counts

    normalize_raw()


dag = normalize()
```

- [ ] **Step 4: Import check + first run against the live DB**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler airflow dags list-import-errors 2>&1 | grep -i normalize || echo "no import errors"
docker compose run --rm airflow-scheduler airflow dags test normalize 2>&1 | grep -iE "\[normalize\] \{|state=success|state=failed|Traceback|Error" | tail -10
```
Expected: no import errors; a `[normalize] {...}` counts line with non-zero markets/events; `state=success`.

- [ ] **Step 5: Verify normalized tables populated + idempotency (run twice)**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
echo "after run 1:"; docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select 'markets',count(*) from markets union all select 'outcomes',count(*) from market_outcomes union all select 'price_history',count(*) from price_history union all select 'events',count(*) from sports_events union all select 'odds',count(*) from sportsbook_odds_history;"
# reset watermark and re-run to confirm appends don't duplicate
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "DELETE FROM transform_state WHERE name='normalize';"
cd airflow && docker compose run --rm airflow-scheduler airflow dags test normalize >/dev/null 2>&1
cd /c/Users/tkmer/bountygate
echo "after run 2 (should match run 1):"; docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select 'price_history',count(*) from price_history union all select 'odds',count(*) from sportsbook_odds_history;"
```
Expected: counts > 0 after run 1; `price_history`/`odds` counts **identical** after run 2 (idempotent appends).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/normalize.py airflow/dags/normalize.py db/migrations/009_history_unique.sql
git commit -m "feat(transforms): normalize DAG (raw -> normalized, idempotent)"
```

---

## Task 11: `build_marts.py` DAG (end-to-end against live DB)

**Files:** Create `airflow/dags/build_marts.py`; add mart-writer helpers to `app/shared/python/bountygate/transforms/marts/__init__.py`

The DAG reads the normalized tables, reduces to the latest snapshot per book/outcome with **polars**, calls the pure mart math, full-refreshes `mart_edge_signals`, and upserts `mart_market_history`.

- [ ] **Step 1: Write the mart writers**

Replace `app/shared/python/bountygate/transforms/marts/__init__.py` with:

```python
"""DB I/O for the marts: read normalized -> compute -> write. polars for reshaping."""
from __future__ import annotations

import os

import polars as pl
from sqlalchemy import create_engine, text

from bountygate.transforms.marts.edge_signals import compute_edge_signals
from bountygate.transforms.marts.market_history import compute_market_history


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def _latest_odds_rows(conn) -> list[dict]:
    rows = conn.execute(text(
        "SELECT event_id::text AS event_id, market_type, bookmaker, outcome_name, "
        "       decimal_price, captured_at "
        "FROM sportsbook_odds_history")).mappings().all()
    if not rows:
        return []
    df = pl.DataFrame([dict(r) for r in rows])
    # latest row per (event, market_type, bookmaker, outcome)
    latest = (df.sort("captured_at")
                .group_by(["event_id", "market_type", "bookmaker", "outcome_name"])
                .last())
    return latest.select(
        ["event_id", "market_type", "bookmaker", "outcome_name", "decimal_price"]
    ).to_dicts()


def build_edge_signals() -> int:
    engine = _engine()
    try:
        with engine.begin() as conn:
            rows = _latest_odds_rows(conn)
            signals = compute_edge_signals(rows)
            conn.execute(text("TRUNCATE mart_edge_signals"))
            for s in signals:
                conn.execute(text(
                    "INSERT INTO mart_edge_signals "
                    "  (detected_at, event_id, bookmaker, market_type, outcome_name, "
                    "   signal_type, fair_prob, venue_price, edge, kelly_fraction) "
                    "VALUES (now(), :event_id, :bookmaker, :market_type, :outcome_name, "
                    "   :signal_type, :fair_prob, :venue_price, :edge, :kelly_fraction)"),
                    {"event_id": s["event_id"], "bookmaker": s["bookmaker"],
                     "market_type": s["market_type"], "outcome_name": s["outcome_name"],
                     "signal_type": s["signal_type"], "fair_prob": s["fair_prob"],
                     "venue_price": s["venue_price"], "edge": s["edge"],
                     "kelly_fraction": s.get("kelly_fraction")})
        return len(signals)
    finally:
        engine.dispose()


def build_market_history() -> int:
    engine = _engine()
    try:
        with engine.begin() as conn:
            markets = conn.execute(text(
                "SELECT market_id::text AS market_id, resolved_outcome, "
                "       close_time::text AS close_time, resolution_time::text AS resolution_time "
                "FROM markets "
                "WHERE resolved_outcome IS NOT NULL AND close_time IS NOT NULL")).mappings().all()
            resolved, prices_by_market = [], {}
            for m in markets:
                # tracked outcome = the resolved outcome's price series (fallback: first outcome)
                tracked = m["resolved_outcome"]
                resolved.append({**dict(m), "tracked_outcome": tracked})
                pts = conn.execute(text(
                    "SELECT ph.captured_at::text AS ts, ph.price "
                    "FROM price_history ph JOIN market_outcomes o ON o.outcome_id=ph.outcome_id "
                    "WHERE o.market_id = :mid AND o.outcome_name = :name AND ph.price IS NOT NULL"),
                    {"mid": m["market_id"], "name": tracked}).all()
                prices_by_market[m["market_id"]] = [(ts, price) for ts, price in pts]
            rows = compute_market_history(resolved, prices_by_market)
            for r in rows:
                conn.execute(text(
                    "INSERT INTO mart_market_history "
                    "  (market_id, resolved_outcome, resolution_time, predicted_prob, realized, clv) "
                    "VALUES (:market_id, :resolved_outcome, :resolution_time, :predicted_prob, "
                    "        :realized, :clv) "
                    "ON CONFLICT (market_id) DO UPDATE SET "
                    "  resolved_outcome=EXCLUDED.resolved_outcome, predicted_prob=EXCLUDED.predicted_prob, "
                    "  realized=EXCLUDED.realized, clv=EXCLUDED.clv"), r)
        return len(rows)
    finally:
        engine.dispose()
```

- [ ] **Step 2: Add the upsert key for `mart_market_history` (migration 010)**

`mart_market_history` has no PK. The `ON CONFLICT (market_id)` needs one. Create `db/migrations/010_market_history_pk.sql`:

```sql
-- Support upsert-by-market in the market-history mart.
CREATE UNIQUE INDEX IF NOT EXISTS uq_mart_hist_market ON mart_market_history (market_id);
```

Apply:
```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
if MSYS_NO_PATHCONV=1 docker run --rm -e PGURL="$PGURL" -v /c/Users/tkmer/bountygate/db/migrations:/mig postgres:16 psql "$PGURL" -v ON_ERROR_STOP=1 -f /mig/010_market_history_pk.sql 2>&1; then
  docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -c "INSERT INTO schema_migrations(version) VALUES ('010_market_history_pk') ON CONFLICT DO NOTHING;"
else echo FAILED; fi
```
Expected: `CREATE INDEX`.

- [ ] **Step 3: Write the build_marts DAG**

Create `airflow/dags/build_marts.py`:

```python
"""normalized -> marts. Asset-triggered by the normalized tables."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.transforms.marts import build_edge_signals, build_market_history

ODDS_ASSET = Asset(name="sportsbook_odds_history")
PRICE_ASSET = Asset(name="price_history")


@dag(
    dag_id="build_marts",
    schedule=[ODDS_ASSET, PRICE_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["transform", "marts"],
)
def build_marts():
    @task(outlets=[Asset(name="mart_edge_signals")])
    def edges() -> int:
        n = build_edge_signals()
        print(f"[build_marts] edge_signals rows={n}")
        return n

    @task(outlets=[Asset(name="mart_market_history")])
    def history() -> int:
        n = build_market_history()
        print(f"[build_marts] market_history rows={n}")
        return n

    edges()
    history()


dag = build_marts()
```

- [ ] **Step 4: Import check + run against the live DB**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler airflow dags list-import-errors 2>&1 | grep -i build_marts || echo "no import errors"
docker compose run --rm airflow-scheduler airflow dags test build_marts 2>&1 | grep -iE "\[build_marts\]|state=success|state=failed|Traceback|Error" | tail -10
```
Expected: no import errors; `[build_marts] edge_signals rows=N`, `[build_marts] market_history rows=M` lines; `state=success`.

> **`market_history` will report 0 rows in v1 — by design.** The parsers set `resolved_outcome=None` (the sampled payloads are all active markets, and Kalshi/Polymarket resolution extraction is its own follow-up). The mart's math and DB writer are complete and unit-tested (Task 9); they simply have no resolved markets to act on until a later task teaches the parsers to extract resolution. `edge_signals` is the mart that returns real data in this spec. This is the expected end state, not a defect.

- [ ] **Step 5: Verify the mart + live API**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select signal_type, count(*) from mart_edge_signals group by 1 order by 1;"
echo "live /edges (first 300 chars):"; curl -s https://bountygate-880dea148b95.herokuapp.com/edges | head -c 300; echo ""
```
Expected: `mart_edge_signals` has rows (ev and/or arb); `/edges` now returns a non-empty JSON array.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/marts/__init__.py airflow/dags/build_marts.py db/migrations/010_market_history_pk.sql
git commit -m "feat(transforms): build_marts DAG (edge signals + market history)"
```

---

## Task 12: Final verification

**Files:** none.

- [ ] **Step 1: All migrations recorded**

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c "select version from schema_migrations order by 1;"
```
Expected: `001`…`010` present.

- [ ] **Step 2: Pure tests (host) + mart tests (container) green**

```bash
cd /c/Users/tkmer/bountygate
python -m pytest app/shared/python/bountygate/transforms/tests/test_parsers.py app/shared/python/bountygate/connectors/tests/test_registry_regions.py -q 2>&1 | tail -3
cd airflow
docker compose run --rm -e PYTHONDONTWRITEBYTECODE=1 airflow-scheduler python -m pytest /opt/bountygate-shared/python/bountygate/transforms/tests/test_edge_signals.py /opt/bountygate-shared/python/bountygate/transforms/tests/test_market_history.py -q 2>&1 | tail -3
```
Expected: all passing (5 parser + 1 registry on host; 6 mart in container).

- [ ] **Step 3: DAGs present, no import errors, legacy gone**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler airflow dags list-import-errors 2>&1 | tail -5
docker compose run --rm airflow-scheduler airflow dags list 2>&1 | grep -E "normalize|build_marts|ingest_|partman" 
docker compose run --rm airflow-scheduler airflow dags list 2>&1 | grep -c "bg_" || true
```
Expected: no import errors; `normalize` + `build_marts` + the 3 `ingest_*` + `partman_maintenance` listed; `bg_` count = 0.

- [ ] **Step 4: Live API serves real data**

```bash
B=https://bountygate-880dea148b95.herokuapp.com
echo -n "/edges count: "; curl -s "$B/edges" | python -c "import sys,json; print(len(json.load(sys.stdin)))"
echo -n "/markets count: "; curl -s "$B/markets" | python -c "import sys,json; print(len(json.load(sys.stdin)))"
```
Expected: `/markets` > 0; `/edges` ≥ 0 (rows when actionable signals exist).

- [ ] **Step 5: Push + report**

```bash
cd /c/Users/tkmer/bountygate
git push origin HEAD 2>&1 | tail -2
```

Summarize against the spec §9 success criteria: Pinnacle lands (us,eu); normalize populates dimensions (upsert) + time-series (idempotent append) incrementally; build_marts fills `mart_edge_signals` (Pinnacle-anchored EV + arb, consensus fallback) full-refresh and upserts `mart_market_history` (empty until markets resolve — expected); both DAGs Asset-triggered and `state=success`; legacy `bg_*` no longer load; all tests pass; `/edges` serves real data.

> **Deploy note:** the transform DAGs run in your local Airflow (the Heroku web dyno only serves the read API). The marts they write live in the same Heroku Postgres the API reads, so populating them locally makes the live API serve real data with no redeploy.
