# Ingestion Connectors — Design

**Date:** 2026-06-05
**Status:** Design — approved, pending implementation plan
**Spec #1 of the downstream queue** in `docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`
(connectors → Postgres backend → frontend).

---

## 1. Context & goal

`bountygate` is now a read-only prediction-market analytics aggregator (see the decommission spec).
This spec builds the **ingestion layer**: read-only GET connectors to prediction marketplaces, scheduled
Airflow pollers, and a minimal raw Postgres landing table. End state: Kalshi, Polymarket, and The Odds
API data lands automatically on a 5-minute cadence; the Airflow stack runs 3.2 with a healthy triggerer;
every connector is fixture-tested.

### Locked decisions (from brainstorming)

| Decision | Choice |
|---|---|
| v1 sources | **Kalshi (read-only), Polymarket, The Odds API.** Other venues (PredictIt/Manifold) deferred. |
| Persistence | **Minimal raw landing** — one append-only snapshot table. Normalized/marts = next spec. |
| Scheduling | **Include polling DAGs** (one per source) in the new clean TaskFlow/asset style. |
| Airflow infra | **Reuse the existing `airflow/` stack**, upgraded **3.0.6 → 3.2.x**, with the **triggerer service enabled**. |
| Connectors location | `app/shared/python/bountygate/connectors/` (the pip-installable shared lib). |
| Architecture | **Uniform `Connector` interface + small registry**, fixture-tested. |

---

## 2. Component architecture

New package `app/shared/python/bountygate/connectors/` — both DAGs and the future web backend import it via
`from bountygate.connectors import ...`.

| File | Responsibility | Depends on |
|---|---|---|
| `base.py` | `Connector` ABC (`source: str`, `fetch_snapshots() -> list[RawRecord]`) + `RawRecord` dataclass | — |
| `kalshi.py` | `KalshiConnector` — read-only subset migrated from the Kalshi client | `kalshi_python_sync`, RSA key |
| `polymarket.py` | `PolymarketConnector` — Gamma + CLOB read APIs | `requests` |
| `odds_api.py` | `OddsApiConnector` — The Odds API v4 | `requests` |
| `landing.py` | `land_raw(records, engine=None)` — single writer into `raw_market_snapshots` | `bountygate.utils.db_connection`, SQLAlchemy |
| `registry.py` | `CONNECTORS: dict[str, Connector]` — for DAGs / CLI / tests to iterate | the three connectors |

**`RawRecord`** (the uniform shape every connector emits, so `land_raw()` is source-agnostic):

```python
@dataclass
class RawRecord:
    source: str          # 'kalshi' | 'polymarket' | 'the_odds_api'
    source_key: str      # natural id: ticker / condition_id / f"{event_id}:{market}"
    record_type: str     # 'market' | 'orderbook' | 'odds_line'
    captured_at: datetime # fetch time, UTC, tz-aware
    payload: dict        # normalized raw JSON for this record
```

Each unit is independently testable: a connector is "fetch + normalize," `land_raw()` is "write," the
registry is "lookup." No connector knows about Postgres; `land_raw()` knows nothing about any source.

---

## 3. The three connectors

### 3.1 KalshiConnector (read-only)
Migrate the **read** methods out of `kalshi/dags/utils/kalshi_client.py` into `KalshiConnector`:
- `get_markets(series_ticker)` — open events + nested markets with yes/no bid/ask, open interest, liquidity.
- `get_market_orderbook(ticker)` — top-of-book depth.
- `get_settlements()` — resolution history (for later backtest/calibration).

**Drop** all execution methods (`place_order`, `cancel_order`, `get_balance`, `get_portfolio_*`, Kelly,
resting orders). Keep the **raw-HTTP-body bypass** (`*_without_preload_content` + `json.loads`) — the
Kalshi SDK's pydantic models are stale vs. the live API. Auth: the working RSA key + `KALSHI_ENV=production`
(`KALSHI_API_KEY_ID` / `KALSHI_PRIVATE_KEY_PATH`). `fetch_snapshots()` iterates `SERIES_BY_SPORT` and emits
one `RawRecord(record_type='market')` per market (plus `record_type='orderbook'` where fetched).

### 3.2 PolymarketConnector (new, no auth for reads)
- **Gamma API** (`https://gamma-api.polymarket.com`): `/markets`, `/events` — market/event metadata, outcomes,
  volume, liquidity, end dates.
- **CLOB read** (`https://clob.polymarket.com`): `/markets`, price/midpoint/book read endpoints — current
  outcome prices.
Both are public (no API key). `fetch_snapshots()` pulls active markets and emits `RawRecord(record_type='market')`
keyed by Polymarket `condition_id` (with token/outcome prices in `payload`). Pagination handled with the
APIs' cursor/offset.

### 3.3 OddsApiConnector
Wrap The Odds API v4 (reuse `SPORT_KEYS` and the credit-aware **events-then-odds** pattern from
`bg_arb_pipeline`: list events in a commence window first, then request odds per event to avoid wasted
credits). Emits `RawRecord(record_type='odds_line')` keyed by `f"{event_id}:{market}:{book}"`. Key from
`ODDS_API_KEY`.

---

## 4. Raw landing schema

One append-only table (matches the uniform `RawRecord`), added via the existing migration runner
(`scripts/migrate.py`) as `db/migrations/011_raw_market_snapshots.sql`:

```sql
CREATE TABLE IF NOT EXISTS raw_market_snapshots (
  id           bigserial   PRIMARY KEY,
  source       text        NOT NULL,   -- 'kalshi' | 'polymarket' | 'the_odds_api'
  source_key   text        NOT NULL,   -- ticker / condition_id / event+market+book
  record_type  text        NOT NULL,   -- 'market' | 'orderbook' | 'odds_line'
  captured_at  timestamptz NOT NULL,   -- fetch time (UTC)
  payload      jsonb       NOT NULL,
  ingested_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_snap_source_time ON raw_market_snapshots (source, captured_at);
CREATE INDEX IF NOT EXISTS ix_raw_snap_source_key  ON raw_market_snapshots (source, source_key, captured_at);
```

Append-only; the normalization spec reads latest-per-`source_key`. `land_raw()` bulk-inserts a batch of
`RawRecord`s in one transaction.

---

## 5. Ingestion DAGs + Airflow 3.2 upgrade

### 5.1 Pollers — one DAG per source
`airflow/dags/ingest_kalshi.py`, `ingest_polymarket.py`, `ingest_odds.py`, each in the new clean style:
- TaskFlow `@task` functions; idempotent (append-only landing is naturally re-runnable).
- Body: `records = CONNECTORS[source].fetch_snapshots(); land_raw(records)`.
- Emits a `raw_market_snapshots` **Airflow asset** (Dataset) on success so the future normalization DAG
  schedules off it.
- **Schedule: every 5 minutes for all three** (the user has a high Odds API monthly quota).
- `max_active_runs=1`, `catchup=False`, retries with backoff. Per-source DAGs give independent failure
  isolation and retry policy.

### 5.2 Airflow infra upgrade
- `airflow/requirements.txt`: `apache-airflow==3.0.6` → `3.2.x` (align provider pins).
- `airflow/docker-compose.yaml`: align service layout with Kalshi's 3.2 compose
  (`db migrate`, `api-server`, `scheduler`, `dag-processor`) and **add the `airflow-triggerer` service**
  (`command: triggerer`) — removes the red "triggerer not running" banner and enables deferrable operators
  for future long-polls.
- New connectors must be importable inside the containers (the shared lib is already mounted/installed;
  verify the install path covers `bountygate.connectors`).

---

## 6. Testing

- **Per-connector unit tests** (`tests/connectors/test_<source>.py`): feed recorded JSON fixtures
  (`tests/fixtures/connectors/`, seeded from Kalshi's existing `markets_response.json`/`event_response.json`
  and captured Polymarket/Odds samples) into the connector's normalizer → assert the emitted `RawRecord`s
  (source, source_key, record_type, payload shape). No network — the HTTP layer is injected/monkeypatched.
- **`land_raw()` test**: insert `RawRecord`s into a throwaway table, assert row count + column mapping.
- **DAG-import smoke test**: all `ingest_*` DAGs import without error (catches TaskFlow/asset wiring bugs).

---

## 7. Out of scope (explicit)

- Normalized → marts modeling and any `dim_*`/`fact_*` work — the **Postgres-backend** spec.
- Stripping `bot_execution_queue` / `trigger_bot_execution` from the paused `bg_*` DAGs — deferred to their
  rewrite. (The table was already dropped in the decommission; the DAGs stay paused.)
- Other venues (PredictIt, Manifold), the web/API layer, and the frontend.
- Kalshi WebSocket/streaming — v1 is REST polling only.

---

## 8. Success criteria

- `bountygate.connectors` package exists with `base`, `kalshi`, `polymarket`, `odds_api`, `landing`,
  `registry`; `CONNECTORS` has all three.
- `raw_market_snapshots` migration applies cleanly via `scripts/migrate.py up`.
- Each connector returns valid `RawRecord`s from its fixtures (tests green).
- The three `ingest_*` DAGs import, are scheduled every 5 min, and on a manual run land rows into
  `raw_market_snapshots` for their source.
- `docker compose up` brings up Airflow 3.2 with a running triggerer (no red banner).
