# Postgres Backend — Extensions, Partitioning, Schema Contract & Read API

**Date:** 2026-06-06
**Status:** Design — approved, pending implementation plan
**Spec #2 of the downstream queue** (`docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`):
connectors ✅ → **Postgres backend (this)** → frontend.

---

## 1. Context & goal

Spec #1 landed read-only marketplace data into the append-only `raw_market_snapshots` table
(Kalshi/Polymarket/The Odds API, every 5 min). This spec builds the **database foundation and the
read API**: enable the needed extensions, partition the time-series with retention, define the
**normalized + marts schema as the stable contract** (empty tables), and ship a **thin FastAPI read
API** the frontend will consume.

The actual transforms that populate normalized + marts are **done in Airflow with pandas/polars** and
are **out of scope here** — they are their own later spec(s). This spec defines the schema those
transforms target so the API and partitioning have concrete tables to build on.

### Locked decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Spec scope | Extensions + partitioning + normalized/marts **DDL** + thin FastAPI read API. |
| Transform location | **Airflow + pandas/polars** (user's comfort zone) — **deferred to a later spec**, not built here. |
| Modeling language | DDL/migrations here; transform logic later. New transforms prefer **polars** (perf) with pandas acceptable. |
| Time-series engine | **Native declarative partitioning** (TimescaleDB is unavailable on this Postgres). |
| Partition automation | **`pg_partman`**, maintenance triggered by **`pg_cron`** (in-DB, set-and-forget). |
| API surface | **Thin FastAPI read API** on the Heroku web dyno (brought back from 0). |

### Environment facts
- PostgreSQL **16.13** on AWS RDS (the bountygate `DATABASE_URL`).
- Currently only `raw_market_snapshots` + `schema_migrations`; only `plpgsql` extension installed
  (the decommission `DROP SCHEMA` removed the old `uuid-ossp`/`pg_stat_statements`).
- Available extensions confirmed via `pg_available_extensions`: `pg_partman`, `pg_cron`, `pg_trgm`,
  `pgcrypto`, `pg_stat_statements`, `postgis`, `citext`, `hstore`, `uuid-ossp`. **`timescaledb` is NOT
  available.**
- The three `ingest_*` DAGs are **paused** (no concurrent writers) — safe to recreate
  `raw_market_snapshots` as partitioned.

---

## 2. Deliverables

1. Extensions enabled (idempotent migration).
2. `raw_market_snapshots` converted to a partitioned table (data preserved) + `price_history` and
   `sportsbook_odds_history` created partitioned; `pg_partman` config + `pg_cron` maintenance job.
3. Normalized schema DDL (empty contract tables).
4. Marts schema DDL (empty contract tables).
5. Thin FastAPI read API serving the marts, on the Heroku web dyno.

**Explicitly out of scope:** the Airflow pandas/polars transforms that populate normalized + marts;
cross-venue entity-matching logic; the frontend (spec #3); any write/execution path.

---

## 3. Extensions

One idempotent migration (`CREATE EXTENSION IF NOT EXISTS`):
- **`pg_partman`** — partition automation for the time-series tables.
- **`pg_cron`** — schedules `partman.run_maintenance()` (new partitions + retention drops).
- **`pg_trgm`** — fuzzy text matching for future cross-venue event/team linking (in-DB replacement
  for the old rapidfuzz approach).
- **`pg_stat_statements`** — query observability.

UUID primary keys use native **`gen_random_uuid()`** (built into PG16 — no `pgcrypto`/`uuid-ossp`
needed).

> Note: `pg_cron` and `pg_partman` may require placement in a specific schema and the cron database
> setting; the implementation plan resolves the exact `CREATE EXTENSION` schema/grants for this RDS
> instance (e.g. `partman` schema, `cron` in the right database).

---

## 4. Partitioning & retention

Two append-only firehose tables, both **`PARTITIONED BY RANGE(captured_at)`**, daily partitions via
`pg_partman`, maintenance via `pg_cron`, BRIN index on `captured_at`:

| Table | Retention | Notes |
|---|---|---|
| `raw_market_snapshots` | **90 days** | reproducible raw firehose; recreate partitioned, copy existing rows |
| `price_history` | **2 years** | analytical record (prediction-market outcome prices) |
| `sportsbook_odds_history` | **2 years** | analytical record (sportsbook lines) |

`raw_market_snapshots` cutover (DAGs are paused, so no concurrent writes): create the partitioned
table, copy existing rows, swap into place, register with `pg_partman`. Retentions are defaults and
can be tuned later by changing the `pg_partman` config rows.

---

## 5. Normalized schema (the contract)

UUID PKs via `gen_random_uuid()`. Two parallel sides + a link table. Empty here; Airflow transforms
populate later.

**Reference:**
```
venues(venue_key text PK, kind text)        -- 'kalshi'|'polymarket'|'the_odds_api'; kind 'prediction'|'sportsbook'
```

**Prediction markets (Kalshi, Polymarket):**
```
markets(
  market_id uuid PK DEFAULT gen_random_uuid(),
  venue_key text NOT NULL REFERENCES venues,
  external_id text NOT NULL,                 -- raw source_key
  title text, category text, status text,
  open_time timestamptz, close_time timestamptz,
  resolved_outcome text, resolution_time timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(venue_key, external_id))

market_outcomes(
  outcome_id uuid PK DEFAULT gen_random_uuid(),
  market_id uuid NOT NULL REFERENCES markets ON DELETE CASCADE,
  outcome_name text NOT NULL, outcome_index int,
  last_price numeric, last_seen timestamptz,
  UNIQUE(market_id, outcome_name))

price_history(                               -- PARTITIONED BY RANGE(captured_at)
  market_id uuid NOT NULL, outcome_id uuid NOT NULL,
  captured_at timestamptz NOT NULL,
  price numeric, bid numeric, ask numeric, volume numeric, liquidity numeric)
```

**Sportsbook (The Odds API)** — bookmaker dimension prediction markets lack:
```
sports_events(
  event_id uuid PK DEFAULT gen_random_uuid(),
  source_event_id text UNIQUE NOT NULL,
  sport_key text, commence_time timestamptz, home_team text, away_team text)

sportsbook_odds_history(                     -- PARTITIONED BY RANGE(captured_at)
  event_id uuid NOT NULL, market_type text NOT NULL, bookmaker text NOT NULL,
  outcome_name text NOT NULL, captured_at timestamptz NOT NULL,
  decimal_price numeric)
```

**Cross-venue linking** (populated later by the matching transform):
```
market_event_links(
  market_id uuid NOT NULL REFERENCES markets ON DELETE CASCADE,
  event_id uuid NOT NULL REFERENCES sports_events ON DELETE CASCADE,
  confidence numeric, method text,
  UNIQUE(market_id, event_id))
```

Partitioned tables (`price_history`, `sportsbook_odds_history`) carry no FK constraints to keep
partition maintenance cheap; referential integrity is the transforms' responsibility.

---

## 6. Marts (lighter contract — Airflow populates)

Essential columns now; the transform spec may extend via migration. These map to the kept
`devig`/`ev`/`kelly`/`clv`/`consensus` analytics lib outputs — **read-only, no execution**.

```
mart_cross_market_prices(
  question_key text, captured_at timestamptz,
  kalshi_prob numeric, polymarket_prob numeric, sportsbook_consensus_prob numeric,
  max_spread numeric)                                  -- cross-venue price comparison

mart_edge_signals(
  signal_id uuid PK DEFAULT gen_random_uuid(), detected_at timestamptz,
  venue_key text, market_id uuid, outcome_id uuid,
  signal_type text,                                    -- 'arb' | 'ev'
  fair_prob numeric, venue_price numeric, edge numeric, kelly_fraction numeric)

mart_market_history(
  market_id uuid, resolved_outcome text, resolution_time timestamptz,
  predicted_prob numeric, realized boolean, clv numeric)   -- backtest / calibration
```

---

## 7. Thin FastAPI read API

A rebuilt `app/web/` FastAPI app, on the existing bountygate Heroku web dyno (scaled back from 0).

- **Read-only `GET` JSON endpoints** over the marts:
  - `GET /health`
  - `GET /markets` (filters: `venue`, `status`, `limit`/`offset`)
  - `GET /markets/{market_id}/history` (price_history for a market)
  - `GET /edges` (mart_edge_signals; filters: `signal_type`, `since`)
  - `GET /cross-market` (mart_cross_market_prices)
  - `GET /history` (mart_market_history)
- Direct SQL / SQLAlchemy Core against the marts (no ORM models); cursor/offset pagination; simple
  query-param filters only. **No business logic** — it serves what the marts contain.
- Reuses `bountygate.utils.db_connection` `DATABASE_URL`. New `Procfile` (`web: uvicorn app.web.main:app`).
- CORS open (frontend is a separate origin, spec #3).
- Tested with FastAPI `TestClient` against a seeded throwaway schema (SQLite or a transactional test DB)
  asserting status codes, shape, and filter behavior.

---

## 8. Component isolation

| Unit | Responsibility |
|---|---|
| (a) extensions migration | enable pg_partman/pg_cron/pg_trgm/pg_stat_statements |
| (b) partitioning migration | recreate raw partitioned + create history tables + pg_partman/pg_cron config |
| (c) normalized DDL migration | venues/markets/outcomes/price_history/events/odds/links |
| (d) marts DDL migration | the three mart tables |
| (e) FastAPI app | one router per resource (markets, edges, cross-market, history) + db access module |

Migrations are ordered SQL files (`db/migrations/00N_*.sql`) applied by the existing
`scripts/migrate.py`. Each unit is independently reviewable.

---

## 9. Success criteria

- The extensions migration enables `pg_partman`, `pg_cron`, `pg_trgm`, `pg_stat_statements` (verified
  via `\dx`).
- `raw_market_snapshots` is partitioned by `captured_at` with its prior rows intact; `price_history`
  and `sportsbook_odds_history` exist partitioned; a `pg_cron` job runs `partman` maintenance.
- All normalized + marts tables exist (empty) matching §5/§6, applied via `scripts/migrate.py`.
- The FastAPI app boots and every endpoint returns valid JSON (empty arrays over the empty marts);
  `TestClient` tests pass; the Heroku web dyno serves it (scaled to 1).
- No write/execution endpoints; all SQL is read-only.

---

## 10. Risks / notes

- **pg_cron/pg_partman provisioning on RDS** can need a specific install schema and the `cron.database_name`
  setting; the plan must verify the exact `CREATE EXTENSION` placement and that maintenance actually fires.
- **Marts are a starting contract**, intentionally light; the transform spec will likely add columns —
  acceptable, additive migrations.
- **Empty-API milestone**: endpoints return empty arrays until the transform spec populates the marts.
  That is the expected end state of this spec, not a defect.
- The web dyno coming back to 1 resumes a (small) Heroku cost.
