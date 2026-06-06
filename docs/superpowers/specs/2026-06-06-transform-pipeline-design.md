# Transform Pipeline — Normalized Layer + Sportsbook Marts

**Date:** 2026-06-06
**Status:** Design — approved, pending implementation plan
**Spec #3 of the downstream queue** (`docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`):
connectors ✅ → Postgres backend ✅ → **transform pipeline (this)** → frontend.

---

## 1. Context & goal

Spec #1 lands read-only marketplace data into the partitioned `raw_market_snapshots` firehose
(Kalshi/Polymarket/The Odds API, every 5 min). Spec #2 built the Postgres backend: extensions,
partitioning, the **empty** normalized + marts contract tables, and a thin read-only FastAPI API.
Every API endpoint currently returns `[]` because nothing populates those tables.

This spec builds the **Airflow transform pipeline** that fills them:

1. **Normalize** raw JSON payloads into the normalized tables (`markets`, `market_outcomes`,
   `sports_events`, `price_history`, `sportsbook_odds_history`).
2. **Build the two marts that need no cross-venue matching**: `mart_edge_signals` (sportsbook EV +
   arb) and `mart_market_history` (prediction-market calibration / CLV).

After this spec the live API serves **real data end-to-end** (ingest → normalized → marts → API).

### Locked decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Scope | Normalized layer + `mart_edge_signals` + `mart_market_history`. **Cross-venue matching and `mart_cross_market_prices` are deferred to their own spec.** |
| Engine | **polars** for raw-JSON read / reshape / write; convert to **pandas** only at the analytics-lib boundary. |
| DAG wiring | **Layered, Asset-triggered chain**: `normalize` consumes `Asset("raw_market_snapshots")`; `build_marts` consumes the normalized Assets `normalize` emits. |
| Legacy `bg_*` DAGs | **Archive** the ~18 `bg_*` DAGs + their helper dirs out of `airflow/dags/` (to `airflow/dags/_archive_pre_pivot/`) so the scheduler ignores them. Build fresh. Keep the venue-agnostic analytics lib. |
| Fair-prob anchor | **Pinnacle no-vig** (devig Pinnacle's two-way line), with **`no_vig_consensus` fallback** when Pinnacle is absent for an event. |
| Odds ingestion | Flip the odds connector to **`regions="us,eu"`** so Pinnacle (eu region) lands. Small ingestion tweak, hard dependency of the edge mart → folded into this spec. |

### Environment facts
- New transform code is **pure / Airflow-free** and lives in `app/shared/python/bountygate/transforms/`
  beside `connectors/` and `analytics/`. DAGs are thin orchestration.
- The analytics lib (`bountygate/analytics/`) is pandas-based and stays **untouched**. Its **low-level
  primitives are venue-agnostic** and are what we reuse: `devig.multiplicative_devig` / `devig_all` /
  `shin_devig`, `consensus.no_vig_consensus(over_odds[], under_odds[])`, `ev.edge` / `edge_pct` /
  `is_actionable`, `kelly.kelly_fraction` / `quarter_kelly`, `clv.clv_from_fair`. The high-level
  orchestrators (`build_ev_opportunities`, `size_opportunities`, `compute_clv`) are shaped for the old
  player-props schema (`player_name`/`line`/over-under) and are **not** reused.
- The odds connector (`connectors/odds_api.py`) defaults to `regions="us"` today (no Pinnacle).
- Migrations apply via Docker `psql` (host lacks psycopg2); web tests run on host; the Airflow image
  has polars added (new dependency).

### Raw payload shapes (sampled from live data)
- **Kalshi** (`record_type='market'`): `ticker`, `event_ticker`, `series_ticker`, `title`, `status`,
  `yes_bid`/`yes_ask`/`no_bid`/`no_ask`, `yes_sub_title`/`no_sub_title`, `open_interest`,
  `liquidity_dollars`. Binary Yes/No.
- **Polymarket** (`record_type='market'`): `condition_id`, `question`, `slug`, `active`/`closed`,
  `volume`, `liquidity`, `end_date`, parallel `outcomes[]` / `outcome_prices[]`.
- **The Odds API** (`record_type='odds_line'`): `event_id`, `sport_key`, `home_team`/`away_team`,
  `commence_time`, `market` (e.g. `h2h`), `bookmaker`, `outcomes[]` of `{name, price}` (decimal).
  `source_key = "{event_id}:{market}:{bookmaker}"`.

---

## 2. Deliverables

1. Odds ingestion switched to `regions="us,eu"` (Pinnacle lands).
2. `transforms/` package: per-source parsers, `normalize`, and the two mart builders (pure functions).
3. `normalize` Airflow DAG (Asset-triggered by raw firehose; emits normalized Assets).
4. `build_marts` Airflow DAG (Asset-triggered by normalized Assets; emits mart Assets).
5. Additive migrations: `007_extend_edge_signals.sql` (sportsbook columns on `mart_edge_signals`) and
   `008_transform_state.sql` (the normalize watermark table).
6. The ~18 legacy `bg_*` DAGs archived out of the scheduler's view.

**Explicitly out of scope:** cross-venue entity matching and `mart_cross_market_prices`; the frontend;
backfilling history beyond what is already landed; any write/execution path.

---

## 3. Architecture & DAG chain

```
ingest_kalshi ─┐
ingest_polymarket ─┼─ emit ▶ Asset("raw_market_snapshots")
ingest_odds ─┘                       │  (data-aware trigger)
                                     ▼
                            ┌──────────────────┐
                            │  normalize  DAG  │   raw JSON → normalized tables
                            └──────────────────┘
   emits ▶ Asset("markets") Asset("market_outcomes") Asset("price_history")
           Asset("sports_events") Asset("sportsbook_odds_history")
                                     │  (data-aware trigger)
                                     ▼
                            ┌──────────────────┐
                            │ build_marts DAG  │   normalized → marts (polars + lib primitives)
                            └──────────────────┘
           emits ▶ Asset("mart_edge_signals") Asset("mart_market_history")
```

- `normalize` is triggered by `Asset("raw_market_snapshots")` (fires ~every 5 min as any ingest lands).
- `build_marts` is triggered by the normalized Assets `normalize` emits.
- Assets are referenced by **name** (`Asset(name=...)`), matching the existing ingest DAGs (a
  `postgres://` URI trips Airflow's asset-URI normalizer).
- Code package `app/shared/python/bountygate/transforms/`:
  - `parsers/kalshi.py`, `parsers/polymarket.py`, `parsers/odds.py` — `payload dict → typed rows`
    (the only source-specific code).
  - `normalize.py` — orchestrates parse → upsert dimensions → append time-series (given a connection).
  - `marts/edge_signals.py`, `marts/market_history.py` — consume normalized frames, call analytics
    primitives, return mart frames.
  - DAGs wire DB I/O around these pure functions.

---

## 4. Normalization layer (raw → normalized)

`normalize` is **incremental**: it processes only raw rows newer than a stored watermark
(`max(captured_at)` last processed, persisted in a small `transform_state(name text PK, watermark
timestamptz)` table created by additive migration `008_transform_state.sql`). On each run it reads the new window, parses by
source, and writes.

| Source | → tables | Key logic |
|---|---|---|
| Kalshi | `markets` (upsert), `market_outcomes` (Yes/No), `price_history` (append) | `external_id=ticker`; `category` from `series_ticker`; outcome `last_price` from the bid/ask mid (`(yes_bid+yes_ask)/2`, `(no_bid+no_ask)/2`); `price_history.price` = same mid, `bid`/`ask` from the raw fields. |
| Polymarket | `markets` (upsert), `market_outcomes` (zip outcomes), `price_history` (append) | `external_id=condition_id`; `title=question`; `close_time=end_date`; `status` from `active`/`closed`; outcome prices from parallel `outcome_prices[]`. |
| The Odds API | `sports_events` (upsert), `sportsbook_odds_history` (append) | `source_event_id=event_id`; `sport_key`/`home_team`/`away_team`/`commence_time`; one history row per `outcomes[]` entry × bookmaker; `decimal_price=price`; `market_type=market`. |

**Write strategy:**
- **Upsert** dimension tables (`markets`, `market_outcomes`, `sports_events`) with
  `INSERT … ON CONFLICT (<natural key>) DO UPDATE` — one current row per entity; refreshes
  `updated_at` / `last_price` / `last_seen`.
- **Append** the partitioned time-series (`price_history`, `sportsbook_odds_history`) — every snapshot
  is a new row keyed by `captured_at`; pg_partman routes/retains. **Idempotent append**: dedupe on
  `(outcome_id, captured_at)` and `(event_id, bookmaker, outcome_name, market_type, captured_at)` so a
  re-run of the same window cannot double-insert (enforced by a `UNIQUE` index or `ON CONFLICT DO
  NOTHING`).
- `price_history` needs `outcome_id` (a uuid): within a run, normalize upserts `markets` +
  `market_outcomes` first, reads back the `(venue_key, external_id, outcome_name) → outcome_id` map,
  then appends history.

---

## 5. Marts layer (normalized → marts)

### 5a. `mart_edge_signals` — sportsbook EV + arb

The Spec-2 contract table lacks the columns sportsbook signals need. **Additive migration
`007_extend_edge_signals.sql`** adds nullable columns `event_id uuid`, `bookmaker text`,
`market_type text`, `outcome_name text` (the existing `market_id`/`outcome_id` stay nullable, reserved
for future prediction-market signals).

Computation, per `sports_event × market_type`, using the **latest `captured_at` per bookmaker** from
`sportsbook_odds_history`:
1. Assemble each bookmaker's two-way decimal prices (home as "over", away as "under").
2. **Fair prob = Pinnacle no-vig**: devig Pinnacle's two-way line via `devig.multiplicative_devig`
   (`shin_devig` available as an alternative). **Fallback** to `consensus.no_vig_consensus` across the
   non-Pinnacle books when Pinnacle is missing for that event.
3. **EV signals**: for each **non-Pinnacle** ("soft") book/outcome, `edge = ev.edge(fair_prob,
   book_decimal_price)`; emit rows where `ev.is_actionable(edge, threshold=0.025)`; size with
   `kelly.quarter_kelly(fair_prob, book_decimal_price)`. `signal_type='ev'`.
4. **Arb signals**: if `1/best_home_decimal + 1/best_away_decimal < 1` across books, emit a
   `signal_type='arb'` row with `edge` = the guaranteed margin (`1 - that sum`).
5. **Refresh = full replace** each run: the mart holds *currently actionable* signals (truncate +
   insert, or delete-by-current-batch then insert). Small and always fresh; the underlying
   time-series in `sportsbook_odds_history` can reconstruct history later if ever needed.

Emitted columns: `detected_at`, `event_id`, `bookmaker`, `market_type`, `outcome_name`, `signal_type`,
`fair_prob`, `venue_price` (the book's decimal price), `edge`, `kelly_fraction`. `signal_id` defaults
via `gen_random_uuid()`.

### 5b. `mart_market_history` — calibration / CLV

Populated as **prediction markets resolve** (Kalshi/Polymarket rows whose `status` indicates resolved
and that have a `resolved_outcome`):
- `predicted_prob` = the market's fair prob at a fixed pre-close horizon (the last `price_history`
  point at least 1h before `close_time`).
- `realized` = whether `resolved_outcome` matched the tracked outcome (boolean).
- `clv` = `clv.clv_from_fair(predicted_prob, closing_prob)` where `closing_prob` is the last
  `price_history` point before `close_time`.
- **Upsert on `market_id`** — one row per resolved market; accumulates over time.
- **v1 note:** with no markets resolved in the currently landed window, this mart legitimately stays
  empty until markets close. That is expected, not a defect.

---

## 6. Engine & code structure

- **polars** reads the raw JSON window, reshapes, and writes; convert a slice to **pandas** only if a
  lib function requires it (the reused primitives are scalar/list, so conversion is rare).
- DB access via SQLAlchemy Core + psycopg2 (`execute_values` / `ON CONFLICT`), already a dependency.
- **polars** is added to the Airflow image (`airflow/requirements.txt`); it is **not** added to the
  Heroku web slug (the read API does not transform).
- Two thin DAGs in `airflow/dags/`: `normalize.py`, `build_marts.py`. Both use the
  `from airflow.sdk import Asset, dag, task` pattern and `Asset(name=...)` references already
  established by the ingest DAGs.

---

## 7. Component isolation

| Unit | Responsibility |
|---|---|
| (a) odds connector region change | request `regions="us,eu"` so Pinnacle lands |
| (b) `transforms/parsers/*` | pure `payload → typed rows` per source |
| (c) `transforms/normalize.py` | parse → upsert dimensions → append time-series (idempotent) |
| (d) `transforms/marts/edge_signals.py` | Pinnacle-anchored EV + arb (lib primitives) |
| (e) `transforms/marts/market_history.py` | resolved-market CLV / calibration |
| (f) `normalize` + `build_marts` DAGs | Asset-triggered orchestration around (c)/(d)/(e) |
| (g) migrations `007` + `008` | edge-signals sportsbook columns + `transform_state` watermark |
| (h) archive `bg_*` | move legacy DAGs out of the scheduler's path |

---

## 8. Testing

- **Parsers** (pure): real sampled payloads as fixtures → assert exact normalized rows. No DB.
- **Marts** (pure): hand-built normalized frames with known odds → assert the math (e.g. symmetric
  −110/−110 → Pinnacle fair 0.5; a constructed cross-book arb; an EV row above/below the 2.5%
  threshold; consensus fallback when Pinnacle absent). No DB.
- **Normalize + marts integration**: one test against Docker `postgres:16` — apply migrations, seed a
  raw row, run `normalize` then `build_marts`, assert tables populated; **re-run and assert
  idempotent** (no duplicate history rows, upserts stable).
- The analytics lib is untouched, so its existing tests still pass.

---

## 9. Success criteria

- Odds ingestion lands Pinnacle (eu region) rows; `normalize` populates `markets`/`market_outcomes`/
  `sports_events` (upsert) and appends `price_history`/`sportsbook_odds_history`, incrementally and
  idempotently.
- `build_marts` populates `mart_edge_signals` with Pinnacle-anchored EV + arb rows (consensus
  fallback), full-refreshed each run; the live `/edges` endpoint returns real rows.
- `mart_market_history` upserts resolved-market CLV rows as markets resolve (empty until then —
  expected).
- The two DAGs are Asset-triggered (normalize off the raw firehose, build_marts off normalized
  Assets); both import cleanly and run `state=success`.
- The legacy `bg_*` DAGs no longer load in the scheduler.
- All parser/mart unit tests + the integration test pass.

---

## 10. Risks / notes

- **Pinnacle delay**: The Odds API serves Pinnacle from its public site "which may incur a delay", and
  some events won't carry Pinnacle at all — hence the mandatory consensus fallback. EV edges are
  near-sharp, not live-sharp.
- **Quota**: `regions="us,eu"` doubles per-market credit cost (one charge per region). Acceptable given
  the high monthly quota; revisit if usage spikes.
- **`mart_edge_signals` schema growth** is additive (nullable columns), per Spec 2's anticipation.
- **No signal history in v1**: `mart_edge_signals` is full-refreshed; if signal history is wanted later
  it can be reconstructed from `sportsbook_odds_history` or the mart switched to append + partition.
- **Two-way markets only**: EV/arb logic targets two-outcome `h2h`. Multi-way markets (3-way soccer,
  Kalshi multi-outcome) are out of scope for the edge mart in v1.
- **Cross-venue matching deferred**: `mart_cross_market_prices` stays empty until its own spec; the
  `/cross-market` endpoint returns `[]` after this spec — expected.
