# Unified Arbitrage Pipeline — Design

**Status:** Draft (awaiting user review)
**Date:** 2026-05-15
**Author:** Tim Merkel + Claude
**Implements:** New self-contained arb data pipeline that covers all 4 std/alt pairing directions and gives the bot an unambiguous per-leg execution contract.

---

## Why

The current arb data layer is leaking opportunities and confusing the bot. Two concrete symptoms:

1. **Coverage gap.** The existing alt DAG only models `std-under × alt-over` arbitrage pairs. FanDuel uniquely offers `alt-under` markets, and `alt × alt` cross-book pairs exist whenever both books offer the alt direction in question. Today, neither `alt-under × std-over` nor `alt × alt` is generated. Per the user's analysis, alt-bearing opportunities have higher ROI and higher volume than std-only — the missing pairings are likely where the biggest unrealized value sits.

2. **Schema "lying" to the bot.** The current alt table strips the `_alternate` suffix from `_base_market_key` and projects it as `market_key`. Downstream code reads `market_key` and cannot tell which leg is the alt variant without inspecting `under_market_key` vs `over_market_key` — and `map_selectors.py` (until today) didn't even use those columns, producing the "no opportunities found" failure for any `*_alternate` market we tried to map. The implicit half-encoding of leg-type into one collapsed column is the root of the recurring bot fragility around alt markets.

In addition, the arb pipeline is **coupled** to the unified/underdog pipeline (`bg_arbitrage_player_props.py` reads from `bg_unified_lines_stage_odds`). The user wants the two pipelines fully decoupled so changes to one cannot break the other.

## What

A new self-contained arb pipeline that:

- Makes its own calls to the-odds-api (no shared raw tables with `bg_unified_*`).
- Produces **one unified opportunity table** (`bg_arbitrage_opportunities`) with explicit per-leg columns and no collapsed `market_key`.
- Generates rows for **all four pairing directions** symmetrically.
- Gives the bot a clean contract: per row, two legs, each with `(book, market_key, line, direction, price)`. The `_alternate` suffix on `market_key` remains the bot's signal to use the alt-execution path.

The unified/underdog pipeline (`bg_unified_dag.py`, `bg_unified_analysis_dag.py`, `update_underdog_outlier_analysis.py`) is untouched.

## Scope

**In scope:**
- New Airflow DAG: `bg_arb_pipeline` (own ingest, own normalize, own build).
- New tables: `bg_arb_stage_lines`, `bg_arbitrage_opportunities`, `bg_arb_opportunities_history`.
- Schema change to `bg_executed_opportunities` (add `under_market_key`, `over_market_key`; backfill).
- Bot-side code changes: `opportunity.py`, `map_selectors.py`, `bet_placer.py`, `execute_arb.py` consume the new schema.
- Unit tests for the builder, ingest, and hash functions (new file: `airflow/tests/`).
- SQL parity-check scripts for the Phase 1 cutover validation.
- Old arb DAG (`bg_arbitrage_player_props.py`) is removed in Phase 3 of the migration.
- Old arb tables (`bg_arbitrage_player_props`, `bg_arbitrage_player_props_alt`) drop in Phase 4 (deferred — kept until the new pipeline has run cleanly for ~1 month).

**Out of scope:**
- Any changes to `bg_unified_*` DAGs or tables.
- Chrome/CDP launch behavior (frozen).
- Selector YAML schema (works as-is with the new per-leg keys).
- CI/CD setup (deferred to a follow-up; tests run locally for now).
- Reliability items from `docs/archive/CRITIQUE-2026-04-20.md` (orphan reconciler, balance reconciliation, credential rotation) — those are separate concerns.
- `bet_placer.py` god-object refactor (CRITIQUE #12).

## Architecture

```
the-odds-api  ─────┐
                   │ /v4/sports/{sport}/events
                   │ /v4/sports/{sport}/events/{event_id}/odds
                   ▼
        ┌───────────────────────────────────────┐
        │  bg_arb_pipeline DAG  (new)           │
        │                                       │
        │  task: ingest_odds                    │
        │    -> bg_arb_stage_lines              │
        │                                       │
        │  task: build_opportunities            │
        │    -> bg_arbitrage_opportunities      │
        │                                       │
        │  task: record_history                 │
        │    -> bg_arb_opportunities_history    │
        └───────────────────────────────────────┘
                   │
                   ▼
        ┌───────────────────────────────────────┐
        │  arbitrage_executor (bot)             │
        │                                       │
        │  opportunity.py  --> reads            │
        │     bg_arbitrage_opportunities        │
        │                                       │
        │  map_selectors.py --> reads same      │
        │                                       │
        │  execute_arb.py / bet_placer.py       │
        │     consume per-leg fields            │
        │     (under_market_key, over_market_key) │
        │                                       │
        │  on success/failure --> writes        │
        │     bg_executed_opportunities         │
        └───────────────────────────────────────┘
```

The unified/underdog pipeline runs in parallel against the same `the-odds-api`, with its own `bg_unified_*` tables. The two pipelines share zero state below the level of "they both call the-odds-api."

## The unified opportunity table

**`bg_arbitrage_opportunities`** replaces both `bg_arbitrage_player_props` and `bg_arbitrage_player_props_alt`.

```sql
CREATE TABLE bg_arbitrage_opportunities (
    -- identity / dedup
    opportunity_hash         text PRIMARY KEY,

    -- event context
    event_id                 text         NOT NULL,
    sport_title              text         NOT NULL,
    home_team                text         NOT NULL,
    away_team                text         NOT NULL,
    player_name              text         NOT NULL,

    -- the "fact" of the line being arb-able
    canonical_market         text         NOT NULL,  -- 'player_assists' (base, no _alternate)
    pairing_type             text         NOT NULL,  -- 'std_std' | 'std_alt' | 'alt_std' | 'alt_alt'

    -- under leg (what the bot needs to place the under bet)
    under_book               text         NOT NULL,  -- 'fanduel' | 'betmgm'
    under_market_key         text         NOT NULL,  -- 'player_assists' or 'player_assists_alternate'
    under_line               numeric      NOT NULL,
    under_price              numeric      NOT NULL,

    -- over leg (symmetric)
    over_book                text         NOT NULL,
    over_market_key          text         NOT NULL,
    over_line                numeric      NOT NULL,
    over_price               numeric      NOT NULL,

    -- arb economics
    wager_under              numeric      NOT NULL,
    wager_over               numeric      NOT NULL,
    payout                   numeric      NOT NULL,
    arb_ev                   numeric      NOT NULL,
    roi                      numeric      NOT NULL,

    -- freshness
    hours_until_commence     numeric      NOT NULL,
    fetched_at_utc           timestamp    NOT NULL
);

CREATE INDEX idx_arb_opp_fetched         ON bg_arbitrage_opportunities (fetched_at_utc DESC);
CREATE INDEX idx_arb_opp_roi             ON bg_arbitrage_opportunities (roi DESC);
CREATE INDEX idx_arb_opp_pairing         ON bg_arbitrage_opportunities (pairing_type);
CREATE INDEX idx_arb_opp_player          ON bg_arbitrage_opportunities (player_name);
```

**Why this shape:**

- **No collapsed `market_key` column.** The bot reads `under_market_key` to load the under leg's selector YAML entry, `over_market_key` for the over leg. The `_alternate` suffix on either key is the bot's signal to take the alt-execution path on that book.
- **`canonical_market`** is the base name (no `_alternate` suffix), denormalized for fast filtering ("show me all `player_assists` opportunities regardless of pairing type"). Cheap to derive at write time, expensive to derive at query time.
- **`pairing_type`** is derived from the two market keys at write time and stored. Saves repeated suffix-stripping in dashboards.
- **No `canonical_line` column.** The-odds-api already normalizes line displays — FanDuel's `over 3+` UI label arrives as `line=2.5`. After ingest, `under_line == over_line` always.
- **`opportunity_hash` includes prices.** A price shift produces a new hash → new row, while the old row ages out. Keeps the table aligned with `bg_executed_opportunities`' price-sensitive dedup.

**Hash derivation:**

```python
def opportunity_hash(row) -> str:
    parts = (
        row["event_id"],
        row["player_name"],
        row["under_book"],
        row["under_market_key"],
        row["over_book"],
        row["over_market_key"],
        f"{row['under_line']:.2f}",
        f"{row['under_price']:.4f}",
        f"{row['over_price']:.4f}",
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
```

**Pairing type derivation:**

```python
def derive_pairing_type(under_market_key: str, over_market_key: str) -> str:
    under_is_alt = under_market_key.endswith("_alternate")
    over_is_alt  = over_market_key.endswith("_alternate")
    if   not under_is_alt and not over_is_alt: return "std_std"
    elif not under_is_alt and     over_is_alt: return "std_alt"
    elif     under_is_alt and not over_is_alt: return "alt_std"
    else:                                       return "alt_alt"
```

## The DAG transformation logic

One Airflow DAG with three tasks. All `bg_arb_*` prefixed.

### Task: `ingest_odds`

- Fetches the-odds-api for each sport in `["NBA", "NHL", "NFL", "MLB"]`:
  - `GET /v4/sports/{sport}/events?apiKey=…&regions=us` → list of events
  - For each event: `GET /v4/sports/{sport}/events/{event_id}/odds?regions=us&bookmakers=fanduel,betmgm&markets={market_list}` → odds payload
- Markets list lives as a Python constant `ARB_MARKETS` at the top of the DAG file (player_points, player_assists, player_rebounds, etc., plus `_alternate` variants). Pulled directly from the-odds-api `/v4/sports/{sport}/markets` reference. Maintaining the list is a future improvement (move to DB or YAML) but a Python constant is fine for v1.
- Normalizes JSON into a per-(book, market_key, line, side) row schema:
  - Columns: `event_id, sport_title, home_team, away_team, commence_time_utc, player_name, bookmaker_key, market_key, line, side, price, fetched_at_utc`
  - `side` is `"under"` or `"over"`
- Writes to `bg_arb_stage_lines` with `if_exists='replace'` (each run is a fresh snapshot).

### Task: `build_opportunities`

Reads `bg_arb_stage_lines`, applies the symmetric cartesian, writes `bg_arbitrage_opportunities` with `if_exists='replace'`.

```
1. Add canonical_market column (strip "_alternate" suffix from market_key).
2. Partition the lines into unders and overs:
   unders = rows where side = "under"
   overs  = rows where side = "over"
3. Cartesian join unders × overs on:
     (event_id, player_name, canonical_market, line)
   Constraint: under.bookmaker_key != over.bookmaker_key
4. For each candidate pair, compute arb economics:
     implied_under = 1.0 / under_price
     implied_over  = 1.0 / over_price
     overround     = implied_under + implied_over
     if overround >= 1.0: skip (no arb)
     else:
        wager_under = base_wager / overround * implied_under
        wager_over  = base_wager / overround * implied_over
        payout      = base_wager / overround
        arb_ev      = payout - base_wager
        roi         = arb_ev / base_wager
5. Derive pairing_type from under_market_key and over_market_key.
6. Filter:
     roi > 0
     hours_until_commence > 0
     sport_title in ARB_SPORTS
7. Compute opportunity_hash.
8. Write the result set with if_exists='replace'.
```

Base wager (e.g., $100) is a constant used to compute relative `wager_under` / `wager_over` ratios. Actual stake sizing happens in the bot (`opportunity.py` re-scales based on the FanDuel max-wager limit).

### Task: `record_history`

Reads the just-written `bg_arbitrage_opportunities`. For each `opportunity_hash` not already in `bg_arb_opportunities_history`, appends. Idempotent via `ON CONFLICT (opportunity_hash) DO NOTHING`. Append-only.

History table is for analysis ("which `pairing_type` × `canonical_market` × hour-of-day produced the highest historical ROI?"). Not consumed by the bot.

### DAG schedule

Match the existing arb DAG cadence. The existing `bg_arbitrage_player_props.py` schedule is the reference — likely every 5-15 minutes pregame. Locked in implementation, not design.

## Bot consumption side

### `opportunity.py`

Becomes one query against `bg_arbitrage_opportunities`. No more two-table union, no more divergent SELECT projections.

```python
query = f"""
SELECT *
FROM bg_arbitrage_opportunities
WHERE under_book IN ('fanduel', 'betmgm')
  AND over_book  IN ('fanduel', 'betmgm')
  AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
  AND fetched_at_utc >= now() AT TIME ZONE 'utc' - INTERVAL '{window}'
  AND hours_until_commence > 0
  AND hours_until_commence < {max_hours}
  AND roi >= {MIN_ROI_THRESHOLD}
  AND NOT EXISTS (
    SELECT 1 FROM bg_executed_opportunities eo
    WHERE eo.player_name      = bg_arbitrage_opportunities.player_name
      AND eo.under_market_key = bg_arbitrage_opportunities.under_market_key
      AND eo.over_market_key  = bg_arbitrage_opportunities.over_market_key
      AND eo.executed_at_utc >= CURRENT_DATE
  )
ORDER BY roi DESC
LIMIT 10;
"""
```

### `map_selectors.py`

```python
def fetch_opportunity_for_market(market_key: str, bookmaker: str):
    query = f"""
    SELECT *
    FROM bg_arbitrage_opportunities
    WHERE (
            ('{market_key}' = under_market_key AND '{bookmaker}' = under_book)
         OR ('{market_key}' = over_market_key  AND '{bookmaker}' = over_book)
        )
      AND fetched_at_utc >= now() AT TIME ZONE 'utc' - INTERVAL '4 hours'
      AND hours_until_commence > 0
    LIMIT 1;
    """
```

The bug fixed earlier today (the `_alt` fallback querying a column that doesn't exist) cannot exist in this version — there is only one table and one schema.

### `bet_placer.py` and `execute_arb.py`

The opportunity dict gains explicit per-leg fields. Code that today reads `opportunity['market_key']` is updated per leg:

```python
# Under leg
under_selectors = load_selectors(book=opp['under_book'],
                                  market_key=opp['under_market_key'])
place_bet(under_selectors, line=opp['under_line'], side='under', ...)

# Over leg
over_selectors  = load_selectors(book=opp['over_book'],
                                  market_key=opp['over_market_key'])
place_bet(over_selectors, line=opp['over_line'], side='over', ...)
```

The 3-phase execution (tease FD limit → MGM bet → FD hedge) is unchanged. Only the source of `market_key` shifts from "row-level" to "per-leg."

### `bg_executed_opportunities`

Schema change. Two new columns:

```sql
ALTER TABLE bg_executed_opportunities ADD COLUMN under_market_key text;
ALTER TABLE bg_executed_opportunities ADD COLUMN over_market_key  text;

-- Backfill from existing rows:
-- Rows from std table history: both new columns = the existing market_key
-- Rows from alt table history: under_market_key = market_key,
--                              over_market_key  = market_key || '_alternate'
UPDATE bg_executed_opportunities
SET under_market_key = market_key,
    over_market_key  = market_key
WHERE under_market_key IS NULL
  AND source_table = 'bg_arbitrage_player_props';

UPDATE bg_executed_opportunities
SET under_market_key = market_key,
    over_market_key  = market_key || '_alternate'
WHERE under_market_key IS NULL
  AND source_table = 'bg_arbitrage_player_props_alt';
```

(The exact backfill SQL depends on what's actually in `bg_executed_opportunities` today — whether it has a `source_table` column to disambiguate, or whether we need to infer from another signal. Validate at implementation time.)

### Selector YAMLs

**Unchanged.** Existing entries key on full market_key strings (`player_assists`, `player_assists_alternate`). The new schema feeds the bot per-leg keys directly. No YAML migration.

Some books will surface alt-bearing opportunities that weren't previously mapped (e.g., FanDuel `*_alternate` on the under side). Expect a 1-2 day "map the gaps" pass after cutover, using the existing `python map_selectors.py --site <site> --market <market>` flow.

## Migration plan

Four phases, each independently pausable.

### Phase 1 — Build the new pipeline alongside (no behavior change)

- Add `airflow/dags/bg_arb_pipeline.py` (new DAG, three tasks).
- Add migrations for the new tables (`bg_arb_stage_lines`, `bg_arbitrage_opportunities`, `bg_arb_opportunities_history`).
- Run for 24-48 hours alongside the existing arb DAG.
- Validate using parity-check SQL (see Testing section). Expect new pipeline to be a **superset** of old: every old row corresponds to a new row, plus new rows in `alt_std` and `alt_alt`.

### Phase 2 — Switch reads (bot moves to new table)

In a single coordinated commit:

- Schema migration on `bg_executed_opportunities` (add columns + backfill).
- Update `arbitrage_executor/opportunity.py` to query `bg_arbitrage_opportunities`.
- Update `arbitrage_executor/map_selectors.py` to query the same.
- Update `arbitrage_executor/bet_placer.py` and `execute_arb.py` to read `under_market_key` / `over_market_key` per leg.

Run the bot supervised for 2-3 sessions. Watch:

- Heartbeat counts (attempted / placed / skipped).
- `selectors not mapped` warnings — expect a small uptick from previously-unattempted alt combinations. Map them via the existing flow.
- ROI distribution per `pairing_type` — confirm or refute the hypothesis that alt-bearing pairings dominate.

### Phase 3 — Stop writing to old tables

- Remove `airflow/dags/bg_arbitrage_player_props.py` from the DAG bag.
- Old tables (`bg_arbitrage_player_props`, `bg_arbitrage_player_props_alt`) remain in the DB as read-only history. No code writes to them.

### Phase 4 — Drop old tables (deferred, optional)

- Defer until ~1 month of stable operation on the new pipeline.
- Drop `bg_arbitrage_player_props` and `bg_arbitrage_player_props_alt`.
- Drop the (now legacy) `market_key` column from `bg_executed_opportunities` if it has been replaced by the new pair of columns and nothing else depends on it.

### What the migration does NOT require

- Re-ingest of historical data. The-odds-api is the upstream source of truth; old odds data is point-in-time and not needed.
- Selector YAML changes.
- Chrome / CDP / launch-behavior changes.
- `bg_unified_*` changes.
- Existing audit_logs/* state — they continue to land in `arbitrage_executor/audit_logs/`.

## Testing strategy

### Layer 1 — Unit tests on pure functions

New file: `airflow/tests/test_arb_builder.py` (or `app/shared/python/bountygate/tests/test_arb_builder.py`).

The build step is refactored into a pure function: `build_opportunities(lines_df: DataFrame) -> DataFrame`. No DB, no network, no Airflow runtime in the test path.

Required test cases:
- `test_std_under_std_over_emits_std_std_row`
- `test_std_under_alt_over_emits_std_alt_row`
- `test_alt_under_std_over_emits_alt_std_row` (the missing case today)
- `test_alt_under_alt_over_emits_alt_alt_row` (the other missing case)
- `test_intra_book_pair_emits_nothing` (under and over both on fanduel: no arb)
- `test_line_mismatch_emits_nothing` (joins require equal line)
- `test_negative_roi_pair_emits_nothing` (overround >= 1.0 filtered)
- `test_pairing_type_is_purely_string_based` (function correctness)
- `test_opportunity_hash_stable_across_runs` (same input → same hash)
- `test_opportunity_hash_differs_on_price_change` (rounding-stable encoding)
- `test_canonical_market_strips_alternate_suffix_only_when_present`

Runs in < 1 second total via `pytest`.

### Layer 2 — Fixture-based tests for the ingest task

New file: `airflow/tests/test_arb_ingest.py`. Fixtures in `airflow/tests/fixtures/`:

- `fixture_std_only.json` — a the-odds-api event response with only standard markets.
- `fixture_alt_rich.json` — a response with both std and `_alternate` variants.
- `fixture_fanduel_alt_under.json` — a response demonstrating FanDuel-specific alt-under markets.

Mock the HTTP call. Run the ingest normalizer. Assert the produced rows match the expected shape and count. Purpose: catch the-odds-api schema drift in CI, not at 3 AM.

### Layer 3 — Phase 1 SQL parity-check (one-shot, not recurring)

Script in `scripts/dq_checks_arb_parity.py` or notebook:

```sql
-- Every std_std row in the new table must also exist in the old std table:
SELECT player_name, under_market_key, over_market_key, under_book, over_book
FROM bg_arbitrage_player_props
WHERE fetched_at_utc >= now() - INTERVAL '1 day'
EXCEPT
SELECT player_name, under_market_key, over_market_key, under_book, over_book
FROM bg_arbitrage_opportunities
WHERE pairing_type = 'std_std'
  AND fetched_at_utc >= now() - INTERVAL '1 day';
-- Expect: zero rows. Non-zero means parity bug.

-- Every std_alt row in the new table must also exist in the old alt table:
SELECT player_name, under_market_key, over_market_key, under_book, over_book
FROM bg_arbitrage_player_props_alt
WHERE fetched_at_utc >= now() - INTERVAL '1 day'
EXCEPT
SELECT player_name, under_market_key, over_market_key, under_book, over_book
FROM bg_arbitrage_opportunities
WHERE pairing_type = 'std_alt'
  AND fetched_at_utc >= now() - INTERVAL '1 day';
-- Expect: zero rows.

-- Sanity-check: the new pipeline is actually emitting alt_std and alt_alt rows:
SELECT pairing_type, COUNT(*), AVG(roi), MAX(roi)
FROM bg_arbitrage_opportunities
WHERE fetched_at_utc >= now() - INTERVAL '1 day'
GROUP BY pairing_type
ORDER BY pairing_type;
-- Expect: all 4 pairing_types present with non-zero counts and positive ROI.
```

If the first two return non-zero rows during Phase 1 → fix before Phase 2.
If the third shows zero `alt_std` or `alt_alt` rows after a full ingest → the symmetric logic is not firing → investigate.

### What is explicitly NOT tested

- Bot-side Playwright execution against live sportsbook UIs (already covered by `toolkit/selector_smoke_test.py` and live operation).
- End-to-end "from API to placed bet" — no reliable sandbox.
- CI infrastructure — out of scope; tests run locally via `pytest` for now.

## Open questions for implementation

1. **Markets list:** the exact list of `market_key` strings to fetch from the-odds-api. Pull from the-odds-api docs at implementation time. v1 lives as a Python constant in the DAG; future iteration can move to YAML or a DB-driven config.
2. **DAG schedule:** match the existing `bg_arbitrage_player_props.py` interval. Locked at implementation, not design.
3. **`bg_executed_opportunities` backfill SQL:** depends on the exact existing schema (does it have a `source_table` discriminator?). Validate before running the backfill.
4. **Base wager value:** matched to current arb DAG ($100 likely). Confirm at implementation.

## Out-of-scope follow-ups

These are real concerns but not part of this design:

- The `bet_placer.py` 1062-line god-object split (CRITIQUE #12). Now safer with `tests/` in place but still risk-bearing.
- Reliability items from CRITIQUE: orphan task reconciler, balance reconciliation between phases, Discord webhook fallback hardening.
- CI workflow that runs the new test suite on every push (CRITIQUE #18).
- `scripts/local_dev_setup.ps1` is broken (references nonexistent `app/requirements.txt`).
- The `screen_recorder.py` ffmpeg-leak bug (a stray ffmpeg process can hold a recording file open indefinitely on abrupt termination).

These all get their own future plans.
