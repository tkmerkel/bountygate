# Quant Core (Tier 1) — Fair Prices, Closing Lines, Results, Scoring

**Date:** 2026-06-10
**Status:** Design — approved, pending implementation plan
**Stage 1 of the queue in** `docs/superpowers/specs/2026-06-10-analytics-platform-blueprint.md`.

The quant core puts Tier 1 of the modeling ladder into the database: per-method fair prices,
derived closing lines, game results, and the scoring loop (CLV, venue sharpness, calibration)
that every later stage — frontend, fundamental models, ML — reads from.

## 1. Context: what exists and is reused

- `analytics/devig.py` already implements **multiplicative, power, and Shin** devig as pure
  functions (`devig_all` aggregates all three). Not rebuilt — consumed.
- `analytics/consensus.py` — unweighted multiplicative no-vig consensus. Extended, not replaced.
- `enrichment/results.py` + `enrichment/clients.py` — ESPN scoreboard / MLB StatsAPI / NHL API
  parsers and URL builders for game finals. Reused by the new results DAG.
- `enrichment/match.py` — team-name + date matching, reused to match results to `sports_events`.
- `sportsbook_odds_history` / `price_history` — partitioned snapshot tables continuously fed by
  the ingest DAGs. The raw material for fair prices, closing lines, and movement.
- `analytics/clv.py` is keyed to the legacy player-prop schema (`bg_event_id`/`player_name`);
  it stays untouched. New-schema scoring lives in the new `models/` package.

### Key design decision: closing lines are a derivation, not a race

Ingest already snapshots continuously, so the closing line for an event is **the last snapshot
before `commence_time`**, derivable any time after the game starts. No time-critical capture
window. Each derived row records `staleness_minutes` (commence_time − snapshot captured_at) as a
data-quality flag; rows with large staleness are still stored but flagged.

## 2. Scope guards

- **Markets:** h2h (moneyline) and totals for fair prices, closing lines, and movement;
  **two-way markets only** (matches the existing edge-signals v1 constraint). **Scoring
  (Brier/CLV/calibration) covers h2h only in this stage** — resolving a totals bet needs the
  point line, and `sportsbook_odds_history` carries no line column; totals scoring is deferred
  until that column lands (a later, additive migration). `game_results` stores both scores, so
  backfilled totals scoring is possible once lines are captured.
- **Sports:** MLB, NBA, NHL (`sport_key` prefixes already present in `sports_events`).
- **Out of scope:** frontend work (stage 2), Tier 2/3 models (stages 3/5), player props, alerts.
  Odds API call cadence is **unchanged** by this stage — fair prices and closing lines derive
  from snapshots already being captured. The `BG_INCLUDE_SHARP_BOOKS` toggle decision is
  recorded per-environment in `.env`, not changed here.

## 3. Library — new `bountygate/models/` package

Pure functions + thin DB builders, mirroring the `transforms/` pattern (pure module + builder
that owns SQL). All call `analytics/` primitives; no I/O in the pure modules.

| Module | Responsibility |
|---|---|
| `models/fair.py` | Per-book, per-method fair probs for a two-way group (`devig_all`); consensus blend across books. `weighted_consensus(probs_by_book, weights)` takes a `{book: weight}` dict; equal weights reproduce today's unweighted consensus. |
| `models/weights.py` | Sharpness weights from accumulated venue Brier scores: inverse-Brier normalized. **Pinnacle-anchored prior** (Pinnacle weight 1.0, others 0.5) until a venue has ≥ N scored games (default N=200, configurable via env `BG_SHARPNESS_MIN_GAMES`). |
| `models/closing.py` | Closing-line derivation: given an event's history rows + commence_time, pick the last pre-commence snapshot per (bookmaker, outcome) and compute fair probs + staleness. |
| `models/scoring.py` | Brier score, log loss, calibration bucketing (10 equal-width prob buckets), CLV vs consensus close. Operates on (predicted_prob, realized) pairs — venue closing lines and `model_predictions` rows score through the same functions. |

## 4. Schema — migrations 011–014

**011 — `fair_prices`** (partitioned by `captured_at`, partman daily, 2y retention like the
history tables):

```sql
fair_prices (
  event_id      uuid        NOT NULL,
  market_type   text        NOT NULL,
  bookmaker     text        NOT NULL,   -- or 'consensus'
  outcome_name  text        NOT NULL,
  method        text        NOT NULL,   -- 'mult' | 'power' | 'shin' | 'weighted'
  fair_prob     numeric     NOT NULL,
  captured_at   timestamptz NOT NULL
) PARTITION BY RANGE (captured_at);
```

**012 — `closing_lines`** (regular table; one row per event/market/book/outcome):

```sql
closing_lines (
  event_id          uuid NOT NULL,
  market_type       text NOT NULL,
  bookmaker         text NOT NULL,
  outcome_name      text NOT NULL,
  decimal_price     numeric,
  fair_prob         numeric,            -- multiplicative devig of this book's close
  captured_at       timestamptz,        -- snapshot used
  staleness_minutes numeric,
  UNIQUE (event_id, market_type, bookmaker, outcome_name)
)
```

**013 — `model_versions` + `model_predictions`:**

```sql
model_versions (
  model_key   text NOT NULL,            -- e.g. 'consensus_v1'
  version     text NOT NULL,
  created_at  timestamptz DEFAULT now(),
  description text,
  PRIMARY KEY (model_key, version)
)
model_predictions (
  model_key    text NOT NULL,
  version      text NOT NULL,
  event_id     uuid NOT NULL,
  market_type  text NOT NULL,
  outcome_name text NOT NULL,
  prob         numeric NOT NULL,
  predicted_at timestamptz NOT NULL,
  UNIQUE (model_key, version, event_id, market_type, outcome_name, predicted_at)
)
```

The sharpness-weighted consensus registers as `consensus_v1` — the first row of the common
prediction shape all later tiers write to.

**014 — results + scoring outputs:**

```sql
game_results (
  event_id     uuid PRIMARY KEY REFERENCES sports_events(event_id),
  home_score   int, away_score int,
  winner       text,                    -- team name, matches outcome_name convention
  completed_at timestamptz,
  source       text                     -- 'espn' | 'mlb_statsapi' | 'nhl_api'
)
venue_sharpness (
  venue_key text NOT NULL, sport_key text NOT NULL,
  window    text NOT NULL,              -- 'all' | 'last_90d'
  n_games   int, brier numeric, logloss numeric, avg_clv numeric,
  computed_at timestamptz,
  UNIQUE (venue_key, sport_key, window)
)
mart_calibration (
  source      text NOT NULL,            -- venue_key or model_key
  sport_key   text NOT NULL,
  prob_bucket numeric NOT NULL,         -- bucket lower bound, 0.0..0.9
  n int, predicted_mean numeric, realized_rate numeric,
  computed_at timestamptz,
  UNIQUE (source, sport_key, prob_bucket)
)
```

## 5. DAGs

| DAG | Trigger | Work |
|---|---|---|
| `build_fair_odds` | asset: `sportsbook_odds_history` | Latest snapshot per event/market/book → per-method `fair_prices` rows + weighted-consensus rows (bookmaker='consensus', method='weighted') + `model_predictions` rows as `consensus_v1`. |
| `derive_closing_lines` | hourly | Events with `commence_time < now()` and no `closing_lines` rows → derive from history via `models/closing.py`. Idempotent (`ON CONFLICT DO NOTHING`). |
| `ingest_results` | hourly (sport-aware: only when games could be final) | ESPN scoreboard primary; MLB StatsAPI / NHL API fallback per sport. Match to `sports_events` by team-name + date → upsert `game_results`. |
| `score_results` | asset: `game_results` | For newly resolved events: score each venue's closing fair prob and each `model_predictions` row against the result → recompute `venue_sharpness` (full + 90d windows) and `mart_calibration`; per-venue CLV vs consensus close. Full-recompute of the two small output tables (idempotent by construction). |

**Gap detection:** a task in `derive_closing_lines` flags events whose closing
`staleness_minutes > 60` and pings Discord (existing `discord_notify` util) with a daily summary
— this is the ingest-uptime monitor the blueprint requires.

## 6. API

New routers under `app/web/routers/`, same TestClient-tested pattern:

- `GET /fair-odds?sport&date` — per event/outcome: weighted-consensus prob, each venue's latest
  price + implied prob, edge vs consensus. The fair-odds screen's data source.
- `GET /closing-lines?event_id` — closing rows for an event.
- `GET /sharpness` — `venue_sharpness` rows (the leaderboard source).
- `GET /calibration?source` — `mart_calibration` rows (reliability-curve source).
- `GET /movement/{event_id}?market_type` — time series from `sportsbook_odds_history` +
  `price_history` for charting (downsampled to ≤500 points server-side).

Existing `/history` router unchanged.

## 7. Testing

- **Pure functions:** known-answer fixtures for Shin/power devig (published Buchdahl examples);
  weighted-consensus algebra (equal weights == current unweighted output); calibration/Brier/
  log-loss against hand-computed values; closing-line derivation over synthetic history fixtures
  (picks last pre-commence row, computes staleness, handles missing books).
- **Builders:** SQL builders tested against a transaction-rolled-back test DB where the existing
  transform tests have one, else mocked-connection row-shape tests (follow existing pattern in
  `transforms/tests/`).
- **DAGs:** import smoke tests (pattern exists in `airflow/tests`).
- **API:** contract tests per endpoint via TestClient (pattern exists in `app/web/tests/`).

## 8. Success criteria

1. After a normal ingest cycle, `fair_prices` holds per-method rows per book + consensus rows,
   and `model_predictions` holds `consensus_v1` rows for upcoming MLB/NBA/NHL games.
2. The morning after a game day: `closing_lines` rows exist with sane staleness;
   `game_results` has finals; `venue_sharpness` and `mart_calibration` are populated.
3. `GET /fair-odds` returns consensus + venue prices + edges; all five endpoints pass contract
   tests; existing tests stay green.
4. Discord receives a gap summary only when closing staleness exceeded threshold.
5. All new pure functions unit-tested; DAG import smoke green.
