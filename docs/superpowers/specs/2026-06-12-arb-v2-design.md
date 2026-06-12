# Arbitrage Detection v2 — Design Spec

**Date:** 2026-06-12
**Status:** Approved (user answered scoping questions; decisions locked below)

## Problem

The pre-pivot arbitrage DAGs (`airflow/dags/_archive_pre_pivot/bg_arb_pipeline.py` for player
props, `bg_game_arb_pipeline.py` for game lines) are archived and incompatible with the
post-pivot data model (raw → normalize → marts, Airflow 3 assets). We want their detection
logic back, rewritten to fit the new model, with scope expanded to **sportsbook × prediction
market** arbs (Kalshi/Polymarket) for **both game lines and player props**. We also want four
new preview/experimental views in the web app.

## Locked decisions

1. **Detection-only.** New arb DAGs write to NEW tables only. Zero writes to
   `bg_arbitrage_opportunities`, `bot_execution_queue`, `bg_executed_opportunities`, or
   anything in `arbitrage_executor/`. The Chrome bot stays frozen.
2. **Full pipeline integration for props.** Player props flow ingest → `raw_market_snapshots`
   → normalize → new partitioned `player_props_odds_history` table. Props become durable,
   replayable history (movement charts and props analytics later).
3. **Cadence: 15 min, near-game window.** Props fetched every 15 minutes, only for events
   commencing within 24h. Regions `us` only for props (eu/Pinnacle rarely quotes US props and
   doubles cost).
4. **Web views:** Arbitrage board, Sharpness + calibration, Edge/EV signals, Props browser —
   all under an EXPERIMENTAL nav section.

## Architecture decisions

### D1. One `arb_opportunities` table, hash-PK upsert (no truncate/history pair)

The archive truncate-replaced a "current" table and appended a history table because a bot
polled it. Detection-only + concurrent web reads make truncate-rebuild wrong (empty-read
races). Instead: single table, `opportunity_hash` PK, upsert
`ON CONFLICT (opportunity_hash) DO UPDATE SET last_seen_at = now()`. The hash includes prices
(archive convention `.3f` line / `.6f` price), so any price move is a new row — the table is
naturally a history. `first_detected_at` vs `last_seen_at` distinguishes live from gone.
`/arbs` defaults to live rows (last_seen_at within a recent cutoff); `?include_stale=true`
exposes history. Not partitioned in v1 (the `BUILDER_MIN_ROI` storage floor keeps cardinality
sane); retention can be added later.

Unified leg schema covers all four classes (game/prop × book_book/book_venue): `kind`
('game'|'prop'), `pairing` ('book_book'|'book_venue'), `market_segment` ('h2h'|'spreads'|
'totals'| canonical prop key), `player_name`, `line`, `pairing_type` (std_std…, props
book_book only), generic `leg_a_*`/`leg_b_*` (kind book|venue, source, outcome, point, price,
stake), `payout`, `arb_ev`, `roi`, `fee_adjusted_roi`, `hours_until_commence`, `details` jsonb
(market_id/outcome_id/ticker/fee inputs/leg captured_ats).

### D2. Kalshi fee model; both-legs-normalized arb math

Kalshi taker fee: `ceil_to_cent(0.07 × contracts × P × (1−P))` (rate env-tunable
`BG_KALSHI_FEE_RATE`, default 0.07; verify the published fee schedule at implementation and
cite it). Polymarket: no trading fee. Detection uses the **ask** from `price_history.ask`
(cost to take), not `last_price`.

Book×venue condition, both legs normalized to $1 payout: book leg cost `1/d` (decimal d),
venue leg cost `ask + fee(ask)`. Total `T`; `fee_adjusted_roi = 1/T − 1`; arb iff > 0. Both
directions generated (book home + venue away-outcome YES, and vice versa). Buying NO ≡ YES on
the other outcome — iterating both `market_outcomes` rows covers it. Stakes split
proportional to leg cost.

### D3. Asset wiring

`build_marts` already uses list-schedule AND semantics and it works because `normalize` emits
ALL `NORMALIZED_ASSETS` from one task on every run. Therefore: add
`Asset("player_props_odds_history")` to `NORMALIZED_ASSETS`; schedule `build_arbs` on
`[Asset("sportsbook_odds_history"), Asset("player_props_odds_history"),
Asset("price_history")]`. `market_event_links` is read but not a trigger (refreshed by
build_marts; staleness only delays detection of newly listed venue markets by one cycle).

### D4. Props through the standard pipeline, new `record_type="player_prop"`

Existing `OddsApiConnector` extended (per-sport market lists, commence window, record_type
param, credit-header logging) and registered as a second registry entry
`the_odds_api_props` (regions `us`). **Critical:** `run_normalize`'s SELECT must add
`record_type` so props route to a new parser — today the router would shove player names into
`sportsbook_odds_history.outcome_name`.

Migration 017 `player_props_odds_history` mirrors `sportsbook_odds_history` (daily
range-partition on captured_at via partman premake 4, 2y retention, BRIN + event index,
partition-key-inclusive unique index `(event_id, market_key, player_name, line, side,
bookmaker, captured_at)` backing ON CONFLICT DO NOTHING). `partman_maintenance` picks it up
with zero changes. Side values: 'over'|'under' only (yes-tiles dropped at parse, archive
convention).

### D5. Game-line spreads/totals require a `point` column

`sportsbook_odds_history` has no point column and live ingestion is h2h-only. Migration 019:
`ALTER TABLE ... ADD COLUMN point numeric`; replace `uq_sb_odds_natural` with a unique index
including `point` (`NULLS NOT DISTINCT` if PG≥15, else COALESCE expression index);
`parse_odds_line` captures `outcome.point`; `_append_odds` writes it; registry main connector
becomes `markets="h2h,spreads,totals"`. **Blast-radius guards (required):**
`build_fair_prices` input filters `market_type = 'h2h'` explicitly (totals without per-line
grouping would corrupt fair probs); `edge_signals` grouping gains `point` or its feed query
filters to h2h. `build_market_history`/`cross_market` already filter h2h. Also add a free
commence window to game-line event listing (`BG_ODDS_WINDOW_HOURS`, default 48) — reduces
today's unwindowed burn and offsets the 3× market multiplier.

### D6. Props market lists / sports gating

Port `ARB_MARKETS_BY_SPORT`, `ARB_SPORTS`, `SPORT_TITLE`, `ARB_MARKET_BLACKLIST`
(pitcher_strikeouts ± _alternate) verbatim from
`_archive_pre_pivot/bg_arb_pipeline_lib/markets.py` into
`app/shared/python/bountygate/arb/markets.py`. Blacklist enforced in the pair builder, not at
ingest. Sports gated by env `BG_PROPS_SPORTS` (comma list; default
`basketball_nba,baseball_mlb,icehockey_nhl,americanfootball_nfl,basketball_wnba`) so
off-season sports cost only a free /events call.

### D7. Kalshi props matching: precision-first, config-driven, default-dormant

Kalshi prop series tickers are unverified. `BG_KALSHI_PROP_SERIES` (comma list, default
empty) feeds `connectors/kalshi.py`; until populated the book×venue props path is dormant but
fully tested against fixtures. Prop markets land as ordinary Kalshi `record_type="market"`
rows (existing parser handles them). Matching in `bountygate/arb/props_matching.py` off
**title parsing** with a per-sport STAT_PATTERNS regex map ("30+ points" → player_points,
threshold k → book line k−0.5; YES≡Over k−0.5, NO≡Under k−0.5), normalized exact player-name
match (casefold, strip punctuation; NO fuzzy matching in v1), event linkage required via
`market_event_links`. Unmatched/ambiguous counted in a stats dict, never guessed
(`link_rows` philosophy). Polymarket props ride the same path; expected yield ≈0.

### D8. Module layout / DAGs / alerts / freshness

`bountygate/arb/`: `markets.py`, `fees.py`, `hashing.py` (leg-ordered SHA-256, symmetric
pairs collapse, `.3f`/`.6f` convention), `pairs.py` (pure dict-based builders — NOT pandas,
post-pivot style), `venue.py`, `props_matching.py`, `build.py` (`run_build_arbs()`
orchestrator), `tests/`. DAGs: `ingest_props` (`*/15 * * * *`, outlet raw asset),
`build_arbs` (asset-triggered per D3, outlet `Asset("arb_opportunities")`, max_active_runs=1).
Discord via `bountygate.utils.discord_notify.notify(..., level="warning")` for newly-inserted
opps with `fee_adjusted_roi >= BG_ARB_ALERT_MIN_ROI` (default 0.01), batched per run.

Freshness guards (the archive got these free by truncating its stage table): book legs use
latest row per natural key within 20 min (game) / 35 min (props); venue asks within 30 min;
stale-legged pairs are not built. Port `hours_until_commence > 0`.

### D9. Web

Routers: `arb.py` (`GET /arbs?kind=&pairing=&min_roi=&include_stale=&limit=&offset=`,
live-cutoff computed in Python for sqlite portability) and `props.py`
(`GET /props?sport=&player=&market_key=&limit=`, latest row per natural key via correlated
MAX(captured_at) subquery — portable, no DISTINCT ON; player search via
LOWER(...) LIKE LOWER(:p)). `/sharpness`, `/calibration`, `/edges` endpoints already exist.

Frontend: `Masthead.tsx` gains a second nav row labeled EXPERIMENTAL with ARBITRAGE / PROPS /
SHARPNESS / EDGES. Four pages following the established useApi + DataTable + Pixel Augusta
patterns; recharts ^3 only. e2e seeds for `arb_opportunities`, `player_props_odds_history`,
`venue_sharpness`, `mart_calibration`, `mart_edge_signals` + one smoke per page.

### D10. Migration numbering

Main ends at 014; 015/016 are reserved by the in-flight bet-matching branch. Use 017/018/019
(gaps are harmless — `scripts/migrate.py` applies in name order). Check for collisions at
execution time.

## Credit cost estimate (The Odds API; bills ≈ markets × regions per event-odds call)

Props at 15-min cadence (96 cycles/day), 24h window, us-only:
- June now (MLB ~15ev×6mk, WNBA ~3×25, NBA finals ~1×25): ~190/cycle ≈ **~18k/day**
- Fall Sunday worst case (NFL 13×47 + NBA 10×25 + NHL 8×14): ~975/cycle ≈ **~94k/day**

Game lines after D5 (6 credits/event-call × ~30-40 windowed events × 288 cycles):
~55-70k/day — roughly offset by the new 48h window vs today's unwindowed h2h burn.

Mitigations: `BG_PROPS_SPORTS`, `BG_PROPS_WINDOW_HOURS` (default 24), connector logs
`x-requests-remaining`/`x-requests-used` every run, and the live smoke measures ONE manual
cycle empirically before the schedule is unpaused. Budget knobs if needed: 30-min cadence,
drop `_alternate` variants, us-only game lines.

## Risks

- **Credit burn** is the #1 risk — measured before steady-state; user confirms tier.
- Kalshi fee schedule must be verified at implementation (rate is configurable).
- Kalshi prop series existence unverified — path is default-dormant, fixture-tested.
- `NULLS NOT DISTINCT` needs PG≥15 — check server; COALESCE fallback documented.
- Book vs venue snapshot skew → freshness windows mitigate; leg captured_ats stored in
  `details` for display.
- normalize throughput: props add bulk through row-at-a-time appends; batch if lag appears.
