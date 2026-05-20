# Game-Line Arbitrage Pipeline — Design & Plan

**Status:** Draft (awaiting user approval)
**Date:** 2026-05-19
**Repo:** `C:\Users\tkmer\bountygate`
**Companion deliverable on approval:** `bountygate/docs/superpowers/specs/2026-05-19-game-line-arb-pipeline-design.md` (proper home for the spec inside bountygate; this plan file is the planning-mode mirror).

---

## Context

Bountygate's arbitrage stack today only detects **player-prop arbitrage**. The just-shipped `bg_arb_pipeline` (Phase 2 of `arb-rewrite-phase2`) gives the bot a clean per-leg contract for player props but is hard-coded to player-prop semantics: the `bg_arbitrage_opportunities` schema has `player_name NOT NULL`, the ingest only requests player-prop markets, and the bot's selector mapping is keyed for player-prop UI flows.

The `/pp-odds-api` CLI surfaced what's left on the table from the same upstream (The Odds API) that bountygate already pays for: **game-level markets (h2h, spreads, totals)** across the same 5 sports, on the same US books the executor already automates (FanDuel + BetMGM) plus two more US books bountygate has historical pricing for (Caesars/williamhill_us, DraftKings). Game-line arbs are simpler than player props (no alt-line variants, no name normalization across books) and operate on a wholly different pricing surface, so they don't compete with the in-flight refactor for capacity.

**Outcome we want:** within ~2 weeks, see a steady Discord feed of game-level arb opportunities across FD/MGM/Caesars/DK that we can manually execute for 2–4 weeks to validate EV. If validated, Phase 2 promotes the FD×MGM subset to the existing executor.

## Scope

**In scope (v1):**
- New sidecar Airflow DAG: `bg_game_arb_pipeline` — own ingest, own builder, own tables.
- New tables: `bg_game_arb_stage_lines`, `bg_arbitrage_game_opportunities`, `bg_arb_game_opportunities_history`.
- Direct REST calls to `the-odds-api` (no CLI / no MCP in the runtime path — `/pp-odds-api` was the research tool, not the runtime tool).
- 5 sports: `americanfootball_nfl`, `basketball_nba`, `icehockey_nhl`, `baseball_mlb`, `basketball_ncaab`.
- 3 markets: `h2h`, `spreads`, `totals` (outrights deferred — see Out of scope).
- 4 books: `fanduel`, `betmgm`, `williamhill_us` (Caesars), `draftkings`.
- 1 region: `us`.
- Builder produces 2-leg opportunities with explicit per-leg `outcome`/`point`/`price` columns (mirrors the per-leg discipline of the player-prop refactor; does not reuse the player-prop schema).
- Discord alerter: posts new high-value opps with a per-book deeplink stub (event-page URLs where stable, otherwise sport landing page).
- Unit tests on the pure builder function with fixture inputs covering h2h, spreads, totals, and the negative cases.
- One-off SQL spot-check queries committed as `scripts/dq_checks_game_arb.sql` for the first 48-hour shake-out.

**Out of scope (v1):**
- Executor integration (Phase 2 after 2–4 weeks of validation).
- Outrights — N-way arb math and futures-specific stake sizing get their own design.
- New regions (`us2`, `uk`, `eu`, `au`) — detection-only value is dubious for US-resident execution.
- New sports beyond the existing 5 — NCAAF/WNBA/MLS reserved for a future expansion.
- Any change to `bg_arb_pipeline` or `bg_unified_*`.
- Selector mapping YAMLs (only needed when Phase 2 promotes to executor).
- Tier-promotion logic between `roi > 0` (table) and `roi >= ALERT_ROI_THRESHOLD` (Discord) — keep both, threshold tunable via env var.

## Architecture

```
the-odds-api ─────┐
                  │ /v4/sports/{sport}/odds?regions=us
                  │   &bookmakers=fanduel,betmgm,williamhill_us,draftkings
                  │   &markets=h2h,spreads,totals
                  ▼
   ┌────────────────────────────────────────────────┐
   │  bg_game_arb_pipeline DAG  (new, */10 * * * *) │
   │                                                │
   │  task: ingest_game_odds                        │
   │    -> bg_game_arb_stage_lines                  │
   │                                                │
   │  task: build_game_opportunities                │
   │    -> bg_arbitrage_game_opportunities          │
   │                                                │
   │  task: record_game_history                     │
   │    -> bg_arb_game_opportunities_history        │
   │                                                │
   │  task: alert_new_high_value                    │
   │    -> Discord (BG_DISCORD_WEBHOOK_URL)         │
   └────────────────────────────────────────────────┘
```

Runs in parallel to (a) `bg_arb_pipeline` (player props, Odds API per-event endpoint) and (b) `bg_unified_dag` (multi-source unified lines). Shares only the upstream API key — no shared tables, no shared library code that would couple the lifecycles. New code lives under `airflow/dags/bg_game_arb_pipeline_lib/` so it can be unit-tested independently.

## Schema — `bg_arbitrage_game_opportunities`

Mirrors the per-leg discipline of `bg_arbitrage_opportunities` but with game-line semantics. **Two legs** = the two sides of a 2-way market on different books.

```sql
CREATE TABLE bg_arbitrage_game_opportunities (
    opportunity_hash       text PRIMARY KEY,

    -- event context
    event_id               text         NOT NULL,
    sport_key              text         NOT NULL,  -- 'basketball_nba' (Odds API key)
    sport_title            text         NOT NULL,
    home_team              text         NOT NULL,
    away_team              text         NOT NULL,
    commence_time_utc      timestamp    NOT NULL,

    -- market shape
    market_key             text         NOT NULL CHECK (market_key IN ('h2h','spreads','totals')),

    -- leg A
    leg_a_book             text         NOT NULL,  -- 'fanduel'
    leg_a_outcome          text         NOT NULL,  -- team name (h2h/spreads), 'Over' (totals)
    leg_a_point            numeric      NULL,      -- NULL for h2h, signed spread or total for the others
    leg_a_price            numeric      NOT NULL,  -- decimal odds

    -- leg B (opposite side, different book)
    leg_b_book             text         NOT NULL,
    leg_b_outcome          text         NOT NULL,
    leg_b_point            numeric      NULL,
    leg_b_price            numeric      NOT NULL,

    -- arb economics
    wager_leg_a            numeric      NOT NULL,
    wager_leg_b            numeric      NOT NULL,
    payout                 numeric      NOT NULL,
    arb_ev                 numeric      NOT NULL,
    roi                    numeric      NOT NULL,

    -- freshness
    hours_until_commence   numeric      NOT NULL,
    fetched_at_utc         timestamp    NOT NULL
);

CREATE INDEX idx_game_opp_fetched  ON bg_arbitrage_game_opportunities (fetched_at_utc DESC);
CREATE INDEX idx_game_opp_roi      ON bg_arbitrage_game_opportunities (roi DESC);
CREATE INDEX idx_game_opp_market   ON bg_arbitrage_game_opportunities (market_key);
CREATE INDEX idx_game_opp_event    ON bg_arbitrage_game_opportunities (event_id);
```

`bg_game_arb_stage_lines` mirrors `bg_arb_stage_lines` but the row identity is `(event_id, market_key, book, outcome, point)` — replaced every run. `bg_arb_game_opportunities_history` mirrors the player-prop history table — append-only, same columns plus `first_seen_at_utc`.

### Why `outcome` + `point` instead of `under`/`over`
- **h2h**: `leg_a_outcome=Lakers, leg_b_outcome=Celtics, point=NULL`
- **spreads**: `leg_a_outcome=Lakers, leg_a_point=-7.5, leg_b_outcome=Celtics, leg_b_point=+7.5`
- **totals**: `leg_a_outcome=Over, leg_a_point=224.5, leg_b_outcome=Under, leg_b_point=224.5`

One schema handles all three market shapes cleanly. Builder pairs legs by `(event_id, market_key, point)` — for h2h `point` is `NULL` on both sides; for spreads the pairing requires `leg_a_point = -leg_b_point`; for totals the pairing requires `leg_a_point = leg_b_point`.

## Builder logic

Pure function `build_game_opportunities(lines_df, base_wager) -> DataFrame`:

```
1. Validate: each row has (event_id, market_key, book, outcome, point, price).
2. Group by (event_id, market_key) and pair within group:
   - For h2h: pair every (book_a, outcome_a, price_a) with every
     (book_b, outcome_b, price_b) where outcome_a != outcome_b and book_a != book_b.
   - For totals: same, but require point_a == point_b AND outcomes are {Over, Under}.
   - For spreads: pair where outcome_a is one team, outcome_b is the other team,
     AND point_a == -point_b AND book_a != book_b.
3. Compute arb economics (identical math to player-prop builder):
     implied_a = 1.0 / price_a
     implied_b = 1.0 / price_b
     overround = implied_a + implied_b
     if overround >= 1.0: skip
     else:
        wager_a   = base_wager * implied_a / overround
        wager_b   = base_wager * implied_b / overround
        payout    = base_wager / overround
        arb_ev    = payout - base_wager
        roi       = arb_ev / base_wager
4. Filter: roi > 0 AND hours_until_commence > 0.
5. Compute opportunity_hash:
     sha256("|".join([event_id, market_key,
                       leg_a_book, leg_a_outcome, f"{leg_a_point:.2f}", f"{leg_a_price:.4f}",
                       leg_b_book, leg_b_outcome, f"{leg_b_point:.2f}", f"{leg_b_price:.4f}"]))
6. Canonicalize leg ordering before hashing (alphabetical by book) so (A,B) and (B,A)
   yield the same hash — dedup safety.
7. Return DataFrame matching bg_arbitrage_game_opportunities columns.
```

## Discord alerter

New task `alert_new_high_value_game_opps` runs after `record_game_history`:

- Reads opportunities just appended to history (those new in this run).
- Filters: `roi >= ALERT_ROI_THRESHOLD` (env var, default `0.0075` to match player-prop threshold).
- For each, posts an embed to `BG_DISCORD_WEBHOOK_URL` with:
  - Title: `[{sport_title}] {away_team} @ {home_team} — {market_key.upper()} arb`
  - Field: leg A details (book, outcome, point, price, wager)
  - Field: leg B details
  - Field: ROI %, payout, hours-to-commence
  - Footer: per-book event URLs (FD/MGM stable landing URLs; DK/Caesars sport-page fallbacks)
- Uses the same `bountygate.alerts.discord` module the player-prop pipeline uses (already wired to `BG_DISCORD_WEBHOOK_URL`).

Deep-linking note: The Odds API doesn't return book-specific event IDs. v1 uses stable sport-landing URLs (`https://sportsbook.fanduel.com/navigation/{sport_slug}` etc.). A future enhancement could build a `{odds_api_event_id} -> {book_event_id}` mapping table, but it's not blocking for manual execution.

## File-level deliverables

```
bountygate/
├── airflow/
│   ├── dags/
│   │   ├── bg_game_arb_pipeline.py                  (NEW — mirrors bg_arb_pipeline.py)
│   │   └── bg_game_arb_pipeline_lib/                (NEW)
│   │       ├── __init__.py
│   │       ├── ingest.py                            (REST calls; mirrors bg_arb_pipeline_lib/ingest.py)
│   │       ├── builder.py                           (the pure function above)
│   │       ├── db.py                                (bulk_replace + bulk_append_new — copy or import shared)
│   │       └── alerts.py                            (Discord embed builder)
│   └── tests/
│       ├── test_game_arb_builder.py                 (NEW — fixture-driven unit tests)
│       └── fixtures/
│           ├── fixture_h2h_arb.json                 (NEW)
│           ├── fixture_spreads_arb.json             (NEW)
│           ├── fixture_totals_arb.json              (NEW)
│           └── fixture_no_arb.json                  (NEW)
├── db/migrations/
│   └── 006_bg_game_arb_pipeline_tables.sql          (NEW — three tables + indexes)
├── scripts/
│   └── dq_checks_game_arb.sql                       (NEW — Phase-1 shake-out queries)
└── docs/superpowers/specs/
    └── 2026-05-19-game-line-arb-pipeline-design.md  (NEW — copy of this design)
```

**Reused (do not modify):**
- `bountygate/utils/etl_assets.py` for `odds_apiKey`, `odds_url` constants.
- `bountygate/utils/db_connection.py` for `fetch_data` and any shared bulk write helpers.
- `bountygate/alerts/discord.py` (or wherever the player-prop alerter lives) — call its public function, do not fork.

## Verification

End-to-end shake-out plan:

1. **Migration** — run `006_bg_game_arb_pipeline_tables.sql` on dev DB, confirm tables exist.
2. **Builder unit tests** — `pytest airflow/tests/test_game_arb_builder.py -v`. Required cases:
   - `test_h2h_two_books_arb_emits_one_row`
   - `test_spreads_with_mirrored_points_arb_emits_one_row`
   - `test_spreads_with_different_points_emits_nothing`
   - `test_totals_with_matching_threshold_arb_emits_one_row`
   - `test_totals_with_different_threshold_emits_nothing`
   - `test_intra_book_pair_emits_nothing`
   - `test_overround_above_one_emits_nothing`
   - `test_opportunity_hash_invariant_under_leg_swap`
3. **Ingest dry-run** — `python -c "from airflow.dags.bg_game_arb_pipeline_lib.ingest import ingest_all; print(len(ingest_all(...)))"`. Confirm: rows include all 3 markets across the 4 books for at least one upcoming event.
4. **First DAG run** — trigger `bg_game_arb_pipeline` once via Airflow UI. Confirm: `bg_game_arb_stage_lines` populated, `bg_arbitrage_game_opportunities` populated or empty depending on real-world overround, `bg_arb_game_opportunities_history` appended.
5. **Spot-check arb math** — pick one row from the opportunities table, compute by hand: `(1/leg_a_price + 1/leg_b_price) < 1` and `roi = (payout - 100)/100`. Match the row's `roi` field to 4 decimal places.
6. **Discord smoke** — temporarily lower `ALERT_ROI_THRESHOLD` to `-1` to force at least one alert; verify the embed renders with correct fields. Restore threshold.
7. **48-hour soak** — let it run on the production cadence. Spot-check `scripts/dq_checks_game_arb.sql` for: row counts by market, ROI distribution, books represented in pairings, any nulls in non-null columns.

## Open questions for implementation

1. **Cadence** — match `bg_arb_pipeline`'s `*/10 * * * *` for v1; tune via Airflow UI based on observed arb-window survival times.
2. **`ALERT_ROI_THRESHOLD` default** — start at `0.0075` (matches player-prop threshold). Adjust based on first 48-hour signal-to-noise ratio.
3. **Spread-line pairing tolerance** — strict `leg_a_point == -leg_b_point` or allow `±0.5` slack? v1 = strict; revisit if the data shows that off-by-half-point spreads from different books are a real arb stream rather than noise.
4. **Caesars under `williamhill_us` vs. `caesars`** — confirm which Odds API book key the bountygate-tracked Caesars actually maps to before locking in the bookmakers list. v1 ships `williamhill_us`; flip at code-review if needed.
5. **NCAAB scope window** — pre-game only or include in-season tournament/March Madness? v1 = whatever Odds API exposes by default (pre-game only). Live/in-play is a separate effort.

## Out of scope follow-ups

- **Phase 2: executor integration** — promote FD×MGM game arbs to the existing bot via new game-market selector YAMLs. Triggered after ≥30 days of manual execution showing positive realized ROI on the alert feed.
- **Outrights / futures arb** — design pass on N-way arb math, capital allocation, and stake sizing for season-long holds.
- **Multi-region expansion** — `us2`/`uk`/`eu`/`au` detection-only, valuable if user adopts a VPN/multi-jurisdiction execution path.
- **`pp-odds-api sync` as resilience layer** — let the CLI maintain a local SQLite mirror that the pipeline falls back to when the live API has a hiccup.
- **Quota awareness** — move from a hardcoded API key in `etl_assets.py:11` to a credit-aware ingest that respects `X-Requests-Remaining` headers and degrades gracefully.
- **In-play / live game arbs** — separate cadence, separate DAG, separate alert lane.
