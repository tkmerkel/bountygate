# Cross-Venue Matching — `market_event_links` + `mart_cross_market_prices`

**Date:** 2026-06-10
**Status:** Design — approved, pending implementation plan
**Spec #4 of the downstream queue** (`docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`):
connectors ✅ → Postgres backend ✅ → transform pipeline ✅ → **cross-venue matching (this)** → frontend.

Directly continues the transform-pipeline spec
(`docs/superpowers/specs/2026-06-06-transform-pipeline-design.md`), which explicitly deferred
"cross-venue entity matching and `mart_cross_market_prices` to their own spec."

---

## 1. Context & goal

The transform pipeline fills the normalized layer and the two marts that need **no** cross-venue
matching (`mart_edge_signals`, `mart_market_history`). Two contract tables it left empty:

- `market_event_links` — the join between a `markets` row (Kalshi/Polymarket) and a `sports_events`
  row (The Odds API).
- `mart_cross_market_prices` — the same real-world outcome's probability quoted by all three venues
  side by side, with a disagreement metric.

The `/cross-market` API endpoint (`app/web/routers/cross_market.py`) reads `mart_cross_market_prices`
directly and currently returns `[]`. **After this spec it returns real rows.**

This spec builds the matching stage that links markets to games and the mart that compares prices
across venues.

### Locked decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Match scope | **Sports games only, 3-way.** `sports_events` (Odds API) is the spine; Kalshi + Polymarket sports markets attach to a game. Non-sports prediction markets are deferred. |
| Match method | **Deterministic, curated alias table + commence-time window.** Precision over recall — a wrong match compares two different games' probabilities and produces a garbage signal. No fuzzy/ML tier in v1. |
| Snapshot mode | **Latest snapshot, full-replace** each `build_marts` run (mirrors `mart_edge_signals`). The `*_history` tables retain the time-series for any future backfill. |
| League coverage | **NFL, NBA, MLB** — all three the connectors pull. NFL produces no matches out of season but the alias curation is done once. |
| Architecture | **Approach A:** pure matcher in `transforms/matching/`; version-controlled alias data; linking + the mart added as two tasks to the **existing `build_marts` DAG** (no new DAG, no new migration). |

### Environment facts
- New matching code is **pure / Airflow-free** and lives in
  `app/shared/python/bountygate/transforms/matching/`, beside `parsers/` and `marts/`. DAGs are thin
  orchestration (`link() >> cross_market()`), matching the existing `build_marts` shape.
- `market_event_links` and `mart_cross_market_prices` already exist (migrations `005_normalized.sql`,
  `006_marts.sql`). **No new migration is required** — runtime population only.
- The analytics lib stays untouched; we reuse its venue-agnostic primitives `devig.multiplicative_devig`
  and `consensus.no_vig_consensus`.
- Prior art to migrate: the Kalshi repo's `utils/team_names` and `utils/event_match` (frozen at
  `arb-execution-final`) seed the alias data and the Kalshi-ticker / team-name logic.

### Relevant existing schema (already migrated, not changed here)

```
markets(market_id uuid, venue_key, external_id, title, category, status,
        open_time, close_time, resolved_outcome, resolution_time, updated_at)   -- kalshi & polymarket populated
market_outcomes(outcome_id uuid, market_id, outcome_name, outcome_index, last_price, last_seen)
sports_events(event_id uuid, source_event_id, sport_key, commence_time, home_team, away_team)  -- odds populated
market_event_links(market_id, event_id, confidence numeric, method text, UNIQUE(market_id, event_id))  -- EMPTY
mart_cross_market_prices(question_key text, captured_at timestamptz, kalshi_prob, polymarket_prob,
                         sportsbook_consensus_prob, max_spread)                                  -- EMPTY
price_history                 -- Kalshi/PM outcome prices over time (partitioned)
sportsbook_odds_history       -- Odds API book lines over time (partitioned)
```

---

## 2. Deliverables

1. `transforms/matching/` package: alias data + pure matcher (`link_rows`) + a DB-facing
   `link_markets()`.
2. `transforms/marts/cross_market.py`: `build_cross_market()` (DB-facing) around a pure assemble
   function.
3. Two new tasks on the **existing `build_marts` DAG**: `link` (emits `Asset("market_event_links")`)
   and `cross_market` (emits `Asset("mart_cross_market_prices")`), wired `link() >> cross_market()`.
4. Unit tests (fixture-only, no DB/network) for aliases, event-key extraction, matching, and the mart
   assemble; an extended DAG-import smoke test; an end-to-end verification.

**Explicitly out of scope:** non-sports prediction-market matching (Kalshi↔Polymarket on
elections/econ/weather); any fuzzy/ML matching tier; the frontend; backfilling mart history beyond the
current snapshot; any write/execution path. No schema migration.

---

## 3. Architecture & DAG chain

The matching stage sits between the (existing) normalize DAG and the API. Nothing upstream changes.

```
normalize DAG  ──emits──▶  markets (kalshi, polymarket)   sports_events (odds)
                                        │
                build_marts DAG         ▼
                ┌──────────────────────────────────────────────────┐
                │ link_markets   ──▶ market_event_links              │  (NEW)
                │      │              Asset("market_event_links")     │
                │      ▼                                              │
                │ cross_market   ──▶ mart_cross_market_prices         │  (NEW)
                │                     Asset("mart_cross_market_prices")│
                │ edges   (existing, unchanged, parallel)             │
                │ history (existing, unchanged, parallel)             │
                └──────────────────────────────────────────────────┘
                                        │
                          /cross-market API  ──▶ real rows
```

- The spine is `sports_events` (Odds API): the sportsbook *is* the game; prediction markets attach to
  it.
- `link()` and `cross_market()` run in the same `build_marts` run; `cross_market` depends on the fresh
  links. `edges()`/`history()` stay independent and parallel.
- Pure-core / thin-DAG split, matching `transforms/marts/edge_signals.py`: a `build_X()` /
  `link_markets()` function owns DB I/O and calls pure helpers that take/return frames.

### Module layout

```
transforms/matching/__init__.py
transforms/matching/aliases/nfl.json · nba.json · mlb.json   # canonical id → spellings
transforms/matching/aliases.py        # load_aliases(); canonical_team(name, sport) -> id | None
transforms/matching/event_key.py      # parse_kalshi_ticker(); teams_from_text(); within_window()
transforms/matching/match.py          # link_rows(markets_df, events_df) -> list[link]   (PURE)
transforms/matching/link.py           # link_markets(): read frames → link_rows → upsert links (DB I/O)
transforms/marts/cross_market.py      # build_cross_market(): read links+latest history → assemble → replace mart (DB I/O)
```

---

## 4. Matching logic (raw → `market_event_links`)

Deterministic, alias-driven, precision-first. `link_rows(markets_df, events_df)` is pure: it takes the
normalized `markets` (+ `market_outcomes`) and `sports_events` frames and returns link rows. No DB, no
network.

### 4a. Canonical team identity

One alias file per league (`matching/aliases/{nfl,nba,mlb}.json`), each mapping a canonical id to all
spellings seen across venues:

```json
{
  "DAL": ["Dallas Cowboys", "Cowboys", "Dallas", "DAL"],
  "NYG": ["New York Giants", "Giants", "NYG"]
}
```

`canonical_team(name_or_token, sport) -> canonical_id | None` resolves any venue's spelling to the
canonical id (case-insensitive, exact alias membership). A miss returns `None` (and the market goes
unmatched — see §4d). Seeded by migrating the Kalshi repo's `team_names`/`event_match` utils.

### 4b. Per-venue team & date extraction (all deterministic — exact alias hits only)

| Venue | Sport | Teams | Date |
|---|---|---|---|
| **Odds API** (spine) | `sport_key` | `home_team` / `away_team` → canonical via full-name alias | `commence_time` |
| **Kalshi** | `series_ticker` (`KXNFLGAME`→NFL, `KXNBAGAME`→NBA, `KXMLBGAME`→MLB) | market ticker team suffix (`…-DAL`) → canonical; one **side** per market ("Will DAL win?") | `event_ticker` embedded date / `close_time` |
| **Polymarket** | inferred from which league's aliases the question text hits | scan `question` text for known alias substrings; accept only if **exactly two distinct canonical teams of the same league** appear, else **no match** | `end_date` / `close_time` |

`parse_kalshi_ticker(ticker, event_ticker, series_ticker)` returns `(sport, team_id, opponent_id?,
date?)`. `teams_from_text(question)` returns the set of canonical `(sport, team_id)` hits; it is
accepted only when the set is exactly two same-league teams. The Polymarket question's **subject team**
(the one the "Yes" outcome refers to) is identified so the mart can align Yes/No to sides (§5b).

### 4c. Matching rule

A Kalshi or Polymarket market links to a `sports_event` when **both** hold:
1. The market's two canonical teams equal the event's two canonical teams (order-independent), in the
   same sport.
2. The market's time (`close_time` / `end_date`) falls within a **±36-hour window** of the event's
   `commence_time`. A window, not exact-date equality — a 00:40 UTC game is the prior US evening, so a
   naive date compare would miss.

A Kalshi market carries only one team in its ticker suffix; its opponent is taken from the
`event_ticker` pairing (e.g. `…-DALNYG` → {DAL, NYG}) so it still presents two teams for rule 1.

`market_event_links` is written with:
- `confidence` = `1.0` for these exact matches (the column is retained for a future fuzzy tier).
- `method` = `'kalshi_ticker'` or `'polymarket_text'`.

### 4d. Precision guards & non-matches

- **Ambiguous** — a market matching **more than one** in-window event ⇒ **skipped and counted**, never
  guessed.
- **Unmatched** — non-sports markets, unknown teams, or `< 2` resolvable teams ⇒ unmatched; expected
  (the long tail), not an error.
- **Match-rate logging** — `link_markets()` logs a per-source breakdown each run, e.g.
  `linked kalshi=38/120 polymarket=12/90 ambiguous=3`. A missing/broken alias craters the rate; the
  log surfaces it instead of silently masking the miss. (Project ethos: a miss is a regression to
  investigate, never reframed as benign.)

### 4e. Write strategy

`link_markets()` re-derives links each run and **upserts** on the existing `UNIQUE(market_id,
event_id)` — one current link per market/event pair, refreshed every run. Because `cross_market`
full-replaces (§5c), links that no longer resolve simply stop feeding the mart.

---

## 5. The cross-market mart (`market_event_links` + latest history → `mart_cross_market_prices`)

`build_cross_market()` reads the fresh links plus the latest snapshot from the history tables, calls a
pure assemble function, and full-replaces the mart.

### 5a. Grain & key

**One row per (game, winning side).** An h2h game yields up to two rows — "home wins" and "away wins" —
matching how the three venues quote probability and keeping each API row self-describing.

`question_key` is human-readable, stable, and sortable:

```
{sport}:{YYYY-MM-DD}:{away}@{home}:{team}        e.g.  nba:2026-06-09:NYK@BOS:BOS
```

- date from the event's `commence_time` (UTC date);
- `away@home` from the event's canonical teams;
- `team` = the canonical side this row's probabilities refer to.

### 5b. Probability alignment — every column is **P(this side wins)**

| Column | Source | Rule |
|---|---|---|
| `sportsbook_consensus_prob` | `sportsbook_odds_history`, latest `captured_at` per book, `market_type='h2h'` | Devig each book's two-way decimal line (`devig.multiplicative_devig`) and take `consensus.no_vig_consensus` across books → fair P(side). Both sides come from one consensus (they sum to 1). |
| `kalshi_prob` | linked Kalshi market's `Yes` outcome (`market_outcomes.last_price`, the bid/ask mid) | A Kalshi market is per-team and fills **its own side**; the opposite side is filled from the other team's Kalshi market if one is linked, else `NULL`. |
| `polymarket_prob` | linked Polymarket market's `outcome_prices` | One 2-outcome market fills **both sides**: subject team = `Yes` price; opponent = `No` price (= 1 − Yes). |

The Odds-API outcome `name` is mapped to a canonical side via the alias table so the right book price
lands on the right row.

`max_spread` = `max(non-null probs) − min(non-null probs)` for the row — the headline cross-venue
disagreement metric.

### 5c. Emission gate & refresh

- **≥2-venue gate:** a side is emitted only if **at least two of the three** probs are non-null. A
  one-venue side is not a cross-market comparison; it is dropped and counted. In practice
  `sportsbook_consensus_prob` is the spine and almost always present, so a row needs the sportsbook
  plus ≥1 prediction market.
- **Refresh = full replace:** each `build_marts` run truncates + reinserts the current snapshot;
  `captured_at` = run time. Mirrors `mart_edge_signals`. Dropped/stale games do not linger.

---

## 6. DAG wiring

`build_marts.py` gains two tasks; the existing `edges()`/`history()` are untouched.

```python
@task(outlets=[Asset(name="market_event_links")])
def link() -> dict:
    stats = link_markets()                 # {'kalshi': n, 'polymarket': n, 'ambiguous': n}
    print(f"[build_marts] links {stats}")
    return stats

@task(outlets=[Asset(name="mart_cross_market_prices")])
def cross_market() -> int:
    n = build_cross_market()
    print(f"[build_marts] cross_market rows={n}")
    return n

link() >> cross_market()                   # cross_market consumes the fresh links
edges()                                     # unchanged, parallel
history()                                   # unchanged, parallel
```

No new DAG. No migration. Assets referenced by **name** (matching the rest of the pipeline — a
`postgres://` URI trips Airflow's asset-URI normalizer).

---

## 7. Error handling & edge cases

| Case | Handling |
|---|---|
| Unmatched market (non-sports / unknown team) | Skipped, counted; expected, not an error. |
| Ambiguous (>1 in-window event) | Skipped, counted separately; never guessed. |
| Partial coverage (`<2` venues on a side) | Row dropped (gate); `≥2` ⇒ emitted with `NULL`s allowed. |
| Sport out of season (NFL in June) | 0 Kalshi/PM sports markets ⇒ 0 links ⇒ no rows; a clean empty run is success. |
| Stale links | `link_markets()` re-derives + upserts each run; `cross_market` full-replaces. |
| Missing alias | Affected markets go unmatched; the logged match-rate surfaces the drop. |
| Book with a one-sided / missing line | Excluded from that event's consensus; if no usable two-way book remains, `sportsbook_consensus_prob` is `NULL` and the ≥2-venue gate applies. |

---

## 8. Testing

Pure modules are fixture-only — **no DB, no network.**

1. `test_aliases.py` — canonical resolution across venue spellings; miss → `None`.
2. `test_event_key.py` — Kalshi ticker parse; Polymarket two-team text extraction (incl. reject when
   ≠ 2 same-league teams); `within_window(±36h)` boundaries.
3. `test_match.py` — frames → link rows: clean 3-venue match; ambiguous rejection; out-of-window
   rejection; non-sports skip; correct `confidence`/`method`.
4. `test_cross_market.py` — frames → mart rows: P(side) alignment per venue; Yes/No→side mapping for
   Polymarket; `max_spread`; ≥2-venue gate; two-rows-per-game.
5. Extend the existing DAG-import smoke test to cover the updated `build_marts`.
6. **End-to-end verification** — run `build_marts` in the container; assert `mart_cross_market_prices`
   has rows for in-season leagues (MLB/NBA in June) and that `/cross-market` returns them.

**Test command:** `cd app/shared/python && python -m pytest tests -v` (and the transforms package tests
under `bountygate/transforms/tests/`).

---

## 9. Success criteria

- `transforms/matching/` exists: alias data for NFL/NBA/MLB, a pure `link_rows`, and a DB-facing
  `link_markets()` with match-rate logging.
- `transforms/marts/cross_market.py` assembles per-(game, side) rows with correctly aligned
  per-venue probabilities, `max_spread`, and the ≥2-venue gate.
- `build_marts` runs `link() >> cross_market()` alongside the existing tasks and emits the two new
  Assets.
- All matching/mart unit tests green on fixtures; the DAG imports.
- After a live `build_marts` run, `mart_cross_market_prices` is populated for in-season leagues and
  `GET /cross-market` returns real rows.
- No wrong matches: ambiguous and out-of-window candidates are rejected, not guessed.
