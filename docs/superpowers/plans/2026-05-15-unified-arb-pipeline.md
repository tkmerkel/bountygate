# Unified Arb Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two existing arb tables and the asymmetric alt-only DAG with one self-contained pipeline that covers all 4 std/alt pairing directions and gives the bot an unambiguous per-leg execution contract.

**Architecture:** New Airflow DAG `bg_arb_pipeline` (own ingest from the-odds-api → stage table → symmetric cartesian builder → unified `bg_arbitrage_opportunities`). Bot reads single table with explicit `under_market_key` / `over_market_key` columns. Phased migration: parallel-write, switch reads, retire old DAG.

**Tech Stack:** Python 3.11+, Airflow 3, SQLAlchemy 2.0, Pandas, requests, the-odds-api v4, Postgres, pytest.

**Spec:** `docs/superpowers/specs/2026-05-15-unified-arb-pipeline-design.md`

**Note on hash precision:** The spec proposed `.2f` for lines and `.4f` for prices. The existing arb DAG's `_build_opportunity_key` uses `.3f` and `.6f`. This plan uses the existing convention (`.3f` / `.6f`) for consistency with `bg_executed_opportunities` keying. If anything in `bg_executed_opportunities` depends on the existing precision (it does — historical rows were keyed that way), changing it would invalidate dedup history.

---

## File Structure

**New files:**

| Path | Responsibility |
|------|----------------|
| `db/migrations/005_bg_arb_pipeline_tables.sql` | Create `bg_arb_stage_lines`, `bg_arbitrage_opportunities`, `bg_arb_opportunities_history` with indexes |
| `db/migrations/006_bg_executed_opportunities_market_keys.sql` | Add `under_market_key`, `over_market_key` columns to `bg_executed_opportunities`; backfill from existing `market_key` column |
| `airflow/dags/bg_arb_pipeline.py` | Airflow DAG file: schedule, task wiring, no business logic |
| `airflow/dags/bg_arb_pipeline_lib/__init__.py` | Package marker |
| `airflow/dags/bg_arb_pipeline_lib/markets.py` | `ARB_SPORTS`, `ARB_MARKETS` constants |
| `airflow/dags/bg_arb_pipeline_lib/ingest.py` | the-odds-api HTTP + JSON → stage rows DataFrame |
| `airflow/dags/bg_arb_pipeline_lib/builder.py` | Pure function: stage rows DataFrame → opportunities DataFrame |
| `airflow/dags/bg_arb_pipeline_lib/hashing.py` | `opportunity_hash`, `derive_pairing_type` |
| `airflow/tests/__init__.py` | Empty package marker |
| `airflow/tests/conftest.py` | Add `airflow/dags/` to `sys.path` for tests |
| `airflow/tests/test_arb_hashing.py` | Unit tests for `opportunity_hash` + `derive_pairing_type` |
| `airflow/tests/test_arb_builder.py` | Unit tests for `build_opportunities` (11 cases per spec) |
| `airflow/tests/test_arb_ingest.py` | Fixture tests for `normalize_odds_response` |
| `airflow/tests/fixtures/the_odds_api_std_only.json` | Sample the-odds-api response: std markets only |
| `airflow/tests/fixtures/the_odds_api_alt_rich.json` | Sample: std + `_alternate` markets |
| `airflow/tests/fixtures/the_odds_api_fanduel_alt_under.json` | Sample: FanDuel alt-under |
| `scripts/dq_checks_arb_parity.py` | Phase 1 parity check (CLI: runs the 3 SQL queries from the spec, prints summaries) |

**Modified files:**

| Path | Change |
|------|--------|
| `arbitrage_executor/opportunity.py` | Rewrite the two-table query into one `bg_arbitrage_opportunities` query |
| `arbitrage_executor/map_selectors.py` | Replace `fetch_opportunity_for_market` body with single-table query (drops the `_alt` fallback we patched earlier today) |
| `arbitrage_executor/bet_placer.py` | Per-leg market_key reads: callers pass leg-specific `market_key` instead of opportunity-level |
| `arbitrage_executor/execute_arb.py` | Per-leg market_key resolution: under leg uses `opp['under_market_key']`, over leg uses `opp['over_market_key']` |

**Deleted (Phase 3):**

| Path | Reason |
|------|--------|
| `airflow/dags/bg_arbitrage_player_props.py` | Replaced by `bg_arb_pipeline.py` |

---

## Phase 1 — Analytics (build the new pipeline, no behavior change)

### Task 1: Create the new tables (migration 005)

**Files:**
- Create: `db/migrations/005_bg_arb_pipeline_tables.sql`

- [ ] **Step 1: Write the migration SQL**

Create `db/migrations/005_bg_arb_pipeline_tables.sql`:

```sql
-- Stage table: raw line-level rows from the-odds-api after JSON normalization.
-- One row per (book, market_key, line, side). Replaced every ingest run.
CREATE TABLE IF NOT EXISTS bg_arb_stage_lines (
    event_id              text         NOT NULL,
    sport_title           text         NOT NULL,
    home_team             text         NOT NULL,
    away_team             text         NOT NULL,
    commence_time_utc     timestamp    NOT NULL,
    player_name           text         NOT NULL,
    bookmaker_key         text         NOT NULL,
    market_key            text         NOT NULL,
    line                  numeric      NOT NULL,
    side                  text         NOT NULL CHECK (side IN ('under', 'over')),
    price                 numeric      NOT NULL,
    fetched_at_utc        timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_arb_stage_event
    ON bg_arb_stage_lines (event_id, player_name);
CREATE INDEX IF NOT EXISTS idx_arb_stage_market
    ON bg_arb_stage_lines (market_key);

-- Current arb-able opportunities: what the bot reads.
CREATE TABLE IF NOT EXISTS bg_arbitrage_opportunities (
    opportunity_hash       text PRIMARY KEY,
    event_id               text         NOT NULL,
    sport_title            text         NOT NULL,
    home_team              text         NOT NULL,
    away_team              text         NOT NULL,
    player_name            text         NOT NULL,
    canonical_market       text         NOT NULL,
    pairing_type           text         NOT NULL CHECK (pairing_type IN ('std_std','std_alt','alt_std','alt_alt')),
    under_book             text         NOT NULL,
    under_market_key       text         NOT NULL,
    under_line             numeric      NOT NULL,
    under_price            numeric      NOT NULL,
    over_book              text         NOT NULL,
    over_market_key        text         NOT NULL,
    over_line              numeric      NOT NULL,
    over_price             numeric      NOT NULL,
    wager_under            numeric      NOT NULL,
    wager_over             numeric      NOT NULL,
    payout                 numeric      NOT NULL,
    arb_ev                 numeric      NOT NULL,
    roi                    numeric      NOT NULL,
    hours_until_commence   numeric      NOT NULL,
    fetched_at_utc         timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_arb_opp_fetched
    ON bg_arbitrage_opportunities (fetched_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_arb_opp_roi
    ON bg_arbitrage_opportunities (roi DESC);
CREATE INDEX IF NOT EXISTS idx_arb_opp_pairing
    ON bg_arbitrage_opportunities (pairing_type);
CREATE INDEX IF NOT EXISTS idx_arb_opp_player
    ON bg_arbitrage_opportunities (player_name);

-- Append-only history of every opportunity ever produced. For analysis only.
-- Same shape as bg_arbitrage_opportunities; ON CONFLICT DO NOTHING on hash.
CREATE TABLE IF NOT EXISTS bg_arb_opportunities_history (
    opportunity_hash       text PRIMARY KEY,
    event_id               text         NOT NULL,
    sport_title            text         NOT NULL,
    home_team              text         NOT NULL,
    away_team              text         NOT NULL,
    player_name            text         NOT NULL,
    canonical_market       text         NOT NULL,
    pairing_type           text         NOT NULL,
    under_book             text         NOT NULL,
    under_market_key       text         NOT NULL,
    under_line             numeric      NOT NULL,
    under_price            numeric      NOT NULL,
    over_book              text         NOT NULL,
    over_market_key        text         NOT NULL,
    over_line              numeric      NOT NULL,
    over_price             numeric      NOT NULL,
    wager_under            numeric      NOT NULL,
    wager_over             numeric      NOT NULL,
    payout                 numeric      NOT NULL,
    arb_ev                 numeric      NOT NULL,
    roi                    numeric      NOT NULL,
    hours_until_commence   numeric      NOT NULL,
    fetched_at_utc         timestamp    NOT NULL,
    first_seen_at_utc      timestamp    NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX IF NOT EXISTS idx_arb_history_pairing_fetched
    ON bg_arb_opportunities_history (pairing_type, fetched_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_arb_history_player_market
    ON bg_arb_opportunities_history (player_name, canonical_market);
```

- [ ] **Step 2: Apply the migration locally**

Run:
```powershell
python scripts/migrate.py up
```

Expected output (last lines):
```
Applied: 005_bg_arb_pipeline_tables.sql
```

- [ ] **Step 3: Verify tables exist**

Run:
```powershell
python -c "from bountygate.utils.db_connection import fetch_data; print(fetch_data(\"SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'bg_arb%'\"))"
```

Expected: three table names in the output (bg_arb_stage_lines, bg_arbitrage_opportunities, bg_arb_opportunities_history).

- [ ] **Step 4: Commit**

```powershell
git add db/migrations/005_bg_arb_pipeline_tables.sql
git commit -m "db: migration 005 — create bg_arb_pipeline tables"
```

---

### Task 2: Hashing + pairing_type — TDD

**Files:**
- Create: `airflow/dags/bg_arb_pipeline_lib/__init__.py`
- Create: `airflow/dags/bg_arb_pipeline_lib/hashing.py`
- Create: `airflow/tests/__init__.py`
- Create: `airflow/tests/conftest.py`
- Create: `airflow/tests/test_arb_hashing.py`

- [ ] **Step 1: Create the package marker files**

Create `airflow/dags/bg_arb_pipeline_lib/__init__.py` (empty).
Create `airflow/tests/__init__.py` (empty).

- [ ] **Step 2: Create the test conftest**

Create `airflow/tests/conftest.py`:

```python
import sys
from pathlib import Path

# Tests live in airflow/tests/; modules live in airflow/dags/bg_arb_pipeline_lib/.
# Inject airflow/dags onto sys.path so tests can `from bg_arb_pipeline_lib.X import Y`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
```

- [ ] **Step 3: Write the failing tests**

Create `airflow/tests/test_arb_hashing.py`:

```python
from bg_arb_pipeline_lib.hashing import opportunity_hash, derive_pairing_type


def _sample_row(**overrides):
    base = {
        "event_id": "evt_abc",
        "player_name": "Dennis Schroder",
        "under_book": "betmgm",
        "under_market_key": "player_assists",
        "over_book": "fanduel",
        "over_market_key": "player_assists_alternate",
        "under_line": 2.5,
        "under_price": 1.90,
        "over_line": 2.5,
        "over_price": 1.85,
    }
    base.update(overrides)
    return base


class TestOpportunityHash:
    def test_stable_across_runs(self):
        row = _sample_row()
        assert opportunity_hash(row) == opportunity_hash(row)

    def test_differs_when_price_changes(self):
        a = _sample_row(under_price=1.90)
        b = _sample_row(under_price=1.91)
        assert opportunity_hash(a) != opportunity_hash(b)

    def test_differs_when_book_changes(self):
        a = _sample_row(under_book="betmgm")
        b = _sample_row(under_book="fanduel")
        assert opportunity_hash(a) != opportunity_hash(b)

    def test_rounding_stable_at_third_decimal_for_line(self):
        # Line precision is .3f — beyond that, identical rows hash identically.
        a = _sample_row(under_line=2.5)
        b = _sample_row(under_line=2.5000001)
        assert opportunity_hash(a) == opportunity_hash(b)

    def test_rounding_stable_at_sixth_decimal_for_price(self):
        # Price precision is .6f.
        a = _sample_row(under_price=1.90)
        b = _sample_row(under_price=1.9000001)
        assert opportunity_hash(a) == opportunity_hash(b)

    def test_returns_sha256_hex(self):
        h = opportunity_hash(_sample_row())
        assert len(h) == 64
        int(h, 16)  # valid hex


class TestDerivePairingType:
    def test_std_std_when_neither_ends_in_alternate(self):
        assert derive_pairing_type("player_assists", "player_assists") == "std_std"

    def test_std_alt_when_only_over_is_alternate(self):
        assert derive_pairing_type("player_assists", "player_assists_alternate") == "std_alt"

    def test_alt_std_when_only_under_is_alternate(self):
        assert derive_pairing_type("player_assists_alternate", "player_assists") == "alt_std"

    def test_alt_alt_when_both_are_alternate(self):
        assert derive_pairing_type(
            "player_assists_alternate", "player_assists_alternate"
        ) == "alt_alt"

    def test_substring_match_does_not_count(self):
        # A market named "alternate_player_assists" should NOT count as alt.
        # Only the _alternate suffix matters.
        assert derive_pairing_type("alternate_player_assists", "player_assists") == "std_std"
```

- [ ] **Step 4: Run tests to verify they fail**

Run:
```powershell
cd C:\Users\tkmer\bountygate
.\arbitrage_executor\.venv\Scripts\python.exe -m pytest airflow/tests/test_arb_hashing.py -v
```

Expected: ImportError or `ModuleNotFoundError: No module named 'bg_arb_pipeline_lib.hashing'` — that's the failing-state we want.

- [ ] **Step 5: Write the minimal implementation**

Create `airflow/dags/bg_arb_pipeline_lib/hashing.py`:

```python
"""Hash + pairing-type derivation for the arb pipeline.

Precision matches the existing bg_arbitrage_player_props._build_opportunity_key:
  - line:  .3f
  - price: .6f
Changing this would invalidate dedup against bg_executed_opportunities history.
"""
from __future__ import annotations

import hashlib
from typing import Any, Mapping


def opportunity_hash(row: Mapping[str, Any]) -> str:
    """Stable SHA-256 hash identifying an opportunity at a point in time.

    Same input → same hex string. Includes prices so a price shift produces
    a new hash (and the old hash ages out of bg_arbitrage_opportunities).
    """
    parts = (
        str(row["event_id"]),
        str(row["player_name"]),
        str(row["under_book"]),
        str(row["under_market_key"]),
        str(row["over_book"]),
        str(row["over_market_key"]),
        f"{float(row['under_line']):.3f}",
        f"{float(row['under_price']):.6f}",
        f"{float(row['over_price']):.6f}",
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def derive_pairing_type(under_market_key: str, over_market_key: str) -> str:
    """Classify a pair by which leg(s) are alternate-line variants.

    Returns one of 'std_std', 'std_alt', 'alt_std', 'alt_alt'.
    Only the literal `_alternate` suffix counts — substring matches don't.
    """
    under_is_alt = under_market_key.endswith("_alternate")
    over_is_alt = over_market_key.endswith("_alternate")
    if not under_is_alt and not over_is_alt:
        return "std_std"
    if not under_is_alt and over_is_alt:
        return "std_alt"
    if under_is_alt and not over_is_alt:
        return "alt_std"
    return "alt_alt"
```

- [ ] **Step 6: Run tests to verify they pass**

Run:
```powershell
.\arbitrage_executor\.venv\Scripts\python.exe -m pytest airflow/tests/test_arb_hashing.py -v
```

Expected: `13 passed`.

- [ ] **Step 7: Commit**

```powershell
git add airflow/dags/bg_arb_pipeline_lib/__init__.py airflow/dags/bg_arb_pipeline_lib/hashing.py airflow/tests/__init__.py airflow/tests/conftest.py airflow/tests/test_arb_hashing.py
git commit -m "arb-pipeline: opportunity_hash + derive_pairing_type with tests"
```

---

### Task 3: Markets + sports constants

**Files:**
- Create: `airflow/dags/bg_arb_pipeline_lib/markets.py`

- [ ] **Step 1: Write the markets constants**

Create `airflow/dags/bg_arb_pipeline_lib/markets.py`:

```python
"""Sports and market_keys the arb pipeline ingests from the-odds-api.

Both standard and `_alternate` variants are listed explicitly. Future iteration:
move to YAML or a DB-driven config so updates don't require a code deploy.

Reference: https://the-odds-api.com/sports-odds-data/betting-markets.html
"""
from __future__ import annotations

ARB_SPORTS: tuple[str, ...] = (
    "basketball_nba",
    "icehockey_nhl",
    "americanfootball_nfl",
    "baseball_mlb",
)

# Sport-key -> sport_title used in DB rows. The-odds-api uses sport keys like
# "basketball_nba"; downstream tables use the friendlier title.
SPORT_TITLE: dict[str, str] = {
    "basketball_nba": "NBA",
    "icehockey_nhl": "NHL",
    "americanfootball_nfl": "NFL",
    "baseball_mlb": "MLB",
}

ARB_BOOKMAKERS: tuple[str, ...] = ("fanduel", "betmgm")

# Player-prop markets we want for arb. Add to this list as books expose new ones.
# Maintained from MAP_MISSING_MARKETS-2026-05-14.md and current selectors.
ARB_MARKETS: tuple[str, ...] = (
    # NBA / basketball
    "player_points", "player_points_alternate",
    "player_assists", "player_assists_alternate",
    "player_rebounds", "player_rebounds_alternate",
    "player_threes", "player_threes_alternate",
    "player_blocks", "player_blocks_alternate",
    "player_steals", "player_steals_alternate",
    "player_points_rebounds", "player_points_rebounds_alternate",
    "player_points_assists", "player_points_assists_alternate",
    "player_points_rebounds_assists", "player_points_rebounds_assists_alternate",
    "player_points_q1",
    # NHL
    "player_shots_on_goal", "player_shots_on_goal_alternate",
    "player_total_saves",
    # MLB
    "batter_singles", "batter_singles_alternate",
    "batter_doubles", "batter_doubles_alternate",
    "batter_stolen_bases", "batter_stolen_bases_alternate",
    # NFL (placeholders — add as needed)
)

# Markets we never bet (debug-only). Mirror bg_arbitrage_player_props.MARKET_BLACKLIST.
ARB_MARKET_BLACKLIST: frozenset[str] = frozenset({
    "pitcher_strikeouts",
    "pitcher_strikeouts_alternate",
})
```

- [ ] **Step 2: Commit**

```powershell
git add airflow/dags/bg_arb_pipeline_lib/markets.py
git commit -m "arb-pipeline: ARB_SPORTS + ARB_MARKETS constants"
```

---

### Task 4: Ingest task — TDD with fixtures

**Files:**
- Create: `airflow/tests/fixtures/the_odds_api_std_only.json`
- Create: `airflow/tests/fixtures/the_odds_api_alt_rich.json`
- Create: `airflow/tests/test_arb_ingest.py`
- Create: `airflow/dags/bg_arb_pipeline_lib/ingest.py`

- [ ] **Step 1: Capture a real fixture from the-odds-api**

Use the user's API key (from `.env`) to fetch one sample response. Save it as `airflow/tests/fixtures/the_odds_api_std_only.json`:

```powershell
$apiKey = (Get-Content .env | Where-Object { $_ -match '^ODDS_API_KEY=' }) -replace 'ODDS_API_KEY=', ''
# Pick any upcoming NBA event id from /v4/sports/basketball_nba/events?apiKey=...
# Then fetch its odds:
$eventId = "<paste-real-event-id-from-events-endpoint>"
$url = "https://api.the-odds-api.com/v4/sports/basketball_nba/events/$eventId/odds?apiKey=$apiKey&regions=us&bookmakers=fanduel,betmgm&markets=player_assists"
Invoke-RestMethod -Uri $url -OutFile airflow\tests\fixtures\the_odds_api_std_only.json
```

If you don't want to spend a call, hand-craft a minimal fixture matching the schema documented at <https://the-odds-api.com/liveapi/guides/v4/#get-event-odds>:

```json
{
  "id": "evt_test_001",
  "sport_key": "basketball_nba",
  "sport_title": "NBA",
  "commence_time": "2026-05-15T22:00:00Z",
  "home_team": "Cleveland Cavaliers",
  "away_team": "Detroit Pistons",
  "bookmakers": [
    {
      "key": "fanduel",
      "markets": [
        {
          "key": "player_assists",
          "outcomes": [
            {"name": "Over",  "description": "Dennis Schroder", "price": 1.85, "point": 2.5},
            {"name": "Under", "description": "Dennis Schroder", "price": 2.00, "point": 2.5}
          ]
        }
      ]
    },
    {
      "key": "betmgm",
      "markets": [
        {
          "key": "player_assists",
          "outcomes": [
            {"name": "Over",  "description": "Dennis Schroder", "price": 1.90, "point": 2.5},
            {"name": "Under", "description": "Dennis Schroder", "price": 1.95, "point": 2.5}
          ]
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Create the alt-rich fixture**

Save as `airflow/tests/fixtures/the_odds_api_alt_rich.json`:

```json
{
  "id": "evt_test_002",
  "sport_key": "basketball_nba",
  "sport_title": "NBA",
  "commence_time": "2026-05-15T22:00:00Z",
  "home_team": "Cleveland Cavaliers",
  "away_team": "Detroit Pistons",
  "bookmakers": [
    {
      "key": "fanduel",
      "markets": [
        {
          "key": "player_assists",
          "outcomes": [
            {"name": "Over",  "description": "Dennis Schroder", "price": 1.85, "point": 2.5},
            {"name": "Under", "description": "Dennis Schroder", "price": 2.00, "point": 2.5}
          ]
        },
        {
          "key": "player_assists_alternate",
          "outcomes": [
            {"name": "Over",  "description": "Dennis Schroder", "price": 1.65, "point": 2.5},
            {"name": "Over",  "description": "Dennis Schroder", "price": 2.40, "point": 3.5},
            {"name": "Under", "description": "Dennis Schroder", "price": 2.20, "point": 2.5},
            {"name": "Under", "description": "Dennis Schroder", "price": 1.50, "point": 3.5}
          ]
        }
      ]
    },
    {
      "key": "betmgm",
      "markets": [
        {
          "key": "player_assists",
          "outcomes": [
            {"name": "Over",  "description": "Dennis Schroder", "price": 1.90, "point": 2.5},
            {"name": "Under", "description": "Dennis Schroder", "price": 1.95, "point": 2.5}
          ]
        },
        {
          "key": "player_assists_alternate",
          "outcomes": [
            {"name": "Over",  "description": "Dennis Schroder", "price": 1.75, "point": 2.5}
          ]
        }
      ]
    }
  ]
}
```

- [ ] **Step 3: Write the failing tests**

Create `airflow/tests/test_arb_ingest.py`:

```python
import json
from datetime import datetime, timezone
from pathlib import Path

from bg_arb_pipeline_lib.ingest import normalize_odds_response

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class TestNormalizeOddsResponse:
    def test_std_only_produces_four_rows(self):
        # 1 event × 2 books × 1 market × 2 sides = 4 rows
        rows = normalize_odds_response(
            _load("the_odds_api_std_only.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        assert len(rows) == 4

    def test_std_only_row_shape(self):
        rows = normalize_odds_response(
            _load("the_odds_api_std_only.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        # Find FanDuel under
        fd_under = next(
            r for r in rows
            if r["bookmaker_key"] == "fanduel" and r["side"] == "under"
        )
        assert fd_under["event_id"] == "evt_test_001"
        assert fd_under["sport_title"] == "NBA"
        assert fd_under["player_name"] == "Dennis Schroder"
        assert fd_under["market_key"] == "player_assists"
        assert fd_under["line"] == 2.5
        assert fd_under["price"] == 2.00
        assert fd_under["home_team"] == "Cleveland Cavaliers"
        assert fd_under["away_team"] == "Detroit Pistons"

    def test_side_is_lowercase(self):
        rows = normalize_odds_response(
            _load("the_odds_api_std_only.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        for row in rows:
            assert row["side"] in ("under", "over")

    def test_alt_rich_produces_nine_rows(self):
        # 1 event × books:
        #   FanDuel: 2 std outcomes + 4 alt outcomes = 6
        #   BetMGM:  2 std outcomes + 1 alt outcome  = 3
        # Total = 9.
        rows = normalize_odds_response(
            _load("the_odds_api_alt_rich.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        assert len(rows) == 9

    def test_alt_market_keys_preserved(self):
        rows = normalize_odds_response(
            _load("the_odds_api_alt_rich.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        alt_rows = [r for r in rows if r["market_key"].endswith("_alternate")]
        assert len(alt_rows) == 5  # 4 FD alt + 1 MGM alt

    def test_fanduel_alt_under_captured(self):
        # FD-only alt-under (the unique direction we want to start capturing).
        rows = normalize_odds_response(
            _load("the_odds_api_alt_rich.json"),
            fetched_at_utc=datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
        )
        fd_alt_under = [
            r for r in rows
            if r["bookmaker_key"] == "fanduel"
               and r["market_key"] == "player_assists_alternate"
               and r["side"] == "under"
        ]
        assert len(fd_alt_under) == 2  # 2 alt-under lines in the fixture

    def test_fetched_at_utc_stamped_consistently(self):
        ts = datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc)
        rows = normalize_odds_response(
            _load("the_odds_api_std_only.json"),
            fetched_at_utc=ts,
        )
        for row in rows:
            assert row["fetched_at_utc"] == ts
```

- [ ] **Step 4: Run tests to verify they fail**

Run:
```powershell
.\arbitrage_executor\.venv\Scripts\python.exe -m pytest airflow/tests/test_arb_ingest.py -v
```

Expected: `ModuleNotFoundError: No module named 'bg_arb_pipeline_lib.ingest'`.

- [ ] **Step 5: Write the minimal implementation**

Create `airflow/dags/bg_arb_pipeline_lib/ingest.py`:

```python
"""Ingest task for the arb pipeline.

Fetches odds from the-odds-api v4 and normalizes JSON responses into
per-(book, market_key, line, side) row dicts ready to write to
bg_arb_stage_lines.

The fetch logic and the JSON normalization are split intentionally so the
normalizer can be tested against fixtures without real HTTP.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

import requests

from bg_arb_pipeline_lib.markets import (
    ARB_BOOKMAKERS,
    ARB_MARKETS,
    ARB_SPORTS,
    SPORT_TITLE,
)


def fetch_events(sport_key: str, api_key: str, base_url: str) -> list[dict[str, Any]]:
    """List in-window events for a sport."""
    url = f"{base_url}/v4/sports/{sport_key}/events"
    r = requests.get(url, params={"apiKey": api_key, "regions": "us"}, timeout=15)
    r.raise_for_status()
    return r.json() or []


def fetch_event_odds(
    sport_key: str,
    event_id: str,
    api_key: str,
    base_url: str,
    markets: Iterable[str] = ARB_MARKETS,
    bookmakers: Iterable[str] = ARB_BOOKMAKERS,
) -> dict[str, Any]:
    """Fetch one event's odds across all configured markets and bookmakers."""
    url = f"{base_url}/v4/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "apiKey": api_key,
        "regions": "us",
        "bookmakers": ",".join(bookmakers),
        "markets": ",".join(markets),
        "oddsFormat": "decimal",
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json() or {}


def normalize_odds_response(
    payload: dict[str, Any],
    *,
    fetched_at_utc: datetime,
) -> list[dict[str, Any]]:
    """Convert one event's odds payload into per-(book, market, line, side) rows.

    Schema reference: https://the-odds-api.com/liveapi/guides/v4/#get-event-odds
    Each outcome has: name (Over/Under), description (player name), price, point.
    """
    if not payload or "bookmakers" not in payload:
        return []

    event_id = payload["id"]
    sport_key = payload.get("sport_key", "")
    sport_title = payload.get("sport_title") or SPORT_TITLE.get(sport_key, sport_key)
    home_team = payload.get("home_team", "")
    away_team = payload.get("away_team", "")
    commence_time_str = payload.get("commence_time")
    commence_time_utc = (
        datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
        if commence_time_str else None
    )

    rows: list[dict[str, Any]] = []
    for book in payload["bookmakers"]:
        book_key = book.get("key", "")
        for market in book.get("markets", []):
            market_key = market.get("key", "")
            for outcome in market.get("outcomes", []):
                side_raw = outcome.get("name", "").lower()
                if side_raw not in ("over", "under"):
                    continue
                rows.append({
                    "event_id": event_id,
                    "sport_title": sport_title,
                    "home_team": home_team,
                    "away_team": away_team,
                    "commence_time_utc": commence_time_utc,
                    "player_name": outcome.get("description", ""),
                    "bookmaker_key": book_key,
                    "market_key": market_key,
                    "line": float(outcome["point"]),
                    "side": side_raw,
                    "price": float(outcome["price"]),
                    "fetched_at_utc": fetched_at_utc,
                })
    return rows


def ingest_all(api_key: str, base_url: str = "https://api.the-odds-api.com") -> list[dict[str, Any]]:
    """Fetch every (sport, event) and return the flat list of stage rows."""
    fetched_at_utc = datetime.now(timezone.utc)
    all_rows: list[dict[str, Any]] = []
    for sport_key in ARB_SPORTS:
        for event in fetch_events(sport_key, api_key, base_url):
            payload = fetch_event_odds(sport_key, event["id"], api_key, base_url)
            all_rows.extend(normalize_odds_response(payload, fetched_at_utc=fetched_at_utc))
    return all_rows
```

- [ ] **Step 6: Run tests to verify they pass**

Run:
```powershell
.\arbitrage_executor\.venv\Scripts\python.exe -m pytest airflow/tests/test_arb_ingest.py -v
```

Expected: `6 passed`.

- [ ] **Step 7: Commit**

```powershell
git add airflow/dags/bg_arb_pipeline_lib/ingest.py airflow/tests/test_arb_ingest.py airflow/tests/fixtures/
git commit -m "arb-pipeline: ingest task with fixture-based tests"
```

---

### Task 5: Builder — TDD (the symmetric cartesian, the core value)

**Files:**
- Create: `airflow/dags/bg_arb_pipeline_lib/builder.py`
- Create: `airflow/tests/test_arb_builder.py`

- [ ] **Step 1: Write the failing tests**

Create `airflow/tests/test_arb_builder.py`:

```python
from datetime import datetime, timezone

import pandas as pd
import pytest

from bg_arb_pipeline_lib.builder import build_opportunities


def _stage_row(**overrides):
    base = {
        "event_id": "evt_test",
        "sport_title": "NBA",
        "home_team": "Cleveland Cavaliers",
        "away_team": "Detroit Pistons",
        "commence_time_utc": datetime(2026, 5, 15, 23, 0, tzinfo=timezone.utc),
        "player_name": "Dennis Schroder",
        "bookmaker_key": "fanduel",
        "market_key": "player_assists",
        "line": 2.5,
        "side": "under",
        "price": 2.00,
        "fetched_at_utc": datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _lines_df(rows):
    return pd.DataFrame(rows)


class TestBuildOpportunities:
    def test_std_under_std_over_emits_std_std_row(self):
        # FD under 2.00 × MGM over 1.90: implied = 0.5 + 0.526 = 1.026 ... no arb.
        # Need overround < 1.0. Use FD under 2.10 × MGM over 2.20 → implied = 0.476 + 0.455 = 0.931.
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        row = opps.iloc[0]
        assert row["pairing_type"] == "std_std"
        assert row["under_book"] == "fanduel"
        assert row["over_book"] == "betmgm"
        assert row["under_market_key"] == "player_assists"
        assert row["over_market_key"] == "player_assists"
        assert row["canonical_market"] == "player_assists"
        assert row["roi"] > 0

    def test_std_under_alt_over_emits_std_alt_row(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="betmgm", market_key="player_assists",           side="under", price=2.10),
            _stage_row(bookmaker_key="fanduel", market_key="player_assists_alternate", side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        assert opps.iloc[0]["pairing_type"] == "std_alt"
        assert opps.iloc[0]["canonical_market"] == "player_assists"

    def test_alt_under_std_over_emits_alt_std_row(self):
        # The case the old code never produced.
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", market_key="player_assists_alternate", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  market_key="player_assists",           side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        assert opps.iloc[0]["pairing_type"] == "alt_std"

    def test_alt_under_alt_over_emits_alt_alt_row(self):
        # The other case the old code never produced.
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", market_key="player_assists_alternate", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  market_key="player_assists_alternate", side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        assert opps.iloc[0]["pairing_type"] == "alt_alt"

    def test_intra_book_pair_emits_nothing(self):
        # Same book on both sides is not an arb.
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10),
            _stage_row(bookmaker_key="fanduel", side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_line_mismatch_emits_nothing(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10, line=2.5),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20, line=3.5),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_negative_roi_pair_emits_nothing(self):
        # Overround >= 1.0 means no arb.
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=1.85),  # implied 0.541
            _stage_row(bookmaker_key="betmgm",  side="over",  price=1.85),  # implied 0.541 — overround 1.08
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_economics_columns_are_populated(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        row = opps.iloc[0]
        assert row["wager_under"] > 0
        assert row["wager_over"] > 0
        assert row["payout"] > 100.0
        assert row["arb_ev"] > 0
        assert row["roi"] > 0
        # Wager-under × under-price ≈ payout (arb invariant)
        assert abs(row["wager_under"] * 2.10 - row["payout"]) < 0.01
        assert abs(row["wager_over"] * 2.20 - row["payout"]) < 0.01

    def test_hours_until_commence_computed_from_commence_time(self):
        commence = datetime(2026, 5, 15, 23, 0, tzinfo=timezone.utc)
        fetched  = datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc)
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10,
                       commence_time_utc=commence, fetched_at_utc=fetched),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20,
                       commence_time_utc=commence, fetched_at_utc=fetched),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        assert opps.iloc[0]["hours_until_commence"] == pytest.approx(4.0, abs=0.01)

    def test_empty_input_returns_empty_dataframe(self):
        lines = _lines_df([])
        opps = build_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_opportunity_hash_is_unique_per_row(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20),
            _stage_row(bookmaker_key="betmgm",  market_key="player_assists_alternate", side="over", price=2.30),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        # 1 under × 2 overs (different markets) = 2 opportunities, distinct hashes.
        assert len(opps) == 2
        assert opps["opportunity_hash"].nunique() == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```powershell
.\arbitrage_executor\.venv\Scripts\python.exe -m pytest airflow/tests/test_arb_builder.py -v
```

Expected: `ModuleNotFoundError: No module named 'bg_arb_pipeline_lib.builder'`.

- [ ] **Step 3: Write the implementation**

Create `airflow/dags/bg_arb_pipeline_lib/builder.py`:

```python
"""Symmetric arb builder.

Pure function: stage-lines DataFrame in, opportunities DataFrame out.
Generates all four pairing directions (std_std, std_alt, alt_std, alt_alt)
via a cartesian self-join of unders × overs on the join key
(event_id, player_name, canonical_market, line) with the constraint that
under_book != over_book.
"""
from __future__ import annotations

from datetime import timezone
from typing import Any

import pandas as pd

from bg_arb_pipeline_lib.hashing import derive_pairing_type, opportunity_hash
from bg_arb_pipeline_lib.markets import ARB_MARKET_BLACKLIST

_ALT_SUFFIX = "_alternate"


def _strip_alt_suffix(market_key: str) -> str:
    if market_key.endswith(_ALT_SUFFIX):
        return market_key[: -len(_ALT_SUFFIX)]
    return market_key


def build_opportunities(lines: pd.DataFrame, *, base_wager: float = 100.0) -> pd.DataFrame:
    """Build arb-able pairs from a per-(book, market, line, side) DataFrame.

    Returns a DataFrame whose columns match bg_arbitrage_opportunities.
    """
    if lines is None or lines.empty:
        return pd.DataFrame()

    work = lines.copy()
    work["canonical_market"] = work["market_key"].astype(str).map(_strip_alt_suffix)
    work = work[~work["market_key"].isin(ARB_MARKET_BLACKLIST)]
    if work.empty:
        return pd.DataFrame()

    unders = work[work["side"] == "under"].copy()
    overs  = work[work["side"] == "over"].copy()
    if unders.empty or overs.empty:
        return pd.DataFrame()

    join_cols = ["event_id", "player_name", "canonical_market", "line"]
    merged = unders.merge(
        overs,
        on=join_cols,
        suffixes=("_u", "_o"),
        how="inner",
    )

    # Reject intra-book pairs.
    merged = merged[merged["bookmaker_key_u"] != merged["bookmaker_key_o"]]
    if merged.empty:
        return pd.DataFrame()

    # Arb economics
    implied_under = 1.0 / merged["price_u"]
    implied_over  = 1.0 / merged["price_o"]
    overround     = implied_under + implied_over

    merged = merged.assign(
        wager_under = base_wager / overround * implied_under,
        wager_over  = base_wager / overround * implied_over,
        payout      = base_wager / overround,
    )
    merged["arb_ev"] = merged["payout"] - base_wager
    merged["roi"]    = merged["arb_ev"] / base_wager

    merged = merged[merged["roi"] > 0]
    if merged.empty:
        return pd.DataFrame()

    # Hours until commence — use the under-side fetched_at_utc as reference.
    commence_u = pd.to_datetime(merged["commence_time_utc_u"], utc=True)
    fetched_u  = pd.to_datetime(merged["fetched_at_utc_u"], utc=True)
    merged["hours_until_commence"] = (commence_u - fetched_u).dt.total_seconds() / 3600.0

    # Pairing type — derived from the two market_keys.
    merged["pairing_type"] = merged.apply(
        lambda r: derive_pairing_type(r["market_key_u"], r["market_key_o"]), axis=1
    )

    # Final shape
    out = pd.DataFrame({
        "event_id":             merged["event_id"],
        "sport_title":          merged["sport_title_u"],
        "home_team":            merged["home_team_u"],
        "away_team":            merged["away_team_u"],
        "player_name":          merged["player_name"],
        "canonical_market":     merged["canonical_market"],
        "pairing_type":         merged["pairing_type"],
        "under_book":           merged["bookmaker_key_u"],
        "under_market_key":     merged["market_key_u"],
        "under_line":           merged["line"],
        "under_price":          merged["price_u"],
        "over_book":            merged["bookmaker_key_o"],
        "over_market_key":      merged["market_key_o"],
        "over_line":            merged["line"],
        "over_price":           merged["price_o"],
        "wager_under":          merged["wager_under"],
        "wager_over":           merged["wager_over"],
        "payout":               merged["payout"],
        "arb_ev":               merged["arb_ev"],
        "roi":                  merged["roi"],
        "hours_until_commence": merged["hours_until_commence"],
        "fetched_at_utc":       merged["fetched_at_utc_u"],
    })

    # Hashes.
    out["opportunity_hash"] = out.apply(
        lambda r: opportunity_hash({
            "event_id":         r["event_id"],
            "player_name":      r["player_name"],
            "under_book":       r["under_book"],
            "under_market_key": r["under_market_key"],
            "over_book":        r["over_book"],
            "over_market_key":  r["over_market_key"],
            "under_line":       r["under_line"],
            "under_price":      r["under_price"],
            "over_price":       r["over_price"],
        }),
        axis=1,
    )

    # Reorder columns to put hash first (matches DB PK position).
    cols = ["opportunity_hash"] + [c for c in out.columns if c != "opportunity_hash"]
    return out[cols].reset_index(drop=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```powershell
.\arbitrage_executor\.venv\Scripts\python.exe -m pytest airflow/tests/test_arb_builder.py -v
```

Expected: `11 passed`.

- [ ] **Step 5: Commit**

```powershell
git add airflow/dags/bg_arb_pipeline_lib/builder.py airflow/tests/test_arb_builder.py
git commit -m "arb-pipeline: symmetric build_opportunities + 11 unit tests"
```

---

### Task 6: DAG file wiring

**Files:**
- Create: `airflow/dags/bg_arb_pipeline.py`

- [ ] **Step 1: Check the existing DAG's schedule + asset patterns**

Read `airflow/dags/bg_arbitrage_player_props.py` lines 1-50 and note its `schedule`, default args, and Asset triggers.

- [ ] **Step 2: Write the DAG file**

Create `airflow/dags/bg_arb_pipeline.py`:

```python
"""bg_arb_pipeline: self-contained arb data pipeline.

Pulls odds from the-odds-api, builds the cartesian of arb-able pairs across
all four std/alt pairing directions, writes:
  - bg_arb_stage_lines              (replaced every run)
  - bg_arbitrage_opportunities      (replaced every run)
  - bg_arb_opportunities_history    (appended)

Decoupled from bg_unified_* — owns its own ingest and tables.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.sdk import Asset

# Ensure the per-package library is importable inside Airflow runtime.
_dag_dir = os.path.dirname(os.path.abspath(__file__))
if _dag_dir not in sys.path:
    sys.path.insert(0, _dag_dir)

from bg_arb_pipeline_lib.builder import build_opportunities  # noqa: E402
from bg_arb_pipeline_lib.ingest import ingest_all  # noqa: E402

# Shared helpers (already on PYTHONPATH inside the Airflow image).
from bountygate.utils.db_connection import (  # noqa: E402
    execute_raw_sql,
    fetch_data,
    insert_data,
)
from bountygate.utils.etl_assets import odds_apiKey, odds_url  # noqa: E402

# Completion asset (downstream DAGs / bot can wait on this).
arb_opportunities_ready_asset = Asset("bg_arbitrage_opportunities_ready")

STAGE_TABLE = "bg_arb_stage_lines"
OPP_TABLE = "bg_arbitrage_opportunities"
HISTORY_TABLE = "bg_arb_opportunities_history"
BASE_WAGER = 100.0


@dag(
    dag_id="bg_arb_pipeline",
    schedule="*/10 * * * *",  # every 10 minutes; tune via Airflow UI
    catchup=False,
    start_date=datetime(2026, 5, 15, tzinfo=timezone.utc),
    tags=["arb", "the-odds-api"],
    default_args={"retries": 1},
)
def bg_arb_pipeline_dag():

    @task()
    def ingest_odds_task() -> int:
        """Fetch every (sport, event) odds page and write to bg_arb_stage_lines."""
        rows = ingest_all(api_key=odds_apiKey, base_url=odds_url)
        if not rows:
            print("[ingest] no rows fetched")
            return 0
        df = pd.DataFrame(rows)
        insert_data(df, STAGE_TABLE, if_exists="replace")
        print(f"[ingest] wrote {len(df)} stage rows")
        return int(len(df))

    @task(outlets=[arb_opportunities_ready_asset])
    def build_opportunities_task(stage_row_count: int) -> int:
        """Read stage lines, build opportunities, write bg_arbitrage_opportunities."""
        if stage_row_count == 0:
            print("[build] no stage rows; clearing opportunities table")
            execute_raw_sql(f"DELETE FROM {OPP_TABLE};")
            return 0
        lines = fetch_data(f"SELECT * FROM {STAGE_TABLE}")
        if lines is None or lines.empty:
            execute_raw_sql(f"DELETE FROM {OPP_TABLE};")
            return 0
        opps = build_opportunities(lines, base_wager=BASE_WAGER)
        if opps.empty:
            print("[build] no arb-able pairs found")
            execute_raw_sql(f"DELETE FROM {OPP_TABLE};")
            return 0
        insert_data(opps, OPP_TABLE, if_exists="replace")
        print(f"[build] wrote {len(opps)} opportunities")
        return int(len(opps))

    @task()
    def record_history_task(opportunity_count: int) -> int:
        """Append unseen opportunity hashes to history."""
        if opportunity_count == 0:
            return 0
        current = fetch_data(f"SELECT * FROM {OPP_TABLE}")
        if current is None or current.empty:
            return 0
        existing = fetch_data(f"SELECT opportunity_hash FROM {HISTORY_TABLE}")
        existing_set = (
            set(existing["opportunity_hash"]) if existing is not None and not existing.empty else set()
        )
        new_rows = current[~current["opportunity_hash"].isin(existing_set)].copy()
        if new_rows.empty:
            print("[history] no new hashes")
            return 0
        insert_data(new_rows, HISTORY_TABLE, if_exists="append")
        print(f"[history] appended {len(new_rows)} rows")
        return int(len(new_rows))

    stage_count = ingest_odds_task()
    opp_count = build_opportunities_task(stage_count)
    record_history_task(opp_count)


dag = bg_arb_pipeline_dag()
```

- [ ] **Step 3: Verify the DAG imports without error**

Run:
```powershell
cd C:\Users\tkmer\bountygate
.\arbitrage_executor\.venv\Scripts\python.exe -c "import sys; sys.path.insert(0, 'airflow/dags'); import bg_arb_pipeline; print('DAG imports OK')"
```

Expected: `DAG imports OK`. If there's an `airflow` import error, that's fine for local check — what matters is no syntax/structural errors. (Airflow itself is only installed in the docker compose image.)

- [ ] **Step 4: Commit**

```powershell
git add airflow/dags/bg_arb_pipeline.py
git commit -m "arb-pipeline: DAG file wiring (ingest -> build -> history)"
```

---

### Task 7: Phase 1 parity check script

**Files:**
- Create: `scripts/dq_checks_arb_parity.py`

- [ ] **Step 1: Write the parity check**

Create `scripts/dq_checks_arb_parity.py`:

```python
"""Phase 1 parity check between old arb tables and the new unified table.

Run during the 24-48 hour parallel-write window before flipping bot reads.
Expects new pipeline to be a SUPERSET of old: zero rows missing from new,
plus new alt_std and alt_alt rows.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Make the shared package importable from repo root.
_repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root / "app" / "shared" / "python"))

from bountygate.utils.db_connection import fetch_data  # noqa: E402

CHECKS = {
    "std_std parity (old std rows missing from new)": """
        SELECT player_name, market_key AS under_mk, market_key AS over_mk,
               under_bookmaker_key, over_bookmaker_key
        FROM bg_arbitrage_player_props
        WHERE fetched_at_utc >= now() - INTERVAL '1 day'
        EXCEPT
        SELECT player_name, under_market_key, over_market_key,
               under_book, over_book
        FROM bg_arbitrage_opportunities
        WHERE pairing_type = 'std_std'
          AND fetched_at_utc >= now() - INTERVAL '1 day';
    """,
    "std_alt parity (old alt rows missing from new)": """
        SELECT player_name, under_market_key, over_market_key,
               under_bookmaker_key, over_bookmaker_key
        FROM bg_arbitrage_player_props_alt
        WHERE fetched_at_utc >= now() - INTERVAL '1 day'
        EXCEPT
        SELECT player_name, under_market_key, over_market_key,
               under_book, over_book
        FROM bg_arbitrage_opportunities
        WHERE pairing_type = 'std_alt'
          AND fetched_at_utc >= now() - INTERVAL '1 day';
    """,
    "pairing_type breakdown (new pipeline)": """
        SELECT pairing_type, COUNT(*) AS cnt, AVG(roi) AS avg_roi, MAX(roi) AS max_roi
        FROM bg_arbitrage_opportunities
        WHERE fetched_at_utc >= now() - INTERVAL '1 day'
        GROUP BY pairing_type
        ORDER BY pairing_type;
    """,
}


def main() -> int:
    failures = 0
    for label, sql in CHECKS.items():
        print(f"\n=== {label} ===")
        df = fetch_data(sql)
        if df is None or df.empty:
            if "parity" in label:
                print("OK — zero rows.")
            else:
                print("WARN — no rows. New pipeline produced nothing in the last day.")
                failures += 1
            continue
        print(df.to_string(index=False))
        if "parity" in label:
            print(f"FAIL — {len(df)} rows in old but not in new.")
            failures += 1
        elif "pairing_type" in label:
            missing = {"std_std", "std_alt", "alt_std", "alt_alt"} - set(df["pairing_type"])
            if missing:
                print(f"WARN — missing pairing_types: {sorted(missing)}")
    return failures


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Commit**

```powershell
git add scripts/dq_checks_arb_parity.py
git commit -m "arb-pipeline: Phase 1 parity check script"
```

---

### Task 8: Phase 1 cutover — let it run, validate

**No code changes. This task is purely operational.**

- [ ] **Step 1: Deploy the new DAG**

Restart Airflow so the new DAG file is picked up:

```powershell
cd airflow
docker compose restart scheduler
```

- [ ] **Step 2: Trigger one run from the Airflow UI**

Open <http://localhost:8080>, find `bg_arb_pipeline`, click "Trigger DAG."

- [ ] **Step 3: Confirm tables populate**

Run:
```powershell
python -c "from bountygate.utils.db_connection import fetch_data; print(fetch_data('SELECT COUNT(*) FROM bg_arb_stage_lines')); print(fetch_data('SELECT COUNT(*) FROM bg_arbitrage_opportunities')); print(fetch_data('SELECT COUNT(*) FROM bg_arb_opportunities_history'))"
```

Expected: three non-zero counts (or zero if no live arb opportunities at the moment; in that case wait 30 min and retry).

- [ ] **Step 4: Let both pipelines run in parallel for 24-48 hours**

No action — wait.

- [ ] **Step 5: Run the parity check**

Run:
```powershell
python scripts/dq_checks_arb_parity.py
```

Expected: parity checks return zero rows; pairing_type breakdown shows all four pairing_types with non-zero counts. If any parity check fails, halt and investigate before Phase 2.

---

## Phase 2 — Bot consumption (switch reads)

### Task 9: Migration 006 — add per-leg market_key columns to bg_executed_opportunities

**Files:**
- Create: `db/migrations/006_bg_executed_opportunities_market_keys.sql`

- [ ] **Step 1: Inspect the current schema first**

Run:
```powershell
python -c "from bountygate.utils.db_connection import fetch_data; print(fetch_data(\"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='bg_executed_opportunities' ORDER BY ordinal_position\"))"
```

Note: check whether `source_table` exists. The backfill SQL below assumes it does NOT; adjust if needed.

- [ ] **Step 2: Write the migration**

Create `db/migrations/006_bg_executed_opportunities_market_keys.sql`:

```sql
-- Add per-leg market_key columns to support the unified arb pipeline contract.
-- See docs/superpowers/specs/2026-05-15-unified-arb-pipeline-design.md.

ALTER TABLE bg_executed_opportunities
    ADD COLUMN IF NOT EXISTS under_market_key text,
    ADD COLUMN IF NOT EXISTS over_market_key  text;

-- Backfill: best-effort guess based on existing market_key.
-- Historical rows from std table had market_key = single std key.
-- Historical rows from alt table had market_key = std base (after the rename).
-- We can't tell them apart without a source_table column, so assume both
-- new columns equal the existing market_key. Imperfect but matches dedup
-- semantics for std_std and won't false-positive against future alt rows.
UPDATE bg_executed_opportunities
SET under_market_key = COALESCE(under_market_key, market_key),
    over_market_key  = COALESCE(over_market_key,  market_key)
WHERE under_market_key IS NULL OR over_market_key IS NULL;

-- NOT NULL going forward.
ALTER TABLE bg_executed_opportunities
    ALTER COLUMN under_market_key SET NOT NULL,
    ALTER COLUMN over_market_key  SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_executed_under_market_key
    ON bg_executed_opportunities (under_market_key);
CREATE INDEX IF NOT EXISTS idx_executed_over_market_key
    ON bg_executed_opportunities (over_market_key);
```

- [ ] **Step 3: Apply the migration**

```powershell
python scripts/migrate.py up
```

- [ ] **Step 4: Verify columns exist and are populated**

```powershell
python -c "from bountygate.utils.db_connection import fetch_data; print(fetch_data('SELECT COUNT(*) FROM bg_executed_opportunities WHERE under_market_key IS NULL OR over_market_key IS NULL'))"
```

Expected: `0` (all rows backfilled).

- [ ] **Step 5: Commit**

```powershell
git add db/migrations/006_bg_executed_opportunities_market_keys.sql
git commit -m "db: migration 006 — per-leg market_key columns on bg_executed_opportunities"
```

---

### Task 10: Switch `opportunity.py` to query the new table

**Files:**
- Modify: `arbitrage_executor/opportunity.py`

- [ ] **Step 1: Read the current file to find the query builder**

```powershell
Get-Content arbitrage_executor/opportunity.py | Select-Object -First 200
```

The function is `_build_opportunity_query` (or similar) around lines 60-160. The two table-specific branches and the union logic in the caller will all be replaced.

- [ ] **Step 2: Replace the query construction**

Open `arbitrage_executor/opportunity.py`. Find the existing two-table query build (the `if table == "bg_arbitrage_player_props"` / `elif table == "bg_arbitrage_player_props_alt"` branches) and replace the body of the public `fetch_best_opportunity` (or equivalent caller) with a single query against `bg_arbitrage_opportunities`:

```python
def _build_query(*, testing_mode: bool) -> str:
    """Single-table query against bg_arbitrage_opportunities.

    Supersedes the old two-table union (bg_arbitrage_player_props +
    bg_arbitrage_player_props_alt) per the unified-arb-pipeline design.
    """
    executed_today_filter = """
    AND NOT EXISTS (
        SELECT 1 FROM bg_executed_opportunities eo
        WHERE eo.player_name      = bg_arbitrage_opportunities.player_name
          AND eo.under_market_key = bg_arbitrage_opportunities.under_market_key
          AND eo.over_market_key  = bg_arbitrage_opportunities.over_market_key
          AND eo.executed_at_utc >= CURRENT_DATE
    )
    """

    base = f"""
    SELECT *
    FROM bg_arbitrage_opportunities
    WHERE under_book IN ('fanduel', 'betmgm')
      AND over_book  IN ('fanduel', 'betmgm')
      AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
    {executed_today_filter}
    """

    if testing_mode:
        return base + """
    AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '4 hours'
    AND hours_until_commence > 0
    AND hours_until_commence < 72
ORDER BY fetched_at_utc DESC, roi DESC
LIMIT 20;
"""
    return base + f"""
    AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '10 minutes'
    AND hours_until_commence > 0.03
    AND hours_until_commence < 24
    AND roi >= {MIN_ROI_THRESHOLD}
ORDER BY roi DESC
LIMIT 10;
"""
```

Then update the caller (was probably doing `pd.concat([fetch_data(query_std), fetch_data(query_alt)])`) to just `df = fetch_data(_build_query(testing_mode=TESTING_MODE))`.

- [ ] **Step 3: Update field name access throughout `opportunity.py`**

The old opportunity dict had `under_bookmaker_key` / `over_bookmaker_key`. The new one has `under_book` / `over_book`. Search-and-replace within `opportunity.py`:

```powershell
(Get-Content arbitrage_executor/opportunity.py) -replace 'under_bookmaker_key', 'under_book' -replace 'over_bookmaker_key', 'over_book' | Set-Content arbitrage_executor/opportunity.py
```

Also: any code that read `opportunity['market_key']` on the OLD alt rows would have gotten the base key (after the rename). Now opportunity has BOTH `under_market_key` and `over_market_key` — there is no `market_key` field. Find every `opportunity['market_key']` usage and rewrite per leg context.

- [ ] **Step 4: Run a quick smoke that the file still imports**

```powershell
cd arbitrage_executor
.venv\Scripts\python.exe -c "from opportunity import fetch_best_opportunity; print('OK')"
```

- [ ] **Step 5: Commit**

```powershell
cd C:\Users\tkmer\bountygate
git add arbitrage_executor/opportunity.py
git commit -m "executor: switch opportunity fetcher to bg_arbitrage_opportunities"
```

---

### Task 11: Switch `map_selectors.py` to single-table query

**Files:**
- Modify: `arbitrage_executor/map_selectors.py`

- [ ] **Step 1: Replace the fetcher body**

Find `fetch_opportunity_for_market` in `arbitrage_executor/map_selectors.py` (around line 200). Replace its body:

```python
def fetch_opportunity_for_market(market_key: str, bookmaker: str) -> Optional[Dict]:
    """Fetch a recent opportunity touching this (market_key, book) on either leg.

    Single-table query against bg_arbitrage_opportunities. Drops the previous
    two-table fallback (which had the alt-column bug we patched on 2026-05-15).
    """
    query = f"""
    SELECT player_name,
           sport_title,
           home_team,
           away_team,
           canonical_market,
           pairing_type,
           under_book,
           under_market_key,
           under_line,
           under_price,
           over_book,
           over_market_key,
           over_line,
           over_price
    FROM bg_arbitrage_opportunities
    WHERE (
            ('{market_key}' = under_market_key AND '{bookmaker}' = under_book)
         OR ('{market_key}' = over_market_key  AND '{bookmaker}' = over_book)
        )
      AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '4 hours'
      AND hours_until_commence > 0
      AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
    LIMIT 1;
    """

    print(f"Fetching opportunity for market: {market_key}, bookmaker: {bookmaker}")
    df = fetch_data(query)

    if df is None or df.empty:
        print(f"❌ No recent opportunities found for market: {market_key}, bookmaker: {bookmaker}")
        print(f"   Try expanding the time window or check if this market is currently active")
        return None

    opportunity = df.iloc[0].to_dict()
    print(f"✓ Found opportunity: {opportunity['player_name']} - {opportunity['canonical_market']} ({opportunity['pairing_type']})")
    print(f"  {opportunity['away_team']} @ {opportunity['home_team']}")
    print(f"  Under: {opportunity['under_book']} {opportunity['under_market_key']} @ {opportunity['under_line']} ({opportunity['under_price']})")
    print(f"  Over:  {opportunity['over_book']} {opportunity['over_market_key']} @ {opportunity['over_line']} ({opportunity['over_price']})")
    return opportunity
```

- [ ] **Step 2: Update downstream field reads in `map_selectors.py`**

The dict no longer has `under_line` / `over_line` separately if they're equal — wait, the new schema DOES have both (they're equal but both present). Code that read `opportunity['over_line']` works unchanged. Code that read `opportunity['market_key']` must pick a leg: typically the one matching the `--site` / `--market` CLI args.

Update the body of `map_selectors.py`'s main function:

```python
# After fetch_opportunity_for_market returns:
opportunity = fetch_opportunity_for_market(market_key, site)
if not opportunity:
    print(f"\n❌ Cannot map without a real opportunity for {site} - {market_key}.")
    return

# Pick whichever leg matches the (site, market_key) we're mapping.
if opportunity['under_book'] == site and opportunity['under_market_key'] == market_key:
    line = opportunity['under_line']
    direction = 'under'
elif opportunity['over_book'] == site and opportunity['over_market_key'] == market_key:
    line = opportunity['over_line']
    direction = 'over'
else:
    print(f"❌ Internal: opportunity row didn't match the requested (site, market).")
    return

player_name = opportunity['player_name']
home_team = opportunity['home_team']
away_team = opportunity['away_team']
```

- [ ] **Step 3: Smoke-test map_selectors against a real market**

```powershell
cd arbitrage_executor
.venv\Scripts\python.exe map_selectors.py --site betmgm --market player_assists_alternate
```

Expected: finds an opportunity (assuming the new pipeline has run and produced rows). If the bug we patched earlier today now produces a useful result, that's a positive signal.

- [ ] **Step 4: Commit**

```powershell
cd C:\Users\tkmer\bountygate
git add arbitrage_executor/map_selectors.py
git commit -m "executor: map_selectors now reads single bg_arbitrage_opportunities table"
```

---

### Task 12: Per-leg market_key in `bet_placer.py` and `execute_arb.py`

**Files:**
- Modify: `arbitrage_executor/execute_arb.py`
- Modify: `arbitrage_executor/bet_placer.py`

- [ ] **Step 1: Find all `opportunity['market_key']` (and equivalents) in execute_arb.py**

```powershell
Select-String -Path arbitrage_executor/execute_arb.py -Pattern "market_key" -SimpleMatch
```

Each hit must be re-resolved per leg: under-leg sites get `opp['under_market_key']`; over-leg sites get `opp['over_market_key']`.

- [ ] **Step 2: Update execute_arb.py's 3-phase orchestration**

Edit `arbitrage_executor/execute_arb.py`. The orchestrator's phase calls today look something like:

```python
fd_max = placer.discover_fanduel_max(opp, market_key=opp['market_key'])
placer.place_betmgm(opp, market_key=opp['market_key'], wager=mgm_stake)
placer.place_fanduel(opp, market_key=opp['market_key'], wager=hedge_stake)
```

Replace with explicit per-leg keys. Phase 1 is the FD tease — which leg is FD on? Depends on the opportunity. Use:

```python
# Determine which leg each book is on.
fd_leg = 'under' if opp['under_book'] == 'fanduel' else 'over'
mgm_leg = 'under' if opp['under_book'] == 'betmgm'  else 'over'

fd_market_key  = opp[f'{fd_leg}_market_key']
mgm_market_key = opp[f'{mgm_leg}_market_key']
fd_line        = opp[f'{fd_leg}_line']
mgm_line       = opp[f'{mgm_leg}_line']
fd_price       = opp[f'{fd_leg}_price']
mgm_price      = opp[f'{mgm_leg}_price']

# Phase 1 — tease FD limit on the FD leg
fd_max = placer.discover_fanduel_max(opp, market_key=fd_market_key, line=fd_line, side=fd_leg)

# Phase 2 — place on MGM
placer.place_betmgm(opp, market_key=mgm_market_key, line=mgm_line, side=mgm_leg, wager=mgm_stake)

# Phase 3 — hedge on FD
placer.place_fanduel_hedge(opp, market_key=fd_market_key, line=fd_line, side=fd_leg, wager=hedge_stake)
```

- [ ] **Step 3: Update bet_placer.py method signatures**

The methods on `BetPlacer` (e.g., `discover_fanduel_max`, `place_betmgm`, `place_fanduel_hedge`) currently take `opportunity` and read `opportunity['market_key']` internally. Update them to take an explicit `market_key` parameter and stop reading the opportunity-level field.

Inside each method, replace `self.market_key = opp['market_key']` with `self.market_key = market_key` (or the local variable). Audit each line that references `opp['market_key']` and either:
- Take the value from the new parameter.
- Take it from the appropriate per-leg field (`opp[f'{leg}_market_key']`).

Similarly: replace `opp['line']`, `opp['under_line']`/`opp['over_line']` usages with explicit per-leg `line` parameters where appropriate.

- [ ] **Step 4: Quick import check**

```powershell
cd arbitrage_executor
.venv\Scripts\python.exe -c "from execute_arb import ArbExecutor; from bet_placer import BetPlacer; print('OK')"
```

- [ ] **Step 5: Run an end-to-end test with a small candidate set**

```powershell
.venv\Scripts\python.exe execute_arb.py --max-candidates 3
```

Watch for `selectors not mapped` warnings — expect a small uptick from previously-unattempted alt combinations. Map them via the existing flow:

```powershell
.venv\Scripts\python.exe map_selectors.py --site <site> --market <market_with_miss>
```

- [ ] **Step 6: Commit**

```powershell
cd C:\Users\tkmer\bountygate
git add arbitrage_executor/execute_arb.py arbitrage_executor/bet_placer.py
git commit -m "executor: per-leg market_key resolution in 3-phase orchestrator"
```

---

### Task 13: Supervised sessions + map the gaps

**No code changes. Operational task.**

- [ ] **Step 1: Run two supervised sessions back-to-back**

```powershell
cd arbitrage_executor
.venv\Scripts\python.exe execute_arb.py --max-candidates 10
.venv\Scripts\python.exe execute_arb.py --max-candidates 10
```

- [ ] **Step 2: Inventory unmapped markets from the runs**

```powershell
Get-Content logs/unmapped_markets.log -Tail 50
```

- [ ] **Step 3: Map each gap**

For each unique `(site, market_key)` pair in the unmapped log:

```powershell
.venv\Scripts\python.exe map_selectors.py --site <site> --market <market_key>
```

- [ ] **Step 4: Re-run and confirm the gap is closed**

```powershell
.venv\Scripts\python.exe execute_arb.py --max-candidates 10
```

- [ ] **Step 5: Inspect pairing_type distribution in the new history**

```powershell
python -c "from bountygate.utils.db_connection import fetch_data; print(fetch_data(\"SELECT pairing_type, COUNT(*), AVG(roi)::numeric(6,4), MAX(roi)::numeric(6,4) FROM bg_arbitrage_opportunities WHERE fetched_at_utc >= now() - INTERVAL '6 hours' GROUP BY pairing_type ORDER BY pairing_type\"))"
```

Expected: rows for all four pairing_types with non-zero counts. Validates the hypothesis that alt_std and alt_alt are real and produce opportunities.

---

## Phase 3 — Cleanup

### Task 14: Retire the old arb DAG

**Files:**
- Delete: `airflow/dags/bg_arbitrage_player_props.py`

- [ ] **Step 1: Confirm nothing else imports it**

```powershell
Get-ChildItem -Recurse -Include *.py,*.md,*.yaml,*.ps1 -Path C:\Users\tkmer\bountygate -Exclude .venv | Select-String -Pattern "bg_arbitrage_player_props" | Where-Object { $_.Path -notlike "*\.venv\*" -and $_.Path -notlike "*\dags\bg_arbitrage_player_props\.py" -and $_.Path -notlike "*\docs\*" }
```

Expected: only references in spec/plan docs (acceptable). If any live code references it, halt.

- [ ] **Step 2: Delete the old DAG**

```powershell
git rm airflow/dags/bg_arbitrage_player_props.py
```

- [ ] **Step 3: Restart Airflow to drop the DAG from the scheduler**

```powershell
cd airflow
docker compose restart scheduler
```

Confirm via Airflow UI that `bg_arbitrage_player_props` is no longer listed.

- [ ] **Step 4: Commit**

```powershell
cd C:\Users\tkmer\bountygate
git commit -m "cleanup: remove legacy bg_arbitrage_player_props DAG (replaced by bg_arb_pipeline)"
```

---

### Task 15: Confirm bot still runs cleanly after old DAG is gone

**No code changes. Verification only.**

- [ ] **Step 1: Run a small live session**

```powershell
cd arbitrage_executor
.venv\Scripts\python.exe execute_arb.py --max-candidates 5
```

Expected: opportunities fetched from `bg_arbitrage_opportunities` only; no errors referring to the deleted DAG.

- [ ] **Step 2: Verify heartbeat continues**

Run the task worker for one full heartbeat cycle (~30 min default) and confirm the Discord heartbeat fires with sane counts.

---

## What this plan does NOT do (deferred to future plans)

- **Phase 4 of the migration** (dropping the old `bg_arbitrage_player_props` and `bg_arbitrage_player_props_alt` tables) — defer ~1 month for safety. Trivial when ready.
- **`bg_executed_opportunities.market_key` column drop** — same reasoning; defer.
- **`bet_placer.py` god-object split** (CRITIQUE #12) — out of scope.
- **CI workflow for the new tests** (CRITIQUE #18) — out of scope; tests run locally for now.
- **Reliability items from CRITIQUE** (orphan reconciler, balance reconciliation, Discord webhook hardening) — separate plans.

---

## Self-review checklist

After all 15 tasks complete:

- [ ] **Spec coverage.** Every section of the spec has a task implementing it:
  - Why / What → reflected in the goal and architecture.
  - Schema → Task 1.
  - DAG transformation logic → Tasks 4, 5, 6.
  - Bot consumption side → Tasks 10, 11, 12.
  - Migration plan Phase 1 → Tasks 1-8.
  - Migration plan Phase 2 → Tasks 9-13.
  - Migration plan Phase 3 → Tasks 14-15.
  - Testing strategy → Tasks 2, 4, 5 (Layer 1+2), Task 7 (Layer 3).
- [ ] **Placeholder scan.** No "TBD", "TODO", "implement appropriately." Each step has actual code or an exact command.
- [ ] **Type consistency.**
  - `under_book`, `over_book` used uniformly (not `under_bookmaker_key`).
  - `under_market_key`, `over_market_key`, `canonical_market` used uniformly.
  - `opportunity_hash` precision `.3f` / `.6f` consistent between `hashing.py` and the existing `_build_opportunity_key`.
  - `derive_pairing_type` returns exactly `std_std | std_alt | alt_std | alt_alt`.
- [ ] **Reversibility.** Phases 1-3 each commit independently; if Phase 2 reveals a bug, revert is `git revert` of the Phase 2 commits while leaving the new tables in place.

## Validation gates (between phases)

After Phase 1 commits:
```powershell
.\arbitrage_executor\.venv\Scripts\python.exe -m pytest airflow/tests/ -v
```
Expected: all unit tests pass.

After Phase 2 commits:
```powershell
.\arbitrage_executor\.venv\Scripts\python.exe -c "from execute_arb import ArbExecutor; print('imports OK')"
.\arbitrage_executor\.venv\Scripts\python.exe execute_arb.py --max-candidates 3
```
Expected: imports clean; bot runs end-to-end against the new table.

After Phase 3 commits:
```powershell
Get-ChildItem -Recurse -Include *.py -Path C:\Users\tkmer\bountygate -Exclude .venv | Select-String -Pattern "bg_arbitrage_player_props"
```
Expected: only references in spec/plan/archive docs.
