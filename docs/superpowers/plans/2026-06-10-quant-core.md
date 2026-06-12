# Quant Core (Stage 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put Tier 1 of the modeling ladder in the database: per-method fair prices, derived closing lines, game results, and the scoring loop (CLV, venue sharpness, calibration), served by five new API endpoints.

**Architecture:** New pure-function package `bountygate/models/` (fair, weights, closing, scoring) + DB builders in `models/__init__.py` mirroring the existing `transforms/marts/` pattern; four migrations (011–014); four Airflow DAGs; three FastAPI routers. Closing lines are *derived* from already-captured snapshots (last pre-commence row), never raced.

**Tech Stack:** Python 3.12, SQLAlchemy text() SQL, polars (already used in marts), Airflow 3 TaskFlow + Assets, FastAPI + TestClient/sqlite tests, pg_partman partitioning, pytest.

**Spec:** `docs/superpowers/specs/2026-06-10-quant-core-design.md`

**Conventions used throughout (from the existing codebase):**
- Run everything with global `py -3.12` from the repo root (NOT the arbitrage_executor venv).
- DB engine helper pattern: `os.environ["DATABASE_URL"]`, replace `postgres://` → `postgresql+psycopg2://`.
- Pure modules have zero I/O; builders own SQL; DAG files are thin wrappers.
- Web routers: `text()` SQL compatible with BOTH Postgres and sqlite (tests inject a sqlite engine via `app.dependency_overrides[get_engine]`).
- `scripts/migrate.py` strips `--` comments then splits statements on `;` — never put `;` or `--` inside SQL string literals in migrations.
- Sport keys: `baseball_mlb`, `basketball_nba`, `icehockey_nhl`.

---

### Task 1: Migrations 011 (fair_prices + mart_fair_odds)

**Files:**
- Create: `db/migrations/011_fair_prices.sql`

- [ ] **Step 1: Write the migration**

```sql
-- Per-method fair probabilities per snapshot (time-series, partitioned like 004).
CREATE TABLE fair_prices (
  event_id     uuid        NOT NULL,
  market_type  text        NOT NULL,
  bookmaker    text        NOT NULL,
  outcome_name text        NOT NULL,
  method       text        NOT NULL,
  fair_prob    numeric     NOT NULL,
  captured_at  timestamptz NOT NULL
) PARTITION BY RANGE (captured_at);
CREATE INDEX ix_fair_prices_event ON fair_prices (event_id, captured_at);
CREATE INDEX brin_fair_prices_captured ON fair_prices USING brin (captured_at);

SELECT partman.create_parent(p_parent_table := 'public.fair_prices',
  p_control := 'captured_at', p_interval := '1 day', p_type := 'range', p_premake := 4);

UPDATE partman.part_config SET retention = '2 years', retention_keep_table = false
WHERE parent_table = 'public.fair_prices';

-- Serving table for GET /fair-odds, truncate-rebuilt by build_fair_odds.
CREATE TABLE mart_fair_odds (
  event_id       uuid NOT NULL,
  sport_key      text,
  commence_time  timestamptz,
  home_team      text,
  away_team      text,
  market_type    text NOT NULL,
  outcome_name   text NOT NULL,
  consensus_prob numeric,
  best_price     numeric,
  best_bookmaker text,
  edge           numeric,
  computed_at    timestamptz
);
CREATE INDEX ix_mart_fair_odds_event ON mart_fair_odds (event_id);
```

- [ ] **Step 2: Check it is pending**

Run: `py -3.12 scripts/migrate.py status`
Expected: `011_fair_prices  PENDING` (001–010 APPLIED)

- [ ] **Step 3: Apply**

Run: `py -3.12 scripts/migrate.py up`
Expected: `Applied 011_fair_prices`

- [ ] **Step 4: Commit**

```bash
git add db/migrations/011_fair_prices.sql
git commit -m "feat(db): fair_prices (partitioned) + mart_fair_odds (mig 011)"
```

---

### Task 2: Migrations 012–014 (closing_lines, model registry, results + scoring)

**Files:**
- Create: `db/migrations/012_closing_lines.sql`
- Create: `db/migrations/013_model_registry.sql`
- Create: `db/migrations/014_results_scoring.sql`

- [ ] **Step 1: Write 012_closing_lines.sql**

```sql
-- Derived closing line per event/market/book/outcome (last pre-commence snapshot).
CREATE TABLE closing_lines (
  event_id          uuid NOT NULL,
  market_type       text NOT NULL,
  bookmaker         text NOT NULL,
  outcome_name      text NOT NULL,
  decimal_price     numeric,
  fair_prob         numeric,
  captured_at       timestamptz,
  staleness_minutes numeric,
  UNIQUE (event_id, market_type, bookmaker, outcome_name)
);
CREATE INDEX ix_closing_lines_event ON closing_lines (event_id);
```

- [ ] **Step 2: Write 013_model_registry.sql**

```sql
-- Common prediction shape all model tiers write to. consensus_v1 is the first source.
CREATE TABLE model_versions (
  model_key   text NOT NULL,
  version     text NOT NULL,
  created_at  timestamptz DEFAULT now(),
  description text,
  PRIMARY KEY (model_key, version)
);

CREATE TABLE model_predictions (
  model_key    text NOT NULL,
  version      text NOT NULL,
  event_id     uuid NOT NULL,
  market_type  text NOT NULL,
  outcome_name text NOT NULL,
  prob         numeric NOT NULL,
  predicted_at timestamptz NOT NULL,
  UNIQUE (model_key, version, event_id, market_type, outcome_name, predicted_at)
);
CREATE INDEX ix_model_predictions_event ON model_predictions (event_id, predicted_at);
```

- [ ] **Step 3: Write 014_results_scoring.sql**

```sql
-- Game finals + scoring outputs. winner is 'home' or 'away' (resolved against the
-- event's own team names, immune to feed-vs-odds naming differences).
CREATE TABLE game_results (
  event_id     uuid PRIMARY KEY REFERENCES sports_events(event_id),
  home_score   int,
  away_score   int,
  winner       text,
  completed_at timestamptz,
  source       text
);

CREATE TABLE venue_sharpness (
  venue_key    text NOT NULL,
  sport_key    text NOT NULL,
  score_window text NOT NULL,
  n_games      int,
  brier        numeric,
  logloss      numeric,
  avg_clv      numeric,
  computed_at  timestamptz,
  UNIQUE (venue_key, sport_key, score_window)
);

CREATE TABLE mart_calibration (
  source      text NOT NULL,
  sport_key   text NOT NULL,
  prob_bucket numeric NOT NULL,
  n           int,
  predicted_mean numeric,
  realized_rate  numeric,
  computed_at timestamptz,
  UNIQUE (source, sport_key, prob_bucket)
);
```

- [ ] **Step 4: Apply and verify**

Run: `py -3.12 scripts/migrate.py up`
Expected: `Applied 012_closing_lines`, `Applied 013_model_registry`, `Applied 014_results_scoring`
Then: `py -3.12 scripts/migrate.py status` → all 14 APPLIED.

- [ ] **Step 5: Commit**

```bash
git add db/migrations/012_closing_lines.sql db/migrations/013_model_registry.sql db/migrations/014_results_scoring.sql
git commit -m "feat(db): closing_lines, model registry, results + scoring tables (migs 012-014)"
```

---

### Task 3: `models` package scaffold + `fair.py`

**Files:**
- Create: `app/shared/python/bountygate/models/fair.py`
- Create: `app/shared/python/bountygate/models/tests/__init__.py` (empty)
- Create: `app/shared/python/bountygate/models/tests/conftest.py`
- Test: `app/shared/python/bountygate/models/tests/test_fair.py`

Note: `models/__init__.py` is created in Task 7 (it holds the DB builders, mirroring
`transforms/marts/__init__.py`). Until then create it as an empty file so imports resolve.

- [ ] **Step 1: Create scaffold**

Create empty `app/shared/python/bountygate/models/__init__.py` and
`app/shared/python/bountygate/models/tests/__init__.py`.

Create `app/shared/python/bountygate/models/tests/conftest.py` (same as transforms/tests):

```python
import os
import sys

# app/shared/python on sys.path so `import bountygate.models...` resolves on host.
_PKG_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)
```

- [ ] **Step 2: Write the failing tests**

`app/shared/python/bountygate/models/tests/test_fair.py`:

```python
import pytest

from bountygate.models.fair import book_fair_probs, weighted_consensus

EVEN = 1.9091  # -110 American


def test_book_fair_probs_symmetric_book():
    out = book_fair_probs({"fanduel": {"Over": EVEN, "Under": EVEN}}, ["Over", "Under"])
    assert set(out) == {"fanduel"}
    for method in ("mult", "power", "shin"):
        assert out["fanduel"][method]["Over"] == pytest.approx(0.5, abs=1e-6)
        assert out["fanduel"][method]["Under"] == pytest.approx(0.5, abs=1e-6)


def test_book_fair_probs_skips_one_sided_books():
    out = book_fair_probs(
        {"fanduel": {"Over": EVEN, "Under": EVEN}, "draftkings": {"Over": 1.87}},
        ["Over", "Under"],
    )
    assert "draftkings" not in out and "fanduel" in out


def test_weighted_consensus_equal_weights_is_mean():
    probs = {"a": {"X": 0.6, "Y": 0.4}, "b": {"X": 0.5, "Y": 0.5}}
    cons = weighted_consensus(probs)
    assert cons["X"] == pytest.approx(0.55)
    assert cons["X"] + cons["Y"] == pytest.approx(1.0)


def test_weighted_consensus_respects_weights():
    probs = {"a": {"X": 0.6, "Y": 0.4}, "b": {"X": 0.5, "Y": 0.5}}
    cons = weighted_consensus(probs, {"a": 1.0, "b": 0.0})
    assert cons["X"] == pytest.approx(0.6)


def test_weighted_consensus_unknown_book_gets_default_half_weight():
    probs = {"a": {"X": 0.6, "Y": 0.4}, "b": {"X": 0.5, "Y": 0.5}}
    # weights dict missing 'b' -> b gets 0.5: X = (1*0.6 + 0.5*0.5) / 1.5
    cons = weighted_consensus(probs, {"a": 1.0})
    assert cons["X"] == pytest.approx((0.6 + 0.25) / 1.5)


def test_weighted_consensus_empty_returns_none():
    assert weighted_consensus({}) is None
    assert weighted_consensus({"a": {"X": 0.6}}, {"a": 0.0}) is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests/test_fair.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.models.fair'`

- [ ] **Step 4: Write `fair.py`**

```python
"""Per-book fair probabilities and the sharpness-weighted consensus blend.

Pure functions over two-way market prices; devig math comes from analytics.devig.
"""
from __future__ import annotations

from bountygate.analytics.devig import devig_all

DEFAULT_WEIGHT = 0.5  # weight for books absent from the weights dict


def book_fair_probs(prices_by_book: dict, names: list) -> dict:
    """Fair probs per book per devig method for one two-way group.

    prices_by_book: {bookmaker: {outcome_name: decimal_price}}
    names: the ordered two-way pair [name0, name1].
    Returns {bookmaker: {'mult'|'power'|'shin': {name: fair_prob}}}; books
    missing either side (devig_all -> None) are skipped.
    """
    n0, n1 = names
    out: dict = {}
    for book, prices in prices_by_book.items():
        d = devig_all(prices.get(n0), prices.get(n1))
        if d is None:
            continue
        out[book] = {
            "mult": {n0: d["fair_prob_over_mult"], n1: d["fair_prob_under_mult"]},
            "power": {n0: d["fair_prob_over_power"], n1: d["fair_prob_under_power"]},
            "shin": {n0: d["fair_prob_over_shin"], n1: d["fair_prob_under_shin"]},
        }
    return out


def weighted_consensus(probs_by_book: dict, weights: dict | None = None):
    """Weighted mean of per-book fair probs, renormalized to sum 1.

    probs_by_book: {bookmaker: {outcome_name: fair_prob}} (one method's probs).
    weights: {bookmaker: weight}; missing books get DEFAULT_WEIGHT; None means
    equal weights (reproduces the unweighted consensus). Books not quoting every
    outcome are skipped. Returns {outcome_name: prob} or None if nothing usable.
    """
    if not probs_by_book:
        return None
    names = set()
    for probs in probs_by_book.values():
        names |= set(probs)
    totals = {n: 0.0 for n in names}
    wsum = 0.0
    for book, probs in probs_by_book.items():
        if any(probs.get(n) is None for n in names):
            continue
        w = 1.0 if weights is None else float(weights.get(book, DEFAULT_WEIGHT))
        if w <= 0:
            continue
        wsum += w
        for n in names:
            totals[n] += w * float(probs[n])
    if wsum <= 0:
        return None
    cons = {n: totals[n] / wsum for n in names}
    s = sum(cons.values())
    return {n: v / s for n, v in cons.items()}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests/test_fair.py -v`
Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add app/shared/python/bountygate/models
git commit -m "feat(models): fair.py - per-book devig fair probs + weighted consensus"
```

---

### Task 4: `weights.py` — sharpness weights

**Files:**
- Create: `app/shared/python/bountygate/models/weights.py`
- Test: `app/shared/python/bountygate/models/tests/test_weights.py`

- [ ] **Step 1: Write the failing tests**

```python
import pytest

from bountygate.models.weights import sharpness_weights


def test_prior_when_no_venue_qualifies():
    stats = {"pinnacle": {"brier": 0.20, "n_games": 10},
             "fanduel": {"brier": 0.26, "n_games": 10}}
    w = sharpness_weights(stats, min_games=200)
    assert w == {"pinnacle": 1.0, "fanduel": 0.5}


def test_qualified_venues_get_inverse_brier_weights():
    stats = {"pinnacle": {"brier": 0.24, "n_games": 300},
             "fanduel": {"brier": 0.25, "n_games": 300},
             "newbook": {"brier": 0.20, "n_games": 10}}
    w = sharpness_weights(stats, min_games=200)
    assert w["pinnacle"] == pytest.approx(1.0)           # sharpest qualified
    assert w["fanduel"] == pytest.approx(0.24 / 0.25)
    assert w["newbook"] == 0.5                            # unqualified -> prior


def test_empty_stats_returns_empty_dict():
    assert sharpness_weights({}, min_games=200) == {}


def test_zero_brier_is_clamped_not_divided():
    stats = {"a": {"brier": 0.0, "n_games": 300}, "b": {"brier": 0.25, "n_games": 300}}
    w = sharpness_weights(stats, min_games=200)
    assert w["a"] == pytest.approx(1.0)
    assert 0 < w["b"] < 1e-3      # 1e-6 / 0.25
```

- [ ] **Step 2: Run to verify failure**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests/test_weights.py -v`
Expected: FAIL — `No module named 'bountygate.models.weights'`

- [ ] **Step 3: Write `weights.py`**

```python
"""Sharpness weights for the consensus blend, from accumulated venue Brier scores.

Pinnacle-anchored prior until a venue has enough scored games:
prior weight 1.0 for pinnacle, 0.5 for everyone else. Once >= min_games,
weight = min_qualified_brier / venue_brier (sharpest qualified venue = 1.0).
"""
from __future__ import annotations

import os

PRIOR_SHARP = 1.0
PRIOR_OTHER = 0.5
SHARP_VENUE = "pinnacle"
_BRIER_FLOOR = 1e-6


def min_games_default() -> int:
    return int(os.environ.get("BG_SHARPNESS_MIN_GAMES", "200"))


def sharpness_weights(stats: dict, *, min_games: int | None = None) -> dict:
    """stats: {venue: {'brier': float, 'n_games': int}} -> {venue: weight}.

    Venues absent from stats simply aren't in the result; consumers fall back
    to fair.DEFAULT_WEIGHT for unknown books.
    """
    if min_games is None:
        min_games = min_games_default()
    qualified = {
        v: max(float(s["brier"]), _BRIER_FLOOR)
        for v, s in stats.items()
        if s.get("brier") is not None and (s.get("n_games") or 0) >= min_games
    }
    out: dict = {}
    min_b = min(qualified.values()) if qualified else None
    for venue in stats:
        if venue in qualified:
            out[venue] = min_b / qualified[venue]
        else:
            out[venue] = PRIOR_SHARP if venue == SHARP_VENUE else PRIOR_OTHER
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests/test_weights.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add app/shared/python/bountygate/models/weights.py app/shared/python/bountygate/models/tests/test_weights.py
git commit -m "feat(models): weights.py - Pinnacle-anchored sharpness weights"
```

---

### Task 5: `closing.py` — closing-line derivation

**Files:**
- Create: `app/shared/python/bountygate/models/closing.py`
- Test: `app/shared/python/bountygate/models/tests/test_closing.py`

- [ ] **Step 1: Write the failing tests**

```python
from datetime import datetime, timedelta, timezone

import pytest

from bountygate.models.closing import derive_closing

T0 = datetime(2026, 6, 10, 19, 0, tzinfo=timezone.utc)   # commence


def _row(book, name, price, minutes_before):
    return {"bookmaker": book, "outcome_name": name, "decimal_price": price,
            "captured_at": T0 - timedelta(minutes=minutes_before)}


def test_picks_last_pre_commence_snapshot_per_book_outcome():
    rows = [
        _row("fanduel", "A", 1.90, 30),
        _row("fanduel", "A", 1.95, 5),       # latest pre-commence -> wins
        _row("fanduel", "B", 1.90, 5),
        {"bookmaker": "fanduel", "outcome_name": "A", "decimal_price": 2.10,
         "captured_at": T0 + timedelta(minutes=5)},   # post-commence -> ignored
    ]
    out = derive_closing(rows, T0)
    a = next(r for r in out if r["outcome_name"] == "A")
    assert a["decimal_price"] == 1.95
    assert a["staleness_minutes"] == pytest.approx(5.0)


def test_two_sided_book_gets_mult_devig_fair_prob():
    rows = [_row("fanduel", "A", 1.9091, 5), _row("fanduel", "B", 1.9091, 5)]
    out = derive_closing(rows, T0)
    assert all(r["fair_prob"] == pytest.approx(0.5, abs=1e-6) for r in out)


def test_one_sided_book_has_null_fair_prob_but_keeps_price():
    out = derive_closing([_row("dk", "A", 1.87, 10)], T0)
    assert len(out) == 1
    assert out[0]["fair_prob"] is None and out[0]["decimal_price"] == 1.87


def test_invalid_prices_skipped():
    assert derive_closing([_row("dk", "A", 1.0, 10), _row("dk", "A", None, 5)], T0) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests/test_closing.py -v`
Expected: FAIL — `No module named 'bountygate.models.closing'`

- [ ] **Step 3: Write `closing.py`**

```python
"""Closing-line derivation: the last pre-commence snapshot per (book, outcome).

Pure: rows in, closing dicts out. The builder owns the SQL and the insert.
"""
from __future__ import annotations

from bountygate.analytics.devig import implied_prob, multiplicative_devig


def derive_closing(rows: list, commence_time) -> list:
    """rows: dicts with bookmaker, outcome_name, decimal_price, captured_at
    (tz-aware datetime), all for ONE (event, market_type).

    Returns one dict per (bookmaker, outcome): bookmaker, outcome_name,
    decimal_price, fair_prob (multiplicative devig when the book quotes both
    sides of a two-way pair, else None), captured_at, staleness_minutes.
    """
    pre = [
        r for r in rows
        if r.get("decimal_price") and float(r["decimal_price"]) > 1.0
        and r["captured_at"] <= commence_time
    ]
    latest: dict = {}
    for r in sorted(pre, key=lambda r: r["captured_at"]):
        latest[(r["bookmaker"], r["outcome_name"])] = r

    by_book: dict = {}
    for (book, name), r in latest.items():
        by_book.setdefault(book, {})[name] = r

    out = []
    for book, by_name in sorted(by_book.items()):
        fair = None
        if len(by_name) == 2:
            (n0, r0), (n1, r1) = sorted(by_name.items())
            f0, f1 = multiplicative_devig(
                implied_prob(float(r0["decimal_price"])),
                implied_prob(float(r1["decimal_price"])),
            )
            fair = {n0: f0, n1: f1}
        for name, r in sorted(by_name.items()):
            out.append({
                "bookmaker": book,
                "outcome_name": name,
                "decimal_price": float(r["decimal_price"]),
                "fair_prob": fair[name] if fair else None,
                "captured_at": r["captured_at"],
                "staleness_minutes":
                    (commence_time - r["captured_at"]).total_seconds() / 60.0,
            })
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests/test_closing.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add app/shared/python/bountygate/models/closing.py app/shared/python/bountygate/models/tests/test_closing.py
git commit -m "feat(models): closing.py - derive closing lines from snapshots"
```

---

### Task 6: `scoring.py` — Brier, log loss, calibration buckets

**Files:**
- Create: `app/shared/python/bountygate/models/scoring.py`
- Test: `app/shared/python/bountygate/models/tests/test_scoring.py`

- [ ] **Step 1: Write the failing tests**

```python
import math

import pytest

from bountygate.models.scoring import brier_score, calibration_buckets, log_loss_score


def test_brier_hand_computed():
    # ((0.8-1)^2 + (0.4-0)^2) / 2 = (0.04 + 0.16) / 2
    assert brier_score([(0.8, 1), (0.4, 0)]) == pytest.approx(0.10)


def test_brier_empty_is_none():
    assert brier_score([]) is None


def test_log_loss_hand_computed():
    expected = -(math.log(0.8) + math.log(0.6)) / 2
    assert log_loss_score([(0.8, 1), (0.4, 0)]) == pytest.approx(expected)


def test_log_loss_clamps_extremes():
    assert math.isfinite(log_loss_score([(1.0, 0), (0.0, 1)]))


def test_calibration_buckets():
    pairs = [(0.75, 1), (0.78, 1), (0.72, 0), (0.05, 0), (1.0, 1)]
    buckets = {b["prob_bucket"]: b for b in calibration_buckets(pairs)}
    b7 = buckets[0.7]
    assert b7["n"] == 3
    assert b7["predicted_mean"] == pytest.approx((0.75 + 0.78 + 0.72) / 3)
    assert b7["realized_rate"] == pytest.approx(2 / 3)
    assert buckets[0.0]["n"] == 1
    assert buckets[0.9]["n"] == 1     # p=1.0 lands in the top bucket
```

- [ ] **Step 2: Run to verify failure**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests/test_scoring.py -v`
Expected: FAIL — `No module named 'bountygate.models.scoring'`

- [ ] **Step 3: Write `scoring.py`**

```python
"""Prediction scoring: Brier, log loss, calibration buckets.

pairs = [(predicted_prob, realized), ...] with realized in {0, 1} (bools ok).
Venue closing lines and model_predictions rows score through the same functions.
"""
from __future__ import annotations

import math

_EPS = 1e-12
N_BUCKETS = 10


def brier_score(pairs: list):
    if not pairs:
        return None
    return sum((float(p) - float(bool(y))) ** 2 for p, y in pairs) / len(pairs)


def log_loss_score(pairs: list):
    if not pairs:
        return None
    total = 0.0
    for p, y in pairs:
        p = min(max(float(p), _EPS), 1.0 - _EPS)
        total += -(math.log(p) if y else math.log(1.0 - p))
    return total / len(pairs)


def calibration_buckets(pairs: list, n_buckets: int = N_BUCKETS) -> list:
    """Equal-width prob buckets; returns [{prob_bucket (lower bound), n,
    predicted_mean, realized_rate}, ...] sorted by bucket."""
    acc: dict = {}
    for p, y in pairs:
        idx = min(int(float(p) * n_buckets), n_buckets - 1)
        lb = round(idx / n_buckets, 10)
        n, sp, sy = acc.get(lb, (0, 0.0, 0.0))
        acc[lb] = (n + 1, sp + float(p), sy + float(bool(y)))
    return [
        {"prob_bucket": lb, "n": n, "predicted_mean": sp / n, "realized_rate": sy / n}
        for lb, (n, sp, sy) in sorted(acc.items())
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests/test_scoring.py -v`
Expected: 5 passed

- [ ] **Step 5: Run the whole models test dir + commit**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests -v`
Expected: all passed (fair 6, weights 4, closing 4, scoring 5)

```bash
git add app/shared/python/bountygate/models/scoring.py app/shared/python/bountygate/models/tests/test_scoring.py
git commit -m "feat(models): scoring.py - brier, log loss, calibration buckets"
```

---

### Task 7: Builder — `build_fair_prices` (fair_prices + model_predictions + mart_fair_odds)

**Files:**
- Modify: `app/shared/python/bountygate/models/__init__.py` (currently empty)

Builders mirror `transforms/marts/__init__.py`: `_engine()` helper + one function per
output, SQL via `text()`, all pure logic delegated to the tested modules. Per the
existing codebase pattern, builders have no direct unit tests — the pure functions
carry the logic; DAG runs exercise the SQL.

- [ ] **Step 1: Write the builder module header + fair builder**

`app/shared/python/bountygate/models/__init__.py`:

```python
"""DB I/O for the quant core: snapshots -> fair prices, closing lines, results, scoring.

Mirrors transforms/marts: pure logic lives in fair/weights/closing/scoring; this
module owns SQL. All builders create and dispose their own engine.
"""
from __future__ import annotations

import os

from sqlalchemy import create_engine, text

SPORTS = ("baseball_mlb", "basketball_nba", "icehockey_nhl")
MARKET_TYPES = ("h2h", "totals")
CONSENSUS_KEY, CONSENSUS_VERSION = "consensus_v1", "1"
_MIN_PROB_DELTA = 0.001   # skip model_predictions insert when move is smaller


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def _sharpness_stats(conn) -> dict:
    # venue_sharpness is per (venue, sport); blend weights are global, so
    # aggregate: game-count-weighted mean brier + total games per venue.
    return {
        r["venue_key"]: {"brier": float(r["brier"]), "n_games": r["n_games"]}
        for r in conn.execute(text(
            "SELECT venue_key, "
            "       sum(brier * n_games) / sum(n_games) AS brier, "
            "       sum(n_games) AS n_games "
            "FROM venue_sharpness "
            "WHERE score_window = 'all' AND brier IS NOT NULL AND n_games > 0 "
            "GROUP BY venue_key")).mappings()
    }


def build_fair_prices() -> int:
    """Latest snapshot per (event, market, book) -> per-method fair_prices rows,
    consensus_v1 model_predictions (on-change only), and a mart_fair_odds rebuild."""
    from bountygate.models.fair import book_fair_probs, weighted_consensus
    from bountygate.models.weights import sharpness_weights
    from bountygate.transforms.marts import _latest_odds_rows

    engine = _engine()
    try:
        with engine.begin() as conn:
            events = {
                r["event_id"]: dict(r)
                for r in conn.execute(text(
                    "SELECT event_id::text AS event_id, sport_key, commence_time, "
                    "       home_team, away_team FROM sports_events "
                    "WHERE sport_key = ANY(:sports)"), {"sports": list(SPORTS)}).mappings()
            }
            weights = sharpness_weights(_sharpness_stats(conn))
            last_pred = {
                (r["event_id"], r["market_type"], r["outcome_name"]): float(r["prob"])
                for r in conn.execute(text(
                    "SELECT DISTINCT ON (event_id, market_type, outcome_name) "
                    "       event_id::text AS event_id, market_type, outcome_name, prob "
                    "FROM model_predictions WHERE model_key = :mk "
                    "ORDER BY event_id, market_type, outcome_name, predicted_at DESC"),
                    {"mk": CONSENSUS_KEY}).mappings()
            }
            conn.execute(text(
                "INSERT INTO model_versions (model_key, version, description) "
                "VALUES (:mk, :v, 'Sharpness-weighted no-vig consensus') "
                "ON CONFLICT DO NOTHING"), {"mk": CONSENSUS_KEY, "v": CONSENSUS_VERSION})

            groups: dict = {}
            for r in _latest_odds_rows(conn):
                if r["event_id"] not in events or r["market_type"] not in MARKET_TYPES:
                    continue
                groups.setdefault((r["event_id"], r["market_type"]), {}) \
                      .setdefault(r["bookmaker"], {})[r["outcome_name"]] = r["decimal_price"]

            n_rows = 0
            mart_rows = []
            for (eid, mtype), by_book in groups.items():
                names = sorted({n for prices in by_book.values() for n in prices})
                if len(names) != 2:
                    continue
                fair = book_fair_probs(by_book, names)
                for book, methods in fair.items():
                    for method, probs in methods.items():
                        for name, prob in probs.items():
                            conn.execute(text(
                                "INSERT INTO fair_prices (event_id, market_type, bookmaker, "
                                "  outcome_name, method, fair_prob, captured_at) "
                                "VALUES (cast(:eid AS uuid), :mt, :book, :name, :method, "
                                "        :prob, now())"),
                                {"eid": eid, "mt": mtype, "book": book, "name": name,
                                 "method": method, "prob": prob})
                            n_rows += 1
                cons = weighted_consensus({b: m["shin"] for b, m in fair.items()}, weights)
                if cons is None:
                    continue
                ev = events[eid]
                for name, prob in cons.items():
                    conn.execute(text(
                        "INSERT INTO fair_prices (event_id, market_type, bookmaker, "
                        "  outcome_name, method, fair_prob, captured_at) "
                        "VALUES (cast(:eid AS uuid), :mt, 'consensus', :name, 'weighted', "
                        "        :prob, now())"),
                        {"eid": eid, "mt": mtype, "name": name, "prob": prob})
                    n_rows += 1
                    prev = last_pred.get((eid, mtype, name))
                    if prev is None or abs(prob - prev) > _MIN_PROB_DELTA:
                        conn.execute(text(
                            "INSERT INTO model_predictions (model_key, version, event_id, "
                            "  market_type, outcome_name, prob, predicted_at) "
                            "VALUES (:mk, :v, cast(:eid AS uuid), :mt, :name, :prob, now())"),
                            {"mk": CONSENSUS_KEY, "v": CONSENSUS_VERSION, "eid": eid,
                             "mt": mtype, "name": name, "prob": prob})
                    best = max(
                        ((float(p[name]), b) for b, p in by_book.items() if p.get(name)),
                        default=None)
                    mart_rows.append({
                        "eid": eid, "sport": ev["sport_key"], "ct": ev["commence_time"],
                        "home": ev["home_team"], "away": ev["away_team"], "mt": mtype,
                        "name": name, "prob": prob,
                        "best_price": best[0] if best else None,
                        "best_book": best[1] if best else None,
                        "edge": (prob * best[0] - 1.0) if best else None,
                    })

            conn.execute(text("TRUNCATE mart_fair_odds"))
            for m in mart_rows:
                conn.execute(text(
                    "INSERT INTO mart_fair_odds (event_id, sport_key, commence_time, "
                    "  home_team, away_team, market_type, outcome_name, consensus_prob, "
                    "  best_price, best_bookmaker, edge, computed_at) "
                    "VALUES (cast(:eid AS uuid), :sport, :ct, :home, :away, :mt, :name, "
                    "        :prob, :best_price, :best_book, :edge, now())"), m)
        return n_rows
    finally:
        engine.dispose()
```

- [ ] **Step 2: Import smoke check**

Run: `py -3.12 -c "import sys; sys.path.insert(0, 'app/shared/python'); from bountygate.models import build_fair_prices; print('ok')"`
Expected: `ok`

- [ ] **Step 3: Verify existing tests still green (the empty __init__ became real)**

Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests -v`
Expected: all passed

- [ ] **Step 4: Commit**

```bash
git add app/shared/python/bountygate/models/__init__.py
git commit -m "feat(models): build_fair_prices builder (fair_prices + consensus_v1 + mart_fair_odds)"
```

---

### Task 8: Builder — `derive_closing_lines_db`

**Files:**
- Modify: `app/shared/python/bountygate/models/__init__.py` (append)

- [ ] **Step 1: Append the builder**

```python
def derive_closing_lines_db(*, lookback_days: int = 7) -> tuple[int, list]:
    """Derive closing lines for commenced events that have none yet.

    Returns (events_processed, stale) where stale is [(event_id, staleness_minutes)]
    for h2h consensus rows with staleness > 60 (the ingest-gap signal).
    """
    from bountygate.models.closing import derive_closing
    from bountygate.models.fair import weighted_consensus
    from bountygate.models.weights import sharpness_weights

    engine = _engine()
    stale: list = []
    n_events = 0
    try:
        with engine.begin() as conn:
            weights = sharpness_weights(_sharpness_stats(conn))
            pending = conn.execute(text(
                "SELECT e.event_id::text AS event_id, e.commence_time "
                "FROM sports_events e "
                "WHERE e.sport_key = ANY(:sports) AND e.commence_time < now() "
                "  AND e.commence_time > now() - make_interval(days => :days) "
                "  AND NOT EXISTS (SELECT 1 FROM closing_lines c "
                "                  WHERE c.event_id = e.event_id)"),
                {"sports": list(SPORTS), "days": lookback_days}).mappings().all()
            for ev in pending:
                wrote_any = False
                for mtype in MARKET_TYPES:
                    rows = [dict(r) for r in conn.execute(text(
                        "SELECT bookmaker, outcome_name, decimal_price, captured_at "
                        "FROM sportsbook_odds_history "
                        "WHERE event_id = cast(:eid AS uuid) AND market_type = :mt "
                        "  AND captured_at <= :ct"),
                        {"eid": ev["event_id"], "mt": mtype,
                         "ct": ev["commence_time"]}).mappings()]
                    closing = derive_closing(rows, ev["commence_time"])
                    if not closing:
                        continue
                    for c in closing:
                        conn.execute(text(
                            "INSERT INTO closing_lines (event_id, market_type, bookmaker, "
                            "  outcome_name, decimal_price, fair_prob, captured_at, "
                            "  staleness_minutes) "
                            "VALUES (cast(:eid AS uuid), :mt, :book, :name, :price, :fair, "
                            "        :cat, :stale) "
                            "ON CONFLICT (event_id, market_type, bookmaker, outcome_name) "
                            "DO NOTHING"),
                            {"eid": ev["event_id"], "mt": mtype, "book": c["bookmaker"],
                             "name": c["outcome_name"], "price": c["decimal_price"],
                             "fair": c["fair_prob"], "cat": c["captured_at"],
                             "stale": c["staleness_minutes"]})
                    wrote_any = True
                    # consensus closing row (the CLV reference)
                    probs_by_book: dict = {}
                    for c in closing:
                        if c["fair_prob"] is not None:
                            probs_by_book.setdefault(c["bookmaker"], {})[
                                c["outcome_name"]] = c["fair_prob"]
                    cons = weighted_consensus(probs_by_book, weights)
                    if cons:
                        # staleness/captured_at reflect only books that fed the consensus
                        contributing = [c for c in closing if c["bookmaker"] in probs_by_book]
                        worst = max(c["staleness_minutes"] for c in contributing)
                        latest = max(c["captured_at"] for c in contributing)
                        for name, prob in cons.items():
                            conn.execute(text(
                                "INSERT INTO closing_lines (event_id, market_type, "
                                "  bookmaker, outcome_name, decimal_price, fair_prob, "
                                "  captured_at, staleness_minutes) "
                                "VALUES (cast(:eid AS uuid), :mt, 'consensus', :name, "
                                "        NULL, :prob, :cat, :stale) "
                                "ON CONFLICT (event_id, market_type, bookmaker, "
                                "  outcome_name) DO NOTHING"),
                                {"eid": ev["event_id"], "mt": mtype, "name": name,
                                 "prob": prob, "cat": latest, "stale": worst})
                        if mtype == "h2h" and worst > 60:
                            stale.append((ev["event_id"], round(worst, 1)))
                if wrote_any:
                    n_events += 1
        return n_events, stale
    finally:
        engine.dispose()
```

- [ ] **Step 2: Import smoke + tests still green**

Run: `py -3.12 -c "import sys; sys.path.insert(0, 'app/shared/python'); from bountygate.models import derive_closing_lines_db; print('ok')"`
Expected: `ok`
Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests -v` → all passed

- [ ] **Step 3: Commit**

```bash
git add app/shared/python/bountygate/models/__init__.py
git commit -m "feat(models): derive_closing_lines_db builder with consensus close + gap signal"
```

---

### Task 9: Builder — `ingest_game_results`

**Files:**
- Modify: `app/shared/python/bountygate/models/__init__.py` (append)

- [ ] **Step 1: Append the builder**

```python
def ingest_game_results() -> int:
    """ESPN scoreboard finals (today + yesterday UTC) -> game_results upserts.

    winner is 'home'/'away', resolved by matching the feed game to sports_events
    via team-name+date (enrichment.match), so odds-vs-feed naming never matters.
    """
    from datetime import datetime, timedelta, timezone

    from bountygate.enrichment.clients import build_espn_scoreboard_url, fetch_json
    from bountygate.enrichment.match import match_game_to_event
    from bountygate.enrichment.results import parse_espn_scoreboard

    # all network I/O happens before the transaction opens
    today_utc = datetime.now(timezone.utc).date()
    feed_games = []
    for sport in SPORTS:
        for d in (today_utc, today_utc - timedelta(days=1)):
            payload = fetch_json(build_espn_scoreboard_url(sport, d))
            for g in parse_espn_scoreboard(payload or {}, sport):
                feed_games.append((sport, g))

    engine = _engine()
    try:
        with engine.begin() as conn:
            events = [dict(r) for r in conn.execute(text(
                "SELECT event_id::text AS bg_event_id, sport_key, "
                "       home_team AS home_team_name, away_team AS away_team_name, "
                "       commence_time AS commence_at_utc "
                "FROM sports_events "
                "WHERE sport_key = ANY(:sports) "
                "  AND commence_time > now() - interval '3 days'"),
                {"sports": list(SPORTS)}).mappings()]
            n = 0
            for sport, g in feed_games:
                if not g.get("completed"):
                    continue
                hs, as_ = g.get("home_score"), g.get("away_score")
                if hs is None or as_ is None or hs == as_:
                    continue
                eid = match_game_to_event(
                    sport, g["home_team_name"], g["away_team_name"],
                    g["commence_at_utc"], events)
                if not eid or eid == "None":
                    continue
                conn.execute(text(
                    "INSERT INTO game_results (event_id, home_score, away_score, "
                    "  winner, completed_at, source) "
                    "VALUES (cast(:eid AS uuid), :hs, :as_, :w, now(), 'espn') "
                    "ON CONFLICT (event_id) DO UPDATE SET "
                    "  home_score = EXCLUDED.home_score, "
                    "  away_score = EXCLUDED.away_score, "
                    "  winner = EXCLUDED.winner, "
                    "  completed_at = EXCLUDED.completed_at"),
                    {"eid": eid, "hs": hs, "as_": as_,
                     "w": "home" if hs > as_ else "away"})
                n += 1
            return n
    finally:
        engine.dispose()
```

(MLB StatsAPI / NHL API fallbacks from the spec are deliberately NOT wired in v1 —
ESPN covers all three sports through one parser; the fallback clients already exist
in `enrichment/clients.py` for the day ESPN proves unreliable. This is recorded as a
follow-up, not built speculatively.)

- [ ] **Step 2: Import smoke**

Run: `py -3.12 -c "import sys; sys.path.insert(0, 'app/shared/python'); from bountygate.models import ingest_game_results; print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add app/shared/python/bountygate/models/__init__.py
git commit -m "feat(models): ingest_game_results builder (ESPN finals -> game_results)"
```

---

### Task 10: Builder — `score_results_db`

**Files:**
- Modify: `app/shared/python/bountygate/models/__init__.py` (append)

- [ ] **Step 1: Append the builder**

```python
def score_results_db() -> dict:
    """Score venue closing lines and model predictions against game_results.

    Full recompute of venue_sharpness (windows: all, last_90d) and mart_calibration.
    h2h only (totals scoring needs a line column; see spec scope guards).
    """
    from datetime import datetime, timedelta, timezone

    from bountygate.analytics.clv import clv_from_fair
    from bountygate.models.scoring import brier_score, calibration_buckets, log_loss_score

    engine = _engine()
    try:
        with engine.begin() as conn:
            closing = [dict(r) for r in conn.execute(text(
                "SELECT c.event_id::text AS event_id, c.bookmaker, c.outcome_name, "
                "       c.fair_prob, e.sport_key, e.home_team, e.away_team, "
                "       e.commence_time, r.winner "
                "FROM closing_lines c "
                "JOIN sports_events e ON e.event_id = c.event_id "
                "JOIN game_results  r ON r.event_id = c.event_id "
                "WHERE c.market_type = 'h2h' AND c.fair_prob IS NOT NULL "
                "  AND r.winner IN ('home', 'away')")).mappings()]
            preds = [dict(r) for r in conn.execute(text(
                "SELECT p.model_key, p.event_id::text AS event_id, p.outcome_name, "
                "       p.prob, e.sport_key, e.home_team, e.away_team, "
                "       e.commence_time, r.winner "
                "FROM model_predictions p "
                "JOIN sports_events e ON e.event_id = p.event_id "
                "JOIN game_results  r ON r.event_id = p.event_id "
                "JOIN (SELECT model_key, version, event_id, market_type, outcome_name, "
                "             max(predicted_at) AS mx "
                "      FROM model_predictions pp "
                "      JOIN sports_events ee ON ee.event_id = pp.event_id "
                "      WHERE pp.predicted_at <= ee.commence_time "
                "      GROUP BY 1, 2, 3, 4, 5) last "
                "  ON last.model_key = p.model_key AND last.version = p.version "
                " AND last.event_id = p.event_id AND last.market_type = p.market_type "
                " AND last.outcome_name = p.outcome_name AND last.mx = p.predicted_at "
                "WHERE p.market_type = 'h2h' AND r.winner IN ('home', 'away')")).mappings()]

            def realized(row):
                if row["outcome_name"] == row["home_team"]:
                    return 1 if row["winner"] == "home" else 0
                if row["outcome_name"] == row["away_team"]:
                    return 1 if row["winner"] == "away" else 0
                return None

            cutoff_90d = datetime.now(timezone.utc) - timedelta(days=90)
            cons_close = {
                (c["event_id"], c["outcome_name"]): float(c["fair_prob"])
                for c in closing if c["bookmaker"] == "consensus"
            }

            # venue_sharpness: per (venue, sport, window); consensus excluded
            # (it is scored as a model in mart_calibration instead).
            sharp: dict = {}
            for c in closing:
                y = realized(c)
                if y is None or c["bookmaker"] == "consensus":
                    continue
                windows = ["all"] + (["last_90d"] if c["commence_time"] >= cutoff_90d else [])
                ref = cons_close.get((c["event_id"], c["outcome_name"]))
                for w in windows:
                    s = sharp.setdefault((c["bookmaker"], c["sport_key"], w),
                                         {"pairs": [], "clv": [], "events": set()})
                    s["pairs"].append((float(c["fair_prob"]), y))
                    s["events"].add(c["event_id"])
                    if ref is not None:
                        s["clv"].append(clv_from_fair(float(c["fair_prob"]), ref))

            conn.execute(text("DELETE FROM venue_sharpness"))
            for (venue, sport, w), s in sorted(sharp.items()):
                conn.execute(text(
                    "INSERT INTO venue_sharpness (venue_key, sport_key, score_window, "
                    "  n_games, brier, logloss, avg_clv, computed_at) "
                    "VALUES (:v, :sp, :w, :n, :b, :ll, :clv, now())"),
                    {"v": venue, "sp": sport, "w": w, "n": len(s["events"]),
                     "b": brier_score(s["pairs"]), "ll": log_loss_score(s["pairs"]),
                     "clv": (sum(s["clv"]) / len(s["clv"])) if s["clv"] else None})

            # mart_calibration: venues (closing) + models (pre-commence preds), per sport
            cal: dict = {}
            for c in closing:
                y = realized(c)
                if y is not None:
                    cal.setdefault((c["bookmaker"], c["sport_key"]), []).append(
                        (float(c["fair_prob"]), y))
            for p in preds:
                y = realized(p)
                if y is not None:
                    cal.setdefault((p["model_key"], p["sport_key"]), []).append(
                        (float(p["prob"]), y))

            conn.execute(text("DELETE FROM mart_calibration"))
            n_buckets = 0
            for (source, sport), pairs in sorted(cal.items()):
                for b in calibration_buckets(pairs):
                    conn.execute(text(
                        "INSERT INTO mart_calibration (source, sport_key, prob_bucket, "
                        "  n, predicted_mean, realized_rate, computed_at) "
                        "VALUES (:s, :sp, :pb, :n, :pm, :rr, now())"),
                        {"s": source, "sp": sport, "pb": b["prob_bucket"], "n": b["n"],
                         "pm": b["predicted_mean"], "rr": b["realized_rate"]})
                    n_buckets += 1
        return {"sharpness_rows": len(sharp), "calibration_rows": n_buckets}
    finally:
        engine.dispose()
```

- [ ] **Step 2: Import smoke + full models tests**

Run: `py -3.12 -c "import sys; sys.path.insert(0, 'app/shared/python'); from bountygate.models import score_results_db; print('ok')"`
Expected: `ok`
Run: `py -3.12 -m pytest app/shared/python/bountygate/models/tests -v` → all passed

- [ ] **Step 3: Commit**

```bash
git add app/shared/python/bountygate/models/__init__.py
git commit -m "feat(models): score_results_db builder (venue sharpness + calibration)"
```

---

### Task 11: Four DAGs + import smoke test

**Files:**
- Create: `airflow/dags/build_fair_odds.py`
- Create: `airflow/dags/derive_closing_lines.py`
- Create: `airflow/dags/ingest_results.py`
- Create: `airflow/dags/score_results.py`
- Test: `airflow/tests/test_quant_dags_import.py`

- [ ] **Step 1: Write the failing smoke test**

`airflow/tests/test_quant_dags_import.py` (same pattern as test_transform_dags_import.py):

```python
import importlib.util
from pathlib import Path

import pytest

DAGS = Path(__file__).parent.parent / "dags"
QUANT_DAGS = [
    "build_fair_odds.py",
    "derive_closing_lines.py",
    "ingest_results.py",
    "score_results.py",
]


@pytest.mark.parametrize("filename", QUANT_DAGS)
def test_quant_dag_imports(filename):
    path = DAGS / filename
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "dag"), f"{filename} must define a top-level `dag`"
```

- [ ] **Step 2: Run to verify failure**

Run: `py -3.12 -m pytest airflow/tests/test_quant_dags_import.py -v`
Expected: 4 FAIL — `FileNotFoundError` (dag files don't exist)

- [ ] **Step 3: Write `airflow/dags/build_fair_odds.py`**

```python
"""sportsbook snapshots -> fair_prices + consensus_v1 + mart_fair_odds."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.models import build_fair_prices

ODDS_ASSET = Asset(name="sportsbook_odds_history")


@dag(
    dag_id="build_fair_odds",
    schedule=[ODDS_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["quant", "tier1"],
)
def build_fair_odds():
    @task(outlets=[Asset(name="fair_prices")])
    def fair() -> int:
        n = build_fair_prices()
        print(f"[build_fair_odds] fair_prices rows={n}")
        return n

    fair()


dag = build_fair_odds()
```

- [ ] **Step 4: Write `airflow/dags/derive_closing_lines.py`**

```python
"""Hourly closing-line derivation (last pre-commence snapshot) + ingest-gap alert."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.models import derive_closing_lines_db


@dag(
    dag_id="derive_closing_lines",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["quant", "tier1"],
)
def derive_closing_lines():
    @task(outlets=[Asset(name="closing_lines")])
    def derive() -> int:
        n_events, stale = derive_closing_lines_db()
        print(f"[derive_closing_lines] events={n_events} stale={len(stale)}")
        if stale:
            from bountygate.utils.discord_notify import notify
            lines = ", ".join(f"{eid[:8]}…({mins}m)" for eid, mins in stale[:10])
            notify(
                f"closing-line staleness >60m on {len(stale)} event(s): {lines}",
                level="warn", source="derive_closing_lines",
            )
        return n_events

    derive()


dag = derive_closing_lines()
```

- [ ] **Step 5: Write `airflow/dags/ingest_results.py`**

```python
"""Hourly ESPN finals -> game_results."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.models import ingest_game_results


@dag(
    dag_id="ingest_results",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["quant", "ingest"],
)
def ingest_results():
    @task(outlets=[Asset(name="game_results")])
    def fetch() -> int:
        n = ingest_game_results()
        print(f"[ingest_results] upserted {n} finals")
        return n

    fetch()


dag = ingest_results()
```

- [ ] **Step 6: Write `airflow/dags/score_results.py`**

```python
"""game_results -> venue_sharpness + mart_calibration (full recompute)."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.models import score_results_db

RESULTS_ASSET = Asset(name="game_results")


@dag(
    dag_id="score_results",
    schedule=[RESULTS_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["quant", "tier1"],
)
def score_results():
    @task(outlets=[Asset(name="venue_sharpness"), Asset(name="mart_calibration")])
    def score() -> dict:
        stats = score_results_db()
        print(f"[score_results] {stats}")
        return stats

    score()


dag = score_results()
```

- [ ] **Step 7: Run smoke tests to verify they pass**

Run: `py -3.12 -m pytest airflow/tests/test_quant_dags_import.py -v`
Expected: 4 passed
Also: `py -3.12 -m pytest airflow/tests/test_transform_dags_import.py airflow/tests/test_ingest_dags_import.py -v` → still green.

- [ ] **Step 8: Commit**

```bash
git add airflow/dags/build_fair_odds.py airflow/dags/derive_closing_lines.py airflow/dags/ingest_results.py airflow/dags/score_results.py airflow/tests/test_quant_dags_import.py
git commit -m "feat(dags): quant core DAGs - fair odds, closing lines, results, scoring"
```

---

### Task 12: Router — `/fair-odds`

**Files:**
- Create: `app/web/routers/fair_odds.py`
- Modify: `app/web/main.py` (import + include_router; full edit shown in Task 14)
- Test: `app/web/tests/test_quant_endpoints.py` (new file, grows over Tasks 12–14)

- [ ] **Step 1: Write the failing test**

`app/web/tests/test_quant_endpoints.py` (helpers copied from test_endpoints.py so this
file stands alone):

```python
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.web.db import get_engine
from app.web.main import app


def _engine_with(ddl, inserts):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text(ddl))
        for ins in inserts:
            conn.execute(text(ins))
    return engine


def _use(engine):
    app.dependency_overrides[get_engine] = lambda: engine
    return TestClient(app)


_FAIR_DDL = (
    "CREATE TABLE mart_fair_odds (event_id text, sport_key text, commence_time text, "
    "home_team text, away_team text, market_type text, outcome_name text, "
    "consensus_prob real, best_price real, best_bookmaker text, edge real, "
    "computed_at text)"
)


def test_fair_odds_filters_and_orders_by_edge():
    engine = _engine_with(_FAIR_DDL, [
        "INSERT INTO mart_fair_odds (event_id, sport_key, market_type, outcome_name, "
        "consensus_prob, edge) VALUES ('e1','baseball_mlb','h2h','Yankees',0.6,0.04)",
        "INSERT INTO mart_fair_odds (event_id, sport_key, market_type, outcome_name, "
        "consensus_prob, edge) VALUES ('e2','basketball_nba','h2h','Knicks',0.5,0.09)",
    ])
    try:
        client = _use(engine)
        body = client.get("/fair-odds").json()
        assert [r["event_id"] for r in body] == ["e2", "e1"]   # edge desc
        only_mlb = client.get("/fair-odds", params={"sport": "baseball_mlb"}).json()
        assert len(only_mlb) == 1 and only_mlb[0]["event_id"] == "e1"
        assert client.get("/fair-odds", params={"market_type": "totals"}).json() == []
    finally:
        app.dependency_overrides.clear()
```

- [ ] **Step 2: Run to verify failure**

Run: `py -3.12 -m pytest app/web/tests/test_quant_endpoints.py -v`
Expected: FAIL — 404 (route not registered) after Task 14 wiring, or assertion error now.
(Registration happens in Task 14; until then this test fails — acceptable to defer the
green run to Task 14, OR wire `app.include_router(fair_odds.router)` into main.py now.
**Do the wiring now** — add the import and include_router line per Task 14's diff — so
each router task ends green.)

- [ ] **Step 3: Write `app/web/routers/fair_odds.py`**

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = ("event_id, sport_key, commence_time, home_team, away_team, market_type, "
         "outcome_name, consensus_prob, best_price, best_bookmaker, edge, computed_at")


@router.get("/fair-odds")
def list_fair_odds(
    sport: str | None = Query(None),
    market_type: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    where, params = [], {"lim": limit, "off": offset}
    if sport:
        where.append("sport_key = :sport")
        params["sport"] = sport
    if market_type:
        where.append("market_type = :mt")
        params["mt"] = market_type
    sql = f"SELECT {_COLS} FROM mart_fair_odds"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY edge DESC NULLS LAST LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
```

In `app/web/main.py`, change the routers import line and add the include:

```python
from app.web.routers import cross_market, edges, fair_odds, history, markets
...
app.include_router(fair_odds.router)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `py -3.12 -m pytest app/web/tests/test_quant_endpoints.py -v`
Expected: 1 passed
Also: `py -3.12 -m pytest app/web/tests -v` → all existing web tests still green.

- [ ] **Step 5: Commit**

```bash
git add app/web/routers/fair_odds.py app/web/main.py app/web/tests/test_quant_endpoints.py
git commit -m "feat(web): GET /fair-odds from mart_fair_odds"
```

---

### Task 13: Router — `/sharpness` + `/calibration`

**Files:**
- Create: `app/web/routers/scoring.py`
- Modify: `app/web/main.py`
- Modify: `app/web/tests/test_quant_endpoints.py` (append)

- [ ] **Step 1: Append failing tests**

```python
def test_sharpness_rows():
    engine = _engine_with(
        "CREATE TABLE venue_sharpness (venue_key text, sport_key text, score_window text, "
        "n_games integer, brier real, logloss real, avg_clv real, computed_at text)",
        ["INSERT INTO venue_sharpness (venue_key, sport_key, score_window, n_games, brier) "
         "VALUES ('pinnacle','baseball_mlb','all',250,0.21)"],
    )
    try:
        client = _use(engine)
        body = client.get("/sharpness").json()
        assert len(body) == 1 and body[0]["venue_key"] == "pinnacle"
    finally:
        app.dependency_overrides.clear()


def test_calibration_filters_by_source():
    engine = _engine_with(
        "CREATE TABLE mart_calibration (source text, sport_key text, prob_bucket real, "
        "n integer, predicted_mean real, realized_rate real, computed_at text)",
        ["INSERT INTO mart_calibration (source, sport_key, prob_bucket, n) "
         "VALUES ('consensus_v1','baseball_mlb',0.7,42)",
         "INSERT INTO mart_calibration (source, sport_key, prob_bucket, n) "
         "VALUES ('fanduel','baseball_mlb',0.7,42)"],
    )
    try:
        client = _use(engine)
        assert len(client.get("/calibration").json()) == 2
        only = client.get("/calibration", params={"source": "consensus_v1"}).json()
        assert len(only) == 1 and only[0]["source"] == "consensus_v1"
    finally:
        app.dependency_overrides.clear()
```

- [ ] **Step 2: Run to verify failure**

Run: `py -3.12 -m pytest app/web/tests/test_quant_endpoints.py -v`
Expected: 2 new FAIL (404), first test still passes.

- [ ] **Step 3: Write `app/web/routers/scoring.py`**

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_SHARP_COLS = "venue_key, sport_key, score_window, n_games, brier, logloss, avg_clv, computed_at"
_CAL_COLS = "source, sport_key, prob_bucket, n, predicted_mean, realized_rate, computed_at"


@router.get("/sharpness")
def list_sharpness(
    sport: str | None = Query(None),
    engine: Engine = Depends(get_engine),
):
    sql = f"SELECT {_SHARP_COLS} FROM venue_sharpness"
    params = {}
    if sport:
        sql += " WHERE sport_key = :sport"
        params["sport"] = sport
    sql += " ORDER BY brier ASC NULLS LAST"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


@router.get("/calibration")
def list_calibration(
    source: str | None = Query(None),
    sport: str | None = Query(None),
    engine: Engine = Depends(get_engine),
):
    where, params = [], {}
    if source:
        where.append("source = :source")
        params["source"] = source
    if sport:
        where.append("sport_key = :sport")
        params["sport"] = sport
    sql = f"SELECT {_CAL_COLS} FROM mart_calibration"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY source, sport_key, prob_bucket"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
```

In `app/web/main.py`: add `scoring` to the routers import, `app.include_router(scoring.router)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `py -3.12 -m pytest app/web/tests/test_quant_endpoints.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add app/web/routers/scoring.py app/web/main.py app/web/tests/test_quant_endpoints.py
git commit -m "feat(web): GET /sharpness + /calibration"
```

---

### Task 14: Router — `/movement/{event_id}` + `/closing-lines`

**Files:**
- Create: `app/web/routers/movement.py`
- Modify: `app/web/main.py` (final routers state shown below)
- Modify: `app/web/tests/test_quant_endpoints.py` (append)

- [ ] **Step 1: Append failing tests**

```python
_EID = "11111111-1111-1111-1111-111111111111"


def test_movement_series_and_uuid_guard():
    engine = _engine_with(
        "CREATE TABLE sportsbook_odds_history (event_id text, market_type text, "
        "bookmaker text, outcome_name text, captured_at text, decimal_price real)",
        [f"INSERT INTO sportsbook_odds_history VALUES ('{_EID}','h2h','fanduel','A',"
         f"'2026-06-10T0{i}:00:00',1.9{i})" for i in range(3)],
    )
    try:
        client = _use(engine)
        body = client.get(f"/movement/{_EID}").json()
        assert len(body) == 3
        assert body[0]["captured_at"] < body[-1]["captured_at"]   # ascending
        assert client.get("/movement/not-a-uuid").json() == []
    finally:
        app.dependency_overrides.clear()


def test_closing_lines_by_event():
    engine = _engine_with(
        "CREATE TABLE closing_lines (event_id text, market_type text, bookmaker text, "
        "outcome_name text, decimal_price real, fair_prob real, captured_at text, "
        "staleness_minutes real)",
        [f"INSERT INTO closing_lines VALUES ('{_EID}','h2h','consensus','A',NULL,0.55,"
         "'2026-06-10T18:55:00',5.0)"],
    )
    try:
        client = _use(engine)
        body = client.get("/closing-lines", params={"event_id": _EID}).json()
        assert len(body) == 1 and body[0]["fair_prob"] == 0.55
        assert client.get("/closing-lines", params={"event_id": "not-a-uuid"}).json() == []
    finally:
        app.dependency_overrides.clear()
```

- [ ] **Step 2: Run to verify failure**

Run: `py -3.12 -m pytest app/web/tests/test_quant_endpoints.py -v`
Expected: 2 new FAIL (404), prior 3 pass.

- [ ] **Step 3: Write `app/web/routers/movement.py`**

```python
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_MAX_POINTS_PER_SERIES = 500


def _valid_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


@router.get("/movement/{event_id}")
def movement(
    event_id: str,
    market_type: str | None = Query(None),
    engine: Engine = Depends(get_engine),
):
    if not _valid_uuid(event_id):
        return []
    where, params = ["event_id = :eid"], {"eid": event_id}
    if market_type:
        where.append("market_type = :mt")
        params["mt"] = market_type
    sql = ("SELECT market_type, bookmaker, outcome_name, decimal_price, captured_at "
           "FROM sportsbook_odds_history WHERE " + " AND ".join(where) +
           " ORDER BY captured_at ASC")
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(text(sql), params).mappings()]
    # downsample each (bookmaker, outcome) series to <= _MAX_POINTS_PER_SERIES
    series: dict = {}
    for r in rows:
        series.setdefault((r["market_type"], r["bookmaker"], r["outcome_name"]), []).append(r)
    out = []
    for pts in series.values():
        stride = max(1, len(pts) // _MAX_POINTS_PER_SERIES)
        kept = pts[::stride]
        if kept[-1] is not pts[-1]:
            kept.append(pts[-1])      # always keep the latest point
        out.extend(kept)
    out.sort(key=lambda r: str(r["captured_at"]))
    return out


_CLOSE_COLS = ("event_id, market_type, bookmaker, outcome_name, decimal_price, "
               "fair_prob, captured_at, staleness_minutes")


@router.get("/closing-lines")
def closing_lines(
    event_id: str = Query(...),
    engine: Engine = Depends(get_engine),
):
    if not _valid_uuid(event_id):
        return []
    sql = (f"SELECT {_CLOSE_COLS} FROM closing_lines WHERE event_id = :eid "
           "ORDER BY market_type, bookmaker, outcome_name")
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"eid": event_id}).mappings().all()
    return [dict(r) for r in rows]
```

Final `app/web/main.py` routers block:

```python
from app.web.routers import (cross_market, edges, fair_odds, history, markets,
                             movement, scoring)
...
app.include_router(markets.router)
app.include_router(edges.router)
app.include_router(cross_market.router)
app.include_router(history.router)
app.include_router(fair_odds.router)
app.include_router(scoring.router)
app.include_router(movement.router)
```

- [ ] **Step 4: Run all web tests**

Run: `py -3.12 -m pytest app/web/tests -v`
Expected: all passed (5 quant tests + all pre-existing).

- [ ] **Step 5: Commit**

```bash
git add app/web/routers/movement.py app/web/main.py app/web/tests/test_quant_endpoints.py
git commit -m "feat(web): GET /movement/{event_id} + /closing-lines"
```

---

### Task 15: Full verification + live smoke

- [ ] **Step 1: Run every test suite touched**

```bash
py -3.12 -m pytest app/shared/python/bountygate/models/tests app/web/tests airflow/tests/test_quant_dags_import.py airflow/tests/test_transform_dags_import.py airflow/tests/test_ingest_dags_import.py -v
```
Expected: all passed, zero failures.

- [ ] **Step 2: One manual builder run against the live DB**

With `DATABASE_URL` set (same env the marts builders use):

```bash
py -3.12 -c "import sys; sys.path.insert(0, 'app/shared/python'); from bountygate.models import build_fair_prices; print('fair rows:', build_fair_prices())"
py -3.12 -c "import sys; sys.path.insert(0, 'app/shared/python'); from bountygate.models import derive_closing_lines_db; print('closing:', derive_closing_lines_db())"
py -3.12 -c "import sys; sys.path.insert(0, 'app/shared/python'); from bountygate.models import ingest_game_results; print('results:', ingest_game_results())"
py -3.12 -c "import sys; sys.path.insert(0, 'app/shared/python'); from bountygate.models import score_results_db; print('scored:', score_results_db())"
```
Expected: non-zero fair rows while games are listed; closing/results counts depend on
the slate (0 is valid out-of-slate); no exceptions.

- [ ] **Step 3: Spot-check the API locally**

```bash
py -3.12 -m uvicorn app.web.main:app --port 8000
```
Then `curl http://localhost:8000/fair-odds?limit=5` → JSON rows (or `[]` pre-slate),
`curl http://localhost:8000/sharpness` → rows after the first scored game day.

- [ ] **Step 4: Deploy DAGs (local Airflow picks up airflow/dags automatically) and confirm in the UI**

The four new DAGs appear unpaused-able in the Airflow UI; trigger `build_fair_odds`
manually once and confirm fair_prices rows land.

- [ ] **Step 5: Final commit if anything moved, then done**

```bash
git status
```
Expected: clean tree (everything committed per-task).
```
