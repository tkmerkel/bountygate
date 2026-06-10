# Cross-Venue Matching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Link Kalshi/Polymarket sports markets to the same real-world game in `sports_events` (Odds API), then populate `mart_cross_market_prices` so `GET /cross-market` returns real cross-venue probability comparisons.

**Architecture:** A pure `transforms/matching/` package (version-controlled alias files + deterministic matcher) and a pure `transforms/marts/cross_market.py`, wired into the **existing** `build_marts` DAG as two new tasks (`link() >> cross_market()`). No new DAG, no new migration — `market_event_links` and `mart_cross_market_prices` already exist (migrations 005/006). Mirrors the established pure-core / thin-DB-wrapper split (`marts/*.py` pure, `marts/__init__.py` does DB I/O).

**Tech Stack:** Python 3.12, SQLAlchemy Core, the venue-agnostic `bountygate.analytics` primitives (`devig`, `consensus`), Apache Airflow 3.2 (Asset-triggered), pytest, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-06-10-cross-venue-matching-design.md`

> **Deviation from spec §4c (improvement):** the spec specified a flat ±36h match window. This plan instead reuses the frozen Kalshi repo's `dags/utils/event_match.py` windowing — **6h** when the Kalshi ticker carries an explicit time, **30h** for date-only tickers — which was written specifically to stop "cross-pollinating two different games' prices" in back-to-back series. Strictly tighter precision where we have a time; same intent. The spec's §4c window sentence is updated to match.

**Test command (matching/marts, host):** `cd app/shared/python && python -m pytest bountygate/transforms/tests -v`

---

## File Structure

| Path | Responsibility | Status |
|---|---|---|
| `app/shared/python/bountygate/transforms/matching/__init__.py` | package marker | Create |
| `app/shared/python/bountygate/transforms/matching/aliases/nfl.json` · `nba.json` · `mlb.json` | canonical team id → spellings, per league | Create |
| `app/shared/python/bountygate/transforms/matching/aliases.py` | `load_aliases`, `canonical_team`, `sport_for_series/odds`, `teams_in_title` | Create |
| `app/shared/python/bountygate/transforms/matching/event_key.py` | `parse_kalshi_external_id`, `parse_ts`, `within_window` | Create |
| `app/shared/python/bountygate/transforms/matching/match.py` | `link_rows` (pure), `polymarket_teams` | Create |
| `app/shared/python/bountygate/transforms/matching/link.py` | `link_markets()` DB wrapper | Create |
| `app/shared/python/bountygate/transforms/marts/cross_market.py` | `sportsbook_side_probs`, `assemble_rows` (pure) | Create |
| `app/shared/python/bountygate/transforms/marts/__init__.py` | add `build_cross_market()` DB wrapper | Modify |
| `app/shared/python/bountygate/transforms/tests/test_matching_*.py`, `test_cross_market.py` | unit tests | Create |
| `airflow/dags/build_marts.py` | add `link` + `cross_market` tasks | Modify |
| `airflow/tests/test_transform_dags_import.py` | DAG-import smoke for normalize + build_marts | Create |

---

## Task 1: Alias data + canonical resolution

**Files:**
- Create: `app/shared/python/bountygate/transforms/matching/__init__.py`
- Create: `app/shared/python/bountygate/transforms/matching/aliases/nfl.json`, `nba.json`, `mlb.json`
- Create: `app/shared/python/bountygate/transforms/matching/aliases.py`
- Test: `app/shared/python/bountygate/transforms/tests/test_matching_aliases.py`

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/bountygate/transforms/tests/test_matching_aliases.py`:

```python
from bountygate.transforms.matching.aliases import (
    canonical_team, load_aliases, sport_for_odds, sport_for_series, teams_in_title,
)


def test_roster_counts():
    a = load_aliases()
    assert len(a["NFL"]) == 32
    assert len(a["NBA"]) == 30
    assert len(a["MLB"]) == 30


def test_canonical_resolves_full_name_abbrev_nickname():
    assert canonical_team("Dallas Cowboys", "NFL") == "DAL"
    assert canonical_team("DAL", "NFL") == "DAL"
    assert canonical_team("Cowboys", "NFL") == "DAL"
    assert canonical_team("Los Angeles Dodgers", "MLB") == "LAD"
    assert canonical_team("LAD", "MLB") == "LAD"


def test_canonical_miss_returns_none():
    assert canonical_team("Toronto Maple Leafs", "NFL") is None
    assert canonical_team("", "NBA") is None


def test_sport_lookups():
    assert sport_for_series("KXNFLGAME") == "NFL"
    assert sport_for_odds("baseball_mlb") == "MLB"
    assert sport_for_series("KXNHLGAME") is None


def test_teams_in_title_first_mention_order():
    res = teams_in_title("Will the Cowboys beat the Giants?")
    assert res["NFL"] == ["DAL", "NYG"]


def test_teams_in_title_single_team_only():
    res = teams_in_title("Will the Celtics three-peat?")
    assert res["NBA"] == ["BOS"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests/test_matching_aliases.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms.matching'`.

- [ ] **Step 3: Create the package marker + alias files**

Create `app/shared/python/bountygate/transforms/matching/__init__.py` (empty file).

Create `app/shared/python/bountygate/transforms/matching/aliases/nfl.json` (canonical id = standard abbreviation; values include full name, nickname, and abbreviation variants seen across Kalshi tickers / Odds names):

```json
{
  "ARI": ["Arizona Cardinals", "Cardinals", "ARI"],
  "ATL": ["Atlanta Falcons", "Falcons", "ATL"],
  "BAL": ["Baltimore Ravens", "Ravens", "BAL"],
  "BUF": ["Buffalo Bills", "Bills", "BUF"],
  "CAR": ["Carolina Panthers", "Panthers", "CAR"],
  "CHI": ["Chicago Bears", "Bears", "CHI"],
  "CIN": ["Cincinnati Bengals", "Bengals", "CIN"],
  "CLE": ["Cleveland Browns", "Browns", "CLE"],
  "DAL": ["Dallas Cowboys", "Cowboys", "DAL"],
  "DEN": ["Denver Broncos", "Broncos", "DEN"],
  "DET": ["Detroit Lions", "Lions", "DET"],
  "GB": ["Green Bay Packers", "Packers", "GB", "GNB"],
  "HOU": ["Houston Texans", "Texans", "HOU"],
  "IND": ["Indianapolis Colts", "Colts", "IND"],
  "JAX": ["Jacksonville Jaguars", "Jaguars", "JAX", "JAC"],
  "KC": ["Kansas City Chiefs", "Chiefs", "KC", "KAN"],
  "LV": ["Las Vegas Raiders", "Raiders", "LV", "LVR", "OAK"],
  "LAC": ["Los Angeles Chargers", "Chargers", "LAC"],
  "LAR": ["Los Angeles Rams", "Rams", "LAR"],
  "MIA": ["Miami Dolphins", "Dolphins", "MIA"],
  "MIN": ["Minnesota Vikings", "Vikings", "MIN"],
  "NE": ["New England Patriots", "Patriots", "NE", "NWE"],
  "NO": ["New Orleans Saints", "Saints", "NO", "NOR"],
  "NYG": ["New York Giants", "Giants", "NYG"],
  "NYJ": ["New York Jets", "Jets", "NYJ"],
  "PHI": ["Philadelphia Eagles", "Eagles", "PHI"],
  "PIT": ["Pittsburgh Steelers", "Steelers", "PIT"],
  "SF": ["San Francisco 49ers", "49ers", "Niners", "SF", "SFO"],
  "SEA": ["Seattle Seahawks", "Seahawks", "SEA"],
  "TB": ["Tampa Bay Buccaneers", "Buccaneers", "Bucs", "TB", "TAM"],
  "TEN": ["Tennessee Titans", "Titans", "TEN"],
  "WAS": ["Washington Commanders", "Commanders", "WAS", "WSH"]
}
```

Create `app/shared/python/bountygate/transforms/matching/aliases/nba.json`:

```json
{
  "ATL": ["Atlanta Hawks", "Hawks", "ATL"],
  "BOS": ["Boston Celtics", "Celtics", "BOS"],
  "BKN": ["Brooklyn Nets", "Nets", "BKN", "BRK"],
  "CHA": ["Charlotte Hornets", "Hornets", "CHA"],
  "CHI": ["Chicago Bulls", "Bulls", "CHI"],
  "CLE": ["Cleveland Cavaliers", "Cavaliers", "Cavs", "CLE"],
  "DAL": ["Dallas Mavericks", "Mavericks", "Mavs", "DAL"],
  "DEN": ["Denver Nuggets", "Nuggets", "DEN"],
  "DET": ["Detroit Pistons", "Pistons", "DET"],
  "GSW": ["Golden State Warriors", "Warriors", "GSW", "GS"],
  "HOU": ["Houston Rockets", "Rockets", "HOU"],
  "IND": ["Indiana Pacers", "Pacers", "IND"],
  "LAC": ["Los Angeles Clippers", "Clippers", "LAC"],
  "LAL": ["Los Angeles Lakers", "Lakers", "LAL"],
  "MEM": ["Memphis Grizzlies", "Grizzlies", "MEM"],
  "MIA": ["Miami Heat", "Heat", "MIA"],
  "MIL": ["Milwaukee Bucks", "Bucks", "MIL"],
  "MIN": ["Minnesota Timberwolves", "Timberwolves", "Wolves", "MIN"],
  "NOP": ["New Orleans Pelicans", "Pelicans", "NOP"],
  "NYK": ["New York Knicks", "Knicks", "NYK"],
  "OKC": ["Oklahoma City Thunder", "Thunder", "OKC"],
  "ORL": ["Orlando Magic", "Magic", "ORL"],
  "PHI": ["Philadelphia 76ers", "76ers", "Sixers", "PHI"],
  "PHX": ["Phoenix Suns", "Suns", "PHX", "PHO"],
  "POR": ["Portland Trail Blazers", "Trail Blazers", "Blazers", "POR"],
  "SAC": ["Sacramento Kings", "Kings", "SAC"],
  "SAS": ["San Antonio Spurs", "Spurs", "SAS", "SA"],
  "TOR": ["Toronto Raptors", "Raptors", "TOR"],
  "UTA": ["Utah Jazz", "Jazz", "UTA", "UTAH"],
  "WAS": ["Washington Wizards", "Wizards", "WAS", "WSH"]
}
```

Create `app/shared/python/bountygate/transforms/matching/aliases/mlb.json`:

```json
{
  "ARI": ["Arizona Diamondbacks", "Diamondbacks", "Dbacks", "ARI"],
  "ATL": ["Atlanta Braves", "Braves", "ATL"],
  "BAL": ["Baltimore Orioles", "Orioles", "BAL"],
  "BOS": ["Boston Red Sox", "Red Sox", "BOS"],
  "CHC": ["Chicago Cubs", "Cubs", "CHC"],
  "CWS": ["Chicago White Sox", "White Sox", "CWS", "CHW"],
  "CIN": ["Cincinnati Reds", "Reds", "CIN"],
  "CLE": ["Cleveland Guardians", "Guardians", "CLE"],
  "COL": ["Colorado Rockies", "Rockies", "COL"],
  "DET": ["Detroit Tigers", "Tigers", "DET"],
  "HOU": ["Houston Astros", "Astros", "HOU"],
  "KC": ["Kansas City Royals", "Royals", "KC", "KCR"],
  "LAA": ["Los Angeles Angels", "Angels", "LAA", "ANA"],
  "LAD": ["Los Angeles Dodgers", "Dodgers", "LAD"],
  "MIA": ["Miami Marlins", "Marlins", "MIA"],
  "MIL": ["Milwaukee Brewers", "Brewers", "MIL"],
  "MIN": ["Minnesota Twins", "Twins", "MIN"],
  "NYM": ["New York Mets", "Mets", "NYM"],
  "NYY": ["New York Yankees", "Yankees", "NYY"],
  "ATH": ["Athletics", "Oakland Athletics", "Sacramento Athletics", "OAK", "ATH"],
  "PHI": ["Philadelphia Phillies", "Phillies", "PHI"],
  "PIT": ["Pittsburgh Pirates", "Pirates", "PIT"],
  "SD": ["San Diego Padres", "Padres", "SD", "SDP"],
  "SF": ["San Francisco Giants", "Giants", "SF", "SFG"],
  "SEA": ["Seattle Mariners", "Mariners", "SEA"],
  "STL": ["St. Louis Cardinals", "Cardinals", "STL"],
  "TB": ["Tampa Bay Rays", "Rays", "TB", "TBR"],
  "TEX": ["Texas Rangers", "Rangers", "TEX"],
  "TOR": ["Toronto Blue Jays", "Blue Jays", "TOR"],
  "WSH": ["Washington Nationals", "Nationals", "WSH", "WAS", "WSN"]
}
```

- [ ] **Step 4: Write `aliases.py`**

Create `app/shared/python/bountygate/transforms/matching/aliases.py`:

```python
"""Curated team aliases + canonical resolution. Pure; reads the version-controlled
JSON alias files once (cached). Canonical id = standard abbreviation."""
from __future__ import annotations

import json
import os
import re
from functools import lru_cache

_DIR = os.path.dirname(os.path.abspath(__file__))
_FILES = {"NFL": "nfl.json", "NBA": "nba.json", "MLB": "mlb.json"}
_SERIES_SPORT = {"KXNFLGAME": "NFL", "KXNBAGAME": "NBA", "KXMLBGAME": "MLB"}
_ODDS_SPORT = {"americanfootball_nfl": "NFL", "basketball_nba": "NBA", "baseball_mlb": "MLB"}
SPORTS = ("NFL", "NBA", "MLB")


@lru_cache(maxsize=1)
def load_aliases() -> dict:
    """{sport: {canonical_id: [aliases...]}}."""
    out = {}
    for sport, fn in _FILES.items():
        with open(os.path.join(_DIR, "aliases", fn), encoding="utf-8") as f:
            out[sport] = json.load(f)
    return out


@lru_cache(maxsize=1)
def _index() -> dict:
    """{sport: {lowered_alias: canonical_id}}."""
    idx = {}
    for sport, teams in load_aliases().items():
        m = {}
        for cid, names in teams.items():
            m[cid.lower()] = cid
            for n in names:
                m[n.lower()] = cid
        idx[sport] = m
    return idx


def canonical_team(token, sport):
    if not token or sport not in _index():
        return None
    return _index()[sport].get(str(token).strip().lower())


def sport_for_series(series_ticker):
    return _SERIES_SPORT.get(str(series_ticker or "").upper())


def sport_for_odds(sport_key):
    return _ODDS_SPORT.get(sport_key)


def teams_in_title(title) -> dict:
    """{sport: [canonical ids in first-mention order]} for teams whose alias appears
    as a whole word in the title. Aliases shorter than 3 chars (bare abbreviations)
    are skipped here to avoid false substring hits in free text; longer aliases win
    on overlap."""
    if not title:
        return {s: [] for s in SPORTS}
    low = str(title).lower()
    out = {}
    for sport, amap in _index().items():
        hits = []  # (position, -len, cid)
        for alias, cid in amap.items():
            if len(alias) < 3:
                continue
            if re.search(r"\b" + re.escape(alias) + r"\b", low):
                hits.append((low.find(alias), -len(alias), cid))
        seen, ordered = set(), []
        for _, _, cid in sorted(hits):
            if cid not in seen:
                seen.add(cid)
                ordered.append(cid)
        out[sport] = ordered
    return out
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests/test_matching_aliases.py -v`
Expected: PASS (6 passed).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/matching/__init__.py \
        app/shared/python/bountygate/transforms/matching/aliases \
        app/shared/python/bountygate/transforms/matching/aliases.py \
        app/shared/python/bountygate/transforms/tests/test_matching_aliases.py
git commit -m "feat(matching): team alias files (NFL/NBA/MLB) + canonical resolution"
```

---

## Task 2: Event-key extraction (Kalshi ticker parse + window)

**Files:**
- Create: `app/shared/python/bountygate/transforms/matching/event_key.py`
- Test: `app/shared/python/bountygate/transforms/tests/test_matching_event_key.py`

> Adapted from the frozen Kalshi repo's `dags/utils/event_match.py` (ticker regex + same-game window). The Kalshi market `external_id` is `{series}-{YYMMMDD[HHMM]}{TEAMS}-{TEAM}` (e.g. `KXMLBGAME-26MAY031415LADSTL-LAD`); sport comes from the market's `category` (= series_ticker).

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/bountygate/transforms/tests/test_matching_event_key.py`:

```python
from datetime import datetime, timezone

from bountygate.transforms.matching.event_key import (
    parse_kalshi_external_id, parse_ts, within_window,
)


def test_parse_kalshi_with_time():
    p = parse_kalshi_external_id("KXMLBGAME-26MAY031415LADSTL-LAD", "KXMLBGAME")
    assert p["sport"] == "MLB"
    assert p["team"] == "LAD"
    assert p["opponent"] == "STL"
    assert p["has_time"] is True
    assert p["dt"] == datetime(2026, 5, 3, 14, 15, tzinfo=timezone.utc)


def test_parse_kalshi_date_only():
    p = parse_kalshi_external_id("KXNBAGAME-26APR30NYKATL-NYK", "KXNBAGAME")
    assert p["sport"] == "NBA"
    assert {p["team"], p["opponent"]} == {"NYK", "ATL"}
    assert p["has_time"] is False
    assert p["dt"] == datetime(2026, 4, 30, tzinfo=timezone.utc)


def test_parse_kalshi_unknown_series_returns_none():
    assert parse_kalshi_external_id("KXNHLGAME-26MAY01XYZABC-XYZ", "KXNHLGAME") is None


def test_within_window_tight_when_time():
    g = datetime(2026, 5, 3, 14, 15, tzinfo=timezone.utc)
    assert within_window(g, True, datetime(2026, 5, 3, 18, 0, tzinfo=timezone.utc))      # ~3.75h
    assert not within_window(g, True, datetime(2026, 5, 4, 0, 0, tzinfo=timezone.utc))   # ~9.75h


def test_within_window_wide_when_date_only():
    g = datetime(2026, 4, 30, tzinfo=timezone.utc)
    assert within_window(g, False, datetime(2026, 4, 30, 23, 30, tzinfo=timezone.utc))   # 23.5h


def test_parse_ts_handles_z_and_datetime():
    assert parse_ts("2026-05-03T14:15:00Z") == datetime(2026, 5, 3, 14, 15, tzinfo=timezone.utc)
    assert parse_ts(datetime(2026, 5, 3, tzinfo=timezone.utc)).year == 2026
    assert parse_ts(None) is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests/test_matching_event_key.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms.matching.event_key'`.

- [ ] **Step 3: Write `event_key.py`**

Create `app/shared/python/bountygate/transforms/matching/event_key.py`:

```python
"""Extract sport / date / teams from normalized market identifiers.
Adapted from the frozen Kalshi repo's dags/utils/event_match.py (the windowing that
fixed the 'cross-pollinating two different games' prices' bug in back-to-back series)."""
from __future__ import annotations

import re
from datetime import datetime, timezone

from bountygate.transforms.matching.aliases import canonical_team, sport_for_series

_MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
           "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
# {series}-{YY}{MON}{DD}{optional HHMM}; the rest of the id holds the teams.
_TICKER_RE = re.compile(r"^[A-Z]+-(\d{2})([A-Z]{3})(\d{2})(\d{4})?")

MAX_HOURS_WITH_TIME = 6
MAX_HOURS_DATE_ONLY = 30


def parse_ts(ts):
    """ISO string (tolerating trailing 'Z') or datetime -> aware UTC datetime, or None."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def parse_kalshi_external_id(external_id, category):
    """Normalized Kalshi market id + category(series_ticker) -> dict or None.
    Returns {sport, dt, has_time, team, opponent} with team/opponent canonicalized."""
    sport = sport_for_series(category)
    if sport is None or not external_id:
        return None
    m = _TICKER_RE.match(external_id)
    if not m:
        return None
    yy, mon3, dd, hhmm = m.group(1), m.group(2), m.group(3), m.group(4)
    month = _MONTHS.get(mon3)
    if month is None:
        return None
    try:
        year, day = 2000 + int(yy), int(dd)
        if hhmm:
            dt = datetime(year, month, day, int(hhmm[:2]), int(hhmm[2:]), tzinfo=timezone.utc)
            has_time = True
        else:
            dt = datetime(year, month, day, tzinfo=timezone.utc)
            has_time = False
    except ValueError:
        return None
    rest = external_id[m.end():]            # e.g. 'LADSTL-LAD'
    pair, _, suffix = rest.partition("-")   # pair='LADSTL', suffix='LAD'
    if not pair or not suffix:
        return None
    if pair.startswith(suffix):
        opp_tok = pair[len(suffix):]
    elif pair.endswith(suffix):
        opp_tok = pair[:-len(suffix)]
    else:
        return None
    team = canonical_team(suffix, sport)
    opponent = canonical_team(opp_tok, sport)
    if team is None or opponent is None:
        return None
    return {"sport": sport, "dt": dt, "has_time": has_time, "team": team, "opponent": opponent}


def within_window(market_dt, has_time, event_dt) -> bool:
    """True iff the market time is within the same-game window of the event commence time."""
    if market_dt is None or event_dt is None:
        return False
    limit = MAX_HOURS_WITH_TIME if has_time else MAX_HOURS_DATE_ONLY
    return abs((market_dt - event_dt).total_seconds()) / 3600.0 <= limit
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests/test_matching_event_key.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/matching/event_key.py \
        app/shared/python/bountygate/transforms/tests/test_matching_event_key.py
git commit -m "feat(matching): Kalshi ticker parse + same-game window (from event_match.py)"
```

---

## Task 3: The pure matcher — `link_rows`

**Files:**
- Create: `app/shared/python/bountygate/transforms/matching/match.py`
- Test: `app/shared/python/bountygate/transforms/tests/test_matching_match.py`

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/bountygate/transforms/tests/test_matching_match.py`:

```python
from bountygate.transforms.matching.match import link_rows

EVENTS = [
    {"event_id": "E1", "sport_key": "baseball_mlb",
     "commence_time": "2026-05-03T14:30:00Z",
     "home_team": "St. Louis Cardinals", "away_team": "Los Angeles Dodgers"},
]


def test_kalshi_links_to_event():
    markets = [{"market_id": "M1", "venue_key": "kalshi",
                "external_id": "KXMLBGAME-26MAY031415LADSTL-LAD",
                "title": "Will the Dodgers win?", "category": "KXMLBGAME",
                "close_time": None}]
    r = link_rows(markets, EVENTS)
    assert r["links"] == [{"market_id": "M1", "event_id": "E1",
                           "confidence": 1.0, "method": "kalshi_ticker"}]
    assert r["stats"]["kalshi"] == 1


def test_polymarket_links_via_title():
    markets = [{"market_id": "M2", "venue_key": "polymarket",
                "external_id": "0xabc", "title": "Will the Dodgers beat the Cardinals?",
                "category": None, "close_time": "2026-05-03T23:59:00Z"}]
    r = link_rows(markets, EVENTS)
    assert len(r["links"]) == 1
    assert r["links"][0]["method"] == "polymarket_text"
    assert r["stats"]["polymarket"] == 1


def test_out_of_window_does_not_link():
    markets = [{"market_id": "M3", "venue_key": "kalshi",
                "external_id": "KXMLBGAME-26MAY051415LADSTL-LAD",  # May 5; event is May 3
                "title": "", "category": "KXMLBGAME", "close_time": None}]
    r = link_rows(markets, EVENTS)
    assert r["links"] == []
    assert r["stats"]["unmatched"] == 1


def test_ambiguous_two_events_same_teams_in_window():
    events = EVENTS + [{"event_id": "E2", "sport_key": "baseball_mlb",
                        "commence_time": "2026-05-03T20:00:00Z",
                        "home_team": "St. Louis Cardinals", "away_team": "Los Angeles Dodgers"}]
    markets = [{"market_id": "M4", "venue_key": "kalshi",
                "external_id": "KXMLBGAME-26MAY031415LADSTL-LAD",
                "title": "", "category": "KXMLBGAME", "close_time": None}]
    r = link_rows(markets, events)
    assert r["links"] == []
    assert r["stats"]["ambiguous"] == 1


def test_non_sports_market_unmatched():
    markets = [{"market_id": "M5", "venue_key": "polymarket",
                "external_id": "0xff", "title": "Will it rain in NYC tomorrow?",
                "category": None, "close_time": "2026-05-03T23:59:00Z"}]
    r = link_rows(markets, EVENTS)
    assert r["links"] == []
    assert r["stats"]["unmatched"] == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests/test_matching_match.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms.matching.match'`.

- [ ] **Step 3: Write `match.py`**

Create `app/shared/python/bountygate/transforms/matching/match.py`:

```python
"""Pure cross-venue matcher: normalized markets + sports_events -> link rows + stats.
Deterministic, alias-driven, precision-first (ambiguous/out-of-window candidates are
rejected, never guessed)."""
from __future__ import annotations

from bountygate.transforms.matching.aliases import (
    canonical_team, sport_for_odds, teams_in_title,
)
from bountygate.transforms.matching.event_key import (
    parse_kalshi_external_id, parse_ts, within_window,
)


def _index_events(events):
    out = []
    for e in events:
        sport = sport_for_odds(e.get("sport_key"))
        if sport is None:
            continue
        home = canonical_team(e.get("home_team"), sport)
        away = canonical_team(e.get("away_team"), sport)
        dt = parse_ts(e.get("commence_time"))
        if home and away and dt:
            out.append({"event_id": e["event_id"], "sport": sport,
                        "teams": frozenset((home, away)), "dt": dt})
    return out


def polymarket_teams(title):
    """(sport, frozenset(two canonical teams), subject) or None.
    subject = the first team mentioned in the title (the 'Yes' side)."""
    for sport, teams in teams_in_title(title).items():
        if len(teams) == 2:
            return sport, frozenset(teams), teams[0]
    return None


def link_rows(markets, events):
    """markets: dicts with market_id, venue_key, external_id, title, category, close_time.
    events: dicts with event_id, sport_key, commence_time, home_team, away_team.
    Returns {'links': [...], 'stats': {...}}."""
    idx = _index_events(events)
    links = []
    stats = {"kalshi": 0, "polymarket": 0, "ambiguous": 0, "unmatched": 0}
    for m in markets:
        venue = m.get("venue_key")
        if venue == "kalshi":
            p = parse_kalshi_external_id(m.get("external_id"), m.get("category"))
            if not p:
                stats["unmatched"] += 1
                continue
            sport = p["sport"]
            teams = frozenset((p["team"], p["opponent"]))
            mdt, has_time = p["dt"], p["has_time"]
            method = "kalshi_ticker"
        elif venue == "polymarket":
            r = polymarket_teams(m.get("title"))
            mdt = parse_ts(m.get("close_time"))
            if not r or mdt is None:
                stats["unmatched"] += 1
                continue
            sport, teams, _subject = r
            has_time = False
            method = "polymarket_text"
        else:
            continue
        cands = [e for e in idx
                 if e["sport"] == sport and e["teams"] == teams
                 and within_window(mdt, has_time, e["dt"])]
        if len(cands) == 1:
            links.append({"market_id": m["market_id"], "event_id": cands[0]["event_id"],
                          "confidence": 1.0, "method": method})
            stats[venue] += 1
        elif len(cands) > 1:
            stats["ambiguous"] += 1
        else:
            stats["unmatched"] += 1
    return {"links": links, "stats": stats}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests/test_matching_match.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/matching/match.py \
        app/shared/python/bountygate/transforms/tests/test_matching_match.py
git commit -m "feat(matching): pure link_rows matcher (deterministic, precision-first)"
```

---

## Task 4: `link_markets()` DB wrapper

**Files:**
- Create: `app/shared/python/bountygate/transforms/matching/link.py`

> No unit test: this is thin DB I/O (Postgres `TRUNCATE` + uuid casts), verified end-to-end in Task 8. This mirrors `marts/__init__.py:build_edge_signals` (the pure `compute_*` is tested; the DB wrapper is verified by the live run). Re-derives every run: `TRUNCATE` then insert fresh links.

- [ ] **Step 1: Write `link.py`**

Create `app/shared/python/bountygate/transforms/matching/link.py`:

```python
"""normalized markets + sports_events -> market_event_links. Re-derives every run
(TRUNCATE + insert). The pure matching lives in match.py."""
from __future__ import annotations

import os

from sqlalchemy import create_engine, text

from bountygate.transforms.matching.match import link_rows


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def link_markets() -> dict:
    engine = _engine()
    try:
        with engine.begin() as conn:
            markets = conn.execute(text(
                "SELECT market_id::text AS market_id, venue_key, external_id, title, category, "
                "       close_time::text AS close_time FROM markets "
                "WHERE venue_key IN ('kalshi', 'polymarket')")).mappings().all()
            events = conn.execute(text(
                "SELECT event_id::text AS event_id, sport_key, "
                "       commence_time::text AS commence_time, home_team, away_team "
                "FROM sports_events")).mappings().all()
            result = link_rows([dict(m) for m in markets], [dict(e) for e in events])
            conn.execute(text("TRUNCATE market_event_links"))
            for ln in result["links"]:
                conn.execute(text(
                    "INSERT INTO market_event_links (market_id, event_id, confidence, method) "
                    "VALUES (cast(:market_id AS uuid), cast(:event_id AS uuid), :confidence, :method)"),
                    ln)
        print(f"[link_markets] {result['stats']}")
        return result["stats"]
    finally:
        engine.dispose()
```

- [ ] **Step 2: Verify it imports (no DB needed)**

Run: `cd app/shared/python && python -c "from bountygate.transforms.matching.link import link_markets; print('ok')"`
Expected: prints `ok` (import resolves; no DB call at import time).

- [ ] **Step 3: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/matching/link.py
git commit -m "feat(matching): link_markets() DB wrapper (re-derive market_event_links)"
```

---

## Task 5: Pure cross-market assembly — `cross_market.py`

**Files:**
- Create: `app/shared/python/bountygate/transforms/marts/cross_market.py`
- Test: `app/shared/python/bountygate/transforms/tests/test_cross_market.py`

- [ ] **Step 1: Write the failing test**

Create `app/shared/python/bountygate/transforms/tests/test_cross_market.py`:

```python
from datetime import datetime, timezone

from bountygate.transforms.marts.cross_market import assemble_rows, sportsbook_side_probs


def test_sportsbook_side_probs_pinnacle_devig():
    by_book = {"pinnacle": {"LAD": 1.5, "STL": 2.6}}
    p = sportsbook_side_probs(by_book, "LAD", "STL")
    assert round(p["LAD"] + p["STL"], 6) == 1.0
    assert p["LAD"] > p["STL"]


def test_sportsbook_side_probs_consensus_fallback():
    by_book = {"fanduel": {"LAD": 1.5, "STL": 2.6}, "draftkings": {"LAD": 1.52, "STL": 2.55}}
    p = sportsbook_side_probs(by_book, "LAD", "STL")
    assert p is not None and 0 < p["LAD"] < 1


def test_sportsbook_side_probs_none_when_no_two_way():
    assert sportsbook_side_probs({"fanduel": {"LAD": 1.5}}, "LAD", "STL") is None


def test_assemble_two_rows_per_game_and_spread():
    games = [{
        "sport": "MLB", "date": datetime(2026, 5, 3, tzinfo=timezone.utc),
        "home": "STL", "away": "LAD",
        "sportsbook": {"LAD": 0.60, "STL": 0.40},
        "kalshi": {"LAD": 0.58},
        "polymarket": {"LAD": 0.62, "STL": 0.38},
    }]
    by_key = {r["question_key"]: r for r in assemble_rows(games)}
    lad = by_key["mlb:2026-05-03:LAD@STL:LAD"]
    assert lad["kalshi_prob"] == 0.58
    assert lad["polymarket_prob"] == 0.62
    assert lad["sportsbook_consensus_prob"] == 0.60
    assert round(lad["max_spread"], 2) == 0.04  # 0.62 - 0.58
    stl = by_key["mlb:2026-05-03:LAD@STL:STL"]
    assert stl["kalshi_prob"] is None           # only the LAD-side Kalshi market was linked
    assert round(stl["max_spread"], 2) == 0.02  # 0.40 - 0.38


def test_one_venue_side_dropped():
    games = [{
        "sport": "NBA", "date": datetime(2026, 6, 9, tzinfo=timezone.utc),
        "home": "BOS", "away": "NYK",
        "sportsbook": {}, "kalshi": {"BOS": 0.7}, "polymarket": {},
    }]
    assert assemble_rows(games) == []
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests/test_cross_market.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bountygate.transforms.marts.cross_market'`.

- [ ] **Step 3: Write `cross_market.py`**

Create `app/shared/python/bountygate/transforms/marts/cross_market.py`:

```python
"""Pure assembly of mart_cross_market_prices rows from per-game venue probabilities.
Reuses the venue-agnostic analytics primitives for the sportsbook consensus."""
from __future__ import annotations

from bountygate.analytics.consensus import no_vig_consensus
from bountygate.analytics.devig import implied_prob, multiplicative_devig

PINNACLE = "pinnacle"


def sportsbook_side_probs(by_book, home, away):
    """by_book: {bookmaker: {canonical_side: decimal_price}} (sides already canonicalized
    to home/away). Fair P(side) via Pinnacle no-vig, else multi-book consensus.
    Returns {home: prob, away: prob} or None."""
    pin = by_book.get(PINNACLE)
    if pin and pin.get(home) and pin.get(away):
        fh, fa = multiplicative_devig(implied_prob(pin[home]), implied_prob(pin[away]))
        return {home: fh, away: fa}
    over, under = [], []
    for book, prices in by_book.items():
        if book == PINNACLE:
            continue
        over.append(prices.get(home))
        under.append(prices.get(away))
    c = no_vig_consensus(over, under)
    if c is None:
        return None
    return {home: c[0], away: c[1]}


def assemble_rows(games):
    """games: dicts with sport, date(datetime), home, away (canonical ids), and optional
    sportsbook/kalshi/polymarket dicts {canonical_side: prob}. Emits up to two rows per
    game (one per winning side); a side is kept only if >=2 venues have a prob."""
    rows = []
    for g in games:
        home, away = g["home"], g["away"]
        sb = g.get("sportsbook") or {}
        ka = g.get("kalshi") or {}
        pm = g.get("polymarket") or {}
        date = g["date"].strftime("%Y-%m-%d")
        sport = g["sport"].lower()
        for side in (home, away):
            k, p, s = ka.get(side), pm.get(side), sb.get(side)
            present = [v for v in (k, p, s) if v is not None]
            if len(present) < 2:
                continue
            rows.append({
                "question_key": f"{sport}:{date}:{away}@{home}:{side}",
                "kalshi_prob": k,
                "polymarket_prob": p,
                "sportsbook_consensus_prob": s,
                "max_spread": max(present) - min(present),
            })
    return rows
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests/test_cross_market.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/marts/cross_market.py \
        app/shared/python/bountygate/transforms/tests/test_cross_market.py
git commit -m "feat(marts): pure cross-market assembly (side probs + >=2-venue gate)"
```

---

## Task 6: `build_cross_market()` DB wrapper

**Files:**
- Modify: `app/shared/python/bountygate/transforms/marts/__init__.py`

> No unit test (DB I/O verified in Task 8), consistent with the existing `build_edge_signals`/`build_market_history` wrappers. This reads events + links + latest history, canonicalizes outcome names to sides, computes the sportsbook consensus, aligns each venue's prob per side, then `TRUNCATE`s + inserts via the pure `assemble_rows`.

- [ ] **Step 1: Append `build_cross_market()` to `marts/__init__.py`**

Add to the end of `app/shared/python/bountygate/transforms/marts/__init__.py`:

```python
def build_cross_market() -> int:
    from bountygate.transforms.matching.aliases import canonical_team, sport_for_odds
    from bountygate.transforms.matching.event_key import parse_kalshi_external_id
    from bountygate.transforms.matching.match import polymarket_teams
    from bountygate.transforms.marts.cross_market import assemble_rows, sportsbook_side_probs

    engine = _engine()
    try:
        with engine.begin() as conn:
            # events -> game scaffolds keyed by event_id, canonical home/away
            games = {}
            for e in conn.execute(text(
                "SELECT event_id::text AS event_id, sport_key, commence_time, "
                "       home_team, away_team FROM sports_events")).mappings().all():
                sport = sport_for_odds(e["sport_key"])
                if not sport:
                    continue
                home = canonical_team(e["home_team"], sport)
                away = canonical_team(e["away_team"], sport)
                if not (home and away and e["commence_time"]):
                    continue
                games[e["event_id"]] = {
                    "sport": sport, "date": e["commence_time"], "home": home, "away": away,
                    "sportsbook": {}, "kalshi": {}, "polymarket": {},
                }

            # sportsbook consensus: latest h2h decimal price per (event, book, side)
            by_event_book = {}      # event_id -> {book: {side: price}}
            latest_at = {}          # (event_id, book, side) -> captured_at
            for o in conn.execute(text(
                "SELECT event_id::text AS event_id, bookmaker, outcome_name, "
                "       decimal_price, captured_at FROM sportsbook_odds_history "
                "WHERE market_type = 'h2h'")).mappings().all():
                g = games.get(o["event_id"])
                if not g:
                    continue
                side = canonical_team(o["outcome_name"], g["sport"])
                if side is None:
                    continue
                key = (o["event_id"], o["bookmaker"], side)
                if key in latest_at and o["captured_at"] <= latest_at[key]:
                    continue
                latest_at[key] = o["captured_at"]
                by_event_book.setdefault(o["event_id"], {}).setdefault(
                    o["bookmaker"], {})[side] = float(o["decimal_price"])
            for eid, by_book in by_event_book.items():
                probs = sportsbook_side_probs(by_book, games[eid]["home"], games[eid]["away"])
                if probs:
                    games[eid]["sportsbook"] = probs

            # linked prediction markets -> venue prob per side
            for ln in conn.execute(text(
                "SELECT l.event_id::text AS event_id, m.venue_key, m.external_id, m.title, "
                "       m.category, m.market_id::text AS market_id "
                "FROM market_event_links l JOIN markets m ON m.market_id = l.market_id")).mappings().all():
                g = games.get(ln["event_id"])
                if not g:
                    continue
                if ln["venue_key"] == "kalshi":
                    p = parse_kalshi_external_id(ln["external_id"], ln["category"])
                    if not p or p["team"] not in (g["home"], g["away"]):
                        continue
                    yes = conn.execute(text(
                        "SELECT last_price FROM market_outcomes "
                        "WHERE market_id = cast(:mid AS uuid) AND outcome_name = 'Yes'"),
                        {"mid": ln["market_id"]}).scalar()
                    if yes is not None:
                        g["kalshi"][p["team"]] = float(yes)
                elif ln["venue_key"] == "polymarket":
                    r = polymarket_teams(ln["title"])
                    if not r:
                        continue
                    _sport, teams, subject = r
                    opponent = next(iter(teams - {subject})) if subject in teams else None
                    for o in conn.execute(text(
                        "SELECT outcome_name, last_price FROM market_outcomes "
                        "WHERE market_id = cast(:mid AS uuid)"),
                        {"mid": ln["market_id"]}).mappings().all():
                        if o["last_price"] is None:
                            continue
                        nm = str(o["outcome_name"]).strip().lower()
                        named = canonical_team(o["outcome_name"], g["sport"])
                        if named in (g["home"], g["away"]):
                            g["polymarket"][named] = float(o["last_price"])
                        elif nm == "yes" and subject in (g["home"], g["away"]):
                            g["polymarket"][subject] = float(o["last_price"])
                        elif nm == "no" and opponent in (g["home"], g["away"]):
                            g["polymarket"][opponent] = float(o["last_price"])

            rows = assemble_rows(list(games.values()))
            conn.execute(text("TRUNCATE mart_cross_market_prices"))
            for r in rows:
                conn.execute(text(
                    "INSERT INTO mart_cross_market_prices "
                    "  (question_key, captured_at, kalshi_prob, polymarket_prob, "
                    "   sportsbook_consensus_prob, max_spread) "
                    "VALUES (:question_key, now(), :kalshi_prob, :polymarket_prob, "
                    "        :sportsbook_consensus_prob, :max_spread)"), r)
        return len(rows)
    finally:
        engine.dispose()
```

- [ ] **Step 2: Verify it imports**

Run: `cd app/shared/python && python -c "from bountygate.transforms.marts import build_cross_market; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Run the full transforms suite (no regressions)**

Run: `cd app/shared/python && python -m pytest bountygate/transforms/tests -v`
Expected: all green — the existing parser/edge/history tests plus the new matching/cross-market tests (22 new: aliases 6, event_key 6, match 5, cross_market 5).

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/shared/python/bountygate/transforms/marts/__init__.py
git commit -m "feat(marts): build_cross_market() DB wrapper (links+history -> mart)"
```

---

## Task 7: Wire `build_marts` DAG + DAG-import smoke test

**Files:**
- Modify: `airflow/dags/build_marts.py`
- Create: `airflow/tests/test_transform_dags_import.py`

- [ ] **Step 1: Write the failing DAG-import smoke test**

Create `airflow/tests/test_transform_dags_import.py`:

```python
import importlib.util
from pathlib import Path

import pytest

DAGS = Path(__file__).parent.parent / "dags"
TRANSFORM_DAGS = ["normalize.py", "build_marts.py"]


@pytest.mark.parametrize("filename", TRANSFORM_DAGS)
def test_transform_dag_imports(filename):
    path = DAGS / filename
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "dag"), f"{filename} must define a top-level `dag`"
```

- [ ] **Step 2: Add the two tasks to `build_marts.py`**

In `airflow/dags/build_marts.py`, update the imports block to add the two new builders:

```python
from bountygate.transforms.marts import build_cross_market, build_edge_signals, build_market_history
from bountygate.transforms.matching.link import link_markets
```

Then, inside the `build_marts()` function body, add the two tasks and wire them (the existing `edges()`/`history()` calls stay):

```python
    @task(outlets=[Asset(name="market_event_links")])
    def link() -> dict:
        stats = link_markets()
        print(f"[build_marts] links {stats}")
        return stats

    @task(outlets=[Asset(name="mart_cross_market_prices")])
    def cross_market() -> int:
        n = build_cross_market()
        print(f"[build_marts] cross_market rows={n}")
        return n

    edges()
    history()
    link() >> cross_market()
```

(The original `edges()` / `history()` lines are replaced by the block above — they remain present, with `link() >> cross_market()` added after them.)

- [ ] **Step 3: Run the smoke test inside a container** (the host lacks `airflow.sdk`)

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm -v "$(pwd)/tests:/opt/airflow/project_tests" airflow-scheduler \
  python -m pytest /opt/airflow/project_tests/test_transform_dags_import.py -v 2>&1 | tail -20
```
Expected: 2 passed (`normalize`, `build_marts` both import and expose a top-level `dag`).

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add airflow/dags/build_marts.py airflow/tests/test_transform_dags_import.py
git commit -m "feat(dags): build_marts link() >> cross_market() + transform DAG import smoke"
```

---

## Task 8: End-to-end verification

**Files:** none (verification only).

- [ ] **Step 1: Rebuild the Airflow image so the new package + alias files are in the container**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose build 2>&1 | tail -8
docker compose up -d 2>&1 | tail -8
```
Expected: build succeeds; containers up.

- [ ] **Step 2: Trigger one `build_marts` run and watch the link stats**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose run --rm airflow-scheduler airflow dags test build_marts 2>&1 | tail -25
```
Expected: state `success`; logs include a `[link_markets] {...}` line with non-zero `kalshi`/`polymarket` for in-season leagues (MLB/NBA in June) and a `[build_marts] cross_market rows=N` line. If links are all zero while `markets`/`sports_events` are populated, that is a match-rate regression to investigate (e.g. an abbreviation not in the alias files) — not "no data".

- [ ] **Step 3: Verify rows landed** (Docker psql — the Heroku pg CLI is broken, per project memory)

```bash
cd /c/Users/tkmer/bountygate
export PGURL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
docker run --rm -e PGURL="$PGURL" postgres:16 psql "$PGURL" -tA -c \
  "SELECT count(*) AS links FROM market_event_links;
   SELECT count(*) AS xmkt_rows FROM mart_cross_market_prices;
   SELECT question_key, kalshi_prob, polymarket_prob, sportsbook_consensus_prob, max_spread
   FROM mart_cross_market_prices ORDER BY max_spread DESC NULLS LAST LIMIT 5;" 2>&1
```
Expected: `links` > 0 and `xmkt_rows` > 0 for in-season leagues; the sample rows show ≥2 non-null probs each and a `question_key` like `mlb:2026-06-09:LAD@SF:LAD`. (Out of season for all leagues, 0 is acceptable only if `markets`/`sports_events` are themselves empty — confirm those first.)

- [ ] **Step 4: Confirm the API now returns real rows**

```bash
cd /c/Users/tkmer/bountygate/app/web && python -m pytest tests -q 2>&1 | tail -5
```
Then hit the live endpoint (web dyno or local uvicorn):
```bash
curl -s "http://localhost:8000/cross-market?limit=5" 2>&1 | head -40
```
Expected: web tests green; `/cross-market` returns a non-empty JSON array whose objects carry `question_key`, the three prob fields, and `max_spread`.

- [ ] **Step 5: Full transforms suite green on host**

```bash
cd /c/Users/tkmer/bountygate/app/shared/python && python -m pytest bountygate/transforms/tests -q 2>&1 | tail -3
```
Expected: all passed (existing + 22 new).

- [ ] **Step 6: Report completion**

Summarize against the spec's §9 success criteria: `transforms/matching/` exists (alias files + pure `link_rows` + `link_markets()` with match-rate logging); `cross_market.py` assembles per-(game, side) rows with aligned probs, `max_spread`, and the ≥2-venue gate; `build_marts` runs `link() >> cross_market()` and emits the two new Assets; unit tests green; after a live run `mart_cross_market_prices` is populated and `GET /cross-market` returns real rows; ambiguous/out-of-window candidates were rejected, not guessed.
