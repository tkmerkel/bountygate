"""Box-score stat extraction + prop settlement.

Two responsibilities:

  * ``settle(stat_value, side, line)`` grades an over/under prop into
    win / loss / push / unknown.
  * ``extract_*_player_stats(boxscore)`` flattens a provider box score into
    rows shaped for fact_player_stat_result, keyed by the BASE market_key
    vocabulary (batter_hits, pitcher_strikeouts, player_goals, …) so a row's
    ``stat_key`` joins directly to an EV pick's ``base_market_key``.

MLB (StatsAPI) carries full player names → reliable joins to dim_player.
NHL (api-web.nhle.com) abbreviates first names ("N. Roy"), so NHL rows are
best-effort for name-based joins; the raw nhl playerId is carried in
``source_player_id`` for a future id-based join once dim_team/dim_player ids
are populated.
"""
from __future__ import annotations

from typing import List, Optional

from bountygate.enrichment import match

MLB_SPORT_KEY = "baseball_mlb"
NHL_SPORT_KEY = "icehockey_nhl"


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------
def settle(stat_value, side, line) -> str:
    """Grade an over/under prop.

    Returns 'win' | 'loss' | 'push' | 'unknown'. 'unknown' when the value,
    line, or side is missing/unsupported (push only on an exact whole-line tie).
    """
    if stat_value is None or line is None:
        return "unknown"
    try:
        value = float(stat_value)
        line_f = float(line)
    except (TypeError, ValueError):
        return "unknown"

    s = str(side).strip().lower()
    if s == "over":
        if value > line_f:
            return "win"
        if value < line_f:
            return "loss"
        return "push"
    if s == "under":
        if value < line_f:
            return "win"
        if value > line_f:
            return "loss"
        return "push"
    return "unknown"


def _num(d: dict, key: str) -> Optional[float]:
    v = d.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _row(sport_key: str, name, stat_key: str, value, source_player_id=None) -> dict:
    normalized = match.normalize_player_name(name)
    return {
        "player_name": name,
        "normalized_name": normalized,
        "player_id": match.player_id(sport_key, normalized),
        "sport_key": sport_key,
        "stat_key": stat_key,
        "stat_value": value,
        "source_player_id": source_player_id,
    }


# ---------------------------------------------------------------------------
# MLB — statsapi.mlb.com boxscore
# ---------------------------------------------------------------------------
def extract_mlb_player_stats(boxscore: dict, sport_key: str = MLB_SPORT_KEY) -> List[dict]:
    """Flatten an MLB StatsAPI boxscore into per-player stat rows."""
    rows: List[dict] = []
    teams = (boxscore or {}).get("teams", {}) or {}
    for side_key in ("away", "home"):
        players = (teams.get(side_key, {}) or {}).get("players", {}) or {}
        for pdata in players.values():
            person = pdata.get("person", {}) or {}
            name = person.get("fullName")
            pid = person.get("id")
            if not name:
                continue
            stats = pdata.get("stats", {}) or {}
            rows.extend(_mlb_batting_rows(sport_key, name, pid, stats.get("batting") or {}))
            rows.extend(_mlb_pitching_rows(sport_key, name, pid, stats.get("pitching") or {}))
    return rows


def _mlb_batting_rows(sport_key, name, pid, bat: dict) -> List[dict]:
    if not bat:
        return []
    hits = _num(bat, "hits")
    doubles = _num(bat, "doubles")
    triples = _num(bat, "triples")
    home_runs = _num(bat, "homeRuns")
    runs = _num(bat, "runs")
    rbi = _num(bat, "rbi")

    singles = None
    if None not in (hits, doubles, triples, home_runs):
        singles = max(0.0, hits - doubles - triples - home_runs)
    hrr = None
    if None not in (hits, runs, rbi):
        hrr = hits + runs + rbi

    mapped = {
        "batter_hits": hits,
        "batter_total_bases": _num(bat, "totalBases"),
        "batter_home_runs": home_runs,
        "batter_rbis": rbi,
        "batter_runs_scored": runs,
        "batter_doubles": doubles,
        "batter_triples": triples,
        "batter_singles": singles,
        "batter_walks": _num(bat, "baseOnBalls"),
        "batter_strikeouts": _num(bat, "strikeOuts"),
        "batter_stolen_bases": _num(bat, "stolenBases"),
        "batter_hits_runs_rbis": hrr,
    }
    return [
        _row(sport_key, name, k, v, pid) for k, v in mapped.items() if v is not None
    ]


def _mlb_pitching_rows(sport_key, name, pid, pit: dict) -> List[dict]:
    if not pit:
        return []
    wins = _num(pit, "wins")
    mapped = {
        "pitcher_strikeouts": _num(pit, "strikeOuts"),
        "pitcher_hits_allowed": _num(pit, "hits"),
        "pitcher_walks": _num(pit, "baseOnBalls"),
        "pitcher_earned_runs": _num(pit, "earnedRuns"),
        "pitcher_outs": _num(pit, "outs"),
        # yes/no prop: 1 if credited the win else 0
        "pitcher_record_a_win": (1.0 if (wins or 0) >= 1 else 0.0) if wins is not None else None,
    }
    return [
        _row(sport_key, name, k, v, pid) for k, v in mapped.items() if v is not None
    ]


# ---------------------------------------------------------------------------
# NHL — api-web.nhle.com boxscore
# ---------------------------------------------------------------------------
def extract_nhl_player_stats(boxscore: dict, sport_key: str = NHL_SPORT_KEY) -> List[dict]:
    """Flatten an NHL boxscore into per-player stat rows (best-effort names)."""
    rows: List[dict] = []
    pbg = (boxscore or {}).get("playerByGameStats", {}) or {}
    for team_key in ("awayTeam", "homeTeam"):
        team = pbg.get(team_key, {}) or {}
        for grp in ("forwards", "defense"):
            for p in team.get(grp, []) or []:
                rows.extend(_nhl_skater_rows(sport_key, p))
        for g in team.get("goalies", []) or []:
            rows.extend(_nhl_goalie_rows(sport_key, g))
    return rows


def _nhl_name(p: dict):
    name = p.get("name")
    if isinstance(name, dict):
        return name.get("default")
    return name


def _nhl_skater_rows(sport_key, p: dict) -> List[dict]:
    name = _nhl_name(p)
    if not name:
        return []
    pid = p.get("playerId")
    mapped = {
        "player_goals": _num(p, "goals"),
        "player_assists": _num(p, "assists"),
        "player_points": _num(p, "points"),
        "player_shots_on_goal": _num(p, "sog"),
        "player_blocked_shots": _num(p, "blockedShots"),
        "player_power_play_points": _num(p, "powerPlayGoals"),
    }
    return [
        _row(sport_key, name, k, v, pid) for k, v in mapped.items() if v is not None
    ]


def _nhl_goalie_rows(sport_key, g: dict) -> List[dict]:
    name = _nhl_name(g)
    if not name:
        return []
    pid = g.get("playerId")
    mapped = {
        "player_total_saves": _num(g, "saves"),
        "player_goals_against": _num(g, "goalsAgainst"),
    }
    return [
        _row(sport_key, name, k, v, pid) for k, v in mapped.items() if v is not None
    ]
