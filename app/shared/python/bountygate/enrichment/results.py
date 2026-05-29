"""Parsers for game results, schedules, and injuries.

Pure functions over provider JSON → rows the DAGs match to bg_event_id and
write to fact_game_result / dim_player_status. Player-stat (prop) extraction
lives in statmap; this module is game-level + availability.

  * parse_espn_scoreboard  — universal game results (NBA/NHL/NFL/NCAA/MLB)
  * parse_mlb_schedule     — MLB games + final scores + gamePk (for boxscore)
  * parse_nhl_schedule     — NHL games + final scores + game id (for boxscore)
  * parse_espn_injuries    — player availability rows
"""
from __future__ import annotations

from typing import List, Optional

from bountygate.enrichment import match


def _int(v) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


# ---------------------------------------------------------------------------
# ESPN scoreboard — site.api.espn.com/.../scoreboard
# ---------------------------------------------------------------------------
def parse_espn_scoreboard(payload: dict, sport_key: str) -> List[dict]:
    rows: List[dict] = []
    for ev in (payload or {}).get("events", []) or []:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        status_type = (comp.get("status") or ev.get("status") or {}).get("type", {}) or {}
        home_name = away_name = None
        home_score = away_score = None
        for c in comp.get("competitors", []) or []:
            team_name = (c.get("team") or {}).get("displayName")
            score = _int(c.get("score"))
            if c.get("homeAway") == "home":
                home_name, home_score = team_name, score
            elif c.get("homeAway") == "away":
                away_name, away_score = team_name, score
        rows.append(
            {
                "source_event_id": ev.get("id"),
                "sport_key": sport_key,
                "home_team_name": home_name,
                "away_team_name": away_name,
                "home_score": home_score,
                "away_score": away_score,
                "status": status_type.get("name"),
                "completed": bool(status_type.get("completed")),
                "commence_at_utc": ev.get("date"),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# MLB StatsAPI schedule — statsapi.mlb.com/api/v1/schedule
# ---------------------------------------------------------------------------
def parse_mlb_schedule(payload: dict) -> List[dict]:
    rows: List[dict] = []
    for day in (payload or {}).get("dates", []) or []:
        for g in day.get("games", []) or []:
            teams = g.get("teams", {}) or {}
            away = teams.get("away", {}) or {}
            home = teams.get("home", {}) or {}
            status = g.get("status", {}) or {}
            rows.append(
                {
                    "game_pk": g.get("gamePk"),
                    "sport_key": "baseball_mlb",
                    "away_team_name": (away.get("team") or {}).get("name"),
                    "home_team_name": (home.get("team") or {}).get("name"),
                    "away_score": _int(away.get("score")),
                    "home_score": _int(home.get("score")),
                    "commence_at_utc": g.get("gameDate"),
                    "status": status.get("detailedState"),
                    "final": status.get("abstractGameState") == "Final"
                    or status.get("codedGameState") == "F",
                }
            )
    return rows


# ---------------------------------------------------------------------------
# NHL — api-web.nhle.com/v1/schedule
# ---------------------------------------------------------------------------
def _nhl_team_name(team: dict) -> Optional[str]:
    if not team:
        return None
    place = (team.get("placeName") or {}).get("default")
    common = (team.get("commonName") or {}).get("default")
    parts = [p for p in (place, common) if p]
    return " ".join(parts) if parts else None


def parse_nhl_schedule(payload: dict) -> List[dict]:
    rows: List[dict] = []
    for day in (payload or {}).get("gameWeek", []) or []:
        for g in day.get("games", []) or []:
            away = g.get("awayTeam", {}) or {}
            home = g.get("homeTeam", {}) or {}
            state = g.get("gameState")
            rows.append(
                {
                    "game_id": g.get("id"),
                    "sport_key": "icehockey_nhl",
                    "away_team_name": _nhl_team_name(away),
                    "home_team_name": _nhl_team_name(home),
                    "away_score": _int(away.get("score")),
                    "home_score": _int(home.get("score")),
                    "commence_at_utc": g.get("startTimeUTC"),
                    "status": state,
                    "final": state in ("OFF", "FINAL"),
                }
            )
    return rows


# ---------------------------------------------------------------------------
# ESPN injuries — site.api.espn.com/.../injuries
# ---------------------------------------------------------------------------
def _injury_description(injury: dict) -> Optional[str]:
    details = injury.get("details", {}) or {}
    parts = [details.get("type"), details.get("detail")]
    desc = " ".join(p for p in parts if p)
    if desc:
        return desc
    return injury.get("shortComment") or (injury.get("type") or {}).get("description")


def parse_espn_injuries(payload: dict, sport_key: str) -> List[dict]:
    rows: List[dict] = []
    for team_entry in (payload or {}).get("injuries", []) or []:
        team_name = team_entry.get("displayName")
        for injury in team_entry.get("injuries", []) or []:
            athlete = injury.get("athlete", {}) or {}
            name = athlete.get("displayName")
            if not name:
                continue
            normalized = match.normalize_player_name(name)
            rows.append(
                {
                    "player_name": name,
                    "normalized_name": normalized,
                    "player_id": match.player_id(sport_key, normalized),
                    "sport_key": sport_key,
                    "team_name": team_name,
                    "status": injury.get("status"),
                    "description": _injury_description(injury),
                    "report_at_utc": injury.get("date"),
                    "source": "espn",
                    "source_player_id": athlete.get("id"),
                }
            )
    return rows
