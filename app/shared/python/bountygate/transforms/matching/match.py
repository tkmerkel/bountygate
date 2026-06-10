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
