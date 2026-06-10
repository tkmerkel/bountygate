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
