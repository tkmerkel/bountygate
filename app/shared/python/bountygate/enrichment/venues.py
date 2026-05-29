"""Ballpark coordinates for weather enrichment.

dim_event.venue is empty, so the weather DAG keys on the home team's normalized
name. Only MLB is mapped (the in-season outdoor sport whose props/totals are
weather-sensitive). ``is_dome`` parks have fixed-roof/indoor conditions; the
weather DAG can skip them or record them as a no-op.

Coordinates are the stadium location (approx, 2-4 dp) — precise enough for an
Open-Meteo hourly grid cell.
"""
from __future__ import annotations

from typing import Optional

from bountygate.enrichment.match import normalize_team_name


def _park(lat: float, lon: float, name: str, dome: bool = False) -> dict:
    return {"lat": lat, "lon": lon, "park": name, "is_dome": dome}


# Keyed by normalized full team name (normalize_team_name).
MLB_PARKS = {
    "arizona diamondbacks": _park(33.4455, -112.0667, "Chase Field", dome=True),
    "atlanta braves": _park(33.8907, -84.4677, "Truist Park"),
    "baltimore orioles": _park(39.2839, -76.6217, "Oriole Park at Camden Yards"),
    "boston red sox": _park(42.3467, -71.0972, "Fenway Park"),
    "chicago cubs": _park(41.9484, -87.6553, "Wrigley Field"),
    "chicago white sox": _park(41.8299, -87.6338, "Guaranteed Rate Field"),
    "cincinnati reds": _park(39.0975, -84.5069, "Great American Ball Park"),
    "cleveland guardians": _park(41.4962, -81.6852, "Progressive Field"),
    "colorado rockies": _park(39.7559, -104.9942, "Coors Field"),
    "detroit tigers": _park(42.3390, -83.0485, "Comerica Park"),
    "houston astros": _park(29.7570, -95.3555, "Minute Maid Park", dome=True),
    "kansas city royals": _park(39.0517, -94.4803, "Kauffman Stadium"),
    "los angeles angels": _park(33.8003, -117.8827, "Angel Stadium"),
    "los angeles dodgers": _park(34.0739, -118.2400, "Dodger Stadium"),
    "miami marlins": _park(25.7781, -80.2197, "loanDepot park", dome=True),
    "milwaukee brewers": _park(43.0280, -87.9712, "American Family Field", dome=True),
    "minnesota twins": _park(44.9817, -93.2776, "Target Field"),
    "new york mets": _park(40.7571, -73.8458, "Citi Field"),
    "new york yankees": _park(40.8296, -73.9262, "Yankee Stadium"),
    "athletics": _park(37.7516, -121.2680, "Sutter Health Park"),
    "philadelphia phillies": _park(39.9061, -75.1665, "Citizens Bank Park"),
    "pittsburgh pirates": _park(40.4469, -80.0057, "PNC Park"),
    "san diego padres": _park(32.7073, -117.1566, "Petco Park"),
    "san francisco giants": _park(37.7786, -122.3893, "Oracle Park"),
    "seattle mariners": _park(47.5914, -122.3325, "T-Mobile Park", dome=True),
    "st louis cardinals": _park(38.6226, -90.1928, "Busch Stadium"),
    "tampa bay rays": _park(27.7683, -82.6534, "Tropicana Field", dome=True),
    "texas rangers": _park(32.7473, -97.0833, "Globe Life Field", dome=True),
    "toronto blue jays": _park(43.6414, -79.3894, "Rogers Centre", dome=True),
    "washington nationals": _park(38.8730, -77.0074, "Nationals Park"),
}


# Alternate team names that should resolve to a canonical park key.
ALIASES = {
    "oakland athletics": "athletics",
}


def lookup_park(team_name) -> Optional[dict]:
    """Return {lat, lon, park, is_dome} for a home team, or None if unmapped."""
    key = normalize_team_name(team_name)
    key = ALIASES.get(key, key)
    return MLB_PARKS.get(key)
