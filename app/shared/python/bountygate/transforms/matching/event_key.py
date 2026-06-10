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
