"""Name + date based matching of external feed entities to the dim model.

dim_team is empty, so external games/players cannot be joined via external ids.
Instead we match on normalized full team names + UTC date (events) and on
normalized player names (players).

CRITICAL: ``normalize_player_name`` and ``player_id`` MUST stay byte-for-byte
identical to ``bg_dimensional_model._normalize_player_name`` / ``_player_id``
(copied here, like bg_analytics_lib.common copies the event-id logic) so that
box-score stats produced here join straight onto the dim_player rows the
dimensional-model DAG writes. Keep in sync if either side changes.
"""
from __future__ import annotations

import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

# --- copied verbatim from bg_dimensional_model -----------------------------
_PUNCT_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE = re.compile(r"\s+")


def normalize_player_name(name) -> str:
    """lower / strip / collapse-whitespace / remove punctuation.

    Byte-for-byte identical to bg_dimensional_model._normalize_player_name.
    """
    if name is None:
        return ""
    if isinstance(name, float):
        # NaN guard without importing pandas
        if name != name:  # noqa: PLR0124 - NaN is the only value != itself
            return ""
    s = str(name).strip().lower()
    s = _PUNCT_RE.sub("", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def player_id(sport_key: str, normalized_name: str) -> str:
    """Deterministic uuid5 (hex) over '{sport_key}|{normalized_name}'.

    Byte-for-byte identical to bg_dimensional_model._player_id. Expects an
    already-normalized name (use ``player_id_for_name`` to normalize first).
    """
    return uuid.uuid5(uuid.NAMESPACE_URL, f"{sport_key}|{normalized_name}").hex


def player_id_for_name(sport_key: str, raw_name) -> str:
    """Normalize ``raw_name`` then return its player_id."""
    return player_id(sport_key, normalize_player_name(raw_name))


# --- team / event matching --------------------------------------------------
def normalize_team_name(name) -> str:
    """Normalize a team display name with the same rules as player names."""
    return normalize_player_name(name)


def _to_utc_date(value) -> Optional[date]:
    """Coerce an ISO string / datetime / date to a UTC calendar date."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        return value
    else:
        s = str(value).strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # bare date string
            try:
                return date.fromisoformat(s[:10])
            except ValueError:
                return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.date()


def match_game_to_event(
    sport_key: str,
    home_team_name,
    away_team_name,
    commence,
    events: Iterable[dict],
    *,
    day_tolerance: int = 1,
) -> Optional[str]:
    """Return the bg_event_id for the feed game, or None.

    Matches on sport_key + the *set* of normalized team names (so a feed that
    lists home/away in the opposite order still matches) + UTC date within
    ``day_tolerance`` days. Returns the closest-date match on ties.
    """
    target_date = _to_utc_date(commence)
    feed_teams = frozenset(
        t for t in (normalize_team_name(home_team_name), normalize_team_name(away_team_name)) if t
    )
    if len(feed_teams) != 2 or target_date is None:
        return None

    best_id: Optional[str] = None
    best_delta = timedelta(days=day_tolerance) + timedelta(days=1)

    for ev in events:
        if str(ev.get("sport_key")) != str(sport_key):
            continue
        ev_teams = frozenset(
            t
            for t in (
                normalize_team_name(ev.get("home_team_name")),
                normalize_team_name(ev.get("away_team_name")),
            )
            if t
        )
        if ev_teams != feed_teams:
            continue
        ev_date = _to_utc_date(ev.get("commence_at_utc"))
        if ev_date is None:
            continue
        delta = abs(ev_date - target_date)
        if delta <= timedelta(days=day_tolerance) and delta < best_delta:
            best_delta = delta
            best_id = str(ev.get("bg_event_id"))

    return best_id
