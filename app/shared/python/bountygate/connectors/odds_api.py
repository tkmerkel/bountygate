from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import requests

from bountygate.connectors.base import Connector, RawRecord

log = logging.getLogger(__name__)


def _is_auth_error(exc) -> bool:
    resp = getattr(exc, "response", None)
    return resp is not None and getattr(resp, "status_code", None) in (401, 403)


# The Odds API requires exactly this format on commenceTime params:
# no microseconds, no +00:00 offset — a literal trailing Z.
_ODDS_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


def _window_params(window_hours: int, now: datetime) -> dict[str, str]:
    """Pure: build {commenceTimeFrom, commenceTimeTo} from a window in hours.

    from = now (UTC), to = now + window_hours. Both formatted YYYY-MM-DDTHH:MM:SSZ
    (the only format The Odds API accepts — no microseconds, no offset).

    # commenceTimeFrom=now deliberately excludes in-play games (pre-game only, archive convention).
    """
    start = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    end = start + timedelta(hours=window_hours)
    return {
        "commenceTimeFrom": start.strftime(_ODDS_TS_FMT),
        "commenceTimeTo": end.strftime(_ODDS_TS_FMT),
    }


BASE_URL = "https://api.the-odds-api.com/v4/sports"
SPORT_KEYS = {"NFL": "americanfootball_nfl", "NBA": "basketball_nba", "MLB": "baseball_mlb"}


class OddsApiConnector(Connector):
    """Read-only sportsbook odds via The Odds API v4. Credit-aware two-step: list events per sport,
    then request odds per event (keeps credit use proportional to live events). Key from ODDS_API_KEY env.

    Props mode: pass `markets_by_sport` to override `markets` per sport (sports absent from the dict
    are skipped entirely — no /events call), `window_hours` to cap credits via a commence-time window,
    and `record_type` so the normalizer routes props payloads separately."""

    source = "the_odds_api"

    def __init__(
        self,
        sport_keys: dict | None = None,
        markets: str = "h2h",
        regions: str = "us",
        *,
        markets_by_sport: dict[str, tuple[str, ...]] | None = None,
        window_hours: int | None = None,
        record_type: str = "odds_line",
    ):
        self.api_key = os.getenv("ODDS_API_KEY")
        self.sport_keys = sport_keys or SPORT_KEYS
        self.markets = markets
        self.regions = regions
        self.markets_by_sport = markets_by_sport
        self.window_hours = window_hours
        self.record_type = record_type
        self._last_headers = None

    def normalize_event(self, event: dict, captured_at: datetime) -> list[RawRecord]:
        """Pure: one /events/{id}/odds payload -> one RawRecord per (book, market).

        Stamps `self.record_type`; outcomes are passed through whole so prop fields
        (description, point) survive into the payload untouched."""
        records: list[RawRecord] = []
        event_id = event.get("id")
        if not event_id:
            return records
        common = {
            "event_id": event_id,
            "sport_key": event.get("sport_key"),
            "commence_time": event.get("commence_time"),
            "home_team": event.get("home_team"),
            "away_team": event.get("away_team"),
        }
        for book in event.get("bookmakers") or []:
            book_key = book.get("key")
            for market in book.get("markets") or []:
                market_key = market.get("key")
                if not book_key or not market_key:
                    continue
                payload = {
                    **common,
                    "bookmaker": book_key,
                    "market": market_key,
                    "last_update": book.get("last_update"),
                    "outcomes": market.get("outcomes") or [],
                }
                records.append(
                    RawRecord(
                        source="the_odds_api",
                        source_key=f"{event_id}:{market_key}:{book_key}",
                        record_type=self.record_type,
                        captured_at=captured_at,
                        payload=payload,
                    )
                )
        return records

    def _list_events(self, session, sport_key: str) -> list:
        params = {"apiKey": self.api_key}
        if self.window_hours is not None:
            params.update(_window_params(self.window_hours, datetime.now(timezone.utc)))
        resp = session.get(f"{BASE_URL}/{sport_key}/events", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _event_odds(self, session, sport_key: str, event_id: str) -> dict:
        if self.markets_by_sport is not None:
            markets = ",".join(self.markets_by_sport[sport_key])
        else:
            markets = self.markets
        resp = session.get(
            f"{BASE_URL}/{sport_key}/events/{event_id}/odds",
            params={
                "apiKey": self.api_key,
                "regions": self.regions,
                "markets": markets,
                "oddsFormat": "decimal",
            },
            timeout=30,
        )
        resp.raise_for_status()
        self._last_headers = resp.headers
        return resp.json()

    def fetch_snapshots(self) -> list[RawRecord]:
        captured_at = datetime.now(timezone.utc)
        out: list[RawRecord] = []
        session = requests.Session()
        for sport_key in self.sport_keys.values():
            self._last_headers = None  # reset so credit log never prints a stale previous sport's numbers
            if self.markets_by_sport is not None and sport_key not in self.markets_by_sport:
                continue
            try:
                events = self._list_events(session, sport_key)
            except Exception as e:
                if _is_auth_error(e):
                    raise
                log.warning("[odds] list events failed for %s: %s", sport_key, e)
                continue
            for ev in events:
                try:
                    odds = self._event_odds(session, sport_key, ev["id"])
                except Exception as e:
                    if _is_auth_error(e):
                        raise
                    log.warning("[odds] odds fetch failed for %s: %s", ev.get("id"), e)
                    continue
                out.extend(self.normalize_event(odds, captured_at))
            if self._last_headers is not None:
                used = self._last_headers.get("x-requests-used")
                remaining = self._last_headers.get("x-requests-remaining")
                print(f"[odds] {sport_key}: credits used={used} remaining={remaining}")
        return out
