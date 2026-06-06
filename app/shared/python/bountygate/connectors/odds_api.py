from __future__ import annotations

import os
from datetime import datetime, timezone

import requests

from bountygate.connectors.base import Connector, RawRecord

BASE_URL = "https://api.the-odds-api.com/v4/sports"
SPORT_KEYS = {"NFL": "americanfootball_nfl", "NBA": "basketball_nba", "MLB": "baseball_mlb"}


class OddsApiConnector(Connector):
    """Read-only sportsbook odds via The Odds API v4. Credit-aware: list events in a
    commence window first, then request odds per event. Key from ODDS_API_KEY env."""

    source = "the_odds_api"

    def __init__(self, sport_keys: dict | None = None, markets: str = "h2h", regions: str = "us"):
        self.api_key = os.getenv("ODDS_API_KEY")
        self.sport_keys = sport_keys or SPORT_KEYS
        self.markets = markets
        self.regions = regions

    @staticmethod
    def normalize_event(event: dict, captured_at: datetime) -> list[RawRecord]:
        """Pure: one /events/{id}/odds payload -> one RawRecord per (book, market)."""
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
                        record_type="odds_line",
                        captured_at=captured_at,
                        payload=payload,
                    )
                )
        return records

    def _list_events(self, session, sport_key: str) -> list:
        resp = session.get(
            f"{BASE_URL}/{sport_key}/events", params={"apiKey": self.api_key}, timeout=30
        )
        resp.raise_for_status()
        return resp.json()

    def _event_odds(self, session, sport_key: str, event_id: str) -> dict:
        resp = session.get(
            f"{BASE_URL}/{sport_key}/events/{event_id}/odds",
            params={
                "apiKey": self.api_key,
                "regions": self.regions,
                "markets": self.markets,
                "oddsFormat": "decimal",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_snapshots(self) -> list[RawRecord]:
        captured_at = datetime.now(timezone.utc)
        out: list[RawRecord] = []
        session = requests.Session()
        for sport_key in self.sport_keys.values():
            try:
                events = self._list_events(session, sport_key)
            except Exception as e:
                print(f"[odds] list events failed for {sport_key}: {e}")
                continue
            for ev in events:
                try:
                    odds = self._event_odds(session, sport_key, ev["id"])
                except Exception as e:
                    print(f"[odds] odds fetch failed for {ev.get('id')}: {e}")
                    continue
                out.extend(self.normalize_event(odds, captured_at))
        return out
