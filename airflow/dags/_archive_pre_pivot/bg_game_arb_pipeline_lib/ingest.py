"""Ingest task for the game-line arb pipeline.

Uses the-odds-api `/v4/sports/{sport}/odds` (bulk endpoint) — one call per
sport returns h2h/spreads/totals across every in-window event. This is 1
credit per market per region per call regardless of event count, which is
substantially cheaper than the per-event endpoint bg_arb_pipeline uses for
player props (player props are only available per-event).

The fetch + normalize split mirrors bg_arb_pipeline_lib/ingest.py so the
normalizer can be tested against fixtures without real HTTP.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

from bg_game_arb_pipeline_lib.markets import (
    GAME_ARB_BOOKMAKERS,
    GAME_ARB_MARKETS,
    GAME_ARB_REGION,
    GAME_ARB_SPORTS,
    SPORT_TITLE,
)

LOGGER = logging.getLogger(__name__)

# Wall-time bound on the bulk /odds calls. One call per sport, run in parallel.
SPORT_FETCH_CONCURRENCY = 5

SESSION = requests.Session()


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_sport_odds(
    sport_key: str,
    api_key: str,
    base_url: str,
    markets: Iterable[str] = GAME_ARB_MARKETS,
    bookmakers: Iterable[str] = GAME_ARB_BOOKMAKERS,
    region: str = GAME_ARB_REGION,
) -> list[dict[str, Any]]:
    """Fetch h2h/spreads/totals for every in-window event in one sport.

    Returns the raw list of event payloads as the-odds-api returns them.
    """
    url = f"{base_url}/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": api_key,
        "regions": region,
        "bookmakers": ",".join(bookmakers),
        "markets": ",".join(markets),
        "oddsFormat": "decimal",
    }
    r = SESSION.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json() or []


def normalize_sport_odds(
    events: list[dict[str, Any]],
    *,
    fetched_at_utc: datetime,
) -> list[dict[str, Any]]:
    """Convert the-odds-api event list into per-(book, market, outcome, point) rows.

    Schema:
      https://the-odds-api.com/liveapi/guides/v4/#get-odds
    Each outcome has: name (team or 'Over'/'Under'), price, and optionally point.
    """
    rows: list[dict[str, Any]] = []
    for event in events or []:
        event_id = event.get("id")
        if not event_id:
            continue
        sport_key = event.get("sport_key", "")
        sport_title = event.get("sport_title") or SPORT_TITLE.get(sport_key, sport_key)
        home_team = event.get("home_team", "")
        away_team = event.get("away_team", "")
        commence_time_utc = _parse_iso_utc(event.get("commence_time"))
        if commence_time_utc is None:
            continue
        for book in event.get("bookmakers", []):
            book_key = book.get("key", "")
            for market in book.get("markets", []):
                market_key = market.get("key", "")
                if market_key not in GAME_ARB_MARKETS:
                    continue
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name")
                    if not name:
                        continue
                    try:
                        price = float(outcome["price"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    raw_point = outcome.get("point")
                    if raw_point is None:
                        # h2h has no point.
                        point: float | None = None
                    else:
                        try:
                            point = float(raw_point)
                        except (TypeError, ValueError):
                            continue
                    rows.append({
                        "event_id": event_id,
                        "sport_key": sport_key,
                        "sport_title": sport_title,
                        "home_team": home_team,
                        "away_team": away_team,
                        "commence_time_utc": commence_time_utc,
                        "bookmaker_key": book_key,
                        "market_key": market_key,
                        "outcome": name,
                        "point": point,
                        "price": price,
                        "fetched_at_utc": fetched_at_utc,
                    })
    return rows


def ingest_all(api_key: str, base_url: str = "https://api.the-odds-api.com") -> list[dict[str, Any]]:
    """Fetch every (sport, in-window event) for game markets and return flat stage rows.

    One bulk /odds call per sport, run in parallel.
    """
    now_utc = datetime.now(timezone.utc)
    all_rows: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=SPORT_FETCH_CONCURRENCY) as pool:
        futures = {
            pool.submit(fetch_sport_odds, sport_key, api_key, base_url): sport_key
            for sport_key in GAME_ARB_SPORTS
        }
        for fut in as_completed(futures):
            sport_key = futures[fut]
            try:
                events = fut.result()
            except requests.RequestException as e:
                LOGGER.warning("[ingest] sport=%s fetch failed: %s", sport_key, e)
                continue
            rows = normalize_sport_odds(events, fetched_at_utc=now_utc)
            LOGGER.info("[ingest] sport=%s events=%d rows=%d", sport_key, len(events), len(rows))
            all_rows.extend(rows)

    LOGGER.info("[ingest] produced %d total stage rows across %d sports", len(all_rows), len(GAME_ARB_SPORTS))
    return all_rows
