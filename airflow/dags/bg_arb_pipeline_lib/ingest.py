"""Ingest task for the arb pipeline.

Fetches odds from the-odds-api v4 and normalizes JSON responses into
per-(book, market_key, line, side) row dicts ready to write to
bg_arb_stage_lines.

The fetch logic and the JSON normalization are split intentionally so the
normalizer can be tested against fixtures without real HTTP.

Performance notes:
  - Per-sport market lists (ARB_MARKETS_BY_SPORT) so each event call only
    asks for markets relevant to that sport. The-odds-api bills per-market
    per-request, so a wide markets list wastes credits and slows wall time.
  - Events are filtered by commence_time to those within EVENT_WINDOW_HOURS
    BEFORE we spend the credit-expensive /odds calls. /events is a single
    cheap call per sport; /odds is per-event and expensive.
  - Per-event /odds calls run in a thread pool so wall time scales sublinearly.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

from bg_arb_pipeline_lib.markets import (
    ARB_BOOKMAKERS,
    ARB_MARKETS,
    ARB_MARKETS_BY_SPORT,
    ARB_SPORTS,
    SPORT_TITLE,
)

LOGGER = logging.getLogger(__name__)

# Only fetch odds for events starting within this many hours.
# Anything further out has volatile lines and isn't bettable right now anyway.
EVENT_WINDOW_HOURS = 24

# How many event-odds calls to run concurrently. The-odds-api can handle this;
# the bottleneck is round-trip latency per call.
ODDS_FETCH_CONCURRENCY = 8

# Shared session for connection reuse (one TCP/TLS handshake instead of N).
SESSION = requests.Session()


def fetch_events(sport_key: str, api_key: str, base_url: str) -> list[dict[str, Any]]:
    """List in-window events for a sport. Single cheap call."""
    url = f"{base_url}/v4/sports/{sport_key}/events"
    r = SESSION.get(url, params={"apiKey": api_key, "regions": "us"}, timeout=15)
    r.raise_for_status()
    return r.json() or []


def _parse_commence_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def filter_events_in_window(
    events: list[dict[str, Any]],
    *,
    now_utc: datetime,
    window_hours: int = EVENT_WINDOW_HOURS,
) -> list[dict[str, Any]]:
    """Keep only events starting between now and now+window_hours.

    Drops events that have already started (commence_time < now) — those are
    in-play and the line dynamics are different (and we don't bet them).
    Drops events further out than the window — line drift makes them stale
    before we'd act.
    """
    window_end = now_utc + timedelta(hours=window_hours)
    kept: list[dict[str, Any]] = []
    for event in events:
        ct = _parse_commence_time(event.get("commence_time"))
        if ct is None:
            continue
        if now_utc <= ct <= window_end:
            kept.append(event)
    return kept


def fetch_event_odds(
    sport_key: str,
    event_id: str,
    api_key: str,
    base_url: str,
    markets: Iterable[str] = ARB_MARKETS,
    bookmakers: Iterable[str] = ARB_BOOKMAKERS,
) -> dict[str, Any]:
    """Fetch one event's odds across all configured markets and bookmakers."""
    url = f"{base_url}/v4/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "apiKey": api_key,
        "regions": "us",
        "bookmakers": ",".join(bookmakers),
        "markets": ",".join(markets),
        "oddsFormat": "decimal",
    }
    r = SESSION.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json() or {}


def normalize_odds_response(
    payload: dict[str, Any],
    *,
    fetched_at_utc: datetime,
) -> list[dict[str, Any]]:
    """Convert one event's odds payload into per-(book, market, line, side) rows.

    Schema: https://the-odds-api.com/liveapi/guides/v4/#get-event-odds
    Each outcome has: name (Over/Under), description (player name), price, point.
    """
    if not payload or "bookmakers" not in payload:
        return []

    event_id = payload["id"]
    sport_key = payload.get("sport_key", "")
    sport_title = payload.get("sport_title") or SPORT_TITLE.get(sport_key, sport_key)
    home_team = payload.get("home_team", "")
    away_team = payload.get("away_team", "")
    commence_time_utc = _parse_commence_time(payload.get("commence_time"))

    rows: list[dict[str, Any]] = []
    for book in payload["bookmakers"]:
        book_key = book.get("key", "")
        for market in book.get("markets", []):
            market_key = market.get("key", "")
            for outcome in market.get("outcomes", []):
                side_raw = outcome.get("name", "").lower()
                if side_raw not in ("over", "under"):
                    continue
                rows.append({
                    "event_id": event_id,
                    "sport_title": sport_title,
                    "home_team": home_team,
                    "away_team": away_team,
                    "commence_time_utc": commence_time_utc,
                    "player_name": outcome.get("description", ""),
                    "bookmaker_key": book_key,
                    "market_key": market_key,
                    "line": float(outcome["point"]),
                    "side": side_raw,
                    "price": float(outcome["price"]),
                    "fetched_at_utc": fetched_at_utc,
                })
    return rows


def ingest_all(api_key: str, base_url: str = "https://api.the-odds-api.com") -> list[dict[str, Any]]:
    """Fetch every (sport, in-window event) and return flat stage rows.

    Order of operations:
      1. For each sport, fetch the events list (cheap, one call).
      2. Filter events by commence_time to the EVENT_WINDOW_HOURS window.
      3. Fetch odds for the filtered events in parallel (expensive, N calls).

    The credit-expensive /odds calls run only against events worth fetching.
    """
    now_utc = datetime.now(timezone.utc)

    # Collect (sport, event_id, sport_markets) tuples up front so the parallel
    # pool sees the whole job at once instead of one sport at a time.
    jobs: list[tuple[str, str, tuple[str, ...]]] = []
    for sport_key in ARB_SPORTS:
        sport_markets = ARB_MARKETS_BY_SPORT.get(sport_key, ())
        if not sport_markets:
            LOGGER.info("[ingest] skip sport=%s — no markets configured", sport_key)
            continue
        try:
            events = fetch_events(sport_key, api_key, base_url)
        except requests.RequestException as e:
            LOGGER.warning("[ingest] events fetch failed for sport=%s: %s", sport_key, e)
            continue
        in_window = filter_events_in_window(events, now_utc=now_utc)
        LOGGER.info(
            "[ingest] sport=%s events=%d in_window=%d markets=%d",
            sport_key, len(events), len(in_window), len(sport_markets),
        )
        for event in in_window:
            jobs.append((sport_key, event["id"], sport_markets))

    if not jobs:
        LOGGER.info("[ingest] no in-window events across all sports")
        return []

    LOGGER.info("[ingest] dispatching %d /odds calls (concurrency=%d)", len(jobs), ODDS_FETCH_CONCURRENCY)

    all_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=ODDS_FETCH_CONCURRENCY) as pool:
        futures = {
            pool.submit(fetch_event_odds, sport_key, event_id, api_key, base_url, markets=markets): (sport_key, event_id)
            for (sport_key, event_id, markets) in jobs
        }
        for fut in as_completed(futures):
            sport_key, event_id = futures[fut]
            try:
                payload = fut.result()
            except requests.RequestException as e:
                LOGGER.warning("[ingest] odds fetch failed for %s/%s: %s", sport_key, event_id, e)
                continue
            all_rows.extend(normalize_odds_response(payload, fetched_at_utc=now_utc))

    LOGGER.info("[ingest] produced %d total stage rows", len(all_rows))
    return all_rows
