"""Raw -> normalized. Reads new raw_market_snapshots rows since the stored watermark,
parses by source, upserts dimension tables, and appends the partitioned time-series
idempotently. I/O lives here; the per-source row shaping lives in parsers/."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from bountygate.transforms.parsers.kalshi import parse_kalshi
from bountygate.transforms.parsers.polymarket import parse_polymarket
from bountygate.transforms.parsers.odds import parse_odds_line

WATERMARK_NAME = "normalize"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def run_normalize() -> dict:
    """Process all raw rows newer than the watermark. Returns a counts summary."""
    engine = _engine()
    try:
        with engine.begin() as conn:
            wm = conn.execute(
                text("SELECT watermark FROM transform_state WHERE name = :n"),
                {"n": WATERMARK_NAME},
            ).scalar() or _EPOCH
            raw = conn.execute(text(
                "SELECT source, captured_at, payload FROM raw_market_snapshots "
                "WHERE captured_at > :wm ORDER BY captured_at"), {"wm": wm}).mappings().all()

            counts = {"markets": 0, "outcomes": 0, "prices": 0, "events": 0, "odds": 0}
            max_ts = wm
            for row in raw:
                src, captured_at, payload = row["source"], row["captured_at"], row["payload"]
                if captured_at and captured_at > max_ts:
                    max_ts = captured_at
                if src in ("kalshi", "polymarket"):
                    parsed = parse_kalshi(payload) if src == "kalshi" else parse_polymarket(payload)
                    counts["markets"] += _upsert_market(conn, parsed["market"])
                    oid_map = _upsert_outcomes(conn, parsed["market"], parsed["outcomes"])
                    counts["outcomes"] += len(oid_map)
                    counts["prices"] += _append_prices(conn, oid_map, parsed["prices"], captured_at)
                elif src == "the_odds_api":
                    parsed = parse_odds_line(payload)
                    eid = _upsert_event(conn, parsed["event"])
                    counts["events"] += 1
                    counts["odds"] += _append_odds(conn, eid, parsed["odds"], captured_at)

            if raw:
                conn.execute(text(
                    "INSERT INTO transform_state(name, watermark) VALUES (:n, :w) "
                    "ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark"),
                    {"n": WATERMARK_NAME, "w": max_ts})
        return counts
    finally:
        engine.dispose()


def _upsert_market(conn, m: dict) -> int:
    conn.execute(text(
        "INSERT INTO markets (venue_key, external_id, title, category, status, "
        "  open_time, close_time, resolved_outcome, resolution_time, updated_at) "
        "VALUES (:venue_key, :external_id, :title, :category, :status, "
        "  :open_time, :close_time, :resolved_outcome, :resolution_time, now()) "
        "ON CONFLICT (venue_key, external_id) DO UPDATE SET "
        "  title=EXCLUDED.title, category=EXCLUDED.category, status=EXCLUDED.status, "
        "  close_time=EXCLUDED.close_time, resolved_outcome=EXCLUDED.resolved_outcome, "
        "  resolution_time=EXCLUDED.resolution_time, updated_at=now()"), m)
    return 1


def _upsert_outcomes(conn, market: dict, outcomes: list[dict]) -> dict:
    """Upsert outcomes; return {outcome_name: outcome_id}."""
    oid_map = {}
    for o in outcomes:
        conn.execute(text(
            "INSERT INTO market_outcomes (market_id, outcome_name, outcome_index, last_price, last_seen) "
            "SELECT m.market_id, :outcome_name, :outcome_index, :last_price, now() "
            "FROM markets m WHERE m.venue_key=:venue_key AND m.external_id=:external_id "
            "ON CONFLICT (market_id, outcome_name) DO UPDATE SET "
            "  last_price=EXCLUDED.last_price, last_seen=now()"),
            {**o, "venue_key": market["venue_key"], "external_id": market["external_id"]})
        oid = conn.execute(text(
            "SELECT o.outcome_id FROM market_outcomes o JOIN markets m ON m.market_id=o.market_id "
            "WHERE m.venue_key=:venue_key AND m.external_id=:external_id AND o.outcome_name=:outcome_name"),
            {"venue_key": market["venue_key"], "external_id": market["external_id"],
             "outcome_name": o["outcome_name"]}).scalar()
        oid_map[o["outcome_name"]] = oid
    return oid_map


def _append_prices(conn, oid_map: dict, prices: list[dict], captured_at) -> int:
    n = 0
    for p in prices:
        oid = oid_map.get(p["outcome_name"])
        if oid is None:
            continue
        res = conn.execute(text(
            "INSERT INTO price_history (market_id, outcome_id, captured_at, price, bid, ask, volume, liquidity) "
            "SELECT o.market_id, :oid, :captured_at, :price, :bid, :ask, :volume, :liquidity "
            "FROM market_outcomes o WHERE o.outcome_id = :oid "
            "ON CONFLICT (outcome_id, captured_at) DO NOTHING"),
            {"oid": oid, "captured_at": captured_at, "price": p["price"], "bid": p["bid"],
             "ask": p["ask"], "volume": p["volume"], "liquidity": p["liquidity"]})
        n += res.rowcount or 0
    return n


def _upsert_event(conn, e: dict):
    return conn.execute(text(
        "INSERT INTO sports_events (source_event_id, sport_key, commence_time, home_team, away_team) "
        "VALUES (:source_event_id, :sport_key, :commence_time, :home_team, :away_team) "
        "ON CONFLICT (source_event_id) DO UPDATE SET "
        "  sport_key=EXCLUDED.sport_key, commence_time=EXCLUDED.commence_time, "
        "  home_team=EXCLUDED.home_team, away_team=EXCLUDED.away_team "
        "RETURNING event_id"), e).scalar()


def _append_odds(conn, event_id, odds: list[dict], captured_at) -> int:
    n = 0
    for o in odds:
        res = conn.execute(text(
            "INSERT INTO sportsbook_odds_history "
            "  (event_id, market_type, bookmaker, outcome_name, captured_at, decimal_price) "
            "VALUES (:event_id, :market_type, :bookmaker, :outcome_name, :captured_at, :decimal_price) "
            "ON CONFLICT (event_id, market_type, bookmaker, outcome_name, captured_at) DO NOTHING"),
            {"event_id": event_id, "captured_at": captured_at, **o})
        n += res.rowcount or 0
    return n
