"""normalized markets + sports_events -> market_event_links. Re-derives every run
(TRUNCATE + insert). The pure matching lives in match.py."""
from __future__ import annotations

import os

from sqlalchemy import create_engine, text

from bountygate.transforms.matching.match import link_rows


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def link_markets() -> dict:
    engine = _engine()
    try:
        with engine.begin() as conn:
            markets = conn.execute(text(
                "SELECT market_id::text AS market_id, venue_key, external_id, title, category, "
                "       close_time::text AS close_time FROM markets "
                "WHERE venue_key IN ('kalshi', 'polymarket')")).mappings().all()
            events = conn.execute(text(
                "SELECT event_id::text AS event_id, sport_key, "
                "       commence_time::text AS commence_time, home_team, away_team "
                "FROM sports_events")).mappings().all()
            result = link_rows([dict(m) for m in markets], [dict(e) for e in events])
            conn.execute(text("TRUNCATE market_event_links"))
            for ln in result["links"]:
                conn.execute(text(
                    "INSERT INTO market_event_links (market_id, event_id, confidence, method) "
                    "VALUES (cast(:market_id AS uuid), cast(:event_id AS uuid), :confidence, :method)"),
                    ln)
        print(f"[link_markets] {result['stats']}")
        return result["stats"]
    finally:
        engine.dispose()
