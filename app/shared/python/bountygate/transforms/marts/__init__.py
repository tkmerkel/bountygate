"""DB I/O for the marts: read normalized -> compute -> write. polars for reshaping."""
from __future__ import annotations

import os

import polars as pl
from sqlalchemy import create_engine, text

from bountygate.transforms.marts.edge_signals import compute_edge_signals
from bountygate.transforms.marts.market_history import compute_market_history


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def _latest_odds_rows(conn) -> list[dict]:
    rows = conn.execute(text(
        "SELECT event_id::text AS event_id, market_type, bookmaker, outcome_name, "
        "       decimal_price, captured_at "
        "FROM sportsbook_odds_history")).mappings().all()
    if not rows:
        return []
    df = pl.DataFrame([dict(r) for r in rows])
    # decimal_price arrives as Decimal from the DB; the analytics math expects float
    df = df.with_columns(pl.col("decimal_price").cast(pl.Float64))
    # latest row per (event, market_type, bookmaker, outcome)
    latest = (df.sort("captured_at")
                .group_by(["event_id", "market_type", "bookmaker", "outcome_name"])
                .last())
    return latest.select(
        ["event_id", "market_type", "bookmaker", "outcome_name", "decimal_price"]
    ).to_dicts()


def build_edge_signals() -> int:
    engine = _engine()
    try:
        with engine.begin() as conn:
            rows = _latest_odds_rows(conn)
            signals = compute_edge_signals(rows)
            conn.execute(text("TRUNCATE mart_edge_signals"))
            for s in signals:
                conn.execute(text(
                    "INSERT INTO mart_edge_signals "
                    "  (detected_at, event_id, bookmaker, market_type, outcome_name, "
                    "   signal_type, fair_prob, venue_price, edge, kelly_fraction) "
                    "VALUES (now(), :event_id, :bookmaker, :market_type, :outcome_name, "
                    "   :signal_type, :fair_prob, :venue_price, :edge, :kelly_fraction)"),
                    {"event_id": s["event_id"], "bookmaker": s["bookmaker"],
                     "market_type": s["market_type"], "outcome_name": s["outcome_name"],
                     "signal_type": s["signal_type"], "fair_prob": s["fair_prob"],
                     "venue_price": s["venue_price"], "edge": s["edge"],
                     "kelly_fraction": s.get("kelly_fraction")})
        return len(signals)
    finally:
        engine.dispose()


def build_market_history() -> int:
    engine = _engine()
    try:
        with engine.begin() as conn:
            markets = conn.execute(text(
                "SELECT market_id::text AS market_id, resolved_outcome, "
                "       close_time::text AS close_time, resolution_time::text AS resolution_time "
                "FROM markets "
                "WHERE resolved_outcome IS NOT NULL AND close_time IS NOT NULL")).mappings().all()
            resolved, prices_by_market = [], {}
            for m in markets:
                # tracked outcome = the resolved outcome's price series (fallback: first outcome)
                tracked = m["resolved_outcome"]
                resolved.append({**dict(m), "tracked_outcome": tracked})
                pts = conn.execute(text(
                    "SELECT ph.captured_at::text AS ts, ph.price "
                    "FROM price_history ph JOIN market_outcomes o ON o.outcome_id=ph.outcome_id "
                    "WHERE o.market_id = :mid AND o.outcome_name = :name AND ph.price IS NOT NULL"),
                    {"mid": m["market_id"], "name": tracked}).all()
                prices_by_market[m["market_id"]] = [(ts, price) for ts, price in pts]
            rows = compute_market_history(resolved, prices_by_market)
            for r in rows:
                conn.execute(text(
                    "INSERT INTO mart_market_history "
                    "  (market_id, resolved_outcome, resolution_time, predicted_prob, realized, clv) "
                    "VALUES (:market_id, :resolved_outcome, :resolution_time, :predicted_prob, "
                    "        :realized, :clv) "
                    "ON CONFLICT (market_id) DO UPDATE SET "
                    "  resolved_outcome=EXCLUDED.resolved_outcome, predicted_prob=EXCLUDED.predicted_prob, "
                    "  realized=EXCLUDED.realized, clv=EXCLUDED.clv"), r)
        return len(rows)
    finally:
        engine.dispose()
