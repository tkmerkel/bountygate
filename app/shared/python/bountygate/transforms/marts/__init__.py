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
    # h2h only: feeds both fair-price consensus and edge signals, which group by
    # (event, market, book, outcome) with no per-line key. Spreads/totals carry a
    # point value that those consumers don't group on, so including them would
    # collapse distinct lines (e.g. Over 8.5 vs Over 9.5) into one bogus pair.
    rows = conn.execute(text(
        "SELECT event_id::text AS event_id, market_type, bookmaker, outcome_name, "
        "       decimal_price, captured_at "
        "FROM sportsbook_odds_history WHERE market_type = 'h2h'")).mappings().all()
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


def build_cross_market() -> int:
    from bountygate.transforms.matching.aliases import canonical_team, sport_for_odds
    from bountygate.transforms.matching.event_key import parse_kalshi_external_id
    from bountygate.transforms.matching.match import polymarket_teams
    from bountygate.transforms.marts.cross_market import assemble_rows, sportsbook_side_probs

    engine = _engine()
    try:
        with engine.begin() as conn:
            # events -> game scaffolds keyed by event_id, canonical home/away
            games = {}
            for e in conn.execute(text(
                "SELECT event_id::text AS event_id, sport_key, commence_time, "
                "       home_team, away_team FROM sports_events")).mappings().all():
                sport = sport_for_odds(e["sport_key"])
                if not sport:
                    continue
                home = canonical_team(e["home_team"], sport)
                away = canonical_team(e["away_team"], sport)
                if not (home and away and e["commence_time"]):
                    continue
                games[e["event_id"]] = {
                    "sport": sport, "date": e["commence_time"], "home": home, "away": away,
                    "sportsbook": {}, "kalshi": {}, "polymarket": {},
                }

            # sportsbook consensus: latest h2h decimal price per (event, book, side)
            by_event_book = {}      # event_id -> {book: {side: price}}
            latest_at = {}          # (event_id, book, side) -> captured_at
            for o in conn.execute(text(
                "SELECT event_id::text AS event_id, bookmaker, outcome_name, "
                "       decimal_price, captured_at FROM sportsbook_odds_history "
                "WHERE market_type = 'h2h'")).mappings().all():
                g = games.get(o["event_id"])
                if not g:
                    continue
                side = canonical_team(o["outcome_name"], g["sport"])
                if side is None:
                    continue
                key = (o["event_id"], o["bookmaker"], side)
                if key in latest_at and o["captured_at"] <= latest_at[key]:
                    continue
                latest_at[key] = o["captured_at"]
                by_event_book.setdefault(o["event_id"], {}).setdefault(
                    o["bookmaker"], {})[side] = float(o["decimal_price"])
            for eid, by_book in by_event_book.items():
                probs = sportsbook_side_probs(by_book, games[eid]["home"], games[eid]["away"])
                if probs:
                    games[eid]["sportsbook"] = probs

            # linked prediction markets -> venue prob per side
            for ln in conn.execute(text(
                "SELECT l.event_id::text AS event_id, m.venue_key, m.external_id, m.title, "
                "       m.category, m.market_id::text AS market_id "
                "FROM market_event_links l JOIN markets m ON m.market_id = l.market_id")).mappings().all():
                g = games.get(ln["event_id"])
                if not g:
                    continue
                if ln["venue_key"] == "kalshi":
                    p = parse_kalshi_external_id(ln["external_id"], ln["category"])
                    if not p or p["team"] not in (g["home"], g["away"]):
                        continue
                    yes = conn.execute(text(
                        "SELECT last_price FROM market_outcomes "
                        "WHERE market_id = cast(:mid AS uuid) AND outcome_name = 'Yes'"),
                        {"mid": ln["market_id"]}).scalar()
                    if yes is not None:
                        g["kalshi"][p["team"]] = float(yes)
                elif ln["venue_key"] == "polymarket":
                    r = polymarket_teams(ln["title"])
                    if not r:
                        continue
                    _sport, teams, subject = r
                    opponent = next(iter(teams - {subject})) if subject in teams else None
                    for o in conn.execute(text(
                        "SELECT outcome_name, last_price FROM market_outcomes "
                        "WHERE market_id = cast(:mid AS uuid)"),
                        {"mid": ln["market_id"]}).mappings().all():
                        if o["last_price"] is None:
                            continue
                        nm = str(o["outcome_name"]).strip().lower()
                        named = canonical_team(o["outcome_name"], g["sport"])
                        if named in (g["home"], g["away"]):
                            g["polymarket"][named] = float(o["last_price"])
                        elif nm == "yes" and subject in (g["home"], g["away"]):
                            g["polymarket"][subject] = float(o["last_price"])
                        elif nm == "no" and opponent in (g["home"], g["away"]):
                            g["polymarket"][opponent] = float(o["last_price"])

            rows = assemble_rows(list(games.values()))
            conn.execute(text("TRUNCATE mart_cross_market_prices"))
            for r in rows:
                conn.execute(text(
                    "INSERT INTO mart_cross_market_prices "
                    "  (question_key, captured_at, kalshi_prob, polymarket_prob, "
                    "   sportsbook_consensus_prob, max_spread) "
                    "VALUES (:question_key, now(), :kalshi_prob, :polymarket_prob, "
                    "        :sportsbook_consensus_prob, :max_spread)"), r)
        return len(rows)
    finally:
        engine.dispose()
