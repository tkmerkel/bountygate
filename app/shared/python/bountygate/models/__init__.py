"""DB I/O for the quant core: snapshots -> fair prices, closing lines, results, scoring.

Mirrors transforms/marts: pure logic lives in fair/weights/closing/scoring; this
module owns SQL. All builders create and dispose their own engine.
"""
from __future__ import annotations

import os

from sqlalchemy import create_engine, text

SPORTS = ("baseball_mlb", "basketball_nba", "icehockey_nhl")
MARKET_TYPES = ("h2h", "totals")
CONSENSUS_KEY, CONSENSUS_VERSION = "consensus_v1", "1"
_MIN_PROB_DELTA = 0.001   # skip model_predictions insert when move is smaller


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def _sharpness_stats(conn) -> dict:
    # venue_sharpness is per (venue, sport); blend weights are global, so
    # aggregate: game-count-weighted mean brier + total games per venue.
    return {
        r["venue_key"]: {"brier": float(r["brier"]), "n_games": r["n_games"]}
        for r in conn.execute(text(
            "SELECT venue_key, "
            "       sum(brier * n_games) / sum(n_games) AS brier, "
            "       sum(n_games) AS n_games "
            "FROM venue_sharpness "
            "WHERE score_window = 'all' AND brier IS NOT NULL AND n_games > 0 "
            "GROUP BY venue_key")).mappings()
    }


def build_fair_prices() -> int:
    """Latest snapshot per (event, market, book) -> per-method fair_prices rows,
    consensus_v1 model_predictions (on-change only), and a mart_fair_odds rebuild."""
    from bountygate.models.fair import book_fair_probs, weighted_consensus
    from bountygate.models.weights import sharpness_weights
    from bountygate.transforms.marts import _latest_odds_rows

    engine = _engine()
    try:
        with engine.begin() as conn:
            events = {
                r["event_id"]: dict(r)
                for r in conn.execute(text(
                    "SELECT event_id::text AS event_id, sport_key, commence_time, "
                    "       home_team, away_team FROM sports_events "
                    "WHERE sport_key = ANY(:sports)"), {"sports": list(SPORTS)}).mappings()
            }
            weights = sharpness_weights(_sharpness_stats(conn))
            last_pred = {
                (r["event_id"], r["market_type"], r["outcome_name"]): float(r["prob"])
                for r in conn.execute(text(
                    "SELECT DISTINCT ON (event_id, market_type, outcome_name) "
                    "       event_id::text AS event_id, market_type, outcome_name, prob "
                    "FROM model_predictions WHERE model_key = :mk "
                    "ORDER BY event_id, market_type, outcome_name, predicted_at DESC"),
                    {"mk": CONSENSUS_KEY}).mappings()
            }
            conn.execute(text(
                "INSERT INTO model_versions (model_key, version, description) "
                "VALUES (:mk, :v, 'Sharpness-weighted no-vig consensus') "
                "ON CONFLICT DO NOTHING"), {"mk": CONSENSUS_KEY, "v": CONSENSUS_VERSION})

            groups: dict = {}
            for r in _latest_odds_rows(conn):
                if r["event_id"] not in events or r["market_type"] not in MARKET_TYPES:
                    continue
                groups.setdefault((r["event_id"], r["market_type"]), {}) \
                      .setdefault(r["bookmaker"], {})[r["outcome_name"]] = r["decimal_price"]

            n_rows = 0
            mart_rows = []
            for (eid, mtype), by_book in groups.items():
                names = sorted({n for prices in by_book.values() for n in prices})
                if len(names) != 2:
                    continue
                fair = book_fair_probs(by_book, names)
                for book, methods in fair.items():
                    for method, probs in methods.items():
                        for name, prob in probs.items():
                            conn.execute(text(
                                "INSERT INTO fair_prices (event_id, market_type, bookmaker, "
                                "  outcome_name, method, fair_prob, captured_at) "
                                "VALUES (cast(:eid AS uuid), :mt, :book, :name, :method, "
                                "        :prob, now())"),
                                {"eid": eid, "mt": mtype, "book": book, "name": name,
                                 "method": method, "prob": prob})
                            n_rows += 1
                cons = weighted_consensus({b: m["shin"] for b, m in fair.items()}, weights)
                if cons is None:
                    continue
                ev = events[eid]
                for name, prob in cons.items():
                    conn.execute(text(
                        "INSERT INTO fair_prices (event_id, market_type, bookmaker, "
                        "  outcome_name, method, fair_prob, captured_at) "
                        "VALUES (cast(:eid AS uuid), :mt, 'consensus', :name, 'weighted', "
                        "        :prob, now())"),
                        {"eid": eid, "mt": mtype, "name": name, "prob": prob})
                    n_rows += 1
                    prev = last_pred.get((eid, mtype, name))
                    if prev is None or abs(prob - prev) > _MIN_PROB_DELTA:
                        conn.execute(text(
                            "INSERT INTO model_predictions (model_key, version, event_id, "
                            "  market_type, outcome_name, prob, predicted_at) "
                            "VALUES (:mk, :v, cast(:eid AS uuid), :mt, :name, :prob, now())"),
                            {"mk": CONSENSUS_KEY, "v": CONSENSUS_VERSION, "eid": eid,
                             "mt": mtype, "name": name, "prob": prob})
                    best = max(
                        ((float(p[name]), b) for b, p in by_book.items() if p.get(name)),
                        default=None)
                    mart_rows.append({
                        "eid": eid, "sport": ev["sport_key"], "ct": ev["commence_time"],
                        "home": ev["home_team"], "away": ev["away_team"], "mt": mtype,
                        "name": name, "prob": prob,
                        "best_price": best[0] if best else None,
                        "best_book": best[1] if best else None,
                        "edge": (prob * best[0] - 1.0) if best else None,
                    })

            conn.execute(text("TRUNCATE mart_fair_odds"))
            for m in mart_rows:
                conn.execute(text(
                    "INSERT INTO mart_fair_odds (event_id, sport_key, commence_time, "
                    "  home_team, away_team, market_type, outcome_name, consensus_prob, "
                    "  best_price, best_bookmaker, edge, computed_at) "
                    "VALUES (cast(:eid AS uuid), :sport, :ct, :home, :away, :mt, :name, "
                    "        :prob, :best_price, :best_book, :edge, now())"), m)
        return n_rows
    finally:
        engine.dispose()


def derive_closing_lines_db(*, lookback_days: int = 7) -> tuple[int, list]:
    """Derive closing lines for commenced events that have none yet.

    Returns (events_processed, stale) where stale is [(event_id, staleness_minutes)]
    for h2h consensus rows with staleness > 60 (the ingest-gap signal).
    """
    from bountygate.models.closing import derive_closing
    from bountygate.models.fair import weighted_consensus
    from bountygate.models.weights import sharpness_weights

    engine = _engine()
    stale: list = []
    n_events = 0
    try:
        with engine.begin() as conn:
            weights = sharpness_weights(_sharpness_stats(conn))
            pending = conn.execute(text(
                "SELECT e.event_id::text AS event_id, e.commence_time "
                "FROM sports_events e "
                "WHERE e.sport_key = ANY(:sports) AND e.commence_time < now() "
                "  AND e.commence_time > now() - make_interval(days => :days) "
                "  AND NOT EXISTS (SELECT 1 FROM closing_lines c "
                "                  WHERE c.event_id = e.event_id)"),
                {"sports": list(SPORTS), "days": lookback_days}).mappings().all()
            for ev in pending:
                wrote_any = False
                for mtype in MARKET_TYPES:
                    rows = [dict(r) for r in conn.execute(text(
                        "SELECT bookmaker, outcome_name, decimal_price, captured_at "
                        "FROM sportsbook_odds_history "
                        "WHERE event_id = cast(:eid AS uuid) AND market_type = :mt "
                        "  AND captured_at <= :ct"),
                        {"eid": ev["event_id"], "mt": mtype,
                         "ct": ev["commence_time"]}).mappings()]
                    closing = derive_closing(rows, ev["commence_time"])
                    if not closing:
                        continue
                    for c in closing:
                        conn.execute(text(
                            "INSERT INTO closing_lines (event_id, market_type, bookmaker, "
                            "  outcome_name, decimal_price, fair_prob, captured_at, "
                            "  staleness_minutes) "
                            "VALUES (cast(:eid AS uuid), :mt, :book, :name, :price, :fair, "
                            "        :cat, :stale) "
                            "ON CONFLICT (event_id, market_type, bookmaker, outcome_name) "
                            "DO NOTHING"),
                            {"eid": ev["event_id"], "mt": mtype, "book": c["bookmaker"],
                             "name": c["outcome_name"], "price": c["decimal_price"],
                             "fair": c["fair_prob"], "cat": c["captured_at"],
                             "stale": c["staleness_minutes"]})
                    wrote_any = True
                    # consensus closing row (the CLV reference)
                    probs_by_book: dict = {}
                    for c in closing:
                        if c["fair_prob"] is not None:
                            probs_by_book.setdefault(c["bookmaker"], {})[
                                c["outcome_name"]] = c["fair_prob"]
                    cons = weighted_consensus(probs_by_book, weights)
                    if cons:
                        # staleness/captured_at reflect only books that fed the consensus
                        contributing = [c for c in closing if c["bookmaker"] in probs_by_book]
                        worst = max(c["staleness_minutes"] for c in contributing)
                        latest = max(c["captured_at"] for c in contributing)
                        for name, prob in cons.items():
                            conn.execute(text(
                                "INSERT INTO closing_lines (event_id, market_type, "
                                "  bookmaker, outcome_name, decimal_price, fair_prob, "
                                "  captured_at, staleness_minutes) "
                                "VALUES (cast(:eid AS uuid), :mt, 'consensus', :name, "
                                "        NULL, :prob, :cat, :stale) "
                                "ON CONFLICT (event_id, market_type, bookmaker, "
                                "  outcome_name) DO NOTHING"),
                                {"eid": ev["event_id"], "mt": mtype, "name": name,
                                 "prob": prob, "cat": latest, "stale": worst})
                        if mtype == "h2h" and worst > 60:
                            stale.append((ev["event_id"], round(worst, 1)))
                if wrote_any:
                    n_events += 1
        return n_events, stale
    finally:
        engine.dispose()


def ingest_game_results() -> int:
    """ESPN scoreboard finals (today + yesterday UTC) -> game_results upserts.

    winner is 'home'/'away', resolved by matching the feed game to sports_events
    via team-name+date (enrichment.match), so odds-vs-feed naming never matters.
    """
    from datetime import date, timedelta

    from bountygate.enrichment.clients import build_espn_scoreboard_url, fetch_json
    from bountygate.enrichment.match import match_game_to_event
    from bountygate.enrichment.results import parse_espn_scoreboard

    engine = _engine()
    try:
        with engine.begin() as conn:
            events = [dict(r) for r in conn.execute(text(
                "SELECT event_id::text AS bg_event_id, sport_key, "
                "       home_team AS home_team_name, away_team AS away_team_name, "
                "       commence_time AS commence_at_utc "
                "FROM sports_events "
                "WHERE sport_key = ANY(:sports) "
                "  AND commence_time > now() - interval '3 days'"),
                {"sports": list(SPORTS)}).mappings()]
            n = 0
            for sport in SPORTS:
                for d in (date.today(), date.today() - timedelta(days=1)):
                    payload = fetch_json(build_espn_scoreboard_url(sport, d))
                    for g in parse_espn_scoreboard(payload or {}, sport):
                        if not g.get("completed"):
                            continue
                        hs, as_ = g.get("home_score"), g.get("away_score")
                        if hs is None or as_ is None or hs == as_:
                            continue
                        eid = match_game_to_event(
                            sport, g["home_team_name"], g["away_team_name"],
                            g["commence_at_utc"], events)
                        if not eid or eid == "None":
                            continue
                        conn.execute(text(
                            "INSERT INTO game_results (event_id, home_score, away_score, "
                            "  winner, completed_at, source) "
                            "VALUES (cast(:eid AS uuid), :hs, :as_, :w, now(), 'espn') "
                            "ON CONFLICT (event_id) DO UPDATE SET "
                            "  home_score = EXCLUDED.home_score, "
                            "  away_score = EXCLUDED.away_score, "
                            "  winner = EXCLUDED.winner, "
                            "  completed_at = EXCLUDED.completed_at"),
                            {"eid": eid, "hs": hs, "as_": as_,
                             "w": "home" if hs > as_ else "away"})
                        n += 1
            return n
    finally:
        engine.dispose()
