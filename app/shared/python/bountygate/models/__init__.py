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
    return {
        r["venue_key"]: {"brier": float(r["brier"]), "n_games": r["n_games"]}
        for r in conn.execute(text(
            "SELECT venue_key, brier, n_games FROM venue_sharpness "
            "WHERE score_window = 'all' AND brier IS NOT NULL")).mappings()
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
