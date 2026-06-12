"""Blast-radius guard: spreads/totals rows must never reach the fair-price consensus
or edge signals, which group by (event, market, book, outcome) with no per-line key.

The guard lives as `WHERE market_type = 'h2h'` in _latest_odds_rows (the single SQL
read feeding both build_fair_prices and build_edge_signals). We can't call that
function against sqlite (its SELECT uses Postgres `event_id::text`), so this test
seeds an in-memory sqlite table and exercises the same filter the guard applies,
proving a totals row is excluded while the h2h rows survive.
"""
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from bountygate.transforms.marts.edge_signals import compute_edge_signals

_DDL = (
    "CREATE TABLE sportsbook_odds_history (event_id text, market_type text, "
    "bookmaker text, outcome_name text, captured_at text, decimal_price real, point real)"
)


def _seed():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text(_DDL))
        rows = [
            # h2h pair (should feed consensus/edges)
            ("e1", "h2h", "pinnacle", "Mets", "2026-06-10T18:00:00Z", 1.91, None),
            ("e1", "h2h", "pinnacle", "Padres", "2026-06-10T18:00:00Z", 1.91, None),
            # totals lines on the SAME event (must be excluded; same Over/Under names
            # at different points would otherwise collapse into a bogus two-way pair)
            ("e1", "totals", "fanduel", "Over", "2026-06-10T18:00:00Z", 1.91, 8.5),
            ("e1", "totals", "fanduel", "Under", "2026-06-10T18:00:00Z", 1.91, 8.5),
            ("e1", "totals", "fanduel", "Over", "2026-06-10T18:00:00Z", 1.95, 9.5),
        ]
        for r in rows:
            conn.execute(text(
                "INSERT INTO sportsbook_odds_history VALUES "
                "(:e, :mt, :b, :o, :c, :p, :pt)"),
                {"e": r[0], "mt": r[1], "b": r[2], "o": r[3], "c": r[4],
                 "p": r[5], "pt": r[6]})
    return engine


# the portable WHERE the guard applies inside _latest_odds_rows (sans Postgres cast)
_GUARD_SQL = (
    "SELECT event_id, market_type, bookmaker, outcome_name, decimal_price "
    "FROM sportsbook_odds_history WHERE market_type = 'h2h'"
)


def test_guard_excludes_totals_rows():
    engine = _seed()
    try:
        with engine.begin() as conn:
            rows = [dict(r) for r in conn.execute(text(_GUARD_SQL)).mappings()]
    finally:
        engine.dispose()
    assert {r["market_type"] for r in rows} == {"h2h"}
    assert len(rows) == 2          # totals rows dropped, h2h pair kept
    assert all(r["outcome_name"] not in ("Over", "Under") for r in rows)


def test_totals_excluded_means_no_corrupt_edge_signals():
    # If the totals rows had leaked through, compute_edge_signals would see THREE
    # outcome names (Mets/Padres + Over/Under) for the event or a duplicated Over,
    # corrupting the two-way pairing. With the guard, only the clean h2h pair feeds it.
    engine = _seed()
    try:
        with engine.begin() as conn:
            guarded = [dict(r) for r in conn.execute(text(_GUARD_SQL)).mappings()]
    finally:
        engine.dispose()
    signals = compute_edge_signals(guarded, threshold=0.0)
    # all signals come from the h2h market only
    assert all(s["market_type"] == "h2h" for s in signals)
    assert {s["outcome_name"] for s in signals} <= {"Mets", "Padres"}
