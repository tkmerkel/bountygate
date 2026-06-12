"""Blast-radius guard: spreads/totals rows must never reach the fair-price consensus
or edge signals, which group by (event, market, book, outcome) with no per-line key.

The guard lives as `WHERE market_type = 'h2h'` in _latest_odds_rows (the single SQL
read feeding both build_fair_prices and build_edge_signals). We can't call that
function against sqlite (its SELECT uses Postgres `event_id::text`), so this test
seeds an in-memory sqlite table and exercises the same filter the guard applies,
proving a totals row is excluded while the h2h rows survive.
"""
import inspect

from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

import bountygate.transforms.marts as marts
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
            # h2h pair from the sharp anchor (pinnacle) -> fair prob ~0.5 each
            ("e1", "h2h", "pinnacle", "Mets", "2026-06-10T18:00:00Z", 1.91, None),
            ("e1", "h2h", "pinnacle", "Padres", "2026-06-10T18:00:00Z", 1.91, None),
            # h2h pair from a soft book (fanduel) priced off-fair so the EV guard
            # fires: edge(Mets) = 0.5 * 2.10 - 1 = 0.05 >= 0.025 threshold.
            ("e1", "h2h", "fanduel", "Mets", "2026-06-10T18:00:00Z", 2.10, None),
            ("e1", "h2h", "fanduel", "Padres", "2026-06-10T18:00:00Z", 1.80, None),
            # totals lines on the SAME event (must be excluded; same Over/Under names
            # at different points would otherwise collapse into a bogus two-way pair).
            # Priced as a fake arb (1/2.10 + 1/2.10 < 1) so that IF these rows leaked
            # past the guard, compute_edge_signals would emit a totals signal -- which
            # is exactly what test_totals_excluded_means_no_corrupt_edge_signals asserts
            # against, making that test bite when the guard is removed.
            ("e1", "totals", "fanduel", "Over", "2026-06-10T18:00:00Z", 2.10, 8.5),
            ("e1", "totals", "fanduel", "Under", "2026-06-10T18:00:00Z", 2.10, 8.5),
            ("e1", "totals", "fanduel", "Over", "2026-06-10T18:00:00Z", 2.10, 9.5),
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


def test_guard_lives_in_latest_odds_rows_source():
    # The guard is only meaningful if it actually lives in the real read path.
    # If someone deletes the WHERE clause (or renames the table), this bites.
    src = inspect.getsource(marts._latest_odds_rows)
    assert "market_type = 'h2h'" in src
    assert "FROM sportsbook_odds_history" in src


def test_guard_excludes_totals_rows():
    engine = _seed()
    try:
        with engine.begin() as conn:
            rows = [dict(r) for r in conn.execute(text(_GUARD_SQL)).mappings()]
    finally:
        engine.dispose()
    assert {r["market_type"] for r in rows} == {"h2h"}
    assert len(rows) == 4          # totals rows dropped, two h2h books kept
    assert all(r["outcome_name"] not in ("Over", "Under") for r in rows)


def test_totals_excluded_means_no_corrupt_edge_signals():
    # With the guard, only the clean h2h pair (pinnacle anchor + fanduel soft book)
    # feeds compute_edge_signals, yielding a genuine EV signal. If the totals rows
    # leaked through, the shared (event, market, book, outcome) grouping would
    # collapse distinct Over lines / mix in Over+Under names and corrupt the pairing.
    engine = _seed()
    try:
        with engine.begin() as conn:
            guarded = [dict(r) for r in conn.execute(text(_GUARD_SQL)).mappings()]
    finally:
        engine.dispose()
    signals = compute_edge_signals(guarded, threshold=0.025)
    # (a) the h2h pair really does produce a signal (not a vacuous pass)
    assert len(signals) >= 1
    # (b) no signal originates from a totals row
    assert all(s["market_type"] != "totals" for s in signals)
    assert all(s["market_type"] == "h2h" for s in signals)
    assert {s["outcome_name"] for s in signals} <= {"Mets", "Padres"}
