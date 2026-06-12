from datetime import datetime, timedelta, timezone

import pytest

from bountygate.models.closing import derive_closing

T0 = datetime(2026, 6, 10, 19, 0, tzinfo=timezone.utc)   # commence


def _row(book, name, price, minutes_before):
    return {"bookmaker": book, "outcome_name": name, "decimal_price": price,
            "captured_at": T0 - timedelta(minutes=minutes_before)}


def test_picks_last_pre_commence_snapshot_per_book_outcome():
    rows = [
        _row("fanduel", "A", 1.90, 30),
        _row("fanduel", "A", 1.95, 5),       # latest pre-commence -> wins
        _row("fanduel", "B", 1.90, 5),
        {"bookmaker": "fanduel", "outcome_name": "A", "decimal_price": 2.10,
         "captured_at": T0 + timedelta(minutes=5)},   # post-commence -> ignored
    ]
    out = derive_closing(rows, T0)
    a = next(r for r in out if r["outcome_name"] == "A")
    assert a["decimal_price"] == 1.95
    assert a["staleness_minutes"] == pytest.approx(5.0)


def test_two_sided_book_gets_mult_devig_fair_prob():
    rows = [_row("fanduel", "A", 1.9091, 5), _row("fanduel", "B", 1.9091, 5)]
    out = derive_closing(rows, T0)
    assert all(r["fair_prob"] == pytest.approx(0.5, abs=1e-6) for r in out)


def test_one_sided_book_has_null_fair_prob_but_keeps_price():
    out = derive_closing([_row("dk", "A", 1.87, 10)], T0)
    assert len(out) == 1
    assert out[0]["fair_prob"] is None and out[0]["decimal_price"] == 1.87


def test_invalid_prices_skipped():
    assert derive_closing([_row("dk", "A", 1.0, 10), _row("dk", "A", None, 5)], T0) == []
