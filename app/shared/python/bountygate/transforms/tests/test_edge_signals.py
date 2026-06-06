from bountygate.transforms.marts.edge_signals import compute_edge_signals


def _row(event_id, book, name, price, mt="h2h"):
    return {"event_id": event_id, "market_type": mt, "bookmaker": book,
            "outcome_name": name, "decimal_price": price}


def test_pinnacle_anchored_ev_signal():
    # Pinnacle symmetric 1.9091/1.9091 -> fair 0.5/0.5. Soft book offers 2.2 on Mets -> +EV.
    rows = [
        _row("e1", "pinnacle", "Mets", 1.9091), _row("e1", "pinnacle", "Padres", 1.9091),
        _row("e1", "softbook", "Mets", 2.20),  _row("e1", "softbook", "Padres", 1.74),
    ]
    out = compute_edge_signals(rows, threshold=0.025)
    ev = [s for s in out if s["signal_type"] == "ev"]
    mets_ev = next(s for s in ev if s["bookmaker"] == "softbook" and s["outcome_name"] == "Mets")
    assert abs(mets_ev["fair_prob"] - 0.5) < 1e-3
    assert mets_ev["venue_price"] == 2.20
    assert mets_ev["edge"] > 0.025          # 0.5*2.2 - 1 = 0.10
    assert mets_ev["kelly_fraction"] > 0
    # Pinnacle itself is never a soft/EV book
    assert all(s["bookmaker"] != "pinnacle" for s in ev)


def test_consensus_fallback_when_no_pinnacle():
    # No Pinnacle -> consensus fair (~0.5 each). bookA at 2.10/2.10 yields +EV vs that fair.
    rows = [
        _row("e2", "bookA", "Over", 2.10), _row("e2", "bookA", "Under", 2.10),
        _row("e2", "bookB", "Over", 1.95), _row("e2", "bookB", "Under", 1.95),
    ]
    out = compute_edge_signals(rows, threshold=0.0)
    assert out, "expected signals via consensus fallback"
    assert all(0.0 <= s["fair_prob"] <= 1.0 for s in out)


def test_arb_signal_detected():
    # 1/2.10 + 1/2.10 = 0.952 < 1 -> guaranteed-profit arb across the two outcomes.
    rows = [
        _row("e3", "pinnacle", "A", 1.90), _row("e3", "pinnacle", "B", 1.90),
        _row("e3", "bookX", "A", 2.10),    _row("e3", "bookY", "B", 2.10),
    ]
    out = compute_edge_signals(rows, threshold=0.025)
    arbs = [s for s in out if s["signal_type"] == "arb"]
    assert arbs, "expected an arb signal"
    assert all(s["edge"] > 0 for s in arbs)


def test_skips_non_two_way_markets():
    rows = [_row("e4", "pinnacle", "A", 2.0), _row("e4", "pinnacle", "B", 3.0),
            _row("e4", "pinnacle", "C", 4.0)]
    assert compute_edge_signals(rows) == []
