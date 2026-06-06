from bountygate.transforms.marts.market_history import compute_market_history


def test_clv_and_realized_for_resolved_market():
    market = {
        "market_id": "m1", "resolved_outcome": "Yes",
        "close_time": "2026-06-06T12:00:00Z", "resolution_time": "2026-06-06T12:00:00Z",
        "tracked_outcome": "Yes",
    }
    # price points (captured_at, price) for the tracked outcome
    points = [
        ("2026-06-06T09:00:00Z", 0.40),   # >= 1h before close -> predicted
        ("2026-06-06T11:30:00Z", 0.55),   # < 1h before close
        ("2026-06-06T11:59:00Z", 0.60),   # closing (last before close)
    ]
    out = compute_market_history([market], {"m1": points})
    assert len(out) == 1
    row = out[0]
    assert row["market_id"] == "m1"
    assert row["predicted_prob"] == 0.40
    assert row["realized"] is True
    # clv_from_fair(0.40, 0.60) = 0.60/0.40 - 1 = 0.5
    assert abs(row["clv"] - 0.5) < 1e-9


def test_skips_market_with_no_prior_horizon_point():
    market = {"market_id": "m2", "resolved_outcome": "No", "close_time": "2026-06-06T12:00:00Z",
              "resolution_time": "2026-06-06T12:00:00Z", "tracked_outcome": "Yes"}
    points = [("2026-06-06T11:40:00Z", 0.7)]   # only inside the 1h horizon
    out = compute_market_history([market], {"m2": points})
    assert out == []
