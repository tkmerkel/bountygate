from bountygate.transforms.parsers.kalshi import parse_kalshi


def test_parse_kalshi_market_outcomes_and_prices():
    payload = {
        "title": "Will Kansas City win?",
        "ticker": "KXNFLGAME-26SEP14DENKC-KC",
        "series_ticker": "KXNFLGAME",
        "status": "active",
        "yes_bid": 0.53, "yes_ask": 0.64,
        "no_bid": 0.36, "no_ask": 0.47,
        "open_interest": 213.41, "liquidity_dollars": 0.0,
    }
    out = parse_kalshi(payload)
    assert out["market"]["venue_key"] == "kalshi"
    assert out["market"]["external_id"] == "KXNFLGAME-26SEP14DENKC-KC"
    assert out["market"]["title"] == "Will Kansas City win?"
    assert out["market"]["category"] == "KXNFLGAME"
    assert out["market"]["status"] == "active"
    names = [o["outcome_name"] for o in out["outcomes"]]
    assert names == ["Yes", "No"]
    yes = next(o for o in out["outcomes"] if o["outcome_name"] == "Yes")
    assert abs(yes["last_price"] - 0.585) < 1e-9   # (0.53+0.64)/2
    yes_price = next(p for p in out["prices"] if p["outcome_name"] == "Yes")
    assert yes_price["bid"] == 0.53 and yes_price["ask"] == 0.64
    assert abs(yes_price["price"] - 0.585) < 1e-9
    assert yes_price["volume"] == 213.41


def test_parse_kalshi_missing_quote_yields_none_price():
    payload = {"ticker": "KX-X", "title": "t", "series_ticker": "S", "status": "active",
               "yes_bid": None, "yes_ask": None, "no_bid": 0.4, "no_ask": 0.5}
    out = parse_kalshi(payload)
    yes = next(o for o in out["outcomes"] if o["outcome_name"] == "Yes")
    assert yes["last_price"] is None
