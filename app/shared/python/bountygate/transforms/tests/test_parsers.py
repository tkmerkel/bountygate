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


from bountygate.transforms.parsers.polymarket import parse_polymarket


def test_parse_polymarket_zips_outcomes_and_prices():
    payload = {
        "condition_id": "0xabc", "question": "New Rihanna Album before GTA VI?",
        "slug": "rihanna", "active": True, "closed": False,
        "volume": 818640.13, "liquidity": 19582.37, "end_date": "2026-07-31T12:00:00Z",
        "outcomes": ["Yes", "No"], "outcome_prices": [0.545, 0.455],
    }
    out = parse_polymarket(payload)
    assert out["market"]["venue_key"] == "polymarket"
    assert out["market"]["external_id"] == "0xabc"
    assert out["market"]["title"] == "New Rihanna Album before GTA VI?"
    assert out["market"]["status"] == "active"
    assert out["market"]["close_time"] == "2026-07-31T12:00:00Z"
    yes = next(o for o in out["outcomes"] if o["outcome_name"] == "Yes")
    assert yes["outcome_index"] == 0 and yes["last_price"] == 0.545
    yes_price = next(p for p in out["prices"] if p["outcome_name"] == "Yes")
    assert yes_price["price"] == 0.545 and yes_price["volume"] == 818640.13
    assert yes_price["liquidity"] == 19582.37


def test_parse_polymarket_closed_status():
    payload = {"condition_id": "0xd", "question": "q", "active": False, "closed": True,
               "outcomes": ["Yes", "No"], "outcome_prices": [1.0, 0.0]}
    out = parse_polymarket(payload)
    assert out["market"]["status"] == "closed"


from bountygate.transforms.parsers.odds import parse_odds_line


def test_parse_odds_line_event_and_odds():
    payload = {
        "event_id": "evt1", "sport_key": "baseball_mlb",
        "home_team": "San Diego Padres", "away_team": "New York Mets",
        "commence_time": "2026-06-07T02:11:00Z", "market": "h2h", "bookmaker": "pinnacle",
        "outcomes": [{"name": "New York Mets", "price": 1.82},
                     {"name": "San Diego Padres", "price": 2.04}],
    }
    out = parse_odds_line(payload)
    assert out["event"]["source_event_id"] == "evt1"
    assert out["event"]["sport_key"] == "baseball_mlb"
    assert out["event"]["home_team"] == "San Diego Padres"
    assert len(out["odds"]) == 2
    mets = next(o for o in out["odds"] if o["outcome_name"] == "New York Mets")
    assert mets["bookmaker"] == "pinnacle" and mets["market_type"] == "h2h"
    assert mets["decimal_price"] == 1.82
