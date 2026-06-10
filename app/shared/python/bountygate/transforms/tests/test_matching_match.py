from bountygate.transforms.matching.match import link_rows

EVENTS = [
    {"event_id": "E1", "sport_key": "baseball_mlb",
     "commence_time": "2026-05-03T14:30:00Z",
     "home_team": "St. Louis Cardinals", "away_team": "Los Angeles Dodgers"},
]


def test_kalshi_links_to_event():
    markets = [{"market_id": "M1", "venue_key": "kalshi",
                "external_id": "KXMLBGAME-26MAY031415LADSTL-LAD",
                "title": "Will the Dodgers win?", "category": "KXMLBGAME",
                "close_time": None}]
    r = link_rows(markets, EVENTS)
    assert r["links"] == [{"market_id": "M1", "event_id": "E1",
                           "confidence": 1.0, "method": "kalshi_ticker"}]
    assert r["stats"]["kalshi"] == 1


def test_polymarket_links_via_title():
    markets = [{"market_id": "M2", "venue_key": "polymarket",
                "external_id": "0xabc", "title": "Will the Dodgers beat the Cardinals?",
                "category": None, "close_time": "2026-05-03T23:59:00Z"}]
    r = link_rows(markets, EVENTS)
    assert len(r["links"]) == 1
    assert r["links"][0]["method"] == "polymarket_text"
    assert r["stats"]["polymarket"] == 1


def test_out_of_window_does_not_link():
    markets = [{"market_id": "M3", "venue_key": "kalshi",
                "external_id": "KXMLBGAME-26MAY051415LADSTL-LAD",  # May 5; event is May 3
                "title": "", "category": "KXMLBGAME", "close_time": None}]
    r = link_rows(markets, EVENTS)
    assert r["links"] == []
    assert r["stats"]["unmatched"] == 1


def test_ambiguous_two_events_same_teams_in_window():
    events = EVENTS + [{"event_id": "E2", "sport_key": "baseball_mlb",
                        "commence_time": "2026-05-03T20:00:00Z",
                        "home_team": "St. Louis Cardinals", "away_team": "Los Angeles Dodgers"}]
    markets = [{"market_id": "M4", "venue_key": "kalshi",
                "external_id": "KXMLBGAME-26MAY031415LADSTL-LAD",
                "title": "", "category": "KXMLBGAME", "close_time": None}]
    r = link_rows(markets, events)
    assert r["links"] == []
    assert r["stats"]["ambiguous"] == 1


def test_non_sports_market_unmatched():
    markets = [{"market_id": "M5", "venue_key": "polymarket",
                "external_id": "0xff", "title": "Will it rain in NYC tomorrow?",
                "category": None, "close_time": "2026-05-03T23:59:00Z"}]
    r = link_rows(markets, EVENTS)
    assert r["links"] == []
    assert r["stats"]["unmatched"] == 1
