import os
import sys

_PKG_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

from bountygate.connectors.registry import get_connector


def test_odds_connector_requests_us_and_eu_regions():
    conn = get_connector("the_odds_api")
    assert conn.regions == "us,eu"


def test_odds_connector_requests_h2h_spreads_totals():
    conn = get_connector("the_odds_api")
    assert conn.markets == "h2h,spreads,totals"
    assert conn.record_type == "odds_line"


def test_props_connector_registered():
    conn = get_connector("the_odds_api_props")
    assert conn.source == "the_odds_api"  # landing source unchanged
    assert conn.regions == "us"
    assert conn.record_type == "player_prop"
    assert conn.markets_by_sport is not None
    # Per-sport markets are wired from props_sports().
    assert "basketball_nba" in conn.markets_by_sport
