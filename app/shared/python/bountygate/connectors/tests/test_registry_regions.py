import os
import sys

_PKG_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

from bountygate.connectors.registry import get_connector


def test_odds_connector_requests_us_and_eu_regions():
    conn = get_connector("the_odds_api")
    assert conn.regions == "us,eu"
