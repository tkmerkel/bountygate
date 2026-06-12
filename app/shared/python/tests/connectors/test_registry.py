from bountygate.connectors.base import Connector
from bountygate.connectors.registry import CONNECTORS, get_connector


def test_registry_has_expected_sources():
    assert set(CONNECTORS) == {
        "kalshi",
        "polymarket",
        "the_odds_api",
        "the_odds_api_props",
    }


def test_registry_values_are_connectors():
    for conn in CONNECTORS.values():
        assert isinstance(conn, Connector)


def test_registry_keys_match_source_except_props_alias():
    # Every entry's key equals its landing source, except the props alias which
    # deliberately reuses the_odds_api landing source under a distinct key.
    for source, conn in CONNECTORS.items():
        if source == "the_odds_api_props":
            assert conn.source == "the_odds_api"
        else:
            assert conn.source == source


def test_get_connector_returns_instance():
    assert get_connector("polymarket").source == "polymarket"
