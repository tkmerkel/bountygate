from bountygate.connectors.base import Connector
from bountygate.connectors.registry import CONNECTORS, get_connector


def test_registry_has_three_sources():
    assert set(CONNECTORS) == {"kalshi", "polymarket", "the_odds_api"}


def test_registry_values_are_connectors():
    for source, conn in CONNECTORS.items():
        assert isinstance(conn, Connector)
        assert conn.source == source


def test_get_connector_returns_instance():
    assert get_connector("polymarket").source == "polymarket"
