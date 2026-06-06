from __future__ import annotations

from bountygate.connectors.base import Connector
from bountygate.connectors.kalshi import KalshiConnector
from bountygate.connectors.odds_api import OddsApiConnector
from bountygate.connectors.polymarket import PolymarketConnector

CONNECTORS: dict[str, Connector] = {
    KalshiConnector.source: KalshiConnector(),
    PolymarketConnector.source: PolymarketConnector(),
    OddsApiConnector.source: OddsApiConnector(),
}


def get_connector(source: str) -> Connector:
    return CONNECTORS[source]
