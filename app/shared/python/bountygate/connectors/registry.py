from __future__ import annotations

import os

from bountygate.arb.markets import props_sports
from bountygate.connectors.base import Connector
from bountygate.connectors.kalshi import KalshiConnector
from bountygate.connectors.odds_api import OddsApiConnector
from bountygate.connectors.polymarket import PolymarketConnector

CONNECTORS: dict[str, Connector] = {
    KalshiConnector.source: KalshiConnector(),
    PolymarketConnector.source: PolymarketConnector(),
    OddsApiConnector.source: OddsApiConnector(
        regions="us,eu",
        markets="h2h,spreads,totals",
        window_hours=int(os.getenv("BG_ODDS_WINDOW_HOURS", "48")),
    ),
    "the_odds_api_props": OddsApiConnector(
        sport_keys={k: k for k in props_sports()},
        regions="us",
        markets_by_sport=props_sports(),
        window_hours=int(os.getenv("BG_PROPS_WINDOW_HOURS", "24")),
        record_type="player_prop",
    ),
}


def get_connector(source: str) -> Connector:
    return CONNECTORS[source]
