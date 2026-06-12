from __future__ import annotations

import os

from bountygate.arb.markets import props_sports
from bountygate.connectors.base import Connector
from bountygate.connectors.kalshi import KalshiConnector
from bountygate.connectors.odds_api import OddsApiConnector
from bountygate.connectors.polymarket import PolymarketConnector


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"[registry] {name}={raw!r} is not an int; using default {default}")
        return default


CONNECTORS: dict[str, Connector] = {
    KalshiConnector.source: KalshiConnector(),
    PolymarketConnector.source: PolymarketConnector(),
    OddsApiConnector.source: OddsApiConnector(
        regions="us,eu",
        markets="h2h,spreads,totals",
        window_hours=_env_int("BG_ODDS_WINDOW_HOURS", 48),
    ),
    "the_odds_api_props": OddsApiConnector(
        sport_keys={k: k for k in props_sports()},
        regions="us",
        markets_by_sport=props_sports(),
        window_hours=_env_int("BG_PROPS_WINDOW_HOURS", 24),
        record_type="player_prop",
    ),
}


def get_connector(source: str) -> Connector:
    return CONNECTORS[source]
