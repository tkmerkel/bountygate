from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class RawRecord:
    """A single normalized snapshot from a source, ready for raw landing."""

    source: str          # 'kalshi' | 'polymarket' | 'the_odds_api'
    source_key: str      # natural id: ticker / condition_id / f"{event_id}:{market}:{book}"
    record_type: str     # 'market' | 'orderbook' | 'odds_line'
    captured_at: datetime  # fetch time, UTC, tz-aware
    payload: dict[str, Any]


class Connector(ABC):
    """Uniform read-only source interface. Subclasses set `source` and implement
    `fetch_snapshots`. Keep network I/O in `fetch_snapshots`; keep parsing in a
    pure `normalize(...)` staticmethod so it can be fixture-tested."""

    source: str = ""

    @abstractmethod
    def fetch_snapshots(self) -> list[RawRecord]:
        """Fetch current data from the source and return normalized RawRecords."""
        raise NotImplementedError
