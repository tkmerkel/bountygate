from __future__ import annotations

import json
from datetime import datetime, timezone

import requests

from bountygate.connectors.base import Connector, RawRecord

GAMMA_BASE = "https://gamma-api.polymarket.com"


def _maybe_json(value):
    """Gamma returns `outcomes`/`outcomePrices` as JSON-encoded strings."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class PolymarketConnector(Connector):
    """Read-only Polymarket market data via the public Gamma API (no auth)."""

    source = "polymarket"

    def __init__(self, gamma_base: str = GAMMA_BASE, page_limit: int = 500, max_pages: int = 20):
        self.gamma_base = gamma_base
        self.page_limit = page_limit
        self.max_pages = max_pages

    @staticmethod
    def normalize(raw_markets: list, captured_at: datetime) -> list[RawRecord]:
        """Pure: list of Gamma market dicts -> RawRecords."""
        records: list[RawRecord] = []
        for m in raw_markets or []:
            cond = m.get("conditionId")
            if not cond:
                continue
            prices = _maybe_json(m.get("outcomePrices"))
            if isinstance(prices, list):
                prices = [_to_float(p) for p in prices]
            payload = {
                "condition_id": cond,
                "question": m.get("question"),
                "slug": m.get("slug"),
                "outcomes": _maybe_json(m.get("outcomes")),
                "outcome_prices": prices,
                "volume": _to_float(m.get("volume")),
                "liquidity": _to_float(m.get("liquidity")),
                "end_date": m.get("endDate"),
                "active": m.get("active"),
                "closed": m.get("closed"),
            }
            records.append(
                RawRecord(
                    source="polymarket",
                    source_key=cond,
                    record_type="market",
                    captured_at=captured_at,
                    payload=payload,
                )
            )
        return records

    def _fetch_raw(self) -> list:
        """Page active, non-closed markets from Gamma."""
        out: list = []
        session = requests.Session()
        for page in range(self.max_pages):
            params = {
                "active": "true",
                "closed": "false",
                "limit": self.page_limit,
                "offset": page * self.page_limit,
            }
            resp = session.get(f"{self.gamma_base}/markets", params=params, timeout=30)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            out.extend(batch)
            if len(batch) < self.page_limit:
                break
        return out

    def fetch_snapshots(self) -> list[RawRecord]:
        captured_at = datetime.now(timezone.utc)
        return self.normalize(self._fetch_raw(), captured_at)
