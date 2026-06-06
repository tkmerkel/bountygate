from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from bountygate.connectors.base import Connector, RawRecord

log = logging.getLogger(__name__)


def _is_auth_error(exc) -> bool:
    status = getattr(exc, "status", None)
    resp = getattr(exc, "response", None)
    resp_status = getattr(resp, "status_code", None) if resp is not None else None
    return status in (401, 403) or resp_status in (401, 403)

SERIES_BY_SPORT = {"NFL": "KXNFLGAME", "NBA": "KXNBAGAME", "MLB": "KXMLBGAME"}


def _to_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class KalshiConnector(Connector):
    """Read-only Kalshi market data. Migrated from kalshi/dags/utils/kalshi_client.py,
    dropping all execution methods. Keeps the raw-HTTP-body bypass (the SDK's pydantic
    models are stale vs. the live API)."""

    source = "kalshi"

    def __init__(self, series_by_sport: dict | None = None):
        self.series_by_sport = series_by_sport or SERIES_BY_SPORT

    @staticmethod
    def normalize(raw: dict, series_ticker: str, captured_at: datetime) -> list[RawRecord]:
        """Pure: raw get_events body -> RawRecords. No I/O."""
        records: list[RawRecord] = []
        for event in (raw.get("events") or []):
            for m in (event.get("markets") or []):
                ticker = m.get("ticker")
                if not ticker:
                    continue
                yes_bid = _to_float(m.get("yes_bid_dollars"))
                yes_ask = _to_float(m.get("yes_ask_dollars"))
                no_bid = _to_float(m.get("no_bid_dollars"))
                no_ask = _to_float(m.get("no_ask_dollars"))
                payload = {
                    "ticker": ticker,
                    "event_ticker": m.get("event_ticker"),
                    "series_ticker": series_ticker,
                    "title": m.get("title"),
                    "yes_sub_title": m.get("yes_sub_title"),
                    "no_sub_title": m.get("no_sub_title"),
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "no_bid": no_bid,
                    "no_ask": no_ask,
                    "open_interest": (
                        _oi if (_oi := _to_float(m.get("open_interest_fp"))) is not None
                        else m.get("open_interest")
                    ),
                    "liquidity_dollars": _to_float(m.get("liquidity_dollars")),
                    "status": m.get("status"),
                }
                records.append(
                    RawRecord(
                        source="kalshi",
                        source_key=ticker,
                        record_type="market",
                        captured_at=captured_at,
                        payload=payload,
                    )
                )
        return records

    def _client(self):
        """Build the authenticated Kalshi SDK client (RSA-signed). Lazy import so
        unit tests of normalize() don't require kalshi_python_sync."""
        from kalshi_python_sync import Configuration, KalshiClient

        host = "https://api.elections.kalshi.com/trade-api/v2"
        with open(os.environ["KALSHI_PRIVATE_KEY_PATH"], "r") as f:
            private_key_pem = f.read()
        config = Configuration(host=host)
        config.api_key_id = os.environ["KALSHI_API_KEY_ID"]
        config.private_key_pem = private_key_pem
        return KalshiClient(config)

    def _fetch_raw(self, client, series_ticker: str) -> dict:
        resp = client.get_events_without_preload_content(
            series_ticker=series_ticker, status="open", with_nested_markets=True
        )
        return json.loads(resp.data)

    def fetch_snapshots(self) -> list[RawRecord]:
        client = self._client()
        out: list[RawRecord] = []
        captured_at = datetime.now(timezone.utc)
        for series_ticker in self.series_by_sport.values():
            try:
                raw = self._fetch_raw(client, series_ticker)
            except Exception as e:
                if _is_auth_error(e):
                    raise
                log.warning("[kalshi] fetch failed for %s: %s", series_ticker, e)
                continue
            out.extend(self.normalize(raw, series_ticker, captured_at))
        return out
