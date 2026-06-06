"""Pure computation of resolved-market calibration / CLV rows for mart_market_history.

Inputs:
  resolved_markets: list of dicts with market_id, resolved_outcome, close_time,
      resolution_time, tracked_outcome (the outcome whose price series we track).
  prices_by_market: {market_id: [(captured_at_iso, price), ...]} for the tracked outcome.
Output: list of mart_market_history row dicts.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from bountygate.analytics.clv import clv_from_fair

_HORIZON = timedelta(hours=1)


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def compute_market_history(resolved_markets: list[dict], prices_by_market: dict) -> list[dict]:
    out: list[dict] = []
    for m in resolved_markets:
        points = sorted(
            ((_parse(ts), price) for ts, price in prices_by_market.get(m["market_id"], [])),
            key=lambda x: x[0],
        )
        if not points:
            continue
        close = _parse(m["close_time"])
        prior = [p for p in points if p[0] <= close - _HORIZON]
        before_close = [p for p in points if p[0] <= close]
        if not prior or not before_close:
            continue
        predicted_prob = prior[-1][1]            # last point >= 1h before close
        closing_prob = before_close[-1][1]       # last point before close
        out.append({
            "market_id": m["market_id"],
            "resolved_outcome": m.get("resolved_outcome"),
            "resolution_time": m.get("resolution_time"),
            "predicted_prob": predicted_prob,
            "realized": m.get("resolved_outcome") == m.get("tracked_outcome"),
            "clv": clv_from_fair(predicted_prob, closing_prob),
        })
    return out
