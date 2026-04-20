"""
Discord webhook notifier shared by Airflow DAGs and arbitrage_executor.

Single public entry point: ``notify(message, *, level, source)``.

The webhook URL comes from the env var ``BG_DISCORD_WEBHOOK_URL`` with a
hardcoded fallback to the existing project webhook (so the DAG keeps working
without additional config). Discord rate limit per webhook is plenty for our
use case (a few messages per minute at peak).

Failures are swallowed and printed — a failed Discord post must never crash
the executor or a DAG task.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import List

import pandas as pd
import requests


# Hardcoded fallback so existing DAG behavior is preserved if the env var is
# unset. Promote to env-only by deleting this and setting BG_DISCORD_WEBHOOK_URL
# in `.env` (already gitignored).
_DEFAULT_WEBHOOK_URL = (
    "https://discord.com/api/webhooks/1336061346088751104/"
    "G630NXDcVY6ZqejMM-pyIZVDgazNAUKG0rAiDWZ3oBi-2TkR2qoGkte-1fX0HaKyT5_Q"
)
WEBHOOK_ENV_VAR = "BG_DISCORD_WEBHOOK_URL"

_DISCORD_CONTENT_LIMIT = 1900  # Real cap is 2000; stay safely under.
_HTTP_TIMEOUT_SECONDS = 10

_LEVEL_PREFIXES = {
    "critical": "🚨 CRITICAL",
    "warning": "⚠️ WARNING",
    "info": "ℹ️ INFO",
}


def _webhook_url() -> str:
    return os.getenv(WEBHOOK_ENV_VAR, _DEFAULT_WEBHOOK_URL)


def notify(message: str, *, level: str = "info", source: str = "") -> None:
    """Post a message to the project Discord webhook.

    Args:
        message: Plain text. Truncated to 1900 chars.
        level: One of "critical", "warning", "info" (case-insensitive).
            Unknown levels fall through as "info".
        source: Short tag identifying the originating system, e.g.
            ``"arbitrage_executor"`` or ``"airflow:bg_arbitrage_player_props"``.
            Empty is allowed.
    """
    url = _webhook_url()
    if not url:
        return

    body = (message or "").strip()
    if not body:
        return

    prefix = _LEVEL_PREFIXES.get(level.lower(), _LEVEL_PREFIXES["info"])
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    tag = f"[{source}] " if source else ""
    header = f"{prefix} {tag}{timestamp}"
    content = f"{header}\n{body}"[:_DISCORD_CONTENT_LIMIT]

    try:
        response = requests.post(
            url,
            data=json.dumps({"content": content}),
            headers={"Content-Type": "application/json"},
            timeout=_HTTP_TIMEOUT_SECONDS,
        )
        if response.status_code != 204:
            print(
                f"Discord webhook send failed: status={response.status_code} body={response.text}"
            )
    except Exception as exc:
        print(f"Discord webhook send raised: {exc}")


def notify_opportunities(df: pd.DataFrame, *, label: str) -> None:
    """Format a dataframe of arbitrage opportunities and notify (info level)."""
    body = _format_opportunities_message(df, label=label)
    if not body:
        return
    notify(body, level="info", source=f"airflow:{label}")


def _format_opportunities_message(df: pd.DataFrame, *, label: str) -> str:
    """Build a human-readable Discord summary of arbitrage opportunities.

    Preserved verbatim from bg_arbitrage_player_props.py so the existing DAG
    output format does not change.
    """
    if df is None or df.empty:
        return ""

    working = df.copy()
    working["roi"] = pd.to_numeric(working.get("roi"), errors="coerce")
    working.sort_values(by=["roi"], ascending=False, inplace=True)

    lines: List[str] = []
    lines.append("====== +++++++++++++++ ======")
    lines.append(f"====== {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ======")
    lines.append(f"New high-value opportunity ({label})")
    lines.append(f"Count: {int(len(working))}")

    max_rows = 8
    for idx, row in working.head(max_rows).reset_index(drop=True).iterrows():
        roi = row.get("roi")
        roi_pct = f"{float(roi) * 100:.2f}%" if pd.notna(roi) else ""
        hours = row.get("hours_until_commence")
        hours_str = f"{float(hours):.2f}h" if pd.notna(hours) else ""
        commence = row.get("commence_time")
        commence_str = (
            pd.to_datetime(commence, errors="coerce", utc=True).strftime("%Y-%m-%d %H:%MZ")
            if pd.notna(commence)
            else ""
        )

        sport = str(row.get("sport_title") or row.get("sport_key") or "").strip()
        away = str(row.get("away_team") or "").strip()
        home = str(row.get("home_team") or "").strip()
        matchup = f"{away} @ {home}".strip(" @")

        player = str(row.get("player_name") or "").strip()
        market = str(row.get("market_key") or "").strip()
        line_value = row.get("under_line") if "under_line" in working.columns else row.get("line")
        line_str = (
            f"{float(line_value):g}" if pd.notna(pd.to_numeric(line_value, errors="coerce")) else ""
        )

        under_book = str(row.get("under_bookmaker_key") or "").strip()
        over_book = str(row.get("over_bookmaker_key") or "").strip()
        under_price = row.get("under_price")
        over_price = row.get("over_price")
        under_price_str = f"{float(under_price):.4g}" if pd.notna(under_price) else ""
        over_price_str = f"{float(over_price):.4g}" if pd.notna(over_price) else ""

        arb_ev = row.get("arb_ev")
        total_wager = row.get("total_wager")
        ev_str = (
            f"EV ${float(arb_ev):.2f} on ${float(total_wager):.2f}"
            if pd.notna(arb_ev) and pd.notna(total_wager)
            else ""
        )

        header_bits = " | ".join([bit for bit in [sport, matchup, commence_str, hours_str] if bit])
        prop_bits = " ".join([bit for bit in [player, market, line_str] if bit]).strip()
        price_bits = " | ".join(
            [
                bit
                for bit in [
                    f"U {under_book} {under_price_str}".strip(),
                    f"O {over_book} {over_price_str}".strip(),
                ]
                if bit
            ]
        )
        metric_bits = " | ".join([bit for bit in [f"ROI {roi_pct}" if roi_pct else "", ev_str] if bit])

        lines.append(f"{idx + 1}) {header_bits}")
        if prop_bits:
            lines.append(prop_bits)
        if price_bits:
            lines.append(price_bits)
        if metric_bits:
            lines.append(metric_bits)
        lines.append("")

    if len(working) > max_rows:
        lines.append(f"(+{len(working) - max_rows} more)")

    return "\n".join(lines).strip()
