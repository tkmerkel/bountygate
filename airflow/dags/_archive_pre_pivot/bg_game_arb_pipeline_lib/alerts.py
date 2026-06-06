"""Discord alerter for game-line arb opportunities.

The shared bountygate.utils.discord_notify.notify_opportunities is shaped for
player-prop columns (player_name, market_key, under_*, over_*). Game-line
opportunities have a different shape (leg_a_*, leg_b_*, outcome, point), so
this module owns its own formatter and calls the shared `notify()` text path.

Per-book deeplink stubs use stable sport landing pages — the-odds-api doesn't
return book-specific event IDs, so true deeplinking would require a separate
mapping table (out of scope for v1).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from bountygate.utils.discord_notify import notify

# Discord content cap; the shared notify() also truncates but we want our
# multi-line summary to fit within one post.
_MAX_OPPS_PER_POST = 8

# Stable sport landing pages per book. Real per-event URLs would need a
# mapping layer; this gets users to the right surface in two clicks.
_SPORT_LANDING_URLS: dict[str, dict[str, str]] = {
    "americanfootball_nfl": {
        "fanduel":        "https://sportsbook.fanduel.com/navigation/nfl",
        "betmgm":         "https://sports.betmgm.com/en/sports/football-11",
        "williamhill_us": "https://www.caesars.com/sportsbook-and-casino/sport/american-football",
        "draftkings":     "https://sportsbook.draftkings.com/leagues/football/nfl",
    },
    "basketball_nba": {
        "fanduel":        "https://sportsbook.fanduel.com/navigation/nba",
        "betmgm":         "https://sports.betmgm.com/en/sports/basketball-7/betting/usa-9/nba-6004",
        "williamhill_us": "https://www.caesars.com/sportsbook-and-casino/sport/basketball",
        "draftkings":     "https://sportsbook.draftkings.com/leagues/basketball/nba",
    },
    "icehockey_nhl": {
        "fanduel":        "https://sportsbook.fanduel.com/navigation/nhl",
        "betmgm":         "https://sports.betmgm.com/en/sports/ice-hockey-12",
        "williamhill_us": "https://www.caesars.com/sportsbook-and-casino/sport/ice-hockey",
        "draftkings":     "https://sportsbook.draftkings.com/leagues/hockey/nhl",
    },
    "baseball_mlb": {
        "fanduel":        "https://sportsbook.fanduel.com/navigation/mlb",
        "betmgm":         "https://sports.betmgm.com/en/sports/baseball-23",
        "williamhill_us": "https://www.caesars.com/sportsbook-and-casino/sport/baseball",
        "draftkings":     "https://sportsbook.draftkings.com/leagues/baseball/mlb",
    },
    "basketball_ncaab": {
        "fanduel":        "https://sportsbook.fanduel.com/navigation/college-basketball",
        "betmgm":         "https://sports.betmgm.com/en/sports/basketball-7/betting/usa-9/ncaab-6005",
        "williamhill_us": "https://www.caesars.com/sportsbook-and-casino/sport/basketball",
        "draftkings":     "https://sportsbook.draftkings.com/leagues/basketball/ncaab",
    },
}


def _alert_threshold() -> float:
    raw = os.getenv("BG_GAME_ARB_ALERT_ROI_THRESHOLD", "0.0075")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0075


def _format_point(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return ""
    sign = "+" if f > 0 else ""
    return f"{sign}{f:g}"


def _deeplink_for(sport_key: str, book_key: str) -> str:
    return _SPORT_LANDING_URLS.get(sport_key, {}).get(book_key, "")


def _format_opportunities_message(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return ""

    working = df.copy()
    working["roi"] = pd.to_numeric(working["roi"], errors="coerce")
    working.sort_values(by="roi", ascending=False, inplace=True)

    lines: list[str] = []
    lines.append("====== +++++++++++++++ ======")
    lines.append(f"====== {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ======")
    lines.append("New high-value game-line arbs")
    lines.append(f"Count: {int(len(working))}")

    for idx, row in working.head(_MAX_OPPS_PER_POST).reset_index(drop=True).iterrows():
        sport = str(row.get("sport_title") or row.get("sport_key") or "").strip()
        matchup = f"{row.get('away_team', '')} @ {row.get('home_team', '')}".strip(" @")
        market = str(row.get("market_key", "")).upper()

        roi_pct = f"{float(row['roi']) * 100:.2f}%" if pd.notna(row.get("roi")) else ""
        hours = row.get("hours_until_commence")
        hours_str = f"{float(hours):.2f}h" if pd.notna(hours) else ""

        leg_a_point = _format_point(row.get("leg_a_point"))
        leg_b_point = _format_point(row.get("leg_b_point"))
        leg_a_price = f"{float(row['leg_a_price']):.3f}" if pd.notna(row.get("leg_a_price")) else ""
        leg_b_price = f"{float(row['leg_b_price']):.3f}" if pd.notna(row.get("leg_b_price")) else ""

        leg_a_label = f"{row.get('leg_a_book', '')} {row.get('leg_a_outcome', '')} {leg_a_point} @ {leg_a_price}".strip()
        leg_b_label = f"{row.get('leg_b_book', '')} {row.get('leg_b_outcome', '')} {leg_b_point} @ {leg_b_price}".strip()

        wager_a = f"${float(row['wager_leg_a']):.2f}" if pd.notna(row.get("wager_leg_a")) else ""
        wager_b = f"${float(row['wager_leg_b']):.2f}" if pd.notna(row.get("wager_leg_b")) else ""
        ev = f"EV ${float(row['arb_ev']):.2f}" if pd.notna(row.get("arb_ev")) else ""

        link_a = _deeplink_for(str(row.get("sport_key", "")), str(row.get("leg_a_book", "")))
        link_b = _deeplink_for(str(row.get("sport_key", "")), str(row.get("leg_b_book", "")))

        header = " | ".join(b for b in [sport, matchup, market, hours_str] if b)
        lines.append(f"{idx + 1}) {header}")
        lines.append(f"   A: {leg_a_label} | stake {wager_a}")
        lines.append(f"   B: {leg_b_label} | stake {wager_b}")
        metric = " | ".join(b for b in [f"ROI {roi_pct}" if roi_pct else "", ev] if b)
        if metric:
            lines.append(f"   {metric}")
        if link_a or link_b:
            lines.append(f"   {link_a} | {link_b}".strip())
        lines.append("")

    if len(working) > _MAX_OPPS_PER_POST:
        lines.append(f"(+{len(working) - _MAX_OPPS_PER_POST} more)")

    return "\n".join(lines).strip()


def notify_high_value_game_opportunities(df: pd.DataFrame) -> int:
    """Filter to roi >= threshold, post to Discord. Returns count posted.

    Caller is expected to pass only the rows that are NEW this run (the
    history task's diff). This function is responsible for the ROI gate and
    the formatting only.
    """
    if df is None or df.empty:
        return 0
    threshold = _alert_threshold()
    working = df.copy()
    working["roi"] = pd.to_numeric(working["roi"], errors="coerce")
    high_value = working[working["roi"] >= threshold]
    if high_value.empty:
        return 0
    body = _format_opportunities_message(high_value)
    if not body:
        return 0
    notify(body, level="info", source="airflow:bg_game_arb_pipeline")
    return int(len(high_value))
