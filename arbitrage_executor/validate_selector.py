"""Executable selector validation harness.

This replaces the old mental model of "mapping exists" with a stronger
contract: the bot can navigate to a real opportunity, select the requested
site/market/side into the betslip, and clear the slip afterward.

Safety:
  - never enters a wager
  - never clicks Place Bet
  - clears the slip at the end and verifies empty state

Usage:
    python validate_selector.py --site fanduel --market batter_doubles_alternate
    python validate_selector.py --site betmgm --market player_assists --testing-mode
    python validate_selector.py --site fanduel --market player_points --no-save
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from typing import Optional

import pandas as pd
from playwright.sync_api import sync_playwright

# Keep legacy modules that resolve paths from CWD (notably chrome_helpers and
# SelectorManager callers) anchored to arbitrage_executor/ regardless of where
# the command is launched.
EXECUTOR_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(EXECUTOR_DIR)

from auth import LoginError, LoginInterventionRequired, ensure_logged_in
from bet_placer import BetPlacer, BetPlacerError
from chrome_helpers import CDP_PORT, ensure_chrome_cdp, profile_dir
from db_connection import fetch_data
from human.session import viewport_from_cdp
from human.modals import ModalWatcher
from selector_finder import SelectorManager

VALID_SITES = {"fanduel", "betmgm"}
MARKET_RE = re.compile(r"^[a-z0-9_]+$")
HASH_RE = re.compile(r"^[a-fA-F0-9]{16,64}$")


class ValidationError(Exception):
    pass


def _json_safe(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    try:
        import numpy as np
        if isinstance(value, np.generic):
            return value.item()
    except Exception:
        pass
    return value


def _df_records(df: pd.DataFrame) -> list[dict]:
    return [
        {k: _json_safe(v) for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]


def _numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in (
        "under_line",
        "over_line",
        "under_price",
        "over_price",
        "wager_under",
        "wager_over",
        "payout",
        "arb_ev",
        "roi",
        "hours_until_commence",
    ):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _validate_args(site: str, market: str) -> None:
    if site not in VALID_SITES:
        raise ValidationError(f"site must be one of {sorted(VALID_SITES)}, got {site!r}")
    if not MARKET_RE.match(market):
        raise ValidationError(f"market must be a simple market key, got {market!r}")


def _freshness_interval(*, testing_mode: bool, max_age_hours: Optional[float]) -> str:
    if max_age_hours is not None:
        if max_age_hours <= 0 or max_age_hours > 168:
            raise ValidationError("--max-age-hours must be > 0 and <= 168")
        return f"{max_age_hours:g} hours"
    return "4 hours" if testing_mode else "30 minutes"


def _new_table_query(
    site: str,
    market: str,
    *,
    testing_mode: bool,
    opportunity_hash: Optional[str],
    max_age_hours: Optional[float],
) -> str:
    if opportunity_hash and not HASH_RE.match(opportunity_hash):
        raise ValidationError(f"invalid opportunity hash: {opportunity_hash!r}")
    freshness = _freshness_interval(testing_mode=testing_mode, max_age_hours=max_age_hours)
    hash_filter = f"AND opportunity_hash = '{opportunity_hash}'" if opportunity_hash else ""
    return f"""
    SELECT player_name,
           sport_title,
           home_team,
           away_team,
           canonical_market,
           canonical_market AS market_key,
           pairing_type,
           under_market_key,
           over_market_key,
           under_line,
           over_line,
           under_book,
           over_book,
           under_book AS under_bookmaker_key,
           over_book  AS over_bookmaker_key,
           under_price,
           over_price,
           wager_under,
           wager_over,
           payout,
           arb_ev,
           roi,
           hours_until_commence,
           fetched_at_utc,
           opportunity_hash,
           'bg_arbitrage_opportunities' AS source_table
    FROM bg_arbitrage_opportunities
    WHERE (
            (under_market_key = '{market}' AND under_book = '{site}')
         OR (over_market_key  = '{market}' AND over_book  = '{site}')
        )
      {hash_filter}
      AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '{freshness}'
      AND hours_until_commence > 0
      AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
    ORDER BY roi DESC, fetched_at_utc DESC
    LIMIT 10;
    """


def _legacy_std_query(
    site: str,
    market: str,
    *,
    testing_mode: bool,
    max_age_hours: Optional[float],
) -> str:
    freshness = _freshness_interval(testing_mode=testing_mode, max_age_hours=max_age_hours)
    return f"""
    SELECT player_name,
           sport_title,
           home_team,
           away_team,
           market_key,
           market_key AS under_market_key,
           market_key AS over_market_key,
           under_line,
           over_line,
           under_bookmaker_key,
           over_bookmaker_key,
           under_price,
           over_price,
           wager_under,
           wager_over,
           payout,
           arb_ev,
           roi,
           hours_until_commence,
           fetched_at_utc,
           NULL AS opportunity_hash,
           'bg_arbitrage_player_props' AS source_table
    FROM bg_arbitrage_player_props
    WHERE market_key = '{market}'
      AND (under_bookmaker_key = '{site}' OR over_bookmaker_key = '{site}')
      AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '{freshness}'
      AND hours_until_commence > 0
      AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
    ORDER BY roi DESC, fetched_at_utc DESC
    LIMIT 10;
    """


def _legacy_alt_query(
    site: str,
    market: str,
    *,
    testing_mode: bool,
    max_age_hours: Optional[float],
) -> str:
    freshness = _freshness_interval(testing_mode=testing_mode, max_age_hours=max_age_hours)
    return f"""
    SELECT player_name,
           sport_title,
           home_team,
           away_team,
           COALESCE(NULLIF(REPLACE('{market}', '_alternate', ''), ''), '{market}') AS market_key,
           under_market_key,
           over_market_key,
           under_line,
           over_line,
           under_bookmaker_key,
           over_bookmaker_key,
           under_price,
           over_price,
           wager_under,
           wager_over,
           payout,
           arb_ev,
           roi,
           hours_until_commence,
           fetched_at_utc,
           NULL AS opportunity_hash,
           'bg_arbitrage_player_props_alt' AS source_table
    FROM bg_arbitrage_player_props_alt
    WHERE (
            (under_market_key = '{market}' AND under_bookmaker_key = '{site}')
         OR (over_market_key  = '{market}' AND over_bookmaker_key  = '{site}')
        )
      AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '{freshness}'
      AND hours_until_commence > 0
      AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
    ORDER BY roi DESC, fetched_at_utc DESC
    LIMIT 10;
    """


def _fetch_first(query: str) -> list[dict]:
    try:
        df = fetch_data(query)
    except Exception as e:
        print(f"[validator] query failed: {type(e).__name__}: {e}")
        return []
    if df is None or df.empty:
        return []
    return _df_records(_numeric_columns(df))


def fetch_validation_opportunities(
    site: str,
    market: str,
    *,
    testing_mode: bool,
    opportunity_hash: Optional[str] = None,
    max_age_hours: Optional[float] = None,
) -> list[dict]:
    """Fetch candidate opportunities from the new table, falling back to legacy tables."""
    _validate_args(site, market)
    rows = _fetch_first(_new_table_query(
        site,
        market,
        testing_mode=testing_mode,
        opportunity_hash=opportunity_hash,
        max_age_hours=max_age_hours,
    ))
    if rows:
        return rows
    if opportunity_hash:
        return []
    rows = _fetch_first(_legacy_std_query(
        site,
        market,
        testing_mode=testing_mode,
        max_age_hours=max_age_hours,
    ))
    if rows:
        return rows
    return _fetch_first(_legacy_alt_query(
        site,
        market,
        testing_mode=testing_mode,
        max_age_hours=max_age_hours,
    ))


def _leg_for_site_market(opportunity: dict, site: str, market: str) -> str:
    under_book = (opportunity.get("under_bookmaker_key") or opportunity.get("under_book") or "").lower()
    over_book = (opportunity.get("over_bookmaker_key") or opportunity.get("over_book") or "").lower()
    under_market = opportunity.get("under_market_key") or opportunity.get("market_key")
    over_market = opportunity.get("over_market_key") or opportunity.get("market_key")

    if under_book == site and under_market == market:
        return "under"
    if over_book == site and over_market == market:
        return "over"

    # Legacy std rows have one market_key for both legs.
    if opportunity.get("market_key") == market:
        if under_book == site:
            return "under"
        if over_book == site:
            return "over"

    raise ValidationError(
        f"opportunity does not contain requested leg: site={site}, market={market}, "
        f"under={under_book}/{under_market}, over={over_book}/{over_market}"
    )


def _audit_dir(site: str, market: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join("audit_logs", f"selector_validation_{ts}_{site}_{market}")
    os.makedirs(path, exist_ok=True)
    return path


def _validation_details(opportunity: dict, site: str, market: str, direction: str, audit_dir: str) -> dict:
    return {
        "validated_by": "validate_selector.py",
        "site": site,
        "market_key": market,
        "direction": direction,
        "player_name": opportunity.get("player_name"),
        "line": opportunity.get("under_line") if direction == "under" else opportunity.get("over_line"),
        "home_team": opportunity.get("home_team"),
        "away_team": opportunity.get("away_team"),
        "source_table": opportunity.get("source_table"),
        "opportunity_hash": opportunity.get("opportunity_hash"),
        "audit_dir": audit_dir,
    }


def validate_selector(
    site: str,
    market: str,
    opportunity: dict,
    *,
    save: bool,
) -> bool:
    config = SelectorManager.get_market(site, market)
    if not config:
        raise ValidationError(f"{site}/{market} has no YAML config to validate")

    direction = _leg_for_site_market(opportunity, site, market)
    audit_dir = _audit_dir(site, market)
    with open(os.path.join(audit_dir, "opportunity_info.json"), "w", encoding="utf-8") as f:
        json.dump(opportunity, f, indent=2, default=str)

    endpoint_url = ensure_chrome_cdp(profile_dir, CDP_PORT)

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(endpoint_url)
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = context.new_page()
        # Sweep Reality Check / responsible-gambling / promo modals on
        # the main thread before each settle(). Without this a modal
        # firing mid-validation (FD Reality Check fires every ~270 min)
        # leaves the page blocked and the validator hangs on actionability
        # waits. PR #32 review follow-up — flagged as a hole the
        # production orchestrator was already plugging.
        modal_watcher = ModalWatcher(page)
        modal_watcher.start()
        try:
            viewport_from_cdp(page)

            ensure_logged_in(page, site, audit_dir)
            placer = BetPlacer(page, site, audit_dir)
            placer.clear_betslip()
            placer.navigate_and_expand_market(opportunity, config, direction)
            placer.find_and_click_bet(opportunity, direction, config)
            placer.assert_betslip_has_bet()
            placer.clear_betslip()
            placer.assert_betslip_empty()
        finally:
            modal_watcher.stop()
            try:
                page.close()
            except Exception:
                pass

    details = _validation_details(opportunity, site, market, direction, audit_dir)
    if save:
        SelectorManager.update_validation_status(
            site,
            market,
            status="passed",
            details=details,
        )
    print(f"[validator] PASS {site}/{market} direction={direction} audit={audit_dir}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--site", required=True, choices=sorted(VALID_SITES))
    parser.add_argument("--market", required=True)
    parser.add_argument("--opportunity-hash", default=None)
    parser.add_argument("--testing-mode", action="store_true",
                        help="use a wider freshness window for candidate lookup")
    parser.add_argument("--max-age-hours", type=float, default=None,
                        help="override candidate freshness window, bounded to 168 hours")
    parser.add_argument("--candidate-index", type=int, default=0,
                        help="0-based candidate index after ROI/freshness sort")
    parser.add_argument("--no-save", action="store_true",
                        help="run validation without writing validation metadata to YAML")
    args = parser.parse_args()

    try:
        candidates = fetch_validation_opportunities(
            args.site,
            args.market,
            testing_mode=args.testing_mode,
            opportunity_hash=args.opportunity_hash,
            max_age_hours=args.max_age_hours,
        )
        if not candidates:
            raise ValidationError(
                f"no recent opportunity found for {args.site}/{args.market}"
            )
        if args.candidate_index < 0 or args.candidate_index >= len(candidates):
            raise ValidationError(
                f"--candidate-index must be between 0 and {len(candidates) - 1}"
            )
        opportunity = candidates[args.candidate_index]
        validate_selector(args.site, args.market, opportunity, save=not args.no_save)
        return 0
    except (ValidationError, BetPlacerError, LoginError, LoginInterventionRequired) as e:
        print(f"[validator] FAIL: {type(e).__name__}: {e}")
        if not args.no_save:
            try:
                SelectorManager.update_validation_status(
                    args.site,
                    args.market,
                    status="failed",
                    details={"error": f"{type(e).__name__}: {e}"},
                )
            except Exception:
                pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
