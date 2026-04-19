"""
Arbitrage Execution Orchestrator
Main script that executes the tease-probe-execute strategy for FanDuel/BetMGM pairs.
"""

import os
import hashlib
import json
from datetime import datetime
from typing import Dict, Optional

from playwright.sync_api import sync_playwright

from opportunity import fetch_and_prepare_opportunity, fetch_all_opportunities, infer_direction_for_book, get_market_keys, MIN_ROI_THRESHOLD
from selector_finder import SelectorManager
from bet_placer import BetPlacer, BetPlacerError
from execution_logger import ExecutionLogger
from db_connection import mark_opportunity_executed
from chrome_helpers import CDP_PORT, profile_dir, ensure_chrome_cdp


def calculate_roi(price1: float, price2: float) -> float:
    """Calculate ROI from two decimal odds.

    For arbitrage, the ROI formula is:
    ROI = 1 - (1/price1 + 1/price2)

    A positive ROI means guaranteed profit.
    A negative ROI means guaranteed loss.

    Args:
        price1: Decimal odds for side 1
        price2: Decimal odds for side 2

    Returns:
        ROI as a decimal (e.g., 0.02 = 2% profit)
    """
    implied_prob_sum = (1 / price1) + (1 / price2)
    roi = 1 - implied_prob_sum
    return roi


def _opportunity_hash(opportunity: Dict) -> str:
    """Generate unique hash for opportunity to prevent duplicates."""
    parts = [
        opportunity.get("player_name"),
        opportunity.get("sport_title"),
        opportunity.get("home_team"),
        opportunity.get("away_team"),
        opportunity.get("market_key"),
        opportunity.get("over_market_key"),
        opportunity.get("under_market_key"),
        opportunity.get("under_line"),
        opportunity.get("over_line"),
        opportunity.get("under_bookmaker_key"),
        opportunity.get("over_bookmaker_key"),
        opportunity.get("fetched_at_utc"),
    ]
    serialized = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def check_selectors_mapped(opportunity: Dict) -> tuple[bool, Optional[str]]:
    """
    Check if both bookmakers have selectors mapped for this market.
    Supports separate over_market_key and under_market_key for alternate markets.

    Returns:
        (all_mapped: bool, missing_info: Optional[str])
    """
    over_book = opportunity['over_bookmaker_key']
    under_book = opportunity['under_bookmaker_key']
    over_market_key, under_market_key = get_market_keys(opportunity)

    missing = []

    if not SelectorManager.has_market(over_book, over_market_key):
        missing.append(f"{over_book} - {over_market_key}")

    if not SelectorManager.has_market(under_book, under_market_key):
        missing.append(f"{under_book} - {under_market_key}")

    if missing:
        return False, ", ".join(missing)

    return True, None


def calculate_hedge_stake(primary_stake: float, primary_price: float, hedge_price: float) -> float:
    """
    Calculate hedge stake to lock in profit.

    Args:
        primary_stake: Amount wagered on primary bet
        primary_price: Decimal odds of primary bet
        hedge_price: Decimal odds of hedge bet

    Returns:
        Hedge stake amount
    """
    # To break even on hedge: hedge_stake * hedge_price = primary_stake * primary_price
    # We want slightly more to lock profit
    hedge = (primary_stake * primary_price) / hedge_price
    return round(hedge, 2)


class ArbExecutor:
    """Orchestrates arbitrage execution with tease-probe-execute strategy."""

    def __init__(self, opportunity: Dict):
        self.opportunity = opportunity
        self.opp_hash = _opportunity_hash(opportunity)

        # Create audit directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        player_safe = opportunity['player_name'].replace(" ", "_")
        over_market_key, under_market_key = get_market_keys(opportunity)
        market_safe = over_market_key or 'unknown'
        self.audit_dir = f"audit_logs/{timestamp}_{player_safe}_{market_safe}"
        os.makedirs(self.audit_dir, exist_ok=True)

        # Save opportunity info
        with open(os.path.join(self.audit_dir, "opportunity_info.json"), "w") as f:
            json.dump(opportunity, f, indent=2, default=str)

        print(f"\n{'='*60}")
        print(f"ARBITRAGE EXECUTION")
        print(f"{'='*60}")
        print(f"Player: {opportunity['player_name']}")
        over_market = over_market_key or 'unknown'
        under_market = under_market_key or 'unknown'
        if over_market == under_market:
            print(f"Market: {over_market}")
        else:
            print(f"Markets: over={over_market}, under={under_market}")
        print(f"Event: {opportunity['away_team']} @ {opportunity['home_team']}")
        print(f"Line: {opportunity['over_line']}")
        print(f"Books: {opportunity['over_bookmaker_key']} vs {opportunity['under_bookmaker_key']}")
        print(f"ROI: {opportunity.get('roi', 0) * 100:.2f}%")
        print(f"Audit: {self.audit_dir}")
        print(f"{'='*60}\n")

    def execute(self) -> bool:
        """
        Execute the arbitrage opportunity.

        Returns:
            True if both legs placed successfully, False otherwise
        """
        # Check selectors mapped
        mapped, missing = check_selectors_mapped(self.opportunity)
        if not mapped:
            print(f"❌ SKIPPED: Selectors not mapped for {missing}")
            ExecutionLogger.log_unmapped_market(
                missing.split(" - ")[0], missing.split(" - ")[1], self.opportunity
            )
            return False

        # Load market configs - support separate market keys for alternate markets
        over_book = self.opportunity['over_bookmaker_key']
        under_book = self.opportunity['under_bookmaker_key']
        over_market_key, under_market_key = get_market_keys(self.opportunity)

        over_config = SelectorManager.get_market(over_book, over_market_key)
        under_config = SelectorManager.get_market(under_book, under_market_key)

        # Determine which is FanDuel and which is BetMGM
        if over_book == 'fanduel':
            fd_direction = 'over'
            mgm_direction = 'under'
            fd_config = over_config
            mgm_config = under_config
            fd_market_key = over_market_key
            mgm_market_key = under_market_key
        else:
            fd_direction = 'under'
            mgm_direction = 'over'
            fd_config = under_config
            mgm_config = over_config
            fd_market_key = under_market_key
            mgm_market_key = over_market_key

        print(f"FanDuel side: {fd_direction} (market: {fd_market_key})")
        print(f"BetMGM side: {mgm_direction} (market: {mgm_market_key})\n")

        # Setup browser
        endpoint_url = ensure_chrome_cdp(profile_dir, CDP_PORT)

        try:
            with sync_playwright() as p:
                browser = p.chromium.connect_over_cdp(endpoint_url)
                context = browser.contexts[0] if browser.contexts else browser.new_context()

                # === PHASE 1: Tease FanDuel Limit ===
                print(f"\n{'─'*60}")
                print(f"PHASE 1: DISCOVER FANDUEL MAX WAGER")
                print(f"{'─'*60}\n")

                # Open FanDuel page
                print("Opening FanDuel tab...")
                page_fd = context.new_page()
                page_fd.set_viewport_size({"width": 943, "height": 944})
                placer_fd = BetPlacer(page_fd, "fanduel", self.audit_dir)

                try:
                    placer_fd.navigate_and_expand_market(self.opportunity, fd_config, fd_direction)
                    placer_fd.find_and_click_bet(self.opportunity, fd_direction, fd_config)

                    # Extract actual FanDuel odds
                    fd_actual_odds = placer_fd.get_actual_odds_fanduel()

                    fd_max_wager, fd_max_text = placer_fd.discover_max_wager_fanduel()

                    print(f"\n✓ FanDuel max wager: ${fd_max_wager:.2f}")

                except BetPlacerError as e:
                    print(f"❌ Phase 1 failed: {e}")
                    ExecutionLogger.log_execution_failure("FanDuel limit discovery failed", self.opportunity, "fanduel", e)
                    return False

                # === PHASE 2: Execute BetMGM Primary ===
                print(f"\n{'─'*60}")
                print(f"PHASE 2: PLACE BETMGM BET")
                print(f"{'─'*60}\n")

                # Open BetMGM page
                print("Opening BetMGM tab...")
                page_mgm = context.new_page()
                page_mgm.set_viewport_size({"width": 958, "height": 944})
                placer_mgm = BetPlacer(page_mgm, "betmgm", self.audit_dir)

                # Original prices from opportunity
                mgm_price_original = self.opportunity.get(f'{mgm_direction}_price', 2.0)
                fd_price_original = self.opportunity.get(f'{fd_direction}_price', 2.0)

                try:
                    placer_mgm.navigate_and_expand_market(self.opportunity, mgm_config, mgm_direction)
                    placer_mgm.find_and_click_bet(self.opportunity, mgm_direction, mgm_config)

                    # Extract actual BetMGM odds
                    mgm_actual_odds = placer_mgm.get_actual_odds_betmgm()

                    # === VERIFY ROI WITH ACTUAL ODDS ===
                    # Use actual odds if available, otherwise use original
                    fd_price = fd_actual_odds if fd_actual_odds else fd_price_original
                    mgm_price = mgm_actual_odds if mgm_actual_odds else mgm_price_original

                    # Check if odds have changed
                    if fd_actual_odds and abs(fd_actual_odds - fd_price_original) > 0.01:
                        print(f"⚠ FanDuel odds changed: {fd_price_original:.3f} → {fd_actual_odds:.3f}")
                    if mgm_actual_odds and abs(mgm_actual_odds - mgm_price_original) > 0.01:
                        print(f"⚠ BetMGM odds changed: {mgm_price_original:.3f} → {mgm_actual_odds:.3f}")

                    # Recalculate ROI with actual odds
                    actual_roi = calculate_roi(fd_price, mgm_price)
                    print(f"\nActual ROI: {actual_roi * 100:.2f}% (threshold: {MIN_ROI_THRESHOLD * 100:.2f}%)")

                    if actual_roi < MIN_ROI_THRESHOLD:
                        print(f"❌ ABORTING: ROI {actual_roi * 100:.2f}% is below threshold {MIN_ROI_THRESHOLD * 100:.2f}%")
                        ExecutionLogger.log_execution_failure(
                            f"ROI dropped below threshold: {actual_roi * 100:.2f}% < {MIN_ROI_THRESHOLD * 100:.2f}%",
                            self.opportunity, "betmgm"
                        )
                        page_mgm.close()
                        page_fd.close()
                        return False

                    print(f"✓ ROI verified: {actual_roi * 100:.2f}%\n")

                except BetPlacerError as e:
                    print(f"❌ Phase 2 navigation failed: {e}")
                    ExecutionLogger.log_execution_failure("BetMGM navigation failed", self.opportunity, "betmgm", e)
                    return False

                # Calculate MGM stake (don't bet more than we can hedge)
                hedge_ratio = fd_price / mgm_price
                max_mgm_stake = fd_max_wager / hedge_ratio

                # Use the calculated wager from opportunity, capped by max
                planned_mgm_wager = self.opportunity.get(f'wager_{mgm_direction}', 10.0)
                actual_mgm_stake = min(planned_mgm_wager, max_mgm_stake)

                print(f"Planned BetMGM wager: ${planned_mgm_wager:.2f}")
                print(f"Max allowed (based on FD limit): ${max_mgm_stake:.2f}")
                print(f"Actual BetMGM wager: ${actual_mgm_stake:.2f}\n")

                try:
                    placer_mgm.enter_wager(actual_mgm_stake)

                    # Check for BetMGM max limit alert
                    limit_hit, adjusted_stake = placer_mgm.check_betmgm_limit_alert()

                    if limit_hit:
                        if adjusted_stake is None:
                            print(f"❌ BetMGM limit hit but couldn't get adjusted stake")
                            ExecutionLogger.log_execution_failure("BetMGM limit hit, no adjusted stake", self.opportunity, "betmgm")
                            page_mgm.close()
                            page_fd.close()
                            return False

                        print(f"BetMGM adjusted stake: ${adjusted_stake:.2f} (was ${actual_mgm_stake:.2f})")

                        # Recalculate hedge and ROI with adjusted stake
                        adjusted_fd_hedge = calculate_hedge_stake(adjusted_stake, mgm_price, fd_price)

                        # Check if adjusted ROI is still acceptable
                        # (ROI doesn't change with stake, but let's verify we can still hedge)
                        if adjusted_fd_hedge > fd_max_wager:
                            print(f"❌ Cannot hedge: adjusted FD stake ${adjusted_fd_hedge:.2f} > max ${fd_max_wager:.2f}")
                            ExecutionLogger.log_execution_failure(
                                f"BetMGM limit hit, cannot hedge: ${adjusted_fd_hedge:.2f} > ${fd_max_wager:.2f}",
                                self.opportunity, "betmgm"
                            )
                            page_mgm.close()
                            page_fd.close()
                            return False

                        print(f"✓ Can still hedge with adjusted stakes")
                        actual_mgm_stake = adjusted_stake

                    mgm_status, mgm_msg = placer_mgm.place_bet()

                    if mgm_status != "ACCEPTED":
                        print(f"❌ BetMGM bet {mgm_status}: {mgm_msg}")
                        ExecutionLogger.log_execution_failure(f"BetMGM {mgm_status}: {mgm_msg}", self.opportunity, "betmgm")
                        page_mgm.close()
                        page_fd.close()
                        return False

                    print(f"\n✓ BetMGM bet ACCEPTED: ${actual_mgm_stake:.2f} @ {mgm_price}")

                except BetPlacerError as e:
                    print(f"❌ Phase 2 failed: {e}")
                    ExecutionLogger.log_execution_failure("BetMGM bet placement failed", self.opportunity, "betmgm", e)
                    page_mgm.close()
                    page_fd.close()
                    return False

                # === PHASE 3: Execute FanDuel Hedge ===
                print(f"\n{'─'*60}")
                print(f"PHASE 3: PLACE FANDUEL HEDGE")
                print(f"{'─'*60}\n")

                # Calculate hedge stake based on actual MGM fill
                fd_hedge_stake = calculate_hedge_stake(actual_mgm_stake, mgm_price, fd_price)

                print(f"Hedge stake (calculated): ${fd_hedge_stake:.2f}\n")

                try:
                    # FanDuel already has bet in slip, just update wager
                    placer_fd.enter_wager(fd_hedge_stake)

                    fd_status, fd_msg = placer_fd.place_bet()

                    if fd_status != "ACCEPTED":
                        print(f"❌ HEDGE FAILURE! FanDuel bet {fd_status}: {fd_msg}")
                        print(f"⚠️ MANUAL INTERVENTION REQUIRED: BetMGM bet placed but FanDuel hedge failed!")
                        ExecutionLogger.log_execution_failure(
                            f"FanDuel hedge {fd_status}: {fd_msg} (BetMGM bet placed!)",
                            self.opportunity, "fanduel"
                        )
                        return False

                    print(f"\n✓ FanDuel hedge ACCEPTED: ${fd_hedge_stake:.2f} @ {fd_price}")

                except BetPlacerError as e:
                    print(f"❌ HEDGE FAILURE! Phase 3 failed: {e}")
                    print(f"⚠️ MANUAL INTERVENTION REQUIRED: BetMGM bet placed but FanDuel hedge failed!")
                    ExecutionLogger.log_execution_failure(
                        f"FanDuel hedge failed: {e} (BetMGM bet placed!)",
                        self.opportunity, "fanduel", e
                    )
                    return False

                # === SUCCESS ===
                print(f"\n{'='*60}")
                print(f"✓ ARBITRAGE EXECUTED SUCCESSFULLY")
                print(f"{'='*60}\n")

                ExecutionLogger.log_execution_success(
                    self.opportunity,
                    {
                        "side": fd_direction,
                        "stake": fd_hedge_stake,
                        "price": fd_price,
                        "actual_odds": fd_price,
                        "original_odds": fd_price_original,
                        "max_wager_discovered": fd_max_wager
                    },
                    {
                        "side": mgm_direction,
                        "stake": actual_mgm_stake,
                        "price": mgm_price,
                        "actual_odds": mgm_price,
                        "original_odds": mgm_price_original
                    },
                    self.audit_dir
                )

                # === CLEANUP: Close browser tabs ===
                print("Closing browser tabs...")
                try:
                    page_mgm.close()
                    print("  ✓ BetMGM tab closed")
                except Exception as e:
                    print(f"  ⚠ Error closing BetMGM tab: {e}")

                try:
                    page_fd.close()
                    print("  ✓ FanDuel tab closed")
                except Exception as e:
                    print(f"  ⚠ Error closing FanDuel tab: {e}")

                return True

        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            ExecutionLogger.log_execution_failure(f"Unexpected error: {e}", self.opportunity, error=e)
            return False


def main() -> bool:
    """Main execution — iterate candidates until one succeeds or all are exhausted.

    Returns True on success, False on failure.
    """
    print("Arbitrage Bot Starting...\n")

    opportunities = fetch_all_opportunities(testing_mode=True)

    if not opportunities:
        print("No opportunities found.")
        return False

    for i, opportunity in enumerate(opportunities):
        over_book = opportunity.get('over_bookmaker_key', '').lower()
        under_book = opportunity.get('under_bookmaker_key', '').lower()

        display_market = opportunity.get('over_market_key') or opportunity.get('market_key', '?')
        player = opportunity.get('player_name', '?')
        label = f"[{i+1}/{len(opportunities)}] {player} - {display_market}"

        # Pre-check: bookmaker pair
        if not ({'fanduel', 'betmgm'} == {over_book, under_book}):
            print(f"⏭ {label}: not a FanDuel/BetMGM pair ({over_book} vs {under_book})")
            continue

        # Pre-check: selectors mapped
        mapped, missing = check_selectors_mapped(opportunity)
        if not mapped:
            print(f"⏭ {label}: selectors not mapped for {missing}")
            ExecutionLogger.log_unmapped_market(
                missing.split(", ")[0].split(" - ")[0],
                missing.split(", ")[0].split(" - ")[-1],
                opportunity,
            )
            continue

        # Viable candidate — attempt execution
        print(f"\n▶ {label}: attempting execution")
        executor = ArbExecutor(opportunity)
        success = executor.execute()

        if success:
            opp_hash = _opportunity_hash(opportunity)
            opportunity["market_key"] = opportunity.get("under_market_key") or opportunity.get("over_market_key")
            mark_opportunity_executed(opp_hash, opportunity)
            print("\n✓ Execution complete")
            return True

        # Execution failed (browser error, ROI dropped, bet rejected, etc.)
        # Stop here — browser state may be dirty, let the next queued task retry.
        print(f"\n✗ Execution failed for {label} - stopping")
        return False

    print("\n✗ All opportunities exhausted — none viable")
    return False


if __name__ == "__main__":
    main()
