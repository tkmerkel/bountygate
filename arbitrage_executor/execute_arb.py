"""
Arbitrage Execution Orchestrator
Main script that executes the tease-probe-execute strategy for FanDuel/BetMGM pairs.
"""

import os
import sys
import hashlib
import json
import time
from datetime import datetime
from typing import Dict, Optional

# Force UTF-8 on stdout/stderr so emoji prints (⏭ ▶ ✓ ✗) don't crash when
# run via subprocess on Windows (cp1252 default). Mirrors task_worker.py.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from playwright.sync_api import sync_playwright

from opportunity import fetch_and_prepare_opportunity, fetch_all_opportunities, infer_direction_for_book, get_market_keys, MIN_ROI_THRESHOLD
from selector_finder import SelectorManager
from bet_placer import BetPlacer, BetPlacerError, BetPlacerSkipError
from execution_logger import ExecutionLogger
from db_connection import mark_opportunity_executed
from chrome_helpers import CDP_PORT, profile_dir, ensure_chrome_cdp
from auth import ensure_logged_in, LoginError, LoginInterventionRequired
from screen_recorder import start_recording, stop_recording
from dashboard_tab import ensure_dashboard_tab
from human.session import viewport_from_cdp, warmup_browse, intra_book_idle
from human.modals import ModalWatcher
from human import SlipDrainedDuringIdleError, FdOddsDriftedDuringIdleError


from human.waiting import step_timer


def _print_cycle_time_breakdown(timings: list) -> None:
    """Print a longest-first cycle-time breakdown — the per-run time study."""
    if not timings:
        return
    total = sum(ms for _, ms in timings)
    print(f"\n{'─'*60}")
    print("CYCLE TIME BREAKDOWN (longest first)")
    print(f"{'─'*60}")
    for label, ms in sorted(timings, key=lambda x: -x[1]):
        pct = (ms / total * 100) if total else 0
        print(f"  {ms/1000:6.1f}s  {pct:4.0f}%  {label}")
    print(f"  {total/1000:6.1f}s  100%  TOTAL (instrumented steps)")
    print(f"{'─'*60}\n")


class OrphanedBetError(Exception):
    """Raised when BetMGM was placed but the FanDuel hedge failed.

    Carrying this signal up to the worker lets it halt the polling loop
    instead of moving on to the next opportunity while one bet is unhedged.
    The accompanying Discord CRITICAL alert (sent before raising) tells the
    user exactly what was placed so they can manually hedge.
    """


class WorkerHaltError(Exception):
    """Halt the worker — operator intervention required (no orphan bet).

    Distinct from OrphanedBetError: there is no money at risk, but a human
    must act before any more bets can be attempted. The canonical trigger is
    2FA / CAPTCHA on login. A CRITICAL Discord alert is sent before raising.
    """


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


# Same-player+market cooldown. Once an opportunity is attempted (success
# OR failure), block re-attempting the same (player, market, event) tuple
# for the worker's lifetime. Prevents the cross-book correlation pattern
# that risk teams cluster on (5 attempts on the same prop in 14 min).
_attempted_events: set[tuple] = set()


def _event_cooldown_key(opportunity: Dict) -> tuple:
    """Stable key for the same-player+market+event cooldown."""
    market_key = (
        opportunity.get("over_market_key")
        or opportunity.get("under_market_key")
        or opportunity.get("market_key")
    )
    return (
        opportunity.get("player_name"),
        market_key,
        opportunity.get("home_team"),
        opportunity.get("away_team"),
    )


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
    require_executable = os.getenv("REQUIRE_EXECUTABLE_SELECTORS", "false").lower() == "true"

    missing = []

    if require_executable:
        over_ok = SelectorManager.is_market_executable(over_book, over_market_key)
        under_ok = SelectorManager.is_market_executable(under_book, under_market_key)
    else:
        over_ok = SelectorManager.has_market(over_book, over_market_key)
        under_ok = SelectorManager.has_market(under_book, under_market_key)

    if not over_ok:
        missing.append(f"{over_book} - {over_market_key}")

    if not under_ok:
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

        # Track the asymmetric-risk window: BetMGM placed but FanDuel hedge
        # not yet confirmed. If we exit with betmgm_placed=True and
        # fanduel_hedged=False, that's an orphaned bet — page the human.
        self.betmgm_placed = False
        self.fanduel_hedged = False
        self.betmgm_bet_details: Optional[Dict] = None

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

        # Original prices from opportunity — hoisted above Phase 1 so the
        # intra-book idle window (run between Phase 1 and Phase 2) has a
        # baseline FD price to drift-check against if get_actual_odds()
        # didn't return a value.
        fd_price_original = self.opportunity.get(f'{fd_direction}_price', 2.0)
        mgm_price_original = self.opportunity.get(f'{mgm_direction}_price', 2.0)

        # Start screen recording, then warm sessions, then run phases —
        # all inside a single try/finally so the recording always stops
        # and review.pending always writes, regardless of which step
        # raises (warmup credential-modal halt, Phase login, orphan bet).
        record_proc = None
        # Modal watchers — bound after each tab is opened so the outer
        # finally can stop them on every exit path (early return, raise,
        # orphan escalation) without having to thread .stop() through the
        # ~12 page_*.close() sites scattered across error handlers.
        fd_modal_watcher: Optional[ModalWatcher] = None
        mgm_modal_watcher: Optional[ModalWatcher] = None

        try:
            # Recording first so cold-session login flows are captured.
            # Previously placed warmup before recording for clean duration
            # metrics, but that hid the 2026-05-15 BetMGM stuck-credential-
            # modal failure from the reviewer. Visibility > clean baseline.
            record_proc = start_recording(os.path.join(self.audit_dir, "recording.mp4"))

            # Pre-warm both sessions before Phase 1 so cold-session auth
            # happens once up front (and is captured for diagnosis).
            self._warmup_sessions()

            # Setup browser
            endpoint_url = ensure_chrome_cdp(profile_dir, CDP_PORT)

            with sync_playwright() as p:
                browser = p.chromium.connect_over_cdp(endpoint_url)
                context = browser.contexts[0] if browser.contexts else browser.new_context()
                ensure_dashboard_tab(browser)

                # === PHASE 1: Tease FanDuel Limit ===
                # Phase 1→3 wall-clock for orphan-risk visibility. The
                # humanized layer adds 30-100s vs legacy; if this elapsed
                # grows past ~120s the FD-odds drift window between
                # Phase 2 placement and Phase 3 hedge gets uncomfortably
                # wide. Watched in `logs/execution_success.log`.
                phase_1_start_monotonic = time.monotonic()
                timings: list = []  # cycle-time sink for the end-of-run breakdown
                print(f"\n{'─'*60}")
                print(f"PHASE 1: DISCOVER FANDUEL MAX WAGER")
                print(f"{'─'*60}\n")

                # Open FanDuel page
                print("Opening FanDuel tab...")
                page_fd = context.new_page()
                # Cap element-action auto-waits (click/press/fill/focus). The
                # sportsbook slips re-render constantly, so Playwright's default
                # 30s stability timeout turns each best-effort action into a
                # 30s hang. Navigation keeps the generous 30s timeout.
                page_fd.set_default_timeout(8000)
                page_fd.set_default_navigation_timeout(30000)
                viewport_from_cdp(page_fd)
                fd_modal_watcher = ModalWatcher(page_fd)
                fd_modal_watcher.start()

                try:
                    ensure_logged_in(page_fd, "fanduel", self.audit_dir)
                except LoginInterventionRequired as e:
                    ExecutionLogger.log_critical(
                        reason=f"LOGIN INTERVENTION REQUIRED on FanDuel: {e}",
                        opportunity=self.opportunity,
                        action_required=(
                            "Open the FanDuel tab in the bot's Chrome window "
                            "and finish the login by hand (2FA/CAPTCHA). Then "
                            "restart the worker."
                        ),
                        details={"audit_dir": self.audit_dir, "site": "fanduel"},
                    )
                    raise WorkerHaltError(f"FanDuel login intervention required: {e}") from e
                except LoginError as e:
                    print(f"❌ FanDuel login failed: {e}")
                    ExecutionLogger.log_execution_failure(
                        "FanDuel login failed", self.opportunity, "fanduel", e,
                    )
                    page_fd.close()
                    return False

                placer_fd = BetPlacer(page_fd, "fanduel", self.audit_dir)

                try:
                    with step_timer("p1_fd_navigate_and_expand", timings):
                        placer_fd.navigate_and_expand_market(self.opportunity, fd_config, fd_direction)
                    with step_timer("p1_fd_find_and_click_bet", timings):
                        placer_fd.find_and_click_bet(self.opportunity, fd_direction, fd_config)

                    # Extract actual FanDuel odds
                    fd_actual_odds = placer_fd.get_actual_odds()

                    with step_timer("p1_fd_discover_max_wager", timings):
                        fd_max_wager, fd_max_text = placer_fd.discover_max_wager()

                    print(f"\n✓ FanDuel max wager: ${fd_max_wager:.2f}")

                    # ---- INTRA-BOOK IDLE (Phase 1 → Phase 2) ----
                    # Browse FD adjacent props for 8-25s before opening BetMGM. Reduces
                    # the cross-book temporal correlation that risk teams cluster on.
                    # By design, NO idle between Phase 2 and Phase 3 (orphan window).
                    try:
                        with step_timer("p1_intra_book_idle", timings):
                            intra_book_idle(
                                page_fd,
                                site="fanduel",
                                check_slip_has_bet=placer_fd.slip_has_visible_selection,
                                current_fd_odds=fd_actual_odds or fd_price_original,
                                read_fd_odds=placer_fd.get_actual_odds,
                            )
                    except (SlipDrainedDuringIdleError, FdOddsDriftedDuringIdleError) as idle_err:
                        print(f"⏭ Idle-window skip: {idle_err}")
                        ExecutionLogger.log_execution_failure(
                            f"Intra-book idle benign skip: {type(idle_err).__name__}",
                            self.opportunity, "fanduel", idle_err,
                        )
                        try:
                            page_fd.close()
                        except Exception:
                            pass
                        raise  # subclass of BetPlacerSkipError → main loop advances

                except BetPlacerSkipError:
                    # Idle-window benign skip (slip drained / FD odds drifted).
                    # Re-raise unwrapped so the outer ``except BetPlacerSkipError``
                    # branch surfaces it to main() without counting as an attempt.
                    # Must be ordered BEFORE ``except BetPlacerError`` since
                    # BetPlacerSkipError subclasses BetPlacerError.
                    raise
                except BetPlacerError as e:
                    print(f"❌ Phase 1 failed: {e}")
                    ExecutionLogger.log_execution_failure("FanDuel limit discovery failed", self.opportunity, "fanduel", e)
                    # Close the FD tab so multi-opportunity loops don't
                    # accumulate orphan tabs across iterations (Chrome ends
                    # up with dozens of FD tabs, FD's bot heuristics may
                    # flag the session as a result).
                    try:
                        page_fd.close()
                    except Exception:
                        pass
                    return False

                # === PHASE 2: Execute BetMGM Primary ===
                print(f"\n{'─'*60}")
                print(f"PHASE 2: PLACE BETMGM BET")
                print(f"{'─'*60}\n")

                # Open BetMGM page
                # Wide desktop viewport (1920x1080): in the narrower
                # 958-wide layout BetMGM renders a mobile-style slip where
                # "Clear All" sits in a position the bot's selectors miss.
                # The desktop right-rail layout makes Clear All a <span>
                # that's actually clickable.
                print("Opening BetMGM tab...")
                page_mgm = context.new_page()
                # See page_fd note: cap action auto-waits, keep nav generous.
                page_mgm.set_default_timeout(8000)
                page_mgm.set_default_navigation_timeout(30000)
                viewport_from_cdp(page_mgm)
                mgm_modal_watcher = ModalWatcher(page_mgm)
                mgm_modal_watcher.start()

                try:
                    ensure_logged_in(page_mgm, "betmgm", self.audit_dir)
                except LoginInterventionRequired as e:
                    ExecutionLogger.log_critical(
                        reason=f"LOGIN INTERVENTION REQUIRED on BetMGM: {e}",
                        opportunity=self.opportunity,
                        action_required=(
                            "Open the BetMGM tab in the bot's Chrome window "
                            "and finish the login by hand (2FA/CAPTCHA). Then "
                            "restart the worker."
                        ),
                        details={"audit_dir": self.audit_dir, "site": "betmgm"},
                    )
                    page_fd.close()
                    raise WorkerHaltError(f"BetMGM login intervention required: {e}") from e
                except LoginError as e:
                    print(f"❌ BetMGM login failed: {e}")
                    ExecutionLogger.log_execution_failure(
                        "BetMGM login failed", self.opportunity, "betmgm", e,
                    )
                    page_mgm.close()
                    page_fd.close()
                    return False

                placer_mgm = BetPlacer(page_mgm, "betmgm", self.audit_dir)

                try:
                    with step_timer("p2_mgm_navigate_and_expand", timings):
                        placer_mgm.navigate_and_expand_market(self.opportunity, mgm_config, mgm_direction)
                    with step_timer("p2_mgm_find_and_click_bet", timings):
                        placer_mgm.find_and_click_bet(self.opportunity, mgm_direction, mgm_config)

                    # Extract actual BetMGM odds
                    mgm_actual_odds = placer_mgm.get_actual_odds()

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

                except BetPlacerSkipError:
                    # Structural skip — std accordion not on this event,
                    # falling through to the merged-alt would misclick.
                    # Re-raise unwrapped so the main loop can advance
                    # without counting this as an attempt. See LOGIC.md.
                    try:
                        page_mgm.close()
                    except Exception:
                        pass
                    try:
                        page_fd.close()
                    except Exception:
                        pass
                    raise
                except BetPlacerError as e:
                    print(f"❌ Phase 2 navigation failed: {e}")
                    ExecutionLogger.log_execution_failure("BetMGM navigation failed", self.opportunity, "betmgm", e)
                    try:
                        page_mgm.close()
                    except Exception:
                        pass
                    try:
                        page_fd.close()
                    except Exception:
                        pass
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
                    with step_timer("p2_mgm_enter_wager", timings):
                        placer_mgm.enter_wager(actual_mgm_stake)

                    # Check for BetMGM max limit alert
                    limit_hit, adjusted_stake = placer_mgm.check_limit_alert()

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

                    with step_timer("p2_mgm_place_bet", timings):
                        mgm_status, mgm_msg = placer_mgm.place_bet()

                    if mgm_status != "ACCEPTED":
                        print(f"❌ BetMGM bet {mgm_status}: {mgm_msg}")
                        ExecutionLogger.log_execution_failure(f"BetMGM {mgm_status}: {mgm_msg}", self.opportunity, "betmgm")
                        page_mgm.close()
                        page_fd.close()
                        return False

                    print(f"\n✓ BetMGM bet ACCEPTED: ${actual_mgm_stake:.2f} @ {mgm_price}")

                    # Mark the asymmetric-risk window OPEN. From here until the
                    # FanDuel hedge confirms, any failure is an orphaned bet.
                    self.betmgm_placed = True
                    self.betmgm_bet_details = {
                        "stake": round(actual_mgm_stake, 2),
                        "price": mgm_price,
                        "side": mgm_direction,
                        "market": mgm_market_key,
                        "line": self.opportunity.get(f"{mgm_direction}_line"),
                        "player": self.opportunity.get("player_name"),
                    }

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

                # Log Phase 1→3 entry elapsed + Phase 3 entry FD odds.
                # NEVER abort Phase 3 on drift — by here BetMGM is placed,
                # aborting = orphan. Always hedge, even at worse odds. The
                # logs are for visibility into the humanized-flow tax.
                phase_1_to_3_entry_ms = int(
                    (time.monotonic() - phase_1_start_monotonic) * 1000
                )
                try:
                    phase_3_entry_fd_odds = placer_fd.get_actual_odds()
                except Exception:
                    phase_3_entry_fd_odds = None
                fd_odds_drift = (
                    phase_3_entry_fd_odds - fd_price
                    if phase_3_entry_fd_odds is not None
                    else None
                )
                drift_str = (
                    f"drift={fd_odds_drift:+.3f}"
                    if fd_odds_drift is not None
                    else "drift=unknown"
                )
                print(
                    f"[timing] phase_1_to_3_entry={phase_1_to_3_entry_ms}ms "
                    f"fd_odds_phase1={fd_price:.3f} "
                    f"fd_odds_phase3={phase_3_entry_fd_odds} {drift_str}"
                )

                try:
                    # FanDuel already has bet in slip, just update wager
                    with step_timer("p3_fd_hedge_enter_wager", timings):
                        placer_fd.enter_wager(fd_hedge_stake)

                    with step_timer("p3_fd_hedge_place_bet", timings):
                        fd_status, fd_msg = placer_fd.place_bet()

                    if fd_status != "ACCEPTED":
                        self._raise_orphaned(
                            reason=f"ORPHANED BET: FanDuel hedge {fd_status}: {fd_msg}",
                            planned_hedge_stake=fd_hedge_stake,
                            planned_hedge_price=fd_price,
                            fd_direction=fd_direction,
                            fd_market_key=fd_market_key,
                        )

                    print(f"\n✓ FanDuel hedge ACCEPTED: ${fd_hedge_stake:.2f} @ {fd_price}")
                    self.fanduel_hedged = True

                except BetPlacerError as e:
                    self._raise_orphaned(
                        reason=f"ORPHANED BET: FanDuel hedge raised {type(e).__name__}: {e}",
                        planned_hedge_stake=fd_hedge_stake,
                        planned_hedge_price=fd_price,
                        fd_direction=fd_direction,
                        fd_market_key=fd_market_key,
                        underlying_error=e,
                    )

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

                # Account-balance scrape removed 2026-05-29: its selectors were
                # placeholders that never matched, so it produced no data while
                # costing ~33s of post-placement page navigations every run. A
                # simpler balance check will be implemented separately.

                # Per-run time study — ranks steps so the biggest waste is obvious.
                _print_cycle_time_breakdown(timings)

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

        except OrphanedBetError:
            # Already alerted via log_critical inside _raise_orphaned. Bubble
            # up so the worker halts the polling loop.
            raise
        except WorkerHaltError:
            # Already alerted via log_critical at the raise site (FD/MGM
            # login intervention or stuck-credential modal). Bubble up so
            # task_worker.py halts instead of churning more attempts
            # against a dead session.
            raise
        except BetPlacerSkipError:
            # Structural skip from Phase 2 accordion-expansion (std O/U
            # not on this event, falling back to merged-alt would
            # misclick). Phase 2's inner catch already re-raised cleanly;
            # surface to the main loop unwrapped so the per-opp handler
            # at execute_arb.main() can advance to the next candidate
            # WITHOUT incrementing attempted_any. Without this branch the
            # SkipError fell into the generic ``except Exception`` below,
            # got reclassified as "Unexpected error", and inflated the
            # circuit-breaker count — observed on 2026-05-21 when several
            # std×std opps on a low-traffic event each tripped a SKIP
            # that the breaker counted as a real failure. See LOGIC.md.
            raise
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            # If we crashed inside the asymmetric-risk window, escalate to
            # CRITICAL so the user knows there's potentially an unhedged bet.
            if self.betmgm_placed and not self.fanduel_hedged:
                self._raise_orphaned(
                    reason=f"ORPHANED BET: unexpected exception after BetMGM placement: {type(e).__name__}: {e}",
                    underlying_error=e,
                )
            ExecutionLogger.log_execution_failure(f"Unexpected error: {e}", self.opportunity, error=e)
            return False
        finally:
            # Stop modal watchers BEFORE recording stop / review marker so
            # any background-thread page activity (modal probe in flight)
            # quiesces before the pages are torn down by the surrounding
            # context manager. stop() swallows internal exceptions and is
            # idempotent (see human/modals.py); safe to call from every
            # exit path even when the watcher was never started.
            if mgm_modal_watcher is not None:
                try:
                    mgm_modal_watcher.stop()
                except Exception as _e:
                    print(f"[modal] mgm watcher stop error (ignored): {_e}")
            if fd_modal_watcher is not None:
                try:
                    fd_modal_watcher.stop()
                except Exception as _e:
                    print(f"[modal] fd watcher stop error (ignored): {_e}")

            stop_recording(record_proc)
            try:
                with open(os.path.join(self.audit_dir, "review.pending"), "w") as _f:
                    _f.write("")
            except Exception as marker_err:
                print(f"[rec] Could not write review.pending: {marker_err}")

    def _raise_orphaned(
        self,
        *,
        reason: str,
        planned_hedge_stake: Optional[float] = None,
        planned_hedge_price: Optional[float] = None,
        fd_direction: Optional[str] = None,
        fd_market_key: Optional[str] = None,
        underlying_error: Optional[Exception] = None,
    ) -> None:
        """Emit a CRITICAL Discord alert with everything the user needs to
        manually hedge the orphaned BetMGM bet, then raise OrphanedBetError.
        """
        bet = self.betmgm_bet_details or {}
        details: Dict = {
            "betmgm_placed_stake": bet.get("stake"),
            "betmgm_placed_price": bet.get("price"),
            "betmgm_side": bet.get("side"),
            "betmgm_market": bet.get("market"),
            "betmgm_line": bet.get("line"),
            "audit_dir": self.audit_dir,
        }
        if planned_hedge_stake is not None:
            details["fanduel_planned_hedge_stake"] = round(planned_hedge_stake, 2)
        if planned_hedge_price is not None:
            details["fanduel_planned_hedge_price"] = planned_hedge_price
        if fd_direction is not None:
            details["fanduel_side"] = fd_direction
        if fd_market_key is not None:
            details["fanduel_market"] = fd_market_key
        if underlying_error is not None:
            details["error"] = f"{type(underlying_error).__name__}: {underlying_error}"

        action = (
            "Open FanDuel and manually place "
            f"${details.get('fanduel_planned_hedge_stake', '?')} on "
            f"{details.get('fanduel_side', '?')} {details.get('fanduel_market', '?')} "
            f"@ ~{details.get('fanduel_planned_hedge_price', '?')} to hedge the "
            f"BetMGM bet (${details.get('betmgm_placed_stake', '?')} on "
            f"{details.get('betmgm_side', '?')} {details.get('betmgm_market', '?')})."
        )

        ExecutionLogger.log_critical(
            reason=reason,
            opportunity=self.opportunity,
            action_required=action,
            details=details,
        )
        raise OrphanedBetError(reason)

    def _warmup_sessions(self) -> None:
        """Warm FD + MGM sessions BEFORE recording starts.

        If a book's session is cold, this triggers the credential login here
        instead of inside the recording, so the recording captures only the
        actual bet-placement work (cleaner duration metrics, easier review).
        Re-uses Chrome's persistent profile — cookies set here persist into
        the recorded session.

        Raises WorkerHaltError if a book needs 2FA/CAPTCHA intervention.
        """
        endpoint_url = ensure_chrome_cdp(profile_dir, CDP_PORT)
        print("Warming sessions (FD + MGM) before recording...")
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(endpoint_url)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            for site in ("fanduel", "betmgm"):
                warm_page = None
                try:
                    warm_page = context.new_page()
                    ensure_logged_in(warm_page, site, self.audit_dir)
                    # Watch for modals (e.g. FanDuel "Reality Check" can
                    # fire during the 12-35s warmup browse) while we do
                    # the homepage dwell that humanizes the session.
                    with ModalWatcher(warm_page):
                        try:
                            warmup_browse(warm_page, site=site)
                        except Exception as warm_err:
                            # Non-fatal: warmup is best-effort. The real
                            # Phase 1/2 attempt does the actual work.
                            print(f"[warmup] {site} warmup_browse failed (non-fatal): {warm_err}")
                except LoginInterventionRequired as e:
                    ExecutionLogger.log_critical(
                        reason=f"LOGIN INTERVENTION REQUIRED on {site} (warmup): {e}",
                        opportunity=self.opportunity,
                        action_required=(
                            f"Open the {site} tab in the bot's Chrome window "
                            "and finish the login by hand (2FA/CAPTCHA). Then "
                            "restart the worker."
                        ),
                        details={"audit_dir": self.audit_dir, "site": site, "phase": "warmup"},
                    )
                    raise WorkerHaltError(f"{site} login intervention required (warmup): {e}") from e
                except LoginError as e:
                    # Log but don't halt — the real Phase 1/2 attempt will hit
                    # the same problem and surface it through the normal path.
                    print(f"[warmup] {site} warmup login failed (will retry in-phase): {e}")
                finally:
                    if warm_page is not None:
                        try:
                            warm_page.close()
                        except Exception:
                            pass


def main(max_attempts: int = 3, max_candidates: Optional[int] = None) -> tuple[bool, bool]:
    """Main execution — iterate candidates until one succeeds, max_attempts is hit,
    or all are exhausted.

    ``max_attempts`` caps how many viable candidates actually call ``execute()``
    in a single invocation (unmapped/skipped candidates don't count). Default
    of 3 keeps recordings small enough to review and prevents long churn
    sessions when every candidate keeps failing.

    ``max_candidates`` caps the total opportunity list considered (including
    unmapped/wrong-book skips). ``None`` (default) means no cap — preserves
    existing worker behavior. Useful for tight recordings: e.g. ``--max-candidates 5``
    bounds the session length regardless of how many skips you hit.

    Returns ``(success, attempted)`` so the worker can distinguish:
      - ``(True,  True)`` — a bet was placed.
      - ``(False, True)`` — a viable candidate was attempted but failed
        (selector regression, rejected wager, etc.). Counts toward the
        circuit breaker.
      - ``(False, False)`` — no viable candidates existed at all (everything
        was unmapped or the wrong book pair). Does NOT count toward the
        breaker — quiet days happen.
    """
    print(f"Arbitrage Bot Starting... (max_attempts={max_attempts}, max_candidates={max_candidates})\n")

    opportunities = fetch_all_opportunities(testing_mode=True)

    if not opportunities:
        print("No opportunities found.")
        return False, False

    if max_candidates is not None and max_candidates > 0 and len(opportunities) > max_candidates:
        print(f"Capping candidate list: {len(opportunities)} -> {max_candidates}")
        opportunities = opportunities[:max_candidates]

    attempted_any = False
    attempts = 0
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

        # Pre-check: ROI threshold (BEFORE any Playwright navigation)
        roi = opportunity.get("roi") or 0
        if roi < MIN_ROI_THRESHOLD:
            print(f"⏭ {label}: ROI {roi * 100:.2f}% below threshold {MIN_ROI_THRESHOLD * 100:.2f}%")
            continue

        # Pre-check: same-player+market cooldown
        cooldown_key = _event_cooldown_key(opportunity)
        if cooldown_key in _attempted_events:
            print(f"⏭ {label}: cooldown — already attempted this session")
            continue

        # Viable candidate — attempt execution
        print(f"\n▶ {label}: attempting execution (attempt {attempts + 1}/{max_attempts})")
        _attempted_events.add(cooldown_key)
        executor = ArbExecutor(opportunity)
        try:
            success = executor.execute()
        except BetPlacerSkipError as e:
            # Structural skip from BetMGM accordion-expansion: the std
            # "O/U" accordion isn't on this event, falling back to
            # merged-alt would misclick. Don't count this as an attempt
            # — advance to the next candidate. See LOGIC.md.
            print(f"\n⏭ {label}: structural skip — {e}")
            ExecutionLogger.log_execution_failure(
                "Structural skip (std accordion missing on this event)",
                opportunity, "betmgm", e,
            )
            continue
        attempted_any = True
        attempts += 1

        if success:
            opp_hash = _opportunity_hash(opportunity)
            opportunity["market_key"] = opportunity.get("under_market_key") or opportunity.get("over_market_key")
            mark_opportunity_executed(opp_hash, opportunity)
            print("\n✓ Execution complete")
            return True, True

        # Execution failed but no money committed — orphan-bet paths raise
        # OrphanedBetError which bypasses this loop entirely. With the
        # post-clear assertive slip-clear in BetPlacer, residual betslip
        # state from this attempt will raise on the NEXT opportunity's
        # navigation, so advancing is safe.
        print(f"\n✗ Execution failed for {label} — advancing to next opportunity")

        if attempts >= max_attempts:
            print(f"\n⏹ Hit max_attempts cap ({max_attempts}) — stopping iteration")
            break
        continue

    if attempted_any:
        # Tried at least one viable candidate, none succeeded.
        # attempted=True so the worker's circuit breaker counts the
        # exhausted run — repeated full-queue exhaustion signals a real
        # selector regression worth halting on.
        print("\n✗ All opportunities exhausted after attempts — none placed")
        return False, True

    print("\n✗ All opportunities exhausted — none viable")
    return False, False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Execute arbitrage opportunities.")
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Cap how many viable candidates actually call execute() in one run. "
             "Unmapped/skipped candidates don't count. Default: 3.",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=None,
        help="Cap the total candidate list considered (including skips). "
             "Default: no cap. Useful for tight recordings, e.g. --max-candidates 5.",
    )
    args = parser.parse_args()

    success, _ = main(max_attempts=args.max_attempts, max_candidates=args.max_candidates)
    raise SystemExit(0 if success else 1)
