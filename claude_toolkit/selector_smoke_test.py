"""Pre-session selector smoke test.

Launches Chrome via the existing (frozen) ensure_chrome_cdp, connects with
Playwright, and navigates to the search page on FanDuel and BetMGM. Asserts
that the top-level search-input selectors used by bet_placer.py still
resolve. Takes a screenshot on each site for visual diff.

This script NEVER clicks a "Place Bet" button. It does not tick a wager
amount. It does not submit a ticket. It intentionally does not even click
into a specific market -- the goal is to catch catastrophic drift (search
input disappeared, page redirects to a different URL) before a trading
session, not to exercise the full placement path.

Requires --player for FanDuel and --team for BetMGM (no stale defaults).

Usage:
    python claude_toolkit/selector_smoke_test.py --player "Evan Mobley" --team "Cleveland Cavaliers"
    python claude_toolkit/selector_smoke_test.py --player "LeBron James" --skip-betmgm
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_EXECUTOR = os.path.join(_REPO_ROOT, "arbitrage_executor")
if _EXECUTOR not in sys.path:
    sys.path.insert(0, _EXECUTOR)

from chrome_helpers import CDP_PORT, ensure_chrome_cdp  # noqa: E402


# Anchor profile_dir to the executor directory, not CWD, so running the
# smoke test from the repo root still uses the logged-in Chrome profile.
# (The critique flags the CWD-based default in chrome_helpers.py as P1 #9.)
PROFILE_DIR = os.path.join(_EXECUTOR, "chrome_profile")

SCREENSHOT_DIR = os.path.join(_REPO_ROOT, "claude_toolkit", ".smoke_screenshots")

FANDUEL_SEARCH_URL = "https://mo.sportsbook.fanduel.com/search"
BETMGM_SEARCH_URL = "https://www.mo.betmgm.com/en/sports?popup=betfinder"

FANDUEL_SEARCH_INPUT = 'input[placeholder="Search"], div.aq input'
BETMGM_SEARCH_INPUT = (
    'div.cdk-overlay-container input, '
    'input[placeholder*="Search"], '
    'input[placeholder*="Find"]'
)

# Hard safety: if we ever land on one of these URLs, abort -- we should
# never be near a confirm/place step from this script.
FORBIDDEN_URL_FRAGMENTS = ("betslip-confirm", "place-bet", "confirm-bet", "/confirm")


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _assert_not_forbidden(page, label: str) -> None:
    url = (page.url or "").lower()
    for fragment in FORBIDDEN_URL_FRAGMENTS:
        if fragment in url:
            raise RuntimeError(
                f"Safety abort: {label} page landed on forbidden URL fragment "
                f"'{fragment}' (url={page.url}). Smoke test must never reach "
                "confirm/place state."
            )


def _smoke_fanduel(context, player: str, results: list) -> None:
    page = context.new_page()
    try:
        print(f"[FANDUEL] navigating {FANDUEL_SEARCH_URL}")
        page.goto(FANDUEL_SEARCH_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        _assert_not_forbidden(page, "FanDuel after goto")

        locator = page.locator(FANDUEL_SEARCH_INPUT).first
        try:
            locator.wait_for(state="visible", timeout=15000)
            results.append(("fanduel_search_input", True, "visible"))
        except Exception as e:
            results.append(("fanduel_search_input", False, f"{type(e).__name__}: {e}"))

        try:
            locator.fill(player)
            page.keyboard.press("Enter")
            page.wait_for_timeout(3000)
            _assert_not_forbidden(page, "FanDuel after search")
            results.append(("fanduel_search_submit", True, f"query='{player}'"))
        except RuntimeError:
            raise
        except Exception as e:
            results.append(("fanduel_search_submit", False, f"{type(e).__name__}: {e}"))

        shot = os.path.join(SCREENSHOT_DIR, f"{_timestamp()}_fanduel.png")
        page.screenshot(path=shot, full_page=False)
        print(f"[FANDUEL] screenshot -> {shot}")
    finally:
        page.close()


def _smoke_betmgm(context, team: str, results: list) -> None:
    page = context.new_page()
    try:
        print(f"[BETMGM] navigating {BETMGM_SEARCH_URL}")
        page.goto(BETMGM_SEARCH_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        _assert_not_forbidden(page, "BetMGM after goto")

        locator = page.locator(BETMGM_SEARCH_INPUT).first
        try:
            locator.wait_for(state="visible", timeout=10000)
            results.append(("betmgm_search_input", True, "visible"))
        except Exception as e:
            results.append(("betmgm_search_input", False, f"{type(e).__name__}: {e}"))

        try:
            locator.fill(team)
            page.wait_for_timeout(2000)
            _assert_not_forbidden(page, "BetMGM after fill")
            results.append(("betmgm_search_fill", True, f"query='{team}'"))
        except RuntimeError:
            raise
        except Exception as e:
            results.append(("betmgm_search_fill", False, f"{type(e).__name__}: {e}"))

        shot = os.path.join(SCREENSHOT_DIR, f"{_timestamp()}_betmgm.png")
        page.screenshot(path=shot, full_page=False)
        print(f"[BETMGM] screenshot -> {shot}")
    finally:
        page.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--player", required=True,
                        help="Player name to search on FanDuel (e.g. 'Evan Mobley').")
    parser.add_argument("--team", default=None,
                        help="Team name to search on BetMGM (e.g. 'Cleveland Cavaliers'). "
                             "Defaults to --player if omitted.")
    parser.add_argument("--skip-fanduel", action="store_true")
    parser.add_argument("--skip-betmgm", action="store_true")
    args = parser.parse_args()

    team = args.team or args.player

    os.makedirs(SCREENSHOT_DIR, exist_ok=True)

    endpoint = ensure_chrome_cdp(PROFILE_DIR, CDP_PORT)
    print(f"Chrome CDP endpoint: {endpoint}")

    from playwright.sync_api import sync_playwright
    results: list[tuple[str, bool, str]] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(endpoint)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            if not args.skip_fanduel:
                _smoke_fanduel(context, args.player, results)
            if not args.skip_betmgm:
                _smoke_betmgm(context, team, results)
    except RuntimeError as e:
        print(f"\nABORTED: {e}", file=sys.stderr)
        return 3

    print()
    print("=== Selector smoke test results ===")
    ok = 0
    for name, passed, detail in results:
        mark = "PASS" if passed else "FAIL"
        print(f"  {mark}  {name:30s}  {detail}")
        if passed:
            ok += 1
    print(f"\n{ok}/{len(results)} selectors resolved.")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
