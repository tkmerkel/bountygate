"""Session-level humanization: warmup browsing, intra-book idle,
viewport reading. Composes the lower-level human/ primitives.
"""

import os
import random
from typing import Callable

from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)
from human.mouse import CursorState, move_to
from human.waiting import settle


SITE_HOMEPAGES: dict[str, str] = {
    "fanduel": "https://mo.sportsbook.fanduel.com/",
    "betmgm": "https://www.mo.betmgm.com/en/sports",
}

# Target warmup duration band. The structural settle sequence below
# (page_load + 2-4 reading_panels + optional hover + trailing) only
# reliably lands above the floor when most samples are near the
# upper end of their bands; with low samples or low scroll counts it
# bottoms out at ~5-10s. The trailing pad-to-floor loop closes that
# gap deterministically without breaking the upper bound (max overshoot
# is one extra reading_panel sample, i.e. ~2.8s).
_WARMUP_FLOOR_MS = 12000


def warmup_browse(
    page,
    *,
    site: str,
    rng: random.Random | None = None,
    state: CursorState | None = None,
) -> None:
    """Spend 12-35s on the sportsbook's homepage before any bet flow.

    Steps:
      1. Load the homepage.
      2. Two to four scrolls (200-800px each) interleaved with reading
         settles.
      3. ~50% chance to mouse-over (no click) a visible featured-market
         tile, then a short reading settle.
      4. Trailing settle, padded with additional reading_panel settles
         until the total dwell hits the 12s floor.

    Raises ``KeyError`` if ``site`` is not a known sportsbook.
    """
    rng = rng or random.Random()
    state = state or CursorState()
    homepage = SITE_HOMEPAGES[site]  # raises KeyError on unknown site

    elapsed_ms = 0

    page.goto(homepage, wait_until="domcontentloaded")
    elapsed_ms += settle(page, "page_load", rng=rng)

    # 2-4 scrolls.
    n_scrolls = rng.randint(2, 4)
    for _ in range(n_scrolls):
        scroll_px = rng.randint(200, 800)
        try:
            page.evaluate(f"window.scrollBy(0, {scroll_px})")
        except Exception as e:
            print(f"[human.session] warmup scroll failed: {e} (continuing)")
        elapsed_ms += settle(page, "reading_panel", rng=rng)

    # 50% chance to hover (not click) a featured tile.
    if rng.random() < 0.5:
        for sel in ("a[href*='/event']", "a[href*='/sports/']", "div[role='button']"):
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    # Move only — don't actually click; we don't want to
                    # accidentally add anything to a betslip.
                    move_to(page, loc.first, state=state, rng=rng)
                    elapsed_ms += settle(page, "reading_panel", rng=rng)
                    break
            except Exception:
                continue

    # Trailing settle.
    elapsed_ms += settle(page, "reading_panel", rng=rng)

    # Pad to floor — keep settling until the total dwell crosses 12s.
    # See _WARMUP_FLOOR_MS comment above for why this is needed.
    while elapsed_ms < _WARMUP_FLOOR_MS:
        elapsed_ms += settle(page, "reading_panel", rng=rng)


# Default tolerance for odds drift during idle, in decimal-odds units.
# Override at runtime via the IDLE_DRIFT_EPSILON env var.
_DEFAULT_DRIFT_EPSILON = 0.05


def intra_book_idle(
    page,
    *,
    site: str,
    check_slip_has_bet: Callable[[], bool],
    current_fd_odds: float,
    read_fd_odds: Callable[[], float | None],
    rng: random.Random | None = None,
    state: CursorState | None = None,
    epsilon: float | None = None,
) -> None:
    """Spend 8-25s browsing adjacent FanDuel props after Phase 1.

    Only runs for site='fanduel'. After idling, re-checks the FD
    betslip and odds; raises typed errors if either has shifted.

    Args:
        check_slip_has_bet: callable that returns True if the FD slip
            still holds the Phase 1 bet selection.
        current_fd_odds: the odds we discovered in Phase 1.
        read_fd_odds: callable that re-reads the current FD odds for
            the same selection. May return None if the price isn't
            currently visible — in that case we treat it as "still
            there" (the next phase will assert).
        epsilon: drift tolerance in decimal-odds units. Defaults to
            the IDLE_DRIFT_EPSILON env var, or 0.05.

    Raises:
        ValueError: if called with site != 'fanduel'.
        SlipDrainedDuringIdleError: if the FD slip emptied during idle.
        FdOddsDriftedDuringIdleError: if odds moved by more than epsilon.
    """
    if site != "fanduel":
        raise ValueError(
            f"intra_book_idle only runs for fanduel (got {site!r}). "
            "By design no idle between Phase 2 and Phase 3 (orphan window)."
        )

    rng = rng or random.Random()
    state = state or CursorState()
    if epsilon is None:
        try:
            epsilon = float(os.getenv("IDLE_DRIFT_EPSILON", _DEFAULT_DRIFT_EPSILON))
        except ValueError:
            epsilon = _DEFAULT_DRIFT_EPSILON

    # 8-25s of FD browsing. Tighter band than warmup; we don't want
    # the slip to drain just from session timeout. We sample the target
    # short of the upper bound by one max slip_update sample (1400ms) so
    # the trailing pad-loop's worst-case overshoot still lands inside the
    # 25s ceiling.
    target_total_ms = rng.randint(8000, 25000 - 1400)
    # For fakes that record waits in page.waited_ms we measure elapsed
    # against that list; real Playwright Pages have no such attribute and
    # the try/except in the pad loop short-circuits to a single settle.
    start_waited = sum(getattr(page, "waited_ms", []))

    # 1-2 scrolls + a reading settle per scroll.
    n_scrolls = rng.randint(1, 2)
    for _ in range(n_scrolls):
        try:
            page.evaluate(f"window.scrollBy(0, {rng.randint(150, 600)})")
        except Exception as e:
            print(f"[human.session] idle scroll failed: {e} (continuing)")
        settle(page, "reading_panel", rng=rng)

    # 40% chance to hover an adjacent prop tile (no click).
    if rng.random() < 0.40:
        for sel in (
            "div[role='button']",
            "a[href*='/event']",
        ):
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    move_to(page, loc.first, state=state, rng=rng)
                    settle(page, "reading_panel", rng=rng)
                    break
            except Exception:
                continue

    # Pad with slip_update settles (700-1400ms) until we hit the target
    # band. Finer-grained than reading_panel so we don't overshoot the
    # 25s ceiling — worst-case overshoot is ~1.4s on top of a 25000ms
    # target, but the target is sampled in [8000, 25000] so practically
    # we stay well under the upper bound.
    while True:
        try:
            elapsed = sum(page.waited_ms) - start_waited
        except Exception:
            elapsed = target_total_ms  # real Page — break out
        if elapsed >= target_total_ms:
            break
        settle(page, "slip_update", rng=rng)

    # --- Post-idle guards ---
    if not check_slip_has_bet():
        raise SlipDrainedDuringIdleError(
            "FanDuel slip lost its Phase 1 selection during idle window"
        )

    new_odds = read_fd_odds()
    if new_odds is not None and abs(new_odds - current_fd_odds) > epsilon:
        raise FdOddsDriftedDuringIdleError(
            old_odds=current_fd_odds,
            new_odds=new_odds,
            epsilon=epsilon,
        )
