"""Session-level humanization: warmup browsing, intra-book idle,
viewport reading. Composes the lower-level human/ primitives.
"""

import random
from typing import Optional

from human.mouse import CursorState, click as mouse_click
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
                    from human.mouse import move_to
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
