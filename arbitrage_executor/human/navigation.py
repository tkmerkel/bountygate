"""Humanized link-following navigation.

Instead of ``page.goto(direct_event_url)``, real users:
  1. land on a hub page (sportsbook home, search results, etc.)
  2. scroll a bit
  3. find the link they want
  4. click it

That's what ``click_through`` does. When the link isn't findable
(layout changed, market suspended, etc.) it falls back to the
direct-URL goto and logs LOUDLY — a quiet fallback is the difference
between "this works" and "this used to work but stopped two weeks
ago and nobody noticed."
"""

import random

from human.mouse import CursorState, click as mouse_click
from human.waiting import settle


def click_through(
    page,
    *,
    start_url: str,
    link_selector: str,
    fallback_url: str,
    state: CursorState,
    rng: random.Random | None = None,
    scroll_px_range: tuple[int, int] = (200, 800),
) -> bool:
    """Browse to ``start_url``, scroll a bit, look for ``link_selector``,
    click it humanly. If not found, fall back to ``goto(fallback_url)``
    and log the miss.

    Returns:
        True if the humanized path was taken, False if we fell back.
    """
    rng = rng or random.Random()

    page.goto(start_url, wait_until="domcontentloaded")
    settle(page, "page_load", rng=rng)

    # Small scroll — a few hundred px down, then a beat.
    scroll_px = rng.randint(*scroll_px_range)
    try:
        page.evaluate(f"window.scrollBy(0, {scroll_px})")
    except Exception as e:
        print(f"[human.navigation] scroll failed: {e} (continuing)")
    settle(page, "reading_panel", rng=rng)

    try:
        loc = page.locator(link_selector)
        if loc.count() > 0 and loc.first.is_visible():
            mouse_click(page, loc.first, state=state, rng=rng)
            settle(page, "page_load", rng=rng)
            return True
    except Exception as e:
        print(f"[human.navigation] link probe failed: {e}")

    # Fallback — loud log, then direct goto.
    print(
        f"[human.navigation] ⚠ fallback to direct goto: "
        f"link {link_selector!r} not found on {start_url}, navigating to {fallback_url}"
    )
    page.goto(fallback_url, wait_until="domcontentloaded")
    settle(page, "page_load", rng=rng)
    return False
