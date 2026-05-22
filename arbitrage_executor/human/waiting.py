"""Categorized waits — replaces naked ``page.wait_for_timeout(...)`` calls.

Every wait in the bot exists for a *reason*: page is loading, a user
would be reading a panel, a UI animation needs to settle, a result
needs a moment to render. Naming the reason makes audits possible:
"the bot waited 5s here because we expect a search-results render."

Each category is a (lower_ms, upper_ms) band; ``settle`` samples
uniformly within the band so cross-attempt timing isn't suspiciously
constant.
"""

import random
from typing import Protocol


class _Page(Protocol):
    def wait_for_timeout(self, ms: int) -> None: ...


# Categories — bounds are inclusive. Tune these as we learn what feels
# right; they intentionally err on the longer side to give the modal
# watcher and the lazy-loaded UI time to settle.
WAIT_CATEGORIES: dict[str, tuple[int, int]] = {
    # Generic post-navigation, DOM-loaded but JS still mounting.
    "page_load":            (1800, 3200),
    # Search input has been filled; suggestions/results render.
    "search_results":       (2400, 3800),
    # An accordion or panel has been clicked and is expanding.
    "ui_expansion":         (600, 1300),
    # A modal dismiss button has been clicked; modal animates out.
    "modal_dismiss":        (900, 1700),
    # A bet has just been clicked; slip pill updates.
    "slip_update":          (700, 1400),
    # User-reads-and-decides pause — used between Phase 1 and a follow-up.
    "reading_panel":        (1200, 2800),
    # Between keystrokes during typing (note: typing.py has its own
    # finer-grained per-keystroke distribution; this is for bulk delays
    # like the pre-Enter dwell).
    "pre_submit_dwell":     (450, 950),
    # Very short — between two related clicks in a flow.
    "micro_pause":          (180, 420),
}


def settle(
    page: _Page,
    category: str,
    *,
    jittered: bool = True,
    rng: random.Random | None = None,
) -> int:
    """Wait for the band documented under ``category``.

    Args:
        page: anything that quacks like ``Page.wait_for_timeout(ms)``.
        category: one of the keys in ``WAIT_CATEGORIES``. Raises
            ``KeyError`` if not — typo-safety.
        jittered: when True (default), sample uniformly in the band.
            When False, use the band midpoint — for tests that want
            deterministic timing.
        rng: explicit RNG for tests. Defaults to the module-level
            ``random`` instance.

    Returns:
        The number of milliseconds actually waited.
    """
    lo, hi = WAIT_CATEGORIES[category]
    if jittered:
        r = rng or random
        ms = int(r.uniform(lo, hi))
    else:
        ms = (lo + hi) // 2
    page.wait_for_timeout(ms)
    return ms
