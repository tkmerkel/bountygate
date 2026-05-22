"""Background modal watcher.

Sportsbook UIs interrupt with modals at unpredictable times:
  - FanDuel "Reality Check" every ~270 minutes of session activity
  - BetMGM responsible-gambling popups
  - Promotional overlays at first visit

The legacy approach was to call ``_dismiss_*_modal()`` at the top of
every navigation. That:
  - runs even when there's no modal (no-op cost)
  - misses modals that fire between navigations
  - is duplicated across both placers

A background watcher polls each tab at a random cadence (800-1500ms)
and dismisses modals opportunistically. One watcher per tab, started
when the tab opens, stopped when it closes.

Threading note: the watcher uses its own ``random.Random`` instance
(the module-level ``random`` is not thread-safe). The Playwright sync
API is also not thread-safe across calls, but read-only ``count`` /
``is_visible`` probes are safe enough in practice — we only mutate
the page (click) when we see a modal, and at that point the main
thread is almost always blocked on a wait.
"""

import random
import threading
import time
from typing import Optional


# Selectors that match the modals we see most often. Order is
# best-match-first so we don't fire a generic-overlay close on a
# modal that's about to be dismissed by a more specific path.
_MODAL_SELECTORS = (
    'div[role="dialog"][aria-modal="true"]',
    'div[class*="modal"][class*="open"]',
    'div[class*="reality-check"]',
)


class ModalWatcher:
    """Background-thread modal dismisser. Construct, ``start()``,
    later ``stop()``. Or use as a context manager.

    Args:
        page: Playwright Page (or test fake).
        poll_range_ms: (min, max) for the random poll interval.
    """

    def __init__(self, page, *, poll_range_ms: tuple[int, int] = (800, 1500)):
        self._page = page
        self._poll_lo, self._poll_hi = poll_range_ms
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Thread-local RNG — random module is not thread-safe.
        self._rng = random.Random()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()
        return False

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._maybe_dismiss_once()
            except Exception as e:
                # NEVER let the watcher take down the main flow.
                print(f"[human.modals] watcher tick error (ignored): {e}")
            sleep_ms = self._rng.randint(self._poll_lo, self._poll_hi)
            # Sleep in 50ms slices so stop() doesn't take 1.5s to land.
            slept = 0
            while slept < sleep_ms and not self._stop_event.is_set():
                step = min(50, sleep_ms - slept)
                time.sleep(step / 1000.0)
                slept += step

    def _maybe_dismiss_once(self) -> None:
        for sel in _MODAL_SELECTORS:
            try:
                modal = self._page.locator(sel)
                if modal.count() == 0:
                    continue
                if not modal.first.is_visible():
                    continue
                buttons = modal.first.locator("button")
                if buttons.count() > 0:
                    print(f"[human.modals] dismissing modal via {sel}")
                    buttons.first.click()
                    return
            except Exception:
                continue
