"""Opportunistic modal dismisser.

Sportsbook UIs interrupt with modals at unpredictable times:
  - FanDuel "Reality Check" every ~270 minutes of session activity
  - BetMGM responsible-gambling popups
  - Promotional overlays at first visit

Earlier design: a background thread polled each tab at 800-1500ms and
clicked dismiss buttons opportunistically. That violated Playwright's
sync API thread-affinity rule — calling ``page.locator()`` from a
non-creator thread races the main thread's CDP transport. Symptom:
intermittent ``greenlet.error`` or silently misrouted CDP responses
under load.

Current design: ``ModalWatcher.start()`` registers the watcher in a
module-level set; ``settle()`` (in ``human/waiting.py``) walks the
registered watchers on the MAIN THREAD before each wait and calls
``check_once()`` on each. No background thread; all Playwright calls
happen on the thread that created the page. Public surface — start /
stop / context manager / is_running — is preserved so call sites in
``execute_arb.py`` don't need to change.

Trade-off: modals that fire DURING a long Playwright op (e.g. a 10s
``wait_for_selector``) are no longer dismissed mid-flight. They get
caught at the next ``settle()`` call instead, which is typically a few
hundred ms to a couple seconds later. In practice the bot calls
``settle()`` at every flow boundary, so the latency penalty is small
and bounded — and we lose zero hedging windows because the main
thread, not a racing watcher, drives every page interaction.
"""

import weakref


# Selectors that match the modals we see most often. Order is
# best-match-first so we don't fire a generic-overlay close on a
# modal that's about to be dismissed by a more specific path.
_MODAL_SELECTORS = (
    'div[role="dialog"][aria-modal="true"]',
    'div[class*="modal"][class*="open"]',
    'div[class*="reality-check"]',
)


# WeakSet so an un-stopped watcher doesn't keep its page alive after
# Playwright closes the context. Iteration is main-thread-only — the
# whole point of this refactor — so no lock is required.
_active_watchers: "weakref.WeakSet[ModalWatcher]" = weakref.WeakSet()


class ModalWatcher:
    """Main-thread modal dismisser. Construct, ``start()``, later
    ``stop()``. Or use as a context manager.

    No background thread — ``settle()`` calls ``check_once()`` on every
    active watcher before each wait.

    Args:
        page: Playwright Page (or test fake).
        poll_range_ms: Kept for backward compatibility but unused. The
            polling cadence is now whatever ``settle()`` natural cadence
            is across the bot flow.
    """

    def __init__(self, page, *, poll_range_ms: tuple[int, int] = (800, 1500)):
        self._page = page
        # poll_range_ms is retained for call-site compatibility but the
        # background thread it controlled is gone.
        self._poll_range_ms = poll_range_ms
        self._active = False

    def start(self) -> None:
        if self._active:
            return
        self._active = True
        _active_watchers.add(self)

    def stop(self) -> None:
        if not self._active:
            return
        self._active = False
        _active_watchers.discard(self)

    def is_running(self) -> bool:
        return self._active

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()
        return False

    def check_once(self) -> bool:
        """Look for any known modal on this page; if visible, click its
        first button. Returns True if a dismiss click fired, False
        otherwise. Never raises — modal dismissal must NEVER take down
        the main flow.
        """
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
                    return True
            except Exception as e:
                print(f"[human.modals] dismiss attempt error (ignored): {e}")
                continue
        return False


def check_all_active() -> int:
    """Call ``check_once()`` on every active watcher. Returns the
    number of modals dismissed in this sweep.

    Called by ``settle()`` before each wait. Main-thread only.
    """
    dismissed = 0
    # Snapshot to a list — a dismiss-and-stop pattern in a watcher
    # would otherwise mutate the WeakSet mid-iteration.
    for watcher in list(_active_watchers):
        try:
            if watcher.check_once():
                dismissed += 1
        except Exception as e:
            print(f"[human.modals] watcher tick error (ignored): {e}")
    return dismissed


def _active_watcher_count() -> int:
    """Test helper — number of currently registered watchers."""
    return len(_active_watchers)
