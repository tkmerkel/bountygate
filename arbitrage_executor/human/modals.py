"""Opportunistic modal dismisser.

Sportsbook UIs interrupt with modals at unpredictable times:
  - FanDuel "Reality Check" every ~30 minutes of session activity
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
    # FD ships its Reality Check / responsible-gambling modals via
    # ReactModal; the open marker is the --after-open suffix on the
    # overlay div. Caught sweep #6 dying on market 1 (2026-05-25)
    # because none of the selectors above matched FD's actual class.
    '.ReactModal__Overlay.ReactModal__Overlay--after-open',
)


# Known safe dismiss-button names. Tried in order on a visible modal
# BEFORE the sole-button fallback. Each entry must be an unambiguous
# "I've read this, proceed" action — clicking it must not navigate
# the page or undo prior state.
#
# "Close" was here historically but sweep #8 (2026-05-25) caught it
# detaching the email-input frame on FD's login interstitial: a
# role=dialog with both "Close" and a proceed-button, where Close
# kicked the user back to home. Single-button "Close" modals are
# still handled — the sole-button fallback below catches them.
_DISMISS_BUTTON_NAMES = (
    "Continue Playing",   # FD Reality Check primary action
    "I Understand",       # FD Reality Check alternate
    "Acknowledge",        # FD responsible-gambling variants
    "Got it",             # generic promo overlays
    "Done",
    "OK",
    "Continue",
    "Accept",
    "Dismiss",
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
        """Look for any known modal on this page; if visible, dismiss
        it via a known-safe button name (preferred) or the sole button
        if there's exactly one. Returns True if a dismiss click fired,
        False otherwise. Never raises — modal dismissal must NEVER
        take down the main flow.

        Skips modals that contain input fields — those are credential
        / form modals where blindly clicking a button could cancel a
        user-driven flow (login, deposit, etc.).
        """
        for sel in _MODAL_SELECTORS:
            try:
                modal = self._page.locator(sel)
                if modal.count() == 0:
                    continue
                if not modal.first.is_visible():
                    continue

                # Credential/form-bearing modals — leave them alone.
                try:
                    if modal.first.locator("input").count() > 0:
                        continue
                except Exception:
                    pass

                # Prefer a known-safe dismiss-button name. Tries each
                # via get_by_role so we match accessible name, not
                # rotating class identifiers.
                for name in _DISMISS_BUTTON_NAMES:
                    try:
                        named = modal.first.get_by_role("button", name=name)
                        if named.count() > 0 and named.first.is_visible():
                            print(
                                f"[human.modals] dismissing modal via "
                                f"{sel} → {name!r}"
                            )
                            named.first.click()
                            return True
                    except Exception:
                        continue

                # Sole-button fallback — safe only when the modal has
                # exactly one button (e.g. a "Continue" or "X" close).
                buttons = modal.first.locator("button")
                try:
                    if buttons.count() == 1 and buttons.first.is_visible():
                        print(
                            f"[human.modals] dismissing modal via "
                            f"{sel} (sole button)"
                        )
                        buttons.first.click()
                        return True
                except Exception:
                    pass
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
