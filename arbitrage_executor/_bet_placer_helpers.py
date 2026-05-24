"""Shared helpers for bet_placer site implementations.

Kept narrow on purpose. Modal dismissal, slip inspection, and wager-
entry quirks are site-specific and live in their respective subclass.
What lives here is the handful of patterns that recurred verbatim
across both placers (FD + BetMGM):

  - ``screenshot`` — audit-log capture, never raises
  - ``first_visible`` — try N selectors, return first visible match
  - ``dump_miss_context`` — diagnostic aria/button/pick dump on "not
    found" failures (used at every miss-raise site)
  - ``poll_until`` — settle-and-check loop with screenshot + raise
    on timeout
  - ``with_screenshot_on_error`` — context manager that captures a
    screenshot and re-raises as BetPlacerError; preserves
    BetPlacerSkipError so structural-skip classification doesn't
    get swallowed
"""

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Callable, Iterable, Optional, Sequence, Tuple

from playwright.sync_api import Page, Locator

def screenshot(page: Page, audit_dir: str, site: str, tag: str) -> str:
    """Save screenshot for audit trail. Never raises — logs and returns
    the intended path even if capture fails."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(audit_dir, f"{site}_{tag}_{timestamp}.png")
    try:
        page.screenshot(path=filename, full_page=True)
    except Exception as e:
        print(f"⚠ Screenshot failed: {e}")
    return filename


def first_visible(
    page: Page,
    selectors: Iterable[str],
    *,
    label: str = "",
    site: str = "",
) -> Optional[Locator]:
    """Try each CSS selector in order; return the first locator whose
    `.first` is visible, or None. Logs which selector matched on success
    so the audit trail keeps parity with legacy inline logging.

    Caller is responsible for any selector with placement-sensitive
    semantics (e.g. picking the LAST empty stake input, not the first
    visible one) — that logic stays inline in the caller.
    """
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                if label:
                    print(f"[{site.upper()}] {label} via {sel}")
                return loc.first
        except Exception:
            continue
    return None


def dump_miss_context(
    page: Page,
    *,
    site: str,
    player_name: str,
    extra_locators: Sequence[Tuple[str, str]] = (),
    max_matches: int = 10,
) -> None:
    """Print up to ``max_matches`` matches for each of the standard
    diagnostic queries, plus any caller-supplied (label, selector)
    pairs. Used at every "no bet found" / "no row matched" failure
    site so the audit log captures what WAS on the page when we
    expected the player and didn't find them.

    Queries always run:
      - ``[aria-label*="<player>"]`` — picks up role=button tiles
      - ``button:has-text("<player>")`` — picks up FD-style buttons

    ``extra_locators`` is for site-specific shapes — e.g. BetMGM's
    ``ms-event-pick`` custom element. Each tuple is ``(label, selector)``
    where ``label`` is the print prefix and ``selector`` is the
    Playwright selector string.

    Never raises — diagnostic output must never crash the failure path
    it's annotating. Every internal Locator error is swallowed.
    """
    site_prefix = f"[{site.upper()}]"

    def _dump(label: str, selector: str, get_label: Callable[[Locator], str]) -> None:
        try:
            loc = page.locator(selector)
            samples = []
            for i in range(min(loc.count(), max_matches)):
                try:
                    samples.append(get_label(loc.nth(i)))
                except Exception:
                    continue
            print(
                f"{site_prefix} {label} mentioning {player_name!r} "
                f"({len(samples)}): {samples!r}"
            )
        except Exception:
            pass

    _dump(
        "aria-labels",
        f'[aria-label*="{player_name}"]',
        lambda el: el.get_attribute("aria-label") or "",
    )
    _dump(
        "buttons",
        f'button:has-text("{player_name}")',
        lambda el: ((el.text_content() or "").strip())[:120],
    )
    for label, selector in extra_locators:
        _dump(
            label,
            selector,
            lambda el: ((el.text_content() or "").strip())[:120],
        )


def poll_until(
    *,
    page: Page,
    settle_fn: Callable[[Page, str], int],
    settle_category: str,
    rng,
    condition: Callable[[], bool],
    attempts: int,
    on_timeout_screenshot: Optional[Callable[[], None]] = None,
    on_timeout_message: str = "poll_until timed out",
) -> None:
    """Repeatedly ``settle(page, settle_category)`` then check ``condition()``.

    Returns on the first iteration where ``condition()`` returns True.
    On timeout (``attempts`` iterations without success): calls
    ``on_timeout_screenshot`` (if provided) and raises
    ``BetPlacerError(on_timeout_message)``.

    Args:
        settle_fn: the actual ``human.waiting.settle`` function — passed
            in so this helper doesn't have to import it directly (and
            so tests can inject a no-op fake).
        condition: zero-arg callable that returns True when the state
            we're waiting for has landed. Exceptions inside ``condition``
            propagate out — callers wrap their own try/except inside
            ``condition`` if they need to swallow.
        on_timeout_screenshot: zero-arg callback fired when we time out.
            Typical use: ``lambda: self._screenshot("slip_poll_timeout")``.
    """
    from bet_placer import BetPlacerError  # local import — avoids cycle
    for _ in range(attempts):
        settle_fn(page, settle_category, rng=rng)
        if condition():
            return
    if on_timeout_screenshot is not None:
        try:
            on_timeout_screenshot()
        except Exception:
            pass
    raise BetPlacerError(on_timeout_message)


@contextmanager
def with_screenshot_on_error(placer, tag: str, message: str):
    """Context manager: on any non-BetPlacer{,Skip}Error exception, fire
    a screenshot then re-raise the original as a BetPlacerError with
    a human-readable message.

    Preserves BetPlacerSkipError and existing BetPlacerError so a
    nested classification doesn't get reclassified — the orchestrator's
    ``except BetPlacerSkipError`` branch (PR #30) depends on these
    propagating up unwrapped.

    Args:
        placer: the BetPlacer instance — used for its ``_screenshot``.
        tag: screenshot tag string (becomes part of the file name).
        message: free-text prefix for the re-raised BetPlacerError;
            the original exception is appended.
    """
    from bet_placer import BetPlacerError, BetPlacerSkipError
    try:
        yield
    except BetPlacerSkipError:
        raise
    except BetPlacerError:
        raise
    except Exception as e:
        try:
            placer._screenshot(tag)
        except Exception:
            pass
        raise BetPlacerError(f"{message}: {e}") from e
