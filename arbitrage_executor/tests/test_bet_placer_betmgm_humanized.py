"""Tests for the humanized BetMGM placer (Tasks 12 + 13).

Covers the lazy slip-clear (pill==0 short-circuit), the full clear dance
when the pill is non-zero, the structural-skip BetPlacerSkipError path
when only the merged-alt accordion is on the page, and the Task 13
surface (find/click humanized, wager humanized-typed, shadow-mode
abort, odds/limit DOM probes).

The FakePage harness here extends the one in ``tests/_fakes.py`` with the
extra surface ``human.mouse.click`` needs (a ``mouse`` attr that records
move/down/up, and ``bounding_box`` on the elements we expect to click).
We don't import the existing FakePage because adding ``mouse`` to it
would propagate to every existing test; keep this localized.
"""
import re

import pytest

from _fakes import FakeElement, FakeLocator, FakeKeyboard
from bet_placer import BetPlacerError, BetPlacerSkipError, ShadowAbortError
from bet_placer_betmgm import BetmgmBetPlacer


AUDIT_DIR = "audit_logs/test_bet_placer_betmgm_humanized"


class _FakeMouse:
    """Records the moves/downs/ups produced by ``human.mouse.click``."""

    def __init__(self):
        self.moves: list[tuple[float, float]] = []
        self.events: list[str] = []

    def move(self, x, y, *, steps=None):
        self.moves.append((x, y))

    def down(self):
        self.events.append("down")

    def up(self):
        self.events.append("up")


class _ClickableElement(FakeElement):
    """FakeElement with a bounding_box — needed by ``human.mouse.click``.

    ``human.mouse.click`` does NOT call ``locator.click()``; it physically
    moves the mouse to the locator's bounding box and emits down/up
    events on ``page.mouse``. We track a "mouse-clicked" signal by
    counting calls to ``bounding_box`` (which ``move_to`` issues once
    per click) and by combining that with a post-call mouse-event
    inspection in tests."""

    def __init__(self, *, box=None, on_box_query=None, **kwargs):
        super().__init__(**kwargs)
        self._box = box or {"x": 100.0, "y": 100.0, "width": 80.0, "height": 30.0}
        self.bounding_box_calls = 0
        self.on_box_query = on_box_query

    def bounding_box(self):
        self.bounding_box_calls += 1
        if self.on_box_query:
            self.on_box_query()
        return self._box

    @property
    def mouse_clicked(self) -> bool:
        """True if ``human.mouse.click`` aimed at this element at least once."""
        return self.bounding_box_calls > 0


class _HumanizedFakePage:
    """Minimal page surface that supports both the legacy locator-driven
    lookup *and* the mouse-events ``human.mouse.click`` needs.

    Keep this isolated from the shared ``FakePage`` in ``_fakes.py`` so
    we don't disturb tests that rely on the older (mouse-less) shape.
    """

    def __init__(self, *, locators=None, text_locators=None, role_locators=None,
                 url=""):
        self.locators = locators or {}
        self.text_locators = text_locators or {}
        self.role_locators = role_locators or {}
        self.url = url
        self.waits: list[int] = []
        self.navigations: list[str] = []
        self.evaluations: list[str] = []
        self.viewport_sizes: list[dict] = []
        self.keyboard = FakeKeyboard()
        self.mouse = _FakeMouse()

    def locator(self, selector):
        return self.locators.get(selector, FakeLocator())

    def get_by_text(self, text, exact=False):
        if hasattr(text, "pattern"):
            return self.text_locators.get(text.pattern, FakeLocator())
        return self.text_locators.get(text, FakeLocator())

    def get_by_role(self, role, name=None):
        if name is not None and hasattr(name, "pattern"):
            key = (role, name.pattern)
        else:
            key = (role, name)
        return self.role_locators.get(key, FakeLocator())

    def wait_for_timeout(self, ms):
        self.waits.append(int(ms))

    def wait_for_selector(self, selector, **kwargs):
        return None

    def screenshot(self, *args, **kwargs):
        return None

    def goto(self, url, **kwargs):
        self.navigations.append(url)
        self.url = url

    def set_viewport_size(self, size):
        self.viewport_sizes.append(size)

    def evaluate(self, script):
        self.evaluations.append(script)
        return None


# ---------------------------------------------------------------------------
# Lazy slip-clear
# ---------------------------------------------------------------------------

_PILL_SELECTOR = (
    'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i'
)


def test_lazy_clear_skips_full_clear_when_pill_reads_zero():
    """Pill reads ``Bet slip (0)`` → return immediately, never click Clear All.

    The slip is already empty; opening it and sweeping remove icons is
    wasted work AND would race the modal watcher. The placer must
    short-circuit on the cheap pill read before any further interaction.
    """
    clear_all = _ClickableElement(visible=True)
    page = _HumanizedFakePage(locators={
        _PILL_SELECTOR: FakeLocator(
            [FakeElement(visible=True, text="Bet slip (0)")]
        ),
        # Plant a Clear All that WOULD click if the placer didn't short-circuit.
        'span:has-text("Clear All")': FakeLocator([clear_all]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer.clear_betslip()

    assert clear_all.mouse_clicked is False, (
        "Lazy short-circuit failed: Clear All was clicked even though pill==0"
    )
    # No mouse events should have fired — we never touched the slip.
    assert page.mouse.events == []


def test_lazy_clear_runs_full_dance_when_pill_reads_one():
    """Pill reads ``1 Bet slip`` → placer opens the slip and clicks Clear All.

    Distinct from the lazy short-circuit: a non-zero count is the
    signal that the slip has stale items from a prior run and must be
    swept before we add our own.
    """
    pill_locator = FakeLocator(
        [FakeElement(visible=True, text="1 Bet slip")]
    )
    # Track the "pill flipped" state via an on_box_query side effect on
    # Clear All — when the placer aims its humanized mouse at Clear All,
    # we flip the pill to "(0)" so the post-clear verification passes.
    def _flip_pill_to_zero():
        pill_locator.elements[0] = FakeElement(
            visible=True, text="Bet slip (0)"
        )
    clear_all = _ClickableElement(visible=True, on_box_query=_flip_pill_to_zero)
    slip_pill_button = _ClickableElement(visible=True)
    page = _HumanizedFakePage(locators={
        _PILL_SELECTOR: pill_locator,
        # Slip-open affordances — the placer probes several selectors;
        # the first visible match wins. Plant the 'pays out' div which is
        # the prod-default.
        'div:has-text("pays out")': FakeLocator([slip_pill_button]),
        # The Clear All span is what the placer clicks once the slip is open.
        'span:has-text("Clear All")': FakeLocator([clear_all]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer.clear_betslip()

    assert clear_all.mouse_clicked is True, (
        "Full-clear dance didn't run: Clear All was never aimed-at by "
        "human.mouse.click despite pill showing '1 Bet slip'"
    )
    # human.mouse.click emits down + up on page.mouse — at least one of each.
    assert "down" in page.mouse.events
    assert "up" in page.mouse.events
    assert page.mouse.events.index("down") < page.mouse.events.index("up")


# ---------------------------------------------------------------------------
# Structural-skip — std O/U missing but merged-alt sibling present
# ---------------------------------------------------------------------------

class _AccordionButton(_ClickableElement):
    """Accordion-toggle button that reports its label text."""

    def __init__(self, *, label, **kwargs):
        super().__init__(text=label, **kwargs)


def test_navigate_skips_with_skip_error_when_only_alt_visible(monkeypatch):
    """When the std ``Player rebounds + assists O/U`` accordion isn't on
    the page but the merged-alt ``Player rebounds + assists`` IS, the
    placer raises ``BetPlacerSkipError`` (LOGIC.md hard rule).

    This is the structural-skip path: BetMGM ships per-event variance,
    and a std×std opp at a lower-profile event may only carry the alt
    accordion. Falling back to it can't satisfy ``direction='under'``,
    so the worker classifies the task SKIPPED, not FAILED.
    """
    accordion_name = "Player rebounds + assists O/U"
    alt_sibling = "Player rebounds + assists"

    # Build the page so:
    # * the exact std selector returns no elements
    # * the dsaccordiontoggle scan returns one visible button — the alt sibling
    # * pill is empty (so clear_betslip short-circuits)
    alt_button = _AccordionButton(label=alt_sibling, visible=True)
    page = _HumanizedFakePage(locators={
        _PILL_SELECTOR: FakeLocator(
            [FakeElement(visible=True, text="Bet slip (0)")]
        ),
        # Std exact-text selector — empty (the std accordion isn't shipped).
        f'button[dsaccordiontoggle]:text-is("{accordion_name}")':
            FakeLocator([]),
        # Whole-page scan returns the alt sibling.
        'button[dsaccordiontoggle]': FakeLocator([alt_button]),
        # Authenticated probe — no Log in link.
        'a[href*="/login"]:has-text("Log in")': FakeLocator([]),
        # Search input.
        (
            'div.cdk-overlay-container input, '
            'input[placeholder*="Search"], '
            'input[placeholder*="Find"]'
        ): FakeLocator([_ClickableElement(visible=True)]),
        # Event-anchor scan — empty so the placer falls back to
        # click_through's direct goto (no event link clicked).
        'a[href*="/sports/events/"]': FakeLocator([]),
    })

    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    opp = {
        "home_team": "Knicks",
        "away_team": "Cavaliers",
        "sport_title": "NBA",
        "over_line": 5.5,
        "under_line": 5.5,
    }
    market_config = {
        "accordion_name": accordion_name,
        "is_alternate": False,
    }

    # Bypass the real event-navigation branch — we want to land directly
    # at the accordion-expand step. Patch URL after the homepage goto so
    # the "already on event page" branch fires and skips the search/nav.
    original_goto = page.goto

    def _goto_with_event_hint(url, **kwargs):
        original_goto(url, **kwargs)
        if "betfinder" in url:
            # Make the post-search URL look like an event page so the
            # placer's "already there" short-circuit fires.
            page.url = (
                "https://www.mo.betmgm.com/en/sports/events/"
                "cavaliers-knicks-12345"
            )
    monkeypatch.setattr(page, "goto", _goto_with_event_hint)

    with pytest.raises(BetPlacerSkipError, match="merged-alt accordion"):
        placer.navigate_and_expand_market(opp, market_config, direction="under")
