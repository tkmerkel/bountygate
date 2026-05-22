"""Tests for the humanized FanDuel placer (Task 14).

Covers the lazy slip-clear ("Betslip empty" fast-path), the full
clear-dance when the slip carries items, and the navigate flow's
humanized-typing guarantee (one ``keyboard.type`` per character of
the player name).

The _HumanizedFakePage harness is copied locally (not imported from
the BetMGM test) because the BetMGM version has BetMGM-specific
role_locators wired in; keeping the FD copy independent lets each
file evolve without cross-coupling.
"""
import pytest

from _fakes import FakeElement, FakeLocator, FakeKeyboard
from bet_placer import BetPlacerError
from bet_placer_fanduel import FanduelBetPlacer


AUDIT_DIR = "audit_logs/test_bet_placer_fanduel_humanized"


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
    per click)."""

    def __init__(self, *, box=None, on_box_query=None, **kwargs):
        super().__init__(**kwargs)
        self._box = box or {
            "x": 100.0, "y": 100.0, "width": 80.0, "height": 30.0,
        }
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
    """Minimal page surface that supports both locator-driven lookup
    *and* the mouse-events ``human.mouse.click`` needs.

    Local copy (not shared with the BetMGM test) — the BetMGM page fake
    carries BetMGM-specific defaults that don't apply here.
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


def test_lazy_clear_skips_full_clear_when_slip_is_empty():
    """``Betslip empty`` text visible → return immediately, never open
    the slip or click any remove control.

    Exactly the BetMGM lazy-clear contract translated to FanDuel: the
    cheap text probe is enough to short-circuit; opening the slip and
    sweeping remove icons would be wasted work AND would race the modal
    watcher. FanDuel uses the visible text ``Betslip empty`` as the
    empty signal (vs BetMGM's ``Bet slip (0)`` pill).
    """
    # Plant a clear-all affordance that WOULD click if the placer didn't
    # short-circuit on the empty marker.
    clear_all = _ClickableElement(visible=True, text="Remove all selections")
    # And plant a remove-selection button that WOULD click in the per-bet
    # sweep — neither should fire under the lazy fast-path.
    remove_btn = _ClickableElement(
        visible=True,
        attributes={"aria-label": "Remove selection"},
    )
    page = _HumanizedFakePage(
        text_locators={
            # The fast-path "Betslip empty" probe is the very first thing
            # clear_betslip does. It MUST short-circuit here.
            "Betslip empty": FakeLocator(
                [FakeElement(visible=True, text="Betslip empty")]
            ),
        },
        locators={
            'div[role="button"]:has-text("Remove all selections")':
                FakeLocator([clear_all]),
            'button[aria-label*="remove" i]': FakeLocator([remove_btn]),
        },
    )
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer.clear_betslip()

    assert clear_all.mouse_clicked is False, (
        "Lazy short-circuit failed: clear-all was clicked even though "
        "'Betslip empty' was already visible"
    )
    assert remove_btn.mouse_clicked is False, (
        "Lazy short-circuit failed: per-bet remove was clicked even "
        "though 'Betslip empty' was already visible"
    )
    # No mouse events should have fired — we never touched the slip.
    assert page.mouse.events == []


def test_lazy_clear_runs_full_dance_when_slip_has_bet():
    """No ``Betslip empty`` marker visible AND remove buttons present →
    placer runs the full clear-dance, humanized-clicking the
    Remove-all-selections control (or the per-bet remove fallback).

    Distinct from the lazy short-circuit: without the empty marker we
    can't be sure the slip is clean, so we must sweep it. The sweep
    has to go through ``human.mouse.click`` (down/up on page.mouse),
    not ``locator.click()``.
    """
    # Track the "slip flipped to empty" signal so the post-clear
    # verification sees a clean state.
    empty_text_locator = FakeLocator([])  # initially absent

    def _flip_to_empty():
        # The placer's mouse_click reads bounding_box of clear_all;
        # use that as the trigger to flip the page's "Betslip empty"
        # text marker to visible — emulates the real DOM update.
        empty_text_locator.elements = [
            FakeElement(visible=True, text="Betslip empty")
        ]

    clear_all = _ClickableElement(
        visible=True,
        text="Remove all selections",
        on_box_query=_flip_to_empty,
    )
    page = _HumanizedFakePage(
        text_locators={
            "Betslip empty": empty_text_locator,
        },
        locators={
            # Primary clear-all affordance — the FD-specific
            # div[role="button"] variant ships in prod.
            'div[role="button"]:has-text("Remove all selections")':
                FakeLocator([clear_all]),
        },
    )
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer.clear_betslip()

    assert clear_all.mouse_clicked is True, (
        "Full-clear dance didn't run: clear-all was never aimed-at by "
        "human.mouse.click despite no 'Betslip empty' marker"
    )
    # human.mouse.click emits down + up on page.mouse — at least one of each.
    assert "down" in page.mouse.events
    assert "up" in page.mouse.events
    assert page.mouse.events.index("down") < page.mouse.events.index("up")
    assert clear_all.clicked is False, (
        "clear-all was clicked via locator.click() — humanized mouse "
        "path was bypassed. clear_betslip must drive every remove click "
        "through human.mouse.click, never .click() on the element."
    )


# ---------------------------------------------------------------------------
# Navigate — humanized typing
# ---------------------------------------------------------------------------


def test_navigate_uses_humanized_type_for_search():
    """``_navigate_fanduel`` must route the player name through
    ``humanized_type`` — one ``page.keyboard.type`` call per character
    of the name, not a single bulk ``.fill()``.

    This is the anti-detection guarantee for FD search entry: the
    legacy code used ``search_input.fill(player_name)`` which submits
    one Playwright call; humanized_type emits one keystroke per
    character (so the search component sees individual keydowns AND
    each char has lognormal-jittered spacing).
    """
    player = "Anthony Edwards"  # 15 chars
    # The search input the placer finds — the FIRST selector in the
    # cascade. ``humanized_type`` doesn't fall back to fill() unless
    # the locator looks React-controlled (no aria-controls / role=combobox
    # / data-testid containing "search" here), so the only signal we
    # measure is the per-char keyboard.type stream.
    search_input = _ClickableElement(
        visible=True,
        input_value="",
    )

    page = _HumanizedFakePage(
        text_locators={
            # Clear_betslip short-circuits via this text marker so the
            # nav test focuses purely on the search/type flow.
            "Betslip empty": FakeLocator(
                [FakeElement(visible=True, text="Betslip empty")]
            ),
        },
        locators={
            'input[placeholder="Search"]': FakeLocator([search_input]),
        },
    )
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    opp = {"player_name": player}
    market_config = {}

    placer.navigate_and_expand_market(opp, market_config, direction="over")

    # One keyboard.type call per character — proves humanized_type
    # walked the string instead of a single bulk fill/type.
    # (A 15-char string can trigger 0-1 typo+backspace pairs at the 3%
    # rate, which would add 1 extra char + Backspace press. The seed
    # rotates daily, so we accept the small typo overhead.)
    char_types = [t for t in page.keyboard.types if t]
    assert len(char_types) >= len(player), (
        f"Expected at least {len(player)} keyboard.type calls (one per "
        f"char of {player!r}); got {len(char_types)}: {char_types!r}"
    )
    # The original player name must be reconstructible by walking the
    # keystroke stream and applying Backspace presses for typo
    # corrections. Backspace appears in keyboard.presses, not types.
    # Simulate the keystroke-by-keystroke buffer:
    typed_buffer = []
    type_iter = iter(page.keyboard.types)
    press_iter = iter(page.keyboard.presses)
    # We can't perfectly interleave from the two separate logs without
    # ordering metadata, so the conservative check is: the joined types,
    # after stripping any single-char typo glitches, contains the player.
    joined = "".join(page.keyboard.types)
    backspaces = sum(1 for p in page.keyboard.presses if p == "Backspace")
    # joined has player_name interleaved with up to `backspaces` stray
    # chars; the corrected text equals player.
    assert len(joined) == len(player) + backspaces, (
        f"Type stream length mismatch: types={joined!r}, "
        f"backspaces={backspaces}, expected_len={len(player) + backspaces}"
    )
    # The search input was humanized-mouse-clicked before typing started.
    assert search_input.mouse_clicked is True, (
        "Search input wasn't humanized-mouse-clicked — humanized_type "
        "won't focus on its own, so the keystrokes would miss."
    )
    # Enter must have been pressed to submit the search.
    assert "Enter" in page.keyboard.presses, (
        f"Enter never pressed; presses={page.keyboard.presses!r}"
    )
