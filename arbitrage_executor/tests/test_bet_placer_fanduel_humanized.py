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
from bet_placer import BetPlacerError, ShadowAbortError
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
                 label_locators=None, url=""):
        self.locators = locators or {}
        self.text_locators = text_locators or {}
        self.role_locators = role_locators or {}
        self.label_locators = label_locators or {}
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

    def get_by_label(self, label):
        if label is not None and hasattr(label, "pattern"):
            key = label.pattern
        else:
            key = label
        return self.label_locators.get(key, FakeLocator())

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
    # Humanized cursor path was traced AND the final dispatch went via
    # locator.click(). PR #32 review fix — see human/mouse.py.
    assert len(page.mouse.moves) > 0, (
        "No humanized cursor movement recorded — human.mouse.click "
        "was not used to drive Clear All."
    )
    assert clear_all.clicked is True, (
        "Clear All's locator.click() was never invoked — the click never "
        "reached the React handler and the slip won't actually clear."
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


# ---------------------------------------------------------------------------
# enter_wager — humanized typing (Task 15)
# ---------------------------------------------------------------------------


def test_enter_wager_uses_humanized_type_fd():
    """``enter_wager`` must route the stake amount through
    ``humanized_type`` — i.e., one ``page.keyboard.type`` call per
    character of ``f"{amount:.2f}"``, not a single bulk ``.fill()`` or
    one-shot ``locator.type``.

    Mirrors the BetMGM enter_wager guarantee: per-character keydowns
    with lognormal-jittered spacing. The legacy FD path called
    ``wager_input.type(amount_str, delay=50)`` which Playwright
    executes as one batched action; humanized_type walks the string.
    """
    amount = 12.34
    amount_str = f"{amount:.2f}"  # "12.34" — 5 chars

    # Wager input — found via the FIRST selector in the durable cascade
    # (get_by_label("WAGER $")). ``humanized_type`` doesn't fall back
    # to fill() unless the locator looks React-controlled — a plain
    # input with no aria-controls / role=combobox / data-testid
    # containing "search" only emits per-char keyboard.type.
    wager_input = _ClickableElement(visible=True, input_value="")

    page = _HumanizedFakePage(
        # The wager input lives in label_locators because the placer's
        # most-durable lookup is page.get_by_label("WAGER $"). Plant it
        # there so the cascade picks it up on the first attempt and the
        # rest of the test reads cleanly.
        label_locators={
            "WAGER $": FakeLocator([wager_input]),
        },
    )
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    result = placer.enter_wager(amount)

    assert result is True
    # One keyboard.type call per character — proves humanized_type
    # walked the string instead of a single bulk fill/type.
    assert len(page.keyboard.types) == len(amount_str), (
        f"Expected {len(amount_str)} keyboard.type calls (one per char of "
        f"{amount_str!r}); got {len(page.keyboard.types)}: "
        f"{page.keyboard.types!r}"
    )
    # The full amount should be reconstructible from the per-char calls.
    # (humanized_type may inject typo-and-Backspace pairs on text >=6
    # chars; "12.34" is 5 chars so no typos can fire.)
    assert "".join(page.keyboard.types) == amount_str, (
        f"Per-char typing didn't reconstruct {amount_str!r}: "
        f"got {page.keyboard.types!r}"
    )
    # The wager input must have been humanized-mouse-clicked for focus
    # before typing started — humanized_type does NOT focus on its own.
    assert wager_input.mouse_clicked is True, (
        "enter_wager didn't humanized-mouse-click the wager input; the "
        "input would not be focused and the keystrokes would miss."
    )


def test_enter_wager_clear_step_scopes_to_input_not_page_keyboard():
    """The pre-type clear (``Ctrl+A`` then ``Delete``) MUST go through
    ``wager_input.press`` — the locator-scoped path — and NOT through
    ``page.keyboard.press``.

    ``settle()`` runs ``check_all_active()`` (modal sweep) BEFORE its
    sleep, and probing the DOM can drift focus off the input between
    the ``mouse_click`` and the clear-step. ``page.keyboard.press``
    fires on whatever currently has focus; if that's the page body
    instead of the input, Ctrl+A selects ALL of the bet-slip page
    instead of just the input contents, and the subsequent type lands
    in the wrong place — the symptom the user reported on 2026-05-25
    (wager mis-entered, MAX WAGER banner never appeared).

    ``locator.press()`` re-focuses the element first, which neutralises
    the drift. This test pins the contract.
    """
    wager_input = _ClickableElement(visible=True, input_value="")
    page = _HumanizedFakePage(
        label_locators={"WAGER $": FakeLocator([wager_input])},
    )
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer.enter_wager(12.34)

    # The clear keys must have been routed to the input, not the page
    # keyboard. Catching either one in page.keyboard.presses would mean
    # the regression came back.
    assert "Control+A" in wager_input.presses, (
        f"Ctrl+A clear didn't scope to wager_input; presses="
        f"{wager_input.presses!r}"
    )
    assert "Delete" in wager_input.presses, (
        f"Delete clear didn't scope to wager_input; presses="
        f"{wager_input.presses!r}"
    )
    assert "Control+A" not in page.keyboard.presses, (
        "Regression: Ctrl+A went through page.keyboard.press; that "
        "select-alls the entire bet-slip page if focus drifted. Use "
        "wager_input.press('Control+A') instead."
    )
    assert "Delete" not in page.keyboard.presses, (
        "Regression: Delete went through page.keyboard.press instead "
        "of wager_input.press."
    )


# ---------------------------------------------------------------------------
# place_bet — shadow-mode short-circuit (Task 15)
# ---------------------------------------------------------------------------


def test_place_bet_short_circuits_in_shadow_mode_fd(monkeypatch):
    """When ``BG_SHADOW_MODE=1``, FanDuel's ``place_bet`` must raise
    ``ShadowAbortError`` BEFORE the humanized click on the Place Bet
    button — no mouse-down on the button, no money committed.

    Mirrors the BetMGM contract: the pre-submit validations (slip
    ready, button visible) still run, but the short-circuit fires AT
    the click site. ``ShadowAbortError`` subclasses ``BetPlacerError``
    so the worker classifies it as SKIPPED, not FAILED.
    """
    monkeypatch.setenv("BG_SHADOW_MODE", "1")

    # The first place-button locator the placer checks is
    # ``page.get_by_role("button", name=re.compile(r"Place\\s*\\$[\\d.]+\\s*bet", re.I))``.
    # Plant a visible button under that exact pattern key so the
    # cascade finds it without falling through to the legacy
    # data-testid selectors.
    place_button = _ClickableElement(visible=True, text="Place $10.00 bet")
    page = _HumanizedFakePage(role_locators={
        ("button", r"Place\s*\$[\d.]+\s*bet"): FakeLocator([place_button]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    with pytest.raises(ShadowAbortError, match="shadow run"):
        placer.place_bet()

    # The button must NOT have been humanized-mouse-clicked — the abort
    # happens BEFORE the mouse_click call. If bounding_box were ever
    # queried on this element, the click would have fired and money
    # would have been committed in prod.
    assert place_button.mouse_clicked is False, (
        "Shadow-mode abort fired AFTER the humanized click — money "
        "would have been committed in a live run."
    )
    assert page.mouse.events == [], (
        f"Expected no mouse down/up events in shadow mode; got "
        f"{page.mouse.events!r}"
    )


# ---------------------------------------------------------------------------
# find_and_click_bet — humanized mouse path (Task 15)
# ---------------------------------------------------------------------------


def test_find_and_click_bet_uses_humanized_mouse_fd():
    """The alt-threshold bet click on FanDuel must go through
    ``human.mouse.click`` — i.e., aim at the bet element's bounding
    box and emit down/up events on ``page.mouse``, NOT call
    ``locator.click()`` directly.

    Exercises the alt branch. FD renders alt threshold markets as flat
    sibling pairs: the tile's own aria-label is often bare and the
    threshold ("2+ Total Bases") lives in the section HEADING. The alt
    path collects the player's tiles via ``[role=button][aria-label*=
    <player>]`` and pairs each with its heading text (modeled here by
    ``evaluate_result``), then select_unique matches the exact threshold.
    """
    player = "Jake Fraley"
    over_line = 1.5     # threshold == 2 (calculate_alternate_tab_value)
    market_key = "batter_total_bases"  # forces alt path

    base_display = "Total Bases"
    threshold = 2

    # New collection query: player-name-only; the threshold is read from
    # the section heading via ``el.evaluate(_FD_SECTION_HEADING_JS)``,
    # modeled by ``evaluate_result``.
    primary_selector = f'[role="button"][aria-label*="{player}"]'

    bet_button = _ClickableElement(
        visible=True,
        text=f"{threshold}+ {base_display}",
        # Bare-ish tile aria (carries the player name for the fuzzy match)
        # — the threshold is NOT here; it's in the heading below.
        attributes={"aria-label": f"{player}, +180"},
        # Section heading sibling — where parse_threshold/scoping read from.
        evaluate_result=f"{threshold}+ {base_display}",
    )

    page = _HumanizedFakePage(locators={
        primary_selector: FakeLocator([bet_button]),
        # ``_fanduel_slip_has_bet`` runs after the click to verify
        # the bet actually landed. The default impl returns True if no
        # empty marker is visible — leave both empty markers absent
        # (default FakeLocator() empty) so the verification passes.
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    opp = {
        "player_name": player,
        "over_line": over_line,
        "under_line": over_line,
        "market_key": market_key,
    }
    market_config = {
        "is_alternate": True,
        "display_names": [base_display],
    }

    result = placer.find_and_click_bet(opp, "over", market_config)

    assert result is True
    # Humanized mouse aimed at the bet element (read its bounding_box).
    assert bet_button.mouse_clicked is True, (
        "Humanized mouse never aimed at the bet button — "
        "find_and_click_bet either matched no element or used a "
        "non-humanized .click() path."
    )
    # Humanized cursor path was traced AND final dispatch went via
    # locator.click(). PR #32 review fix — raw mouse.down/up didn't
    # fire FD's React onClick.
    assert len(page.mouse.moves) > 0, (
        "No humanized cursor movement recorded — human.mouse.click "
        "was not used to drive this bet click."
    )
    assert bet_button.clicked is True, (
        "Bet button's locator.click() was never invoked — the click "
        "event never reached the React handler and the slip will stay empty."
    )


def test_alt_threshold_one_verb_phrase_selected_fd():
    """Threshold-1 alt markets (line 0.5) render on FanDuel as a verb phrase
    whose noun is SINGULAR — e.g. ``"To Hit A Double, Jake McCarthy, 4.90"`` —
    while the market display name is the PLURAL ``"Doubles"``. Querying tiles
    by the plural display name alone never matches the singular verb phrase
    (``"Doubles"`` is not a substring of ``"...A Double,..."``), so the bet is
    missed and select_unique raises NoPickError.

    Regression observed live 2026-05-29 (Jake McCarthy batter_doubles): the
    real button carried ``aria-label="To Hit A Double, Jake McCarthy, 4.90"``
    yet the deterministic alt path raised "No pick matched ... threshold=True".
    The collection query must add the exact verb phrase from
    FANDUEL_THRESHOLD_ONE_LABELS for threshold==1.
    """
    player = "Jake McCarthy"
    over_line = 0.5            # threshold == 1
    market_key = "batter_doubles_alternate"
    base_display = "Doubles"   # PLURAL display name; heading says "Double"

    # threshold==1 markets render as a verb phrase in the SECTION HEADING
    # ("To Hit A Double"); the plural display name "Doubles" is not a
    # substring of it, so the alt path adds the exact verb phrase
    # (FANDUEL_THRESHOLD_ONE_LABELS["Doubles"]) as a heading-scope term.
    verb_phrase = "To Hit A Double"
    primary_selector = f'[role="button"][aria-label*="{player}"]'

    bet_button = _ClickableElement(
        visible=True,
        text="4.90",
        attributes={"aria-label": f"{player}, 4.90"},
        evaluate_result=verb_phrase,   # the section heading
    )

    page = _HumanizedFakePage(locators={
        primary_selector: FakeLocator([bet_button]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    opp = {
        "player_name": player,
        "over_line": over_line,
        "under_line": over_line,
        "market_key": market_key,
    }
    market_config = {"is_alternate": True, "display_names": [base_display]}

    result = placer.find_and_click_bet(opp, "over", market_config)

    assert result is True
    assert bet_button.mouse_clicked is True, (
        "The To-Hit-A-Double tile was never matched — the alt query did not "
        "build the singular verb phrase for the threshold-1 market."
    )
    assert bet_button.clicked is True


# ---------------------------------------------------------------------------
# Combo alternates render as "Player - Alt <stat>" line+side rows (2026-05-30)
# ---------------------------------------------------------------------------


def test_alt_combo_matched_by_line_side_fd():
    """Combo alternates (Pts + Reb, PRA) render on FanDuel as a
    ``<Player> - Alt Pts + Reb`` section listing explicit Over/Under at
    multiple lines (5.5, 6.5, 7.5, ...) — NOT as ``N+`` threshold tiles.
    The over-leg must match by EXACT line+side, scoped to the "Alt <stat>"
    heading. (Luguentz Dort over 7.5 missed live 2026-05-30 because the code
    forced threshold matching.)"""
    player = "Luguentz Dort"
    sel = f'[role="button"][aria-label*="{player}"]'
    want = _ClickableElement(
        visible=True, text="2.24",
        attributes={"aria-label": f"{player} - Alt Pts + Reb, {player} Over, 7.5, 2.24"},
        evaluate_result=f"{player} - Alt Pts + Reb",
    )
    decoy = _ClickableElement(
        visible=True, text="1.85",
        attributes={"aria-label": f"{player} - Alt Pts + Reb, {player} Over, 6.5, 1.85"},
        evaluate_result=f"{player} - Alt Pts + Reb",
    )
    page = _HumanizedFakePage(locators={sel: FakeLocator([decoy, want])})
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    opp = {"player_name": player, "over_line": 7.5, "under_line": 7.5,
           "market_key": "player_points_rebounds_alternate"}
    mc = {"is_alternate": True, "display_names": ["Pts + Reb"]}

    assert placer.find_and_click_bet(opp, "over", mc) is True
    assert want.mouse_clicked and want.clicked, "exact 7.5 alt row not clicked"
    assert decoy.clicked is False, "wrong-line (6.5) alt row was clicked"


def test_alt_combo_scope_does_not_cross_into_longer_stat_fd():
    """Scoping must use heading ENDS-WITH, not loose substring: a 'Pts + Reb'
    opp must NOT match a 'Pts + Reb + Ast' section (that would place the wrong
    combo market). With only the +Ast section present, raise rather than
    misclick."""
    player = "Luguentz Dort"
    sel = f'[role="button"][aria-label*="{player}"]'
    pra = _ClickableElement(
        visible=True, text="2.24",
        attributes={"aria-label": f"{player} - Alt Pts + Reb + Ast, {player} Over, 7.5, 2.24"},
        evaluate_result=f"{player} - Alt Pts + Reb + Ast",
    )
    page = _HumanizedFakePage(locators={sel: FakeLocator([pra])})
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    opp = {"player_name": player, "over_line": 7.5, "under_line": 7.5,
           "market_key": "player_points_rebounds_alternate"}
    mc = {"is_alternate": True, "display_names": ["Pts + Reb"]}

    with pytest.raises(BetPlacerError):
        placer.find_and_click_bet(opp, "over", mc)
    assert pra.clicked is False, "clicked the wrong (+Ast) combo market"


# ---------------------------------------------------------------------------
# Orphan prevention — FD wager entry must VERIFY the value landed (2026-05-30)
# ---------------------------------------------------------------------------


def test_enter_wager_raises_when_value_never_registers_fd():
    """If the FanDuel WAGER field stays empty even after the fill() fallback,
    enter_wager must RAISE — never report a false success. In Phase 1 this
    aborts before the BetMGM leg is placed, turning the orphan that occurred
    live (Kevin McGonigle, 2026-05-30) into a benign skip."""
    class _NeverRegisters(_ClickableElement):
        def fill(self, value, **kwargs):
            super().fill(value, **kwargs)   # record the attempt...
            self._input_value = ""          # ...but FD drops it
        def input_value(self, **kwargs):
            return ""

    wager = _NeverRegisters(visible=True, input_value="")
    page = _HumanizedFakePage(label_locators={"WAGER $": FakeLocator([wager])})
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    with pytest.raises(BetPlacerError, match="did not register"):
        placer.enter_wager(0.18)


def test_enter_wager_succeeds_without_fill_when_value_reads_back_fd():
    """Happy path: when the field reads back the typed amount, enter_wager
    succeeds and does NOT invoke the fill() fallback."""
    class _RegistersOnType(_ClickableElement):
        def input_value(self, **kwargs):
            return "0.18"

    wager = _RegistersOnType(visible=True, input_value="0.18")
    page = _HumanizedFakePage(label_locators={"WAGER $": FakeLocator([wager])})
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    assert placer.enter_wager(0.18) is True
    assert wager.fills == [], "fill() fallback fired even though typing landed"
