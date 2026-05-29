# Deterministic Bet Matching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace substring/confidence-ranked bet selection in the FanDuel and BetMGM placers with deterministic selection (exact line/side/threshold), confining fuzzy matching to player-row location.

**Architecture:** A new pure module `pick_matcher.py` is the single source of truth for turning a rendered pick's text into a `(side, line)` decision and selecting the *unique* pick matching a target — exactly one match or it raises. Each placer keeps its book-specific DOM traversal but routes the final line/side/threshold decision through `pick_matcher`. The legacy `SelectorFinder.find_candidates_by_text` + `SelectorCandidate` confidence model is deleted.

**Tech Stack:** Python 3.12, Playwright (sync), pytest, rapidfuzz (`text_match.fuzzy_contains`). All commands use the project venv interpreter `arbitrage_executor/.venv/Scripts/python.exe` (bare `python` lacks playwright).

**Working directory for all paths/commands:** `arbitrage_executor/` (the executor package root). Run tests from there.

**Reference:** Spec at `docs/superpowers/specs/2026-05-29-deterministic-bet-matching-design.md`.

**Note on errors:** `errors.py` is an unadopted skeleton with its *own* `BetPlacerError`. The exception actually caught upstream (`execute_arb.py`, `task_worker.py`) is **`bet_placer.BetPlacerError`**. New errors MUST subclass `bet_placer.BetPlacerError`, not the one in `errors.py`.

---

## File structure

- **Create** `arbitrage_executor/pick_matcher.py` — pure matching logic + `NoPickError`/`AmbiguousPickError`.
- **Create** `arbitrage_executor/tests/test_pick_matcher.py` — unit coverage of the bug classes.
- **Modify** `arbitrage_executor/bet_placer_betmgm.py` — `_click_betmgm_pick_for_player` selects via `pick_matcher`.
- **Modify** `arbitrage_executor/bet_placer_fanduel.py` — standard + alternate `find_and_click` paths select via `pick_matcher`; delete the wrong-side fallback.
- **Modify** `arbitrage_executor/selector_finder.py` — delete `find_candidates_by_text` and `SelectorCandidate`; keep the alternate-market utilities and `SelectorManager`.

---

## Task 1: `pick_matcher.py` — the deterministic matching module

**Files:**
- Create: `arbitrage_executor/pick_matcher.py`
- Test: `arbitrage_executor/tests/test_pick_matcher.py`

- [ ] **Step 1: Write the failing tests**

Create `arbitrage_executor/tests/test_pick_matcher.py`:

```python
import pytest

from pick_matcher import (
    Pick, parse_pick, parse_threshold, line_equals, select_unique,
    NoPickError, AmbiguousPickError,
)
from bet_placer import BetPlacerError


# ---- parse_pick: the core "1.5 vs 11.5" bug class ----
def test_parse_pick_betmgm_over_decimal():
    assert parse_pick("O 11.5  2.00") == Pick(side="over", line=11.5, odds=2.00)

def test_parse_pick_betmgm_under_decimal():
    assert parse_pick("U 3.5 1.85") == Pick(side="under", line=3.5, odds=1.85)

def test_parse_pick_full_word_sides_and_embedded():
    assert parse_pick("LeBron James, Over, 25.5, Points").side == "over"
    assert parse_pick("LeBron James, Over, 25.5, Points").line == 25.5
    assert parse_pick("Anthony Davis, Under, 9.5, Rebounds").side == "under"

def test_parse_pick_captures_full_line_token_not_substring():
    # The whole point: "O 1" must NOT read 11.5 as 1, and 1.5 != 11.5.
    assert parse_pick("O 11.5 2.00").line == 11.5
    assert parse_pick("O 1 2.00").line == 1.0
    assert parse_pick("Over 1.5 Points").line == 1.5

def test_parse_pick_integer_line():
    assert parse_pick("O 1 1.95") == Pick(side="over", line=1.0, odds=1.95)

def test_parse_pick_non_pick_returns_none():
    assert parse_pick("Show More") is None
    assert parse_pick("") is None
    assert parse_pick("Stephen Curry") is None  # no side+number


# ---- parse_threshold: the "5+ vs 15+" bug class ----
def test_parse_threshold_exact():
    assert parse_threshold("5+ Stolen Bases") == 5
    assert parse_threshold("15+ Points") == 15

def test_parse_threshold_does_not_confuse_5_with_15():
    # "5+" must not be found inside "15+".
    assert parse_threshold("15+ Points") == 15  # not 5

def test_parse_threshold_one_verb_labels():
    assert parse_threshold("To Hit A Single") == 1
    assert parse_threshold("To Record An RBI") == 1

def test_parse_threshold_none():
    assert parse_threshold("Over 4.5 Points") is None
    assert parse_threshold("") is None


# ---- line_equals ----
def test_line_equals():
    assert line_equals(11.5, 11.5)
    assert not line_equals(1.5, 11.5)
    assert line_equals(25.0, 25.0)


# ---- select_unique: exactly one or raise, no fallback ----
def _items(*texts):
    # (element, text) pairs; element is just the text for assertion convenience
    return [(t, t) for t in texts]

def test_select_unique_picks_exact_line_and_side():
    items = _items("O 11.5 2.00", "U 11.5 1.85", "O 1.5 1.50")
    assert select_unique(items, 11.5, "over") == "O 11.5 2.00"
    assert select_unique(items, 1.5, "over") == "O 1.5 1.50"

def test_select_unique_no_match_raises_nopick():
    items = _items("O 11.5 2.00", "U 11.5 1.85")
    with pytest.raises(NoPickError):
        select_unique(items, 2.5, "over")

def test_select_unique_no_wrong_side_fallback():
    # Only an under pick exists; asking for over must RAISE, never return it.
    items = _items("U 3.5 1.85")
    with pytest.raises(NoPickError):
        select_unique(items, 3.5, "over")

def test_select_unique_ambiguous_raises():
    items = _items("O 11.5 2.00", "O 11.5 2.01")
    with pytest.raises(AmbiguousPickError):
        select_unique(items, 11.5, "over")

def test_select_unique_threshold_mode():
    items = _items("5+ Stolen Bases", "15+ Stolen Bases", "To Hit A Single")
    # line 4.5 -> threshold 5
    assert select_unique(items, 4.5, "over", threshold=True) == "5+ Stolen Bases"
    # line 0.5 -> threshold 1 (verb label)
    assert select_unique(items, 0.5, "over", threshold=True) == "To Hit A Single"

def test_errors_are_betplacererror_subclasses():
    assert issubclass(NoPickError, BetPlacerError)
    assert issubclass(AmbiguousPickError, BetPlacerError)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_pick_matcher.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pick_matcher'`.

- [ ] **Step 3: Implement `pick_matcher.py`**

Create `arbitrage_executor/pick_matcher.py`:

```python
"""Deterministic pick matching for bet selection.

Player names are matched fuzzily (see ``text_match.fuzzy_contains``);
EVERYTHING else — side (Over/Under), line, and threshold — is matched EXACTLY
here. This module turns a rendered pick's text into a structured ``Pick`` and
selects the *unique* pick that matches a target. No substring matching, no
confidence ranking, no "first match" fallback: a target matches exactly one
rendered pick or we raise.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple, TypeVar

from bet_placer import BetPlacerError


class NoPickError(BetPlacerError):
    """No rendered pick matched the target line/side/threshold exactly."""


class AmbiguousPickError(BetPlacerError):
    """More than one rendered pick matched the target — refuse to guess."""


_LINE_TOL = 1e-6

# Side token (whole word) followed by the FULL numeric line token. Anchoring
# the number with \b on both sides is what prevents "1.5" from matching inside
# "11.5" and "O 1" from matching inside "O 11.5".
_PICK_RE = re.compile(
    r"\b(over|under|o|u)\b[\s,:]*([0-9]+(?:\.[0-9]+)?)\b",
    re.IGNORECASE,
)
# A standalone decimal price (>= 1.0) appearing after the line = odds.
_ODDS_RE = re.compile(r"\b([1-9][0-9]*\.[0-9]+)\b")
# "N+" threshold, exact integer (not preceded by another digit, so "5" is not
# matched inside "15+").
_THRESHOLD_RE = re.compile(r"(?<!\d)(\d+)\+")
# FanDuel MLB threshold-one labels render as verbs, not "1+ X".
_THRESHOLD_ONE_PHRASES = (
    "to hit a single", "to hit a double", "to hit a triple",
    "to record an rbi", "to record a walk", "to record a strikeout",
    "to hit a home run", "to record a stolen base", "to record a hit",
)


@dataclass(frozen=True)
class Pick:
    side: str               # "over" | "under"
    line: float
    odds: Optional[float] = None


def _norm(text: str) -> str:
    return " ".join((text or "").split())


def parse_pick(text: str) -> Optional[Pick]:
    """Parse a rendered pick's text into a ``Pick``, or ``None`` if it isn't a
    pick. Handles BetMGM "O 11.5  2.00" and FanDuel aria-labels that embed the
    side+line in a sentence. Captures the FULL numeric line token."""
    norm = _norm(text)
    if not norm:
        return None
    m = _PICK_RE.search(norm)
    if not m:
        return None
    token, num = m.group(1).lower(), m.group(2)
    side = "over" if token in ("o", "over") else "under"
    line = float(num)
    odds = None
    om = _ODDS_RE.search(norm[m.end():])
    if om:
        odds = float(om.group(1))
    return Pick(side=side, line=line, odds=odds)


def parse_threshold(text: str) -> Optional[int]:
    """Parse an exact 'N+' threshold (so '5+' != '15+'), or a FanDuel MLB
    threshold-one verb label (-> 1), or ``None``."""
    norm = _norm(text)
    if not norm:
        return None
    low = norm.lower()
    for phrase in _THRESHOLD_ONE_PHRASES:
        if phrase in low:
            return 1
    m = _THRESHOLD_RE.search(norm)
    return int(m.group(1)) if m else None


def line_equals(a: float, b: float) -> bool:
    """Exact numeric line equality (never substring)."""
    return abs(float(a) - float(b)) < _LINE_TOL


T = TypeVar("T")


def select_unique(
    items: Iterable[Tuple[T, str]],
    target_line: Optional[float],
    target_side: Optional[str],
    *,
    threshold: bool = False,
) -> T:
    """Return the single item whose text matches the target exactly.

    ``items``: iterable of ``(element, text)``. When ``threshold=True``, match
    ``parse_threshold(text) == int(target_line + 0.5)`` (and ``target_side`` is
    'over' or None); otherwise match ``parse_pick(text).side == target_side``
    and ``line_equals(parse_pick(text).line, target_line)``.

    Raises ``NoPickError`` on 0 matches, ``AmbiguousPickError`` on >1. No
    fallback.
    """
    matches: List[T] = []
    if threshold:
        want = int(float(target_line) + 0.5)
        for elem, text in items:
            if target_side is not None and target_side != "over":
                continue
            if parse_threshold(text) == want:
                matches.append(elem)
    else:
        for elem, text in items:
            p = parse_pick(text)
            if p is None:
                continue
            if target_side is not None and p.side != target_side:
                continue
            if target_line is not None and not line_equals(p.line, target_line):
                continue
            matches.append(elem)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise NoPickError(
            f"No pick matched target_line={target_line} "
            f"target_side={target_side} threshold={threshold}"
        )
    raise AmbiguousPickError(
        f"{len(matches)} picks matched target_line={target_line} "
        f"target_side={target_side} threshold={threshold} — refusing to guess"
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_pick_matcher.py -v`
Expected: PASS (all tests green).

- [ ] **Step 5: Commit**

```bash
git add arbitrage_executor/pick_matcher.py arbitrage_executor/tests/test_pick_matcher.py
git commit -m "feat(matching): deterministic pick_matcher (exact line/side/threshold)"
```

---

## Task 2: Wire BetMGM standard pick selection through `pick_matcher`

Replace the substring `target_text in norm` matching in `_click_betmgm_pick_for_player` with exact `parse_pick` + `select_unique`, scoped to the fuzzy-matched player row. Keep the scroll coax, the player-row resolution, and the `data-test-option-id` click. On no unique match, return `False` (so `find_and_click_bet`'s rich miss diagnostics run); let `AmbiguousPickError` propagate (fail loud).

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py` (`_click_betmgm_pick_for_player`, `:1124-1253`)
- Test: `arbitrage_executor/tests/test_bet_placer_betmgm.py`

- [ ] **Step 1: Add the import**

At the top of `bet_placer_betmgm.py`, with the other imports (near `from text_match import fuzzy_contains`, `:34`), add:

```python
from pick_matcher import parse_pick, select_unique, NoPickError
```

- [ ] **Step 2: Replace the body of `_click_betmgm_pick_for_player`**

Replace the matching loop and selection (`:1144-1253`, from `direction_letter = ...` through the final `return True`) with the version below. The scroll-coax block (`:1156-1164`) is preserved at the top.

```python
        # Best-effort: coax virtual-scroll / lazy-load picks into the DOM.
        try:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            settle(self.page, "micro_pause", rng=self._typing.rng)
            self.page.evaluate("window.scrollTo(0, 0)")
            settle(self.page, "micro_pause", rng=self._typing.rng)
        except Exception:
            pass

        all_picks = self.page.locator("ms-event-pick")
        pick_count = all_picks.count()
        print(f"[BETMGM] scanning {pick_count} ms-event-pick(s) for "
              f"{direction} {line} on player {player_name!r}")

        # Collect (pick, text) for picks that belong to the target player's
        # row. Player name is the ONLY fuzzy match; line/side is decided
        # deterministically by select_unique below.
        player_picks = []
        for i in range(pick_count):
            try:
                pick = all_picks.nth(i)
                txt = " ".join((pick.text_content() or "").split())

                row_player = self._player_name_for_pick(pick)
                if row_player is not None:
                    if not fuzzy_contains(row_player, player_name, threshold=90):
                        continue
                else:
                    ancestor_texts = pick.evaluate(
                        _WALKUP_JS, {"max_depth": 8, "max_text_len": 150}
                    )
                    row_texts = list(ancestor_texts or [])
                    row_texts.extend(self._nearby_row_texts_for_pick(pick))
                    if not any(fuzzy_contains(t, player_name, threshold=90)
                               for t in row_texts):
                        continue

                player_picks.append((pick, txt))
            except Exception as e:
                print(f"[BETMGM] pick #{i} scan error: {e}")
                continue

        try:
            matched = select_unique(player_picks, line, direction)
        except NoPickError:
            # Not found for this player — let find_and_click_bet emit the full
            # miss diagnostics (dump_miss_context + screenshots) and raise.
            print(f"[BETMGM] no unique pick for {player_name!r} {direction} "
                  f"{line}; parsed row picks: "
                  f"{[t for _, t in player_picks]}")
            return False
        # AmbiguousPickError intentionally propagates — refuse to guess.

        matched_option_id = matched.get_attribute("data-test-option-id")
        matched_text = " ".join((matched.text_content() or "").split())
        print(f"[BETMGM] matched bet: text={matched_text!r} "
              f"option_id={matched_option_id!r}")

        with with_screenshot_on_error(
            self, "click_failed", "Failed to click BetMGM bet"
        ):
            if matched_option_id:
                target = self.page.locator(
                    f'ms-event-pick[data-test-option-id="{matched_option_id}"]'
                )
                mouse_click(self.page, target.first, state=self._cursor,
                            rng=self._typing.rng)
            else:
                mouse_click(self.page, matched, state=self._cursor,
                            rng=self._typing.rng)
            settle(self.page, "slip_update", rng=self._typing.rng)
            self._screenshot("bet_clicked")
            print(f"[BETMGM] ✓ Bet added to slip")
            return True
```

- [ ] **Step 3: Add/adjust the regression test**

In `arbitrage_executor/tests/test_bet_placer_betmgm.py`, add a test that a row offering only `O 11.5` does NOT get selected for a target line of `1.5`, and that an exact line `11.5` over is selected. Use the existing fake-page pattern in that file (mirror an existing `_click_betmgm_pick_for_player` test; if none exists, drive `parse_pick`/`select_unique` against the rendered-text fixtures the file already builds). Concretely add:

```python
def test_betmgm_does_not_match_substring_line():
    from pick_matcher import select_unique, NoPickError
    # picks rendered on the player's row
    items = [("pickA", "O 11.5 2.00"), ("pickB", "U 11.5 1.85")]
    import pytest
    with pytest.raises(NoPickError):
        select_unique(items, 1.5, "over")          # 1.5 must NOT match 11.5
    assert select_unique(items, 11.5, "over") == "pickA"
```

- [ ] **Step 4: Run the BetMGM tests**

Run: `.venv/Scripts/python.exe -m pytest tests/test_bet_placer_betmgm.py tests/test_bet_placer_betmgm_humanized.py -v`
Expected: PASS (existing tests still green; new test green).

- [ ] **Step 5: Commit**

```bash
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "fix(betmgm): exact line/side pick selection via pick_matcher"
```

---

## Task 3: Wire FanDuel standard path through `pick_matcher`; delete the wrong-side fallback

Replace the `find_candidates_by_text` call + the "use first candidate" direction fallback in the standard branch of `find_and_click_bet` with deterministic enumeration + `select_unique`.

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py` (`find_and_click_bet` standard branch, `:560-628`)
- Test: `arbitrage_executor/tests/test_bet_placer_fanduel.py`

- [ ] **Step 1: Add the import**

At the top of `bet_placer_fanduel.py`, with the other imports, add:

```python
from pick_matcher import parse_pick, select_unique
```

(Keep `fuzzy_contains` imported — it is used for the player check below. If it is not already imported in this file, add `from text_match import fuzzy_contains`.)

- [ ] **Step 2: Replace the standard-branch body**

Replace `:560-628` (from the `# Standard path:` comment through `return True`) with:

```python
        # Standard path: enumerate the player's market tiles and select the
        # single tile matching the exact line + side. Player name is the only
        # fuzzy match; line/side is exact (no substring, no "first candidate").
        display_names = market_config.get('display_names', [market_key])
        tiles = []
        seen = set()
        for term in display_names:
            try:
                els = self.page.locator(
                    f'[aria-label*="{player_name}"][aria-label*="{term}"]'
                ).all()
            except Exception:
                continue
            for el in els:
                try:
                    if not el.is_visible():
                        continue
                    aria = el.get_attribute("aria-label") or ""
                    if not fuzzy_contains(aria, player_name, threshold=90):
                        continue
                    if aria in seen:
                        continue
                    seen.add(aria)
                    tiles.append((el, aria))
                except Exception:
                    continue

        with with_screenshot_on_error(self, "click_failed", "Failed to click bet"):
            try:
                locator = select_unique(tiles, line, direction)
            except BetPlacerError:
                dump_miss_context(self.page, site=self.site,
                                  player_name=player_name)
                self._screenshot("bet_not_found")
                raise  # NoPickError / AmbiguousPickError are BetPlacerError

            aria = locator.get_attribute("aria-label") or ""
            print(f"[FANDUEL] Clicking bet: {aria[:60]}")
            mouse_click(self.page, locator, state=self._cursor,
                        rng=self._typing.rng)
            settle(self.page, "slip_update", rng=self._typing.rng)

            # Slip-phase viewport pin (unchanged rationale: FD slip controls
            # misrender at narrower widths).
            print(f"[FANDUEL] Pinning viewport to 1920x945 for slip phase...")
            self.page.set_viewport_size({"width": 1920, "height": 945})
            settle(self.page, "micro_pause", rng=self._typing.rng)

            self._screenshot("bet_clicked")
            print(f"[FANDUEL] ✓ Bet added to slip")
            return True
```

Note: `select_unique` returns the Playwright element handle directly (the `(el, aria)` tuples carry the element as the first item), so no separate visibility re-scan is needed — only visible tiles were collected.

- [ ] **Step 3: Add a regression test**

In `arbitrage_executor/tests/test_bet_placer_fanduel.py`, add:

```python
def test_fanduel_no_wrong_side_fallback():
    # Only an Under tile exists for the player; an over request must RAISE,
    # never fall back to the under tile.
    from pick_matcher import select_unique, NoPickError
    import pytest
    tiles = [("under_el", "Anthony Davis, Under, 9.5, Rebounds")]
    with pytest.raises(NoPickError):
        select_unique(tiles, 9.5, "over")
    assert select_unique(tiles, 9.5, "under") == "under_el"

def test_fanduel_line_substring_not_matched():
    from pick_matcher import select_unique, NoPickError
    import pytest
    tiles = [("el", "Stephen Curry, Over, 11.5, Points")]
    with pytest.raises(NoPickError):
        select_unique(tiles, 1.5, "over")   # 1.5 must NOT match 11.5
```

- [ ] **Step 4: Run the FanDuel tests**

Run: `.venv/Scripts/python.exe -m pytest tests/test_bet_placer_fanduel.py tests/test_bet_placer_fanduel_humanized.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "fix(fanduel): exact line/side standard selection; remove wrong-side fallback"
```

---

## Task 4: Wire FanDuel alternate path through `pick_matcher`; remove the `find_candidates_by_text` fallback

The alternate branch (`_find_and_click_alternate_bet_fanduel`, `:630-855`) currently (a) tries a list of substring selectors (including `[aria-label*="{threshold}+"]`, where `"5+"` matches `"15+"`), takes the first visible, and (b) falls back to `find_candidates_by_text`. Replace selection with deterministic enumeration: collect the player's tiles for the market, then `select_unique` by **exact threshold** (over leg) or **exact line+side** (under leg). Preserve the toggle-handling + slip-verify click scaffolding, applied once to the single selected element.

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py` (`_find_and_click_alternate_bet_fanduel`, `:630-855`)

- [ ] **Step 1: Replace the function body after the header/threshold setup**

Keep the signature and the first lines that compute `threshold`, `display_names`, `base_display`, and the `print` (`:643-648`). Replace everything from the `selector_patterns` construction (`:650`) through the final `raise BetPlacerError(...)` (`:852-855`) with:

```python
        # Collect the player's candidate tiles for this market. We query by
        # player + market display name (button/role/aria variants), then decide
        # deterministically:
        #   - over leg  -> exact threshold ("5+" != "15+", or a verb label)
        #   - under leg -> exact line + side on the line-bearing O/U tile
        tiles = []
        seen = set()
        query_terms = display_names or [base_display]
        for term in query_terms:
            for pat in (
                f'button[aria-label*="{player_name}"][aria-label*="{term}"]',
                f'[role="button"][aria-label*="{player_name}"][aria-label*="{term}"]',
                f'[aria-label*="{player_name}"][aria-label*="{term}"]',
            ):
                try:
                    els = self.page.locator(pat).all()
                except Exception:
                    continue
                for el in els:
                    try:
                        if not el.is_visible():
                            continue
                        aria = el.get_attribute("aria-label") or ""
                        if not fuzzy_contains(aria, player_name, threshold=90):
                            continue
                        if aria in seen:
                            continue
                        seen.add(aria)
                        tiles.append((el, aria))
                    except Exception:
                        continue

        with with_screenshot_on_error(
            self, "alternate_click_failed", "Failed to click alternate bet"
        ):
            try:
                if direction == 'over':
                    # Threshold tile: select by exact threshold derived from line.
                    elem = select_unique(tiles, line, 'over', threshold=True)
                else:
                    # Under alternate: line-bearing O/U tile, exact line + side.
                    elem = select_unique(tiles, line, 'under')
            except BetPlacerError:
                dump_miss_context(self.page, site=self.site,
                                  player_name=player_name)
                self._screenshot("alternate_bet_not_found")
                raise

            # Capture pre-click state to detect a TOGGLE (FanDuel bet buttons
            # toggle selected/unselected; clicking a Selected one removes it).
            try:
                tag = elem.evaluate("e => e.tagName") or "?"
                aria_before = elem.get_attribute("aria-label") or ""
                role = elem.get_attribute("role") or ""
                was_selected = " Selected" in aria_before
                clicked_desc = (f"tag={tag} role={role!r} "
                                f"aria={aria_before[:80]!r} "
                                f"was_selected={was_selected}")
            except Exception:
                was_selected = False
                aria_before = ""
                clicked_desc = "<unknown>"

            mouse_click(self.page, elem, state=self._cursor,
                        rng=self._typing.rng)
            settle(self.page, "slip_update", rng=self._typing.rng)

            # If it started Selected, the click likely toggled it OFF —
            # re-locate by the exact aria-label and re-click to re-add.
            if was_selected and aria_before:
                try:
                    elem2 = self.page.locator(
                        f'[aria-label="{aria_before}"]'
                    ).first
                    if " Selected" not in (elem2.get_attribute("aria-label") or ""):
                        print(f"[FANDUEL] Click deselected an already-Selected "
                              f"bet; re-clicking to add.")
                        mouse_click(self.page, elem2, state=self._cursor,
                                    rng=self._typing.rng)
                        settle(self.page, "slip_update", rng=self._typing.rng)
                except Exception as e:
                    print(f"[FANDUEL] Re-locate after toggle failed: {e}")

            print(f"[FANDUEL] Expanding viewport to 1920x945...")
            self.page.set_viewport_size({"width": 1920, "height": 945})
            settle(self.page, "micro_pause", rng=self._typing.rng)
            self._screenshot("alternate_bet_clicked")

            # Verify the click actually added a bet.
            if not self._fanduel_slip_has_bet():
                print(f"[FANDUEL] ⚠ Slip still empty after click — clicked "
                      f"element was {clicked_desc}. Aborting.")
                self._screenshot("alternate_bet_did_not_add_to_slip")
                raise BetPlacerError(
                    f"FanDuel bet click did not add to slip "
                    f"(clicked={clicked_desc})"
                )

            print(f"[FANDUEL] ✓ Alternate bet added to slip")
            return True
```

This deletes the `FANDUEL_THRESHOLD_ONE_LABELS`-driven `selector_patterns` list, the per-pattern loop, and the `find_candidates_by_text` fallback. (`FANDUEL_THRESHOLD_ONE_LABELS` is now superseded by `parse_threshold`'s verb-phrase handling. Leave the constant defined if other code references it; otherwise it can be removed in Task 5's cleanup.)

- [ ] **Step 2: Run the FanDuel tests**

Run: `.venv/Scripts/python.exe -m pytest tests/test_bet_placer_fanduel.py tests/test_bet_placer_fanduel_humanized.py -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add arbitrage_executor/bet_placer_fanduel.py
git commit -m "fix(fanduel): exact threshold/line alternate selection; drop find_candidates_by_text fallback"
```

---

## Task 5: Delete `find_candidates_by_text` and `SelectorCandidate`

With no remaining callers (verified below), remove the guess-and-rank machinery from `selector_finder.py`. Keep `is_alternate_market`, `get_base_market_key`, `calculate_alternate_tab_value`, and `SelectorManager`.

**Files:**
- Modify: `arbitrage_executor/selector_finder.py`

- [ ] **Step 1: Verify there are no remaining callers**

Run: `.venv/Scripts/python.exe -c "import subprocess,sys; sys.exit(0)"` then search:

Run: `grep -rn "find_candidates_by_text\|SelectorCandidate" arbitrage_executor --include=*.py`
Expected: matches ONLY inside `selector_finder.py` (definitions) — no callers in `bet_placer_*.py`. If any caller remains, fix it before deleting (it should have been replaced in Tasks 3-4).

- [ ] **Step 2: Remove the code**

In `arbitrage_executor/selector_finder.py`:
- Delete the `SelectorCandidate` class (`:74-84`).
- Delete the entire `find_candidates_by_text` static method (`:90` through the end of Strategy 4, i.e. up to the next method `def ` or class boundary).
- Remove now-unused imports if they become dead (e.g. `fuzzy_contains` if no other function in the file uses it — check first with `grep -n fuzzy_contains arbitrage_executor/selector_finder.py`; keep it if `SelectorManager` or other retained code uses it).

- [ ] **Step 3: Verify the module still imports and placers still import cleanly**

Run: `.venv/Scripts/python.exe -c "import selector_finder, bet_placer_fanduel, bet_placer_betmgm, pick_matcher; print('imports OK')"`
Expected: `imports OK` (no ImportError / NameError).

- [ ] **Step 4: Run the full test suite**

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: PASS (all tests green).

- [ ] **Step 5: Commit**

```bash
git add arbitrage_executor/selector_finder.py
git commit -m "refactor(selectors): remove find_candidates_by_text + SelectorCandidate guess-and-rank"
```

---

## Task 6: End-to-end live verification (FanDuel + BetMGM)

No code changes — prove correct selection end-to-end on the live site at 1% stakes, per the spec's Definition of Done. Requires a fresh qualifying FanDuel↔BetMGM opportunity (see `scripts/watch_pipeline.py` for the qualifying count) and both books logged into the bot's Chrome profile.

**Files:** none.

- [ ] **Step 1: Confirm a qualifying opportunity exists**

Run (from `arbitrage_executor/`):
```bash
.venv/Scripts/python.exe -c "import opportunity; ops=opportunity.fetch_all_opportunities(testing_mode=True); print(sum(1 for o in ops if {str(o.get('over_bookmaker_key','')).lower(),str(o.get('under_bookmaker_key','')).lower()}=={'fanduel','betmgm'} and (o.get('roi') or 0)>=0.005))"
```
Expected: prints an integer ≥ 1. If 0, wait for the pipeline to produce one before proceeding.

- [ ] **Step 2: Run one real execution at 1% stakes**

Run (from `arbitrage_executor/`):
```bash
WAGER_SCALE_FACTOR=0.01 .venv/Scripts/python.exe execute_arb.py --max-attempts 1
```
Expected: stdout shows `✓ Execution complete`. If it instead raises `NoPickError`/`AmbiguousPickError` or `No bet found`, that is the new fail-loud behavior — capture the audit dir and investigate the selector (do NOT loosen matching to make it pass).

- [ ] **Step 3: Verify the CORRECT line and side were placed (not just that a bet placed)**

Open the newest `audit_logs/{timestamp}_{player}_{market}/` directory and inspect the FanDuel + BetMGM confirmation screenshots (`*place_bet_success*.png` / `bet_clicked*.png`). Confirm the **player, line, and side** on each slip match the opportunity in `opportunity_info.json` in the same directory.

- [ ] **Step 4: Confirm the executed row was recorded**

Run (from `arbitrage_executor/`):
```bash
.venv/Scripts/python.exe -c "from db_connection import fetch_data; print(fetch_data(\"SELECT player_name, market_key, line_value, roi, executed_at_utc FROM bg_executed_opportunities ORDER BY executed_at_utc DESC LIMIT 1\").to_string(index=False))"
```
Expected: the newest row matches the player/line just placed, timestamped within the last few minutes.

- [ ] **Step 5: Mark the spec's Definition of Done complete**

Confirm: `pick_matcher.py` + tests exist and pass; `find_candidates_by_text`/`SelectorCandidate` removed; both placers select via `pick_matcher` (exactly-one-or-raise; FanDuel wrong-side fallback gone); FD + BetMGM each placed a correct bet end-to-end. No commit (verification only).

---

## Self-review

**Spec coverage:**
- New `pick_matcher.py` with `parse_pick`/`parse_threshold`/`line_equals`/`select_unique` + errors → Task 1. ✅
- BetMGM exact selection → Task 2. ✅
- FanDuel standard exact selection + wrong-side fallback deleted → Task 3. ✅
- FanDuel alternate exact threshold/line selection + `find_candidates_by_text` fallback removed → Task 4. ✅
- Remove `find_candidates_by_text` + `SelectorCandidate` (keep alternate-market utils + `SelectorManager`) → Task 5. ✅
- Fail-loud diagnostics (parsed picks + `dump_miss_context` + screenshot, no guessing) → Tasks 2 (parsed-picks log), 3 & 4 (`dump_miss_context` + re-raise). ✅
- Unit tests for the bug classes (`1.5`/`11.5`, `O 1`/`O 11.5`, `5+`/`15+`, int/decimal, Over/Under/O/U, ambiguous/empty, non-pick) → Task 1. ✅
- End-to-end live verification + correct-line/side audit check → Task 6. ✅

**Placeholder scan:** No TBD/TODO; every code step has complete code; commands have expected output. ✅

**Type consistency:** `Pick(side, line, odds)`, `parse_pick`, `parse_threshold`, `line_equals`, `select_unique(items, target_line, target_side, *, threshold=False)`, `NoPickError`, `AmbiguousPickError` are used identically across Tasks 1-4. Errors subclass `bet_placer.BetPlacerError` (the caught base), consistent with the wiring tasks catching `BetPlacerError`. ✅

**Known related issue (out of scope, flagged for a later spec):** the BetMGM threshold-*tab* selector (`tab_selector_pattern: 'button:has-text("{threshold}+")'`) used during accordion navigation is substring-based (`"5+"` ⊂ `"15+"`). This spec scopes navigation/expansion out; harden the tab click in the executor-generalization or a dedicated follow-up.
