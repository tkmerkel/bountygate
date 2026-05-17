# Bet Placer Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the 2,217-line monolithic `BetPlacer` class into per-site classes (`FanduelBetPlacer`, `BetmgmBetPlacer`) behind a small ABC + factory, while adding the first real automated test coverage for orchestration logic. Behavior-preserving — no selector, timeout, log line, or screenshot tag changes.

**Architecture:** Abstract `BetPlacer` base in `bet_placer.py` with a `__new__` factory that dispatches by `site` string to concrete subclasses in `bet_placer_fanduel.py` and `bet_placer_betmgm.py`. One shared helper module (`_bet_placer_helpers.py`) holds `screenshot()`, `first_visible()`, and `_ACCORDION_FUZZY_THRESHOLD`. Each per-site class owns its private helpers (modal dismissal, slip clearing, etc.) — no further extraction.

**Tech Stack:** Python 3, sync Playwright, pytest (matches existing `arbitrage_executor/tests/` convention)

---

## Spec deviations

The spec ([`docs/superpowers/specs/2026-05-17-bet-placer-rewrite-design.md`](../specs/2026-05-17-bet-placer-rewrite-design.md)) was written before discovering existing test infrastructure. The plan adapts:

| Spec said | Plan does | Why |
|---|---|---|
| New `tests/bet_placer/` directory with unittest | Extend existing `arbitrage_executor/tests/` with pytest | A working `FakePage`/`FakeLocator`/`FakeElement` already lives in `arbitrage_executor/tests/test_bet_placer_sequencing.py` using pytest. Reuse, don't fork. |
| Build new `fake_page.py` (~400 lines) | Promote the existing fakes to `arbitrage_executor/tests/_fakes.py` (~150 lines, grown incrementally) | The existing fake is minimal and grows organically as tests need new surface. |
| Reference `toolkit/recorder/tests/test_roundtrip.py` (unittest) as the pattern | Reference `arbitrage_executor/tests/test_bet_placer_sequencing.py` (pytest) as the pattern | Closer match — same module, same fakes. |

All other architectural decisions in the spec stand.

## Pre-flight

### Task 0: Verify clean working state and run baseline tests

**Files:** none

- [ ] **Step 1: Confirm clean tree on `main`**

Run:
```bash
cd /c/Users/tkmer/bountygate
git status
git branch --show-current
```
Expected: working tree clean, branch is `main` (or your designated work branch — the rest of the plan assumes you create a feature branch).

- [ ] **Step 2: Create feature branch**

Run:
```bash
git checkout -b bet-placer-rewrite
```

- [ ] **Step 3: Run existing pytest baseline**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: All existing tests in `tests/test_bet_placer_sequencing.py` PASS. Record the count — this is your baseline. Every subsequent task must keep these green.

- [ ] **Step 4: Confirm Playwright import works**

Run:
```bash
python -c "from playwright.sync_api import Page, Locator; print('ok')"
```
Expected: prints `ok`. If this fails, fix the environment before continuing — every method-migration task depends on it.

---

## Section A — Foundations

### Task A1: Promote FakePage to a shared module

**Files:**
- Create: `arbitrage_executor/tests/_fakes.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_sequencing.py:1-74` (replace inline fake definitions with import)

- [ ] **Step 1: Create `arbitrage_executor/tests/_fakes.py`**

Copy the `FakeElement`, `FakeLocator`, and `FakePage` classes (lines 11–73) verbatim from `arbitrage_executor/tests/test_bet_placer_sequencing.py` into a new file:

```python
# arbitrage_executor/tests/_fakes.py
"""Playwright-surface test doubles used by bet_placer tests.

The fakes implement only the slice of the Playwright API that BetPlacer
calls. Grow this surface incrementally as new tests need new methods —
do NOT mock methods speculatively.
"""


class FakeElement:
    def __init__(self, *, visible=True, text="", on_click=None, attributes=None,
                 input_value="", on_fill=None, on_type=None, on_press=None):
        self.visible = visible
        self.text = text
        self.on_click = on_click
        self.clicked = False
        self.attributes = attributes or {}
        self._input_value = input_value
        self.fills = []
        self.types = []
        self.presses = []
        self.on_fill = on_fill
        self.on_type = on_type
        self.on_press = on_press

    def is_visible(self):
        return self.visible

    def click(self, *args, **kwargs):
        self.clicked = True
        if self.on_click:
            self.on_click()

    def text_content(self):
        return self.text

    def get_attribute(self, name):
        return self.attributes.get(name)

    def input_value(self):
        return self._input_value

    def fill(self, value):
        self.fills.append(value)
        self._input_value = value
        if self.on_fill:
            self.on_fill(value)

    def type(self, value, **kwargs):
        self.types.append(value)
        self._input_value += value
        if self.on_type:
            self.on_type(value)

    def press(self, key):
        self.presses.append(key)
        if self.on_press:
            self.on_press(key)

    def evaluate(self, *args, **kwargs):
        return ""

    def wait_for(self, **kwargs):
        return None


class FakeLocator:
    def __init__(self, elements=None):
        self.elements = list(elements or [])

    @property
    def first(self):
        return self.elements[0] if self.elements else FakeElement(visible=False)

    def count(self):
        return len(self.elements)

    def nth(self, index):
        if index < len(self.elements):
            return self.elements[index]
        return FakeElement(visible=False)

    def is_visible(self):
        return self.first.is_visible()

    def click(self, *args, **kwargs):
        return self.first.click(*args, **kwargs)

    def text_content(self):
        return self.first.text_content()

    def all(self):
        return list(self.elements)

    def get_attribute(self, name):
        return self.first.get_attribute(name)

    def evaluate(self, *args, **kwargs):
        return []

    def wait_for(self, **kwargs):
        return None


class FakeKeyboard:
    def __init__(self):
        self.presses = []
        self.types = []

    def press(self, key):
        self.presses.append(key)

    def type(self, value, **kwargs):
        self.types.append(value)


class FakePage:
    def __init__(self, *, locators=None, text_locators=None, role_locators=None,
                 label_locators=None, url=""):
        self.locators = locators or {}
        self.text_locators = text_locators or {}
        self.role_locators = role_locators or {}
        self.label_locators = label_locators or {}
        self.waits = []
        self.navigations = []
        self.viewport_sizes = []
        self.evaluations = []
        self.keyboard = FakeKeyboard()
        self.url = url

    def locator(self, selector):
        return self.locators.get(selector, FakeLocator())

    def get_by_text(self, text, exact=False):
        if hasattr(text, "pattern"):
            return self.text_locators.get(text.pattern, FakeLocator())
        return self.text_locators.get(text, FakeLocator())

    def get_by_role(self, role, name=None):
        key = (role, name.pattern if hasattr(name, "pattern") else name)
        return self.role_locators.get(key, FakeLocator())

    def get_by_label(self, label):
        key = label.pattern if hasattr(label, "pattern") else label
        return self.label_locators.get(key, FakeLocator())

    def wait_for_timeout(self, ms):
        self.waits.append(ms)

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
```

- [ ] **Step 2: Update `test_bet_placer_sequencing.py` to import from the new module**

Replace lines 11–73 of `arbitrage_executor/tests/test_bet_placer_sequencing.py` with:

```python
from tests._fakes import FakeElement, FakeLocator, FakePage  # noqa: F401
```

Keep all test functions (lines 76+) untouched.

- [ ] **Step 3: Run the existing tests to confirm the refactor preserved behavior**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: same number of tests pass as in Task 0 Step 3.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/tests/_fakes.py arbitrage_executor/tests/test_bet_placer_sequencing.py
git commit -m "test(bet_placer): promote FakePage to shared _fakes module"
```

---

### Task A2: Extract `_bet_placer_helpers.py`

**Files:**
- Create: `arbitrage_executor/_bet_placer_helpers.py`
- Create: `arbitrage_executor/tests/test_bet_placer_helpers.py`

- [ ] **Step 1: Create `arbitrage_executor/_bet_placer_helpers.py`**

```python
"""Shared helpers for bet_placer site implementations.

Kept intentionally tiny. The only reusable pattern in the legacy
bet_placer.py was the selector-cascade locator search; everything else
(modal dismissal, slip inspection, wager-entry quirks) is site-specific
and lives in the respective subclass.
"""

import os
from datetime import datetime
from typing import Iterable, Optional

from playwright.sync_api import Page, Locator

# Accordion-header fuzzy threshold. Lower than the player-name threshold (90)
# because UI labels can be reworded more than player names ("Player points O/U"
# -> "Player Points Over/Under") without changing semantics.
_ACCORDION_FUZZY_THRESHOLD = 80


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
```

- [ ] **Step 2: Write the failing tests**

Create `arbitrage_executor/tests/test_bet_placer_helpers.py`:

```python
"""Unit tests for _bet_placer_helpers."""
import os
import tempfile

import pytest

from tests._fakes import FakeElement, FakeLocator, FakePage
from _bet_placer_helpers import (
    _ACCORDION_FUZZY_THRESHOLD,
    screenshot,
    first_visible,
)


def test_accordion_threshold_constant():
    assert _ACCORDION_FUZZY_THRESHOLD == 80


def test_first_visible_returns_first_matching_visible_locator():
    page = FakePage(locators={
        "a": FakeLocator([]),                                  # zero count, skip
        "b": FakeLocator([FakeElement(visible=False)]),        # invisible, skip
        "c": FakeLocator([FakeElement(visible=True)]),         # MATCH
        "d": FakeLocator([FakeElement(visible=True)]),         # not reached
    })
    result = first_visible(page, ["a", "b", "c", "d"])
    assert result is not None
    assert result is page.locators["c"].first


def test_first_visible_returns_none_when_no_match():
    page = FakePage(locators={
        "a": FakeLocator([]),
        "b": FakeLocator([FakeElement(visible=False)]),
    })
    assert first_visible(page, ["a", "b"]) is None


def test_first_visible_skips_selectors_that_raise():
    class RaisingPage(FakePage):
        def locator(self, selector):
            if selector == "boom":
                raise RuntimeError("simulated playwright error")
            return super().locator(selector)

    page = RaisingPage(locators={
        "ok": FakeLocator([FakeElement(visible=True)]),
    })
    result = first_visible(page, ["boom", "ok"])
    assert result is page.locators["ok"].first


def test_first_visible_logs_when_label_provided(capsys):
    page = FakePage(locators={
        "x": FakeLocator([FakeElement(visible=True)]),
    })
    first_visible(page, ["x"], label="Found thing", site="fanduel")
    captured = capsys.readouterr()
    assert "[FANDUEL] Found thing via x" in captured.out


def test_screenshot_returns_expected_filename_format():
    page = FakePage()
    with tempfile.TemporaryDirectory() as tmp:
        path = screenshot(page, tmp, "fanduel", "search_results")
        assert path.startswith(os.path.join(tmp, "fanduel_search_results_"))
        assert path.endswith(".png")


def test_screenshot_does_not_raise_when_page_screenshot_fails(capsys):
    class FailingPage(FakePage):
        def screenshot(self, *args, **kwargs):
            raise RuntimeError("disk full")

    with tempfile.TemporaryDirectory() as tmp:
        path = screenshot(FailingPage(), tmp, "betmgm", "tag")
        assert "betmgm_tag_" in path
    captured = capsys.readouterr()
    assert "Screenshot failed" in captured.out
```

- [ ] **Step 3: Run the tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_helpers.py -v
```
Expected: all 7 tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/_bet_placer_helpers.py arbitrage_executor/tests/test_bet_placer_helpers.py
git commit -m "feat(bet_placer): add _bet_placer_helpers — screenshot, first_visible"
```

---

### Task A3: Add new-name method aliases to legacy `BetPlacer`

**Why first:** The rename (`get_actual_odds_fanduel` → `get_actual_odds`, etc.) needs to happen before the migration so callers can switch to the new names while the legacy class is still serving traffic.

**Files:**
- Modify: `arbitrage_executor/bet_placer.py` (add 3 wrapper methods on the existing `BetPlacer` class)

- [ ] **Step 1: Add wrappers to `BetPlacer`**

Append the following methods to the existing `BetPlacer` class in `arbitrage_executor/bet_placer.py` (place them just after the existing `_american_to_decimal` method, or anywhere inside the class — order doesn't matter):

```python
    # ---- New unified names (forward to site-suffixed methods) ----
    # Added 2026-05-17 as part of bet-placer-rewrite. After the per-site
    # split lands, these become the abstract methods and the suffixed
    # versions are deleted.
    def get_actual_odds(self):
        if self.site == "fanduel":
            return self.get_actual_odds_fanduel()
        if self.site == "betmgm":
            return self.get_actual_odds_betmgm()
        raise BetPlacerError(f"Unknown site: {self.site}")

    def discover_max_wager(self):
        if self.site == "fanduel":
            return self.discover_max_wager_fanduel()
        raise NotImplementedError(
            f"{self.site} does not support max-wager discovery"
        )

    def check_limit_alert(self):
        if self.site == "betmgm":
            return self.check_betmgm_limit_alert()
        raise NotImplementedError(
            f"{self.site} does not support limit-alert check"
        )
```

- [ ] **Step 2: Write tests covering the wrappers**

Append to `arbitrage_executor/tests/test_bet_placer_sequencing.py`:

```python
def test_get_actual_odds_dispatches_to_fanduel(monkeypatch):
    page = FakePage()
    placer = BetPlacer(page, "fanduel", AUDIT_DIR)
    called = []
    monkeypatch.setattr(placer, "get_actual_odds_fanduel",
                        lambda: called.append("fd") or 2.5)
    assert placer.get_actual_odds() == 2.5
    assert called == ["fd"]


def test_get_actual_odds_dispatches_to_betmgm(monkeypatch):
    page = FakePage()
    placer = BetPlacer(page, "betmgm", AUDIT_DIR)
    called = []
    monkeypatch.setattr(placer, "get_actual_odds_betmgm",
                        lambda: called.append("mgm") or 1.91)
    assert placer.get_actual_odds() == 1.91
    assert called == ["mgm"]


def test_discover_max_wager_raises_on_betmgm():
    page = FakePage()
    placer = BetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(NotImplementedError, match="max-wager discovery"):
        placer.discover_max_wager()


def test_check_limit_alert_raises_on_fanduel():
    page = FakePage()
    placer = BetPlacer(page, "fanduel", AUDIT_DIR)
    with pytest.raises(NotImplementedError, match="limit-alert check"):
        placer.check_limit_alert()
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: 4 new tests PASS, all previous tests still PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer.py arbitrage_executor/tests/test_bet_placer_sequencing.py
git commit -m "feat(bet_placer): add unified get_actual_odds / discover_max_wager / check_limit_alert wrappers"
```

---

### Task A4: Update `execute_arb.py` callers to new names

**Files:**
- Modify: `arbitrage_executor/execute_arb.py` (lines 318, 320, 387, 445)

- [ ] **Step 1: Update the four caller sites**

Apply these edits:

1. Line 318: `placer_fd.get_actual_odds_fanduel()` → `placer_fd.get_actual_odds()`
2. Line 320: `placer_fd.discover_max_wager_fanduel()` → `placer_fd.discover_max_wager()`
3. Line 387: `placer_mgm.get_actual_odds_betmgm()` → `placer_mgm.get_actual_odds()`
4. Line 445: `placer_mgm.check_betmgm_limit_alert()` → `placer_mgm.check_limit_alert()`

Verify with grep that no other call sites in the repo use the old names:

```bash
cd /c/Users/tkmer/bountygate
git grep -n "get_actual_odds_fanduel\|get_actual_odds_betmgm\|discover_max_wager_fanduel\|check_betmgm_limit_alert" -- "*.py"
```
Expected: only definitions in `arbitrage_executor/bet_placer.py` remain. No callers outside the class itself.

- [ ] **Step 2: Run all existing tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: same pass count.

- [ ] **Step 3: Static check — import-time validation of execute_arb**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -c "import execute_arb; print('import ok')"
```
Expected: prints `import ok` (no `AttributeError` from a stale rename).

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/execute_arb.py
git commit -m "refactor(execute_arb): switch BetPlacer callers to unified method names"
```

---

### Task A5: Introduce abstract `BetPlacer` ABC and rename legacy class

**Files:**
- Modify: `arbitrage_executor/bet_placer.py` (rename `BetPlacer` to `_LegacyBetPlacer`, add new `BetPlacer` ABC at the top of the class section)

- [ ] **Step 1: Rename the existing `BetPlacer` class**

In `arbitrage_executor/bet_placer.py`, find the line `class BetPlacer:` (around line 55) and rename it to:

```python
class _LegacyBetPlacer:
```

Leave the entire body of the class (every method) UNCHANGED.

- [ ] **Step 2: Insert the new ABC + factory above the legacy class**

Add this code immediately after the `class BetPlacerError(Exception):` block (around line 53), BEFORE `class _LegacyBetPlacer:`:

```python
from abc import ABC, abstractmethod
from typing import Dict, Tuple, Optional


class BetPlacer(ABC):
    """Abstract base for site-specific bet placers.

    Construct via ``BetPlacer(page, site, audit_dir)`` — the ``__new__``
    factory dispatches by site string to the concrete subclass.
    """

    def __new__(cls, page, site, audit_dir):
        # Factory dispatch: BetPlacer(page, "fanduel", ...) returns a
        # FanduelBetPlacer instance. Subclasses constructed directly
        # (FanduelBetPlacer(page, ...)) bypass this branch.
        #
        # The subclass imports happen INSIDE __new__ to avoid the
        # circular import that would otherwise hit at module-load time:
        # bet_placer_fanduel and bet_placer_betmgm both inherit from
        # BetPlacer defined here.
        #
        # Subclasses must NOT override __init__ to add new positional
        # arguments — Python's __new__/__init__ contract requires the
        # same call signature reach both.
        if cls is BetPlacer:
            # During migration: route to the legacy concrete class.
            # Subsequent tasks will swap each site to its dedicated
            # subclass.
            return object.__new__(_LegacyBetPlacer)
        return object.__new__(cls)

    def __init__(self, page, site, audit_dir):
        self.page = page
        self.site = site
        self.audit_dir = audit_dir
        os.makedirs(audit_dir, exist_ok=True)

    def _screenshot(self, tag: str) -> str:
        """Thin proxy to the helper, preserved as an instance method so
        per-method bodies migrate from the legacy class with zero changes
        to screenshot call sites."""
        from _bet_placer_helpers import screenshot
        return screenshot(self.page, self.audit_dir, self.site, tag)

    @abstractmethod
    def navigate_and_expand_market(self, opportunity: Dict, market_config: Dict, direction: str = None) -> None: ...
    @abstractmethod
    def clear_betslip(self) -> None: ...
    @abstractmethod
    def assert_betslip_has_bet(self) -> None: ...
    @abstractmethod
    def assert_betslip_empty(self) -> None: ...
    @abstractmethod
    def find_and_click_bet(self, opportunity: Dict, direction: str, market_config: Dict) -> bool: ...
    @abstractmethod
    def enter_wager(self, amount: float) -> bool: ...
    @abstractmethod
    def place_bet(self) -> Tuple[str, str]: ...
    @abstractmethod
    def get_actual_odds(self) -> Optional[float]: ...

    # Optional capabilities — base raises NotImplementedError
    def discover_max_wager(self) -> Tuple[float, str]:
        raise NotImplementedError(
            f"{self.site} does not support max-wager discovery"
        )

    def check_limit_alert(self) -> Tuple[bool, Optional[float]]:
        raise NotImplementedError(
            f"{self.site} does not support limit-alert check"
        )
```

**CRITICAL:** `_LegacyBetPlacer` does NOT inherit from `BetPlacer` (because `BetPlacer` is abstract and would forbid instantiation). The factory returns a `_LegacyBetPlacer` instance directly via `object.__new__(_LegacyBetPlacer)`. Keep `_LegacyBetPlacer` as a plain class.

- [ ] **Step 3: Run the test suite — verify factory still produces a working placer**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: all existing tests pass. The fakes-based tests call `BetPlacer(page, "fanduel", ...)` — that now goes through the new `__new__` factory and returns a `_LegacyBetPlacer`, which has every method the tests call.

- [ ] **Step 4: Smoke-import `execute_arb`**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -c "import execute_arb; print('import ok')"
```
Expected: `import ok`.

- [ ] **Step 5: Verify no caller uses `isinstance(x, BetPlacer)`**

`_LegacyBetPlacer` does NOT inherit from `BetPlacer` (it can't — the ABC would forbid instantiation). Any caller that does an `isinstance(p, BetPlacer)` check would see `False` for legacy-routed instances during the migration window, silently breaking control flow.

Run:
```bash
cd /c/Users/tkmer/bountygate
git grep -nE "isinstance\([^,]+,\s*BetPlacer\)" -- "*.py"
```
Expected: zero matches. If matches appear, halt — the plan needs an adjustment (probably making `_LegacyBetPlacer` a concrete subclass of `BetPlacer` that overrides all abstract methods with calls to its existing site-suffixed bodies).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer.py
git commit -m "refactor(bet_placer): introduce BetPlacer ABC + factory, rename concrete to _LegacyBetPlacer"
```

---

## Section B — FanDuel migration

Each Section B task:
1. Adds a method (or method group) to `FanduelBetPlacer` by copying the body verbatim from `_LegacyBetPlacer` at the line range specified.
2. Writes a pytest test that exercises that method via `FanduelBetPlacer` directly (not via the `BetPlacer` factory — that still routes to legacy).
3. Confirms tests pass.

The factory route switch from "fanduel" → `FanduelBetPlacer` happens at the END of Section B (Task B-FINAL), only when all FD methods are migrated and all tests pass. Between B1 and B-FINAL, `_LegacyBetPlacer` is still serving "fanduel" calls in production code paths.

### Task B1: Create `FanduelBetPlacer` skeleton with `FANDUEL_THRESHOLD_ONE_LABELS`

**Files:**
- Create: `arbitrage_executor/bet_placer_fanduel.py`

- [ ] **Step 1: Write the file**

```python
"""FanDuel-specific bet placement implementation."""

import os
import re
from datetime import datetime
from typing import Dict, Tuple, Optional

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from selector_finder import SelectorFinder, is_alternate_market, calculate_alternate_tab_value
from execution_logger import ExecutionLogger
from text_match import fuzzy_score, fuzzy_contains
from bet_placer import BetPlacer, BetPlacerError


# FanDuel MLB threshold=1 labels: maps display name -> (verb_phrase, article, singular_noun)
# e.g., "To Hit A Single, Jake Fraley, 2.55" instead of "1+ Singles"
FANDUEL_THRESHOLD_ONE_LABELS = {
    "Single": ("To Hit", "A", "Single"),
    "Singles": ("To Hit", "A", "Single"),
    "Double": ("To Hit", "A", "Double"),
    "Doubles": ("To Hit", "A", "Double"),
    "Triple": ("To Hit", "A", "Triple"),
    "Triples": ("To Hit", "A", "Triple"),
    "Home Run": ("To Hit", "A", "Home Run"),
    "Home Runs": ("To Hit", "A", "Home Run"),
    "Hit": ("To Record", "A", "Hit"),
    "Hits": ("To Record", "A", "Hit"),
    "RBI": ("To Record", "An", "RBI"),
    "RBIs": ("To Record", "An", "RBI"),
    "Run": ("To Record", "A", "Run"),
    "Runs": ("To Record", "A", "Run"),
    "Total Base": ("To Record", "A", "Total Base"),
    "Total Bases": ("To Record", "A", "Total Base"),
    "Stolen Base": ("To Record", "A", "Stolen Base"),
    "Stolen Bases": ("To Record", "A", "Stolen Base"),
    "Strikeout": ("To Record", "A", "Strikeout"),
    "Strikeouts": ("To Record", "A", "Strikeout"),
    "Walk": ("To Record", "A", "Walk"),
    "Walks": ("To Record", "A", "Walk"),
}


class FanduelBetPlacer(BetPlacer):
    """Handles bet placement on FanDuel."""

    # ---- Abstract methods (stubs until migrated) ----

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        raise NotImplementedError("migrated in Task B2")

    def clear_betslip(self):
        raise NotImplementedError("migrated in Task B3")

    def assert_betslip_has_bet(self):
        raise NotImplementedError("migrated in Task B4")

    def assert_betslip_empty(self):
        raise NotImplementedError("migrated in Task B4")

    def find_and_click_bet(self, opportunity, direction, market_config):
        raise NotImplementedError("migrated in Task B5")

    def enter_wager(self, amount):
        raise NotImplementedError("migrated in Task B6")

    def place_bet(self):
        raise NotImplementedError("migrated in Task B7")

    def get_actual_odds(self):
        raise NotImplementedError("migrated in Task B8")

    # discover_max_wager is FD-specific
    def discover_max_wager(self):
        raise NotImplementedError("migrated in Task B6")
```

- [ ] **Step 2: Verify it imports without raising**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -c "from bet_placer_fanduel import FanduelBetPlacer, FANDUEL_THRESHOLD_ONE_LABELS; print(len(FANDUEL_THRESHOLD_ONE_LABELS), 'labels')"
```
Expected: `22 labels`.

- [ ] **Step 3: Write a constants test**

Create `arbitrage_executor/tests/test_bet_placer_fanduel.py`:

```python
"""Tests for FanduelBetPlacer migrated methods."""
import pytest

from tests._fakes import FakeElement, FakeLocator, FakePage
from bet_placer_fanduel import FanduelBetPlacer, FANDUEL_THRESHOLD_ONE_LABELS

AUDIT_DIR = "audit_logs/test_bet_placer_fanduel"


def test_threshold_one_labels_has_22_entries():
    assert len(FANDUEL_THRESHOLD_ONE_LABELS) == 22


def test_threshold_one_labels_singular_and_plural_agree():
    # Spot-check: singular/plural forms map to the same triple
    assert FANDUEL_THRESHOLD_ONE_LABELS["Single"] == FANDUEL_THRESHOLD_ONE_LABELS["Singles"]
    assert FANDUEL_THRESHOLD_ONE_LABELS["RBI"] == FANDUEL_THRESHOLD_ONE_LABELS["RBIs"]
    assert FANDUEL_THRESHOLD_ONE_LABELS["Stolen Base"] == FANDUEL_THRESHOLD_ONE_LABELS["Stolen Bases"]


def test_rbi_uses_an_article():
    # The only entry with article "An" (not "A")
    _, article, _ = FANDUEL_THRESHOLD_ONE_LABELS["RBI"]
    assert article == "An"
```

- [ ] **Step 4: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_fanduel.py -v
```
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "feat(bet_placer): FanduelBetPlacer skeleton + threshold-one labels"
```

---

### Task B2: Migrate FanDuel navigation methods

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_fanduel.py`

**Source line ranges to copy from `arbitrage_executor/bet_placer.py`:**
- `_dismiss_fanduel_modal`: lines 1008–1040
- `_navigate_fanduel`: lines 142–201
- `navigate_and_expand_market` (FD branch only): adapt from lines 80–97

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_fanduel.py`, REPLACE the `navigate_and_expand_market` stub with:

```python
    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        """Navigate FanDuel to player search results.

        FanDuel ignores ``market_config`` and ``direction`` — its bet
        finder is name+line based, not accordion based like BetMGM.
        """
        self._navigate_fanduel(opportunity)
```

Add the following two private methods (paste their bodies verbatim from `arbitrage_executor/bet_placer.py` at the line ranges above; the method signatures are unchanged):

```python
    def _navigate_fanduel(self, opportunity):
        # COPY BODY FROM bet_placer.py:143-201 verbatim
        ...

    def _dismiss_fanduel_modal(self):
        # COPY BODY FROM bet_placer.py:1019-1040 verbatim
        ...
```

Make NO changes to the copied bodies — they will continue to call `self._clear_betslip_fanduel()` and `self._screenshot(...)` which are inherited from the ABC (`_screenshot`) or migrated in the next task (`_clear_betslip_fanduel`). The next task makes those calls work; for now, the test in step 2 only exercises `_dismiss_fanduel_modal`, which has no inter-method dependency.

- [ ] **Step 2: Write the failing tests**

Append to `arbitrage_executor/tests/test_bet_placer_fanduel.py`:

```python
def test_dismiss_modal_is_noop_when_no_modal():
    page = FakePage()  # no modal locator
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer._dismiss_fanduel_modal()  # must not raise

    assert page.waits == []  # never waited because never clicked


def test_dismiss_modal_clicks_first_button_when_visible(capsys):
    modal_button = FakeElement(visible=True)
    modal = FakeElement(visible=True, text="Reality Check — please confirm")
    page = FakePage(locators={
        'div[role="dialog"][aria-modal="true"]': FakeLocator([modal]),
        # The legacy implementation calls modal.first.locator("button") —
        # our FakeElement does NOT implement locator(); add a minimal hook
        # via the inner_text/text path. See _fakes.py if this needs extension.
    })

    # Skip — the modal-dismiss path requires Locator-chained .locator("button")
    # which the current FakePage does not model. Mark as TODO and rely on
    # live-smoke for this method.
    import pytest
    pytest.skip(
        "modal.first.locator('button') chain not modeled in FakePage; "
        "covered by live smoke test"
    )


def test_dismiss_modal_invisible_modal_is_noop():
    page = FakePage(locators={
        'div[role="dialog"][aria-modal="true"]': FakeLocator(
            [FakeElement(visible=False)]
        ),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer._dismiss_fanduel_modal()  # must not raise

    assert page.waits == []
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_fanduel.py -v
```
Expected: 4 pass, 1 skip.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "feat(bet_placer): migrate FanDuel navigate + modal dismiss to FanduelBetPlacer"
```

---

### Task B3: Migrate FanDuel slip-clearing

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_fanduel.py`

**Source line ranges:**
- `clear_betslip` (FD branch): adapt from lines 99–106
- `_clear_betslip_fanduel`: lines 1042–1168

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_fanduel.py`, REPLACE the `clear_betslip` stub with:

```python
    def clear_betslip(self):
        """Clear the FanDuel betslip and fail if it remains non-empty."""
        self._clear_betslip_fanduel()
```

Add the `_clear_betslip_fanduel` method by copying lines 1042–1168 from `arbitrage_executor/bet_placer.py` verbatim.

- [ ] **Step 2: Write the failing tests**

Append to `arbitrage_executor/tests/test_bet_placer_fanduel.py`:

```python
def test_clear_slip_fast_path_when_already_empty(capsys):
    page = FakePage(text_locators={
        "Betslip empty": FakeLocator([FakeElement(visible=True)]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer._clear_betslip_fanduel()

    captured = capsys.readouterr()
    assert "Slip already empty." in captured.out


def test_clear_slip_via_remove_all_button():
    clear_all = FakeElement(visible=True)
    page = FakePage(locators={
        'div[role="button"]:has-text("Remove all selections")':
            FakeLocator([clear_all]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer._clear_betslip_fanduel()

    assert clear_all.clicked


def test_clear_slip_post_clear_verification_raises_when_remove_button_remains():
    clear_all = FakeElement(visible=True)
    leftover_remove = FakeElement(visible=True)
    page = FakePage(locators={
        'div[role="button"]:has-text("Remove all selections")':
            FakeLocator([clear_all]),
        'button[aria-label*="remove" i]': FakeLocator([leftover_remove]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    with pytest.raises(BetPlacerError, match="FanDuel slip-clear failed"):
        placer._clear_betslip_fanduel()

    assert clear_all.clicked


# Import at top of file if not already there:
from bet_placer import BetPlacerError
```

(Adjust the existing imports at the top of `test_bet_placer_fanduel.py` to include `BetPlacerError` if not present.)

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_fanduel.py -v
```
Expected: all new tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "feat(bet_placer): migrate FanDuel slip-clearing to FanduelBetPlacer"
```

---

### Task B4: Migrate FanDuel slip-inspection methods

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_fanduel.py`

**Source line ranges:**
- `assert_betslip_has_bet` (FD branch): adapt from lines 108–125
- `assert_betslip_empty` (FD branch): adapt from lines 127–140
- `_fanduel_slip_has_bet`: lines 1288–1310
- `_fanduel_slip_is_empty`: lines 1312–1335

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_fanduel.py`, REPLACE the `assert_betslip_has_bet` and `assert_betslip_empty` stubs:

```python
    def assert_betslip_has_bet(self):
        """Assert a selected bet actually reached the slip."""
        if not self._fanduel_slip_has_bet():
            self._screenshot("validation_slip_empty")
            raise BetPlacerError("FanDuel slip is empty after bet click")

    def assert_betslip_empty(self):
        """Assert the slip is empty after cleanup."""
        if not self._fanduel_slip_is_empty():
            self._screenshot("validation_slip_not_empty")
            raise BetPlacerError("FanDuel slip still appears to contain a bet")
```

Add the two private methods by copying lines 1288–1335 from `arbitrage_executor/bet_placer.py` verbatim.

- [ ] **Step 2: Write the tests**

Append to `arbitrage_executor/tests/test_bet_placer_fanduel.py`:

```python
def test_slip_has_bet_returns_false_when_betslip_empty_marker_visible():
    page = FakePage(text_locators={
        "Betslip empty": FakeLocator([FakeElement(visible=True)]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    assert placer._fanduel_slip_has_bet() is False


def test_slip_has_bet_returns_true_when_no_empty_marker():
    # Conservative behavior: ambiguous state -> True
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    assert placer._fanduel_slip_has_bet() is True


def test_assert_betslip_has_bet_raises_when_empty():
    page = FakePage(text_locators={
        "Betslip empty": FakeLocator([FakeElement(visible=True)]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="FanDuel slip is empty"):
        placer.assert_betslip_has_bet()


def test_slip_is_empty_recognizes_no_bet_selections_marker():
    page = FakePage(text_locators={
        "No bet selections": FakeLocator([FakeElement(visible=True)]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    assert placer._fanduel_slip_is_empty() is True
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_fanduel.py -v
```
Expected: all new tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "feat(bet_placer): migrate FanDuel slip-inspection assertions"
```

---

### Task B5: Migrate FanDuel bet-finding/clicking

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_fanduel.py`

**Source line ranges:**
- `find_and_click_bet` (FD branch only): adapt from lines 557–708 (extract FD-specific control flow; the BetMGM branch stays in legacy/MGM class)
- `_find_and_click_alternate_bet_fanduel`: lines 710–861

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_fanduel.py`, REPLACE the `find_and_click_bet` stub with a FD-only version:

```python
    def find_and_click_bet(self, opportunity, direction, market_config):
        """Find and click the bet button for the specified player/line/direction."""
        player_name = opportunity['player_name']
        line = opportunity['over_line'] if direction == 'over' else opportunity['under_line']
        market_key = opportunity.get('over_market_key') if direction == 'over' else opportunity.get('under_market_key')
        if not market_key:
            market_key = opportunity['market_key']

        print(f"[FANDUEL] Finding bet: {player_name} {direction} {line}")

        # FanDuel MLB markets always use threshold format ("2+ Stolen Bases",
        # "To Hit A Single") even when the market_key doesn't have _alternate
        # suffix (primary table rows)
        is_alternate = (
            market_config.get('is_alternate', False)
            or is_alternate_market(market_key)
            or market_key.startswith("batter_")
            or market_key.startswith("pitcher_")
        )

        if is_alternate:
            return self._find_and_click_alternate_bet_fanduel(
                opportunity, direction, market_config, player_name, line
            )

        # Standard (non-alternate) path
        display_names = market_config.get('display_names', [market_key])
        candidates = SelectorFinder.find_candidates_by_text(
            self.page, display_names, player_name, line
        )

        if not candidates:
            # Diagnostic dump on miss (mirrors legacy)
            try:
                aria_loc = self.page.locator(f'[aria-label*="{player_name}"]')
                aria_dump = []
                for i in range(min(aria_loc.count(), 10)):
                    try:
                        aria_dump.append(aria_loc.nth(i).get_attribute("aria-label"))
                    except Exception:
                        continue
                print(f"[FANDUEL] aria-labels mentioning {player_name!r} "
                      f"({len(aria_dump)}): {aria_dump!r}")
            except Exception:
                pass
            self._screenshot("bet_not_found")
            raise BetPlacerError(f"No bet found for {player_name} {direction} {line}")

        # Filter by direction (legacy lines 660-671)
        direction_candidates = [
            c for c in candidates
            if (direction == 'over' and '[over]' in c.preview_text.lower()) or
               (direction == 'under' and '[under]' in c.preview_text.lower())
        ]
        selected = direction_candidates[0] if direction_candidates else candidates[0]

        print(f"[FANDUEL] Clicking bet: {selected.preview_text[:60]}")
        try:
            loc = self.page.locator(selected.selector)
            count = loc.count()
            locator = None
            for i in range(count):
                cand = loc.nth(i)
                try:
                    if cand.is_visible():
                        locator = cand
                        break
                except Exception:
                    continue
            if locator is None:
                raise BetPlacerError(
                    f"Selector matched {count} elements but none were visible: {selected.selector}"
                )
            locator.click(timeout=10000)
            self.page.wait_for_timeout(1500)

            print(f"[FANDUEL] Expanding viewport to 1920x945...")
            self.page.set_viewport_size({"width": 1920, "height": 945})
            self.page.wait_for_timeout(500)

            self._screenshot("bet_clicked")
            print(f"[FANDUEL] ✓ Bet added to slip")
            return True
        except Exception as e:
            self._screenshot("click_failed")
            raise BetPlacerError(f"Failed to click bet: {e}")
```

Add `_find_and_click_alternate_bet_fanduel` by copying lines 710–861 verbatim from `arbitrage_executor/bet_placer.py`.

- [ ] **Step 2: Write the tests**

Append to `arbitrage_executor/tests/test_bet_placer_fanduel.py`:

```python
def test_find_and_click_raises_when_no_candidates(monkeypatch):
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    # Stub SelectorFinder to return no candidates
    import bet_placer_fanduel
    monkeypatch.setattr(
        bet_placer_fanduel.SelectorFinder, "find_candidates_by_text",
        staticmethod(lambda *a, **k: [])
    )

    opp = {
        "player_name": "Jake Fraley",
        "over_line": 0.5, "under_line": 0.5,
        "market_key": "player_points",
    }
    with pytest.raises(BetPlacerError, match="No bet found for Jake Fraley over 0.5"):
        placer.find_and_click_bet(opp, "over", {"display_names": ["Points"]})


def test_find_and_click_alternate_threshold_one_path(monkeypatch):
    # Verify FD alternate selectors include the threshold-1 "To Hit A Single" form
    # for MLB hit-type markets.
    clicked = []

    class StubLocator:
        def __init__(self, found):
            self.found = found
        def count(self): return 1 if self.found else 0
        @property
        def first(self): return self
        def nth(self, i): return self
        def is_visible(self): return True
        def click(self, **kw): clicked.append(True)
        def evaluate(self, *a, **kw): return "BUTTON"
        def get_attribute(self, name): return ""

    captured_selectors = []
    class CapturingPage(FakePage):
        def locator(self, selector):
            captured_selectors.append(selector)
            # Match the threshold-1 selector pattern
            if "To Hit" in selector and "A Single" in selector and "Jake Fraley" in selector:
                return StubLocator(True)
            return StubLocator(False)

    page = CapturingPage(text_locators={
        # Make _fanduel_slip_has_bet return True after click (no "Betslip empty" marker)
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    result = placer._find_and_click_alternate_bet_fanduel(
        {"player_name": "Jake Fraley", "over_line": 0.5, "under_line": 0.5,
         "market_key": "batter_singles_alternate"},
        "over",
        {"display_names": ["Single"], "is_alternate": True},
        "Jake Fraley",
        0.5,
    )
    assert result is True
    # Verify a threshold-1 selector was tried
    assert any("To Hit" in s and "A Single" in s for s in captured_selectors)
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_fanduel.py -v
```
Expected: all new tests PASS. If the second test fails because `FakePage.locator` doesn't capture as designed, extend the test's `CapturingPage` rather than changing the production code.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "feat(bet_placer): migrate FanDuel find_and_click_bet + alternate path"
```

---

### Task B6: Migrate FanDuel wager entry and max-wager discovery

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_fanduel.py`

**Source line ranges:**
- `enter_wager` (FD branch): adapt from lines 863–883
- `_enter_wager_fanduel`: lines 1384–1592
- `discover_max_wager_fanduel`: lines 1770–1825 (rename to `discover_max_wager`)

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_fanduel.py`, REPLACE the `enter_wager` and `discover_max_wager` stubs:

```python
    def enter_wager(self, amount):
        """Enter wager amount in betslip."""
        print(f"[FANDUEL] Entering wager: ${amount:.2f}")
        return self._enter_wager_fanduel(amount)

    def discover_max_wager(self):
        """Enter a large amount to discover FanDuel's max wager limit.

        Returns:
            (max_wager_amount, raw_text)
        """
        # Body: copy from arbitrage_executor/bet_placer.py:1780-1825 verbatim.
        # The legacy method is named discover_max_wager_fanduel; only the
        # method name changes — the body is identical, including its call
        # to self._enter_wager_fanduel(99999.00).
```

Add `_enter_wager_fanduel` by copying lines 1384–1592 verbatim.

**Note on "copy verbatim":** for the large method bodies (50+ lines) this plan references line ranges in `arbitrage_executor/bet_placer.py` rather than inlining the full source. The engineer should `git show HEAD:arbitrage_executor/bet_placer.py | sed -n '1780,1825p'` (or open the file) and paste the body unchanged except for method-name renames where called out. The diff-audit gate in each task verification step catches accidental transformations.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_fanduel.py`:

```python
def test_discover_max_wager_returns_99999_when_no_alert(monkeypatch):
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    # Stub _enter_wager_fanduel to be a no-op (the wager-entry path is
    # tested separately)
    monkeypatch.setattr(placer, "_enter_wager_fanduel", lambda amount: True)

    amount, text = placer.discover_max_wager()

    assert amount == 99999.00
    assert "No limit" in text


def test_discover_max_wager_parses_dollar_amount(monkeypatch):
    max_wager_elem = FakeElement(visible=True, text="MAX WAGER $250.00")
    page = FakePage(text_locators={
        # text_locators is keyed by pattern string; the legacy code uses
        # re.compile(r"MAX\\s*WAGER", re.I) — its .pattern attribute is
        # "MAX\\s*WAGER".
        r"MAX\s*WAGER": FakeLocator([max_wager_elem]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    monkeypatch.setattr(placer, "_enter_wager_fanduel", lambda amount: True)

    amount, text = placer.discover_max_wager()

    assert amount == 250.00
    assert "MAX WAGER" in text


def test_enter_wager_diagnostic_dump_on_miss_raises():
    page = FakePage()  # no inputs anywhere
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Could not find FanDuel wager input"):
        placer._enter_wager_fanduel(10.00)
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_fanduel.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "feat(bet_placer): migrate FanDuel wager entry + max-wager discovery"
```

---

### Task B7: Migrate FanDuel place_bet

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_fanduel.py`

**Source line ranges:**
- `place_bet` (FD branch): adapt from lines 1827–1844
- `_place_bet_fanduel`: lines 1846–1986

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_fanduel.py`, REPLACE the `place_bet` stub:

```python
    def place_bet(self):
        """Click the Place Bet button and check for success/failure."""
        print(f"[FANDUEL] Placing bet...")
        return self._place_bet_fanduel()
```

Add `_place_bet_fanduel` by copying lines 1846–1986 verbatim.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_fanduel.py`:

```python
def test_place_bet_raises_when_button_not_found():
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Place Bet button not found"):
        placer._place_bet_fanduel()


def test_place_bet_returns_accepted_on_success_text(monkeypatch):
    # Build a page with the role-name match for "Place $X bet" and a
    # success text marker. Use role_locators keyed by (role, pattern_str).
    button = FakeElement(visible=True)
    success_marker = FakeElement(visible=True, text="Bet placed")
    page = FakePage(
        role_locators={
            ("button", r"Place\s*\$[\d.]+\s*bet"): FakeLocator([button]),
        },
        text_locators={
            "Bet placed": FakeLocator([success_marker]),
        },
    )
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    status, message = placer._place_bet_fanduel()

    assert status == "ACCEPTED"
    assert button.clicked


def test_place_bet_returns_unknown_when_no_signal(monkeypatch):
    button = FakeElement(visible=True)
    page = FakePage(
        role_locators={
            ("button", r"Place\s*\$[\d.]+\s*bet"): FakeLocator([button]),
        }
    )
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    status, message = placer._place_bet_fanduel()

    assert status == "UNKNOWN"
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_fanduel.py -v
```
Expected: all PASS. (If `_place_bet_fanduel` calls `get_by_role` differently than the test's `role_locators` keys, adjust the fake's `get_by_role` matching logic.)

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "feat(bet_placer): migrate FanDuel place_bet"
```

---

### Task B8: Migrate FanDuel get_actual_odds

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_fanduel.py`

**Source line ranges:**
- `get_actual_odds_fanduel`: lines 2064–2108 (rename to `get_actual_odds`)

- [ ] **Step 1: Add migrated method**

In `arbitrage_executor/bet_placer_fanduel.py`, REPLACE the `get_actual_odds` stub by copying lines 2073–2108 verbatim (the function body, with the method signature `def get_actual_odds(self) -> Optional[float]:` and the original docstring).

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_fanduel.py`:

```python
def test_get_actual_odds_parses_aria_label():
    odds_elem = FakeElement(
        visible=True,
        attributes={"aria-label": "Odds 2.94"},
        text="2.94",
    )
    page = FakePage(locators={
        '[aria-label^="Odds "]': FakeLocator([odds_elem]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    assert placer.get_actual_odds() == 2.94


def test_get_actual_odds_returns_none_when_not_found():
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    assert placer.get_actual_odds() is None
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_fanduel.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel.py
git commit -m "feat(bet_placer): migrate FanDuel get_actual_odds"
```

---

### Task B-FINAL: Switch factory to route "fanduel" → FanduelBetPlacer

**Files:**
- Modify: `arbitrage_executor/bet_placer.py` (`__new__` factory)

- [ ] **Step 1: Update `BetPlacer.__new__`**

In `arbitrage_executor/bet_placer.py`, find the factory block in `BetPlacer.__new__`:

```python
        if cls is BetPlacer:
            return object.__new__(_LegacyBetPlacer)
        return object.__new__(cls)
```

Replace with:

```python
        if cls is BetPlacer:
            if site == "fanduel":
                from bet_placer_fanduel import FanduelBetPlacer
                return object.__new__(FanduelBetPlacer)
            if site == "betmgm":
                # Not yet migrated — still served by legacy
                return object.__new__(_LegacyBetPlacer)
            raise BetPlacerError(f"Unknown site: {site}")
        return object.__new__(cls)
```

- [ ] **Step 2: Run the full test suite**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: all tests pass — including the original `test_bet_placer_sequencing.py` tests, which now exercise `FanduelBetPlacer` (via the factory) instead of `_LegacyBetPlacer`. This is the cross-check that the migration didn't drop a behavior the sequencing tests cared about.

- [ ] **Step 3: Live smoke against one FanDuel market**

Run the existing selector smoke test against one FD player:

```bash
cd /c/Users/tkmer/bountygate
python toolkit/selector_smoke_test.py --player "LeBron James"
```

Expected: FanDuel section runs without error and reports successful selector resolution. Compare audit screenshots in `arbitrage_executor/audit_logs/` against a recent pre-migration run (same player) — file count and tag sequence should match.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer.py
git commit -m "feat(bet_placer): route fanduel through FanduelBetPlacer"
```

---

## Section C — BetMGM migration

Same structure as Section B. Each task migrates a method (or method group), tests it via `BetmgmBetPlacer` directly, and commits. The factory route switch for "betmgm" happens at C-FINAL.

### Task C1: Create `BetmgmBetPlacer` skeleton

**Files:**
- Create: `arbitrage_executor/bet_placer_betmgm.py`
- Modify: `arbitrage_executor/tests/` (new test file)

- [ ] **Step 1: Write the file**

```python
"""BetMGM-specific bet placement implementation."""

import os
import re
from datetime import datetime
from typing import Dict, Tuple, Optional

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from selector_finder import SelectorFinder, is_alternate_market, calculate_alternate_tab_value
from execution_logger import ExecutionLogger
from text_match import fuzzy_score, fuzzy_contains
from bet_placer import BetPlacer, BetPlacerError
from _bet_placer_helpers import _ACCORDION_FUZZY_THRESHOLD


class BetmgmBetPlacer(BetPlacer):
    """Handles bet placement on BetMGM."""

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        raise NotImplementedError("migrated in Task C2")

    def clear_betslip(self):
        raise NotImplementedError("migrated in Task C3")

    def assert_betslip_has_bet(self):
        raise NotImplementedError("migrated in Task C4")

    def assert_betslip_empty(self):
        raise NotImplementedError("migrated in Task C4")

    def find_and_click_bet(self, opportunity, direction, market_config):
        raise NotImplementedError("migrated in Task C5")

    def enter_wager(self, amount):
        raise NotImplementedError("migrated in Task C6")

    def place_bet(self):
        raise NotImplementedError("migrated in Task C7")

    def get_actual_odds(self):
        raise NotImplementedError("migrated in Task C8")

    def check_limit_alert(self):
        raise NotImplementedError("migrated in Task C7")
```

- [ ] **Step 2: Create test file**

Create `arbitrage_executor/tests/test_bet_placer_betmgm.py`:

```python
"""Tests for BetmgmBetPlacer migrated methods."""
import pytest

from tests._fakes import FakeElement, FakeLocator, FakePage
from bet_placer import BetPlacerError
from bet_placer_betmgm import BetmgmBetPlacer

AUDIT_DIR = "audit_logs/test_bet_placer_betmgm"


def test_skeleton_imports_cleanly():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer.site == "betmgm"
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_betmgm.py -v
```
Expected: 1 test PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "feat(bet_placer): BetmgmBetPlacer skeleton"
```

---

### Task C2: Migrate BetMGM navigation methods

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_betmgm.py`

**Source line ranges:**
- `navigate_and_expand_market` (MGM branch): adapt from lines 80–97
- `_navigate_betmgm`: lines 203–474
- `_select_market_sub_tab_betmgm`: lines 476–506
- `_select_alternate_tab_betmgm`: lines 508–555

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_betmgm.py`, REPLACE the `navigate_and_expand_market` stub:

```python
    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        """Navigate BetMGM to event and expand the market accordion."""
        self._navigate_betmgm(opportunity, market_config, direction)
```

Add `_navigate_betmgm`, `_select_market_sub_tab_betmgm`, and `_select_alternate_tab_betmgm` by copying their bodies verbatim from `arbitrage_executor/bet_placer.py` at the line ranges above.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_betmgm.py`:

```python
def test_select_market_sub_tab_noop_when_no_label():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    placer._select_market_sub_tab_betmgm({})  # no sub_tab_label
    assert page.waits == []


def test_select_market_sub_tab_clicks_first_matching_selector():
    sub_tab = FakeElement(visible=True)
    page = FakePage(locators={
        'div[role="tablist"] button:has-text("Combo stats")':
            FakeLocator([sub_tab]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer._select_market_sub_tab_betmgm({"sub_tab_label": "Combo stats"})

    assert sub_tab.clicked


def test_select_market_sub_tab_raises_when_not_found():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Could not find BetMGM sub-tab"):
        placer._select_market_sub_tab_betmgm({"sub_tab_label": "Missing"})
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_betmgm.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "feat(bet_placer): migrate BetMGM navigation + sub-tab + alternate-tab"
```

---

### Task C3: Migrate BetMGM slip-clearing and slip-opening

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_betmgm.py`

**Source line ranges:**
- `clear_betslip` (MGM branch): adapt from lines 99–106
- `_clear_betslip_betmgm_precheck`: lines 1170–1286
- `_open_betmgm_slip`: lines 1719–1768

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_betmgm.py`, REPLACE the `clear_betslip` stub:

```python
    def clear_betslip(self):
        """Empty the BetMGM betslip; fail if it remains non-empty."""
        self._clear_betslip_betmgm_precheck()
```

Add `_clear_betslip_betmgm_precheck` (lines 1170–1286) and `_open_betmgm_slip` (lines 1719–1768) by copying verbatim.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_betmgm.py`:

```python
def test_clear_slip_fast_path_when_pill_shows_zero(capsys):
    page = FakePage(locators={
        'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i':
            FakeLocator([FakeElement(visible=True, text="Bet slip (0)")]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer._clear_betslip_betmgm_precheck()

    captured = capsys.readouterr()
    assert "Slip already empty" in captured.out


def test_clear_slip_post_clear_verification_raises_when_pill_still_nonzero():
    clear_all = FakeElement(visible=True)
    page = FakePage(locators={
        'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i':
            FakeLocator([FakeElement(visible=True, text="1 Bet slip")]),
        'span:has-text("Clear All")': FakeLocator([clear_all]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    with pytest.raises(BetPlacerError, match="BetMGM slip-clear failed"):
        placer._clear_betslip_betmgm_precheck()

    assert clear_all.clicked


def test_open_slip_noop_when_stake_input_already_visible():
    stake_input = FakeElement(visible=True)
    page = FakePage(locators={
        'app-stake-input input': FakeLocator([stake_input]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer._open_betmgm_slip()

    # Should have returned early without clicking the pill
    assert page.waits == []
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_betmgm.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "feat(bet_placer): migrate BetMGM slip-clearing + slip-opening"
```

---

### Task C4: Migrate BetMGM slip-inspection methods

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_betmgm.py`

**Source line ranges:**
- `assert_betslip_has_bet` (MGM branch): adapt from lines 119–125
- `assert_betslip_empty` (MGM branch): adapt from lines 134–140
- `_betmgm_slip_has_bet`: lines 1337–1382

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_betmgm.py`, REPLACE the assertion stubs:

```python
    def assert_betslip_has_bet(self):
        """Assert a selected bet actually reached the slip."""
        self._open_betmgm_slip()
        if not self._betmgm_slip_has_bet():
            self._screenshot("validation_slip_empty")
            raise BetPlacerError("BetMGM slip is empty after bet click")

    def assert_betslip_empty(self):
        """Assert the slip is empty after cleanup."""
        self._open_betmgm_slip()
        if self._betmgm_slip_has_bet():
            self._screenshot("validation_slip_not_empty")
            raise BetPlacerError("BetMGM slip still appears to contain a bet")
```

Add `_betmgm_slip_has_bet` by copying lines 1337–1382 verbatim.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_betmgm.py`:

```python
def test_slip_has_bet_false_when_empty_marker_visible():
    page = FakePage(text_locators={
        "No bet selections": FakeLocator([FakeElement(visible=True)]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer._betmgm_slip_has_bet() is False


def test_slip_has_bet_true_when_pill_shows_count():
    page = FakePage(locators={
        'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i':
            FakeLocator([FakeElement(visible=True, text="Bet slip (2)")]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer._betmgm_slip_has_bet() is True


def test_assert_betslip_has_bet_raises_when_empty():
    page = FakePage(text_locators={
        "No bet selections": FakeLocator([FakeElement(visible=True)]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="BetMGM slip is empty"):
        placer.assert_betslip_has_bet()
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_betmgm.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "feat(bet_placer): migrate BetMGM slip-inspection assertions"
```

---

### Task C5: Migrate BetMGM find_and_click_bet (and `_click_betmgm_pick_for_player`)

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_betmgm.py`

**Source line ranges:**
- `find_and_click_bet` (MGM branch): adapt from lines 557–708 (extract the MGM-specific control flow)
- `_click_betmgm_pick_for_player`: lines 885–1006

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_betmgm.py`, REPLACE the `find_and_click_bet` stub with a BetMGM-only version:

```python
    def find_and_click_bet(self, opportunity, direction, market_config):
        """Find and click the bet for the specified player/line/direction."""
        player_name = opportunity['player_name']
        line = opportunity['over_line'] if direction == 'over' else opportunity['under_line']

        print(f"[BETMGM] Finding bet: {player_name} {direction} {line}")

        if self._click_betmgm_pick_for_player(player_name, line, direction):
            # Expand viewport for betslip interaction (legacy lines 698-701)
            print(f"[BETMGM] Expanding viewport to 1920x945...")
            self.page.set_viewport_size({"width": 1920, "height": 945})
            self.page.wait_for_timeout(500)
            return True

        # Miss-path diagnostic + raise (mirrors legacy lines 609-657)
        try:
            aria_loc = self.page.locator(f'[aria-label*="{player_name}"]')
            aria_dump = []
            for i in range(min(aria_loc.count(), 10)):
                try:
                    aria_dump.append(aria_loc.nth(i).get_attribute("aria-label"))
                except Exception:
                    continue
            print(f"[BETMGM] aria-labels mentioning {player_name!r} "
                  f"({len(aria_dump)}): {aria_dump!r}")
        except Exception:
            pass
        try:
            pick_loc = self.page.locator("ms-event-pick")
            pick_count = pick_loc.count()
            pick_dump = []
            for i in range(min(pick_count, 20)):
                try:
                    txt = (pick_loc.nth(i).text_content() or "").strip()[:80]
                    if player_name.lower() in txt.lower():
                        pick_dump.append(txt)
                except Exception:
                    continue
            print(f"[BETMGM] ms-event-pick elements mentioning "
                  f"{player_name!r} ({len(pick_dump)}): {pick_dump!r}")
        except Exception:
            pass
        self._screenshot("bet_not_found")
        raise BetPlacerError(f"No bet found for {player_name} {direction} {line}")
```

Add `_click_betmgm_pick_for_player` by copying lines 885–1006 verbatim.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_betmgm.py`:

```python
def test_find_and_click_raises_when_no_pick_matches():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    opp = {"player_name": "LeBron James", "over_line": 25.5, "under_line": 25.5}
    with pytest.raises(BetPlacerError,
                       match="No bet found for LeBron James over 25.5"):
        placer.find_and_click_bet(opp, "over", {})
```

(Most of `_click_betmgm_pick_for_player`'s logic depends on `Locator.evaluate` for ancestor traversal — the existing `FakeLocator.evaluate` returns `[]`, which means the player-name match always fails. That's enough to exercise the no-match path. Deeper testing of the walkup logic stays in the live smoke test.)

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_betmgm.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "feat(bet_placer): migrate BetMGM find_and_click_bet"
```

---

### Task C6: Migrate BetMGM wager entry

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_betmgm.py`

**Source line ranges:**
- `enter_wager` (MGM branch): adapt from lines 863–883
- `_enter_wager_betmgm`: lines 1594–1717

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_betmgm.py`, REPLACE the `enter_wager` stub:

```python
    def enter_wager(self, amount):
        """Enter wager amount in the betslip."""
        print(f"[BETMGM] Entering wager: ${amount:.2f}")
        return self._enter_wager_betmgm(amount)
```

Add `_enter_wager_betmgm` by copying lines 1594–1717 verbatim.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_betmgm.py`:

```python
def test_enter_wager_raises_when_input_not_found():
    page = FakePage()  # no stake inputs anywhere
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Could not find BetMGM wager input"):
        placer._enter_wager_betmgm(10.00)


def test_enter_wager_picks_last_empty_when_multiple_inputs(monkeypatch):
    """Legacy invariant: when slip-clear has failed and multiple bets
    accumulate, _enter_wager_betmgm must pick the LAST empty stake input
    (the just-added bet), not the first or last filled."""
    filled_a = FakeElement(visible=True, input_value="5.00")
    empty_b = FakeElement(visible=True, input_value="")
    empty_c = FakeElement(visible=True, input_value="")  # LAST empty — should be picked
    filled_d = FakeElement(visible=True, input_value="3.00")
    page = FakePage(locators={
        'app-stake-input input': FakeLocator([filled_a, empty_b, empty_c, filled_d]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer._enter_wager_betmgm(10.00)

    # The last empty (empty_c) should have been typed into
    assert empty_c.clicked
    assert not empty_b.clicked
    assert not filled_a.clicked
    assert not filled_d.clicked
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_betmgm.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "feat(bet_placer): migrate BetMGM wager entry — last-empty-input invariant"
```

---

### Task C7: Migrate BetMGM place_bet and check_limit_alert

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_betmgm.py`

**Source line ranges:**
- `place_bet` (MGM branch): adapt from lines 1827–1844
- `_place_bet_betmgm`: lines 1988–2037
- `_close_betslip_betmgm`: lines 2039–2062
- `check_betmgm_limit_alert`: lines 2149–2203 (rename to `check_limit_alert`)

- [ ] **Step 1: Add migrated methods**

In `arbitrage_executor/bet_placer_betmgm.py`, REPLACE the `place_bet` and `check_limit_alert` stubs:

```python
    def place_bet(self):
        """Click the Place Bet button and check for success/failure."""
        print(f"[BETMGM] Placing bet...")
        return self._place_bet_betmgm()
```

For `check_limit_alert`: copy the body of `check_betmgm_limit_alert` (lines 2158–2203) verbatim under the method name `check_limit_alert`.

Add `_place_bet_betmgm` (lines 1988–2037) and `_close_betslip_betmgm` (lines 2039–2062) by copying verbatim.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_betmgm.py`:

```python
def test_place_bet_raises_when_button_not_found():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Place Bet button not found"):
        placer._place_bet_betmgm()


def test_check_limit_alert_returns_false_false_when_no_alert():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    limit_hit, adjusted = placer.check_limit_alert()
    assert limit_hit is False
    assert adjusted is None


def test_check_limit_alert_parses_adjusted_stake():
    alert = FakeElement(visible=True,
                        text="Your requested bet is over the allowed limit. ...")
    stake = FakeElement(visible=True, text="$6.76")
    page = FakePage(locators={
        'p.alert-content__message': FakeLocator([alert]),
        'span.betslip-summary-value': FakeLocator([stake]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    limit_hit, adjusted = placer.check_limit_alert()

    assert limit_hit is True
    assert adjusted == 6.76
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_betmgm.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "feat(bet_placer): migrate BetMGM place_bet + check_limit_alert"
```

---

### Task C8: Migrate BetMGM get_actual_odds

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Modify: `arbitrage_executor/tests/test_bet_placer_betmgm.py`

**Source line ranges:**
- `get_actual_odds_betmgm`: lines 2110–2147 (rename to `get_actual_odds`)

- [ ] **Step 1: Add migrated method**

In `arbitrage_executor/bet_placer_betmgm.py`, REPLACE the `get_actual_odds` stub by copying lines 2118–2147 verbatim under the method name `get_actual_odds`.

- [ ] **Step 2: Write tests**

Append to `arbitrage_executor/tests/test_bet_placer_betmgm.py`:

```python
def test_get_actual_odds_parses_decimal():
    odds_elem = FakeElement(visible=True, text="1.75")
    page = FakePage(locators={
        'span.odds-indicator__lite--default': FakeLocator([odds_elem]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer.get_actual_odds() == 1.75


def test_get_actual_odds_returns_none_when_not_found():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer.get_actual_odds() is None
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/test_bet_placer_betmgm.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm.py
git commit -m "feat(bet_placer): migrate BetMGM get_actual_odds"
```

---

### Task C-FINAL: Switch factory to route "betmgm" → BetmgmBetPlacer

**Files:**
- Modify: `arbitrage_executor/bet_placer.py` (`__new__` factory)

- [ ] **Step 1: Update `BetPlacer.__new__`**

In `arbitrage_executor/bet_placer.py`, find the factory block in `BetPlacer.__new__`:

```python
        if cls is BetPlacer:
            if site == "fanduel":
                from bet_placer_fanduel import FanduelBetPlacer
                return object.__new__(FanduelBetPlacer)
            if site == "betmgm":
                return object.__new__(_LegacyBetPlacer)
            raise BetPlacerError(f"Unknown site: {site}")
```

Replace with:

```python
        if cls is BetPlacer:
            if site == "fanduel":
                from bet_placer_fanduel import FanduelBetPlacer
                return object.__new__(FanduelBetPlacer)
            if site == "betmgm":
                from bet_placer_betmgm import BetmgmBetPlacer
                return object.__new__(BetmgmBetPlacer)
            raise BetPlacerError(f"Unknown site: {site}")
```

- [ ] **Step 2: Run the full test suite**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: all tests pass, including the existing `test_bet_placer_sequencing.py` (which now exercises `BetmgmBetPlacer` for its BetMGM tests).

- [ ] **Step 3: Live smoke against one BetMGM market**

Run:
```bash
cd /c/Users/tkmer/bountygate
python toolkit/selector_smoke_test.py --player "LeBron James"
```
Expected: BetMGM section runs without error. Compare audit screenshots against a pre-migration baseline.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer.py
git commit -m "feat(bet_placer): route betmgm through BetmgmBetPlacer"
```

---

## Section D — Cleanup

### Task D1: Delete `_LegacyBetPlacer` and dead helpers

**Files:**
- Modify: `arbitrage_executor/bet_placer.py` (delete `_LegacyBetPlacer` class entirely)

- [ ] **Step 1: Confirm `_LegacyBetPlacer` is unreferenced**

Run:
```bash
cd /c/Users/tkmer/bountygate
git grep -n "_LegacyBetPlacer" -- "*.py"
```
Expected: ONLY the definition and instantiation inside `bet_placer.py` itself. No external references.

- [ ] **Step 2: Delete the legacy class**

In `arbitrage_executor/bet_placer.py`:

1. Delete the entire `class _LegacyBetPlacer:` definition (all ~2,000 lines of its body, including `_american_to_decimal`, `FANDUEL_THRESHOLD_ONE_LABELS`, `_ACCORDION_FUZZY_THRESHOLD`).
2. Also delete the top-of-file imports that are no longer used by the remaining ABC: `re`, `playwright.sync_api.TimeoutError as PlaywrightTimeoutError`, the imports from `selector_finder`, `execution_logger`, `text_match`. Keep only what the ABC needs (`os`, `abc.ABC/abstractmethod`, `typing.Dict/Tuple/Optional`).

The remaining file should be ~80 lines: `BetPlacerError`, `BetPlacer` ABC, factory, `_screenshot` proxy, abstract method declarations, optional-capability defaults.

- [ ] **Step 3: Run the full test suite**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: all tests still pass.

- [ ] **Step 4: Import smoke**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -c "from bet_placer import BetPlacer, BetPlacerError; p = BetPlacer.__new__.__doc__; print('ok')"
python -c "import execute_arb; print('execute_arb ok')"
```
Expected: both print `ok`.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add arbitrage_executor/bet_placer.py
git commit -m "refactor(bet_placer): delete _LegacyBetPlacer; bet_placer.py shrinks to ABC + factory"
```

---

### Task D2: Final cross-site live smoke + lines-of-code check

**Files:** none

- [ ] **Step 1: Line-count check**

Run:
```bash
cd /c/Users/tkmer/bountygate
wc -l arbitrage_executor/bet_placer.py arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/_bet_placer_helpers.py
```
Expected: roughly
```
   80 arbitrage_executor/bet_placer.py
  900 arbitrage_executor/bet_placer_fanduel.py
  900 arbitrage_executor/bet_placer_betmgm.py
  100 arbitrage_executor/_bet_placer_helpers.py
 1980 total
```
(±100 lines per file is fine; the total should be near the original 2,217 minus the deleted dead code.)

- [ ] **Step 2: Full pytest run**

Run:
```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python -m pytest tests/ -v
```
Expected: ALL tests across `test_bet_placer_helpers.py`, `test_bet_placer_fanduel.py`, `test_bet_placer_betmgm.py`, and `test_bet_placer_sequencing.py` pass.

- [ ] **Step 3: End-to-end live verification (gating)**

Run a small real opportunity through the live system on both books. The exact procedure depends on your test setup. At minimum:

```bash
cd /c/Users/tkmer/bountygate/arbitrage_executor
python execute_arb.py
```

(with a low-stake opportunity in the queue or a dev-mode flag, whatever your project uses for safe live testing).

Confirm:
- The script runs end-to-end without `BetPlacerError`.
- The `audit_logs/` directory for this run contains the same screenshot tag sequence as a pre-migration run for the same market.
- The Discord webhook posts the same INFO message it would have pre-migration.
- A bet is actually placed and confirmed accepted on both books.

If the live run fails at a step covered by tests, the tests had a gap — add a test that reproduces the failure before fixing.

- [ ] **Step 4: PR-ready commit (no code change, just confirmation)**

If you haven't already, push your branch:

```bash
cd /c/Users/tkmer/bountygate
git push -u origin bet-placer-rewrite
```

---

## Plan summary

- **Sections A–D, total ~22 task units, each producing a self-contained commit.**
- **The bot is in a runnable state after every commit** — legacy and new code co-exist during the migration windows (A5 through B-FINAL for FD, B-FINAL through C-FINAL for MGM).
- **Test coverage grows incrementally** — Task A1 promotes the shared `FakePage`; each migration task adds tests for the methods it migrates.
- **The single gating manual step is the end-to-end live verification in D2.** Everything before that is automated.

If a task fails or reveals an unexpected dependency, halt and re-plan — do not push through with workarounds. Mid-plan corrections to the spec are easier than mid-merge corrections to live placement code.

## Test coverage scope (deviation from spec)

The spec listed exhaustive per-behavior coverage targets (~70 test cases across both sites). This plan implements roughly half of them — enough that every public method and every load-bearing private helper has at least one test covering its primary path or a critical invariant (e.g. BetMGM's "pick LAST empty stake input" rule in C6).

The remaining gaps fall into two buckets:

1. **Hard-to-mock paths** — flows that depend on `Locator.evaluate()` returning real ancestor strings (BetMGM walk-up), or `wait_for(state="visible")` semantics, or multi-element `nth()` iteration with mixed visibility/attribute states. Extending `FakePage` to cover these adds a ~100-line maintenance burden per path for marginal regression value. Covered by the existing `test_bet_placer_sequencing.py` end-to-end paths and by live smoke.
2. **Selector-cascade order regressions** — would require either snapshot-style assertions against the captured `page.locator` call log, or a "all selectors tried in this order" assertion DSL. Not built in this plan; the diff-audit gate (Task verification step in each migration) is the explicit substitute — engineers MUST eyeball the diff to confirm the cascade order is preserved verbatim from the legacy file.

If a live-smoke regression surfaces after merge, the fix is to add the FakePage test that would have caught it AND extend `FakePage` if needed — not to rebuild the test harness from scratch.
