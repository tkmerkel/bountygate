# Bet Placer Rewrite — Design

**Date:** 2026-05-17
**Status:** Approved (pending spec review)
**Scope:** Behavior-preserving split of `arbitrage_executor/bet_placer.py` (2,217 lines) into per-site modules with a shared interface, a single shared helper, and a new unit-test harness.

---

## Goal

Replace the monolithic `BetPlacer` class with an abstract base + one concrete subclass per sportsbook (FanDuel, BetMGM). The split makes the code holdable in context, removes site-dispatch noise from every public method, and establishes a clean extension point for a future third book. A new `FakePage` test harness gives the first real regression coverage for orchestration logic.

**Non-goal:** changing any selector, any timeout, any control flow, any log message, or any audit-screenshot tag. This is a refactor verified by tests, not a redesign.

**One intentional API break:** the four site-suffixed public method names (`get_actual_odds_fanduel`, `get_actual_odds_betmgm`, `discover_max_wager_fanduel`, `check_betmgm_limit_alert`) are renamed because the site identity now lives in the class. All callers are updated in the same PR. Constructor signature (`BetPlacer(page, site, audit_dir)`) is unchanged.

## Why now

`bet_placer.py` has reached 2,217 lines in one class. Concrete consequences:

- The file no longer fits in a single read window — edits to one method routinely break others through forgotten state assumptions (e.g., viewport size set in `find_and_click_bet`, depended on by `_enter_wager_*`).
- Every public method top-line is `if self.site == "fanduel": ... elif self.site == "betmgm":`, so a FanDuel-only change requires touching shared dispatch code and reasoning about whether BetMGM might be affected.
- A planned third book (DraftKings or Caesars) cannot be added cleanly under the current shape — it would mean three-arm dispatch in every method, growing the file further.
- The codebase has no automated test coverage for `BetPlacer`. Every change is verified by hand via live execution against real sportsbooks, which is slow, requires logged-in browser state, and risks placing real bets during debugging. A rewrite of this scope without first establishing test coverage would be reckless.

## Tech stack

- Python 3, sync Playwright (`playwright.sync_api`) — unchanged
- `unittest` (stdlib) for tests, matching the existing pattern in `toolkit/recorder/tests/test_roundtrip.py`
- No new third-party dependencies

## Architecture

### File layout

```
arbitrage_executor/
├── bet_placer.py                      # Public surface: BetPlacerError, BetPlacer factory + ABC (~180 lines)
├── bet_placer_fanduel.py              # FanduelBetPlacer + FANDUEL_THRESHOLD_ONE_LABELS (~900 lines)
├── bet_placer_betmgm.py               # BetmgmBetPlacer (~900 lines)
└── _bet_placer_helpers.py             # screenshot, first_visible, _ACCORDION_FUZZY_THRESHOLD (~120 lines)

tests/
└── bet_placer/
    ├── __init__.py
    ├── fake_page.py                   # FakePage test double (Playwright surface) (~400 lines)
    ├── test_helpers.py                # first_visible, _screenshot, threshold-1 label table (~150 lines)
    ├── test_fanduel.py                # FanduelBetPlacer methods via FakePage (~400 lines)
    └── test_betmgm.py                 # BetmgmBetPlacer methods via FakePage (~400 lines)
```

### Public interface (preserved)

`bet_placer.py` exposes the same names callers already import:

```python
class BetPlacerError(Exception):
    """Raised when bet placement fails."""

class BetPlacer(ABC):
    """Abstract base. Construct via `BetPlacer(page, site, audit_dir)` —
    the factory dispatches to FanduelBetPlacer or BetmgmBetPlacer."""

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
        # arguments — Python's __new__/__init__ contract requires that
        # the same call signature reach both. If a subclass needs extra
        # init state, override __init__ and call super().__init__(...)
        # first, then set the extra attributes on self.
        if cls is BetPlacer:
            from bet_placer_fanduel import FanduelBetPlacer
            from bet_placer_betmgm import BetmgmBetPlacer
            if site == "fanduel":
                return object.__new__(FanduelBetPlacer)
            if site == "betmgm":
                return object.__new__(BetmgmBetPlacer)
            raise BetPlacerError(f"Unknown site: {site}")
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
        raise NotImplementedError(f"{self.site} does not support max-wager discovery")
    def check_limit_alert(self) -> Tuple[bool, Optional[float]]:
        raise NotImplementedError(f"{self.site} does not support limit-alert check")
```

**Renamed methods** (callers updated in the same PR):

| Old | New | Reason |
|---|---|---|
| `discover_max_wager_fanduel(self)` | `discover_max_wager(self)` on `FanduelBetPlacer` | Site is implied by class |
| `check_betmgm_limit_alert(self)` | `check_limit_alert(self)` on `BetmgmBetPlacer` | Site is implied by class |
| `get_actual_odds_fanduel(self)` / `get_actual_odds_betmgm(self)` | `get_actual_odds(self)` on each subclass | Unified abstract method |

Callers in `execute_arb.py` currently invoke these by their site-suffixed names; those call sites get one-line updates as part of this work.

### `_bet_placer_helpers.py` — the only new abstraction

```python
"""Shared helpers for bet_placer site implementations.

Kept intentionally tiny. The only reusable pattern in the legacy bet_placer.py
was the selector-cascade locator search; everything else (modal dismissal,
slip inspection, wager-entry quirks) is site-specific and lives in the
respective subclass.
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
    """Save screenshot for audit trail. Never raises — logs and returns the
    intended path even if capture fails (so failure paths still produce a
    consistent log line)."""
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
    """Try each CSS selector in order; return the first locator whose `.first`
    is visible, or None. Logs which selector matched on success so the audit
    trail keeps parity with the legacy inline logging.

    Caller is responsible for any selector with placement-sensitive semantics
    (e.g. picking the LAST empty stake input, not the first visible one) —
    that logic stays inline in the caller.
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

That is the **complete** new abstraction surface. Every other inline pattern stays inline. We are not extracting "modal-dismissing strategies" or "slip-clear strategies" or any other higher-order behavior — those are site-specific even when superficially similar.

### What is NOT split out

- **`FANDUEL_THRESHOLD_ONE_LABELS`** stays in `bet_placer_fanduel.py`. FanDuel-only.
- **Per-method selector cascades** stay inline. They are tightly coupled to the surrounding control flow (re-click on toggle-off, scroll before scan, etc.).
- **The `_american_to_decimal` method** is deleted. It is defined but never called.
- **`_dismiss_fanduel_modal`, `_open_betmgm_slip`, `_clear_betslip_*`** stay as private methods on the respective site class. Not extracted to helpers.

## Test architecture

### Layer 1 — Pure-logic unit tests (`test_helpers.py`)

Direct unittest coverage for code with no Playwright dependency:

- `first_visible` cascade order: given a `FakePage` returning different visibility states across selectors, asserts the function picks the first visible one and skips invisible/zero-count locators.
- `screenshot` filename format: assert produced path matches `{audit_dir}/{site}_{tag}_{YYYYMMDD_HHMMSS}.png` and that capture failure doesn't raise.
- `FANDUEL_THRESHOLD_ONE_LABELS` table: spot-check each entry produces the expected `(verb, article, noun)` tuple (catches accidental edits to the lookup table).
- Any regex pattern that's worth its own test: the slip-pill `(N)` parser, the MAX WAGER `$\s*([0-9,]+)` parser, the FanDuel `Place \$X bet` matcher. Extract these to module-level constants in their owning files so the tests can import them without instantiating a placer.

These tests run in well under one second and require no browser.

### Layer 2 — Site method tests via `FakePage` (`test_fanduel.py`, `test_betmgm.py`)

`tests/bet_placer/fake_page.py` implements the slice of the Playwright `Page` and `Locator` surface that `BetPlacer` actually calls:

- `Page`: `goto`, `wait_for_timeout`, `set_viewport_size`, `screenshot`, `locator`, `get_by_text`, `get_by_role`, `get_by_label`, `wait_for_selector`, `keyboard.press`, `keyboard.type`, `evaluate`, `url`
- `Locator`: `count`, `first`, `nth`, `is_visible`, `click`, `fill`, `press`, `type`, `text_content`, `get_attribute`, `input_value`, `wait_for`, `all`, `evaluate`

The fake is **scripted** — each test sets up a sequence of canned responses:

```python
def test_fanduel_clear_slip_skips_when_already_empty(self):
    page = FakePage()
    page.text("Betslip empty", visible=True)            # canned text-search result
    placer = FanduelBetPlacer(page, "fanduel", tmp_audit_dir())

    placer._clear_betslip_fanduel()

    self.assertEqual(page.clicks, [])                   # never tried to remove anything
    self.assertIn("Slip already empty.", page.printed_lines())
```

```python
def test_fanduel_modal_dismissal_clicks_button(self):
    page = FakePage()
    page.locator('div[role="dialog"][aria-modal="true"]').exists(visible=True)
    page.locator('div[role="dialog"][aria-modal="true"] button').exists(visible=True, count=1)
    placer = FanduelBetPlacer(page, "fanduel", tmp_audit_dir())

    placer._dismiss_fanduel_modal()

    self.assertEqual(len(page.clicks), 1)
    self.assertEqual(page.clicks[0].selector, 'div[role="dialog"][aria-modal="true"] button')
```

**Coverage targets** (one test per behavior; not one test per method):

FanDuel:
- `_dismiss_fanduel_modal`: no-op when no modal, clicks first button when modal visible, logs modal text
- `_clear_betslip_fanduel`: empty fast path, single "Remove all" path, individual-remove fallback path, post-clear verification raises when remove buttons remain
- `_fanduel_slip_has_bet` / `_fanduel_slip_is_empty`: empty markers, ambiguous state defaults
- `_find_and_click_alternate_bet_fanduel`: threshold-1 label path, standard `N+` selector cascade order, toggle-off re-click when `aria-label` contains " Selected", slip-empty-after-click raises
- `_enter_wager_fanduel`: input found via `get_by_label('WAGER $')`, fallback to `inputmode="decimal"`, fallback to legacy class selectors, diagnostic dump on miss
- `_place_bet_fanduel`: success via "Bet placed" text, success via data-testid, REJECTED on error text, UNKNOWN on no signal
- `discover_max_wager`: parses dollar amount from MAX WAGER text, returns 99999 when no alert, returns 500 on parse failure
- `get_actual_odds`: parses from `aria-label="Odds X.XX"`, falls back to text content

BetMGM:
- `_navigate_betmgm`: MLB autocomplete suggestion click path, non-MLB Enter path, "All Wagers" card scoring picks card with both teams over one-team cards, futures cards skipped, overlay fallback
- `_select_market_sub_tab_betmgm`: no-op when no `sub_tab_label`, clicks first matching selector
- `_select_alternate_tab_betmgm`: clicks `{N}+` tab matching threshold, continues when tab not found
- `_click_betmgm_pick_for_player`: scrolls before scan, matches by direction-letter + line, walks ancestors for player name, prefers `data-test-option-id` selector for the click
- `_clear_betslip_betmgm_precheck`: "(0)" fast path, slip-pill regex parsing, individual-remove sweep, post-clear verification raises when pill still > 0
- `_open_betmgm_slip`: no-op when stake input already visible, clicks first pill selector
- `_betmgm_slip_has_bet`: empty marker, pill count parsing, remove-control presence
- `_enter_wager_betmgm`: picks LAST empty stake input across multiple inputs (NOT first), uses `keyboard.type` with delay (not `fill`), presses Tab to blur
- `_place_bet_betmgm`: success via "Your bet has been accepted", REJECTED on error text, UNKNOWN on timeout
- `check_limit_alert`: parses adjusted stake from `betslip-summary-value`, returns `(True, None)` when alert present but stake unparseable, returns `(False, None)` when no alert
- `get_actual_odds`: parses from `span.odds-indicator__lite--default`

### Layer 3 — Live smoke test (existing)

`toolkit/selector_smoke_test.py` already exists and runs against live FanDuel + BetMGM with a hard stop before placement. After the rewrite, manually re-run it for one player on each book to verify the abstract interface still works against real DOM. No code changes here.

### Layer 4 — Diff audit (one-time gate)

For each migrated method, eyeball the diff between the old method body in `bet_placer.py` and the new method body in `bet_placer_{site}.py`. Acceptable diffs:

- Indentation changes (method now lives at class top-level of the new file)
- No change to `self._screenshot("tag")` call sites — the helper `screenshot(page, audit_dir, site, tag)` is wrapped by a `_screenshot(self, tag)` proxy on the `BetPlacer` ABC, so per-method bodies migrate unchanged in this regard
- Inline selector-cascade loops replaced by `first_visible(...)` calls **only where semantics are exactly equivalent** (no placement sensitivity, no per-iteration extra checks beyond `count() > 0 and first.is_visible()`)

Anything else is suspicious and gets called out in plan-time review.

### What is explicitly NOT in scope for tests

- **HTML snapshot replay tests.** Capturing real page HTML at every step and replaying through a real headless browser would be higher fidelity, but requires building a snapshot-recording phase, fixture management, and per-snapshot upkeep when sites redesign. Separate project.
- **End-to-end Playwright integration tests.** Slow, flaky, require live login state. Covered by the existing smoke test.
- **Property-based or fuzz testing.** Overkill for orchestration code.

## Component-by-component migration plan

(Detailed task breakdown lives in the implementation plan — this section is the architectural intent.)

1. **Extract helpers and constants first.** `_bet_placer_helpers.py` + tests. No behavior change yet — the original `bet_placer.py` still exists unchanged.
2. **Create the abstract base.** `BetPlacer` (ABC) in a new module, with `__new__` factory. Keep the old `BetPlacer` (concrete) renamed to `_LegacyBetPlacer` temporarily, with the factory still routing to it. Verify nothing breaks.
3. **Migrate FanDuel.** Build `FanduelBetPlacer` by copying all `_fanduel` and `_dismiss_fanduel_modal` / `_clear_betslip_fanduel` / etc. methods into the new file. Wire the factory to route `"fanduel"` to `FanduelBetPlacer`. Land FanDuel tests. Live smoke against one FD market.
4. **Migrate BetMGM.** Same pattern. Wire the factory. Land BetMGM tests. Live smoke.
5. **Delete the legacy class.** `_LegacyBetPlacer` is now unused; remove it. `bet_placer.py` shrinks to just the public surface (`BetPlacerError`, `BetPlacer` ABC, factory). Delete `_american_to_decimal` (dead code, only existed on the legacy class).
6. **Update callers.** `execute_arb.py` switches from `bet_placer.get_actual_odds_fanduel()` to `bet_placer.get_actual_odds()`, etc. The constructor call is unchanged.

Each step is a self-contained commit; after each one the bot is in a runnable state (legacy + new code can co-exist during steps 2-4).

## Data flow (unchanged from current)

```
ArbExecutor (execute_arb.py)
    │
    ├─→ BetPlacer(page, "fanduel", audit_dir)  →  FanduelBetPlacer instance
    │       .navigate_and_expand_market(opp, mc)        # search, dismiss modal, clear slip
    │       .find_and_click_bet(opp, "over", mc)        # find tile, click, verify slip
    │       .enter_wager(amount)                        # open slip, find input, type
    │       .discover_max_wager()                       # (FD only) enter 99999, parse cap
    │       .place_bet()                                 → (status, message)
    │       .get_actual_odds()                           → float | None
    │
    └─→ BetPlacer(page, "betmgm", audit_dir)   →  BetmgmBetPlacer instance
            .navigate_and_expand_market(opp, mc, direction)
            .find_and_click_bet(opp, "over", mc)
            .enter_wager(amount)
            .check_limit_alert()                        # (MGM only) → (limit_hit, adjusted_stake)
            .place_bet()                                 → (status, message)
            .get_actual_odds()                           → float | None
```

The orchestrator's 3-phase strategy in `execute_arb.py` (tease FD limit → place MGM → hedge on FD) is untouched.

## Error handling

Unchanged. `BetPlacerError` remains the only exception type raised from this module. The CRITICAL/WARNING/INFO Discord-paging behavior in `execution_logger.py` and `task_worker.py` is downstream and not in scope.

The single new behavior: calling `discover_max_wager()` on a non-FD placer or `check_limit_alert()` on a non-MGM placer raises `NotImplementedError` (today, calling `get_actual_odds_fanduel` on a BetMGM placer would fail at the `if self.site == "fanduel"` dispatch and fall through). This is a strict improvement — callers in `execute_arb.py` already only invoke these capabilities on the appropriate site.

## Logging and audit

Every `print(f"[FANDUEL] ...")` and `print(f"[BETMGM] ...")` line is preserved verbatim. `execution_logger.py` and the operator runbook (`arbitrage_executor/CLAUDE.md`) depend on these strings for severity classification.

Every `_screenshot("tag")` call is preserved with the same tag. `audit_logs/{timestamp}_{player}_{market}/` will contain the same set of files in the same order as before.

## Migration risks and mitigations

| Risk | Mitigation |
|---|---|
| Test harness becomes a parallel implementation that drifts from real Playwright | Keep `FakePage` surface minimal — only mock methods actually called by `BetPlacer`. Each test that exercises a path the fake can't simulate gets skipped + flagged for live-smoke coverage instead. |
| Replacing inline selector cascades with `first_visible` changes semantics for placement-sensitive cases | Audit gate (Layer 4) explicitly disallows `first_visible` substitution where the original loop did anything beyond `count() > 0 and first.is_visible()`. Examples that stay inline: `_enter_wager_betmgm` (picks LAST empty), `_find_and_click_alternate_bet_fanduel` (per-iteration toggle-state check). |
| Renaming `get_actual_odds_fanduel` → `get_actual_odds` breaks a caller we don't know about | Grep `execute_arb.py` and the rest of the repo (including `app/`, `dashboard/`, `watcher/`, `toolkit/`) for `get_actual_odds_fanduel`, `get_actual_odds_betmgm`, `discover_max_wager_fanduel`, `check_betmgm_limit_alert` before merging. Update every match in the same PR. |
| Factory pattern via `__new__` is confusing for readers | Comment block on `BetPlacer.__new__` explains the dispatch. Subclass constructors do nothing site-aware — all site identity is baked into the class. |
| Tests pass but live placement fails | Live smoke (Layer 3) is mandatory before merge. Place one real bet on each book, end-to-end, in the staging-equivalent environment (real bots, small wager). |

## Open design questions

None. Defaults to merge:

- Split by site (with explicit "good for adding new sites" goal) — approved
- One shared `first_visible` helper, otherwise no new abstractions — approved
- Test coverage via `FakePage` unit tests + reused live smoke — approved
- Drop `_american_to_decimal` dead code — approved

## Out of scope (explicit non-goals)

- Selector externalization to YAML
- Async/await rewrite
- Page-object-model abstraction beyond the per-site split
- HTML snapshot replay tests
- Adding a third sportsbook now (the architecture supports it, but the work is a separate project)
- Changes to `execute_arb.py`'s 3-phase orchestration
- Changes to `execution_logger.py`, `task_worker.py`, `selector_finder.py`, or `validate_selector.py`
- Changes to `audit_logs/` directory structure or screenshot naming
