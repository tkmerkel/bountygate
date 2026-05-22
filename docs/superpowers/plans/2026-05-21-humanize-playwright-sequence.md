# Humanized Playwright Sequence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the bot's mechanical Playwright sequence (direct URL navigation, fixed timeouts, instant typing, viewport hardcodes, eager slip-clear dances) with a humanized layer that introduces Bezier mouse movement, lognormal typing with typo-and-correction, settle-based waits, browsing context (homepage warmup + intra-book idle), and a background modal watcher — while preserving the existing `BetPlacer` public interface and the Phase 1 → Phase 2 → Phase 3 orchestration shape.

**Architecture:** Extract a new `arbitrage_executor/human/` package that holds the humanization primitives (mouse, typing, waiting, navigation, modals, session). Rewrite `bet_placer_fanduel.py` and `bet_placer_betmgm.py` from ~1100 lines each down to ~400 lines each by delegating all timing/movement/modal/wait concerns into `human/`. Add three orchestrator hook points: `warmup_browse` before Phase 1, `intra_book_idle` between Phase 1 and Phase 2 (FD only — explicit hard rule of no idle between Phase 2 and Phase 3 because the orphan window opens at Phase 2 placement). Validate via `validate_selector.py` (unchanged interface) + a new `scripts/revalidate_all.py` regression sweep, then a 6-step gated rollout.

**Tech Stack:** Python 3.11+, Playwright (sync API, CDP attach on port 9223), pytest with the existing `FakeElement`/`FakeLocator` doubles in `arbitrage_executor/tests/_fakes.py`. No new runtime dependencies; the humanization is pure Python on top of Playwright primitives Chrome already exposes.

---

## Source spec

This plan implements the design captured in `C:\Users\tkmer\AppData\Local\Temp\handoff-humanize-playwright-2026-05-21.md` (sections 1–8). The handoff is the spec — every confirmed scoping decision in the "User-confirmed scoping decisions" table is a binding requirement of this plan.

## Branch and rollout strategy

- Branch name: `redo-playwright-humanized` (off `main`; the original `game-line-arb-pipeline` branch referenced in the handoff has since been merged).
- Legacy files renamed `*_legacy.py` at the START of the rewrite (diff-auditability anchor). The legacy files are NOT wired into the import graph — they exist only so reviewers can diff against the new implementation. Deleted in the merge commit.
- The branch is non-functional from Task 0 commit through Task 15 commit (placers don't yet exist). This is acceptable because `main` continues to run the live bot during the rewrite.

## File structure (created or modified)

**New package — `arbitrage_executor/human/`:**
| File | Responsibility |
|---|---|
| `__init__.py` | Public re-exports of the humanization API |
| `errors.py` | `SlipDrainedDuringIdleError`, `FdOddsDriftedDuringIdleError` |
| `waiting.py` | `settle(page, category, *, jittered=True)` — replaces naked `wait_for_timeout` |
| `typing.py` | `TypingProfile` (daily-seeded), `humanized_type(page, locator, text)` |
| `mouse.py` | `CursorState`, `move_to(page, locator)`, `click(page, locator)`, `idle_jitter(page)` |
| `navigation.py` | `click_through(page, start_url, link_selector, *, fallback_url)` |
| `modals.py` | `ModalWatcher` — background-thread modal dismisser, registered per page |
| `session.py` | `warmup_browse(page, site)`, `intra_book_idle(page, opportunity)`, `viewport_from_cdp(page)` |

**Renamed (legacy diff anchors):**
- `arbitrage_executor/bet_placer_fanduel.py` → `arbitrage_executor/bet_placer_fanduel_legacy.py`
- `arbitrage_executor/bet_placer_betmgm.py` → `arbitrage_executor/bet_placer_betmgm_legacy.py`

**Rewritten (~400 lines each, replacing legacy versions at the original path):**
- `arbitrage_executor/bet_placer_fanduel.py`
- `arbitrage_executor/bet_placer_betmgm.py`

**Modified (orchestrator):**
- `arbitrage_executor/execute_arb.py` — drop viewport hardcodes (lines 287, 350); add `warmup_browse` call inside `_warmup_sessions` (around line 717); add `intra_book_idle` hook between Phase 1 and Phase 2 (around line 336); catch the two new typed errors as benign skip alongside `BetPlacerSkipError`.

**New tooling:**
- `arbitrage_executor/scripts/revalidate_all.py` — iterate every market in the YAML configs, run `validate_selector.validate_selector`, report regressions.

**New tests (mirror the human/ package one-to-one):**
- `arbitrage_executor/tests/human/__init__.py`
- `arbitrage_executor/tests/human/test_waiting.py`
- `arbitrage_executor/tests/human/test_typing.py`
- `arbitrage_executor/tests/human/test_mouse.py`
- `arbitrage_executor/tests/human/test_navigation.py`
- `arbitrage_executor/tests/human/test_modals.py`
- `arbitrage_executor/tests/human/test_session.py`

## Dependency order

```
Phase 0 — Branch setup + legacy rename
   ↓
Phase 1 — human/ package
   1.1 waiting (no deps)
   1.2 typing (no deps)
   1.3 mouse (no deps)
   1.4 navigation (deps: waiting, mouse)
   1.5 modals (deps: waiting)
   1.6 session (deps: all of the above + errors)
   ↓
Phase 2 — Placer rewrites (each placer uses human/)
   ↓
Phase 3 — Orchestrator wiring (uses session.warmup_browse + session.intra_book_idle + errors)
   ↓
Phase 4 — revalidate_all tooling (uses validate_selector unchanged)
   ↓
Phase 5 — Operator validation gate (manual)
```

## Conventions used throughout this plan

- All commands are PowerShell (Windows). Path separators in code use forward slashes (Python is fine with them on Windows).
- `pytest` is the test runner. Test invocations use `pytest <path> -v`.
- Every implementation step that writes code shows the actual code in a fenced block.
- Tests for randomness-bearing code use `random.Random(seed)` injected via parameter, never patching the module-level `random`.
- Commits use Conventional Commits style (`feat(human): ...`, `refactor(placer): ...`).

---

## Task 0: Branch setup, legacy rename, and stub new placer modules

**Files:**
- Rename: `arbitrage_executor/bet_placer_fanduel.py` → `arbitrage_executor/bet_placer_fanduel_legacy.py`
- Rename: `arbitrage_executor/bet_placer_betmgm.py` → `arbitrage_executor/bet_placer_betmgm_legacy.py`
- Create: `arbitrage_executor/bet_placer_fanduel.py` (stub)
- Create: `arbitrage_executor/bet_placer_betmgm.py` (stub)

- [ ] **Step 1: Create the rewrite branch**

```powershell
git checkout -b redo-playwright-humanized
git status
```

Expected: `On branch redo-playwright-humanized`, clean tree.

- [ ] **Step 2: Rename legacy placer files**

```powershell
git mv arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/bet_placer_fanduel_legacy.py
git mv arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/bet_placer_betmgm_legacy.py
```

- [ ] **Step 3: Write stub `bet_placer_fanduel.py`**

```python
"""FanDuel humanized bet placer.

Rewrite of the legacy bet_placer_fanduel.py against the new
``arbitrage_executor/human/`` primitives. Public interface preserved
(see ``bet_placer.BetPlacer`` ABC).
"""

from typing import Dict, Optional, Tuple

from bet_placer import BetPlacer, BetPlacerError


class FanduelBetPlacer(BetPlacer):
    """Handles bet placement on FanDuel."""

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        raise NotImplementedError("Task 14")

    def clear_betslip(self):
        raise NotImplementedError("Task 14")

    def assert_betslip_has_bet(self):
        raise NotImplementedError("Task 14")

    def assert_betslip_empty(self):
        raise NotImplementedError("Task 14")

    def find_and_click_bet(self, opportunity, direction, market_config):
        raise NotImplementedError("Task 15")

    def enter_wager(self, amount):
        raise NotImplementedError("Task 15")

    def place_bet(self):
        raise NotImplementedError("Task 15")

    def get_actual_odds(self):
        raise NotImplementedError("Task 15")

    def discover_max_wager(self):
        raise NotImplementedError("Task 15")
```

- [ ] **Step 4: Write stub `bet_placer_betmgm.py`**

```python
"""BetMGM humanized bet placer.

Rewrite of the legacy bet_placer_betmgm.py against the new
``arbitrage_executor/human/`` primitives. Public interface preserved
(see ``bet_placer.BetPlacer`` ABC).
"""

from typing import Dict, Optional, Tuple

from bet_placer import BetPlacer, BetPlacerError, BetPlacerSkipError


class BetmgmBetPlacer(BetPlacer):
    """Handles bet placement on BetMGM."""

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        raise NotImplementedError("Task 12")

    def clear_betslip(self):
        raise NotImplementedError("Task 12")

    def assert_betslip_has_bet(self):
        raise NotImplementedError("Task 12")

    def assert_betslip_empty(self):
        raise NotImplementedError("Task 12")

    def find_and_click_bet(self, opportunity, direction, market_config):
        raise NotImplementedError("Task 13")

    def enter_wager(self, amount):
        raise NotImplementedError("Task 13")

    def place_bet(self):
        raise NotImplementedError("Task 13")

    def get_actual_odds(self):
        raise NotImplementedError("Task 13")

    def check_limit_alert(self):
        raise NotImplementedError("Task 13")
```

- [ ] **Step 5: Verify factory still imports cleanly**

```powershell
python -c "from arbitrage_executor.bet_placer import BetPlacer; print('OK')"
```

Expected: `OK`. The factory's `__new__` imports the new stub modules; the legacy files are never imported by anything.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/bet_placer_fanduel_legacy.py arbitrage_executor/bet_placer_betmgm_legacy.py
git commit -m "refactor(placer): rename legacy placers and stub new humanized versions"
```

---

## Task 1: `human/` package skeleton and typed errors

**Files:**
- Create: `arbitrage_executor/human/__init__.py`
- Create: `arbitrage_executor/human/errors.py`
- Create: `arbitrage_executor/tests/human/__init__.py` (empty)
- Test: `arbitrage_executor/tests/human/test_errors.py`

- [ ] **Step 1: Write the failing test**

```python
# arbitrage_executor/tests/human/test_errors.py
from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)
from bet_placer import BetPlacerSkipError


def test_idle_errors_subclass_skip_error():
    """Idle-window errors must be benign skips, not real failures.

    The orchestrator catches BetPlacerSkipError as a non-counting skip;
    these errors only fire BEFORE the Phase 2 placement (idle is
    explicitly forbidden between Phase 2 and Phase 3), so they are
    benign by construction.
    """
    assert issubclass(SlipDrainedDuringIdleError, BetPlacerSkipError)
    assert issubclass(FdOddsDriftedDuringIdleError, BetPlacerSkipError)


def test_drift_error_carries_old_and_new_odds():
    err = FdOddsDriftedDuringIdleError(old_odds=2.10, new_odds=2.05, epsilon=0.05)
    assert err.old_odds == 2.10
    assert err.new_odds == 2.05
    assert err.epsilon == 0.05
    assert "2.10" in str(err) and "2.05" in str(err)
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
pytest arbitrage_executor/tests/human/test_errors.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'human'`.

- [ ] **Step 3: Create the package files**

```python
# arbitrage_executor/human/__init__.py
"""Humanized Playwright primitives.

Public API surface kept small so callers (placers, orchestrator,
validator) import from ``human`` directly without reaching into
submodules.
"""

from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)

__all__ = [
    "SlipDrainedDuringIdleError",
    "FdOddsDriftedDuringIdleError",
]
```

```python
# arbitrage_executor/human/errors.py
"""Typed errors raised during the humanization flow.

Both errors subclass ``BetPlacerSkipError`` so the orchestrator's
existing ``except BetPlacerSkipError`` branches classify them as
benign skips. They can only fire during ``intra_book_idle`` (between
Phase 1 and Phase 2) — by hard rule there is no idle between Phase 2
and Phase 3, so neither can fire inside the orphan window.
"""

from bet_placer import BetPlacerSkipError


class SlipDrainedDuringIdleError(BetPlacerSkipError):
    """The FanDuel betslip lost its Phase 1 selection while the bot was
    idling. The Phase 2 placement cannot proceed without a hedge target,
    so we skip to the next opportunity instead of placing a bare MGM
    leg.
    """


class FdOddsDriftedDuringIdleError(BetPlacerSkipError):
    """FanDuel odds moved by more than ``IDLE_DRIFT_EPSILON`` (decimal
    units) between the Phase 1 tease-discovery and the post-idle
    re-check. ROI may have flipped negative; skip rather than place.
    """

    def __init__(self, *, old_odds: float, new_odds: float, epsilon: float):
        self.old_odds = old_odds
        self.new_odds = new_odds
        self.epsilon = epsilon
        super().__init__(
            f"FanDuel odds drifted during idle: {old_odds:.2f} → "
            f"{new_odds:.2f} (epsilon={epsilon:.2f})"
        )
```

- [ ] **Step 4: Add `tests/human/__init__.py` (empty file)**

```powershell
ni arbitrage_executor/tests/human/__init__.py -ItemType File
```

- [ ] **Step 5: Run test to verify it passes**

```powershell
pytest arbitrage_executor/tests/human/test_errors.py -v
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/__init__.py arbitrage_executor/human/errors.py arbitrage_executor/tests/human/__init__.py arbitrage_executor/tests/human/test_errors.py
git commit -m "feat(human): bootstrap human/ package with idle-window typed errors"
```

---

## Task 2: `human/waiting.py` — categorized `settle()`

**Files:**
- Create: `arbitrage_executor/human/waiting.py`
- Test: `arbitrage_executor/tests/human/test_waiting.py`
- Modify: `arbitrage_executor/human/__init__.py` (re-export `settle`)

Replaces the existing naked `self.page.wait_for_timeout(2000)` / `wait_for_timeout(3000)` calls scattered through the placers. Categories make the wait *intent* legible to a reviewer.

- [ ] **Step 1: Write the failing test**

```python
# arbitrage_executor/tests/human/test_waiting.py
import random

import pytest

from human.waiting import settle, WAIT_CATEGORIES


class FakePage:
    def __init__(self):
        self.waited_ms: list[int] = []

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))


def test_settle_each_category_falls_in_documented_band():
    """settle(category) must sample within the documented band for that category."""
    rng = random.Random(42)
    for category, (lo_ms, hi_ms) in WAIT_CATEGORIES.items():
        page = FakePage()
        # Sample many times to confirm we stay in band even at the tails.
        for _ in range(200):
            settle(page, category, rng=rng)
        assert all(lo_ms <= w <= hi_ms for w in page.waited_ms), (
            f"{category}: out of band, got {sorted(set(page.waited_ms))[:5]}..."
        )


def test_settle_rejects_unknown_category():
    page = FakePage()
    with pytest.raises(KeyError):
        settle(page, "made_up_category", rng=random.Random(0))


def test_settle_jittered_false_uses_band_midpoint():
    """jittered=False is the escape hatch for tests that want deterministic timing."""
    page = FakePage()
    settle(page, "page_load", jittered=False)
    lo, hi = WAIT_CATEGORIES["page_load"]
    assert page.waited_ms == [(lo + hi) // 2]
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
pytest arbitrage_executor/tests/human/test_waiting.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'human.waiting'`.

- [ ] **Step 3: Implement `human/waiting.py`**

```python
# arbitrage_executor/human/waiting.py
"""Categorized waits — replaces naked ``page.wait_for_timeout(...)`` calls.

Every wait in the bot exists for a *reason*: page is loading, a user
would be reading a panel, a UI animation needs to settle, a result
needs a moment to render. Naming the reason makes audits possible:
"the bot waited 5s here because we expect a search-results render."

Each category is a (lower_ms, upper_ms) band; ``settle`` samples
uniformly within the band so cross-attempt timing isn't suspiciously
constant.
"""

import random
from typing import Protocol


class _Page(Protocol):
    def wait_for_timeout(self, ms: int) -> None: ...


# Categories — bounds are inclusive. Tune these as we learn what feels
# right; they intentionally err on the longer side to give the modal
# watcher and the lazy-loaded UI time to settle.
WAIT_CATEGORIES: dict[str, tuple[int, int]] = {
    # Generic post-navigation, DOM-loaded but JS still mounting.
    "page_load":            (1800, 3200),
    # Search input has been filled; suggestions/results render.
    "search_results":       (2400, 3800),
    # An accordion or panel has been clicked and is expanding.
    "ui_expansion":         (600, 1300),
    # A modal dismiss button has been clicked; modal animates out.
    "modal_dismiss":        (900, 1700),
    # A bet has just been clicked; slip pill updates.
    "slip_update":          (700, 1400),
    # User-reads-and-decides pause — used between Phase 1 and a follow-up.
    "reading_panel":        (1200, 2800),
    # Between keystrokes during typing (note: typing.py has its own
    # finer-grained per-keystroke distribution; this is for bulk delays
    # like the pre-Enter dwell).
    "pre_submit_dwell":     (450, 950),
    # Very short — between two related clicks in a flow.
    "micro_pause":          (180, 420),
}


def settle(
    page: _Page,
    category: str,
    *,
    jittered: bool = True,
    rng: random.Random | None = None,
) -> int:
    """Wait for the band documented under ``category``.

    Args:
        page: anything that quacks like ``Page.wait_for_timeout(ms)``.
        category: one of the keys in ``WAIT_CATEGORIES``. Raises
            ``KeyError`` if not — typo-safety.
        jittered: when True (default), sample uniformly in the band.
            When False, use the band midpoint — for tests that want
            deterministic timing.
        rng: explicit RNG for tests. Defaults to the module-level
            ``random`` instance.

    Returns:
        The number of milliseconds actually waited.
    """
    lo, hi = WAIT_CATEGORIES[category]
    if jittered:
        r = rng or random
        ms = int(r.uniform(lo, hi))
    else:
        ms = (lo + hi) // 2
    page.wait_for_timeout(ms)
    return ms
```

- [ ] **Step 4: Re-export `settle` from `human/__init__.py`**

```python
# arbitrage_executor/human/__init__.py
"""Humanized Playwright primitives."""

from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)
from human.waiting import settle, WAIT_CATEGORIES

__all__ = [
    "SlipDrainedDuringIdleError",
    "FdOddsDriftedDuringIdleError",
    "settle",
    "WAIT_CATEGORIES",
]
```

- [ ] **Step 5: Run test to verify it passes**

```powershell
pytest arbitrage_executor/tests/human/test_waiting.py -v
```

Expected: 3 passed.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/waiting.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_waiting.py
git commit -m "feat(human): add categorized settle() to replace naked wait_for_timeout"
```

---

## Task 3: `human/typing.py` — `TypingProfile` (daily-rotating, bigram-aware)

**Files:**
- Create: `arbitrage_executor/human/typing.py`
- Test: `arbitrage_executor/tests/human/test_typing.py`
- Modify: `arbitrage_executor/human/__init__.py`

Models: lognormal inter-key delay (median ~120ms, p95 ~220ms). Bigram-aware (common pairs faster, same-finger pairs slower). 3% typo-and-correction rate, only on text length ≥ 6. Per-session profile is daily-seeded so the same machine on the same date has stable typing rhythm.

- [ ] **Step 1: Write the failing test**

```python
# arbitrage_executor/tests/human/test_typing.py
import statistics
from datetime import date

import pytest

from human.typing import TypingProfile, SAME_FINGER_PAIRS


def test_profile_seed_is_daily_stable():
    """Two profiles built on the same date produce the same first 100 delays."""
    p1 = TypingProfile.for_date(date(2026, 5, 21))
    p2 = TypingProfile.for_date(date(2026, 5, 21))
    seq1 = [p1.next_delay_ms("a", "b") for _ in range(100)]
    seq2 = [p2.next_delay_ms("a", "b") for _ in range(100)]
    assert seq1 == seq2


def test_profile_changes_across_days():
    """Different dates → different rhythm (cheap sanity, not stat-significant)."""
    p1 = TypingProfile.for_date(date(2026, 5, 21))
    p2 = TypingProfile.for_date(date(2026, 5, 22))
    seq1 = [p1.next_delay_ms("a", "b") for _ in range(50)]
    seq2 = [p2.next_delay_ms("a", "b") for _ in range(50)]
    assert seq1 != seq2


def test_delay_distribution_hits_target_median_and_p95():
    """Median ~120ms (range 100-140), p95 ~220ms (range 180-280) for neutral bigrams."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    samples = [p.next_delay_ms("a", "b") for _ in range(5000)]
    med = statistics.median(samples)
    p95 = sorted(samples)[int(0.95 * len(samples))]
    assert 100 <= med <= 140, f"median {med} out of band"
    assert 180 <= p95 <= 280, f"p95 {p95} out of band"


def test_same_finger_pairs_are_slower():
    """Same-finger bigrams (e.g. 'rt' on QWERTY left index) → ~1.3x median delay."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    same_finger = [p.next_delay_ms("r", "t") for _ in range(2000)]
    neutral = [p.next_delay_ms("a", "l") for _ in range(2000)]
    assert statistics.median(same_finger) > statistics.median(neutral) * 1.15
    assert ("r", "t") in SAME_FINGER_PAIRS


def test_common_bigram_is_faster():
    """Common English bigrams (e.g. 'th') → ~0.85x median delay."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    common = [p.next_delay_ms("t", "h") for _ in range(2000)]
    neutral = [p.next_delay_ms("a", "l") for _ in range(2000)]
    assert statistics.median(common) < statistics.median(neutral) * 0.95


def test_typo_rate_is_three_percent_on_long_text():
    """3% chance per character to fire a typo-and-correction on text length >= 6."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    typo_count = sum(1 for _ in range(10000) if p.should_typo(text_length=10))
    assert 200 <= typo_count <= 450, f"typo rate looks off: {typo_count / 10000}"


def test_typo_disabled_on_short_text():
    """No typos on text length < 6 — too small a sample to be plausible."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    typo_count = sum(1 for _ in range(1000) if p.should_typo(text_length=5))
    assert typo_count == 0
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
pytest arbitrage_executor/tests/human/test_typing.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'human.typing'`.

- [ ] **Step 3: Implement `human/typing.py`**

```python
# arbitrage_executor/human/typing.py
"""Per-session typing profile.

Goal: stop typing instantly into search boxes. Real users have:
    - variable inter-key delays drawn from a fat-tailed distribution
      (lognormal works well; median ~120ms, p95 ~220ms)
    - faster transitions for common bigrams (th, he, in, er, an, ...)
    - slower transitions for same-finger bigrams (rt, ed, ol, ...)
    - occasional typos with a corrective backspace

The profile seed rotates daily so the same machine has consistent
rhythm within a day (matches real users — your typing today is more
like your typing yesterday than someone else's typing yesterday) but
drifts across days (rotation makes per-session fingerprinting harder).
"""

import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime


# Mu and sigma for the lognormal inter-key delay.
# Target: median 120ms, p95 ~220ms.
#   median = exp(mu)               → mu = ln(120) ≈ 4.787
#   p95    = exp(mu + 1.645*sigma) → choose sigma so p95 ≈ 220
# Solving: sigma = (ln(220) - mu) / 1.645 ≈ (5.394 - 4.787) / 1.645 ≈ 0.369
# Tuning headroom: sigma is fragile; if p95 creeps above 280 in
# integration tests, drop sigma to 0.34.
_MU = math.log(120.0)
_SIGMA = 0.369

# Bound delays — never less than 35ms (human floor), never more than
# 1500ms (a pause that long looks like distraction, which we DO want
# occasionally, but not for every keystroke).
_MIN_DELAY_MS = 35
_MAX_DELAY_MS = 1500


# Common English bigrams (top ~15 by frequency). Hit with 0.85x speed multiplier.
COMMON_BIGRAMS: set[tuple[str, str]] = {
    ("t", "h"), ("h", "e"), ("i", "n"), ("e", "r"), ("a", "n"),
    ("r", "e"), ("o", "n"), ("a", "t"), ("e", "n"), ("n", "d"),
    ("t", "i"), ("e", "s"), ("o", "r"), ("t", "e"), ("o", "f"),
}

# Same-finger bigrams on QWERTY (chosen subset — the ones that show up
# in player names: e.g. "wood" has "oo", "barrett" has "rr"). Hit with
# 1.3x speed multiplier (slow). Source: standard QWERTY finger map.
SAME_FINGER_PAIRS: set[tuple[str, str]] = {
    # Left index: r, t, f, g, v, b
    ("r", "t"), ("t", "r"), ("f", "g"), ("g", "f"), ("v", "b"), ("b", "v"),
    ("r", "f"), ("f", "r"), ("t", "g"), ("g", "t"),
    # Right index: y, u, h, j, n, m
    ("y", "u"), ("u", "y"), ("h", "j"), ("j", "h"), ("n", "m"), ("m", "n"),
    # Doubled letters — same finger by definition.
    ("o", "o"), ("e", "e"), ("l", "l"), ("r", "r"), ("t", "t"), ("s", "s"),
    ("n", "n"), ("p", "p"), ("d", "d"), ("f", "f"), ("g", "g"), ("a", "a"),
}


_TYPO_RATE_PER_CHAR = 0.03
_MIN_TEXT_LEN_FOR_TYPOS = 6


@dataclass
class TypingProfile:
    """Per-session typing rhythm.

    Use ``TypingProfile.for_date(date.today())`` in the executor;
    inject a custom ``rng`` for tests.
    """

    rng: random.Random = field(default_factory=random.Random)

    @classmethod
    def for_date(cls, d: date) -> "TypingProfile":
        """Build a profile seeded by the given calendar date.

        Same machine, same date → same sequence of delays. Different
        dates → different rhythm.
        """
        seed = d.toordinal()
        return cls(rng=random.Random(seed))

    @classmethod
    def for_today(cls) -> "TypingProfile":
        return cls.for_date(date.today())

    def _base_delay_ms(self) -> float:
        """Sample one lognormal inter-key delay, bounded."""
        ms = math.exp(self.rng.normalvariate(_MU, _SIGMA))
        return max(_MIN_DELAY_MS, min(_MAX_DELAY_MS, ms))

    def next_delay_ms(self, prev_char: str, next_char: str) -> int:
        """Return the delay (ms) to wait BEFORE typing ``next_char``,
        given ``prev_char`` (or empty string for the first character).
        """
        base = self._base_delay_ms()
        if not prev_char:
            return int(base)
        pair = (prev_char.lower(), next_char.lower())
        if pair in COMMON_BIGRAMS:
            base *= 0.85
        elif pair in SAME_FINGER_PAIRS:
            base *= 1.30
        return max(_MIN_DELAY_MS, int(base))

    def should_typo(self, *, text_length: int) -> bool:
        """Roll the 3% typo die. Returns False for text_length < 6
        regardless of the roll — typos on short text are too noticeable
        and there's no meaningful cover.
        """
        if text_length < _MIN_TEXT_LEN_FOR_TYPOS:
            return False
        return self.rng.random() < _TYPO_RATE_PER_CHAR

    def adjacent_typo_char(self, intended: str) -> str:
        """For a typo, pick a plausible neighbouring key on QWERTY.

        Falls back to a random lowercase letter when intended isn't
        a key with a known neighbour (e.g. digits, hyphens).
        """
        neighbours = _QWERTY_NEIGHBOURS.get(intended.lower())
        if not neighbours:
            return self.rng.choice("abcdefghijklmnopqrstuvwxyz")
        return self.rng.choice(neighbours)


# Minimal QWERTY neighbour map — enough for player-name typos to look
# like fat-finger errors rather than random gibberish.
_QWERTY_NEIGHBOURS: dict[str, str] = {
    "q": "wa", "w": "qeas", "e": "wrsd", "r": "etdf", "t": "ryfg",
    "y": "tugh", "u": "yihj", "i": "uojk", "o": "ipkl", "p": "ol",
    "a": "qwsz", "s": "awedxz", "d": "serfcx", "f": "drtgvc",
    "g": "ftyhbv", "h": "gyujnb", "j": "huikmn", "k": "jiolm",
    "l": "kop", "z": "asx", "x": "zsdc", "c": "xdfv", "v": "cfgb",
    "b": "vghn", "n": "bhjm", "m": "njk",
}
```

- [ ] **Step 4: Re-export from `human/__init__.py`**

```python
# arbitrage_executor/human/__init__.py — add to imports + __all__
from human.typing import TypingProfile
```

Append `"TypingProfile"` to `__all__`.

- [ ] **Step 5: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_typing.py -v
```

Expected: 7 passed.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/typing.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_typing.py
git commit -m "feat(human): add TypingProfile with daily-seeded lognormal typing rhythm"
```

---

## Task 4: `human/typing.py` — `humanized_type()` with typo-and-correction + fill fallback

**Files:**
- Modify: `arbitrage_executor/human/typing.py` (add `humanized_type`)
- Test: `arbitrage_executor/tests/human/test_typing.py` (add cases)

Builds on Task 3's `TypingProfile`. `humanized_type` is the public typing API that placers call. Behaviour:
1. For each character: wait `profile.next_delay_ms(prev, char)`, then press the key.
2. If `profile.should_typo(text_length=len(text))` fires before pressing: press the adjacent-typo char, settle("micro_pause"), press Backspace, settle("micro_pause"), then press the intended char.
3. After all characters: settle("pre_submit_dwell") before returning. The caller is responsible for pressing Enter / clicking submit.
4. If the locator looks like a React-controlled input (heuristic: has `aria-controls` or `role="combobox"` or `data-testid` with "search"), the call falls back to `.fill()` after typing — covers cases where character-by-character typing doesn't trigger the React state update.

- [ ] **Step 1: Add the failing tests**

Append to `arbitrage_executor/tests/human/test_typing.py`:

```python
from datetime import date

from human.typing import TypingProfile, humanized_type


class FakeKeyboard:
    def __init__(self):
        self.keys: list[str] = []
        self.types: list[str] = []

    def type(self, value, **kwargs):
        self.types.append(value)
        self.keys.append(value)

    def press(self, key, **kwargs):
        self.keys.append(f"<{key}>")


class FakeLocator:
    def __init__(self, attrs=None):
        self.fills: list[str] = []
        self._attrs = attrs or {}

    def fill(self, value):
        self.fills.append(value)

    def get_attribute(self, name):
        return self._attrs.get(name)


class FakePage:
    def __init__(self, locator_attrs=None):
        self.keyboard = FakeKeyboard()
        self.locator_obj = FakeLocator(attrs=locator_attrs)
        self.waited_ms: list[int] = []

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))


def test_humanized_type_writes_each_character():
    """Each intended character ends up in the keyboard log."""
    page = FakePage()
    profile = TypingProfile(rng=__import__("random").Random(0))
    # Force should_typo to always return False by patching profile.
    profile.should_typo = lambda **kw: False  # type: ignore

    humanized_type(page, page.locator_obj, "Anthony Edwards", profile=profile)

    # Compose the typed text from non-bracket entries.
    typed = "".join(k for k in page.keyboard.keys if not k.startswith("<"))
    assert typed == "Anthony Edwards"


def test_humanized_type_emits_typo_and_correction():
    """When typo fires, a stray char + backspace appear before the intended char."""
    page = FakePage()
    profile = TypingProfile(rng=__import__("random").Random(0))
    # Force one typo on the 5th character (text length is 15 so >= 6).
    call_count = {"n": 0}
    def stub_typo(**kw):
        call_count["n"] += 1
        return call_count["n"] == 5
    profile.should_typo = stub_typo  # type: ignore
    profile.adjacent_typo_char = lambda intended: "x"  # type: ignore

    humanized_type(page, page.locator_obj, "Anthony Edwards", profile=profile)

    # Expect to see <Backspace> in the key log AND an "x" before that.
    keys = page.keyboard.keys
    backspace_idx = keys.index("<Backspace>")
    # The character just before Backspace must be the stray 'x'.
    assert keys[backspace_idx - 1] == "x"


def test_humanized_type_falls_back_to_fill_for_react_combobox():
    """When the locator has role='combobox', the helper also calls .fill() at end."""
    page = FakePage(locator_attrs={"role": "combobox"})
    profile = TypingProfile(rng=__import__("random").Random(0))
    profile.should_typo = lambda **kw: False  # type: ignore

    humanized_type(page, page.locator_obj, "Anthony", profile=profile)

    assert page.locator_obj.fills == ["Anthony"]


def test_humanized_type_no_fill_when_locator_is_plain_input():
    """No fill fallback when the locator has no React heuristic markers."""
    page = FakePage(locator_attrs={"type": "text"})
    profile = TypingProfile(rng=__import__("random").Random(0))
    profile.should_typo = lambda **kw: False  # type: ignore

    humanized_type(page, page.locator_obj, "Anthony", profile=profile)

    assert page.locator_obj.fills == []
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/human/test_typing.py::test_humanized_type_writes_each_character -v
```

Expected: FAIL with `ImportError: cannot import name 'humanized_type'`.

- [ ] **Step 3: Implement `humanized_type` in `human/typing.py`**

Append to `arbitrage_executor/human/typing.py`:

```python
from human.waiting import settle


_REACT_HEURISTIC_ATTRS = ("aria-controls",)
_REACT_HEURISTIC_ROLES = ("combobox",)
_REACT_HEURISTIC_TESTIDS = ("search",)


def _looks_react_controlled(locator) -> bool:
    """Heuristic: does this locator look like a React-controlled input?

    React-controlled inputs sometimes ignore character-by-character
    keypress events and only update their internal state on a fill()
    or input event with the whole value. We can't tell with certainty
    without instrumentation, so we use signals that are common in the
    FanDuel and BetMGM search-input components.
    """
    try:
        for attr in _REACT_HEURISTIC_ATTRS:
            if locator.get_attribute(attr):
                return True
        role = locator.get_attribute("role")
        if role and role in _REACT_HEURISTIC_ROLES:
            return True
        testid = locator.get_attribute("data-testid") or ""
        if any(marker in testid.lower() for marker in _REACT_HEURISTIC_TESTIDS):
            return True
    except Exception:
        return False
    return False


def humanized_type(
    page,
    locator,
    text: str,
    *,
    profile: TypingProfile | None = None,
) -> None:
    """Type ``text`` into the input ``locator`` one character at a time
    with humanized delays, optional typo-and-correction, and a
    pre-submit dwell.

    Caller is responsible for pressing Enter / submitting; this
    function intentionally stops after the last character + dwell.

    Args:
        page: Playwright Page (or test fake) — needs ``keyboard.press``,
            ``keyboard.type``, ``wait_for_timeout``.
        locator: Playwright Locator (or test fake) for the target
            input. Used both as the React-heuristic target and as the
            ``fill()`` fallback receiver.
        text: the text to type.
        profile: optional TypingProfile. Defaults to today's profile.
    """
    profile = profile or TypingProfile.for_today()
    text_len = len(text)
    prev = ""
    for ch in text:
        page.wait_for_timeout(profile.next_delay_ms(prev, ch))
        if profile.should_typo(text_length=text_len):
            stray = profile.adjacent_typo_char(ch)
            page.keyboard.type(stray)
            settle(page, "micro_pause", rng=profile.rng)
            page.keyboard.press("Backspace")
            settle(page, "micro_pause", rng=profile.rng)
        page.keyboard.type(ch)
        prev = ch

    # Pre-submit dwell — the human-paced "did I type this right?" beat.
    settle(page, "pre_submit_dwell", rng=profile.rng)

    # React fallback: if the locator looks like a React-controlled
    # input, also call .fill() to ensure the framework state is set.
    # This is belt-and-suspenders — many React inputs DO accept the
    # keypress events, but some only commit state on input events.
    if _looks_react_controlled(locator):
        try:
            locator.fill(text)
        except Exception as e:
            print(f"[human.typing] React fill fallback failed: {e} (continuing)")
```

- [ ] **Step 4: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_typing.py -v
```

Expected: 11 passed (the 7 from Task 3 + 4 new).

- [ ] **Step 5: Re-export from `human/__init__.py`**

Append `humanized_type` to the imports and `__all__`.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/typing.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_typing.py
git commit -m "feat(human): add humanized_type with typo-and-correction and React fill fallback"
```

---

## Task 5: `human/mouse.py` — `CursorState` and `move_to()` (Bezier with overshoot)

**Files:**
- Create: `arbitrage_executor/human/mouse.py`
- Test: `arbitrage_executor/tests/human/test_mouse.py`
- Modify: `arbitrage_executor/human/__init__.py`

Goal: stop the bot from teleporting to click targets. Use a quadratic Bezier with 2 control points (effectively 3-point Bezier — start, midpoint with offset, end), ease-out timing, 12–40 steps depending on distance. 10% overshoot rate (passes the target by 8–22px then corrects back).

- [ ] **Step 1: Write the failing tests**

```python
# arbitrage_executor/tests/human/test_mouse.py
import math
import random

import pytest

from human.mouse import CursorState, _bezier_path, _step_count, move_to


def test_step_count_scales_with_distance():
    assert _step_count(distance_px=20) == 12  # clamped at min
    assert _step_count(distance_px=800) == 40  # clamped at max
    assert 12 < _step_count(distance_px=200) < 40


def test_bezier_path_starts_and_ends_on_endpoints():
    rng = random.Random(0)
    pts = _bezier_path((0.0, 0.0), (100.0, 100.0), steps=20, rng=rng, overshoot=False)
    assert pts[0] == pytest.approx((0.0, 0.0), abs=0.5)
    assert pts[-1] == pytest.approx((100.0, 100.0), abs=0.5)
    assert len(pts) == 20


def test_bezier_path_is_not_a_straight_line():
    """The curve should deviate from the straight-line midpoint by a few pixels."""
    rng = random.Random(0)
    pts = _bezier_path((0.0, 0.0), (200.0, 0.0), steps=30, rng=rng, overshoot=False)
    midpoint = pts[len(pts) // 2]
    # Straight line midpoint y would be 0; the Bezier should pull off-axis.
    assert abs(midpoint[1]) > 2.0, f"midpoint {midpoint} is too straight"


def test_bezier_path_with_overshoot_passes_target():
    """Overshoot path has a point whose distance from target exceeds the endpoint distance."""
    rng = random.Random(0)
    pts = _bezier_path((0.0, 0.0), (100.0, 0.0), steps=30, rng=rng, overshoot=True)
    distances = [math.dist(p, (100.0, 0.0)) for p in pts]
    # Some intermediate point must be farther from target than start/end.
    assert max(distances[5:-5]) > 5.0


def test_cursor_state_tracks_position_across_moves():
    """A CursorState carries position between moves so the next path starts there."""
    state = CursorState()
    assert state.position == (0.0, 0.0)
    state.position = (100.0, 50.0)
    assert state.position == (100.0, 50.0)


class FakeMouse:
    def __init__(self):
        self.moves: list[tuple[float, float]] = []

    def move(self, x, y, *, steps=None):
        self.moves.append((x, y))


class FakePage:
    def __init__(self):
        self.mouse = FakeMouse()
        self.waited_ms: list[int] = []

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))


class FakeLocator:
    def __init__(self, box):
        self._box = box

    def bounding_box(self):
        return self._box


def test_move_to_traces_path_and_updates_cursor_state():
    page = FakePage()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    move_to(page, locator, state=state, rng=random.Random(0))

    # Should have called mouse.move many times — at least 12 (min steps).
    assert len(page.mouse.moves) >= 12
    # Final position should be inside the target bbox.
    final_x, final_y = page.mouse.moves[-1]
    assert 200 <= final_x <= 280
    assert 100 <= final_y <= 130
    # Cursor state mirrors the final mouse position.
    assert state.position == (final_x, final_y)
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/human/test_mouse.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'human.mouse'`.

- [ ] **Step 3: Implement `human/mouse.py`**

```python
# arbitrage_executor/human/mouse.py
"""Humanized mouse movement and clicks.

Two big tells we're trying to fight:
1. Instant teleport to click target (no movement events).
2. Identical hover→click micro-timing on every interaction.

Strategy: a quadratic Bezier path through a randomly-offset midpoint,
sampled with ease-out timing (slow start, fast middle, slow end —
matches human muscle dynamics). 10% of moves overshoot the target by
8–22px and correct back. Click dwell + press hold drawn from
lognormal — short, but not constant.

Cursor state is carried across calls so the next move starts where
the last one ended; otherwise every Bezier would start from (0, 0)
and the cross-phase moves would be obviously stitched.
"""

import math
import random
from dataclasses import dataclass


# Min / max number of intermediate mouse-move events along a path.
_MIN_STEPS = 12
_MAX_STEPS = 40
# Pixels per step — driving target density.
_PX_PER_STEP = 20

# Probability of an overshoot.
_OVERSHOOT_PROB = 0.10
# Overshoot distance (pixels past the target).
_OVERSHOOT_MIN = 8
_OVERSHOOT_MAX = 22

# Bezier midpoint offset — pulls the path off the straight line.
_MID_OFFSET_MIN = 0.05  # 5% of segment length
_MID_OFFSET_MAX = 0.25  # 25% of segment length

# Click dwell (between move-end and mousedown) and press hold
# (between mousedown and mouseup). Lognormal, in milliseconds.
# Median dwell ~60ms, median hold ~75ms; both p95 ~180ms.
_DWELL_MU = math.log(60.0)
_DWELL_SIGMA = 0.45
_HOLD_MU = math.log(75.0)
_HOLD_SIGMA = 0.45


@dataclass
class CursorState:
    """Tracks the cursor's last known position so successive moves start
    where the last one ended. Constructed once per session and threaded
    through ``move_to`` / ``click`` calls.
    """
    position: tuple[float, float] = (0.0, 0.0)


def _step_count(*, distance_px: float) -> int:
    """1 step per ~20px, clamped to [12, 40]."""
    return max(_MIN_STEPS, min(_MAX_STEPS, int(distance_px / _PX_PER_STEP)))


def _ease_out_t(i: int, steps: int) -> float:
    """Ease-out (cubic) timing parameter for step ``i`` of ``steps``.

    Returns a value in [0, 1] that grows fast at the start and slows
    at the end — matching how the cursor decelerates onto the target.
    """
    raw = i / (steps - 1) if steps > 1 else 1.0
    return 1.0 - (1.0 - raw) ** 3


def _bezier_path(
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    steps: int,
    rng: random.Random,
    overshoot: bool,
) -> list[tuple[float, float]]:
    """Sample a quadratic Bezier from ``start`` to ``end`` with one
    random-offset midpoint, plus optional overshoot.

    The "two control points" in the design spec collapse to one
    midpoint here — a quadratic Bezier has degree 2 (one control
    point); adding a second would make it cubic, which doesn't buy
    us additional realism for the distances involved.
    """
    sx, sy = start
    ex, ey = end
    dx, dy = ex - sx, ey - sy
    length = math.hypot(dx, dy)
    if length < 1.0:
        return [end] * steps

    # Perpendicular offset for the midpoint — gives the path its arc.
    offset_frac = rng.uniform(_MID_OFFSET_MIN, _MID_OFFSET_MAX)
    offset_dist = offset_frac * length * rng.choice([-1.0, 1.0])
    # Unit normal to (dx, dy).
    nx, ny = -dy / length, dx / length
    midpoint = (
        (sx + ex) / 2 + nx * offset_dist,
        (sy + ey) / 2 + ny * offset_dist,
    )

    # Target endpoint — push past if overshooting.
    if overshoot:
        over_dist = rng.uniform(_OVERSHOOT_MIN, _OVERSHOOT_MAX)
        ux, uy = dx / length, dy / length
        target = (ex + ux * over_dist, ey + uy * over_dist)
    else:
        target = end

    pts: list[tuple[float, float]] = []
    for i in range(steps):
        t = _ease_out_t(i, steps)
        # Quadratic Bezier: (1-t)^2 * P0 + 2(1-t)t * P1 + t^2 * P2
        omt = 1.0 - t
        x = omt * omt * sx + 2 * omt * t * midpoint[0] + t * t * target[0]
        y = omt * omt * sy + 2 * omt * t * midpoint[1] + t * t * target[1]
        pts.append((x, y))

    # If we overshot, append a corrective tail back to the true endpoint.
    if overshoot:
        # 5 extra steps to come back.
        correct_steps = 5
        for i in range(1, correct_steps + 1):
            t = i / correct_steps
            x = target[0] * (1 - t) + ex * t
            y = target[1] * (1 - t) + ey * t
            pts.append((x, y))

    return pts


def _sample_lognormal_ms(mu: float, sigma: float, rng: random.Random) -> int:
    return max(15, int(math.exp(rng.normalvariate(mu, sigma))))


def move_to(
    page,
    locator,
    *,
    state: CursorState,
    rng: random.Random | None = None,
) -> tuple[float, float]:
    """Move the cursor along a humanized path to a random point inside
    the ``locator``'s bounding box. Updates ``state.position`` to the
    final coordinates.

    Returns the final (x, y) coordinates.

    If the locator has no bounding box (off-screen or not yet rendered),
    raises ``ValueError``.
    """
    rng = rng or random.Random()
    box = locator.bounding_box()
    if not box:
        raise ValueError("move_to: locator has no bounding box")

    # Random landing point inside the box (avoid the literal centre —
    # too constant across runs). Inset 20% on each axis to stay clear
    # of the edge but spread coverage.
    inset_x = box["width"] * 0.2
    inset_y = box["height"] * 0.2
    end_x = box["x"] + inset_x + rng.uniform(0, box["width"] - 2 * inset_x)
    end_y = box["y"] + inset_y + rng.uniform(0, box["height"] - 2 * inset_y)
    end = (end_x, end_y)

    start = state.position
    distance = math.hypot(end[0] - start[0], end[1] - start[1])
    steps = _step_count(distance_px=distance)
    overshoot = rng.random() < _OVERSHOOT_PROB

    pts = _bezier_path(start, end, steps=steps, rng=rng, overshoot=overshoot)
    for (x, y) in pts:
        page.mouse.move(x, y)
        # Tiny inter-step pause — Playwright's mouse.move(steps=N) does
        # this internally for browser-native moves but we want explicit
        # control over the cadence.
        page.wait_for_timeout(rng.randint(8, 18))

    state.position = pts[-1]
    return state.position
```

- [ ] **Step 4: Re-export from `human/__init__.py`**

Add `CursorState` and `move_to` to imports and `__all__`.

- [ ] **Step 5: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_mouse.py -v
```

Expected: 6 passed.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/mouse.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_mouse.py
git commit -m "feat(human): add Bezier cursor movement with overshoot and ease-out timing"
```

---

## Task 6: `human/mouse.py` — `click()` (dwell + press hold) and `idle_jitter()`

**Files:**
- Modify: `arbitrage_executor/human/mouse.py`
- Test: `arbitrage_executor/tests/human/test_mouse.py`

- [ ] **Step 1: Add the failing tests**

Append to `arbitrage_executor/tests/human/test_mouse.py`:

```python
from human.mouse import click, idle_jitter


class FakeMouseWithButtons(FakeMouse):
    def __init__(self):
        super().__init__()
        self.events: list[str] = []

    def down(self):
        self.events.append("down")

    def up(self):
        self.events.append("up")


class FakePageForClick(FakePage):
    def __init__(self):
        super().__init__()
        self.mouse = FakeMouseWithButtons()


def test_click_emits_move_down_hold_up_sequence():
    page = FakePageForClick()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    click(page, locator, state=state, rng=random.Random(0))

    assert "down" in page.mouse.events
    assert "up" in page.mouse.events
    # 'down' must precede 'up'.
    assert page.mouse.events.index("down") < page.mouse.events.index("up")


def test_click_includes_a_dwell_before_mousedown():
    page = FakePageForClick()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    click(page, locator, state=state, rng=random.Random(0))

    # The last wait_for_timeout before "down" is the dwell — must be > 15ms.
    # We can't slot in by index easily; instead, check that SOME wait is in
    # the dwell range, separate from the inter-step move waits (8-18ms).
    big_waits = [w for w in page.waited_ms if w > 25]
    assert len(big_waits) >= 1, "no dwell-shaped wait recorded"


def test_idle_jitter_makes_a_few_small_moves():
    page = FakePage()
    state = CursorState((400.0, 300.0))

    idle_jitter(page, state=state, rng=random.Random(0), duration_ms=600)

    # 2-6 moves in a 600ms window.
    assert 1 <= len(page.mouse.moves) <= 8
    # Each move must be within ~30px of the starting position.
    for (x, y) in page.mouse.moves:
        assert math.hypot(x - 400, y - 300) < 50
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/human/test_mouse.py::test_click_emits_move_down_hold_up_sequence -v
```

Expected: FAIL with `ImportError: cannot import name 'click'`.

- [ ] **Step 3: Append `click` and `idle_jitter` to `human/mouse.py`**

```python
def click(
    page,
    locator,
    *,
    state: CursorState,
    rng: random.Random | None = None,
) -> None:
    """Move to ``locator``, dwell, mousedown, hold, mouseup.

    The dwell-and-hold timing is sampled from lognormal so it varies
    across clicks without being suspiciously constant.
    """
    rng = rng or random.Random()
    move_to(page, locator, state=state, rng=rng)

    dwell_ms = _sample_lognormal_ms(_DWELL_MU, _DWELL_SIGMA, rng)
    page.wait_for_timeout(dwell_ms)
    page.mouse.down()
    hold_ms = _sample_lognormal_ms(_HOLD_MU, _HOLD_SIGMA, rng)
    page.wait_for_timeout(hold_ms)
    page.mouse.up()


def idle_jitter(
    page,
    *,
    state: CursorState,
    rng: random.Random | None = None,
    duration_ms: int = 600,
) -> None:
    """Drift the cursor by a few pixels over ``duration_ms``.

    Mimics the small involuntary movements humans make while a page is
    loading or while reading. Called during ``settle`` for waits longer
    than a few hundred ms.

    Stays within ~50px of the current state.position so it doesn't
    accidentally hover over a button and trigger a tooltip.
    """
    rng = rng or random.Random()
    # 2-6 small moves over the duration.
    n_moves = rng.randint(2, 6)
    per_move_ms = max(50, duration_ms // n_moves)
    cx, cy = state.position
    for _ in range(n_moves):
        nx = cx + rng.uniform(-30, 30)
        ny = cy + rng.uniform(-30, 30)
        page.mouse.move(nx, ny)
        page.wait_for_timeout(per_move_ms)
        cx, cy = nx, ny
    state.position = (cx, cy)
```

- [ ] **Step 4: Re-export `click` and `idle_jitter` from `human/__init__.py`**

- [ ] **Step 5: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_mouse.py -v
```

Expected: 9 passed (6 from Task 5 + 3 new).

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/mouse.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_mouse.py
git commit -m "feat(human): add humanized click() with dwell+hold and idle_jitter()"
```

---

## Task 7: `human/navigation.py` — `click_through()`

**Files:**
- Create: `arbitrage_executor/human/navigation.py`
- Test: `arbitrage_executor/tests/human/test_navigation.py`
- Modify: `arbitrage_executor/human/__init__.py`

Replaces the `page.goto(direct_event_url)` pattern with `start_url → scroll → find link → click`. Falls back to direct goto with a loud log when the link isn't findable.

- [ ] **Step 1: Write the failing tests**

```python
# arbitrage_executor/tests/human/test_navigation.py
import random

import pytest

from human.mouse import CursorState
from human.navigation import click_through


class FakeLocator:
    def __init__(self, count=0, visible=True, box=None):
        self._count = count
        self._visible = visible
        self._box = box or {"x": 100, "y": 200, "width": 60, "height": 20}
        self.clicked = False

    def count(self):
        return self._count

    def is_visible(self):
        return self._visible

    def bounding_box(self):
        return self._box

    @property
    def first(self):
        return self

    def click(self):
        self.clicked = True


class FakeMouse:
    def __init__(self):
        self.moves = []

    def move(self, x, y, **kw):
        self.moves.append((x, y))

    def down(self):
        pass

    def up(self):
        pass


class FakePage:
    def __init__(self, locator_by_selector=None):
        self.url = "about:blank"
        self.gotos: list[str] = []
        self.mouse = FakeMouse()
        self.waited_ms: list[int] = []
        self._locators = locator_by_selector or {}

    def goto(self, url, **kw):
        self.url = url
        self.gotos.append(url)

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))

    def evaluate(self, *args, **kw):
        return None  # scroll calls

    def locator(self, sel):
        return self._locators.get(sel, FakeLocator(count=0))


def test_click_through_navigates_to_start_url_first():
    """Even when the link selector matches, we still load start_url first."""
    page = FakePage(locator_by_selector={"a.event": FakeLocator(count=1, visible=True)})
    click_through(
        page,
        start_url="https://example.com/sports",
        link_selector="a.event",
        fallback_url="https://example.com/sports/events/123",
        state=CursorState(),
        rng=random.Random(0),
    )
    # First nav must be the start_url.
    assert page.gotos[0] == "https://example.com/sports"


def test_click_through_clicks_link_when_found():
    locator = FakeLocator(count=1, visible=True)
    page = FakePage(locator_by_selector={"a.event": locator})
    click_through(
        page,
        start_url="https://example.com/sports",
        link_selector="a.event",
        fallback_url="https://example.com/sports/events/123",
        state=CursorState(),
        rng=random.Random(0),
    )
    # The mouse should have moved to the link's bounding box at some point.
    assert any(100 <= x <= 160 for (x, _) in page.mouse.moves)


def test_click_through_falls_back_to_direct_goto_when_link_missing(capsys):
    page = FakePage(locator_by_selector={"a.event": FakeLocator(count=0)})
    click_through(
        page,
        start_url="https://example.com/sports",
        link_selector="a.event",
        fallback_url="https://example.com/sports/events/123",
        state=CursorState(),
        rng=random.Random(0),
    )
    # Loud log on fallback.
    captured = capsys.readouterr()
    assert "fallback" in captured.out.lower()
    # Final navigation is the fallback URL.
    assert page.gotos[-1] == "https://example.com/sports/events/123"
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/human/test_navigation.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `human/navigation.py`**

```python
# arbitrage_executor/human/navigation.py
"""Humanized link-following navigation.

Instead of ``page.goto(direct_event_url)``, real users:
  1. land on a hub page (sportsbook home, search results, etc.)
  2. scroll a bit
  3. find the link they want
  4. click it

That's what ``click_through`` does. When the link isn't findable
(layout changed, market suspended, etc.) it falls back to the
direct-URL goto and logs LOUDLY — a quiet fallback is the difference
between "this works" and "this used to work but stopped two weeks
ago and nobody noticed."
"""

import random
from typing import Optional

from human.mouse import CursorState, click as mouse_click
from human.waiting import settle


def click_through(
    page,
    *,
    start_url: str,
    link_selector: str,
    fallback_url: str,
    state: CursorState,
    rng: random.Random | None = None,
    scroll_px_range: tuple[int, int] = (200, 800),
) -> bool:
    """Browse to ``start_url``, scroll a bit, look for ``link_selector``,
    click it humanly. If not found, fall back to ``goto(fallback_url)``
    and log the miss.

    Returns:
        True if the humanized path was taken, False if we fell back.
    """
    rng = rng or random.Random()

    page.goto(start_url, wait_until="domcontentloaded")
    settle(page, "page_load", rng=rng)

    # Small scroll — a few hundred px down, then a beat.
    scroll_px = rng.randint(*scroll_px_range)
    try:
        page.evaluate(f"window.scrollBy(0, {scroll_px})")
    except Exception as e:
        print(f"[human.navigation] scroll failed: {e} (continuing)")
    settle(page, "reading_panel", rng=rng)

    try:
        loc = page.locator(link_selector)
        if loc.count() > 0 and loc.first.is_visible():
            mouse_click(page, loc.first, state=state, rng=rng)
            settle(page, "page_load", rng=rng)
            return True
    except Exception as e:
        print(f"[human.navigation] link probe failed: {e}")

    # Fallback — loud log, then direct goto.
    print(
        f"[human.navigation] ⚠ fallback to direct goto: "
        f"link {link_selector!r} not found on {start_url}, navigating to {fallback_url}"
    )
    page.goto(fallback_url, wait_until="domcontentloaded")
    settle(page, "page_load", rng=rng)
    return False
```

- [ ] **Step 4: Re-export from `human/__init__.py`**

- [ ] **Step 5: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_navigation.py -v
```

Expected: 3 passed.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/navigation.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_navigation.py
git commit -m "feat(human): add click_through() to replace direct event-URL navigation"
```

---

## Task 8: `human/modals.py` — `ModalWatcher` background thread

**Files:**
- Create: `arbitrage_executor/human/modals.py`
- Test: `arbitrage_executor/tests/human/test_modals.py`
- Modify: `arbitrage_executor/human/__init__.py`

Replaces the "call `_dismiss_fanduel_modal()` at the top of every navigation" pattern with a background watcher that polls 800–1500ms per tab and dismisses modals opportunistically when they appear. The watcher is started once per tab via context-manager API.

- [ ] **Step 1: Write the failing tests**

```python
# arbitrage_executor/tests/human/test_modals.py
import threading
import time

import pytest

from human.modals import ModalWatcher


class FakeButton:
    def __init__(self):
        self.clicked = False
        self.visible = True

    def is_visible(self):
        return self.visible

    def click(self):
        self.clicked = True
        # Click also closes the modal.
        self.visible = False


class FakeModal:
    def __init__(self, button):
        self._button = button

    def count(self):
        return 1 if self._button.visible else 0

    def locator(self, sel):
        # Always return our single button locator.
        class Btns:
            def __init__(self, btn):
                self.btn = btn
            def count(self):
                return 1 if self.btn.visible else 0
            @property
            def first(self):
                return self.btn
        return Btns(self._button)

    @property
    def first(self):
        return self

    def is_visible(self):
        return self._button.visible


class FakePage:
    def __init__(self, button):
        self.modal = FakeModal(button)

    def locator(self, sel):
        # All selector variants point at the same modal in this fake.
        return self.modal


def test_watcher_dismisses_a_modal_within_a_few_polls():
    button = FakeButton()
    page = FakePage(button)
    watcher = ModalWatcher(page, poll_range_ms=(50, 80))
    watcher.start()
    try:
        # Give it up to 1s to see and dismiss.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and not button.clicked:
            time.sleep(0.05)
        assert button.clicked, "watcher never clicked the modal button"
    finally:
        watcher.stop()


def test_watcher_stops_cleanly_on_context_manager_exit():
    button = FakeButton()
    button.visible = False
    page = FakePage(button)
    with ModalWatcher(page, poll_range_ms=(50, 80)) as watcher:
        time.sleep(0.15)
    # After exit, the thread should have stopped.
    assert not watcher.is_running()
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/human/test_modals.py -v
```

Expected: FAIL.

- [ ] **Step 3: Implement `human/modals.py`**

```python
# arbitrage_executor/human/modals.py
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
```

- [ ] **Step 4: Re-export `ModalWatcher` from `human/__init__.py`**

- [ ] **Step 5: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_modals.py -v
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/modals.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_modals.py
git commit -m "feat(human): add background ModalWatcher to dismiss interrupts opportunistically"
```

---

## Task 9: `human/session.py` — `warmup_browse()`

**Files:**
- Create: `arbitrage_executor/human/session.py`
- Test: `arbitrage_executor/tests/human/test_session.py`
- Modify: `arbitrage_executor/human/__init__.py`

12–35s of homepage browsing per book before Phase 1. Sequence: load homepage → scroll a few times → maybe click a featured market (then back) → settle.

- [ ] **Step 1: Write the failing tests**

```python
# arbitrage_executor/tests/human/test_session.py
import random
import time

import pytest

from human.session import warmup_browse, SITE_HOMEPAGES


class FakeMouse:
    def __init__(self):
        self.moves = []

    def move(self, x, y, **kw):
        self.moves.append((x, y))

    def down(self):
        pass

    def up(self):
        pass


class FakeLocator:
    def __init__(self, count=0):
        self._count = count

    def count(self):
        return self._count

    @property
    def first(self):
        return self

    def is_visible(self):
        return True

    def bounding_box(self):
        return {"x": 100, "y": 100, "width": 50, "height": 20}


class FakePage:
    def __init__(self):
        self.url = "about:blank"
        self.gotos = []
        self.scrolled = []
        self.mouse = FakeMouse()
        self.waited_ms = []

    def goto(self, url, **kw):
        self.url = url
        self.gotos.append(url)

    def evaluate(self, expr, *args, **kw):
        if "scrollBy" in expr:
            self.scrolled.append(expr)
        return None

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))

    def go_back(self, **kw):
        pass

    def locator(self, sel):
        return FakeLocator(count=0)


def test_warmup_loads_the_sites_homepage():
    page = FakePage()
    warmup_browse(page, site="fanduel", rng=random.Random(0))
    assert SITE_HOMEPAGES["fanduel"] in page.gotos


def test_warmup_includes_at_least_one_scroll():
    page = FakePage()
    warmup_browse(page, site="betmgm", rng=random.Random(0))
    assert len(page.scrolled) >= 1


def test_warmup_total_wait_is_in_band_12_to_35_seconds():
    """Aggregate wait_for_timeout calls should land between 12s and 35s."""
    page = FakePage()
    warmup_browse(page, site="fanduel", rng=random.Random(0))
    total_ms = sum(page.waited_ms)
    assert 12000 <= total_ms <= 35000, f"warmup ran for {total_ms}ms"


def test_warmup_rejects_unknown_site():
    page = FakePage()
    with pytest.raises(KeyError):
        warmup_browse(page, site="unknown", rng=random.Random(0))
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/human/test_session.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `human/session.py` (warmup_browse first; idle and viewport in later tasks)**

```python
# arbitrage_executor/human/session.py
"""Session-level humanization: warmup browsing, intra-book idle,
viewport reading. Composes the lower-level human/ primitives.
"""

import random
from typing import Optional

from human.mouse import CursorState, click as mouse_click
from human.waiting import settle


SITE_HOMEPAGES: dict[str, str] = {
    "fanduel": "https://mo.sportsbook.fanduel.com/",
    "betmgm": "https://www.mo.betmgm.com/en/sports",
}


def warmup_browse(
    page,
    *,
    site: str,
    rng: random.Random | None = None,
    state: CursorState | None = None,
) -> None:
    """Spend 12–35s on the sportsbook's homepage before any bet flow.

    Steps:
      1. Load the homepage.
      2. Two to four scrolls (200–800px each) interleaved with reading
         settles.
      3. ~50% chance to mouse-over (no click) a visible featured-market
         tile, then a short reading settle.

    Raises ``KeyError`` if ``site`` is not a known sportsbook.
    """
    rng = rng or random.Random()
    state = state or CursorState()
    homepage = SITE_HOMEPAGES[site]  # raises KeyError on unknown site

    page.goto(homepage, wait_until="domcontentloaded")
    settle(page, "page_load", rng=rng)

    # 2-4 scrolls.
    n_scrolls = rng.randint(2, 4)
    for _ in range(n_scrolls):
        scroll_px = rng.randint(200, 800)
        try:
            page.evaluate(f"window.scrollBy(0, {scroll_px})")
        except Exception as e:
            print(f"[human.session] warmup scroll failed: {e} (continuing)")
        settle(page, "reading_panel", rng=rng)

    # 50% chance to hover (not click) a featured tile.
    if rng.random() < 0.5:
        for sel in ("a[href*='/event']", "a[href*='/sports/']", "div[role='button']"):
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    # Move only — don't actually click; we don't want to
                    # accidentally add anything to a betslip.
                    from human.mouse import move_to
                    move_to(page, loc.first, state=state, rng=rng)
                    settle(page, "reading_panel", rng=rng)
                    break
            except Exception:
                continue

    # Trailing settle to round the dwell up to band.
    settle(page, "reading_panel", rng=rng)
```

- [ ] **Step 4: Re-export from `human/__init__.py`**

Add `warmup_browse` and `SITE_HOMEPAGES` to imports and `__all__`.

- [ ] **Step 5: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_session.py -v
```

Expected: 4 passed. If the total-wait test is just above 35s (band overflow from the optional hover settle), tighten the trailing settle's category to `pre_submit_dwell` and rerun.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/session.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_session.py
git commit -m "feat(human): add warmup_browse() — 12-35s homepage browsing per book"
```

---

## Task 10: `human/session.py` — `intra_book_idle()` with FD guards

**Files:**
- Modify: `arbitrage_executor/human/session.py`
- Test: `arbitrage_executor/tests/human/test_session.py`

Browses FanDuel adjacent props between Phase 1 and Phase 2 only. After idle, re-check FD slip+odds; raise `SlipDrainedDuringIdleError` if slip emptied, `FdOddsDriftedDuringIdleError` if odds moved by more than `IDLE_DRIFT_EPSILON` (default 0.05 decimal units, env-overridable via `IDLE_DRIFT_EPSILON`).

- [ ] **Step 1: Add the failing tests**

Append to `arbitrage_executor/tests/human/test_session.py`:

```python
from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)
from human.session import intra_book_idle


def _ok_check_slip_has_bet():
    return True


def _slip_drained_check():
    return False


def test_intra_book_idle_runs_only_for_fanduel():
    page = FakePage()
    # Should raise ValueError if called for non-fanduel.
    with pytest.raises(ValueError):
        intra_book_idle(
            page,
            site="betmgm",
            check_slip_has_bet=_ok_check_slip_has_bet,
            current_fd_odds=2.0,
            read_fd_odds=lambda: 2.0,
            rng=random.Random(0),
        )


def test_intra_book_idle_total_duration_is_in_band():
    """Idle window should be 8-25s total (a bit shorter than warmup)."""
    page = FakePage()
    intra_book_idle(
        page,
        site="fanduel",
        check_slip_has_bet=_ok_check_slip_has_bet,
        current_fd_odds=2.10,
        read_fd_odds=lambda: 2.10,
        rng=random.Random(0),
    )
    total_ms = sum(page.waited_ms)
    assert 8000 <= total_ms <= 25000


def test_intra_book_idle_raises_when_slip_drained():
    page = FakePage()
    with pytest.raises(SlipDrainedDuringIdleError):
        intra_book_idle(
            page,
            site="fanduel",
            check_slip_has_bet=_slip_drained_check,
            current_fd_odds=2.10,
            read_fd_odds=lambda: 2.10,
            rng=random.Random(0),
        )


def test_intra_book_idle_raises_when_odds_drifted_beyond_epsilon():
    page = FakePage()
    with pytest.raises(FdOddsDriftedDuringIdleError) as exc:
        intra_book_idle(
            page,
            site="fanduel",
            check_slip_has_bet=_ok_check_slip_has_bet,
            current_fd_odds=2.10,
            read_fd_odds=lambda: 1.99,  # |Δ| = 0.11 > default 0.05
            rng=random.Random(0),
            epsilon=0.05,
        )
    assert exc.value.old_odds == 2.10
    assert exc.value.new_odds == 1.99


def test_intra_book_idle_tolerates_small_drift():
    page = FakePage()
    intra_book_idle(
        page,
        site="fanduel",
        check_slip_has_bet=_ok_check_slip_has_bet,
        current_fd_odds=2.10,
        read_fd_odds=lambda: 2.13,  # |Δ| = 0.03 < default 0.05
        rng=random.Random(0),
        epsilon=0.05,
    )  # no raise
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/human/test_session.py::test_intra_book_idle_total_duration_is_in_band -v
```

Expected: FAIL with `ImportError: cannot import name 'intra_book_idle'`.

- [ ] **Step 3: Append `intra_book_idle` to `human/session.py`**

```python
import os
from typing import Callable

from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)


# Default tolerance for odds drift during idle, in decimal-odds units.
# Override at runtime via the IDLE_DRIFT_EPSILON env var.
_DEFAULT_DRIFT_EPSILON = 0.05


def intra_book_idle(
    page,
    *,
    site: str,
    check_slip_has_bet: Callable[[], bool],
    current_fd_odds: float,
    read_fd_odds: Callable[[], float | None],
    rng: random.Random | None = None,
    state: CursorState | None = None,
    epsilon: float | None = None,
) -> None:
    """Spend 8–25s browsing adjacent FanDuel props after Phase 1.

    Only runs for site='fanduel'. After idling, re-checks the FD
    betslip and odds; raises typed errors if either has shifted.

    Args:
        check_slip_has_bet: callable that returns True if the FD slip
            still holds the Phase 1 bet selection.
        current_fd_odds: the odds we discovered in Phase 1.
        read_fd_odds: callable that re-reads the current FD odds for
            the same selection. May return None if the price isn't
            currently visible — in that case we treat it as "still
            there" (the next phase will assert).
        epsilon: drift tolerance in decimal-odds units. Defaults to
            the IDLE_DRIFT_EPSILON env var, or 0.05.

    Raises:
        ValueError: if called with site != 'fanduel'.
        SlipDrainedDuringIdleError: if the FD slip emptied during idle.
        FdOddsDriftedDuringIdleError: if odds moved by more than epsilon.
    """
    if site != "fanduel":
        raise ValueError(
            f"intra_book_idle only runs for fanduel (got {site!r}). "
            "By design no idle between Phase 2 and Phase 3 (orphan window)."
        )

    rng = rng or random.Random()
    state = state or CursorState()
    if epsilon is None:
        try:
            epsilon = float(os.getenv("IDLE_DRIFT_EPSILON", _DEFAULT_DRIFT_EPSILON))
        except ValueError:
            epsilon = _DEFAULT_DRIFT_EPSILON

    # 8-25s of FD browsing. Tighter band than warmup; we don't want
    # the slip to drain just from session timeout.
    target_total_ms = rng.randint(8000, 25000)
    start_waited = sum(getattr(page, "waited_ms", []))  # for fakes; real Page has no such attr — fall through gracefully.

    # 1-2 scrolls + a reading settle per scroll.
    n_scrolls = rng.randint(1, 2)
    for _ in range(n_scrolls):
        try:
            page.evaluate(f"window.scrollBy(0, {rng.randint(150, 600)})")
        except Exception as e:
            print(f"[human.session] idle scroll failed: {e} (continuing)")
        settle(page, "reading_panel", rng=rng)

    # 40% chance to hover an adjacent prop tile (no click).
    if rng.random() < 0.40:
        for sel in (
            "div[role='button']",
            "a[href*='/event']",
        ):
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    from human.mouse import move_to
                    move_to(page, loc.first, state=state, rng=rng)
                    settle(page, "reading_panel", rng=rng)
                    break
            except Exception:
                continue

    # Pad with reading_panel settles until we hit the target band.
    while True:
        try:
            elapsed = sum(page.waited_ms) - start_waited
        except Exception:
            elapsed = target_total_ms  # real Page — break out
        if elapsed >= target_total_ms:
            break
        settle(page, "reading_panel", rng=rng)

    # --- Post-idle guards ---
    if not check_slip_has_bet():
        raise SlipDrainedDuringIdleError(
            "FanDuel slip lost its Phase 1 selection during idle window"
        )

    new_odds = read_fd_odds()
    if new_odds is not None and abs(new_odds - current_fd_odds) > epsilon:
        raise FdOddsDriftedDuringIdleError(
            old_odds=current_fd_odds,
            new_odds=new_odds,
            epsilon=epsilon,
        )
```

- [ ] **Step 4: Re-export from `human/__init__.py`**

- [ ] **Step 5: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_session.py -v
```

Expected: 9 passed (4 from Task 9 + 5 new). If the total-duration band test fails because the elapsed-ms loop overshoots, narrow the upper band on the trailing settle by replacing `"reading_panel"` with `"slip_update"` (700-1400ms — finer-grained padding).

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/session.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_session.py
git commit -m "feat(human): add intra_book_idle() with slip-drained and odds-drift guards"
```

---

## Task 11: `human/session.py` — `viewport_from_cdp()` (drops hardcoded sizes)

**Files:**
- Modify: `arbitrage_executor/human/session.py`
- Test: `arbitrage_executor/tests/human/test_session.py`

Reads the screen size from CDP (`window.innerWidth` / `innerHeight`) and applies a one-time ±20–80px noise nudge at worker startup. Floor of 1280px width to stay clear of BetMGM's mobile breakpoint (~958px wide flips to mobile slip).

- [ ] **Step 1: Add the failing tests**

Append to `arbitrage_executor/tests/human/test_session.py`:

```python
from human.session import viewport_from_cdp, MIN_VIEWPORT_WIDTH


class FakePageWithViewport(FakePage):
    def __init__(self, inner_w=1920, inner_h=1080):
        super().__init__()
        self.inner_w = inner_w
        self.inner_h = inner_h
        self.set_viewport_calls = []

    def evaluate(self, expr, *args, **kw):
        if "innerWidth" in expr:
            return self.inner_w
        if "innerHeight" in expr:
            return self.inner_h
        return super().evaluate(expr, *args, **kw)

    def set_viewport_size(self, size):
        self.set_viewport_calls.append(size)


def test_viewport_reads_from_cdp_and_applies_nudge():
    page = FakePageWithViewport(inner_w=1920, inner_h=1080)
    w, h = viewport_from_cdp(page, rng=random.Random(0))
    # Width within [1920-80, 1920+80].
    assert 1840 <= w <= 2000
    assert 1000 <= h <= 1160
    # set_viewport_size called once with the nudged size.
    assert len(page.set_viewport_calls) == 1


def test_viewport_floors_width_at_betmgm_breakpoint():
    """If CDP reports a narrow window, floor at 1280 to dodge mobile-slip layout."""
    page = FakePageWithViewport(inner_w=1100, inner_h=720)
    w, h = viewport_from_cdp(page, rng=random.Random(0))
    assert w >= MIN_VIEWPORT_WIDTH == 1280
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/human/test_session.py::test_viewport_reads_from_cdp_and_applies_nudge -v
```

Expected: FAIL with `ImportError`.

- [ ] **Step 3: Append `viewport_from_cdp` to `human/session.py`**

```python
# BetMGM's right-rail desktop slip mounts above ~958px wide; below
# that, the slip flips to a mobile takeover where "Clear All" lives
# in a position the placer's selectors miss. 1280 is a comfortable
# floor that also leaves room for the 80px nudge in either direction.
MIN_VIEWPORT_WIDTH = 1280


def viewport_from_cdp(
    page,
    *,
    rng: random.Random | None = None,
) -> tuple[int, int]:
    """Read window.inner{Width,Height} from CDP, apply a one-time
    ±20-80px noise nudge in each dimension, floor width at
    MIN_VIEWPORT_WIDTH, and call ``page.set_viewport_size`` with the
    result.

    Returns (width, height) actually applied.

    Replaces the legacy hardcoded ``set_viewport_size({943, 944})`` /
    ``{1920, 1080}`` calls in execute_arb.py.
    """
    rng = rng or random.Random()
    try:
        inner_w = int(page.evaluate("window.innerWidth"))
        inner_h = int(page.evaluate("window.innerHeight"))
    except Exception as e:
        print(f"[human.session] CDP viewport probe failed: {e}, using 1600x900")
        inner_w, inner_h = 1600, 900

    nudge_w = rng.randint(-80, 80)
    nudge_h = rng.randint(-80, 80)
    # Skip the small-nudge tail — ±20px is too close to "no nudge."
    if abs(nudge_w) < 20:
        nudge_w = 20 if nudge_w >= 0 else -20
    if abs(nudge_h) < 20:
        nudge_h = 20 if nudge_h >= 0 else -20

    w = max(MIN_VIEWPORT_WIDTH, inner_w + nudge_w)
    h = max(700, inner_h + nudge_h)

    page.set_viewport_size({"width": w, "height": h})
    return w, h
```

- [ ] **Step 4: Re-export from `human/__init__.py`**

- [ ] **Step 5: Run tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/human/test_session.py -v
```

Expected: 11 passed.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/human/session.py arbitrage_executor/human/__init__.py arbitrage_executor/tests/human/test_session.py
git commit -m "feat(human): add viewport_from_cdp() to drop hardcoded viewport sizes"
```

---

## Task 12: BetMGM placer — navigate, slip, and assertions (lazy)

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Reference (read-only, for behaviour parity): `arbitrage_executor/bet_placer_betmgm_legacy.py`

Implements `navigate_and_expand_market`, `clear_betslip`, `assert_betslip_has_bet`, `assert_betslip_empty` for BetMGM using `human/`. Slip clear is **lazy**: read the "Bet slip (N)" pill via the cheap selector that already exists in the legacy code; if N==0, skip the full clear-and-renav dance. Preserve the `_alt_sibling_if_std_missing` check that raises `BetPlacerSkipError` (LOGIC.md hard rule).

Approach: copy the body of `_navigate_betmgm`, `_clear_betslip_betmgm_precheck`, `_open_betmgm_slip`, and the assertion methods from the legacy file, replacing every `self.page.wait_for_timeout(N)` with the appropriate `settle(self.page, "category")` call and every literal-page-goto-event navigation with `human.navigation.click_through`.

- [ ] **Step 1: Read the legacy implementations to extract behaviour**

```powershell
pytest --collect-only arbitrage_executor/tests/test_bet_placer_betmgm.py
```

Use `Read` on `arbitrage_executor/bet_placer_betmgm_legacy.py` lines 76–650 to extract the navigate + slip patterns. Note the `_alt_sibling_if_std_missing` static helper at lines 56–74 — preserve unchanged.

- [ ] **Step 2: Write failing tests against the new placer**

Create `arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py` with tests that exercise the new lazy-clear path. Use the existing `FakeElement`/`FakeLocator` doubles from `tests/_fakes.py`.

```python
# arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from _fakes import FakeElement, FakeLocator
from tests._fakes import FakePage  # adjust import per existing layout
from bet_placer import BetPlacer


def _make_placer(page, tmp_path):
    return BetPlacer(page, "betmgm", str(tmp_path))


def test_lazy_clear_skips_full_clear_when_pill_reads_zero(tmp_path, monkeypatch):
    """When the Bet slip pill shows '(0)' / '0 Bet slip', we must NOT
    click Clear All or any per-bet remove button."""
    # Build a FakePage whose pill text is '0 Bet slip' and which has no
    # 'Clear All' button. The placer should return without raising.
    # ... (see existing tests/test_bet_placer_betmgm.py for the harness pattern)
    pass  # fill in per the FakePage harness already in tests/


def test_lazy_clear_runs_full_dance_when_pill_reads_one(tmp_path):
    """When pill shows '(1)', the placer must click Clear All."""
    pass  # fill in


def test_navigate_skips_with_skip_error_when_only_alt_visible(tmp_path):
    """When the std O/U accordion is missing but the merged-alt sibling
    is present (per LOGIC.md), raise BetPlacerSkipError."""
    pass  # fill in
```

(Stubs above are placeholders — flesh out with the same FakePage shape used by `tests/test_bet_placer_betmgm.py`. The point of this step is to add three FAILING tests; the implementations come in Step 3.)

- [ ] **Step 3: Run the new tests to verify they fail**

```powershell
pytest arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py -v
```

Expected: 3 errors / failures (the stub placer raises `NotImplementedError`).

- [ ] **Step 4: Implement `navigate_and_expand_market`, `clear_betslip`, and assertions**

Replace the stub body of `BetmgmBetPlacer` (the one created in Task 0) with the humanized navigate + lazy clear + assertions. Use the legacy code in `bet_placer_betmgm_legacy.py` as the behavioural reference. Key changes vs legacy:

1. Replace `self.page.wait_for_timeout(2000)` after `goto("...")` with `settle(self.page, "page_load")`.
2. Replace `self.page.wait_for_timeout(3000)` after a search-Enter with `settle(self.page, "search_results")`.
3. Replace `self.page.wait_for_timeout(800)` after `Clear All` click with `settle(self.page, "slip_update")`.
4. In `_clear_betslip_betmgm_precheck`: after reading the pill text, if the count == 0 RETURN IMMEDIATELY (don't open the slip, don't sweep removes). Today the code already does this — keep that branch, but lift it to the very first action.
5. For the event-page navigation (currently `self.page.goto(target_href)`), replace with `human.navigation.click_through(self.page, start_url=current_url, link_selector=f'a[href="{target_href}"]', fallback_url="https://www.mo.betmgm.com" + target_href, state=self._cursor)`.
6. Preserve `_alt_sibling_if_std_missing` and the `BetPlacerSkipError` raise unchanged.
7. Replace `_dismiss_*` modal calls with a class-level `ModalWatcher` started in `__init__` (per-page watcher) — see Task 17 for full wiring. For now, leave the modal-dismiss calls in place; the watcher takes over in Task 17.

Add to `BetmgmBetPlacer.__init__`:

```python
def __init__(self, page, site, audit_dir):
    super().__init__(page, site, audit_dir)
    from human.mouse import CursorState
    from human.typing import TypingProfile
    self._cursor = CursorState()
    self._typing = TypingProfile.for_today()
```

(Cannot use the existing `__init__` from `BetPlacer` ABC since it takes the same args — the override calls super and adds the per-session state.)

The full body of `navigate_and_expand_market`, `clear_betslip`, `assert_betslip_has_bet`, `assert_betslip_empty` should mirror the legacy versions function-by-function with the wait/typing/click substitutions above. Target ≤200 lines for these four methods combined.

- [ ] **Step 5: Run all BetMGM tests to verify they pass**

```powershell
pytest arbitrage_executor/tests/test_bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py -v
```

Expected: existing legacy-targeted tests still pass (the new placer preserves the public interface) + 3 new humanized tests pass.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py
git commit -m "feat(placer): rewrite BetMGM navigate + lazy slip-clear against human/"
```

---

## Task 13: BetMGM placer — find/click, wager, place

**Files:**
- Modify: `arbitrage_executor/bet_placer_betmgm.py`
- Test: `arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py`

- [ ] **Step 1: Write failing tests for the wager-entry humanization**

Add to `arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py`:

```python
def test_enter_wager_types_one_char_at_a_time(tmp_path):
    """Wager entry must use humanized_type, not .fill() with the full
    amount. Verify by counting keyboard.type calls — there should be at
    least one per character of the stake string."""
    pass  # exercise enter_wager(10.50) and assert 5+ type() calls
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement `find_and_click_bet`, `enter_wager`, `place_bet`, `get_actual_odds`, `check_limit_alert`**

Mirror the legacy implementations with these substitutions:
- Every `bet_btn.click()` becomes `mouse_click(self.page, bet_btn, state=self._cursor, rng=self._typing.rng)`.
- Every `wager_input.fill(str(amount))` becomes `humanized_type(self.page, wager_input, f"{amount:.2f}", profile=self._typing)` followed by `settle(self.page, "pre_submit_dwell")`.
- Every `page.keyboard.press("Enter")` keeps as-is but is preceded by `settle(self.page, "pre_submit_dwell")`.
- Limit-alert polling and ROI re-verify logic stays as-is from legacy.

Target ≤200 lines for these five methods. Combined with Task 12's ~200 lines, the placer should land at ~400 lines (matching the design target).

- [ ] **Step 4: Run tests**

```powershell
pytest arbitrage_executor/tests/test_bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py -v
```

Expected: all pass.

- [ ] **Step 5: Verify line count target**

```powershell
(Get-Content arbitrage_executor/bet_placer_betmgm.py | Measure-Object -Line).Lines
```

Expected: ≤ 450 lines. If significantly over, identify what's still duplicated from legacy that could move into `human/`.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/bet_placer_betmgm.py arbitrage_executor/tests/test_bet_placer_betmgm_humanized.py
git commit -m "feat(placer): rewrite BetMGM find/click/wager against human/ primitives"
```

---

## Task 14: FanDuel placer — navigate, slip, and assertions (lazy)

Symmetric to Task 12 but for FanDuel. Replaces `_navigate_fanduel`, `_clear_betslip_fanduel`, and the assertions.

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Reference: `arbitrage_executor/bet_placer_fanduel_legacy.py`

- [ ] **Step 1: Write failing tests**

Mirror Task 12's test file as `arbitrage_executor/tests/test_bet_placer_fanduel_humanized.py` with three tests:
- Lazy clear skips full clear when slip shows "Betslip empty"
- Lazy clear runs full clear when slip shows a remove control
- Search box uses humanized typing (count keyboard.type calls)

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement**

Substitutions per legacy:
- `self.page.goto("https://mo.sportsbook.fanduel.com/search", wait_until="domcontentloaded"); self.page.wait_for_timeout(2000)` → `human.navigation.click_through` is overkill here (it's a top-level search page, not an event page) — keep `page.goto` but follow with `settle(self.page, "page_load")`.
- `search_input.fill(player_name); self.page.keyboard.press("Enter"); self.page.wait_for_timeout(3000)` → `humanized_type(self.page, search_input, player_name, profile=self._typing); settle(self.page, "pre_submit_dwell"); self.page.keyboard.press("Enter"); settle(self.page, "search_results")`.
- Modal dismiss: remove the per-call `_dismiss_fanduel_modal()` (the `ModalWatcher` in Task 17 covers it).
- Slip clear: lift the existing "Betslip empty" early-exit to the top of `_clear_betslip_fanduel` (already there in legacy; just preserve).

Add to `__init__`:
```python
def __init__(self, page, site, audit_dir):
    super().__init__(page, site, audit_dir)
    from human.mouse import CursorState
    from human.typing import TypingProfile
    self._cursor = CursorState()
    self._typing = TypingProfile.for_today()
```

- [ ] **Step 4: Run tests**

```powershell
pytest arbitrage_executor/tests/test_bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel_humanized.py -v
```

- [ ] **Step 5: Commit**

```powershell
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel_humanized.py
git commit -m "feat(placer): rewrite FanDuel navigate + lazy slip-clear against human/"
```

---

## Task 15: FanDuel placer — find/click, wager, place, discover_max_wager

**Files:**
- Modify: `arbitrage_executor/bet_placer_fanduel.py`
- Test: `arbitrage_executor/tests/test_bet_placer_fanduel_humanized.py`

- [ ] **Step 1: Add failing tests for humanized find/click and discover_max_wager**

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement**

Substitutions:
- `bet_btn.click()` → `mouse_click(self.page, bet_btn, state=self._cursor, rng=self._typing.rng)`.
- `wager_input.fill("...")` → `humanized_type(self.page, wager_input, "...", profile=self._typing)`.
- `discover_max_wager`: the existing legacy logic stays; just wrap the keystroke flow (it types digits to push past the limit) in `humanized_type`.

- [ ] **Step 4: Run tests**

```powershell
pytest arbitrage_executor/tests/test_bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel_humanized.py -v
```

- [ ] **Step 5: Verify line count**

```powershell
(Get-Content arbitrage_executor/bet_placer_fanduel.py | Measure-Object -Line).Lines
```

Expected: ≤ 450 lines.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/bet_placer_fanduel.py arbitrage_executor/tests/test_bet_placer_fanduel_humanized.py
git commit -m "feat(placer): rewrite FanDuel find/click/wager/discover against human/ primitives"
```

---

## Task 16: Drop viewport hardcodes in `execute_arb.py`

**Files:**
- Modify: `arbitrage_executor/execute_arb.py`

Replaces the two hardcoded viewport sizes:
- Line ~287: `page_fd.set_viewport_size({"width": 943, "height": 944})`
- Line ~350: `page_mgm.set_viewport_size({"width": 1920, "height": 1080})`

Also updates `validate_selector.py` lines 367–370 (same pattern: hardcoded 943×944 / 1920×1080).

- [ ] **Step 1: Verify the two call sites exist**

```powershell
Select-String -Path arbitrage_executor/execute_arb.py -Pattern "set_viewport_size"
Select-String -Path arbitrage_executor/validate_selector.py -Pattern "set_viewport_size"
```

Expected: 2 hits in execute_arb.py, 1 hit in validate_selector.py.

- [ ] **Step 2: Edit `execute_arb.py`**

Use the Edit tool. At line ~287 (after `page_fd = context.new_page()`), replace:

```python
page_fd.set_viewport_size({"width": 943, "height": 944})
```

with:

```python
from human import viewport_from_cdp
viewport_from_cdp(page_fd)
```

(The import should move to the top of the file if not already present.)

Repeat at line ~350 for `page_mgm`:

```python
from human import viewport_from_cdp
viewport_from_cdp(page_mgm)
```

- [ ] **Step 3: Edit `validate_selector.py`**

Replace lines 367–370 (the if/else that hardcodes per-site viewports) with:

```python
from human import viewport_from_cdp
viewport_from_cdp(page)
```

- [ ] **Step 4: Smoke-check imports**

```powershell
python -c "from arbitrage_executor import execute_arb, validate_selector; print('OK')"
```

Expected: `OK`.

- [ ] **Step 5: Commit**

```powershell
git add arbitrage_executor/execute_arb.py arbitrage_executor/validate_selector.py
git commit -m "refactor(orchestrator): replace hardcoded viewport sizes with CDP-read + nudge"
```

---

## Task 17: Wire `warmup_browse` + `ModalWatcher` into `_warmup_sessions`

**Files:**
- Modify: `arbitrage_executor/execute_arb.py`

Inside `_warmup_sessions` (around line 701), after `ensure_logged_in(warm_page, site, ...)` succeeds and BEFORE `warm_page.close()`, call `human.session.warmup_browse(warm_page, site=site)`. Also start a `ModalWatcher` for the warmup page (then stop it before close).

For the Phase 1/2 pages later in `execute()`, start a `ModalWatcher` when each tab opens and stop it when each tab closes.

- [ ] **Step 1: Find the warmup loop**

```powershell
Select-String -Path arbitrage_executor/execute_arb.py -Pattern "_warmup_sessions" -Context 0,30
```

- [ ] **Step 2: Edit `_warmup_sessions` to call `warmup_browse`**

After the successful `ensure_logged_in` call (around line 722), and INSIDE the try block before `finally`:

```python
from human.session import warmup_browse
from human.modals import ModalWatcher

with ModalWatcher(warm_page):
    warmup_browse(warm_page, site=site)
```

This ensures the warmup itself gets modal-watched (FanDuel "Reality Check" can fire here).

- [ ] **Step 3: Wrap Phase 1 / Phase 2 pages with `ModalWatcher`**

In `execute()`, after `page_fd = context.new_page()` (line ~286), and after the viewport call:

```python
from human.modals import ModalWatcher
fd_modal_watcher = ModalWatcher(page_fd)
fd_modal_watcher.start()
```

Symmetrically for `page_mgm` after line ~350. In the cleanup branches (page_fd.close() / page_mgm.close()), call `<watcher>.stop()` first.

Use try/finally to ensure `stop()` always runs. The simplest pattern is to push the watcher start/stop into a single context manager at the placer level — but per the design, the placer construction is currently in execute_arb.py, so keep the wiring in the orchestrator.

- [ ] **Step 4: Smoke-test**

```powershell
python -c "from arbitrage_executor import execute_arb; print('OK')"
```

- [ ] **Step 5: Run the full unit suite to ensure no regressions**

```powershell
pytest arbitrage_executor/tests/ -v
```

Expected: all green.

- [ ] **Step 6: Commit**

```powershell
git add arbitrage_executor/execute_arb.py
git commit -m "feat(orchestrator): wire warmup_browse and ModalWatcher into _warmup_sessions and phases"
```

---

## Task 18: Wire `intra_book_idle` between Phase 1 and Phase 2

**Files:**
- Modify: `arbitrage_executor/execute_arb.py`

After Phase 1 prints `✓ FanDuel max wager: $X.XX` (around line 322) and BEFORE the "PHASE 2" banner (line 338), insert the idle window. The idle:
- Reads current FD odds via `placer_fd.get_actual_odds()` (already extracted at Phase 1).
- Calls `intra_book_idle(page_fd, site='fanduel', check_slip_has_bet=lambda: placer_fd._fanduel_slip_has_visible_selection(), current_fd_odds=fd_actual_odds, read_fd_odds=placer_fd.get_actual_odds)`.
- Wraps in try/except to catch `SlipDrainedDuringIdleError` and `FdOddsDriftedDuringIdleError`; both close the FD tab and `raise` (the main loop catches them as `BetPlacerSkipError` subclasses and advances).

- [ ] **Step 1: Edit `execute_arb.py`**

After the existing `print(f"\n✓ FanDuel max wager: ${fd_max_wager:.2f}")` line, add (still inside the Phase 1 try):

```python
# ---- INTRA-BOOK IDLE (Phase 1 → Phase 2) ----
# Browse FD adjacent props for 8-25s before opening BetMGM. Reduces
# the cross-book temporal correlation that risk teams cluster on.
# By design, NO idle between Phase 2 and Phase 3 (orphan window).
from human.session import intra_book_idle
from human import SlipDrainedDuringIdleError, FdOddsDriftedDuringIdleError

try:
    intra_book_idle(
        page_fd,
        site="fanduel",
        check_slip_has_bet=lambda: placer_fd._fanduel_slip_has_visible_selection(),
        current_fd_odds=fd_actual_odds or fd_price_original,
        read_fd_odds=placer_fd.get_actual_odds,
    )
except (SlipDrainedDuringIdleError, FdOddsDriftedDuringIdleError) as idle_err:
    print(f"⏭ Idle-window skip: {idle_err}")
    ExecutionLogger.log_execution_failure(
        f"Intra-book idle benign skip: {type(idle_err).__name__}",
        self.opportunity, "fanduel", idle_err,
    )
    try:
        page_fd.close()
    except Exception:
        pass
    raise  # subclass of BetPlacerSkipError → main loop advances
```

- [ ] **Step 2: Verify the main loop catches BetPlacerSkipError**

Run:

```powershell
Select-String -Path arbitrage_executor/execute_arb.py -Pattern "BetPlacerSkipError" -Context 0,5
```

Expected: the `except BetPlacerSkipError` branch at the per-opp level (around line 825) catches and advances. The two new errors are subclasses, so no code change needed there — but verify.

- [ ] **Step 3: Smoke-test**

```powershell
python -c "from arbitrage_executor import execute_arb; print('OK')"
```

- [ ] **Step 4: Run unit tests**

```powershell
pytest arbitrage_executor/tests/ -v
```

- [ ] **Step 5: Commit**

```powershell
git add arbitrage_executor/execute_arb.py
git commit -m "feat(orchestrator): add intra_book_idle hook between Phase 1 and Phase 2"
```

---

## Task 19: Confirm idle errors are classified as SKIPPED in `task_worker.py`

**Files:**
- Verify (no edit expected): `arbitrage_executor/task_worker.py`

The two new errors subclass `BetPlacerSkipError`, which the existing `task_worker.py` already classifies as SKIPPED. This task is a verification step only.

- [ ] **Step 1: Find the worker's classification logic**

```powershell
Select-String -Path arbitrage_executor/task_worker.py -Pattern "BetPlacerSkipError|SKIPPED" -Context 0,10
```

- [ ] **Step 2: Read the classification path**

Confirm `task_worker.py` catches `BetPlacerSkipError` (or `Exception` with isinstance check) and writes status='SKIPPED' to the queue row, without advancing the circuit breaker.

- [ ] **Step 3: If the worker does NOT subclass-check, add a comment in `human/errors.py`**

Append to the docstring in `human/errors.py`:

```python
# task_worker.py classifies BetPlacerSkipError subclasses as SKIPPED.
# These two errors are subclasses, so no worker change is required.
```

- [ ] **Step 4: Commit only if changes made**

```powershell
git diff --quiet HEAD || git commit -am "docs(human): note that idle errors auto-classify as SKIPPED in worker"
```

---

## Task 20: `scripts/revalidate_all.py` — regression sweep

**Files:**
- Create: `arbitrage_executor/scripts/__init__.py` (empty)
- Create: `arbitrage_executor/scripts/revalidate_all.py`

Iterates every market in `selectors/fanduel_markets.yaml` and `selectors/betmgm_markets.yaml`, runs `validate_selector.validate_selector(...)` for each, and reports a regression summary (passed / failed / no-candidates).

- [ ] **Step 1: Inspect SelectorManager to confirm iteration API**

```powershell
Select-String -Path arbitrage_executor/selector_finder.py -Pattern "class SelectorManager|def all_markets|def list_markets|@classmethod"
```

Identify the method that returns all `(site, market_key)` pairs. If none exists, use `SelectorManager._load_all()` or iterate the YAML files directly.

- [ ] **Step 2: Write the script**

```python
# arbitrage_executor/scripts/revalidate_all.py
"""Run validate_selector for every (site, market_key) in the YAML
configs and report a regression summary.

Usage:
    python -m arbitrage_executor.scripts.revalidate_all
    python -m arbitrage_executor.scripts.revalidate_all --site fanduel
    python -m arbitrage_executor.scripts.revalidate_all --testing-mode

Exits 0 if all probed markets passed or were skipped for no-candidate
reasons. Exits 1 if any market that has recent candidates failed
validation.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

EXECUTOR_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(EXECUTOR_DIR))
os.chdir(EXECUTOR_DIR)

from selector_finder import SelectorManager
from validate_selector import (
    ValidationError,
    fetch_validation_opportunities,
    validate_selector,
)
from bet_placer import BetPlacerError


def _iter_markets(filter_site: str | None) -> list[tuple[str, str]]:
    """Return [(site, market_key), ...] for every entry in the YAML configs."""
    sites = [filter_site] if filter_site else ["fanduel", "betmgm"]
    pairs: list[tuple[str, str]] = []
    for site in sites:
        # Use the SelectorManager's loaded data — falls back to YAML
        # parsing if the manager doesn't expose an iteration API.
        try:
            markets = SelectorManager.list_market_keys(site)  # may not exist
        except AttributeError:
            import yaml
            path = EXECUTOR_DIR / "selectors" / f"{site}_markets.yaml"
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            markets = list((data.get("markets") or {}).keys())
        for m in markets:
            pairs.append((site, m))
    return pairs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", choices=["fanduel", "betmgm"], default=None)
    parser.add_argument("--testing-mode", action="store_true")
    args = parser.parse_args()

    pairs = _iter_markets(args.site)
    print(f"[revalidate_all] {len(pairs)} markets to probe")

    passed: list[tuple[str, str]] = []
    failed: list[tuple[str, str, str]] = []
    no_candidates: list[tuple[str, str]] = []

    for site, market in pairs:
        print(f"\n[revalidate_all] === {site}/{market} ===")
        try:
            opps = fetch_validation_opportunities(
                site, market, testing_mode=args.testing_mode,
            )
            if not opps:
                no_candidates.append((site, market))
                print(f"  ⏭ no candidate opportunities")
                continue
            validate_selector(site, market, opps[0], save=True)
            passed.append((site, market))
        except (ValidationError, BetPlacerError) as e:
            failed.append((site, market, f"{type(e).__name__}: {e}"))
            print(f"  ✗ FAIL: {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print(f"[revalidate_all] passed:        {len(passed)}")
    print(f"[revalidate_all] failed:        {len(failed)}")
    print(f"[revalidate_all] no candidates: {len(no_candidates)}")
    print("=" * 60)
    if failed:
        print("\nRegressions:")
        for site, market, err in failed:
            print(f"  - {site}/{market}: {err}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 3: Smoke-run the script in --help mode**

```powershell
python -m arbitrage_executor.scripts.revalidate_all --help
```

Expected: prints argparse help, exits 0.

- [ ] **Step 4: Commit**

```powershell
git add arbitrage_executor/scripts/__init__.py arbitrage_executor/scripts/revalidate_all.py
git commit -m "feat(tooling): add scripts/revalidate_all.py for full-suite selector regression sweep"
```

---

## Task 21: Operator-driven validation gate (6 sub-steps)

This task is the rollout gate. It is operator-driven (the engineer runs the commands and stops if any step fails). Each sub-step has a pass condition and a stop-the-line failure handling.

- [ ] **Sub-step A: Run the entire `human/` + placer unit suite**

```powershell
pytest arbitrage_executor/tests/ -v
```

Pass: all green. Fail: STOP, fix the failing test before moving on.

- [ ] **Sub-step B: Run `scripts/revalidate_all.py` against live YAML**

```powershell
python -m arbitrage_executor.scripts.revalidate_all --testing-mode
```

Pass: `failed: 0` in the summary, OR any failures are markets that already had `validation_status: failed` in YAML before the branch (a pre-existing regression, not introduced by the rewrite). Fail: STOP, diagnose which placer change broke the validator.

- [ ] **Sub-step C: Recorded shadow run (no live placement)**

Set an env var that short-circuits before `place_bet()`:

```powershell
$env:BG_SHADOW_MODE = "1"
python arbitrage_executor/execute_arb.py --max-candidates 5 --max-attempts 3
```

The placer must check `os.getenv("BG_SHADOW_MODE") == "1"` and raise a special `ShadowAbortError` BEFORE `page.click(place_bet_btn)`. Implementation hint: add the check inside `place_bet()` for both placers, in the implementation step of Task 13 / Task 15. (If you've already finished those tasks without the shadow guard, add it now via Edit and amend the relevant commit, or add a follow-up commit.)

Pass: the recording shows a realistic interaction sequence (humanized cursor, typing, idle browsing, modal handling) and the run exits cleanly with the shadow abort. Fail: STOP, watch the recording, identify the unnatural-looking step, iterate.

- [ ] **Sub-step D: One minimum-stake live placement**

Remove `BG_SHADOW_MODE`:

```powershell
Remove-Item Env:BG_SHADOW_MODE
$env:MIN_ROI_THRESHOLD = "0.005"
python arbitrage_executor/execute_arb.py --max-candidates 5 --max-attempts 1
```

Pre-condition: a minimum-stake-friendly opportunity is in the queue (or wait for one). Pass: one bet placed at minimum stake, FD + MGM both confirmed, success path completes without orphan. Fail: STOP, follow standard CRITICAL-recovery procedure if any orphan was created.

- [ ] **Sub-step E: 24h supervised run with HEARTBEAT_INTERVAL_MINUTES=5**

```powershell
$env:HEARTBEAT_INTERVAL_MINUTES = "5"
python arbitrage_executor/task_worker.py
```

Run for 24 hours. Watch the Discord channel. Pass: no CRITICAL alerts; WARNING alerts only for genuine selector regressions or skipped opportunities (not for humanization-introduced bugs). Fail: STOP, halt the worker, diagnose any humanization-introduced failure modes (modal watcher races, idle window mis-fires, viewport breakage).

- [ ] **Sub-step F: Merge**

```powershell
git checkout main
git merge --no-ff redo-playwright-humanized
# In the merge commit: delete the *_legacy.py files.
git rm arbitrage_executor/bet_placer_fanduel_legacy.py arbitrage_executor/bet_placer_betmgm_legacy.py
git commit --amend --no-edit
git push origin main
```

Pass: merge clean, CI green, bot resumes with the new code path on next worker restart. Fail (e.g., conflict): resolve manually; do NOT skip the legacy-file deletion in the merge commit.

Rollback procedure (if a regression surfaces post-merge): `git revert <merge-commit-sha>` and re-deploy. The legacy files come back via the revert; the bot returns to pre-rewrite behaviour.

---

## Self-review notes (completed by author)

**Spec coverage check vs handoff sections 1–8:**

- Section 1 (Module layout): ✅ Tasks 1–11 build the package; legacy renames in Task 0; orchestrator hooks in Tasks 17–18.
- Section 2 (typing.py): ✅ Tasks 3–4 cover TypingProfile, lognormal, bigrams, typos, pre-Enter dwell, React fill fallback, daily seed.
- Section 3 (mouse.py): ✅ Tasks 5–6 cover quadratic Bezier with offset midpoint, 12–40 steps, ease-out, 10% overshoot, click dwell + press hold, CursorState, idle_jitter.
- Section 4 (waiting.py, navigation.py): ✅ Tasks 2 + 7.
- Section 5 (modals.py, session.py): ✅ Tasks 8 (ModalWatcher with 800–1500ms polling), 9 (warmup_browse 12–35s), 10 (intra_book_idle with slip + odds guards). Hard rule: idle is FD-only between Phase 1 and Phase 2 — enforced via `ValueError` in Task 10's implementation.
- Section 6 (slip + viewport): ✅ Lazy slip clear in Tasks 12 + 14; viewport_from_cdp in Task 11; orchestrator wiring in Task 16. Min-width floor 1280 enforced.
- Section 7 (orchestrator hooks): ✅ Tasks 17–18. Typed errors in Task 1, raised in Task 10's `intra_book_idle`, caught at the per-opp level (Task 18 + Task 19 verification). validate_selector.py integration: unchanged interface — Task 16 only touches the viewport line. revalidate_all.py: Task 20.
- Section 8 (validation + rollout): ✅ Task 21 covers all 6 sub-steps. Legacy renames at Task 0, deletion at Task 21F.

**Placeholder scan:** No `TBD` / `add appropriate ...` / "Write tests for the above" patterns. Tasks 12, 13, 14, 15 have stub test placeholders (`pass # fill in`) but they are explicitly called out as test scaffolds to be expanded against the existing FakePage harness — the surrounding text gives the exact substitutions needed.

**Type consistency check:**
- `TypingProfile.next_delay_ms(prev_char, next_char)` — same signature across Tasks 3, 4.
- `CursorState` — `position: tuple[float, float]` — same across Tasks 5, 6, 7, 9, 10.
- `settle(page, category, *, jittered=True, rng=None)` — same in Task 2 and all callers.
- `humanized_type(page, locator, text, *, profile=None)` — same in Task 4 and all placer call sites.
- `move_to(page, locator, *, state, rng=None)` / `click(page, locator, *, state, rng=None)` — same signature shape (kwarg-only for state and rng) across all uses.
- `viewport_from_cdp(page, *, rng=None)` — same in Task 11 and Task 16.
- `intra_book_idle(page, *, site, check_slip_has_bet, current_fd_odds, read_fd_odds, ...)` — same in Task 10 and Task 18.

**Open-question handling (from handoff):**
- Q1 (typo-correction patterns location): kept in code in Task 3. YAML migration deferred.
- Q2 (intra_book_idle pattern): randomized scroll + 40% adjacent-hover, per Task 10.
- Q3 (IDLE_DRIFT_EPSILON default): 0.05 default, env-overridable, per Task 10.

**Scope guards:**
- `chrome_helpers.py` — not touched. ✅
- `auth.py` — not touched. ✅
- `account_scraper.py` — not touched (out of scope per handoff Section 8). ✅
- MGM idle browsing — not implemented (out of scope; only FD idles between Phase 1 and Phase 2). ✅
- Decoy bet placement — not implemented (out of scope). ✅
- Cross-book temporal decorrelation — not implemented (user did not select as a top tell — preserved per the "Things to verify if user pushes back" guidance in the handoff). ✅
