# Plan: Slim the FanDuel and BetMGM placers

## Context

PR #32 (`humanize Playwright sequence`) replaced the mechanical Playwright sequence with a humanized layer in `arbitrage_executor/human/`. The plan for that PR targeted placers at ≤450 LOC each by delegating timing / movement / modal / wait concerns into `human/`. In practice the humanization substituted in-place but the structural cleanup didn't happen — placers landed at:

- `bet_placer_fanduel.py` — **1436 LOC** (target ≤450, ~3.2× overshoot)
- `bet_placer_betmgm.py` — **1601 LOC** (target ≤450, ~3.6× overshoot)
- Combined: 3037 LOC

The bulk comes from repeated patterns inside individual methods, not from new behavior. The reviewer's complaint at PR #32 was that the parallel `*_legacy.py` files plus the unslimmed live placers made it hard to tell new behavior from preserved behavior at a glance. The legacy files are gone (deleted in `c6f56ca`); this PR is the structural cleanup that should have shipped with PR #32.

**Goal**: cut combined placer LOC by ~30-35% (target ~2000 total) without behavior change. Fuller compression to the 450-LOC plan target requires architectural moves (template-method ABC, per-site config tables) that are out of scope here.

## Approach

Extract the four highest-yield duplicated patterns into shared helpers in a new `arbitrage_executor/_placer_helpers.py` (sibling of the existing `_bet_placer_helpers.py`). Apply at every call site in both placers. Decompose the two largest single methods (`_enter_wager_fanduel`, `navigate_and_expand_market` in BetMGM) into discrete phase-methods so the linear control flow becomes scannable.

No public-API changes. No behavioral changes. Tests must stay green at every step.

### Extraction targets (ranked by yield)

#### 1. Selector-cascade with first-visible-pick — ~150 LOC saved

Pattern repeated 12+ times across both placers:

```python
for sel in (SEL_A, SEL_B, SEL_C):
    try:
        loc = self.page.locator(sel)
        if loc.count() > 0 and loc.first.is_visible():
            return loc.first  # or click, or break
    except Exception:
        continue
```

Extract:
```python
def first_visible(page, selectors: Iterable[str]) -> Optional[Locator]:
    """Return the .first of the first selector whose count>0 and is_visible.
    None if nothing matches. Swallows per-selector Locator errors so
    a malformed selector doesn't block the cascade."""
```

Call sites to update (verified via grep):
- `bet_placer_fanduel.py`: 481-492 (slip-has-visible-selection probes), 660-700 (find_and_click standard path's visibility filter), 870-920 (wager input probes), 1180-1230 (place-bet button probes), 1310-1350 (odds extraction probes).
- `bet_placer_betmgm.py`: 367-410 (accordion search), 489-510 (Show More cascade — already inside `_click_show_more_repeatedly_betmgm`, can use the same helper), 686-720 (slip-open probes), 1471-1500 (close-slip probes), 1500-1535 (odds extraction).

Estimated savings: **~150 LOC**.

#### 2. Diagnostic-dump on selector miss — ~120 LOC saved

Pattern at every "not found" raise:

```python
try:
    aria_loc = self.page.locator(f'[aria-label*="{player_name}"]')
    aria_dump = []
    for i in range(min(aria_loc.count(), 10)):
        try:
            aria_dump.append(aria_loc.nth(i).get_attribute("aria-label"))
        except Exception:
            continue
    print(f"[FANDUEL] aria-labels mentioning {player_name!r} ({len(aria_dump)}): {aria_dump!r}")
except Exception:
    pass
# ... another 30 LOC for buttons, another 30 for picks ...
self._screenshot("bet_not_found")
raise BetPlacerError(...)
```

Extract:
```python
def dump_miss_context(
    page,
    *,
    site: str,
    player_name: str,
    extra_locators: Iterable[tuple[str, str]] = (),
) -> None:
    """Print up to 10 matches for each of:
       - aria-label*="<player>"
       - button:has-text("<player>")
       - any caller-supplied (label, selector) tuples (e.g. ms-event-pick)
    Swallows all internal errors — diagnostic output must never crash
    the failure path it's annotating."""
```

Call sites (5 fanduel + 4 betmgm):
- `bet_placer_fanduel.py`: 575-625 (find_and_click std miss), 740-790 (alt miss), one more in `_place_bet_fanduel`.
- `bet_placer_betmgm.py`: ~1100-1140 (std pick miss), ~1230-1260 (alt pick miss), accordion miss inside `_expand_accordion_betmgm`.

Estimated savings: **~120 LOC**.

#### 3. Slip-state polling with verbose retry — ~90 LOC saved

Both placers have polling loops like:

```python
for attempt in range(N):
    settle(self.page, "slip_update", rng=self._typing.rng)
    if condition():
        break
    if attempt == N-1:
        self._screenshot("slip_poll_timeout")
        raise BetPlacerError(...)
```

Extract:
```python
def poll_until(
    *,
    page,
    rng,
    condition: Callable[[], bool],
    attempts: int,
    settle_category: str,
    on_timeout_screenshot: str,
    on_timeout_message: str,
) -> None:
    """Settle-and-check loop with screenshot + raise on timeout."""
```

Call sites: 3 in FD (slip-confirm after click, wager-set verify, place-bet outcome), 3 in BetMGM (same trio).

Estimated savings: **~90 LOC**.

#### 4. Try-screenshot-raise wrapper — ~40 LOC saved

Pattern:
```python
try:
    <do something>
except Exception as e:
    self._screenshot("<tag>")
    raise BetPlacerError(f"<message>: {e}")
```

Extract a context manager:
```python
@contextmanager
def with_screenshot_on_error(placer, tag: str, message: str):
    try:
        yield
    except BetPlacerSkipError:
        raise  # never swallow structural skips
    except BetPlacerError:
        raise  # already classified
    except Exception as e:
        placer._screenshot(tag)
        raise BetPlacerError(f"{message}: {e}") from e
```

Call sites: ~8 across both files. Light-touch but consistent.

Estimated savings: **~40 LOC**.

### Method decomposition (no shared extraction, just structure)

#### 5. `_enter_wager_fanduel` (241 LOC → ~120 LOC across 3 methods)

`bet_placer_fanduel.py:866-1107`. Reads as one long linear procedure. Decompose into:
- `_find_wager_input_fanduel(self) -> Locator` — the input-locator cascade (~30 LOC)
- `_type_wager_with_react_fallback(self, locator, amount) -> bool` — the humanized-type + .fill() fallback dance (~50 LOC)
- `_verify_wager_set_fanduel(self, locator, amount) -> bool` — the post-type verification + retry (~40 LOC)

`_enter_wager_fanduel` becomes the 3-line orchestrator. Total ~120 LOC, no behavior change.

#### 6. `navigate_and_expand_market` (BetMGM, 198 LOC → ~80 LOC across 4 methods)

`bet_placer_betmgm.py:135-333`. Already has helper methods for sub-tab and accordion expansion, but the main body still does homepage-loading + search + event-anchor scan + nav inline. Decompose into:
- `_load_betmgm_homepage(self, sport: str) -> None`
- `_search_betmgm_for_event(self, home_team: str, away_team: str) -> None`
- `_navigate_to_event_page_betmgm(self, home_team: str, away_team: str) -> None`

The existing `_select_market_sub_tab_betmgm` and `_expand_accordion_betmgm` stay as-is. Total `navigate_and_expand_market` body becomes ~30 LOC, the four step-methods carry the detail.

### LOC summary

| Source | Before | After (estimated) |
|---|---|---|
| FD placer | 1436 | ~950 |
| BetMGM placer | 1601 | ~1050 |
| `_placer_helpers.py` (new) | 0 | ~150 |
| **Total** | **3037** | **~2150** |
| Reduction | — | **-29%** |

Falls short of the original ≤450 per-placer target, but realistic for a no-behavior-change refactor. Closing the remaining gap requires moving site-specific selectors into config tables (a separate, bigger PR).

## Critical files

**Modify:**
- `arbitrage_executor/bet_placer_fanduel.py` — main slim target
- `arbitrage_executor/bet_placer_betmgm.py` — main slim target

**Create:**
- `arbitrage_executor/_placer_helpers.py` — `first_visible`, `dump_miss_context`, `poll_until`, `with_screenshot_on_error`
- `arbitrage_executor/tests/test_placer_helpers.py` — unit tests for the four helpers

**Reference (read, don't modify):**
- `arbitrage_executor/_bet_placer_helpers.py` — existing helpers for cross-cutting concerns (screenshot, etc.); naming pattern to mirror
- `arbitrage_executor/human/__init__.py` — for the existing pattern of extracting cross-cutting concerns
- `arbitrage_executor/bet_placer.py` — ABC + custom exception classes (do not modify the public surface)

**Do not touch:**
- `arbitrage_executor/human/*` — already factored cleanly; LOC there is intentional
- `arbitrage_executor/chrome_helpers.py` — frozen per project memory
- `selectors/*.yaml` — selector data is configuration, not code; not part of this slim
- The test files for FD and BetMGM placers — should pass unchanged. If they break, the refactor changed behavior.

## Execution order

Land in five small commits, each independently green and revertable:

1. **commit 1**: add `_placer_helpers.py` with the four helpers + unit tests. No call-site changes yet. Pure addition.
2. **commit 2**: apply `first_visible` at all 10 identified call sites. Largest LOC drop in this commit; biggest test-regression risk if anything's behaviorally off. Run full suite.
3. **commit 3**: apply `dump_miss_context` at the 9 miss-raise sites. Lower risk (failure paths only); easy to verify by intentionally provoking misses in tests.
4. **commit 4**: apply `poll_until` + `with_screenshot_on_error` at remaining sites.
5. **commit 5**: decompose `_enter_wager_fanduel` and `navigate_and_expand_market`. Pure rearrangement; tests guard.

After each commit: `python -m pytest tests/ -q` must show 137 passing (or more if helpers added tests).

## Verification

1. **Unit suite green at every step.** `python -m pytest tests/ -q` — 137 currently passing (post-PR #32 cleanup, post-`c6f56ca`). If the helpers add tests, count rises; never falls.

2. **Selector regression sweep against live Chrome.** After commit 5, before merge: `python -m scripts.revalidate_all --testing-mode`. Compare regression count against sweep #2 baseline (8 real regressions, the rest were SkipError-misclassified-as-FAIL). Net regression count must not grow.

3. **One shadow live run.** `$env:BG_SHADOW_MODE = "1"; python execute_arb.py --max-candidates 5 --max-attempts 3`. Worker classifies as SKIPPED (per ShadowAbortError BetPlacerSkipError parenting in PR #32). No silent orphans.

4. **Diff scannability check.** Open the final diff and verify that, in the placers, every method body is short enough to read in one screen. If a method is over ~80 LOC, it shouldn't have made the cut.

## Worst case if shipped wrong

The four helper extractions either work (every test passes, every revalidate target unaffected) or fail loudly (broken selector cascade = "no element found" raises, broken polling = timeout raises). No silent behavioral drift is plausible from a no-public-API refactor.

The method decompositions in step 5 carry the most subtle risk: a misplaced state read between extracted methods could mask a timing dependency that the linear original handled by accident. Mitigation: keep the decomposition mechanical — extract contiguous blocks, don't reorder anything, don't change variable scopes. The tests should catch this anyway since `enter_wager` is heavily exercised in `tests/test_bet_placer_fanduel_humanized.py`.

If a regression slips through to live Chrome, the revalidate sweep (step 2 of Verification) catches it before merge — and if it doesn't, the rollback is one PR revert with no schema or external-state implications.
