# Deterministic bet matching (fuzzy only for player rows)

**Date:** 2026-05-29
**Status:** Design approved, pending spec review
**Scope:** Spec 1 of 3 in the DraftKings-expansion track. This spec is foundational and ships first.

## Context

The arbitrage executor (`arbitrage_executor/`) places hedged FanDuel↔BetMGM player-prop
pairs via Playwright/CDP. Bet *selection* — choosing which on-screen pick to click for a
given player / market / line / side — currently relies on **loose, substring-based matching
with confidence ranking**, and is not reliably working end-to-end. The defects directly risk
clicking the wrong line or the wrong side (real money, asymmetric orphan risk).

This is the first of three specs. The other two (book-agnostic executor generalization; the
DraftKings bet-placer) depend on reliable, deterministic matching as their baseline — you
cannot verify a generalization via "FanDuel/BetMGM regression" while the matching layer is the
thing that's broken. Hence: fix matching first.

### Confirmed defects (grounded in current code)

- **Substring line matching, BetMGM** — `bet_placer_betmgm.py:_click_betmgm_pick_for_player`
  builds `target_text = f"{direction_letter} {line}"` (`:1146`) and an integer-alt form
  `f"{direction_letter} {int(line)}"` (`:1148`), then matches with `target_text not in norm`
  (`:1185`). `"O 1"` is a substring of `"O 11.5 2.00"`, so an integer line of 1 matches 11.5 /
  10.5 / etc. It takes the **first** substring hit (`break` at `:1224`) with no uniqueness
  check.
- **Substring line matching, FanDuel** — `selector_finder.py:find_candidates_by_text` filters
  with `str(line) not in aria_label` (`:126`) and builds selectors containing
  `[aria-label*="{line}"]` (`:150`). `"1.5"` is a substring of `"11.5"`; float formatting also
  bites (`str(25.0)` → `"25.0"` vs a DOM `"25"`).
- **Wrong-side fallback, FanDuel** — `bet_placer_fanduel.py:find_and_click_bet` (`:582-586`):
  when direction filtering yields nothing it prints "Could not filter by direction, using first
  candidate" and clicks `candidates[0]` — which can be the **wrong side**.
- **Guess-and-rank architecture** — `find_candidates_by_text` runs four "strategies" each
  emitting `SelectorCandidate`s with confidence tiers (70/80/85/90/95/98); the caller picks the
  best. This heuristic *is* the fuzzy lookup to remove.
- **Substring market terms** — `aria-label*="{term}"` (`:111`) and `button:has-text("{term}")`
  (`:167`): `"Points"` matches "Player Points", "Points Q1", "Rebounds+Points".

Player-name matching is already correctly fuzzy and is the **only** thing that should stay
fuzzy: `text_match.fuzzy_contains` (rapidfuzz `partial_ratio`, threshold 85; selector_finder
and BetMGM use 90). `text_match.py`'s own docstring already states the intended rule
("Markets, directions, books, lines … are matched EXACTLY — never fuzzy"); the code does not
yet honor it.

## Goal

FanDuel and BetMGM select the correct market/line/side **deterministically** and place
reliably end-to-end. Fuzzy matching is confined to locating the **player's row**. Market, line,
side, and threshold are matched **exactly**. Selection asserts **exactly one match or raises** —
no "first match", no confidence ranking, no silent fallback. A miss is surfaced loudly as a
halt-and-investigate signal, never masked.

### Out of scope
- Book-agnostic executor generalization (Spec 2) and the DraftKings bet-placer (Spec 3).
- Navigation / accordion-expansion / "Show More" / search logic — only pick **selection**
  changes. (The matcher operates on picks already rendered by existing navigation.)
- Chrome/CDP launch and login (`auth.py`, `chrome_helpers.py`).
- Wager entry, limit alerts, place-bet confirmation parsing.

## Architecture: one shared matching contract

A new module **`pick_matcher.py`** holds the *only* line/side/threshold decision logic. Both
placers — and later DraftKings — route their final pick decision through it. Each placer keeps
its book-specific DOM traversal:

- FanDuel: aria-label-bearing tiles located near/within the player.
- BetMGM: `ms-event-pick` elements + row-scoped player resolution
  (`_player_name_for_pick`, `_nearby_row_texts_for_pick`, `_WALKUP_JS`).

…but hands the candidate pick text/elements to `pick_matcher` for the exact decision. The
guess-and-rank path (`SelectorCandidate.confidence`, `find_candidates_by_text`) is retired.

```
find_and_click_bet(opportunity, direction, market_config)
  1. market   → navigate/expand by EXACT identity        (existing nav; unchanged)
  2. player   → fuzzy_contains locates the row            (ONLY fuzzy step)
  3. pick     → pick_matcher.select_unique(row picks,     (EXACT line+side / threshold)
                  target_line, target_side)               (exactly one or raise)
  4. click    → via stable attribute selector             (data-test-option-id / precise aria)
```

## Component: `pick_matcher.py` (new)

Pure, browser-free functions (unit-testable in isolation):

```python
@dataclass(frozen=True)
class Pick:
    side: str          # "over" | "under"
    line: float
    odds: float | None # decimal odds if present in the text

def parse_pick(text: str) -> Pick | None:
    """Strict tokenizer. Captures the FULL numeric token, so '11.5' -> 11.5
    (never 1.5). Normalizes side from Over/Under/O/U. Returns None when the
    text is not a pick (so callers skip non-pick elements explicitly)."""

def parse_threshold(text: str) -> int | None:
    """Exact 'N+' threshold so '5+' != '15+'. Also maps FanDuel MLB
    threshold-one labels ('To Hit A Single' / '2+ Stolen Bases') to their
    integer threshold."""

def line_equals(a: float, b: float) -> bool:
    """Numeric equality, abs(a-b) < 1e-6. Never substring."""

def select_unique(items, target_line, target_side, *, threshold=False):
    """items: iterable of (element, text). Filter by exact line+side (or exact
    threshold when threshold=True). Return the single matching item.
    Raise AmbiguousPickError if >1, NoPickError if 0. No 'first match' fallback."""
```

Errors `AmbiguousPickError` and `NoPickError` subclass the existing `BetPlacerError`
hierarchy (`errors.py`) so callers/worker treat them as ordinary bet failures (and the orphan
logic is unaffected — these all fire before any bet is placed).

The numeric tokenizer is the crux: a regex that anchors the side token and captures a complete
decimal/integer line as one group (e.g. `^(?:(over|under|o|u)\b)\s*([0-9]+(?:\.[0-9]+)?)`),
applied to normalized whitespace. This is what kills the `"1.5" ⊂ "11.5"` and `"O 1" ⊂ "O 11.5"`
classes uniformly across all books.

## Changes by file

### `selector_finder.py`
- **Remove** `find_candidates_by_text` and the `SelectorCandidate` confidence model (substring
  market terms, substring line, 4-strategy ranking, direction-by-text-inference).
- **Keep** unchanged: `is_alternate_market`, `get_base_market_key`, `calculate_alternate_tab_value`,
  and the YAML `SelectorManager` (`load_market_config` / `get_market` / `has_market` /
  `is_market_executable` / `save_market_config`).
- If a deterministic player-row finder proves shareable across FD/BetMGM, add it here; otherwise
  row-finding stays per placer (BetMGM already has bespoke row logic).

### `bet_placer_fanduel.py`
- Standard path (`find_and_click_bet`, `:560-628`): locate the player's tiles, `parse_pick`
  each aria-label, `select_unique(..., target_line, target_side)`, click the resolved exact
  element. **Delete** the "use first candidate" direction fallback (`:582-586`) and the
  `[aria-label*="{line}"]` substring selector.
- Alternate path (`_find_and_click_alternate_bet_fanduel`, `:630+`): replace "first visible
  candidate wins" with `parse_threshold` + `select_unique(..., threshold=True)`.
- Keep the visible-element guard, humanized `mouse_click`, and slip-phase viewport pin as-is.

### `bet_placer_betmgm.py`
- `_click_betmgm_pick_for_player` (`:1124`): replace the substring `target_text` matching
  (`:1146-1188`) with `parse_pick` per `ms-event-pick` + exact `line_equals` + exact side,
  scoped to the fuzzy-matched player row, then `select_unique` across that row's picks (raise
  on >1). Keep clicking via the stable `data-test-option-id` selector (`:1241-1246`).
- Alt-Yes / threshold-tab path (`_click_betmgm_alt_yes_pick_for_player`, `:1255+`): exact `"N+"`
  via `parse_threshold`.
- Keep player-row resolution (`_player_name_for_pick`, `_nearby_row_texts_for_pick`,
  `_WALKUP_JS`) and the lazy-scroll coax — these are navigation/row concerns, not matching.

## Failure diagnostics (honor "don't mask selector misses")

When `select_unique` finds 0 or >1 matches, the placer:
1. logs the row's **parsed picks** (side / line / odds) so the mismatch is legible,
2. calls the existing `dump_miss_context` + `_screenshot("bet_not_found")`,
3. raises `NoPickError` / `AmbiguousPickError` (structured `BetPlacerError`).

It never advances to another pick or guesses. Per the operator runbook this becomes a WARNING
(no money at risk — selection fails before placement), and repeated identical misses are a
selector regression to investigate (`SOP.md`).

## Testing

- **Unit — `tests/test_pick_matcher.py` (new).** The bug classes, browser-free:
  `1.5` vs `11.5`; `O 1` vs `O 11.5`; `5+` vs `15+`; integer vs decimal lines; `Over/Under/O/U`
  normalization; `parse_threshold` on MLB threshold-one labels; `select_unique` raises on
  ambiguous/empty; non-pick text → `parse_pick` returns `None`.
- **Placer tests.** Extend `tests/test_bet_placer_fanduel*.py` and
  `tests/test_bet_placer_betmgm*.py` (using `tests/_fakes.py`) to assert exactly-one-or-raise
  and that the wrong-side fallback is gone.
- **End-to-end.** `WAGER_SCALE_FACTOR=0.01 .venv/Scripts/python.exe execute_arb.py
  --max-attempts 1` against a live FanDuel↔BetMGM qualifier; confirm the **correct** line and
  side landed on the slip via the audit screenshots in `audit_logs/{ts}_{player}_{market}/`.
- **Regression.** Existing humanized / sequencing tests stay green
  (`test_bet_placer_*_humanized.py`, `test_bet_placer_sequencing.py`).

## Risks & trade-offs

- **Precision over recall (intended).** Markets whose tiles lack a parseable line/threshold or a
  stable attribute will now **raise** rather than risk a wrong click. This surfaces real selector
  gaps (YAML/selector work) instead of silently mis-betting. This is the deliberate trade in the
  money path.
- **FanDuel aria-label format dependency.** The tokenizer assumes FD encodes side+line in the
  aria-label (as the current substring code already relies on). Where it doesn't, the pick
  fails loud — acceptable, and diagnosable from the parsed-picks dump.
- **No change to navigation.** If a market's accordion/search doesn't render the right picks,
  that's a separate (navigation) failure mode; this spec only guarantees correct *selection*
  among rendered picks.

## Definition of done

- `pick_matcher.py` exists with the API above and full unit coverage of the bug classes.
- `find_candidates_by_text` / `SelectorCandidate` confidence model removed; no substring line or
  market matching remains; `fuzzy_contains` used only for player-row location.
- Both placers select via `pick_matcher` with exactly-one-or-raise; FanDuel wrong-side fallback
  deleted.
- FanDuel and BetMGM each place a correct bet end-to-end on a live qualifier (verified via audit
  screenshots), and the test suite is green.
