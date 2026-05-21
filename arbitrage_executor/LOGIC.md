# LOGIC: Std vs Alt Markets — Direction, Suffixes, and Pairing

**Read this before debugging any "no bet found" / "wrong accordion" / "alt-only can't take direction='under'" failure.** This is the mental model the whole bot is built on. Past sessions have re-derived it incorrectly multiple times.

---

## The headline rule

> The **`side`** column in stage data is authoritative for direction (over vs under). The **`_alternate`** suffix only tells you which UI tile to click — NOT which side. An alt-over and a std-under at the same line are a valid pair across books.

Two corollaries you must internalize:

1. **`side` and `_alternate` are independent dimensions.** A row's direction comes from `side` (`'over'`/`'under'`). A row's UI surface comes from `market_key` (with or without `_alternate` suffix). Do not collapse them.
2. **Across books, std rows pair with alt rows freely.** The arb builder joins on `canonical_market` (strips `_alternate`) + same `line` + opposite `side`. FD's `_alternate` Over at line 5.5 pairs with BetMGM's std Under at line 5.5 just fine — they're at the same line and opposite directions.

---

## How BetMGM actually structures its UI

| Market type | Accordion name (BetMGM) | Inside | API row (Odds API → stage table) |
|---|---|---|---|
| **Std** | `Player <stat> O/U` (e.g. `Player rebounds + assists O/U`) | Both **Over** and **Under** picks at one specific line (`O 5.5  1.89` / `U 5.5  1.91`). | `market_key='player_<stat>'`, `side='over'` AND `side='under'`, same `line` |
| **Alt** | `Player <stat>` (no suffix — the merged form, e.g. `Player rebounds + assists`) | Threshold tabs (`5+`, `7+`, `10+`, ...) with one **Yes <price>** pick per player per threshold. Yes means "over the threshold." | `market_key='player_<stat>_alternate'`, `side='over'` only, `line = threshold − 0.5` |

Notes:

- **The two accordions are siblings on the same event page** when BetMGM ships both. They are NOT alternative renderings of the same market. They're separate tiles with separate pricing.
- **BetMGM ships per-game variance.** A high-profile playoff event (e.g. Thunder@Spurs) usually has both std `O/U` and alt-merged accordions. A lower-traffic event (e.g. some Cavs@Knicks tonight) may have only the merged-alt accordion. Mid-game, BetMGM sometimes suspends the std accordion and leaves only alt.
- **The API is correct.** When BetMGM offers std Over+Under, the Odds API ships both. When the std accordion isn't on the live page for a particular event, the API may still ship the std under-side price (derived from implied probability or available via a different UI surface like the BetMGM app). The pipeline is right to trust the API; the executor has to be defensive about whether the UI actually carries that tile.

---

## The Odds API → stage table mapping

For a given `(event_id, player_name, line=5.5)`:

```
bookmaker_key=fanduel  market_key=player_rebounds_assists            side=over  → FD std Over 5.5
bookmaker_key=fanduel  market_key=player_rebounds_assists            side=under → FD std Under 5.5
bookmaker_key=fanduel  market_key=player_rebounds_assists_alternate  side=over  → FD alt "Yes 6+" (Yes pick on the "6+" tile)
bookmaker_key=betmgm   market_key=player_rebounds_assists            side=over  → BetMGM std Over 5.5
bookmaker_key=betmgm   market_key=player_rebounds_assists            side=under → BetMGM std Under 5.5
bookmaker_key=betmgm   market_key=player_rebounds_assists_alternate  side=over  → BetMGM alt "Yes 6+"
```

**Rows that must not exist** (and don't, verified against stage data):

- Any `market_key='*_alternate'` with `side='under'`. BetMGM's alt-merged accordion has no "No" or "Under" pick — it's Yes-only. The API would only ship this if a book invented an alt under-side, which none do.

---

## The arb builder's pairing rule

`airflow/dags/bg_arb_pipeline_lib/builder.py:46–59`:

```python
work["canonical_market"] = work["market_key"].astype(str).map(_strip_alt_suffix)
unders = work[work["side"] == "under"].copy()
overs  = work[work["side"] == "over"].copy()
merged = unders.merge(overs, on=["event_id","player_name","canonical_market","line"], suffixes=("_u","_o"))
merged = merged[merged["bookmaker_key_u"] != merged["bookmaker_key_o"]]
```

- Strips `_alternate` to get `canonical_market`, so std and alt rows of the same stat share a join key.
- Splits by `side`. Cross-joins unders × overs on the same `(event, player, canonical_market, line)`.
- Filters out same-book pairs.

What gets emitted:

| Pairing | Notes |
|---|---|
| **std × std** | Both books' std O/U accordions at the same line. Normal arb. |
| **std × alt** | One book std-under (line X.5) × other book alt-over (`X+1+` threshold). The alt-over price expresses the same outcome as the std over at line X.5 — pairing them across books is a valid arb. |
| **alt × std** | Symmetric to above. |
| **alt × alt** | **Impossible** — alts never ship an under side, so the `unders` df has no alt rows. |

**The builder is not the source of the Mikal Bridges bug.** It correctly emits std×std when the API ships both sides. The bug was downstream (executor YAML routing).

---

## The executor's accordion selection rule

For each opp leg, the executor looks up the YAML entry by `under_market_key` / `over_market_key`:

| Leg's market_key | YAML entry → accordion |
|---|---|
| `player_<stat>` (std) | `accordion_name: Player <stat> O/U` (the std accordion) |
| `player_<stat>_alternate` (alt) | `accordion_name: Player <stat>` (the merged-alt accordion) + `has_threshold_tabs: true` |

When the std accordion **is not on the live page** for this event:

1. The bot must **not** click into the merged-alt accordion as a fallback. Direction='under' on a Yes-only accordion is impossible to satisfy.
2. The bot must raise `BetPlacerSkipError` (subclass of `BetPlacerError`). The task worker classifies this as `SKIPPED`, not `FAILED`. The circuit breaker does not advance.
3. The opp is logged but no Discord alert fires (it's a benign skip, not a regression).

This is the post-fix behavior. The pre-fix behavior (PR #15 era) routed std market_keys to the merged-alt accordion directly, which produced the misclick attempts that PR #16/#17 then had to refuse — making the failure mode loud but ugly.

---

## Common confusions, with the right answer

| You might think | Reality |
|---|---|
| "BetMGM dropped std O/U for NBA combo markets" | **No** — they ship per-event. Don't generalize from one game. |
| "The `_alternate` suffix means the side is implicitly Over" | No. The suffix names the UI tile. `side` names the direction. They're independent dimensions. |
| "If the API ships BetMGM std under at line 5.5 but the UI doesn't have the O/U accordion tonight, the API is wrong" | The API is **correct** as a price feed. The UI/API divergence is real but small; the executor handles it by skipping. |
| "We should fix this in the builder by filtering phantom arbs at the source" | Not the chosen approach. The right place is the executor (cheap, contained). The builder doesn't have UI knowledge per-event. |
| "Alt accordions have both Over and Under, the Under is just hidden" | No. Alt accordions are Yes-only (each row has one pick: Yes-X+). The Under is not hidden, it doesn't exist. |
| "If both `_alternate + side=over` AND `_alternate + side=under` rows exist in stage, that's normal" | Wrong. `_alternate + side=under` should never appear. If it does, treat as data corruption. |

---

## ROI gating — two gates, two purposes

There are **two** ROI filters in the pipeline. They have different roles. Don't conflate them.

| Gate | Where | Default | Role |
|---|---|---|---|
| `BUILDER_MIN_ROI` | `airflow/dags/bg_arb_pipeline.py` → passed to `build_opportunities(min_roi=...)` in `bg_arb_pipeline_lib/builder.py:75-ish` | `0.0` | **Storage-protection floor.** Drops negative-ROI cartesian pairs before they reach `bg_arbitrage_opportunities`. Without it, ~95% of pairs (book overround eats the margin) bloat the table by ~100×. |
| `MIN_ROI_THRESHOLD` | `arbitrage_executor/opportunity.py:55` (env-overridable) | `0.005` | **Policy gate.** What the executor will actually bet. Tune per-experiment via env var. |

**Setting `MIN_ROI_THRESHOLD=-0.01` does nothing below `BUILDER_MIN_ROI`** — the executor can only filter rows the builder wrote. If you want to explore the negative-ROI tail, set `BUILDER_MIN_ROI=-0.01` (Airflow Docker env) AND `MIN_ROI_THRESHOLD=-0.01` (worker env). Be aware of the storage impact: opps and history tables both grow significantly.

## Quick cross-references

- `arbitrage_executor/selectors/betmgm_markets.yaml` — the routing table. Std entries end in ` O/U`; alt entries don't and carry `has_threshold_tabs: true`.
- `arbitrage_executor/bet_placer_betmgm.py` `_expand_accordion_betmgm` — the std-missing-alt-present detection lives here.
- `arbitrage_executor/bet_placer.py` — `BetPlacerSkipError` class.
- `arbitrage_executor/task_worker.py` — SKIPPED classification on SkipError.
- `airflow/dags/bg_arb_pipeline_lib/builder.py:46–59` — the pairing rule. Don't change without revisiting this doc.
- `arbitrage_executor/SOP.md` — runbook for the moment a sportsbook UI changes and breaks selectors. The diagnostic flow there assumes you've already grokked the rules here.
