# Selector YAML Schema

This document describes the structure of `fanduel_markets.yaml` and `betmgm_markets.yaml`. These files are the source of truth for how the bot locates each player-prop market in the sportsbook UI.

## File layout

- `selectors/fanduel_markets.yaml` — FanDuel markets
- `selectors/betmgm_markets.yaml` — BetMGM markets
- Loaded by `SelectorManager.load_market_config(site)` in `selector_finder.py`. The `site` string is the filename prefix.
- Top-level keys are **market keys** (e.g. `player_points`, `player_points_alternate`, `batter_hits`, `batter_hits_alternate`). These must match the `market` column in `bg_arbitrage_player_props` / `_alt`.

## Market types

Two shapes, distinguished by a `_alternate` suffix on the market key:

| Type | Suffix | Example key | How the bot bets it |
|------|--------|-------------|---------------------|
| Standard (O/U) | _(none)_ | `player_points` | One accordion per market, contains all players; pick a player row, pick over/under at the listed line |
| Alternate (threshold) | `_alternate` | `player_points_alternate` | One accordion per market. On BetMGM also has **threshold tabs** ("5+", "7+", etc.) that must be selected before the player row is visible |

`selector_finder.is_alternate_market(key)` and `get_base_market_key(key)` are the authoritative helpers for this distinction.

## Fields — which are LIVE vs. METADATA

Fields fall into two buckets. **Live** fields are read by `bet_placer.py` at bet time — if they're wrong or missing, placement fails. **Metadata** fields are written by `map_selectors.py` during interactive mapping; they're kept for human debugging and re-mapping but are *not* consumed during a live bet.

### LIVE fields (consumed by bet_placer.py)

| Field | Type | Sites | Used for |
|-------|------|-------|----------|
| `display_names` | list[str] | both | Human-readable market labels the bot searches for in the DOM. First entry is canonical. Ex: `["Points", "Player Points"]` |
| `accordion_name` | str | betmgm | Exact text on the accordion header button. Bot builds `button[dsaccordiontoggle]:has-text("{accordion_name}")` at runtime |
| `is_alternate` | bool | both | Triggers alternate-market code path. Also set implicitly by the `_alternate` key suffix — both work, both are checked |
| `has_threshold_tabs` | bool | betmgm | BetMGM alternates only. When true, bot clicks a threshold tab before scraping the player list |
| `tab_selector_pattern` | str template | betmgm | Template with `{threshold}` placeholder, e.g. `'button:has-text("{threshold}+")'`. Bot substitutes `calculate_alternate_tab_value(line)` at runtime |

### METADATA fields (bookkeeping / debug only)

These are written by `map_selectors.py` when a market is first mapped. They serve as a snapshot of *how* the selector was validated — useful when a contractor needs to re-map a broken market. **Do not delete them**, and keep them roughly in sync when re-mapping, but understand they don't affect live bets.

| Field | Written by | What it records |
|-------|------------|-----------------|
| `selector_type` | map_selectors | High-level pattern family — `aria_label` (FanDuel), `ms_event_pick` (BetMGM) |
| `selector_pattern` | map_selectors | Example working selector string at mapping time. Contains a baked-in test player name, not used at runtime |
| `search_strategy` | map_selectors | Named strategy chosen by the operator: `aria_label_match`, `player_container_then_line`, `alternate_threshold_match`, `alternate_tab_then_player` |
| `accordion_selector` | map_selectors | Pre-built accordion selector string. Bot reconstructs this at runtime from `accordion_name`, so this field is purely for reference |
| `show_more_selector` | map_selectors | BetMGM pagination selector. Hardcoded at runtime in `bet_placer.py:280` — this YAML field is reference only |
| `bet_element_type` | map_selectors | Tag name of the clickable bet element (`ms-event-pick` on BetMGM) |
| `search_validated` | map_selectors | Boolean: did the operator confirm the mapping produced a live bet on the betslip |
| `test_player` | map_selectors | Player name used during interactive mapping |
| `test_line` | map_selectors | Betting line used during interactive mapping |
| `validated_at` | map_selectors | ISO timestamp of the last successful mapping |
| `base_market` | map_selectors | For `_alternate` keys: the standard-market key this alternate pairs with |
| `validation_status` | validate_selector | `passed`, `failed`, or `unknown`. `passed` means the executable harness clicked a real opportunity into the slip and cleared it. |
| `validation` | validate_selector | Structured proof metadata: player, line, side, source table/hash, audit dir, and timestamp. |

## Executable validation

The preferred workflow is **not** hand-mapping a selector and assuming it will run. Use the executable validation harness:

```bash
cd arbitrage_executor
python validate_selector.py --site fanduel --market batter_doubles_alternate
python validate_selector.py --site betmgm --market player_assists --testing-mode
```

Passing validation means:

1. A real recent opportunity was fetched from the same opportunity tables the bot uses.
2. The bot navigated through the real runtime path.
3. The requested site/market/side/player/line was clicked into the betslip.
4. No wager was entered and no bet was placed.
5. The betslip was cleared and verified empty.
6. `validation_status: passed` and a `validation:` block were written to YAML.

`map_selectors.py` is legacy discovery tooling. It can still help find label text, but it does not prove executability by itself.

## Canonical examples

### FanDuel standard (O/U)

```yaml
player_points:
  # LIVE
  display_names:
  - Points
  - Player Points
  # METADATA (from map_selectors.py)
  selector_type: aria_label
  selector_pattern: '[aria-label*="Svi Mykhailiuk"][aria-label*="Points"][aria-label*="7.5"]'
  search_strategy: aria_label_match
  test_player: Svi Mykhailiuk
  test_line: 7.5
  validated_at: '2026-01-22T13:22:32.409618'
```

### FanDuel alternate

```yaml
player_points_alternate:
  # LIVE
  display_names:
  - Points
  is_alternate: true
  # METADATA
  selector_type: aria_label
  search_strategy: alternate_threshold_match
  base_market: player_points
```

### BetMGM standard (O/U)

```yaml
player_points:
  # LIVE
  accordion_name: Player points O/U
  # METADATA
  accordion_selector: button[dsaccordiontoggle]:has-text("Player points O/U")
  show_more_selector: ms-option-panel-bottom-action:has-text("Show More")
  bet_element_type: ms-event-pick
  search_strategy: player_container_then_line
  search_validated: true
  test_player: P.J. Washington
  test_line: 14.5
  validated_at: '2026-01-22T12:39:24.673621'
```

### BetMGM alternate (with threshold tabs)

```yaml
player_points_alternate:
  # LIVE
  accordion_name: Points
  is_alternate: true
  has_threshold_tabs: true
  tab_selector_pattern: 'button:has-text("{threshold}+")'
  # METADATA
  accordion_selector: 'button[dsaccordiontoggle]:has-text("Points")'
  show_more_selector: ms-option-panel-bottom-action:has-text("Show More")
  bet_element_type: ms-event-pick
  search_strategy: alternate_tab_then_player
  base_market: player_points
```

## Rules of thumb when adding a new market

1. Keep the market key identical across both YAML files and to the DB column value (`bg_arbitrage_player_props.market`).
2. Always run `python map_selectors.py --site <site> --market <key>` to populate **both** live and metadata fields. Do not hand-edit unless you understand what's live vs metadata.
3. For BetMGM markets, the only way to know the exact `accordion_name` text is to look at the live page — it changes per sport and sometimes per season.
4. For FanDuel, `display_names[0]` should match the exact text that appears in aria-labels; alternate entries handle display variations.
5. The `_alternate` suffix on the key is load-bearing (`is_alternate_market()` relies on it). Don't rename an alternate market without also updating the base_market reference.

## Fields this doc does NOT describe

Anything not listed above is either undocumented legacy or a field used only by `map_selectors.py` internally. If you see a field in a YAML that isn't here, grep for it in `bet_placer.py` first — if nothing references it at bet time, it's metadata.
