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

## Fields

Every documented field below is either read by `bet_placer.py` at runtime or written by `validate_selector.py`. Anything else does not belong in the YAML.

### LIVE fields (consumed by bet_placer.py)

| Field | Type | Sites | Used for |
|-------|------|-------|----------|
| `display_names` | list[str] | both | Human-readable market labels the bot searches for in the DOM. First entry is canonical. Ex: `["Points", "Player Points"]` |
| `accordion_name` | str | betmgm | Exact text on the accordion header button. Bot builds `button[dsaccordiontoggle]:has-text("{accordion_name}")` at runtime |
| `is_alternate` | bool | both | Triggers alternate-market code path. Also set implicitly by the `_alternate` key suffix — both work, both are checked |
| `has_threshold_tabs` | bool | betmgm | BetMGM alternates only. When true, bot clicks a threshold tab before scraping the player list |
| `tab_selector_pattern` | str template | betmgm | Template with `{threshold}` placeholder, e.g. `'button:has-text("{threshold}+")'`. Bot substitutes `calculate_alternate_tab_value(line)` at runtime |
| `sub_tab_label` | str | betmgm | Optional. When the market sits behind a secondary tab inside its accordion ("Combo stats", "Assists"), the exact tab label to click before scraping. No-op if absent. |

### Validation fields (written by validate_selector.py)

| Field | Written by | What it records |
|-------|------------|-----------------|
| `validation_status` | validate_selector | `passed`, `failed`, or `unknown`. `passed` means the executable harness clicked a real opportunity into the slip and cleared it. |
| `validated_at` | validate_selector | ISO timestamp of the last validation attempt. |
| `validation` | validate_selector | Structured proof metadata: player, line, side, source table/hash, audit dir, and timestamp. |

## Executable validation

Selector mapping itself is hand-edited YAML; ask Claude Code (with the repo-level Playwright + Chrome DevTools MCP plugins attached to your bot's Chrome on port 9223) to walk the live DOM and propose the values. Then prove the mapping executes end-to-end:

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

See `SOP.md § 2 — Map the broken selector with Claude Code` for the recovery flow when a sportsbook UI changes.

## Canonical examples

### FanDuel standard (O/U)

```yaml
player_points:
  display_names:
  - Points
  - Player Points
  validated_at: '2026-01-22T13:22:32.409618'
```

### FanDuel alternate

```yaml
player_points_alternate:
  display_names:
  - Points
  is_alternate: true
```

### BetMGM standard (O/U)

```yaml
player_points:
  accordion_name: Player points O/U
  validated_at: '2026-01-22T12:39:24.673621'
```

### BetMGM standard with sub-tab

```yaml
player_assists:
  accordion_name: Player assists O/U
  sub_tab_label: Assists
  validated_at: '2026-01-22T13:17:27.231320'
```

### BetMGM alternate (with threshold tabs)

```yaml
player_points_alternate:
  accordion_name: Points
  is_alternate: true
  has_threshold_tabs: true
  tab_selector_pattern: 'button:has-text("{threshold}+")'
```

## Rules of thumb when adding a new market

1. Keep the market key identical across both YAML files and to the DB column value (`canonical_market` in `bg_arbitrage_opportunities`).
2. Hand-edit only the fields documented above. Anything extra is silently ignored at runtime and just adds confusion.
3. For BetMGM markets, the only way to know the exact `accordion_name` text is to look at the live page — it changes per sport and sometimes per season. Ask Claude Code to inspect the page via the Playwright MCP and report it.
4. For FanDuel, `display_names[0]` should match the exact text that appears in aria-labels; alternate entries handle display variations.
5. The `_alternate` suffix on the key is load-bearing (`is_alternate_market()` relies on it). Don't rename an alternate market without updating the matching alternate references in `bet_placer_*.py`.
6. After hand-editing, run `python validate_selector.py --site <site> --market <key>` and confirm it writes `validation_status: passed`.
