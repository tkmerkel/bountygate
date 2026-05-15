# Map missing markets — runbook

Generated from the 2026-05-14 10:43 batch (`python execute_arb.py --max-attempts 3`). Every command below was a "selectors not mapped" skip in that run. Ordered by impact (number of candidates each unlocks).

## Before you start

1. Chrome running with CDP: it'll auto-launch on first map command, but easiest if a session is already up. Confirm with: `Get-Process chrome -ErrorAction SilentlyContinue | Select-Object -First 1`
2. You're logged into both FanDuel and BetMGM in that Chrome profile.
3. `map_selectors.py` is interactive — expect 5–8 prompts per market, including a "look at the highlighted element, is this right? (y/n)" style confirmation. Have eyes on the browser.
4. `cd C:\Users\tkmer\bountygate\arbitrage_executor` once and stay there.

## Tier 1 — unblock the most candidates (4 each)

```powershell
# player_points_q1 — Q1 points, blocked Duncan Robinson, Ausar Thompson,
# Julius Randle, Donovan Mitchell in today's batch
python map_selectors.py --site betmgm   --market player_points_q1
python map_selectors.py --site fanduel  --market player_points_q1

# player_shots_on_goal — NHL SOG O/U, blocked Rasmus Dahlin, Cole Caufield,
# Lane Hutson, Ivan Demidov
python map_selectors.py --site betmgm   --market player_shots_on_goal
python map_selectors.py --site fanduel  --market player_shots_on_goal
```

## Tier 2 — alternate-line variants (1 each, but unlocks future days)

```powershell
# Alt-line ladders — these mirror the standard market but use the
# "X+" tab on BetMGM. Map the standard one first (Tier 1) if not already.
python map_selectors.py --site fanduel  --market player_shots_on_goal_alternate
python map_selectors.py --site fanduel  --market player_points_assists_alternate
python map_selectors.py --site fanduel  --market player_points_rebounds_alternate
```

## Tier 3 — single-candidate misses

```powershell
# Combo stat (3-way) — should drop into the BetMGM "Combo stats" sub-tab
# now that the sub_tab_label fix shipped; FanDuel side just needs mapping
python map_selectors.py --site betmgm   --market player_points_rebounds_assists
python map_selectors.py --site fanduel  --market player_points_rebounds_assists

# NHL goalie saves — only Jakub Dobes blocked today, but this unlocks all
# starting goalies on future nights
python map_selectors.py --site fanduel  --market player_total_saves
python map_selectors.py --site betmgm   --market player_total_saves
```

## After each successful mapping

The new selectors land in `selectors/fanduel_markets.yaml` or `selectors/betmgm_markets.yaml`. Sanity-check:

```powershell
# Confirm the new entry exists with search_validated: true
Select-String -Path "selectors\fanduel_markets.yaml" -Pattern "^player_points_q1:"  -Context 0,12
Select-String -Path "selectors\betmgm_markets.yaml"  -Pattern "^player_points_q1:"  -Context 0,12
```

If `search_validated: false` in the saved entry, the mapping tool didn't get a clean hit — re-run that one command, this time picking a different `test_player` when prompted.

## After Tier 1 + 2 are done

Smoke-test that the ROI gate + cooldown + new selectors stack cleanly:

```powershell
python execute_arb.py --max-candidates 5
```

Expect fewer "selectors not mapped" skips, and any positive-EV opps that previously skipped will now attempt. Recordings land in `arbitrage_executor\audit_logs\`.

Then drain the watcher queue:

```powershell
& 'C:\Users\tkmer\bountygate\scripts\start_watcher.ps1'
```

## Notes / gotchas

- For NHL markets, today's opps were Montréal Canadiens @ Buffalo Sabres and Vegas Golden Knights @ Anaheim Ducks. If the mapper can't find players today, the game state may be wrong (game over, postponed) — try a future NHL event instead.
- Q1 markets only appear pre-tipoff. If you're mapping `player_points_q1` after games have started, the market may not exist on the page. Pick a game that hasn't started.
- BetMGM "Combo stats" sub-tab navigation already ships in code — `player_points_rebounds_assists` mapping just needs to find the right accordion *within* that sub-tab. If the mapper lands on the wrong tab, double-check the `sub_tab_label` field gets added to the saved YAML entry.
- If the `map_selectors.py --site fanduel` flow drops you on the Betslip page instead of the search page (the regression flagged in the 21:40 watcher review), close the betslip drawer manually and re-run. That's the pending FanDuel Phase 1 selector regression that wasn't shipped in the last batch of fixes.
