# BetPlacer Sequencing Review - 2026-05-16

Objective: review `bet_placer.py` sequencing and prove the saved selector flow
can navigate, click a real bet into the slip, clear it, and verify empty state.

## Scope

- `arbitrage_executor/bet_placer.py`
- `arbitrage_executor/validate_selector.py`
- Auth unblocker in `arbitrage_executor/auth.py`
- Regression tests in `arbitrage_executor/tests/test_bet_placer_sequencing.py`

The requested Playwright MCP tools were not exposed in this Codex session:
`list_mcp_resources` and `list_mcp_resource_templates` both returned empty
lists. Live checks therefore used the repo's Playwright/CDP harness, which
attaches to Chrome on port 9223 and exercises the same browser flow without
entering a wager or clicking Place Bet.

The local Claude CLI also reports no configured MCP servers:

```powershell
claude mcp list
# No MCP servers configured. Use `claude mcp add` to add a server.
```

## Findings

1. FanDuel clear-all sequencing could click "Remove all selections" and return
   before post-clear verification.
2. BetMGM clear-all sequencing had the same early-return shape.
3. FanDuel validation originally reused a production-optimistic slip probe,
   so an ambiguous state could pass validation without a concrete slip signal.
4. FanDuel auth could be blocked before the bet flow by a non-credential modal
   intercepting the login-link click.
5. A failed `--no-save` validation still wrote failed validation metadata.

## Changes

- `BetPlacer.clear_betslip()` and validation assertions now expose explicit
  no-wager sequencing operations.
- FanDuel and BetMGM clear paths now continue into verification after clear-all
  clicks.
- FanDuel validation uses concrete slip signals, including the current
  "Remove all selections" control and wager input signals.
- Auth dismisses non-credential blocking modals before attempting login.
- `validate_selector.py --no-save` now also suppresses failed-status writes.
- `validate_selector.py --max-age-hours` supports bounded stale-candidate
  validation when current opportunity tables are empty.

## Live Validation Evidence

Commands run with `--no-save`, so successful runs did not persist YAML metadata:

```powershell
python -X utf8 arbitrage_executor\validate_selector.py --site fanduel --market player_rebounds --max-age-hours 48 --no-save
python -X utf8 arbitrage_executor\validate_selector.py --site betmgm --market player_assists --max-age-hours 48 --no-save
```

Observed passes:

- FanDuel: selected `Chet Holmgren Over 8.5` into the slip, cleared it, and
  verified empty.
  Audit: `arbitrage_executor/audit_logs/selector_validation_20260516_205708_fanduel_player_rebounds`
- BetMGM: selected `Jalen Duren Over 1.5` into the slip, cleared it, and
  verified empty.
  Audit: `arbitrage_executor/audit_logs/selector_validation_20260516_205953_betmgm_player_assists`

## Verification

```powershell
python -m pytest arbitrage_executor\tests -q
python -m py_compile arbitrage_executor\auth.py arbitrage_executor\validate_selector.py arbitrage_executor\selector_finder.py arbitrage_executor\bet_placer.py arbitrage_executor\execute_arb.py arbitrage_executor\tests\test_bet_placer_sequencing.py
git diff --check
```

Results:

- `19 passed`
- py_compile passed
- diff check passed

## Prompt-To-Artifact Audit

| Requirement | Evidence | Status |
| --- | --- | --- |
| Review `bet_placer.py` sequencing | Findings and fixes in this document; changed clear, validation, and site-specific slip proof paths | Complete |
| Use browser automation to validate no-wager sequence | `validate_selector.py` runs through Playwright/CDP and never enters wager or clicks Place Bet | Complete via CDP |
| Prove FanDuel can navigate, select, and clear | PASS audit `selector_validation_20260516_205708_fanduel_player_rebounds` | Complete |
| Prove BetMGM can navigate, select, and clear | PASS audit `selector_validation_20260516_205953_betmgm_player_assists` | Complete |
| Add regression coverage for sequencing defects | `arbitrage_executor/tests/test_bet_placer_sequencing.py`; `19 passed` | Complete |
| Avoid persisting validation metadata during dry validation | `validate_selector.py --no-save` now suppresses success and failure YAML writes | Complete |
| Use literal Playwright MCP tools | No `mcp__playwright__*` tools exposed; `claude mcp list` reports no configured MCP servers | Blocked |

## Remaining Gap

The literal Playwright MCP review remains blocked until this session exposes
`mcp__playwright__browser_*` tools or equivalent MCP resources. The browser
sequence itself has been exercised through Playwright/CDP.

Additional local discovery checks found no safer preinstalled MCP path:

- `npm list @playwright/mcp` from the repo returned empty.
- `npm list -g @playwright/mcp` returned empty.
- Searching the enabled Codex plugin cache found browser/chrome Playwright APIs,
  but no bundled `@playwright/mcp` server.
- The Browser and Chrome plugin docs describe `tab.playwright` as an internal
  plugin API, not an external Playwright MCP server.

To configure Playwright MCP for a future Claude Code session, the CLI syntax is:

```powershell
claude mcp add playwright -- npx @playwright/mcp
```

This command is intentionally documented but was not run here because it would
set up a server command that fetches/executes npm code when invoked. Run it only
after approving that risk.

## MCP Replay Checklist

When Playwright MCP is available, run the same no-wager proof with MCP browser
actions and compare against the CDP validation result:

1. Navigate/log in to FanDuel.
2. Clear the FanDuel slip.
3. Search `Chet Holmgren`.
4. Click `Chet Holmgren Over 8.5` rebounds into the slip.
5. Confirm the slip contains the selected bet.
6. Clear the FanDuel slip.
7. Confirm the slip is empty.
8. Navigate/log in to BetMGM.
9. Clear the BetMGM slip.
10. Search `Detroit Pistons`.
11. Open `Cleveland Cavaliers at Detroit Pistons`.
12. Select player-props `Assists`.
13. Expand `Player assists O/U`.
14. Click `Jalen Duren Over 1.5` into the slip.
15. Confirm the slip contains the selected bet.
16. Clear the BetMGM slip.
17. Confirm the slip is empty.

Do not enter a wager amount and do not click Place Bet.

The expected MCP outcome is equivalent to:

```powershell
python -X utf8 arbitrage_executor\validate_selector.py --site fanduel --market player_rebounds --max-age-hours 48 --no-save
python -X utf8 arbitrage_executor\validate_selector.py --site betmgm --market player_assists --max-age-hours 48 --no-save
```
