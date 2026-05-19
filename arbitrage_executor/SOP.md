# Standard Operating Procedure: Recovering from a Sportsbook UI Change

This runbook is for the moment a sportsbook changes its DOM and the bot stops placing a market correctly. It assumes you've received a Discord alert (see `CLAUDE.md` § "Operator runbook (Discord alerts)") or noticed failures in `logs/execution_failures.log`.

**Target audience:** anyone on-call who inherits this bot with no prior context.

---

## 0. Before you start

You need:

- A working local checkout of this repo.
- Chrome running with `--remote-debugging-port=9223` and logged into both FanDuel and BetMGM. If Chrome isn't running, `chrome_helpers.ensure_chrome_cdp` launches it — do **not** modify launch flags or profile.
- Access to the Postgres RDS via the repo-root `.env` (see `CLAUDE.md` § Database).
- The Discord alert message open in front of you, or the relevant rows from `logs/execution_failures.log`. These tell you the **site**, **market key**, **player**, and **line** that failed.

If you are reacting to a `🚨 CRITICAL` alert (orphaned bet), do **steps A–B first**, then come back here:

**A.** Manually hedge or cancel the unhedged BetMGM bet on FanDuel. Confirm zero exposure on both books.

**B.** Resolve the queue row: `UPDATE bot_execution_queue SET status = 'COMPLETED' WHERE id = <task_id>;`

For `⚠️ WARNING` alerts (market skipped, no money at risk), you can proceed directly.

---

## 1. Identify which selector broke

Open the most recent audit directory:

```
arbitrage_executor/audit_logs/{timestamp}_{player}_{market}/
```

Look for the **last** screenshot before failure — the filename hints at which stage failed. Typical stages:

| Screenshot prefix | Meaning | Likely broken selector |
|-------------------|---------|-------------------------|
| `accordion_expansion_failed` | Couldn't find/click the market accordion | `accordion_name` in the YAML (BetMGM) or `display_names` (FanDuel) |
| `place_bet_not_found` | "Place Bet" button didn't render | Confirmation-button selector in `bet_placer.py` |
| `bet_status_unknown` | Bet click went through but the success/failure message wasn't recognized | Confirmation-message text in `bet_placer.py` |
| `bet_rejected` | Book rejected the bet — probably not a selector issue | Check the on-screen error text in the screenshot |

Cross-reference the last log line in `logs/execution_failures.log` with the screenshot filename.

---

## 2. Map the broken selector with Claude Code

With Chrome already running on port 9223 (your bot's existing CDP instance, logged into FD/MGM), open Claude Code in the repo. The repo-level `.mcp.json` attaches both Playwright MCP and Chrome DevTools MCP to that exact Chrome session — your sportsbook logins are reused; no fresh browser spawns.

Ask Claude Code something like:

> "On [site], walk to the [market] page for [a real recent opportunity — player + event]. The current YAML at `selectors/{site}_markets.yaml` is `<paste the entry>`. The audit screenshot at `audit_logs/.../{prefix}.png` shows the page state when the bot failed. Propose a stable selector for the broken element ([accordion / All Wagers link / search input / etc.])."

Claude will use the MCP tools to inspect the live DOM, try selector candidates, and report which one uniquely matches. **Hand-edit the YAML** with the chosen value — the only fields the bot reads at runtime are listed in `selectors/SCHEMA.md` § LIVE fields.

If the live page actually looks fine and the selector wouldn't change, the break may be transient. Check:

- Was the failed event suspended or finished when the bot tried? (Check the game status.)
- Was Chrome in a weird state — logged out, MFA prompt, geolocation prompt? (Look for those in the audit screenshot.)
- Rate-limit error in `logs/execution_failures.log`? Back off and retry in a few hours.

---

## 3. Validate the new selector

Before resuming automated execution, prove the new selector with the executable harness:

```bash
cd arbitrage_executor
python validate_selector.py --site <site> --market <market_key>
```

`validate_selector.py` fetches a real recent opportunity, navigates through the full runtime code path, clicks the bet into the slip, asserts the slip has it, then clears the slip without placing. On success it writes `validation_status: passed` and a `validation:` audit block back to the YAML.

If validation passes, optionally run one real cycle:

```bash
python execute_arb.py
```

Watch:

- Did it find the market? (Check `logs/unmapped_markets.log` stays empty.)
- Did it place a real bet? (Check `bg_executed_opportunities` table in Postgres.)
- Any new screenshot in `audit_logs/` with `_failed` in the name?

If validation or execution fails, go back to step 2 — it's likely a second DOM change you missed (e.g. the accordion name changed AND the player row container changed). Ask Claude Code to inspect the *next* broken element in the same audit dir.

---

## 4. Restart the worker

If the worker was halted by a CRITICAL alert:

```bash
python task_worker.py
```

You'll see a `ℹ️ Worker started` message in Discord confirming it's back up. Watch the first heartbeat (30 min default, or less if you reduced `HEARTBEAT_INTERVAL_MINUTES`) for healthy `attempted`/`placed` counts.

If the worker was never halted (a plain WARNING), it will pick up the fixed market on its next poll (every 15s).

---

## 5. Record what you did

Leave a one-line commit message:

```
fix(selectors): remap <site>/<market_key> for <reason>
```

Good examples of `<reason>`:

- "accordion renamed from 'Player points O/U' to 'Player Points Over/Under'"
- "FanDuel moved from aria-label to data-testid"
- "BetMGM added threshold tabs to previously non-alternate market"

This creates a searchable history the next contractor can learn from.

---

## Common pitfalls

- **Don't edit `chrome_helpers.py`.** The launch flags and profile handling defeat bot detection — see `CLAUDE.md` warning. If Chrome is misbehaving, the fix is almost never in that file.
- **Don't let the MCP plugins spawn their own Chrome.** The repo-level `.mcp.json` pins both Playwright MCP and Chrome DevTools MCP to `http://127.0.0.1:9223` — the existing bot Chrome with the FD/MGM logins. If you see a fresh, logged-out browser open, the attach-mode flag isn't taking effect; don't try to log in there, fix the MCP config.
- **Don't add YAML fields that aren't documented in `selectors/SCHEMA.md` § LIVE fields.** The schema is the contract; extra fields are silently ignored at runtime and confuse later edits.
- **Don't mark queue rows COMPLETED without confirming the bet state on both books.** The FAILED status is the bot's way of asking for human eyes.

## When to escalate

Open a GitHub issue and tag the owner if:

- Three different markets on the same book break in one session — likely a larger UI redesign, not a per-market break.
- You see repeated `FAILED_LEGGING` even after remapping — Chrome/CDP integrity issue, beyond this runbook's scope.
- Discord fires `🚨 CRITICAL` more than twice in 24 hours — the bot is structurally unhealthy; pause it and investigate.
