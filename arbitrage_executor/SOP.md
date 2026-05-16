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

## 2. Reproduce the failure locally

Re-run just the affected market in isolation to confirm the theory:

```bash
cd arbitrage_executor
python map_selectors.py --site <site> --market <market_key>
```

- `<site>` is `fanduel` or `betmgm`.
- `<market_key>` is exactly as it appears in `bg_arbitrage_player_props.market` and in `selectors/{site}_markets.yaml`.
- This tool fetches a real recent opportunity from the DB, navigates to the live event, and walks through the selector discovery interactively.

If the tool itself fails to find the accordion / the player / the bet button, you've confirmed the break.

If the tool succeeds (the DOM loads correctly), the break may be transient. Check:

- Was the failed event suspended or finished when the bot tried? (Check the game status.)
- Was Chrome in a weird state — logged out, MFA prompt, geolocation prompt? (Look for those in the audit screenshot.)
- Rate-limit error in `logs/execution_failures.log`? Back off and retry in a few hours.

---

## 3. Re-map the selector

When `map_selectors.py` confirms the break, run it to remap:

```bash
python map_selectors.py --site <site> --market <market_key>
```

The tool will:

1. Prompt you that the market is already mapped. Answer `y` to re-map.
2. Open a real opportunity in Chrome.
3. Walk through discovery — you watch the browser and confirm which element is the right one.
4. Write the updated YAML back to `selectors/{site}_markets.yaml`.

**Consult `selectors/SCHEMA.md`** for what each YAML field means. You do not need to hand-edit the YAML — let the tool write it — but understanding the schema helps when the tool asks you to choose between candidates.

---

## 4. Validate the new selector

Before resuming automated execution:

```bash
python execute_arb.py
```

This runs a single execution cycle against the best current opportunity. Watch:

- Did it find the market? (Check `logs/unmapped_markets.log` stays empty.)
- Did it place a real bet or dry-run correctly? (Check `bg_executed_opportunities` table in Postgres.)
- Any new screenshot in `audit_logs/` with `_failed` in the name?

If execution succeeds, the selector is fixed.

If not, go back to step 2 with more care — it's likely a second DOM change you missed (e.g., the accordion name changed AND the player row container changed).

---

## 5. Restart the worker

If the worker was halted by a CRITICAL alert:

```bash
python task_worker.py
```

You'll see a `ℹ️ Worker started` message in Discord confirming it's back up. Watch the first heartbeat (30 min default, or less if you reduced `HEARTBEAT_INTERVAL_MINUTES`) for healthy `attempted`/`placed` counts.

If the worker was never halted (a plain WARNING), it will pick up the fixed market on its next poll (every 15s).

---

## 6. Record what you did

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
- **Don't run `map_selectors.py` without a recent opportunity in the DB.** The tool fetches the test player/line from Postgres; if the analytics pipeline hasn't produced opportunities recently (check Airflow), the tool has nothing to walk through.
- **Don't delete the `selector_pattern` / `selector_type` / `validated_at` metadata fields** when hand-editing YAML. They're not used at runtime but they're your audit trail.
- **Don't mark queue rows COMPLETED without confirming the bet state on both books.** The FAILED status is the bot's way of asking for human eyes.

## When to escalate

Open a GitHub issue and tag the owner if:

- `map_selectors.py` fails for three different markets on the same book in one session — likely a larger UI redesign, not a per-market break.
- You see repeated `FAILED_LEGGING` even after remapping — Chrome/CDP integrity issue, beyond this runbook's scope.
- Discord fires `🚨 CRITICAL` more than twice in 24 hours — the bot is structurally unhealthy; pause it and investigate.
