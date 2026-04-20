# claude_toolkit

Small, self-contained diagnostic and recovery scripts for Future Claude (and you) to reach for when triaging bountygate. Every script in here is either read-only or interactive with per-row confirmation. Nothing here places bets, modifies selectors, edits DAGs, or posts anything to Discord beyond a single opt-in test message.

All scripts assume they're run from the repo root with the project's Python environment active:

```bash
python claude_toolkit/<script>.py [flags]
```

They read `DATABASE_URL` from `.env` via the same bootstrap `arbitrage_executor/db_connection.py` uses, so no additional config is needed.

## Script index

| Script | Safety | When to reach for it |
|---|---|---|
| `doctor.py` | read-only | First tool after cloning, after machine reboot, after any env change. Checks DB reachability, `.env` presence, Chrome profile dir, Chrome executable, Playwright install, required tables. Prints a pass/fail table. |
| `inspect_queue.py` | read-only | "What's the queue look like right now?" Shows counts by status, stuck `RUNNING` tasks with age, last 10 FAILED with error excerpt, last 10 COMPLETED. |
| `rescue_stuck_tasks.py` | **writes** (interactive) | After a worker crash. Finds `RUNNING` tasks older than `--threshold-minutes` (default 30) and prompts `y/N` per row before resetting to `PENDING`. Supports `--dry-run`. Never non-interactive. |
| `tail_audit.py` | read-only | "What happened on the last N executions?" Summarizes the most recent `arbitrage_executor/audit_logs/*` directories: timestamp, player, market, outcome. |
| `recent_alerts.py` | read-only | "What's been alerting?" Summarizes `arbitrage_executor/logs/execution_failures.log`, `unmapped_markets.log`, and `execution_success.log` — counts per day for the last 7 days + last 5 failure messages. |
| `test_discord.py` | **posts to Discord** | Sanity-check the webhook. Sends a single `ℹ️ INFO` message "toolkit test from {host} at {ts}". Fails loud if `BG_DISCORD_WEBHOOK_URL` is missing. No secrets in payload. |
| `selector_smoke_test.py` | launches Chrome, **no bets placed** | Before a trading session or after a suspected UI change. Requires `--player "Firstname Lastname"`; navigates to that player on FanDuel and BetMGM, asserts critical selectors resolve, screenshots, reports. Hard-stops before any confirm/place-bet button. |
| `dag_state.py` | read-only | "Are the Airflow DAGs running?" Reads `airflow/logs/dag_id=*/run_id=*` directories and reports the last run timestamp and run count in the last 24h per DAG. Falls back gracefully if the logs directory isn't present. |

## Design principles

- **Read-only by default.** Only `rescue_stuck_tasks.py` writes, and it confirms per row.
- **No tool places bets, cancels bets, or adjusts wagers.** Orphaned-bet resolution is a human-in-the-loop decision (see `arbitrage_executor/CLAUDE.md` → "After a CRITICAL halt — restart procedure"). Tooling here is allowed to observe and prompt; it is not allowed to act on the sportsbook.
- **Reuse, don't duplicate.** Chrome launch flags, Discord webhook logic, and DB connection semantics live in the existing project modules (`arbitrage_executor/chrome_helpers.py`, `app/shared/python/bountygate/utils/discord_notify.py`, `arbitrage_executor/db_connection.py`). Every script here imports them; none reimplements.
- **Small.** Each file aims for < 150 lines. If a tool needs more, it's the wrong tool.
- **No hidden state.** Scripts don't write config files, caches, or histories. One-shot invocations only.

## Known non-goals

- No orphan recovery tool. That decision sits with you.
- No test-bet placer. Would defeat the hedge safety model.
- No selector editor. `arbitrage_executor/map_selectors.py` already does that job.
- No web dashboard. Discord is the established surface.
