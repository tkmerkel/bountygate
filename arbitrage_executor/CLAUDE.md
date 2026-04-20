# CLAUDE.md

## Project Overview

Sports arbitrage bot that executes FanDuel/BetMGM player prop pairs. Pure Playwright automation via Chrome CDP (port 9223). No AI/computer-vision in the execution path — just direct DOM selectors.

## Running

```bash
# One-off execution (fetches best opportunity from DB, places both legs)
python execute_arb.py

# Task queue worker (polls bot_execution_queue, runs execute_arb per task)
python task_worker.py
```

Chrome must be running with `--remote-debugging-port=9223` and logged into both sportsbooks. The bot launches Chrome automatically if not running (see `ensure_chrome_cdp` in `chrome_helpers.py`). The launch config bypasses sportsbook bot detection — do not modify flags or subprocess handling without explicit approval.

## Architecture

### Execution Flow
1. `opportunity.py` — Fetches best arb opportunity from RDS (`bg_arbitrage_player_props` / `_alt` tables)
2. `execute_arb.py` — 3-phase orchestrator: tease FD limit → place MGM bet → hedge on FD
3. `bet_placer.py` — Playwright page-level actions (navigate, click bet, enter wager, confirm)
4. `selector_finder.py` — Loads YAML market configs from `selectors/`

### Task Queue (Airflow integration)
- `bot_execution_queue` table: Airflow inserts PENDING rows, local worker picks them up
- `task_worker.py` polls every 15s, uses `FOR UPDATE SKIP LOCKED` for atomic claim
- Worker calls `execute_arb.main()` directly (no subprocess)

### Key Files
| File | Purpose |
|------|---------|
| `execute_arb.py` | Main orchestrator — ArbExecutor class, 3-phase strategy |
| `opportunity.py` | Fetch + filter best opportunity from DB |
| `bet_placer.py` | Playwright bet placement (navigate, click, wager, confirm) |
| `selector_finder.py` | YAML selector loader |
| `db_connection.py` | Executor-only DB helpers (fetch + executed/queue tracking). Broader admin/ETL helpers live in `app/shared/python/bountygate/utils/db_connection.py` |
| `chrome_helpers.py` | Chrome/CDP launch — shared by `execute_arb.py` and `map_selectors.py`. Do not modify launch behavior |
| `execution_logger.py` | Logs successes/failures/unmapped markets; pages Discord on WARNING/CRITICAL |
| `task_worker.py` | Polling worker for remote-triggered execution; halts on orphaned bet; posts Discord heartbeat |
| `map_selectors.py` | Utility to map new market selectors |
| `selectors/*.yaml` | Per-bookmaker market selector configs |

## Database

- Shared PostgreSQL RDS. `DATABASE_URL` is loaded from the repo-root `.env` (gitignored) via a small bootstrap in `db_connection.py`.
- Always use `engine.begin()` for writes (auto-commits). Do NOT use `engine.connect()` + `conn.commit()` — breaks in newer SQLAlchemy.
- Key tables: `bg_arbitrage_player_props`, `bg_arbitrage_player_props_alt`, `bg_executed_opportunities`, `bot_execution_queue`

## Conventions

- No formal test suite — manual testing via direct execution
- Audit logs saved per execution in `audit_logs/{timestamp}_{player}_{market}/`
- Logs in `logs/` directory (execution_failures.log, unmapped_markets.log, worker.log)
- Market selectors are YAML-driven, per bookmaker, per market type

## Operator runbook (Discord alerts)

The bot pages a Discord webhook (env var `BG_DISCORD_WEBHOOK_URL`, set in repo-root `.env`) for every notable event. Phone notifications are how the user knows what's happening when running unattended.

### Severity levels

| Prefix | Meaning | What to do |
|--------|---------|------------|
| `🚨 CRITICAL` | **Orphaned bet possible.** BetMGM was placed but the FanDuel hedge failed. The worker has already halted itself. | Open the message — it lists the BetMGM bet (stake/side/price/market) and the planned FanDuel hedge. Open FanDuel manually and place the hedge yourself. Then clear the FAILED row from `bot_execution_queue` and restart the worker. |
| `⚠️ WARNING` | A specific opportunity was skipped. No money at risk. Common causes: selectors missing for a market, ROI dropped below threshold mid-execution, BetMGM rejected the bet. | Read the message. If selectors are missing, run `python map_selectors.py --site <site> --market <market>`. If it's ROI/limit related, no action needed — the next opportunity will be tried. |
| `ℹ️ INFO` | Heartbeat or a notable non-error event (worker started, opportunity batch from Airflow). | Glance at the counts. If `attempted` is 0 for a long stretch and `queue` is also 0, the analytics pipeline isn't producing opportunities — check Airflow. |

### Heartbeat cadence

`task_worker.py` posts an `ℹ️` heartbeat every `HEARTBEAT_INTERVAL_MINUTES` (env var, default `30`). The message includes attempts, placements, no-opportunity outcomes, errors, and current `bot_execution_queue` PENDING depth. Set the env var to a smaller value (e.g. `5`) when supervising; raise back to `30` for unattended runs.

### After a CRITICAL halt — restart procedure

1. Manually resolve the unhedged BetMGM bet on FanDuel (or close the BetMGM bet — your call based on the alert message).
2. Confirm zero unhedged exposure on both books.
3. Mark the FAILED queue row resolved: `UPDATE bot_execution_queue SET status = 'COMPLETED' WHERE id = <task_id>;` (or DELETE if you prefer — Airflow inserts new tasks anyway).
4. Investigate why the hedge failed (check `logs/execution_failures.log` and the audit dir referenced in the Discord message). Common causes: FanDuel UI changed (re-run `map_selectors.py`), Chrome lost connection, FanDuel suspended the market.
5. Restart: `python task_worker.py`. The "Worker started" Discord message confirms it's back up.
