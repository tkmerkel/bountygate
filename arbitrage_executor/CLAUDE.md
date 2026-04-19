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
| `execution_logger.py` | Logs successes/failures/unmapped markets |
| `task_worker.py` | Polling worker for remote-triggered execution |
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
