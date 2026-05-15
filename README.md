# bountygate

Sports arbitrage stack. Two halves that share a Postgres database:

1. **Analytics** (`airflow/` + `app/shared/python/`) — Airflow DAGs ingest odds from The Odds API, normalize player/market names, write candidate arbitrage opportunities to Postgres.
2. **Execution** (`arbitrage_executor/`) — Local Windows Playwright bot that polls the queue, opens Chrome with stealth flags via CDP, places paired bets on FanDuel + BetMGM. Audit-screenshots every step. Discord alerts on success/failure/orphans.

A **watcher** loop (`watcher/`) records each execution to `arbitrage_executor/audit_logs/<run>/recording.mp4`, runs the `/watch:watch` skill in a parallel Claude session to review it, and writes findings to `dashboard/data.json` for the static page at `dashboard/index.html`.

## Layout

| Path | Role |
|------|------|
| `airflow/` | Airflow 3 DAGs + docker-compose local stack. Analytics pipeline. |
| `app/shared/python/bountygate/` | Shared ETL utilities imported by both DAGs and the executor. Installable via `pip install -e ./app/shared/python`. |
| `arbitrage_executor/` | The live betting bot. **Start here for operational work.** See `arbitrage_executor/CLAUDE.md`. |
| `toolkit/` | Operational scripts: doctor, smoke test, queue inspector, stuck-task rescue, codegen recorder/replay. |
| `db/migrations/` | Hand-rolled SQL migrations applied by `scripts/migrate.py`. |
| `db/aliases/`, `db/market_aliases/` | Reference data loaded by `scripts/load_aliases.py` and `scripts/load_market_aliases.py`. |
| `dashboard/` | Static HTML dashboard rendering `data.json` (watcher output). |
| `watcher/` | Prompts + stop hook for the video-feedback-loop Claude session. See `watcher/README.md`. |
| `scripts/` | Load-bearing operational scripts (migrate, dq_checks, aliases loaders). `scripts/archive/` is historical one-offs. |
| `docs/` | Plans (`docs/superpowers/plans/`) and historical critiques (`docs/archive/`). |

## Quick start

### Run the bot

```powershell
cd arbitrage_executor
uv sync
python task_worker.py
```

Chrome auto-launches with the stealth profile if not already running. The worker polls `bot_execution_queue` every 15s. See `arbitrage_executor/CLAUDE.md` for the full operator runbook and Discord alert handling.

### Run the analytics pipeline locally

```powershell
cd airflow
docker compose up --build
```

Airflow webserver: <http://localhost:8080>.

### Apply DB migrations

```powershell
python scripts/migrate.py up
```

## Environment

Repo-root `.env` (gitignored). Required:

- `DATABASE_URL` — Postgres RDS connection string
- `BG_DISCORD_WEBHOOK_URL` — Discord webhook for alerts (no default; bot fails fast if missing)
- `ODDS_API_KEY` — The Odds API key
- Optional: `TESTING_MODE`, `WAGER_SCALE_FACTOR`, `MIN_ROI_THRESHOLD`, `HEARTBEAT_INTERVAL_MINUTES`

## Tests

```powershell
cd arbitrage_executor
python -m pytest tests/ -v
```

The bot's hot path is not test-driven (the value is in real Playwright runs against the live UIs). Tests focus on pure-function modules: text matching, ROI math.

## Where to look when something breaks

- Bot stopped placing bets → `arbitrage_executor/SOP.md` (UI-break recovery runbook).
- Stuck task in `RUNNING` → `python toolkit/rescue_stuck_tasks.py`.
- New market not mapped → `python arbitrage_executor/map_selectors.py --site <site> --market <market>`.
- DAG not producing opportunities → check Airflow UI; data in `bg_arbitrage_player_props*` tables.
- Discord alerts → see `arbitrage_executor/CLAUDE.md` § "Operator runbook (Discord alerts)".

## Outstanding architectural concerns

See `docs/archive/CRITIQUE-2026-04-20.md` for a deep-dive triage of reliability and observability gaps (orphan reconciler, balance verification, secret rotation, etc.). Many remain unaddressed.
