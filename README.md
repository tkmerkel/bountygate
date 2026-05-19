# bountygate

The pieces that make up BountyGate end-to-end, sharing a Postgres database:

- **Web app** (FastAPI + static frontend) under `app/web/` — served at `bountygate.io` via Heroku.
- **Analytics** (`airflow/` + `app/shared/python/`) — Airflow DAGs ingest odds from The Odds API, normalize player/market names, write candidate arbitrage opportunities to Postgres. Local-only today.
- **Execution** (`arbitrage_executor/`) — Local Windows Playwright bot that polls the queue, opens Chrome with stealth flags via CDP, places paired bets on FanDuel + BetMGM. Audit-screenshots every step. Discord alerts on success/failure/orphans.
- **Database** (`db/migrations/` + `scripts/migrate.py`) — raw SQL migrations applied via a tiny idempotent runner.

A **watcher** loop (`watcher/`) records each execution to `arbitrage_executor/audit_logs/<run>/recording.mp4`, runs the `/watch:watch` skill in a parallel Claude session to review it, and writes findings to `dashboard/data.json` for the static page at `dashboard/index.html`.

## Layout

| Path | Role |
|------|------|
| `app/web/` | FastAPI web app — deployed to Heroku. |
| `app/shared/python/bountygate/` | Shared ETL utilities imported by both DAGs and the executor. Installable via `pip install -e ./app/shared/python`. |
| `airflow/` | Airflow 3 DAGs + docker-compose local stack. Analytics pipeline. |
| `arbitrage_executor/` | The live betting bot. **Start here for operational work.** See `arbitrage_executor/CLAUDE.md`. |
| `toolkit/` | Operational scripts: doctor, smoke test, queue inspector, stuck-task rescue, codegen recorder/replay. |
| `db/migrations/` | Hand-rolled SQL migrations applied by `scripts/migrate.py`. |
| `db/aliases/`, `db/market_aliases/` | Reference data loaded by `scripts/load_aliases.py` and `scripts/load_market_aliases.py`. |
| `dashboard/` | Static HTML dashboard (`index.html` + `data.json`) — served by `app/web` and updated by the watcher. |
| `watcher/` | Prompts + stop hook for the video-feedback-loop Claude session. See `watcher/README.md`. |
| `scripts/` | Load-bearing operational scripts (migrate, dq_checks, aliases loaders). `scripts/archive/` is historical one-offs. |
| `docs/` | Plans (`docs/superpowers/plans/`) and historical critiques (`docs/archive/`). |

## Quick start

A `Makefile` at the repo root captures common operations (`make doctor`, `make worker`, `make smoke`, `make migrate`, `make test`, `make compose-up`). If `make` isn't available on Windows: `scoop install make` (or `choco install make`). The targets are short enough to run directly without `make` — see the Makefile for the actual commands.

### Bootstrap a local environment

```powershell
./scripts/local_dev_setup.ps1
```

### Run the web app locally

```powershell
$env:DATABASE_URL = "postgresql+psycopg2://user:pass@localhost:5432/bountygate"
uvicorn app.web.main:app --reload --port 8000
```

Then open <http://localhost:8000/>. Health check at `/health`. The web app has two surfaces:

- **Dashboard** at `/` — latest bot runs, account balances per book (FanDuel + BetMGM), watcher health. All cards poll JSON endpoints every 30–60s and show a freshness pill.
- **Wiki** at `/wiki` — internal docs as markdown. Mermaid diagrams always render; React Flow diagrams load on pages that use them. First page: [Bot execution flow](/wiki/bot-flow) — a layered decision graph of the three-phase pipeline.

#### Dashboard endpoints

| Endpoint | Purpose |
|---|---|
| `GET /api/runs?limit=N` | latest bot executions with issue tags |
| `GET /api/account-stats` | per-book balance, available liquidity, 7d P&L |
| `GET /api/watchers` | per-watcher status (ok/amber/red), backlog, last tick |
| `GET /api/wiki/{slug}.json` | per-page metrics for React Flow diagrams |

All four read from Postgres (`dashboard_runs`, `account_stats`, `watcher_heartbeats`). Producers — review-watcher, `account_scraper`, wiki-watcher — run locally and write directly to those tables. The Heroku web dyno never produces data; it only serves it.

#### Wiki auto-update

Each wiki page declares the source files it depends on in front-matter:

```yaml
---
title: Bot execution flow
slug: bot-flow
watches:
  - arbitrage_executor/execute_arb.py
  - arbitrage_executor/task_worker.py
updated_at: 2026-05-16T00:00:00Z
---
```

On every git commit, `.git/hooks/post-commit` reads each page's `watches:` and touches `wiki/.pending/{slug}` for any page whose watched files changed. A wiki-watcher Claude Code session drains the pending queue by invoking `/wiki:sync <slug>` per page; the regenerated `.md` shows up as a dirty file in your working tree — review the diff and commit manually. **No auto-commit.**

One-time setup per clone:

```powershell
pwsh scripts/install_wiki_hook.ps1     # Windows PowerShell
# or
bash scripts/install_wiki_hook.sh      # git-bash / WSL / Linux
```

Run the wiki-watcher when you have pending pages:

```powershell
pwsh scripts/start_wiki_watcher.ps1
```

It loops until `wiki/.pending/` is empty, then exits.

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

Airflow webserver at <http://localhost:8080>.

## Database migrations

Raw SQL migrations live under `db/migrations/` named `NNN_description.sql` (lexicographic order). Apply them with:

```powershell
python scripts/migrate.py status   # list pending vs applied
python scripts/migrate.py up       # apply all pending
```

The runner tracks applied versions in a `schema_migrations` table, so it's safe to re-run.

## Heroku deployment

The web app deploys to Heroku from the repo root:

- `Procfile` — `web:` runs uvicorn against `app.web.main:app`; `release:` runs `scripts/migrate.py up` on every deploy.
- `requirements.txt` — slim, web-app-only deps (FastAPI, uvicorn, SQLAlchemy, psycopg2). Does **not** mirror Poetry or `app/shared/python` extras — those are kept out of the slug.
- `.python-version` — pins Python 3.12.
- `.slugignore` — excludes airflow, arbitrage_executor, watcher, audit_logs, traces, etc., to keep the slug under Heroku's 500 MB soft limit.

App URL is set on the Heroku app `bountygate`. Secrets (currently just `DATABASE_URL`) are managed via `heroku config`.

## Environment

Repo-root `.env` (gitignored). Required for the bot:

- `DATABASE_URL` — Postgres connection string
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
- New market not mapped → `arbitrage_executor/SOP.md § 2` (Claude Code + Playwright/CDP MCP), then `python arbitrage_executor/validate_selector.py --site <site> --market <market>`.
- DAG not producing opportunities → check Airflow UI; data in `bg_arbitrage_player_props*` tables.
- Discord alerts → see `arbitrage_executor/CLAUDE.md` § "Operator runbook (Discord alerts)".

## Outstanding architectural concerns

See `docs/archive/CRITIQUE-2026-04-20.md` for a deep-dive triage of reliability and observability gaps (orphan reconciler, balance verification, secret rotation, etc.). Many remain unaddressed.
