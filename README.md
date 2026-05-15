# BountyGate Monorepo

This repository bundles the pieces that make up BountyGate end-to-end:

- **Web app** (FastAPI + static frontend) under `app/web/` — served at `bountygate.io` via Heroku.
- **Airflow orchestrator** (`airflow/`) for collecting and normalising odds data — local-only today.
- **Shared Python package** (`app/shared/python/`) for reusable ingestion, transformation, and persistence code.
- **Arbitrage executor** (`arbitrage_executor/`) — local automation against sportsbook UIs.
- **Database** (`db/migrations/` + `scripts/migrate.py`) — raw SQL migrations applied via a tiny idempotent runner.

## Folder layout

```text
BountyGate/
├── airflow/            # Local Airflow deployment + DAGs
├── app/
│   ├── shared/python/  # Reusable Python modules (ingestion, db, utils)
│   └── web/            # FastAPI web app (deployed to Heroku)
├── arbitrage_executor/ # Local browser automation against sportsbooks
├── claude_toolkit/     # Internal tooling
├── dashboard/          # Static frontend (index.html + data.json) — served by app/web
├── db/migrations/      # Numbered .sql migrations (NNN_name.sql)
├── scripts/            # Developer utilities (incl. migrate.py)
└── watcher/            # Internal tooling
```

## Quick start

1. **Bootstrap a virtual environment**
   ```powershell
   ./scripts/local_dev_setup.ps1
   ```

2. **Run the web app locally**
   ```powershell
   $env:DATABASE_URL = "postgresql+psycopg2://user:pass@localhost:5432/bountygate"
   uvicorn app.web.main:app --reload --port 8000
   ```
   Then open <http://localhost:8000/>. Health check at `/health`.

3. **Run Airflow locally**
   ```powershell
   cd airflow
   docker compose up --build
   ```
   Webserver at <http://localhost:8080>.

4. **Run tests**
   ```powershell
   pytest
   ```

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
