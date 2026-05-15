# scripts/

## Operational (used in normal operation)

| Script | Purpose |
|--------|---------|
| `migrate.py` | Run/rollback DB migrations from `db/migrations/`. **Run on every fresh DB setup.** |
| `dq_checks.py` | Data-quality checks against the analytics tables. Wired into the Airflow DQ DAG. |
| `load_aliases.py` | Load player aliases from `db/aliases/` into Postgres. |
| `load_market_aliases.py` | Load market name aliases from `db/market_aliases/` into Postgres. |
| `devdb.py` | Local dev DB helpers (drop/reset/seed). |
| `local_dev_setup.ps1` | Bootstrap a fresh dev machine. |
| `start_watcher.ps1` | Launch the video-feedback-loop Claude watcher session. See `../watcher/README.md`. |

## archive/

Historical one-off scripts kept for reference. Not run in normal operation. If a sport is added or aliases need re-seeding from scratch, these are the templates — but expect to update them before running.
