# BountyGate Airflow Deployment

## Local usage

```powershell
python -m venv .venv
. .venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Use docker-compose for services
docker compose up --build
```

Airflow loads DAGs from `dags/` and the shared Python package baked into the image.

PrizePicks integration
- Configure env vars for the loader if needed:
  - `PRIZEPICKS_API_URL` (projections endpoint)
  - `PRIZEPICKS_API_KEY` (if required)
  The unified DAG includes a `fetch_prizepicks_lines` task that runs when configured; otherwise it returns an empty DataFrame.

## Arb v2 environment variables

Set on the Airflow containers (compose env / `.env`). All have safe defaults.

| Var | Default | Purpose |
|-----|---------|---------|
| `BG_PROPS_SPORTS` | `basketball_nba,baseball_mlb,icehockey_nhl,americanfootball_nfl,basketball_wnba` | Sports `ingest_props` fetches (NCAAB excluded by default). Off-season sports cost only a free /events call. |
| `BG_PROPS_WINDOW_HOURS` | `24` | Commence window for props fetching (credit control). |
| `BG_ODDS_WINDOW_HOURS` | `48` | Commence window for game-line fetching (`ingest_odds`). |
| `BUILDER_MIN_ROI` | `0.0` | Storage floor for `arb_opportunities` writes; set negative to keep near-miss arbs for analysis. |
| `BG_ARB_ALERT_MIN_ROI` | `0.01` | Discord alert threshold on fee-adjusted ROI (new opportunities only). |
| `BG_ARB_GAME_FRESH_MIN` | `20` | Freshness window (minutes) for game-line book legs in `build_arbs`. |
| `BG_ARB_PROPS_FRESH_MIN` | `35` | Freshness window for prop book legs (15-min cadence + slack). |
| `BG_ARB_VENUE_FRESH_MIN` | `30` | Freshness window for prediction-market asks. |
| `BG_KALSHI_FEE_RATE` | `0.07` | Kalshi taker-fee multiplier (verified vs the published fee schedule 2026-06-12). |
| `BG_KALSHI_PROP_SERIES` | empty | Comma list of Kalshi single-game player-prop series tickers, unioned into the Kalshi fetch. The book×venue props path stays dormant until populated. Verified live tickers (public series API, 2026-06-12) — recommended starting set: `KXNBAPTS,KXNBAAST,KXNBAREB,KXNBA3PT,KXWNBAPTS,KXWNBAAST,KXWNBAREB,KXWNBA3PT,KXNHLPTS,KXNHLAST`. |

Deploy-order note for migration 019 (`point` column on `sportsbook_odds_history`): the ON
CONFLICT arbiter changes 5-col -> 6-col, so old code fails against the new index and vice
versa. Procedure: pause `ingest_*` + `normalize` -> `python scripts/migrate.py up` ->
`docker compose build && docker compose up -d` -> unpause. See the header of
`db/migrations/019_sportsbook_odds_point.sql`.

## Production

Publish the contents of this directory to your Airflow environment (e.g. Astro, MWAA, self-hosted) and ensure the shared package from `app/shared/python` is built into the runtime image (this repo’s Dockerfile copies it to `/opt/bountygate-shared/python` and installs it).
