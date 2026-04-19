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

## Production

Publish the contents of this directory to your Airflow environment (e.g. Astro, MWAA, self-hosted) and ensure the shared package from `app/shared/python` is built into the runtime image (this repo’s Dockerfile copies it to `/opt/bountygate-shared/python` and installs it).
