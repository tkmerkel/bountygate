# BountyGate Monorepo

This repository bundles the pieces you need to run BountyGate end-to-end:

- **Airflow orchestrator** (under `airflow/`) for collecting and normalising odds data.
- **Streamlit dashboard** (under `app/`) for visualising value plays.
- **Shared Python package** (under `app/shared/python/`) for reusable ingestion, transformation, and persistence code.
- **Deployment configs** (under `infra/`) for Streamlit-on-Heroku and container-based tooling.
- **Automation scripts & tests** to keep quality tight.

## Folder layout

```text
BountyGate/
├── airflow/            # Local Airflow deployment + DAGs
├── app/                # Streamlit application
├── infra/              # Deployment manifests (Heroku, Docker, etc.)
├── scripts/            # Developer utilities
├── app/shared/python/  # Reusable Python modules for both Airflow and Streamlit
└── tests/              # Unit/integration tests spanning shared logic
```

## Quick start

1. **Bootstrap a virtual environment**
   ```powershell
   cd BountyGate
   ./scripts/local_dev_setup.ps1
   ```

2. **Run Airflow locally**
   ```powershell
   cd airflow
   docker compose up --build
   ```
   The webserver will be available at <http://localhost:8080>. The example DAG `bountygate_example`
   demonstrates how to consume the shared odds module.

3. **Launch the Streamlit dashboard**
   ```powershell
   cd app
   streamlit run streamlit_app.py
   ```

4. **Run tests**
   ```powershell
   pytest
   ```

## Packaging the shared code

`app/shared/python` is structured as an installable package. Both the Airflow image and the Streamlit app
pip-install it in editable mode so changes propagate immediately during development.
From the repo root use: `pip install -e ./app/shared/python`.
From the `app/` folder use: `pip install -e ./shared/python`.
For production you can publish it to an internal package index or build a wheel.

## Deployment notes

- **Heroku**: The `infra/heroku` folder contains a `Procfile` and `runtime.txt` for the Streamlit app.
  You can push the repo to a Heroku app or use GitHub Actions for continuous deployment.
- **Airflow in production**: When you move beyond local execution, keep the `airflow/` folder as the
  authoritative DAG source. A CI pipeline can package it into an image or sync the DAGs bucket.

## Next steps

- Update or replace data loader modules under `app/shared/python/bountygate/data_loaders/` with your unified lines reader.
- Add real DAGs and Airflow configurations matching your existing project.
- Wire the Streamlit dashboard to your live database (using credentials injected via environment variables).
- Expand the tests folder with regression coverage as you add shared utilities.
