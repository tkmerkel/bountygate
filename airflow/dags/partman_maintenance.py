"""Daily pg_partman maintenance: create upcoming partitions and drop expired ones
(retention). Runs from Airflow because pg_cron is not allowed on Heroku Postgres."""
from __future__ import annotations

import os

import pendulum
from airflow.sdk import dag, task
from sqlalchemy import create_engine, text


@dag(
    dag_id="partman_maintenance",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["maintenance", "db"],
)
def partman_maintenance():
    @task
    def run_maintenance() -> None:
        url = os.environ["DATABASE_URL"]
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg2://", 1)
        engine = create_engine(url)
        try:
            # AUTOCOMMIT: run_maintenance_proc commits internally; it must not run
            # inside an outer transaction block.
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text("CALL partman.run_maintenance_proc()"))
            print("[partman_maintenance] run_maintenance_proc completed")
        finally:
            engine.dispose()

    run_maintenance()


dag = partman_maintenance()
