from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="cleanup_logs",
    start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["maintenance"],
) as dag:
    BashOperator(
        task_id="delete_old_logs",
        bash_command="find /opt/airflow/logs -type f -mtime +7 -print0 | xargs -0 rm -f",
    )
