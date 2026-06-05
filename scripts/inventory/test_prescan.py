import textwrap
from pathlib import Path

from prescan import scan_file


def test_scan_file_extracts_skeleton(tmp_path: Path):
    dag = tmp_path / "sample_dag.py"
    dag.write_text(
        textwrap.dedent(
            '''
            from airflow import DAG
            from utils.kalshi_client import KalshiClient

            with DAG(dag_id="sample_dag", schedule="@hourly") as dag:
                sql = "INSERT INTO bg_results SELECT * FROM bg_arbitrage_opportunities"
            '''
        ),
        encoding="utf-8",
    )

    rec = scan_file(dag, repo="bountygate")

    assert rec["dag_id"] == "sample_dag"
    assert rec["schedule"] == "@hourly"
    assert "utils.kalshi_client" in rec["imports"]
    assert "bg_results" in rec["tables"]
    assert "bg_arbitrage_opportunities" in rec["tables"]
