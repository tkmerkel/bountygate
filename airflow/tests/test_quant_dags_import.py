import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk", reason="DAG import smoke runs in the Airflow 3 container; host has Airflow 2.10")

DAGS = Path(__file__).parent.parent / "dags"
QUANT_DAGS = [
    "build_fair_odds.py",
    "derive_closing_lines.py",
    "ingest_results.py",
    "ingest_props.py",
    "score_results.py",
]


@pytest.mark.parametrize("filename", QUANT_DAGS)
def test_quant_dag_imports(filename):
    path = DAGS / filename
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "dag"), f"{filename} must define a top-level `dag`"
