import importlib.util
from pathlib import Path

import pytest

DAGS = Path(__file__).parent.parent / "dags"
INGEST_DAGS = ["ingest_kalshi.py", "ingest_polymarket.py", "ingest_odds.py"]


@pytest.mark.parametrize("filename", INGEST_DAGS)
def test_ingest_dag_imports(filename):
    path = DAGS / filename
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "dag"), f"{filename} must define a top-level `dag`"
