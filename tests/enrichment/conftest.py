"""conftest.py — put app/shared/python on sys.path so enrichment tests can
import bountygate.enrichment without installing the package, and expose the
real-API fixture directory."""

import json
import pathlib
import sys

_REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
_SHARED_PYTHON = _REPO_ROOT / "app" / "shared" / "python"
_FIXTURES = _REPO_ROOT / "tests" / "fixtures" / "enrichment"

if str(_SHARED_PYTHON) not in sys.path:
    sys.path.insert(0, str(_SHARED_PYTHON))

import pytest


@pytest.fixture
def fixtures_dir():
    return _FIXTURES


@pytest.fixture
def load_fixture():
    def _load(name):
        with open(_FIXTURES / name, encoding="utf-8") as fh:
            return json.load(fh)

    return _load
