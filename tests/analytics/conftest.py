"""conftest.py — put app/shared/python on sys.path so tests can import
bountygate.analytics without installing the package first."""

import sys
import pathlib

# Repo root is three levels up: tests/analytics/ -> tests/ -> repo root
_REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
_SHARED_PYTHON = _REPO_ROOT / "app" / "shared" / "python"

if str(_SHARED_PYTHON) not in sys.path:
    sys.path.insert(0, str(_SHARED_PYTHON))
