"""Shared test setup: load .env so tests can read DATABASE_URL from environment.

Also ensures the bountygate shared package is importable without explicit pip install.
"""
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_REPO = Path(__file__).resolve().parents[1]
_SHARED = _REPO / "app" / "shared" / "python"
if str(_SHARED) not in sys.path:
    sys.path.insert(0, str(_SHARED))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
