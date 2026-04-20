"""Environment health check for bountygate.

Read-only. Runs a series of checks and prints a pass/fail table. Intended as
the first thing to run on a fresh clone, after a reboot, or when something
is mysteriously broken.

Usage:
    python claude_toolkit/doctor.py
"""

from __future__ import annotations

import os
import sys
import traceback
from typing import Callable, List, Tuple

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_EXECUTOR = os.path.join(_REPO_ROOT, "arbitrage_executor")
_SHARED = os.path.join(_REPO_ROOT, "app", "shared", "python")
for _p in (_EXECUTOR, _SHARED):
    if _p not in sys.path:
        sys.path.insert(0, _p)


# Each check returns (ok: bool, detail: str).
CheckFn = Callable[[], Tuple[bool, str]]


def check_env_file() -> Tuple[bool, str]:
    path = os.path.join(_REPO_ROOT, ".env")
    if os.path.isfile(path):
        return True, path
    return False, f"missing at {path}"


def check_database_url() -> Tuple[bool, str]:
    url = os.environ.get("DATABASE_URL")
    if not url:
        return False, "DATABASE_URL not set after .env bootstrap"
    # Mask credentials in the visible detail string.
    return True, _mask_url(url)


def check_db_reachable() -> Tuple[bool, str]:
    from sqlalchemy import create_engine, text
    url = os.environ.get("DATABASE_URL")
    if not url:
        return False, "DATABASE_URL not set"
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True, "SELECT 1 OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def check_required_tables() -> Tuple[bool, str]:
    from sqlalchemy import create_engine, text
    required = [
        "bot_execution_queue",
        "bg_executed_opportunities",
        "bg_arbitrage_player_props",
        "bg_arbitrage_player_props_alt",
    ]
    url = os.environ.get("DATABASE_URL")
    if not url:
        return False, "skipped: no DATABASE_URL"
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public'"
            )).fetchall()
        engine.dispose()
        existing = {r[0] for r in rows}
        missing = [t for t in required if t not in existing]
        if missing:
            return False, f"missing: {', '.join(missing)}"
        return True, f"all {len(required)} present"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def check_chrome_profile_dir() -> Tuple[bool, str]:
    path = os.path.join(_EXECUTOR, "chrome_profile")
    if os.path.isdir(path):
        return True, path
    return False, f"missing at {path}"


def check_chrome_exe() -> Tuple[bool, str]:
    try:
        from chrome_helpers import _find_chrome_exe
        return True, _find_chrome_exe()
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def check_playwright_installed() -> Tuple[bool, str]:
    try:
        import playwright  # noqa: F401
        from playwright.sync_api import sync_playwright  # noqa: F401
        return True, "playwright importable"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def check_discord_env_var() -> Tuple[bool, str]:
    # discord_notify has a hardcoded fallback (flagged in CRITIQUE.md P0 #3),
    # so the env var being unset is a warning, not a hard failure. We report
    # "set" vs "unset (uses fallback)" to make the state visible.
    if os.environ.get("BG_DISCORD_WEBHOOK_URL"):
        return True, "BG_DISCORD_WEBHOOK_URL set"
    return True, "unset (notify() uses hardcoded fallback)"


def check_logs_dir() -> Tuple[bool, str]:
    path = os.path.join(_EXECUTOR, "logs")
    if os.path.isdir(path):
        return True, path
    return False, f"missing at {path} (created on first worker run)"


CHECKS: List[Tuple[str, CheckFn]] = [
    (".env present", check_env_file),
    ("DATABASE_URL loaded", check_database_url),
    ("DB reachable", check_db_reachable),
    ("required tables present", check_required_tables),
    ("chrome_profile dir", check_chrome_profile_dir),
    ("chrome.exe locatable", check_chrome_exe),
    ("playwright installed", check_playwright_installed),
    ("arbitrage_executor/logs/", check_logs_dir),
    ("BG_DISCORD_WEBHOOK_URL", check_discord_env_var),
]


def _mask_url(url: str) -> str:
    # Hide passwords in postgres URLs for terminal output.
    if "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    creds, host = rest.split("@", 1)
    if ":" in creds:
        user = creds.split(":", 1)[0]
        return f"{scheme}://{user}:***@{host}"
    return f"{scheme}://***@{host}"


def main() -> int:
    # Import db_connection so its .env bootstrap runs before any check that
    # looks at DATABASE_URL. Wrapped because the import itself raises if
    # DATABASE_URL is not found -- that's a valid check-1 failure, not a crash.
    try:
        import db_connection  # noqa: F401
    except Exception:
        # First check will catch this; keep going.
        pass

    name_w = max(len(n) for n, _ in CHECKS)
    total_ok = 0
    print(f"{'CHECK'.ljust(name_w)}  STATUS  DETAIL")
    print(f"{'-' * name_w}  ------  ------")
    for name, fn in CHECKS:
        try:
            ok, detail = fn()
        except Exception:
            ok, detail = False, traceback.format_exc().splitlines()[-1]
        status = "PASS  " if ok else "FAIL  "
        print(f"{name.ljust(name_w)}  {status}  {detail}")
        if ok:
            total_ok += 1

    print()
    print(f"{total_ok}/{len(CHECKS)} checks passed")
    return 0 if total_ok == len(CHECKS) else 1


if __name__ == "__main__":
    sys.exit(main())
