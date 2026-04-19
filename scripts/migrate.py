#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List

from sqlalchemy import create_engine, text


def _shared_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shared = repo_root / "app" / "shared" / "python"
    if str(shared) not in os.sys.path:
        os.sys.path.insert(0, str(shared))


_shared_on_path()
from bountygate.utils import db_connection as dbc  # type: ignore  # noqa: E402


def get_engine_url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


def ensure_migrations_table(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version text PRIMARY KEY,
              applied_at timestamptz DEFAULT now()
            )
            """
        )
    )


def applied_versions(conn) -> set[str]:
    res = conn.execute(text("SELECT version FROM schema_migrations"))
    return {row[0] for row in res}


def read_statements(sql_path: Path) -> List[str]:
    sql = sql_path.read_text(encoding="utf-8")
    # naive split on ';' since our migrations are simple (no functions/DO blocks)
    stmts = []
    for part in sql.split(";"):
        stmt = part.strip()
        if not stmt:
            continue
        # restore the semicolon the executor expects per statement
        stmts.append(stmt)
    return stmts


def apply_migration(conn, version: str, path: Path) -> None:
    stmts = read_statements(path)
    for s in stmts:
        conn.execute(text(s))
    conn.execute(text("INSERT INTO schema_migrations(version) VALUES (:v)"), {"v": version})


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Apply SQL migrations in db/migrations")
    p.add_argument("command", choices=["status", "up"], help="Show status or apply pending migrations")
    ns = p.parse_args(argv)

    mig_dir = Path(__file__).resolve().parents[1] / "db" / "migrations"
    files = sorted(f for f in mig_dir.glob("*.sql"))
    engine = create_engine(get_engine_url())
    try:
        with engine.begin() as conn:
            ensure_migrations_table(conn)
        with engine.connect() as conn:
            done = applied_versions(conn)
        if ns.command == "status":
            for f in files:
                ver = f.stem
                mark = "APPLIED" if ver in done else "PENDING"
                print(f"{ver:>32}  {mark}")
            return 0
        # ns.command == "up"
        applied_any = False
        for f in files:
            ver = f.stem
            if ver in done:
                continue
            with engine.begin() as conn:
                apply_migration(conn, ver, f)
            print(f"Applied {ver}")
            applied_any = True
        if not applied_any:
            print("No pending migrations.")
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

