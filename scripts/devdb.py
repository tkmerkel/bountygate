#!/usr/bin/env python3
"""
Developer DB tools for BountyGate.

Commands help manage and view data during backend/frontend development.

Usage examples:
  - List tables:                 python scripts/devdb.py tables
  - Describe table:              python scripts/devdb.py describe odds_events
  - Row count:                   python scripts/devdb.py count odds_events
  - Preview first rows:          python scripts/devdb.py head odds_events --limit 10
  - Run a SELECT query:          python scripts/devdb.py query "SELECT * FROM odds_events LIMIT 5"
  - Dump table to CSV:           python scripts/devdb.py dump odds_events --out exports/odds_events.csv
  - Load CSV into a table:       python scripts/devdb.py load odds_events --inp data.csv --if-exists append
  - Truncate a table:            python scripts/devdb.py truncate odds_events --yes
  - Connection info + ping:      python scripts/devdb.py info
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Any, Iterable, List

from sqlalchemy import create_engine, text, inspect


def _add_app_shared_to_path() -> None:
    """Ensure `app/shared/python` is importable for shared utils."""
    repo_root = Path(__file__).resolve().parents[1]
    shared_path = repo_root / "app" / "shared" / "python"
    if str(shared_path) not in sys.path:
        sys.path.insert(0, str(shared_path))


_add_app_shared_to_path()

try:
    # Reuse the same connection URL and helpers as the app
    from bountygate.utils import db_connection as dbc
except Exception as e:  # pragma: no cover
    print("Failed to import bountygate.utils.db_connection from app/shared/python.")
    print("Error:", e)
    print("Make sure you run this from the repo root and that app/shared/python exists.")
    sys.exit(1)

# Allow overriding the URL via env for local dev without editing code
_env_url = os.environ.get("DATABASE_URL")
if _env_url:
    try:
        dbc.DATABASE_URL = _env_url  # type: ignore[attr-defined]
    except Exception:
        pass


def _mask_url(url: str) -> str:
    # Mask password in a DB URL for safe printing
    try:
        if "@" in url and "://" in url:
            scheme, rest = url.split("://", 1)
            creds, hostpart = rest.split("@", 1)
            if ":" in creds:
                user, _pwd = creds.split(":", 1)
                creds_masked = f"{user}:*****"
            else:
                creds_masked = creds
            return f"{scheme}://{creds_masked}@{hostpart}"
        return url
    except Exception:
        return url


def _get_url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


def cmd_info(_: argparse.Namespace) -> int:
    url = _get_url()
    print("Database URL:", _mask_url(url) or "<unset>")
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            version = conn.execute(text("select version()"))
            now = conn.execute(text("select current_timestamp"))
            print("Server:", version.scalar())
            print("Time:", now.scalar())
        engine.dispose()
        return 0
    except Exception as e:
        print("Connection check failed:", e)
        return 2


def cmd_tables(_: argparse.Namespace) -> int:
    try:
        dbc.list_all_tables()
        return 0
    except Exception as e:
        print("Error listing tables:", e)
        return 2


def cmd_describe(ns: argparse.Namespace) -> int:
    try:
        info = dbc.get_table_info(ns.table)
        if not info:
            return 1
        print("Columns:")
        for col in info["columns"]:
            # keys: name, type, nullable, default, etc
            print(f"  - {col.get('name')} :: {col.get('type')} "
                  f"nullable={col.get('nullable')} default={col.get('default')}")
        if info.get("primary_keys"):
            print("Primary keys:", info["primary_keys"])  # type: ignore
        if info.get("indexes"):
            print("Indexes:")
            for idx in info["indexes"]:
                print(f"  - {idx.get('name')} (unique={idx.get('unique')}): {idx.get('column_names')}")
        if info.get("foreign_keys"):
            print("Foreign keys:")
            for fk in info["foreign_keys"]:
                print(f"  - {fk.get('name')} -> {fk.get('referred_table')}({fk.get('referred_columns')})"
                      f" via {fk.get('constrained_columns')}")
        return 0
    except Exception as e:
        print("Describe failed:", e)
        return 2


def cmd_count(ns: argparse.Namespace) -> int:
    try:
        dbc.get_row_count(ns.table)
        return 0
    except Exception as e:
        print("Count failed:", e)
        return 2


def _print_rows(rows: Iterable[dict[str, Any]], limit: int | None = None) -> None:
    printed = 0
    for r in rows:
        print(r)
        printed += 1
        if limit is not None and printed >= limit:
            break


def cmd_head(ns: argparse.Namespace) -> int:
    sql = f"SELECT * FROM {ns.table} LIMIT {ns.limit}"
    return cmd_query(argparse.Namespace(sql=sql, limit=None))


def cmd_query(ns: argparse.Namespace) -> int:
    sql = ns.sql
    # Try to detect if it's a SELECT to use pandas for nicer formatting if available
    is_select = sql.strip().lower().startswith("select")
    url = _get_url()
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            if is_select:
                try:
                    import pandas as pd  # type: ignore
                    df = pd.read_sql(sql, conn.connection)
                    if df.empty:
                        print("No rows returned.")
                    else:
                        # Print a compact preview
                        print(df.to_string(max_rows=20, max_cols=20, show_dimensions=True))
                except Exception:
                    result = conn.execute(text(sql))
                    rows = [dict(r._mapping) for r in result]
                    if rows:
                        _print_rows(rows, limit=ns.limit)
                    else:
                        print("No rows returned.")
            else:
                result = conn.execute(text(sql))
                conn.commit()
                try:
                    rowcount = result.rowcount  # type: ignore[attr-defined]
                except Exception:
                    rowcount = None
                print("Statement executed.", f"rowcount={rowcount}" if rowcount is not None else "")
        return 0
    except Exception as e:
        print("Query failed:", e)
        return 2
    finally:
        engine.dispose()


def cmd_dump(ns: argparse.Namespace) -> int:
    # Ensure output directory exists
    out_path = Path(ns.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sql = f"SELECT * FROM {ns.table}"
    try:
        import pandas as pd  # type: ignore
        df = dbc.fetch_data(sql)
        df.to_csv(out_path, index=False)
        print(f"Wrote {len(df)} rows to {out_path}")
        return 0
    except Exception as e:
        print("Dump failed:", e)
        return 2


def cmd_load(ns: argparse.Namespace) -> int:
    inp = Path(ns.inp)
    if not inp.exists():
        print(f"Input file not found: {inp}")
        return 1
    try:
        import pandas as pd  # type: ignore
        df = pd.read_csv(inp)
        dbc.insert_data(df, ns.table, if_exists=ns.if_exists)
        return 0
    except Exception as e:
        print("Load failed:", e)
        return 2


def cmd_truncate(ns: argparse.Namespace) -> int:
    if not ns.yes:
        print("Refusing to truncate without --yes")
        return 1
    try:
        dbc.truncate_table(ns.table, confirm=True)
        return 0
    except Exception as e:
        print("Truncate failed:", e)
        return 2


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="BountyGate DB development tools")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("info", help="Show connection info and ping").set_defaults(func=cmd_info)
    sub.add_parser("tables", help="List all tables").set_defaults(func=cmd_tables)

    d = sub.add_parser("describe", help="Show table schema/details")
    d.add_argument("table")
    d.set_defaults(func=cmd_describe)

    c = sub.add_parser("count", help="Row count for a table")
    c.add_argument("table")
    c.set_defaults(func=cmd_count)

    h = sub.add_parser("head", help="Preview first N rows from a table")
    h.add_argument("table")
    h.add_argument("--limit", type=int, default=5)
    h.set_defaults(func=cmd_head)

    q = sub.add_parser("query", help="Run an arbitrary SQL query")
    q.add_argument("sql")
    q.add_argument("--limit", type=int, default=None, help="Limit printed rows for SELECT without pandas")
    q.set_defaults(func=cmd_query)

    dp = sub.add_parser("dump", help="Export entire table to CSV")
    dp.add_argument("table")
    dp.add_argument("--out", required=True, help="Output CSV path")
    dp.set_defaults(func=cmd_dump)

    ld = sub.add_parser("load", help="Load CSV into a table")
    ld.add_argument("table")
    ld.add_argument("--inp", required=True, help="Input CSV path")
    ld.add_argument("--if-exists", choices=["fail", "replace", "append"], default="append")
    ld.set_defaults(func=cmd_load)

    tr = sub.add_parser("truncate", help="Truncate a table (DANGEROUS)")
    tr.add_argument("table")
    tr.add_argument("--yes", action="store_true", help="Confirm destructive action")
    tr.set_defaults(func=cmd_truncate)

    return p


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    ns = parser.parse_args(argv)
    return ns.func(ns)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
