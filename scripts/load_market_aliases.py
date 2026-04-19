#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from sqlalchemy import create_engine, text


def _add_shared_to_path() -> None:
    import sys
    repo_root = Path(__file__).resolve().parents[1]
    shared = repo_root / "app" / "shared" / "python"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))


_add_shared_to_path()
from bountygate.utils import db_connection as dbc  # type: ignore  # noqa: E402


CSV_FIELDS = ["bookmaker_key", "sport_key", "bm_market_key", "canonical_market_key", "notes"]


def db_url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


@dataclass
class Row:
    bookmaker_key: str
    sport_key: str
    bm_market_key: str
    canonical_market_key: str
    notes: Optional[str] = None


def read_csv(path: Path) -> List[Row]:
    with path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        missing = [c for c in CSV_FIELDS[:-1] if c not in rdr.fieldnames]
        if missing:
            raise ValueError(f"{path} missing required columns: {missing}")
        out: List[Row] = []
        for i, r in enumerate(rdr, 2):
            bookmaker = (r.get("bookmaker_key") or "").strip()
            sport = (r.get("sport_key") or "").strip()
            bm = (r.get("bm_market_key") or "").strip()
            canon = (r.get("canonical_market_key") or "").strip()
            if not bookmaker or not sport or not bm:
                continue
            out.append(Row(bookmaker, sport, bm, canon, (r.get("notes") or "").strip() or None))
    return out


def ensure_table(conn) -> None:
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS market_aliases (
          bookmaker_key text NOT NULL,
          sport_key text NOT NULL,
          bm_market_key text NOT NULL,
          canonical_market_key text,
          notes text,
          PRIMARY KEY (bookmaker_key, sport_key, bm_market_key)
        )
        """
    ))


def upsert_aliases(conn, rows: List[Row]) -> None:
    if not rows:
        return
    ensure_table(conn)
    conn.execute(text(
        """
        INSERT INTO market_aliases(bookmaker_key, sport_key, bm_market_key, canonical_market_key, notes)
        VALUES (:bk, :sk, :bm, :ck, :notes)
        ON CONFLICT (bookmaker_key, sport_key, bm_market_key)
        DO UPDATE SET canonical_market_key = EXCLUDED.canonical_market_key,
                      notes = EXCLUDED.notes
        """
    ), [
        {"bk": r.bookmaker_key, "sk": r.sport_key, "bm": r.bm_market_key, "ck": r.canonical_market_key or None, "notes": r.notes}
        for r in rows
    ])


def backfill_market_keys(conn) -> None:
    # Only fill where market_key is null or differs from canonical
    conn.execute(text(
        """
        UPDATE bg_unified_lines bul
        SET market_key = ma.canonical_market_key
        FROM market_aliases ma
        WHERE bul.bookmaker_key = ma.bookmaker_key
          AND bul.sport_key = ma.sport_key
          AND bul.bm_market_key = ma.bm_market_key
          AND COALESCE(ma.canonical_market_key, '') <> ''
          AND (bul.market_key IS NULL OR bul.market_key <> ma.canonical_market_key)
        """
    ))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load MARKET aliases from CSV(s)")
    p.add_argument("--file", action="append", default=[], help="Specific CSV file(s) to load")
    p.add_argument("--dir", type=str, help="Directory containing CSVs (e.g., db/market_aliases)")
    p.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    p.add_argument("--backfill", action="store_true", help="Backfill market_key in bg_unified_lines after upsert")
    return p


def main(argv: list[str] | None = None) -> int:
    import sys
    args = build_parser().parse_args(argv)
    dir_path = Path(args.dir) if args.dir else None
    files = [Path(f) for f in args.file]
    rows: List[Row] = []
    if dir_path:
        for p in sorted(dir_path.glob("*.csv")):
            rows.extend(read_csv(p))
    for p in files:
        rows.extend(read_csv(p))
    if not rows:
        print("No rows to process.")
        return 1

    engine = create_engine(db_url())
    try:
        with engine.connect() as conn:
            ensure_table(conn)
            # Summarize
            total = len(rows)
            blanks = sum(1 for r in rows if not r.canonical_market_key)
            print({"rows": total, "with_canonical": total - blanks, "unmapped": blanks})
            if not args.apply:
                return 0
        with engine.begin() as conn:
            upsert_aliases(conn, rows)
            if args.backfill:
                backfill_market_keys(conn)
        print("Apply complete.")
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

