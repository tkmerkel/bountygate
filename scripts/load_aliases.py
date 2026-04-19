#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import create_engine, text


def _add_shared_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shared = repo_root / "app" / "shared" / "python"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))


_add_shared_to_path()
from bountygate.utils import db_connection as dbc  # type: ignore  # noqa: E402


CSV_REQUIRED = ["sport_key", "alias", "canonical_name"]
CSV_OPTIONAL = ["source_bookmaker", "abbreviation", "team_id", "notes"]


def db_url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


@dataclass
class Row:
    sport_key: str
    alias: str
    canonical_name: str
    source_bookmaker: Optional[str]
    abbreviation: Optional[str]
    team_id: Optional[str]


def read_csv(path: Path) -> List[Row]:
    with path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        missing = [c for c in CSV_REQUIRED if c not in rdr.fieldnames]
        if missing:
            raise ValueError(f"{path} missing required columns: {missing}")
        rows: List[Row] = []
        for i, r in enumerate(rdr, 2):
            sport = (r.get("sport_key") or "").strip()
            alias = (r.get("alias") or "").strip()
            cname = (r.get("canonical_name") or "").strip()
            if not sport or not alias or not cname:
                # skip incomplete rows
                continue
            rows.append(Row(
                sport_key=sport,
                alias=alias,
                canonical_name=cname,
                source_bookmaker=(r.get("source_bookmaker") or "").strip() or None,
                abbreviation=(r.get("abbreviation") or "").strip() or None,
                team_id=(r.get("team_id") or "").strip() or None,
            ))
    return rows


def collect_csvs(dir_path: Optional[Path], files: List[Path]) -> List[Row]:
    rows: List[Row] = []
    paths: List[Path] = []
    if dir_path:
        paths.extend(sorted(p for p in dir_path.glob("*.csv") if p.is_file()))
    paths.extend(files)
    for p in paths:
        rows.extend(read_csv(p))
    return rows


def load_reference_map(conn) -> Tuple[Dict[Tuple[str, str], str], Dict[Tuple[str, str], str]]:
    # maps: (sport_key, display_name) -> team_id; (sport_key, alias) -> team_id
    ref = conn.execute(text("SELECT sport_key, display_name, team_id FROM team_reference"))
    ref_map = {(r[0], r[1]): r[2] for r in ref}
    alias = conn.execute(text("SELECT sport_key, alias, team_id FROM team_aliases"))
    alias_map = {(r[0], r[1]): r[2] for r in alias}
    return ref_map, alias_map


def plan_changes(rows: List[Row], ref_map: Dict[Tuple[str, str], str], alias_map: Dict[Tuple[str, str], str], *, create_missing: bool) -> Tuple[List[Tuple[Row, str]], List[Tuple[Row, str]], List[Tuple[Row, str]]]:
    inserts_ref: List[Tuple[Row, str]] = []  # (row, team_id)
    inserts_alias: List[Tuple[Row, str]] = []
    conflicts: List[Tuple[Row, str]] = []

    # Pre-compute new team_ids for rows needing creation
    generated_ids: Dict[Tuple[str, str], str] = {}

    for r in rows:
        key_canon = (r.sport_key, r.canonical_name)
        tid = r.team_id or ref_map.get(key_canon) or generated_ids.get(key_canon)
        if not tid:
            if not create_missing:
                conflicts.append((r, "canonical_name not found; use --create-missing or fill team_id"))
                continue
            tid = str(uuid.uuid4())
            generated_ids[key_canon] = tid
            inserts_ref.append((r, tid))

        # Check alias mapping
        key_alias = (r.sport_key, r.alias)
        existing = alias_map.get(key_alias)
        if existing is None:
            inserts_alias.append((r, tid))
        elif existing != tid:
            conflicts.append((r, f"alias already maps to different team_id: {existing}"))

    return inserts_ref, inserts_alias, conflicts


def apply_changes(conn, inserts_ref: List[Tuple[Row, str]], inserts_alias: List[Tuple[Row, str]], *, backfill: bool) -> None:
    # Upsert team_reference
    for r, tid in inserts_ref:
        conn.execute(text(
            "INSERT INTO team_reference(team_id, sport_key, display_name, abbreviation) "
            "VALUES (:id, :s, :n, :abbr) ON CONFLICT (sport_key, display_name) DO NOTHING"
        ), {"id": tid, "s": r.sport_key, "n": r.canonical_name, "abbr": r.abbreviation})

    # Upsert team_aliases
    for r, tid in inserts_alias:
        conn.execute(text(
            "INSERT INTO team_aliases(sport_key, alias, team_id, source_bookmaker) "
            "VALUES (:s, :alias, :tid, :src) ON CONFLICT (sport_key, alias) DO NOTHING"
        ), {"s": r.sport_key, "alias": r.alias, "tid": tid, "src": r.source_bookmaker})

    if backfill and inserts_alias:
        # Backfill IDs in unified lines
        conn.execute(text(
            "UPDATE bg_unified_lines bul SET home_team_id = ta.team_id FROM team_aliases ta "
            "WHERE bul.home_team_id IS NULL AND bul.sport_key = ta.sport_key AND bul.home_team = ta.alias"
        ))
        conn.execute(text(
            "UPDATE bg_unified_lines bul SET away_team_id = ta.team_id FROM team_aliases ta "
            "WHERE bul.away_team_id IS NULL AND bul.sport_key = ta.sport_key AND bul.away_team = ta.alias"
        ))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load TEAM aliases from CSV(s)")
    p.add_argument("--dir", type=str, help="Directory containing CSVs (e.g., db/aliases)")
    p.add_argument("--file", action="append", default=[], help="Specific CSV file(s) to load")
    p.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    p.add_argument("--create-missing", action="store_true", help="Create team_reference rows if canonical_name not found")
    p.add_argument("--backfill", action="store_true", help="Backfill home/away team IDs after alias inserts")
    return p


def main(argv: List[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    dir_path = Path(args.__dict__["dir"]) if args.__dict__.get("dir") else None
    files = [Path(f) for f in args.file]
    rows = collect_csvs(dir_path, files)
    if not rows:
        print("No rows found in provided CSVs.")
        return 1

    engine = create_engine(db_url())
    try:
        with engine.connect() as conn:
            ref_map, alias_map = load_reference_map(conn)
        inserts_ref, inserts_alias, conflicts = plan_changes(rows, ref_map, alias_map, create_missing=args.create_missing)

        print("Dry-run summary:" if not args.apply else "Planned changes:")
        print({
            "rows": len(rows),
            "new_team_reference": len(inserts_ref),
            "new_aliases": len(inserts_alias),
            "conflicts": len(conflicts),
        })
        if conflicts:
            print("Conflicts (showing up to 10):")
            for r, msg in conflicts[:10]:
                print(f"  [{r.sport_key}] {r.alias} -> {r.canonical_name}: {msg}")

        if not args.apply:
            return 0 if not conflicts else 2

        if conflicts:
            print("Aborting due to conflicts. Resolve and re-run.")
            return 2

        with engine.begin() as conn:
            apply_changes(conn, inserts_ref, inserts_alias, backfill=args.backfill)
        print("Apply complete.")
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

