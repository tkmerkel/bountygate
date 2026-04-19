#!/usr/bin/env python3
from __future__ import annotations

import collections
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import create_engine, text


def _add_shared_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shared = repo_root / "app" / "shared" / "python"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))


_add_shared_to_path()
from bountygate.utils import db_connection as dbc  # type: ignore  # noqa: E402


def db_url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


def is_full_team_name(name: str) -> bool:
    if not name:
        return False
    # heuristics: full names often have spaces and mixed case; abbreviations are short and all-caps
    if " " in name and any(c.islower() for c in name):
        return True
    # long single tokens with mixed case
    if len(name) > 8 and any(c.islower() for c in name):
        return True
    return False


def seed_team_reference(conn) -> None:
    # collect candidate full names from home/away across all sports
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT sport_key, name FROM (
              SELECT sport_key, home_team AS name FROM bg_unified_lines
              UNION ALL
              SELECT sport_key, away_team AS name FROM bg_unified_lines
            ) t WHERE name IS NOT NULL AND name <> ''
            """
        )
    )
    existing = conn.execute(text("SELECT sport_key, display_name FROM team_reference"))
    existing_set = {(r[0], r[1]) for r in existing}

    new_rows: List[Tuple[str, str, str]] = []  # (team_id, sport_key, display_name)
    for sport_key, name in rows:
        if not is_full_team_name(name):
            continue
        key = (sport_key, name)
        if key in existing_set:
            continue
        new_rows.append((str(uuid.uuid4()), sport_key, name))

    if not new_rows:
        return

    conn.execute(
        text(
            "INSERT INTO team_reference(team_id, sport_key, display_name) VALUES "
            + ",".join(["(:id{} , :sk{} , :dn{} )".format(i, i, i) for i in range(len(new_rows))])
        ),
        {f"id{i}": r[0] for i, r in enumerate(new_rows)}
        | {f"sk{i}": r[1] for i, r in enumerate(new_rows)}
        | {f"dn{i}": r[2] for i, r in enumerate(new_rows)},
    )


def load_team_reference(conn) -> Dict[Tuple[str, str], str]:
    # map (sport_key, display_name) -> team_id
    res = conn.execute(text("SELECT sport_key, display_name, team_id FROM team_reference"))
    return {(r[0], r[1]): r[2] for r in res}


@dataclass
class EvNames:
    home: Set[str]
    away: Set[str]
    home_src: Dict[str, Set[str]]  # name -> set(bookmakers)
    away_src: Dict[str, Set[str]]


def build_event_name_index(conn) -> Dict[Tuple[str, str], EvNames]:
    # For each (sport_key, event_id), collect distinct home/away names and their sources
    res = conn.execute(
        text(
            "SELECT sport_key, event_id, bookmaker_key, home_team, away_team "
            "FROM bg_unified_lines WHERE event_id IS NOT NULL"
        )
    )
    idx: Dict[Tuple[str, str], EvNames] = {}
    for sport_key, event_id, book, home, away in res:
        key = (sport_key, event_id)
        ev = idx.get(key)
        if ev is None:
            ev = EvNames(home=set(), away=set(), home_src=collections.defaultdict(set), away_src=collections.defaultdict(set))
            idx[key] = ev
        if home:
            ev.home.add(home)
            ev.home_src[home].add(book)
        if away:
            ev.away.add(away)
            ev.away_src[away].add(book)
    return idx


def infer_aliases(conn, ref_map: Dict[Tuple[str, str], str]) -> Dict[Tuple[str, str], Tuple[str, Optional[str]]]:
    # returns mapping: (sport_key, alias) -> (team_id, source_bookmaker)
    idx = build_event_name_index(conn)
    result: Dict[Tuple[str, str], Tuple[str, Optional[str]]] = {}
    conflicts: Set[Tuple[str, str]] = set()

    for (sport_key, _event_id), ev in idx.items():
        # canonical names present in this event
        canon_home = [(n, ref_map.get((sport_key, n))) for n in ev.home]
        canon_home = [(n, tid) for n, tid in canon_home if tid]
        canon_away = [(n, ref_map.get((sport_key, n))) for n in ev.away]
        canon_away = [(n, tid) for n, tid in canon_away if tid]

        # map non-canonical home names to the first canonical home team found in this event
        if canon_home:
            _, home_tid = canon_home[0]
            for alias in ev.home:
                if (sport_key, alias) in ref_map:
                    continue  # alias is actually canonical
                key = (sport_key, alias)
                src = next(iter(ev.home_src.get(alias, {None})))
                prev = result.get(key)
                if prev and prev[0] != home_tid:
                    conflicts.add(key)
                else:
                    result[key] = (home_tid, src)
        # map non-canonical away names similarly
        if canon_away:
            _, away_tid = canon_away[0]
            for alias in ev.away:
                if (sport_key, alias) in ref_map:
                    continue
                key = (sport_key, alias)
                src = next(iter(ev.away_src.get(alias, {None})))
                prev = result.get(key)
                if prev and prev[0] != away_tid:
                    conflicts.add(key)
                else:
                    result[key] = (away_tid, src)

    # drop conflicts
    for k in conflicts:
        result.pop(k, None)
    return result


def upsert_team_aliases(conn, alias_map: Dict[Tuple[str, str], Tuple[str, Optional[str]]]) -> None:
    if not alias_map:
        return
    # Build upsert statements (Postgres ON CONFLICT)
    # We only insert when not exists; if exists with different team_id, we keep existing to avoid flips.
    for (sport_key, alias), (team_id, src) in alias_map.items():
        conn.execute(
            text(
                """
                INSERT INTO team_aliases(sport_key, alias, team_id, source_bookmaker)
                VALUES (:sport_key, :alias, :team_id, :src)
                ON CONFLICT (sport_key, alias) DO NOTHING
                """
            ),
            {"sport_key": sport_key, "alias": alias, "team_id": team_id, "src": src},
        )


def backfill_team_ids(conn) -> None:
    # direct match to canonical display_name
    conn.execute(
        text(
            """
            UPDATE bg_unified_lines bul
            SET home_team_id = tr.team_id
            FROM team_reference tr
            WHERE bul.home_team_id IS NULL
              AND bul.sport_key = tr.sport_key
              AND bul.home_team = tr.display_name
            """
        )
    )
    conn.execute(
        text(
            """
            UPDATE bg_unified_lines bul
            SET away_team_id = tr.team_id
            FROM team_reference tr
            WHERE bul.away_team_id IS NULL
              AND bul.sport_key = tr.sport_key
              AND bul.away_team = tr.display_name
            """
        )
    )

    # alias match
    conn.execute(
        text(
            """
            UPDATE bg_unified_lines bul
            SET home_team_id = ta.team_id
            FROM team_aliases ta
            WHERE bul.home_team_id IS NULL
              AND bul.sport_key = ta.sport_key
              AND bul.home_team = ta.alias
            """
        )
    )
    conn.execute(
        text(
            """
            UPDATE bg_unified_lines bul
            SET away_team_id = ta.team_id
            FROM team_aliases ta
            WHERE bul.away_team_id IS NULL
              AND bul.sport_key = ta.sport_key
              AND bul.away_team = ta.alias
            """
        )
    )


def summarize(conn) -> None:
    res = conn.execute(
        text(
            """
            SELECT
              SUM(CASE WHEN home_team_id IS NULL THEN 1 ELSE 0 END) AS home_missing,
              SUM(CASE WHEN away_team_id IS NULL THEN 1 ELSE 0 END) AS away_missing,
              COUNT(*) AS total
            FROM bg_unified_lines
            """
        )
    ).first()
    print("Backfill summary:")
    print({"home_missing": res[0], "away_missing": res[1], "total": res[2]})

    # show some unmapped examples by sport
    res2 = conn.execute(
        text(
            """
            SELECT sport_key, home_team AS name, COUNT(*) c
            FROM bg_unified_lines
            WHERE home_team_id IS NULL AND home_team IS NOT NULL AND home_team <> ''
            GROUP BY sport_key, home_team
            ORDER BY c DESC
            LIMIT 10
            """
        )
    )
    rows = list(res2)
    if rows:
        print("Top unmapped home_team names:")
        for r in rows:
            print(f"  {r[0]} :: {r[1]} ({r[2]})")


def main(argv: List[str] | None = None) -> int:
    engine = create_engine(db_url())
    try:
        with engine.begin() as conn:
            seed_team_reference(conn)
        with engine.begin() as conn:
            ref_map = load_team_reference(conn)
            alias_map = infer_aliases(conn, ref_map)
            upsert_team_aliases(conn, alias_map)
        with engine.begin() as conn:
            backfill_team_ids(conn)
        with engine.connect() as conn:
            summarize(conn)
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

