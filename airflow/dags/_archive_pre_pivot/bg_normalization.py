"""DAG: Normalize unified lines, seed aliases, run DQ, refresh MV.

Runs hourly. Uses the shared db connection string and SQLAlchemy inside the container.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import Asset

# Reference the same asset
fetch_complete_asset = Asset("bg_fetch_complete")
normalization_complete_asset = Asset("bg_normalization_complete")

@dag(
    dag_id="bg_normalization",
    start_date=datetime(2025, 1, 1),
    schedule=[fetch_complete_asset],
    catchup=False,
    max_active_runs=1,
    tags=["bountygate", "normalize", "dq"],
)
def bg_normalization():
    @task()
    def load_aliases_from_csv() -> None:
        import os
        from pathlib import Path
        from sqlalchemy import create_engine, text
        from bountygate.utils import db_connection as dbc  # type: ignore

        url = os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")
        engine = create_engine(url)

        # CSV directory baked into image at build time
        csv_dir = Path("/opt/bountygate-aliases")
        files = sorted(csv_dir.glob("*.csv"))
        if not files:
            return

        import csv as _csv
        with engine.begin() as conn:
            for file in files:
                with file.open("r", encoding="utf-8") as f:
                    rdr = _csv.DictReader(f)
                    for row in rdr:
                        sport_key = (row.get("sport_key") or "").strip()
                        alias = (row.get("alias") or "").strip()
                        canonical_name = (row.get("canonical_name") or "").strip()
                        src = (row.get("source_bookmaker") or "").strip() or None
                        abbr = (row.get("abbreviation") or "").strip() or None
                        if not sport_key or not alias or not canonical_name:
                            continue
                        # ensure team_reference exists
                        ref = conn.execute(text(
                            "SELECT team_id FROM team_reference WHERE sport_key=:s AND display_name=:n"
                        ), {"s": sport_key, "n": canonical_name}).first()
                        if ref is None:
                            import uuid as _uuid
                            tid = str(_uuid.uuid4())
                            conn.execute(text(
                                "INSERT INTO team_reference(team_id, sport_key, display_name, abbreviation) "
                                "VALUES (:id, :s, :n, :abbr)"
                            ), {"id": tid, "s": sport_key, "n": canonical_name, "abbr": abbr})
                        else:
                            tid = ref[0]
                        # upsert alias
                        conn.execute(text(
                            "INSERT INTO team_aliases(sport_key, alias, team_id, source_bookmaker) "
                            "VALUES (:s, :a, :tid, :src) ON CONFLICT (sport_key, alias) DO NOTHING"
                        ), {"s": sport_key, "a": alias, "tid": tid, "src": src})

            # backfill
            conn.execute(text(
                "UPDATE bg_unified_lines bul SET home_team_id = ta.team_id "
                "FROM team_aliases ta WHERE bul.home_team_id IS NULL AND bul.sport_key = ta.sport_key AND bul.home_team = ta.alias"
            ))
            conn.execute(text(
                "UPDATE bg_unified_lines bul SET away_team_id = ta.team_id "
                "FROM team_aliases ta WHERE bul.away_team_id IS NULL AND bul.sport_key = ta.sport_key AND bul.away_team = ta.alias"
            ))

    @task()
    def backfill_normalized_fields() -> None:
        # Keep commence_at_utc up to date for any newly inserted rows
        import os
        import sys
        from pathlib import Path
        from sqlalchemy import create_engine, text
        from bountygate.utils import db_connection as dbc  # type: ignore

        url = os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")
        engine = create_engine(url)
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE bg_unified_lines SET commence_at_utc = CASE "
                "WHEN commence_time IS NULL OR commence_time='' THEN NULL "
                "WHEN commence_time ~ 'Z$' THEN (commence_time)::timestamptz "
                "WHEN commence_time ~ '\\+\\d{2}:\\d{2}$' THEN (commence_time)::timestamptz "
                "WHEN commence_time ~ '\\+\\d{4}$' THEN (regexp_replace(commence_time, '(\\+\\d{2})(\\d{2})$', '\\1:\\2'))::timestamptz "
                "ELSE commence_at_utc END "
                "WHERE commence_at_utc IS NULL"
            ))

    @task()
    def prune_past_rows() -> None:
        """Delete rows already commenced and any older than 3 days by fetched_at_utc."""
        import os
        from sqlalchemy import create_engine, text
        from bountygate.utils import db_connection as dbc  # type: ignore

        url = os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")
        engine = create_engine(url)
        with engine.begin() as conn:
            # Remove rows that already commenced, and any rows older than 3 days by fetched_at_utc
            conn.execute(text(
                "DELETE FROM bg_unified_lines "
                "WHERE (commence_at_utc IS NOT NULL AND commence_at_utc < now()) "
                "   OR (fetched_at_utc IS NOT NULL AND fetched_at_utc < (now() - INTERVAL '3 days'))"
            ))

    @task()
    def dq_checks() -> None:
        import os, sys, json
        from pathlib import Path
        from sqlalchemy import create_engine, text
        from bountygate.utils import db_connection as dbc  # type: ignore

        url = os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")
        engine = create_engine(url)
        with engine.begin() as conn:
            r = conn.execute(text(
                "SELECT COUNT(*) total, SUM((commence_at_utc IS NULL)::int) miss_time, "
                "SUM((home_team_id IS NULL)::int) miss_home, SUM((away_team_id IS NULL)::int) miss_away FROM bg_unified_lines"
            )).first()
            total, miss_time, miss_home, miss_away = r
            for name, val in [
                ("bg_unified_lines.total", total),
                ("bg_unified_lines.missing.commence_at_utc", miss_time),
                ("bg_unified_lines.missing.home_team_id", miss_home),
                ("bg_unified_lines.missing.away_team_id", miss_away),
            ]:
                conn.execute(text(
                    "INSERT INTO dq_metrics(metric_name, metric_value, dimensions, notes) "
                    "VALUES (:n, :v, CAST(:d AS jsonb), NULL)"
                ), {"n": name, "v": val, "d": json.dumps({})})

            rows = conn.execute(text(
                "SELECT bookmaker_key, sport_key, COUNT(*) cnt, "
                "SUM(CASE WHEN commence_time ~ 'T' THEN 1 ELSE 0 END) has_T, "
                "SUM(CASE WHEN commence_time ~ 'Z$' THEN 1 ELSE 0 END) ends_Z, "
                "SUM(CASE WHEN commence_time ~ '\\+\\d{2}:?\\d{2}$' THEN 1 ELSE 0 END) has_tz_offset "
                "FROM bg_unified_lines GROUP BY 1,2"
            ))
            for bk, sk, cnt, has_t, ends_z, has_off in rows:
                dims = {"bookmaker": bk, "sport": sk}
                for metric, val in [
                    ("bg_unified_lines.timefmt.count", cnt),
                    ("bg_unified_lines.timefmt.has_T", has_t),
                    ("bg_unified_lines.timefmt.ends_Z", ends_z),
                    ("bg_unified_lines.timefmt.has_tz_offset", has_off),
                ]:
                    conn.execute(text(
                        "INSERT INTO dq_metrics(metric_name, metric_value, dimensions, notes) "
                        "VALUES (:n, :v, CAST(:d AS jsonb), NULL)"
                    ), {"n": metric, "v": val, "d": json.dumps(dims)})

    @task()
    def load_market_aliases_from_csv() -> None:
        """Load market alias CSVs and backfill market_key via canonical mapping."""
        import os
        from pathlib import Path
        from sqlalchemy import create_engine, text
        from bountygate.utils import db_connection as dbc  # type: ignore

        url = os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")
        engine = create_engine(url)

        csv_dir = Path("/opt/bountygate-market-aliases")
        files = sorted(csv_dir.glob("*.csv"))
        if not files:
            return

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

        import csv as _csv
        with engine.begin() as conn:
            ensure_table(conn)
            batch = []
            for file in files:
                with file.open("r", encoding="utf-8") as f:
                    rdr = _csv.DictReader(f)
                    for r in rdr:
                        bk = (r.get("bookmaker_key") or "").strip()
                        sk = (r.get("sport_key") or "").strip()
                        bm = (r.get("bm_market_key") or "").strip()
                        ck = (r.get("canonical_market_key") or "").strip() or None
                        notes = (r.get("notes") or "").strip() or None
                        if not bk or not sk or not bm:
                            continue
                        batch.append({"bk": bk, "sk": sk, "bm": bm, "ck": ck, "notes": notes})
            if batch:
                conn.execute(text(
                    """
                    INSERT INTO market_aliases(bookmaker_key, sport_key, bm_market_key, canonical_market_key, notes)
                    VALUES (:bk, :sk, :bm, :ck, :notes)
                    ON CONFLICT (bookmaker_key, sport_key, bm_market_key)
                    DO UPDATE SET canonical_market_key = EXCLUDED.canonical_market_key,
                                  notes = EXCLUDED.notes
                    """
                ), batch)

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

    @task()
    def normalize_team_fields() -> None:
        """Normalize dict/JSON-like team strings and backfill team IDs.

        Handles NBA/MLB/NHL rows where home_team/away_team contains a Python/JSON-like
        blob (e.g., {'team': 'LAD', 'name': 'Dodgers', ...}). Converts to JSON,
        extracts team code or fallback name, updates text fields, then backfills IDs.
        """
        import os
        from sqlalchemy import create_engine, text
        from bountygate.utils import db_connection as dbc  # type: ignore

        url = os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")
        engine = create_engine(url)
        with engine.begin() as conn:
            # Update home_team from blob → code/name
            conn.execute(text(
                """
                UPDATE bg_unified_lines SET home_team = COALESCE(
                  (NULLIF(
                     regexp_replace(
                       regexp_replace(
                         regexp_replace(
                           regexp_replace(home_team, '''', '"', 'g'),
                           'True', 'true', 'g'
                         ),
                         'False', 'false', 'g'
                       ),
                       'None', 'null', 'g'
                     ), ''
                   )::jsonb ->> 'team'),
                  (NULLIF(
                     regexp_replace(
                       regexp_replace(
                         regexp_replace(regexp_replace(home_team, '''', '"', 'g'), 'True', 'true', 'g'),
                         'False', 'false', 'g'
                       ),
                       'None', 'null', 'g'
                     ), ''
                   )::jsonb ->> 'name')
                )
                WHERE sport_key IN ('basketball_nba','basketball_ncaab','baseball_mlb','icehockey_nhl')
                  AND home_team IS NOT NULL
                  AND home_team ~ '^[{\[]';
                """
            ))

            # Update away_team from blob → code/name
            conn.execute(text(
                """
                UPDATE bg_unified_lines SET away_team = COALESCE(
                  (NULLIF(
                     regexp_replace(
                       regexp_replace(
                         regexp_replace(
                           regexp_replace(away_team, '''', '"', 'g'),
                           'True', 'true', 'g'
                         ),
                         'False', 'false', 'g'
                       ),
                       'None', 'null', 'g'
                     ), ''
                   )::jsonb ->> 'team'),
                  (NULLIF(
                     regexp_replace(
                       regexp_replace(
                         regexp_replace(regexp_replace(away_team, '''', '"', 'g'), 'True', 'true', 'g'),
                         'False', 'false', 'g'
                       ),
                       'None', 'null', 'g'
                     ), ''
                   )::jsonb ->> 'name')
                )
                WHERE sport_key IN ('basketball_nba','basketball_ncaab','baseball_mlb','icehockey_nhl')
                  AND away_team IS NOT NULL
                  AND away_team ~ '^[{\[]';
                """
            ))

            # Backfill team IDs based on aliases after normalization
            conn.execute(text(
                """
                UPDATE bg_unified_lines bul
                SET home_team_id = ta.team_id
                FROM team_aliases ta
                WHERE bul.home_team_id IS NULL
                  AND bul.sport_key = ta.sport_key
                  AND bul.home_team = ta.alias
                """
            ))
            conn.execute(text(
                """
                UPDATE bg_unified_lines bul
                SET away_team_id = ta.team_id
                FROM team_aliases ta
                WHERE bul.away_team_id IS NULL
                  AND bul.sport_key = ta.sport_key
                  AND bul.away_team = ta.alias
                """
            ))

    @task(outlets=[normalization_complete_asset])
    def ensure_mv_and_refresh() -> None:
        import os
        from sqlalchemy import create_engine, text
        from bountygate.utils import db_connection as dbc  # type: ignore

        url = os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")
        engine = create_engine(url)
        # Ensure MV exists (transaction is fine here)
        with engine.begin() as conn:
            exists = conn.execute(text("SELECT 1 FROM pg_matviews WHERE schemaname='public' AND matviewname='bg_unified_lines_normalized_mv'"))
            if exists.first() is None:
                conn.execute(text(
                    "CREATE MATERIALIZED VIEW bg_unified_lines_normalized_mv AS "
                    "SELECT * FROM bg_unified_lines_normalized"
                ))

        # Run refresh outside a transaction; try CONCURRENTLY, then fallback
        # Postgres requires CONCURRENTLY outside a transaction block.
        # Use AUTOCOMMIT and separate connections to avoid aborted txn state.
        try:
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY bg_unified_lines_normalized_mv"))
        except Exception:
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW bg_unified_lines_normalized_mv"))

    s = load_aliases_from_csv()
    mk = load_market_aliases_from_csv()
    n = normalize_team_fields()
    b = backfill_normalized_fields()
    p = prune_past_rows()
    d = dq_checks()
    m = ensure_mv_and_refresh()

    # Order: team aliases -> market aliases -> normalize teams -> timestamps -> prune -> DQ -> refresh MV
    s >> mk >> n >> b >> p >> d >> m


dag = bg_normalization()


