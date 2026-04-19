from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset

import os
from sqlalchemy import create_engine, text

from bountygate.utils.db_connection import insert_data
from bountygate.utils import db_connection as dbc  # type: ignore
from bountygate.utils.etl_assets import (
    active_sports,
    league_sport_map_inverted,
    odds_apiKey,
    odds_url,
    resolve_market_key,
    sport_region_map,
)
from bountygate.transformers.underdog import (
    build_ud_unified_lines,
    fetch_underdog_payload,
    normalize_underdog_payload,
    prepare_ud_analysis_frame,
)
from bountygate.data_loaders.prizepicks import fetch_prizepicks_df


# Reference the same asset
fetch_complete_asset = Asset("bg_fetch_complete")
odds_player_props_staged_asset = Asset("odds_player_props_staged")

LOGGER = logging.getLogger(__name__)
SESSION = requests.Session()
REQUEST_TIMEOUT = 10
SPLASH_LIMIT = 500
SPLASH_BASE_URL = "https://api.splashsports.com/props-service/api/props"
SLEEPER_LINES_URL = "https://api.sleeper.app/lines/available"
SLEEPER_PLAYERS_URL = "https://api.sleeper.app/players"
SLEEPER_GAMES_URL = "https://api.sleeper.app/scores/lines_game_picker"
TARGET_TABLE = "bg_unified_lines"
STAGE_TABLE_ODDS = "bg_unified_lines_stage_odds"
STAGE_TABLE_SPLASH = "bg_unified_lines_stage_splash"
STAGE_TABLE_SLEEPER = "bg_unified_lines_stage_sleeper"
STAGE_TABLE_UNDERDOG = "bg_unified_lines_stage_underdog"
STAGE_TABLE_PRIZEPICKS = "bg_unified_lines_stage_prizepicks"
COMMON_COLUMNS = [
    "player_name",
    "outcome",
    "line",
    "market_key",
    "bm_market_key",
    "price",
    "multiplier",
    "bookmaker_key",
    "sport_key",
    "sport_title",
    "commence_time",
    "home_team",
    "away_team",
    "event_id",
    "fetched_at_utc",
]


def _fetch_event_odds(
    sport: str,
    event_id: str,
    market_list: List[str],
    now_utc: datetime,
) -> List[Dict[str, Any]]:
    regions = sport_region_map.get(sport, "us")
    params: Dict[str, Any] = {
        "apiKey": odds_apiKey,
        "regions": regions,
    }
    if market_list:
        params["markets"] = ",".join(market_list)
    props_endpoint = f"{odds_url}/v4/sports/{sport}/events/{event_id}/odds"

    try:
        LOGGER.info("Fetching odds for sport=%s event=%s", sport, event_id)
        response = SESSION.get(props_endpoint, params=params, timeout=REQUEST_TIMEOUT)
        print(response.url)
        if response.status_code != 200:
            LOGGER.warning(
                "Odds API returned %s for sport=%s event=%s (markets=%s)",
                response.status_code,
                sport,
                event_id,
                params.get("markets", ""),
            )
            return []
        payload = response.json()
    except (requests.exceptions.RequestException, ValueError) as exc:
        LOGGER.warning("Failed to fetch odds for sport=%s event=%s: %s", sport, event_id, exc)
        return []

    if isinstance(payload, dict) and payload.get("message"):
        LOGGER.info("Odds API message for sport=%s event=%s: %s", sport, event_id, payload["message"])
        return []

    records: List[Dict[str, Any]] = []
    for bookmaker in payload.get("bookmakers", []):
        bookmaker_key = bookmaker.get("key")
        for market in bookmaker.get("markets", []):
            market_key = market.get("key")
            for outcome in market.get("outcomes", []):
                records.append(
                    {
                        "player_name": outcome.get("description"),
                        "outcome": outcome.get("name"),
                        "line": outcome.get("point"),
                        "market_key": market_key,
                        "bm_market_key": market_key,
                        "price": outcome.get("price"),
                        "bookmaker_key": bookmaker_key,
                        "sport_key": sport,
                        "sport_title": payload.get("sport_title"),
                        "commence_time": payload.get("commence_time"),
                        "home_team": payload.get("home_team", ""),
                        "away_team": payload.get("away_team", ""),
                        "event_id": event_id,
                        "fetched_at_utc": now_utc,
                    }
                )
    return records


def _ensure_columns(df: pd.DataFrame, defaults: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    defaults = defaults or {}
    for column in COMMON_COLUMNS:
        if column not in df.columns:
            df[column] = defaults.get(column)
    return df[COMMON_COLUMNS]


def _get_db_url() -> str:
    # Prefer an app-specific env var so we don't accidentally pick up Airflow/other services.
    env_url = os.environ.get("BOUNTYGATE_DATABASE_URL")
    if env_url:
        return env_url
    module_url = getattr(dbc, "DATABASE_URL", "")
    if module_url:
        return module_url
    return os.environ.get("DATABASE_URL", "")


def _read_table_or_empty(table_name: str) -> pd.DataFrame:
    url = _get_db_url()
    if not url:
        return pd.DataFrame(columns=COMMON_COLUMNS)

    try:
        engine = create_engine(url)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to create engine for stage read (%s): %s", table_name, exc)
        return pd.DataFrame(columns=COMMON_COLUMNS)

    try:
        # Avoid pandas.read_sql* here: some pandas/SQLAlchemy combos treat SQLAlchemy
        # engines/connections as DBAPI objects and expect `.cursor()`, which breaks.
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table_name}"))
            rows = result.fetchall()
            if not rows:
                return pd.DataFrame(columns=COMMON_COLUMNS)
            df = pd.DataFrame([dict(row._mapping) for row in rows])
        if df.empty:
            return pd.DataFrame(columns=COMMON_COLUMNS)
        return _ensure_columns(df)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to read stage table %s: %s", table_name, exc)
        return pd.DataFrame(columns=COMMON_COLUMNS)
    finally:
        engine.dispose()


@dag(
    dag_id="bg_unified",
    description="Unified betting lines pipeline sourcing Odds API, Splash, Sleeper, and Underdog data",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["betting", "odds", "unified"],
)
def bg_unified_pipeline() -> None:
    @task()
    def get_odds_markets() -> Dict[str, List[str]]:
        import pandas as pd

        url = _get_db_url()
        if not url:
            LOGGER.warning("DATABASE_URL not configured; using empty market map")
            return {}

        try:
            engine = create_engine(url)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to create engine for odds markets: %s", exc)
            return {}

        query = text(
            """
            SELECT sport_key, canonical_market_key
            FROM market_aliases
            WHERE bookmaker_key = 'draftkings'
            AND canonical_market_key IS NOT NULL
            """
        )

        try:
            with engine.connect() as conn:
                result = conn.execute(query)
                rows = result.fetchall()
                if not rows:
                    df = pd.DataFrame(columns=result.keys())
                else:
                    df = pd.DataFrame([dict(row._mapping) for row in rows])
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch DraftKings market aliases: %s", exc)
            return {}
        finally:
            engine.dispose()

        if df.empty:
            LOGGER.warning("DraftKings market alias query returned no rows")
            return {}

        df["sport_key"] = df["sport_key"].astype(str).str.strip().str.lower()
        df["canonical_market_key"] = df["canonical_market_key"].astype(str).str.strip()
        grouped = df.groupby("sport_key")["canonical_market_key"].apply(
            lambda values: sorted({value for value in values if value})
        )
        market_map = grouped.to_dict()
        LOGGER.info("Loaded DraftKings market aliases for %d sports", len(market_map))
        return market_map

    @task(outlets=[odds_player_props_staged_asset])
    def stage_odds_lines(market_map: Dict[str, List[str]]) -> int:
        """Fetch Odds API events+odds and stage to DB.

        Returns only a row count to keep XCom small.
        """

        combined_events = pd.DataFrame()
        for sport in ["americanfootball_nfl", "icehockey_nhl", "basketball_nba", "basketball_ncaab", "baseball_mlb"]:
            endpoint = f"{odds_url}/v4/sports/{sport}/events?apiKey={odds_apiKey}"
            try:
                LOGGER.info("Fetching events for sport=%s", sport)
                response = SESSION.get(endpoint, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                events = pd.DataFrame(response.json())
                events["sport_key"] = sport
            except (requests.exceptions.RequestException, ValueError) as exc:
                LOGGER.warning("Failed to retrieve events for sport=%s: %s", sport, exc)
                events = pd.DataFrame()
            combined_events = pd.concat([combined_events, events], ignore_index=True)

        if combined_events.empty:
            staged = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(staged, STAGE_TABLE_ODDS, if_exists="replace")
            return 0

        now_utc = datetime.now(timezone.utc)
        fetch_jobs: List[Iterable[Any]] = []
        for _, row in combined_events.iterrows():
            sport = row.get("sport_key")
            event_id = row.get("id")
            if not sport or not event_id:
                continue
            sport_key = str(sport).strip().lower()
            event_id_str = str(event_id)
            markets = market_map.get(sport_key, [])
            fetch_jobs.append((sport_key, event_id_str, markets))

        all_records: List[Dict[str, Any]] = []
        for sport, event_id, markets in fetch_jobs:
            all_records.extend(_fetch_event_odds(sport, event_id, markets, now_utc))

        df = pd.DataFrame(all_records) if all_records else pd.DataFrame(columns=COMMON_COLUMNS)
        if not df.empty:
            df["multiplier"] = None
            df["fetched_at_utc"] = pd.to_datetime(df["fetched_at_utc"], errors="coerce")
            df = _ensure_columns(df)
        else:
            df = pd.DataFrame(columns=COMMON_COLUMNS)

        insert_data(df, STAGE_TABLE_ODDS, if_exists="replace")
        return int(len(df))

    @task()
    def stage_splash_lines() -> int:
        headers = {
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }

        all_data: List[Dict[str, Any]] = []
        offset = 0
        total_records: Optional[int] = None

        while total_records is None or offset < (total_records or 0):
            url = f"{SPLASH_BASE_URL}?limit={SPLASH_LIMIT}&league=nfl&offset={offset}"
            try:
                LOGGER.info("Fetching Splash data offset=%s", offset)
                response = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                if response.status_code != 200:
                    LOGGER.warning("Splash API returned %s", response.status_code)
                    break
                data = response.json()
            except (requests.exceptions.RequestException, ValueError) as exc:
                LOGGER.warning("Failed to fetch Splash data offset=%s: %s", offset, exc)
                break

            if total_records is None:
                total_records = data.get("total", 0)

            all_data.extend(data.get("data", []))
            offset += SPLASH_LIMIT
            time.sleep(0.5)

        if not all_data:
            LOGGER.warning("No Splash data retrieved")
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_SPLASH, if_exists="replace")
            return 0

        target_keys = [
            "object",
            "id",
            "status",
            "league",
            "type",
            "type_display",
            "entity_name",
            "entity_id",
            "game_id",
            "game_start",
            "game",
            "team_id",
            "line",
        ]

        line_list: List[Dict[str, Any]] = []
        for line in all_data:
            temp: Dict[str, Any] = {}
            for key in target_keys:
                if key == "game":
                    temp[key] = line.get("game", {}).get("name")
                else:
                    temp[key] = line.get(key)
            line_list.append(temp)

        splash_df = pd.DataFrame(line_list)
        if splash_df.empty:
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_SPLASH, if_exists="replace")
            return 0

        splash_df["fetched_at_utc"] = datetime.now(timezone.utc)
        splash_df["player_name"] = splash_df["entity_name"]
        splash_league_map_dict = {
            "nfl": "americanfootball_nfl",
            "nba": "basketball_nba",
            "mlb": "baseball_mlb",
            "nhl": "icehockey_nhl",
        }
        splash_df["bm_market_key"] = splash_df["type_display"]
        splash_df["sport_key"] = splash_df["league"].map(splash_league_map_dict)
        splash_df["market_key"] = splash_df.apply(
            lambda row: resolve_market_key(
                "splash",
                row.get("sport_key"),
                row.get("bm_market_key"),
                fallback=row.get("type"),
            ),
            axis=1,
        )
        splash_df["sport_title"] = splash_df["league"].str.upper()
        splash_df["bookmaker_key"] = "splash"
        splash_df["multiplier"] = 1.78
        splash_df["price"] = None
        splash_df["outcome"] = None
        splash_df["event_id"] = splash_df["game_id"]
        splash_df["commence_time"] = splash_df["game_start"].astype(str)
        splash_df["home_team"] = splash_df["game"].str.split(" vs ").str[1]
        splash_df["away_team"] = splash_df["game"].str.split(" vs ").str[0]

        splash_unified_df = pd.concat(
            [
                splash_df.assign(outcome="over"),
                splash_df.assign(outcome="under"),
            ],
            ignore_index=True,
        )

        staged = _ensure_columns(splash_unified_df)
        insert_data(staged, STAGE_TABLE_SPLASH, if_exists="replace")
        return int(len(staged))

    @task()
    def stage_sleeper_lines() -> int:
        try:
            lines_resp = SESSION.get(SLEEPER_LINES_URL, timeout=REQUEST_TIMEOUT)
            lines_resp.raise_for_status()
            lines = lines_resp.json()
        except (requests.exceptions.RequestException, ValueError) as exc:
            LOGGER.warning("Failed to fetch Sleeper lines: %s", exc)
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_SLEEPER, if_exists="replace")
            return 0

        def _load_options_to_dataframe(json_data: List[Dict[str, Any]]) -> pd.DataFrame:
            all_options = [option for item in json_data for option in item.get("options", [])]
            df = pd.DataFrame(all_options)
            df.drop(columns=["metadata"], inplace=True, errors="ignore")
            return df

        lines_df = _load_options_to_dataframe(lines)

        try:
            players_resp = SESSION.get(SLEEPER_PLAYERS_URL, timeout=REQUEST_TIMEOUT)
            players_resp.raise_for_status()
            players_df = pd.DataFrame(players_resp.json())
            players_df["full_name"] = players_df["first_name"].fillna("") + " " + players_df["last_name"].fillna("")
            players_df = players_df[["player_id", "sport", "full_name", "position", "first_name", "last_name", "team"]]
        except (requests.exceptions.RequestException, ValueError) as exc:
            LOGGER.warning("Failed to fetch Sleeper players: %s", exc)
            players_df = pd.DataFrame(columns=["player_id", "sport", "full_name"])

        try:
            games_resp = SESSION.get(SLEEPER_GAMES_URL, timeout=REQUEST_TIMEOUT)
            games_resp.raise_for_status()
            games = games_resp.json()
            games_data: List[Dict[str, Any]] = []
            for game in games:
                game_info = game.get("metadata", {})
                game_info["game_id"] = game.get("game_id")
                games_data.append(game_info)
            games_df = pd.DataFrame(games_data)
        except (requests.exceptions.RequestException, ValueError) as exc:
            LOGGER.warning("Failed to fetch Sleeper games: %s", exc)
            games_df = pd.DataFrame(columns=["game_id", "home_team", "away_team", "date_time"])

        if lines_df.empty:
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_SLEEPER, if_exists="replace")
            return 0

        sleeper_df = lines_df.merge(
            players_df,
            left_on=["subject_id", "sport"],
            right_on=["player_id", "sport"],
            how="left",
            suffixes=("_line", "_player"),
        )
        sleeper_df["bm_market_key"] = sleeper_df["wager_type"]
        games_df = games_df[["game_id", "home_team", "away_team", "date_time"]].astype(str)
        sleeper_df = sleeper_df.merge(games_df, on="game_id", how="left")
        sleeper_df["player_name"] = sleeper_df["full_name"]
        if "outcome" in sleeper_df.columns:
            sleeper_df["outcome"] = sleeper_df["outcome"].fillna("").str.lower().str.capitalize()
        else:
            sleeper_df["outcome"] = None

        sleeper_league_map_dict = {
            "nfl": "americanfootball_nfl",
            "nba": "basketball_nba",
            "mlb": "baseball_mlb",
            "nhl": "icehockey_nhl",
        }
        sleeper_df["sport_key"] = sleeper_df["sport"].map(sleeper_league_map_dict)
        sleeper_df["market_key"] = sleeper_df.apply(
            lambda row: resolve_market_key(
                "sleeper",
                row.get("sport_key"),
                row.get("bm_market_key"),
                fallback=row.get("wager_type"),
            ),
            axis=1,
        )
        sleeper_df["sport_title"] = sleeper_df["sport"].str.upper()
        sleeper_df["bookmaker_key"] = "sleeper"
        sleeper_df["fetched_at_utc"] = datetime.now(timezone.utc)
        sleeper_df["multiplier"] = pd.to_numeric(sleeper_df.get("payout_multiplier"), errors="coerce")
        sleeper_df["event_id"] = sleeper_df["game_id"]
        sleeper_df["commence_time"] = sleeper_df["date_time"]
        sleeper_df["line"] = sleeper_df["outcome_value"]
        sleeper_df["price"] = None

        staged = _ensure_columns(sleeper_df)
        insert_data(staged, STAGE_TABLE_SLEEPER, if_exists="replace")
        return int(len(staged))
        # empty_df = pd.DataFrame(columns=COMMON_COLUMNS)
        # return empty_df

    @task()
    def stage_prizepicks_lines() -> int:
        try:
            df = fetch_prizepicks_df(SESSION, timeout=REQUEST_TIMEOUT)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch PrizePicks lines: %s", exc)
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_PRIZEPICKS, if_exists="replace")
            return 0
        if df is None or df.empty:
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_PRIZEPICKS, if_exists="replace")
            return 0
        # Ensure correct shape and types
        df = _ensure_columns(df, defaults={"bookmaker_key": "prizepicks", "multiplier": None})
        if "bookmaker_key" in df.columns:
            df["bookmaker_key"] = "prizepicks"
        if "fetched_at_utc" in df.columns:
            df["fetched_at_utc"] = pd.to_datetime(df["fetched_at_utc"], errors="coerce")
        else:
            df["fetched_at_utc"] = datetime.now(timezone.utc)
        df["market_key"] = df.apply(
            lambda row: resolve_market_key(
                "prizepicks",
                row.get("sport_key"),
                row.get("bm_market_key"),
                fallback=row.get("market_key"),
            ),
            axis=1,
        )
        insert_data(df, STAGE_TABLE_PRIZEPICKS, if_exists="replace")
        return int(len(df))

    @task()
    def stage_underdog_lines() -> int:
        try:
            payload = fetch_underdog_payload(SESSION, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as exc:
            LOGGER.warning("Failed to fetch Underdog payload: %s", exc)
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_UNDERDOG, if_exists="replace")
            return 0

        if not payload:
            LOGGER.warning("Underdog payload is empty")
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_UNDERDOG, if_exists="replace")
            return 0

        staging_tables = normalize_underdog_payload(payload)
        ud_analysis_df = prepare_ud_analysis_frame(staging_tables)
        if ud_analysis_df.empty:
            LOGGER.warning("Underdog analysis frame is empty after normalization")
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_UNDERDOG, if_exists="replace")
            return 0

        unified_df = build_ud_unified_lines(ud_analysis_df, league_sport_map_inverted)
        if unified_df.empty:
            LOGGER.warning("No unified Underdog lines produced")
            empty = pd.DataFrame(columns=COMMON_COLUMNS)
            insert_data(empty, STAGE_TABLE_UNDERDOG, if_exists="replace")
            return 0

        unified_df = _ensure_columns(unified_df, defaults={"bookmaker_key": "underdog", "multiplier": None})
        unified_df["bookmaker_key"] = "underdog"
        if "fetched_at_utc" in unified_df.columns:
            unified_df["fetched_at_utc"] = pd.to_datetime(unified_df["fetched_at_utc"], errors="coerce")
        else:
            unified_df["fetched_at_utc"] = datetime.now(timezone.utc)
        unified_df["multiplier"] = pd.to_numeric(unified_df.get("multiplier"), errors="coerce")
        insert_data(unified_df, STAGE_TABLE_UNDERDOG, if_exists="replace")
        return int(len(unified_df))

    @task(outlets=[fetch_complete_asset])
    def unify_and_load_lines(
        odds_rows: int,
        splash_rows: int,
        sleeper_rows: int,
        underdog_rows: int,
        prizepicks_rows: int,
    ) -> None:
        """Unify sources and load directly to DB.

        Avoids pushing the full unified dataset through TaskFlow XCom, which can
        destabilize Airflow (DB bloat / worker OOM) when the dataset is large.
        """

        LOGGER.info(
            "Stage row counts - odds=%s splash=%s sleeper=%s underdog=%s prizepicks=%s",
            odds_rows,
            splash_rows,
            sleeper_rows,
            underdog_rows,
            prizepicks_rows,
        )

        if (odds_rows + splash_rows + sleeper_rows + underdog_rows + prizepicks_rows) == 0:
            raise AirflowSkipException("No betting lines retrieved from any source")

        url = _get_db_url()
        if not url:
            raise AirflowSkipException("No database URL configured; skipping load")

        # Pure SQL unify + dedupe in Postgres to avoid loading large datasets into worker memory.
        # Stage tables may have type drift (e.g., multiplier as text in one stage, double precision in another),
        # so we cast into a consistent projection before UNION ALL.
        cols_sql = ", ".join(COMMON_COLUMNS)

        def _select_from_stage(table_name: str) -> str:
            # Cast most fields to text; cast price/multiplier to double precision when parseable.
            # Keep fetched_at_utc as timestamptz for correct ordering.
            return f"""
            SELECT
                player_name::text AS player_name,
                outcome::text AS outcome,
                line::text AS line,
                market_key::text AS market_key,
                bm_market_key::text AS bm_market_key,
                CASE
                    WHEN price IS NULL THEN NULL
                    WHEN price::text ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN price::text::double precision
                    ELSE NULL
                END AS price,
                CASE
                    WHEN multiplier IS NULL THEN NULL
                    WHEN multiplier::text ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN multiplier::text::double precision
                    ELSE NULL
                END AS multiplier,
                bookmaker_key::text AS bookmaker_key,
                sport_key::text AS sport_key,
                sport_title::text AS sport_title,
                commence_time::text AS commence_time,
                home_team::text AS home_team,
                away_team::text AS away_team,
                event_id::text AS event_id,
                fetched_at_utc::timestamptz AS fetched_at_utc
            FROM {table_name}
            """

        unify_sql = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = '{TARGET_TABLE}'
            ) THEN
                EXECUTE 'CREATE TABLE {TARGET_TABLE} (LIKE {STAGE_TABLE_ODDS} INCLUDING ALL)';
            END IF;
        END $$;

        TRUNCATE TABLE {TARGET_TABLE};

        WITH unified AS (
            {_select_from_stage(STAGE_TABLE_ODDS)}
            UNION ALL
            {_select_from_stage(STAGE_TABLE_SPLASH)}
            UNION ALL
            {_select_from_stage(STAGE_TABLE_SLEEPER)}
            UNION ALL
            {_select_from_stage(STAGE_TABLE_UNDERDOG)}
            UNION ALL
            {_select_from_stage(STAGE_TABLE_PRIZEPICKS)}
        ), ranked AS (
            SELECT
                {cols_sql},
                ROW_NUMBER() OVER (
                    PARTITION BY bookmaker_key, sport_key, event_id, market_key, player_name, outcome, line
                    ORDER BY fetched_at_utc DESC NULLS LAST
                ) AS rn
            FROM unified
            WHERE sport_key IS NOT NULL
        )
        INSERT INTO {TARGET_TABLE} ({cols_sql})
        SELECT {cols_sql}
        FROM ranked
        WHERE rn = 1;
        """

        try:
            engine = create_engine(url)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to create engine for unified load: %s", exc)
            raise

        try:
            with engine.begin() as conn:
                conn.execute(text(unify_sql))
                res = conn.execute(text(f"SELECT COUNT(*) AS cnt FROM {TARGET_TABLE}"))
                count_row = res.fetchone()
                loaded_count = int(count_row[0]) if count_row is not None else 0
            LOGGER.info("Loaded %d unified lines into %s", loaded_count, TARGET_TABLE)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed SQL unify/load into %s: %s", TARGET_TABLE, exc)
            raise
        finally:
            engine.dispose()

    odds_market_map = get_odds_markets()
    odds_rows = stage_odds_lines(odds_market_map)
    splash_rows = stage_splash_lines()
    sleeper_rows = stage_sleeper_lines()
    underdog_rows = stage_underdog_lines()
    prizepicks_rows = stage_prizepicks_lines()
    unify_and_load_lines(odds_rows, splash_rows, sleeper_rows, underdog_rows, prizepicks_rows)


dag = bg_unified_pipeline()
