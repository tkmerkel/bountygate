"""DAG: bg_dimensional_model — Kimball dims + fact_odds_snapshot loader.

Triggered by the bg_normalization_complete asset (runs in parallel with the
existing bg_unified_analysis DAG). Upserts dim_event / dim_player / dim_market
from the normalized MV, then appends every quote into fact_odds_snapshot
(append-only, dedup on quote_hash), then prunes old snapshot rows.

Writes use bulk_append_new (fact) and ON CONFLICT upserts (dims). bg_event_id is
the same deterministic uuid5 as bg_unified_analysis (see bg_analytics_lib.common).
"""
from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset
from sqlalchemy import text

# dags dir is on sys.path in Airflow.
from bg_analytics_lib.common import (
    MODEL_LOADED,
    NORMALIZATION_COMPLETE,
    fetch_normalized_with_event_id,
    get_engine,
    sharp_books,
)
from bg_arb_pipeline_lib.db import bulk_append_new

_PUNCT_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE = re.compile(r"\s+")


def _normalize_player_name(name) -> str:
    """lower / strip / collapse-whitespace / remove punctuation."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = str(name).strip().lower()
    s = _PUNCT_RE.sub("", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _player_id(sport_key: str, normalized_name: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"{sport_key}|{normalized_name}").hex


def _base_market_key(market_key: str) -> str:
    """Strip a trailing '_alternate' suffix; otherwise return market_key unchanged."""
    if market_key is None:
        return market_key
    mk = str(market_key)
    if mk.endswith("_alternate"):
        return mk[: -len("_alternate")]
    return mk


@dag(
    dag_id="bg_dimensional_model",
    description="Upsert Kimball dims and append fact_odds_snapshot from normalized lines",
    schedule=[NORMALIZATION_COMPLETE],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "model"],
)
def bg_dimensional_model_pipeline() -> None:

    @task()
    def upsert_dims() -> int:
        """Upsert dim_event, dim_player, dim_market from the normalized MV."""
        df = fetch_normalized_with_event_id()
        if df is None or df.empty:
            raise AirflowSkipException("No normalized rows to build dims from")

        # ---- dim_event ----
        event_cols = [
            "bg_event_id",
            "sport_key",
            "home_team_id",
            "away_team_id",
            "home_team_name",
            "away_team_name",
            "commence_at_utc",
        ]
        events = df.copy()
        for c in event_cols:
            if c not in events.columns:
                events[c] = pd.NA
        events = events[event_cols].drop_duplicates(subset=["bg_event_id"])
        events["commence_at_utc"] = pd.to_datetime(
            events["commence_at_utc"], errors="coerce", utc=True
        )

        # ---- dim_player ----
        players = df.copy()
        if "player_name" not in players.columns:
            players["player_name"] = pd.NA
        players["player_name"] = (
            players["player_name"].astype("string").str.strip().replace("", pd.NA).replace("None", pd.NA)
        )
        players = players.dropna(subset=["player_name", "sport_key"]).copy()
        players["normalized_name"] = players["player_name"].apply(_normalize_player_name)
        players = players[players["normalized_name"] != ""]
        players = players[["sport_key", "player_name", "normalized_name"]].drop_duplicates(
            subset=["sport_key", "normalized_name"]
        )
        players["player_id"] = players.apply(
            lambda r: _player_id(str(r["sport_key"]), str(r["normalized_name"])), axis=1
        )

        # ---- dim_market ----
        markets = df.copy()
        if "market_key" not in markets.columns:
            markets["market_key"] = pd.NA
        markets["market_key"] = (
            markets["market_key"].astype("string").str.strip().replace("", pd.NA).replace("None", pd.NA)
        )
        markets = markets.dropna(subset=["market_key"]).copy()
        markets = markets[["market_key", "sport_key"]].drop_duplicates(subset=["market_key"])
        markets["is_alternate"] = markets["market_key"].apply(
            lambda m: "alternate" in str(m).lower()
        )
        markets["base_market_key"] = markets["market_key"].apply(_base_market_key)

        def _opt(v):
            return None if (v is None or pd.isna(v)) else v

        engine = get_engine()
        try:
            with engine.begin() as conn:
                # dim_event: upsert on bg_event_id; never null-out an existing event.
                for _, row in events.iterrows():
                    commence = row["commence_at_utc"]
                    commence_param = (
                        None if pd.isna(commence) else commence.to_pydatetime()
                    )
                    conn.execute(
                        text(
                            "INSERT INTO dim_event "
                            "(bg_event_id, sport_key, home_team_id, away_team_id, "
                            " home_team_name, away_team_name, commence_at_utc, updated_at_utc) "
                            "VALUES (:eid, :sk, :hid, :aid, :hn, :an, :ca, now()) "
                            "ON CONFLICT (bg_event_id) DO UPDATE SET "
                            "  sport_key = COALESCE(EXCLUDED.sport_key, dim_event.sport_key), "
                            "  home_team_id = COALESCE(EXCLUDED.home_team_id, dim_event.home_team_id), "
                            "  away_team_id = COALESCE(EXCLUDED.away_team_id, dim_event.away_team_id), "
                            "  home_team_name = COALESCE(EXCLUDED.home_team_name, dim_event.home_team_name), "
                            "  away_team_name = COALESCE(EXCLUDED.away_team_name, dim_event.away_team_name), "
                            "  commence_at_utc = COALESCE(EXCLUDED.commence_at_utc, dim_event.commence_at_utc), "
                            "  updated_at_utc = now()"
                        ),
                        {
                            "eid": _opt(row["bg_event_id"]),
                            "sk": _opt(row["sport_key"]),
                            "hid": _opt(row["home_team_id"]),
                            "aid": _opt(row["away_team_id"]),
                            "hn": _opt(row["home_team_name"]),
                            "an": _opt(row["away_team_name"]),
                            "ca": commence_param,
                        },
                    )

                # dim_player: insert new players only; preserve external ids / team_id.
                for _, row in players.iterrows():
                    conn.execute(
                        text(
                            "INSERT INTO dim_player "
                            "(player_id, sport_key, full_name, normalized_name) "
                            "VALUES (:pid, :sk, :fn, :nn) "
                            "ON CONFLICT (player_id) DO UPDATE SET "
                            "  full_name = COALESCE(dim_player.full_name, EXCLUDED.full_name)"
                        ),
                        {
                            "pid": str(row["player_id"]),
                            "sk": _opt(row["sport_key"]),
                            "fn": _opt(row["player_name"]),
                            "nn": _opt(row["normalized_name"]),
                        },
                    )

                # dim_market: upsert market metadata.
                for _, row in markets.iterrows():
                    conn.execute(
                        text(
                            "INSERT INTO dim_market "
                            "(market_key, sport_key, is_alternate, base_market_key) "
                            "VALUES (:mk, :sk, :alt, :base) "
                            "ON CONFLICT (market_key) DO UPDATE SET "
                            "  sport_key = COALESCE(EXCLUDED.sport_key, dim_market.sport_key), "
                            "  is_alternate = EXCLUDED.is_alternate, "
                            "  base_market_key = COALESCE(EXCLUDED.base_market_key, dim_market.base_market_key)"
                        ),
                        {
                            "mk": _opt(row["market_key"]),
                            "sk": _opt(row["sport_key"]),
                            "alt": bool(row["is_alternate"]),
                            "base": _opt(row["base_market_key"]),
                        },
                    )
        finally:
            engine.dispose()

        return int(len(events))

    @task(outlets=[MODEL_LOADED])
    def load_fact_odds_snapshot(_n_dims: int) -> int:
        """Project the normalized MV into fact_odds_snapshot columns (migration 009)."""
        df = fetch_normalized_with_event_id()
        if df is None or df.empty:
            raise AirflowSkipException("No normalized rows for fact_odds_snapshot")

        sharp = set(sharp_books())

        work = df.copy()
        # Coerce text key columns.
        for col in ("bg_event_id", "sport_key", "bookmaker_key", "market_key", "player_name", "outcome"):
            if col not in work.columns:
                work[col] = pd.NA
            work[col] = work[col].astype("string").str.strip().replace("", pd.NA).replace("None", pd.NA)

        work = work.dropna(subset=["bg_event_id", "sport_key", "bookmaker_key", "market_key"]).copy()

        # Numeric coercion.
        work["line"] = pd.to_numeric(work.get("line"), errors="coerce")
        work["price_decimal"] = pd.to_numeric(work.get("price"), errors="coerce")
        work = work[work["price_decimal"].notna() & (work["price_decimal"] > 0)].copy()
        if work.empty:
            raise AirflowSkipException("No priced quotes for fact_odds_snapshot")

        # Timestamps -> naive UTC (bulk COPY strips tz anyway; we localize for hashing).
        work["commence_at_utc"] = pd.to_datetime(
            work.get("commence_at_utc"), errors="coerce", utc=True
        ).dt.tz_localize(None)
        work["fetched_at_utc"] = pd.to_datetime(
            work.get("fetched_at_utc"), errors="coerce", utc=True
        ).dt.tz_localize(None)
        work = work.dropna(subset=["commence_at_utc", "fetched_at_utc"]).copy()

        # Derived columns.
        work["side"] = work["outcome"].astype("string").str.lower()
        work["base_market_key"] = work["market_key"].apply(_base_market_key)
        work["implied_prob"] = work["price_decimal"].apply(
            lambda d: (1.0 / d) if (d is not None and d > 0) else None
        )
        work["is_sharp"] = work["bookmaker_key"].isin(sharp)
        work["is_consensus_source"] = True

        def _quote_hash(row: pd.Series) -> str:
            key = "|".join(
                str(v)
                for v in [
                    row["bg_event_id"],
                    row["bookmaker_key"],
                    row["market_key"],
                    row.get("player_name"),
                    row["side"],
                    row["line"],
                    row["fetched_at_utc"],
                ]
            )
            return hashlib.md5(key.encode()).hexdigest()

        work["quote_hash"] = work.apply(_quote_hash, axis=1)
        # A quote_hash collision within one batch (identical key) is real duplicate data.
        work = work.drop_duplicates(subset=["quote_hash"])

        out_cols = [
            "quote_hash",
            "bg_event_id",
            "sport_key",
            "bookmaker_key",
            "market_key",
            "base_market_key",
            "player_name",
            "side",
            "line",
            "price_decimal",
            "implied_prob",
            "commence_at_utc",
            "fetched_at_utc",
            "is_sharp",
            "is_consensus_source",
        ]
        out = work[out_cols].copy()

        return int(bulk_append_new(out, "fact_odds_snapshot", "quote_hash"))

    @task()
    def prune_fact_odds_snapshot(_n_loaded: int) -> int:
        """Retention prune: drop snapshot rows older than 14 days."""
        engine = get_engine()
        try:
            with engine.begin() as conn:
                result = conn.execute(
                    text(
                        "DELETE FROM fact_odds_snapshot "
                        "WHERE fetched_at_utc < now() - interval '14 days'"
                    )
                )
                deleted = result.rowcount or 0
        finally:
            engine.dispose()
        return int(deleted)

    ids = upsert_dims()
    n = load_fact_odds_snapshot(ids)
    prune_fact_odds_snapshot(n)


dag = bg_dimensional_model_pipeline()
