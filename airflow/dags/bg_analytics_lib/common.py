"""Shared helpers for the BountyGate analytics DAGs.

This module deliberately re-implements (copies) the proven event-id logic from
``bg_unified_analysis_dag.py`` rather than importing that module — importing a
DAG module triggers Airflow DAG-parsing side-effects. The uuid5-over-canonical
``home|away|commence`` logic here MUST stay byte-for-byte equivalent to
``bg_unified_analysis_dag._fetch_and_prep_normalized`` so that ``bg_event_id``
matches across both pipelines.

All writes elsewhere use ``bulk_replace`` / ``bulk_append_new`` from
``bg_arb_pipeline_lib.db``. This module only exposes read helpers + an engine
factory for ad-hoc upserts.
"""
from __future__ import annotations

import os
import uuid
from typing import List

import pandas as pd
from airflow.sdk import Asset
from sqlalchemy import create_engine

from bountygate.utils.db_connection import fetch_data

# ---------------------------------------------------------------------------
# Asset name constants — the analytics chain anchors on bg_normalization_complete.
# ---------------------------------------------------------------------------
NORMALIZATION_COMPLETE = Asset("bg_normalization_complete")
MODEL_LOADED = Asset("bg_model_loaded")
METHODOLOGY_COMPLETE = Asset("bg_methodology_complete")
GOOD_BETS_READY = Asset("bg_good_bets_ready")


# ---------------------------------------------------------------------------
# Engine factory — mirrors bg_arb_pipeline_lib.db connect_args so ad-hoc
# upserts share the same statement_timeout / keepalive behaviour.
# ---------------------------------------------------------------------------
_CONNECT_ARGS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
    "options": "-c statement_timeout=300000 -c lock_timeout=60000",
}


def _resolve_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        from bountygate.utils.db_connection import DATABASE_URL as _DB  # type: ignore

        url = _DB
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return url


def get_engine():
    """Return a SQLAlchemy engine on DATABASE_URL with the standard connect_args.

    Caller is responsible for ``engine.dispose()``.
    """
    return create_engine(_resolve_database_url(), connect_args=_CONNECT_ARGS)


# ---------------------------------------------------------------------------
# Event-id logic — COPIED from bg_unified_analysis_dag.py. Keep in sync.
# ---------------------------------------------------------------------------
REQUIRED_EVENT_FIELDS: List[str] = ["home_team_name", "away_team_name", "commence_at_utc"]


def _stringify_uuid_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    uuid_columns: List[str] = [
        column
        for column in df.columns
        if df[column].apply(lambda value: isinstance(value, uuid.UUID)).any()
    ]
    for column in uuid_columns:
        df[column] = df[column].apply(
            lambda value: str(value) if isinstance(value, uuid.UUID) else value
        )
    return df


def _canonicalize_commence_for_event_id(
    df: pd.DataFrame,
    *,
    tolerance_minutes: int,
) -> pd.Series:
    """Return a canonical commence_at_utc per (home, away, UTC-day) within a tolerance."""
    if df is None or df.empty or "commence_at_utc" not in df.columns:
        return pd.Series(dtype="datetime64[ns, UTC]")

    tolerance = pd.Timedelta(minutes=max(0, int(tolerance_minutes)))
    working = df[["home_team_name", "away_team_name", "commence_at_utc"]].copy()
    working["commence_at_utc"] = pd.to_datetime(
        working["commence_at_utc"], errors="coerce", utc=True
    )
    working["event_day_utc"] = working["commence_at_utc"].dt.floor("D")

    sort_cols = ["home_team_name", "away_team_name", "event_day_utc", "commence_at_utc"]
    working.sort_values(sort_cols, inplace=True)

    def _cluster_within_tolerance(times: pd.Series) -> pd.Series:
        diffs = times.diff()
        return (diffs > tolerance).cumsum()

    working["_time_cluster"] = working.groupby(
        ["home_team_name", "away_team_name", "event_day_utc"],
        dropna=False,
        sort=False,
    )["commence_at_utc"].transform(_cluster_within_tolerance)

    working["_commence_canonical"] = working.groupby(
        ["home_team_name", "away_team_name", "event_day_utc", "_time_cluster"],
        dropna=False,
        sort=False,
    )["commence_at_utc"].transform("min")

    return working["_commence_canonical"].reindex(df.index)


def fetch_normalized_with_event_id() -> pd.DataFrame:
    """Fetch normalized lines and assign canonical bg_event_id per row.

    Reads ``bg_unified_lines_normalized_mv`` and assigns a deterministic
    ``bg_event_id`` = uuid5(NAMESPACE_URL, "{home}|{away}|{commence_iso}").hex,
    identical to bg_unified_analysis.
    """
    df = fetch_data("SELECT * FROM bg_unified_lines_normalized_mv")
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df = _stringify_uuid_columns(df)
    df["commence_at_utc"] = pd.to_datetime(df.get("commence_at_utc"), errors="coerce", utc=True)

    for column in REQUIRED_EVENT_FIELDS:
        if column not in df.columns:
            df[column] = pd.NA
    for team_col in ("home_team_name", "away_team_name"):
        df[team_col] = (
            df[team_col]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
            .replace("None", pd.NA)
        )

    df = df.dropna(subset=REQUIRED_EVENT_FIELDS).copy()
    if df.empty:
        return pd.DataFrame()

    tolerance_minutes = int(os.environ.get("BG_EVENT_TIME_TOLERANCE_MINUTES", "15"))
    df["_commence_canonical_for_id"] = _canonicalize_commence_for_event_id(
        df,
        tolerance_minutes=tolerance_minutes,
    )

    if "home_team_id" in df.columns:
        df["home_team_id"] = df["home_team_id"].astype("string").str.strip().replace("", pd.NA)
    if "away_team_id" in df.columns:
        df["away_team_id"] = df["away_team_id"].astype("string").str.strip().replace("", pd.NA)

    df["_home_event_key"] = (
        df.get("home_team_id").fillna(df["home_team_name"])  # type: ignore[union-attr]
        if "home_team_id" in df.columns
        else df["home_team_name"]
    ).astype("string").str.lower()

    df["_away_event_key"] = (
        df.get("away_team_id").fillna(df["away_team_name"])  # type: ignore[union-attr]
        if "away_team_id" in df.columns
        else df["away_team_name"]
    ).astype("string").str.lower()

    def _build_event_id(row: pd.Series) -> str:
        commence = row.get("_commence_canonical_for_id")
        commence_iso = commence.isoformat() if pd.notna(commence) else ""
        key = f"{row['_home_event_key']}|{row['_away_event_key']}|{commence_iso}"
        return uuid.uuid5(uuid.NAMESPACE_URL, key).hex

    df["bg_event_id"] = df.apply(_build_event_id, axis=1)
    df.drop(
        columns=["_commence_canonical_for_id", "_home_event_key", "_away_event_key"],
        inplace=True,
    )
    df.sort_values(["commence_at_utc", "bg_event_id"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ---------------------------------------------------------------------------
# Bankroll + bookmaker classification (read-only).
# ---------------------------------------------------------------------------
_BANKROLL_FALLBACK = 1000.0


def get_bankroll() -> float:
    """Return the live betting bankroll for Kelly sizing.

    Sums FanDuel + BetMGM balances from account_stats. Falls back to a safe
    default so Kelly sizing never divides oddly on a missing/zero balance.
    """
    try:
        df = fetch_data(
            "SELECT COALESCE(SUM(balance), 0) AS bankroll "
            "FROM account_stats WHERE book IN ('fanduel', 'betmgm')"
        )
    except Exception:
        return _BANKROLL_FALLBACK
    if df is None or df.empty:
        return _BANKROLL_FALLBACK
    try:
        value = float(df.iloc[0]["bankroll"])
    except (TypeError, ValueError, KeyError):
        return _BANKROLL_FALLBACK
    if value is None or value <= 0:
        return _BANKROLL_FALLBACK
    return value


def sharp_books() -> List[str]:
    """Return the list of sharp bookmaker_key values (e.g. ['pinnacle'])."""
    try:
        df = fetch_data("SELECT bookmaker_key FROM dim_bookmaker WHERE is_sharp = true")
    except Exception:
        return ["pinnacle"]
    if df is None or df.empty:
        return ["pinnacle"]
    return [str(k) for k in df["bookmaker_key"].dropna().tolist()]


def soft_legal_books() -> List[str]:
    """Return the list of legal-for-betting (soft) bookmaker_key values."""
    try:
        df = fetch_data(
            "SELECT bookmaker_key FROM dim_bookmaker WHERE is_legal_for_betting = true"
        )
    except Exception:
        return ["fanduel", "betmgm", "draftkings", "williamhill_us"]
    if df is None or df.empty:
        return ["fanduel", "betmgm", "draftkings", "williamhill_us"]
    return [str(k) for k in df["bookmaker_key"].dropna().tolist()]
