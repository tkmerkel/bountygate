from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import List

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset

from bountygate.utils.db_connection import fetch_data, insert_data


def build_unified_arbitrage(
    df: pd.DataFrame,
    *,
    base_wager: float = 100.0,
    exclude_market_substring: str = "alternate",
) -> pd.DataFrame:
    """Build arbitrage rows from unified (normalized/analysis) lines.

    This expects decimal odds in `price` and two-sided markets with `outcome` in
    {"Over", "Under"} (case-insensitive).

    Output includes:
    - best (highest) price bookmaker for Under
    - best (highest) price bookmaker for Over
    - worst (lowest) price bookmaker for each side (for visibility)
    - stake sizing to equalize payout on both sides (base wager placed on Under)
    - `arb_ev` = payout - sum(wagers)

    Markets with `market_key` containing `exclude_market_substring` (case-insensitive)
    are filtered out.
    """

    if df is None or df.empty:
        return pd.DataFrame()

    working = df.copy()

    required_columns: List[str] = [
        "bg_event_id",
        "player_name",
        "market_key",
        "line",
        "outcome",
        "price",
        "bookmaker_key",
    ]
    for column in required_columns:
        if column not in working.columns:
            working[column] = pd.NA

    working["market_key"] = working["market_key"].astype("string")
    if exclude_market_substring:
        working = working[
            ~working["market_key"].str.contains(exclude_market_substring, case=False, na=False)
        ].copy()

    working["outcome"] = working["outcome"].astype("string").str.strip().str.lower()
    working = working[working["outcome"].isin(["over", "under"])].copy()

    for text_col in ("bg_event_id", "player_name", "market_key", "bookmaker_key"):
        working[text_col] = (
            working[text_col]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
            .replace("None", pd.NA)
        )

    working["line"] = pd.to_numeric(working["line"], errors="coerce")
    working["price"] = pd.to_numeric(working["price"], errors="coerce")

    working.dropna(
        subset=["bg_event_id", "player_name", "market_key", "line", "outcome", "price", "bookmaker_key"],
        inplace=True,
    )

    # Defensive: decimal odds must be > 0 to size wagers.
    working = working[working["price"] > 0].copy()

    if working.empty:
        return pd.DataFrame()

    # Intentionally do NOT group by line.
    # Different books can post different primary lines for the same prop; we surface
    # that via explicit under_line/over_line columns for validation.
    group_cols = ["bg_event_id", "player_name", "market_key"]

    def _pick_extreme(*, side: str, highest: bool) -> pd.DataFrame:
        side_df = working[working["outcome"] == side].copy()
        if side_df.empty:
            return pd.DataFrame(columns=group_cols + [f"{side}_bookmaker_key", f"{side}_price", f"{side}_line"])

        side_df.sort_values(
            by=group_cols + ["price"],
            ascending=[True] * len(group_cols) + ([False] if highest else [True]),
            inplace=True,
        )
        extreme = side_df.drop_duplicates(subset=group_cols, keep="first")
        return extreme[group_cols + ["bookmaker_key", "price", "line"]].rename(
            columns={"bookmaker_key": f"{side}_bookmaker_key", "price": f"{side}_price", "line": f"{side}_line"}
        )

    best_under = _pick_extreme(side="under", highest=True)
    best_over = _pick_extreme(side="over", highest=True)
    worst_under = _pick_extreme(side="under", highest=False)[
        group_cols + ["under_bookmaker_key", "under_price"]
    ].rename(columns={"under_bookmaker_key": "under_bookmaker_key_low", "under_price": "under_price_low"})
    worst_over = _pick_extreme(side="over", highest=False)[
        group_cols + ["over_bookmaker_key", "over_price"]
    ].rename(columns={"over_bookmaker_key": "over_bookmaker_key_low", "over_price": "over_price_low"})

    merged = best_under.merge(best_over, on=group_cols, how="inner")
    merged = merged.merge(worst_under, on=group_cols, how="left")
    merged = merged.merge(worst_over, on=group_cols, how="left")

    if merged.empty:
        return pd.DataFrame()

    wager_under = float(base_wager)
    merged["wager_under"] = wager_under
    merged["payout"] = (merged["wager_under"] * merged["under_price"]).round(2)

    # Size the opposite bet so its payout matches `payout`.
    merged["wager_over"] = (merged["payout"] / merged["over_price"]).round(2)
    merged["payout_over"] = merged["payout"]

    merged["total_wager"] = (merged["wager_under"] + merged["wager_over"]).round(2)
    merged["arb_ev"] = (merged["payout"] - merged["total_wager"]).round(2)

    # Make output a bit nicer/consistent.
    merged.sort_values(by=["arb_ev"], ascending=False, inplace=True)
    merged.reset_index(drop=True, inplace=True)

    return merged


# Reference the same asset
normalization_complete_asset = Asset("bg_normalization_complete")
analysis_complete_asset = Asset("bg_unified_analysis_complete")
arbitrage_complete_asset = Asset("bg_unified_arbitrage_complete")

REQUIRED_EVENT_FIELDS: List[str] = ["home_team_name", "away_team_name", "commence_at_utc"]


def _canonicalize_commence_for_event_id(
    df: pd.DataFrame,
    *,
    tolerance_minutes: int,
) -> pd.Series:
    """Return a canonical commence_at_utc per (home, away, UTC-day) within a tolerance.

    Sources can disagree on commence times by a few minutes; we cluster times within the
    tolerance and use the earliest time in the cluster as the canonical event time.
    """

    if df is None or df.empty or "commence_at_utc" not in df.columns:
        return pd.Series(dtype="datetime64[ns, UTC]")

    tolerance = pd.Timedelta(minutes=max(0, int(tolerance_minutes)))
    working = df[["home_team_name", "away_team_name", "commence_at_utc"]].copy()
    working["commence_at_utc"] = pd.to_datetime(working["commence_at_utc"], errors="coerce", utc=True)
    working["event_day_utc"] = working["commence_at_utc"].dt.floor("D")

    # Sort so diff() inside each group is meaningful.
    sort_cols = ["home_team_name", "away_team_name", "event_day_utc", "commence_at_utc"]
    working.sort_values(sort_cols, inplace=True)

    def _cluster_within_tolerance(times: pd.Series) -> pd.Series:
        diffs = times.diff()
        # New cluster whenever the gap exceeds tolerance.
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

    # Return aligned to the original df index.
    return working["_commence_canonical"].reindex(df.index)


def _stringify_uuid_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    uuid_columns: List[str] = [
        column
        for column in df.columns
        if df[column].apply(lambda value: isinstance(value, uuid.UUID)).any()
    ]
    for column in uuid_columns:
        df[column] = df[column].apply(lambda value: str(value) if isinstance(value, uuid.UUID) else value)
    return df


def _fetch_and_prep_normalized() -> pd.DataFrame:
    """Fetch normalized lines and assign canonical bg_event_id per row.

    Shared by both process tasks — UUID5 over canonical (home, away, commence) is
    deterministic, so running it twice produces matching ids across tasks.
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


def _calculate_market_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Per-market mean/std/count + price_delta. Pure transform, no IO."""
    if df is None or df.empty:
        return pd.DataFrame()

    working_df = df.copy()
    working_df = _stringify_uuid_columns(working_df)

    group_cols = ["bg_event_id", "player_name", "outcome", "line", "market_key"]
    for column in group_cols:
        if column not in working_df.columns:
            working_df[column] = pd.NA

    for column in ("bg_event_id", "player_name", "outcome", "market_key"):
        working_df[column] = (
            working_df[column]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
            .replace("None", pd.NA)
        )
    working_df["line"] = pd.to_numeric(working_df["line"], errors="coerce")

    numeric_columns = []
    if "price" in working_df.columns:
        working_df["price"] = pd.to_numeric(working_df["price"], errors="coerce")
        numeric_columns.append("price")
    if "multiplier" in working_df.columns:
        working_df["multiplier"] = pd.to_numeric(working_df["multiplier"], errors="coerce")
        numeric_columns.append("multiplier")

    if not numeric_columns:
        return working_df

    aggregations = {}
    for column in numeric_columns:
        aggregations[f"{column}_mean"] = (column, "mean")
        aggregations[f"{column}_std"] = (column, "std")
        aggregations[f"{column}_count"] = (column, "count")

    stats_df = (
        working_df.groupby(group_cols, dropna=False)
        .agg(**aggregations)
        .reset_index()
    )

    enriched_df = working_df.merge(stats_df, on=group_cols, how="left")

    if "price_mean" in enriched_df.columns:
        enriched_df["price_delta"] = enriched_df["price"] - enriched_df["price_mean"]
    if "multiplier_mean" in enriched_df.columns:
        enriched_df["multiplier_delta"] = enriched_df["multiplier"] - enriched_df["multiplier_mean"]

    return enriched_df


@dag(
    dag_id="bg_unified_analysis",
    description="Derive canonical event identifiers and analysis aggregates from normalized unified lines",
    schedule=[normalization_complete_asset],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["analysis", "unified"],
)
def bg_unified_analysis_pipeline() -> None:
    # DataFrames are not pushed through XCom — Airflow 3's serializer rejects them.
    # Each process task pulls + assigns event IDs itself; only ints cross task boundaries.

    @task()
    def process_unified_analysis() -> int:
        prepared = _fetch_and_prep_normalized()
        if prepared.empty:
            raise AirflowSkipException("No normalized analysis records to load")

        metrics_df = _calculate_market_metrics(prepared)
        if metrics_df is None or metrics_df.empty:
            raise AirflowSkipException("Market metrics produced no rows")

        metrics_df = _stringify_uuid_columns(metrics_df.copy())
        metrics_df["commence_at_utc"] = pd.to_datetime(
            metrics_df.get("commence_at_utc"), errors="coerce", utc=True
        )
        insert_data(metrics_df, "bg_unified_analysis", if_exists="replace")
        return int(len(metrics_df))

    @task(outlets=[arbitrage_complete_asset])
    def process_unified_arbitrage() -> int:
        prepared = _fetch_and_prep_normalized()
        if prepared.empty:
            raise AirflowSkipException("No unified arbitrage records to load")

        arbitrage_df = build_unified_arbitrage(
            _stringify_uuid_columns(prepared.copy()),
            base_wager=100.0,
            exclude_market_substring="alternate",
        )
        if arbitrage_df is None or arbitrage_df.empty:
            raise AirflowSkipException("No unified arbitrage records to load")

        arbitrage_df = _stringify_uuid_columns(arbitrage_df.copy())
        insert_data(arbitrage_df, "bg_unified_arbitrage", if_exists="replace")
        return int(len(arbitrage_df))

    @task(outlets=[analysis_complete_asset])
    def mark_analysis_complete(_n_analysis: int, _n_arbitrage: int) -> None:
        # Marker task: ensures downstream asset-triggered DAGs only run once
        # both bg_unified_analysis and bg_unified_arbitrage have been loaded.
        return None

    n_analysis = process_unified_analysis()
    n_arbitrage = process_unified_arbitrage()
    mark_analysis_complete(n_analysis, n_arbitrage)


dag = bg_unified_analysis_pipeline()
