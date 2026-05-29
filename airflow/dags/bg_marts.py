"""DAG: bg_marts — market-consensus / arbitrage / ranked good-bets board.

Triggered by the bg_methodology_complete asset (emitted by bg_methodology).
Replicates the bg_unified_analysis outputs from the new dimensional model and
unions the methodology facts into a single ranked board:

  * build_market_consensus -> mart_market_consensus (replicates bg_unified_analysis)
  * build_arbitrage        -> mart_arbitrage        (replicates bg_unified_arbitrage)
  * build_good_bets        -> mart_good_bets        (asset outlet bg_good_bets_ready)

The consensus + arbitrage transforms are COPIED from bg_unified_analysis_dag
(_calculate_market_metrics / build_unified_arbitrage) rather than imported —
importing a DAG module triggers Airflow DAG-parsing side-effects.

All writes use bulk_replace (mart_* are replace-every-run). Empty upstream
frames raise AirflowSkipException (mirrors bg_unified_analysis).
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import List

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

# dags dir is on sys.path in Airflow.
from bg_analytics_lib.common import (
    GOOD_BETS_READY,
    METHODOLOGY_COMPLETE,
    fetch_normalized_with_event_id,
)
from bg_arb_pipeline_lib.db import bulk_replace
from bountygate.utils.db_connection import fetch_data

# +EV edge threshold (fraction) for the good-bets board; mirrors bg_methodology.
_EV_EDGE_THRESHOLD = float(os.environ.get("BG_EV_EDGE_THRESHOLD", "0.025"))
# Recency window (days) for reading methodology facts into the board.
_GOOD_BETS_LOOKBACK_DAYS = int(os.environ.get("BG_GOOD_BETS_LOOKBACK_DAYS", "2"))
# rank_score penalty subtracted when devig methods disagree.
_DISAGREEMENT_PENALTY = float(os.environ.get("BG_DISAGREEMENT_PENALTY", "1.0"))


# ---------------------------------------------------------------------------
# Consensus transform — COPIED from bg_unified_analysis_dag._calculate_market_metrics.
# Keep in sync. Pure transform, no IO.
# ---------------------------------------------------------------------------
def _stringify_uuid_columns(df: pd.DataFrame) -> pd.DataFrame:
    import uuid

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


def _calculate_market_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Per-market mean/std/count + price_delta (+multiplier_* if present)."""
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
        enriched_df["multiplier_delta"] = (
            enriched_df["multiplier"] - enriched_df["multiplier_mean"]
        )

    return enriched_df


# ---------------------------------------------------------------------------
# Arbitrage transform — COPIED from bg_unified_analysis_dag.build_unified_arbitrage.
# Keep in sync. Pure transform, no IO.
# ---------------------------------------------------------------------------
def build_unified_arbitrage(
    df: pd.DataFrame,
    *,
    base_wager: float = 100.0,
    exclude_market_substring: str = "alternate",
) -> pd.DataFrame:
    """Build arbitrage rows from unified (normalized) lines.

    Expects decimal odds in ``price`` and two-sided ``outcome`` in {Over, Under}.
    Sizes the opposite bet so payouts equalize on a 100u Under base wager.
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
            ~working["market_key"].str.contains(
                exclude_market_substring, case=False, na=False
            )
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
        subset=[
            "bg_event_id",
            "player_name",
            "market_key",
            "line",
            "outcome",
            "price",
            "bookmaker_key",
        ],
        inplace=True,
    )
    working = working[working["price"] > 0].copy()
    if working.empty:
        return pd.DataFrame()

    # Intentionally do NOT group by line — books may post different primary lines.
    group_cols = ["bg_event_id", "player_name", "market_key"]

    def _pick_extreme(*, side: str, highest: bool) -> pd.DataFrame:
        side_df = working[working["outcome"] == side].copy()
        if side_df.empty:
            return pd.DataFrame(
                columns=group_cols
                + [f"{side}_bookmaker_key", f"{side}_price", f"{side}_line"]
            )

        side_df.sort_values(
            by=group_cols + ["price"],
            ascending=[True] * len(group_cols) + ([False] if highest else [True]),
            inplace=True,
        )
        extreme = side_df.drop_duplicates(subset=group_cols, keep="first")
        return extreme[group_cols + ["bookmaker_key", "price", "line"]].rename(
            columns={
                "bookmaker_key": f"{side}_bookmaker_key",
                "price": f"{side}_price",
                "line": f"{side}_line",
            }
        )

    best_under = _pick_extreme(side="under", highest=True)
    best_over = _pick_extreme(side="over", highest=True)

    merged = best_under.merge(best_over, on=group_cols, how="inner")
    if merged.empty:
        return pd.DataFrame()

    wager_under = float(base_wager)
    merged["wager_under"] = wager_under
    merged["payout"] = (merged["wager_under"] * merged["under_price"]).round(2)
    merged["wager_over"] = (merged["payout"] / merged["over_price"]).round(2)
    merged["total_wager"] = (merged["wager_under"] + merged["wager_over"]).round(2)
    merged["arb_ev"] = (merged["payout"] - merged["total_wager"]).round(2)

    merged.sort_values(by=["arb_ev"], ascending=False, inplace=True)
    merged.reset_index(drop=True, inplace=True)
    return merged


def _latest_pinnacle_fair() -> pd.DataFrame:
    """Latest Shin over-prob per (event, market, player, line) for join into consensus."""
    fair = fetch_data(
        "SELECT bg_event_id, market_key, player_name, line, "
        "       fair_prob_over_shin, fetched_at_utc "
        "FROM fact_fair_prob "
        f"WHERE fetched_at_utc >= now() - interval '{int(_GOOD_BETS_LOOKBACK_DAYS)} days'"
    )
    if fair is None or fair.empty:
        return pd.DataFrame(
            columns=["bg_event_id", "market_key", "player_name", "line", "pinnacle_fair_prob"]
        )
    fair = fair.copy()
    fair["line"] = pd.to_numeric(fair["line"], errors="coerce")
    fair["fair_prob_over_shin"] = pd.to_numeric(
        fair["fair_prob_over_shin"], errors="coerce"
    )
    fair["fetched_at_utc"] = pd.to_datetime(fair["fetched_at_utc"], errors="coerce")
    fair.sort_values("fetched_at_utc", inplace=True)
    fair = fair.drop_duplicates(
        subset=["bg_event_id", "market_key", "player_name", "line"], keep="last"
    )
    fair["player_name"] = (
        fair["player_name"].astype("string").str.strip().replace("", pd.NA).replace("None", pd.NA)
    )
    fair["market_key"] = fair["market_key"].astype("string")
    fair = fair.rename(columns={"fair_prob_over_shin": "pinnacle_fair_prob"})
    return fair[["bg_event_id", "market_key", "player_name", "line", "pinnacle_fair_prob"]]


@dag(
    dag_id="bg_marts",
    description="Build market-consensus, arbitrage, and ranked good-bets marts",
    schedule=[METHODOLOGY_COMPLETE],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "marts"],
)
def bg_marts_pipeline() -> None:

    @task()
    def build_market_consensus() -> int:
        prepared = fetch_normalized_with_event_id()
        if prepared is None or prepared.empty:
            raise AirflowSkipException("No normalized rows for market consensus")

        metrics = _calculate_market_metrics(_stringify_uuid_columns(prepared.copy()))
        if metrics is None or metrics.empty:
            raise AirflowSkipException("Market metrics produced no rows")

        metrics = metrics.copy()
        # Map MV 'outcome' to the consensus 'outcome' column (already present);
        # ensure all mart_market_consensus columns exist.
        metrics["line"] = pd.to_numeric(metrics.get("line"), errors="coerce")
        metrics["fetched_at_utc"] = pd.to_datetime(
            metrics.get("fetched_at_utc"), errors="coerce", utc=True
        ).dt.tz_localize(None)

        # pinnacle_fair_prob via left join on fact_fair_prob latest Shin over-prob.
        # Join on event/market/player/line; the Shin *over* prob is representative.
        pinn = _latest_pinnacle_fair()
        metrics["player_name"] = (
            metrics["player_name"].astype("string").str.strip().replace("", pd.NA).replace("None", pd.NA)
        )
        metrics["market_key"] = metrics["market_key"].astype("string")
        metrics = metrics.merge(
            pinn,
            on=["bg_event_id", "market_key", "player_name", "line"],
            how="left",
        )

        consensus_cols = [
            "bg_event_id",
            "player_name",
            "outcome",
            "line",
            "market_key",
            "price_mean",
            "price_std",
            "price_count",
            "price_delta",
            "multiplier_mean",
            "multiplier_std",
            "multiplier_count",
            "multiplier_delta",
            "pinnacle_fair_prob",
            "bookmaker_key",
            "price",
            "fetched_at_utc",
        ]
        for c in consensus_cols:
            if c not in metrics.columns:
                metrics[c] = pd.NA
        out = metrics[consensus_cols].copy()
        out = out[out["fetched_at_utc"].notna()].copy()
        if out.empty:
            raise AirflowSkipException("No consensus rows with fetched_at_utc")
        return int(bulk_replace(out, "mart_market_consensus"))

    @task()
    def build_arbitrage() -> int:
        prepared = fetch_normalized_with_event_id()
        if prepared is None or prepared.empty:
            raise AirflowSkipException("No normalized rows for arbitrage")

        arb = build_unified_arbitrage(
            _stringify_uuid_columns(prepared.copy()),
            base_wager=100.0,
            exclude_market_substring="alternate",
        )
        if arb is None or arb.empty:
            raise AirflowSkipException("No arbitrage rows produced")

        arb = arb.copy()
        # roi = arb_ev / total_wager (per migration 013 mapping).
        arb["roi"] = arb.apply(
            lambda r: (float(r["arb_ev"]) / float(r["total_wager"]))
            if (r.get("total_wager") not in (None,) and pd.notna(r.get("total_wager")) and float(r["total_wager"]) != 0)
            else None,
            axis=1,
        )
        arb["fetched_at_utc"] = pd.Timestamp.utcnow().tz_localize(None)

        arb_cols = [
            "bg_event_id",
            "player_name",
            "market_key",
            "line",
            "under_bookmaker_key",
            "under_price",
            "under_line",
            "over_bookmaker_key",
            "over_price",
            "over_line",
            "wager_under",
            "wager_over",
            "payout",
            "arb_ev",
            "roi",
            "fetched_at_utc",
        ]
        # 'line' is not a single column in the arb output (under_line/over_line);
        # surface under_line as the canonical 'line' for the mart.
        if "line" not in arb.columns:
            arb["line"] = arb.get("under_line")
        for c in arb_cols:
            if c not in arb.columns:
                arb[c] = pd.NA
        out = arb[arb_cols].copy()
        return int(bulk_replace(out, "mart_arbitrage"))

    @task(outlets=[GOOD_BETS_READY])
    def build_good_bets(_n_consensus: int, _n_arb: int) -> int:
        # dim_event (+ dim_sport title) for naming the board rows.
        events = fetch_data(
            "SELECT e.bg_event_id, e.sport_key, e.home_team_name, e.away_team_name, "
            "       e.commence_at_utc, s.sport_title "
            "FROM dim_event e "
            "LEFT JOIN dim_sport s ON s.sport_key = e.sport_key"
        )
        if events is None:
            events = pd.DataFrame()
        events = events.copy()
        if not events.empty:
            events["commence_at_utc"] = pd.to_datetime(
                events["commence_at_utc"], errors="coerce", utc=True
            ).dt.tz_localize(None)

        frames: list[pd.DataFrame] = []
        now_naive = pd.Timestamp.utcnow().tz_localize(None)

        # (a) +EV from fact_ev_opportunity (edge_pct >= threshold).
        ev = fetch_data(
            "SELECT bg_event_id, sport_key, market_key, player_name, side, line, "
            "       soft_book, soft_price_decimal, fair_prob, fair_method, edge_pct, "
            "       kelly_quarter, stake_capped, disagreement_flag, "
            "       commence_at_utc, hours_until_commence "
            "FROM fact_ev_opportunity "
            f"WHERE edge_pct >= {_EV_EDGE_THRESHOLD * 100.0}"
        )
        if ev is not None and not ev.empty:
            ev = ev.copy()
            ev["opportunity_type"] = "ev"
            ev["roi"] = pd.NA
            ev["rank_score"] = pd.to_numeric(ev["edge_pct"], errors="coerce")
            frames.append(ev)

        # (b) Arbitrage from mart_arbitrage (roi > 0).
        arb = fetch_data(
            "SELECT bg_event_id, player_name, market_key, under_line AS line, "
            "       over_bookmaker_key AS soft_book, over_price AS soft_price_decimal, "
            "       roi "
            "FROM mart_arbitrage WHERE roi > 0"
        )
        if arb is not None and not arb.empty:
            arb = arb.copy()
            arb["opportunity_type"] = "arb"
            arb["side"] = "over"
            arb["fair_prob"] = pd.NA
            arb["fair_method"] = pd.NA
            arb["edge_pct"] = pd.NA
            arb["kelly_quarter"] = pd.NA
            arb["stake_capped"] = pd.NA
            arb["disagreement_flag"] = False
            arb["sport_key"] = pd.NA
            arb["commence_at_utc"] = pd.NaT  # mart_arbitrage has none; filled via dim_event join
            arb["hours_until_commence"] = pd.NA
            arb["rank_score"] = pd.to_numeric(arb["roi"], errors="coerce") * 100.0
            frames.append(arb)

        # (c) CLV-positive from fact_clv (beat_close).
        clv = fetch_data(
            "SELECT bg_event_id, market_key, player_name, side, line, "
            "       bet_price_decimal AS soft_price_decimal, "
            "       bet_fair_prob AS fair_prob, clv_pct, recorded_at_utc "
            "FROM fact_clv WHERE beat_close = true "
            f"AND recorded_at_utc >= now() - interval '{int(_GOOD_BETS_LOOKBACK_DAYS)} days'"
        )
        if clv is not None and not clv.empty:
            clv = clv.copy()
            clv["opportunity_type"] = "clv"
            clv["soft_book"] = pd.NA
            clv["fair_method"] = "clv"
            clv["edge_pct"] = pd.NA
            clv["roi"] = pd.NA
            clv["kelly_quarter"] = pd.NA
            clv["stake_capped"] = pd.NA
            clv["disagreement_flag"] = False
            clv["sport_key"] = pd.NA
            clv["commence_at_utc"] = pd.NaT
            clv["hours_until_commence"] = pd.NA
            clv["rank_score"] = pd.to_numeric(clv["clv_pct"], errors="coerce") * 100.0
            frames.append(clv)

        if not frames:
            raise AirflowSkipException("No actionable EV / arb / CLV rows for good bets")

        board = pd.concat(frames, ignore_index=True, sort=False)

        # Common columns / coercions.
        board["line"] = pd.to_numeric(board.get("line"), errors="coerce")
        board["soft_price_decimal"] = pd.to_numeric(
            board.get("soft_price_decimal"), errors="coerce"
        )
        board["fair_prob"] = pd.to_numeric(board.get("fair_prob"), errors="coerce")
        board["edge_pct"] = pd.to_numeric(board.get("edge_pct"), errors="coerce")
        board["roi"] = pd.to_numeric(board.get("roi"), errors="coerce")
        board["kelly_quarter"] = pd.to_numeric(board.get("kelly_quarter"), errors="coerce")
        board["stake_capped"] = pd.to_numeric(board.get("stake_capped"), errors="coerce")
        board["rank_score"] = pd.to_numeric(board.get("rank_score"), errors="coerce")
        board["disagreement_flag"] = board.get("disagreement_flag").fillna(False).astype(bool)
        board["commence_at_utc"] = pd.to_datetime(
            board.get("commence_at_utc"), errors="coerce", utc=True
        ).dt.tz_localize(None)

        # Penalize disagreement in rank_score.
        board.loc[board["disagreement_flag"], "rank_score"] = (
            board.loc[board["disagreement_flag"], "rank_score"] - _DISAGREEMENT_PENALTY
        )
        board["rank_score"] = board["rank_score"].fillna(0.0)

        # Join dim_event for naming; prefer event commence when row lacks one.
        board["player_name"] = (
            board["player_name"].astype("string").str.strip().replace("", pd.NA).replace("None", pd.NA)
        )
        if not events.empty:
            board = board.merge(
                events.rename(columns={"commence_at_utc": "event_commence"}),
                on="bg_event_id",
                how="left",
                suffixes=("", "_ev"),
            )
            board["commence_at_utc"] = board["commence_at_utc"].fillna(board["event_commence"])
            # Prefer dim_event sport_key when the row's own is null.
            if "sport_key_ev" in board.columns:
                board["sport_key"] = board["sport_key"].fillna(board["sport_key_ev"])
        else:
            board["home_team_name"] = pd.NA
            board["away_team_name"] = pd.NA
            board["sport_title"] = pd.NA

        # hours_until_commence: recompute from event commence when missing.
        board["hours_until_commence"] = pd.to_numeric(
            board.get("hours_until_commence"), errors="coerce"
        )
        commence_hours = (
            (board["commence_at_utc"] - now_naive).dt.total_seconds().div(3600)
        )
        board["hours_until_commence"] = board["hours_until_commence"].fillna(commence_hours)

        board["fetched_at_utc"] = now_naive

        def _good_bet_hash(row: pd.Series) -> str:
            key = "|".join(
                str(v)
                for v in [
                    row.get("opportunity_type"),
                    row.get("bg_event_id"),
                    row.get("market_key"),
                    row.get("player_name"),
                    row.get("side"),
                    row.get("line"),
                    row.get("soft_book"),
                ]
            )
            return hashlib.md5(key.encode()).hexdigest()

        board["good_bet_hash"] = board.apply(_good_bet_hash, axis=1)

        good_bet_cols = [
            "good_bet_hash",
            "bg_event_id",
            "sport_key",
            "sport_title",
            "home_team_name",
            "away_team_name",
            "commence_at_utc",
            "hours_until_commence",
            "market_key",
            "player_name",
            "side",
            "line",
            "opportunity_type",
            "soft_book",
            "soft_price_decimal",
            "fair_prob",
            "fair_method",
            "edge_pct",
            "roi",
            "kelly_quarter",
            "stake_capped",
            "disagreement_flag",
            "rank_score",
            "fetched_at_utc",
        ]
        for c in good_bet_cols:
            if c not in board.columns:
                board[c] = pd.NA
        out = board[good_bet_cols].drop_duplicates(subset=["good_bet_hash"]).copy()
        out = out.sort_values("rank_score", ascending=False).reset_index(drop=True)
        return int(bulk_replace(out, "mart_good_bets"))

    n_consensus = build_market_consensus()
    n_arb = build_arbitrage()
    build_good_bets(n_consensus, n_arb)


dag = bg_marts_pipeline()
