"""DAG: bg_methodology — devig fair probs, +EV opportunities, line movement.

Triggered by the bg_model_loaded asset (emitted by bg_dimensional_model). Reads
the latest fact_odds_snapshot quotes and runs the quantitative methodology stack
from bountygate.analytics:

  * compute_fair_probs   -> fact_fair_prob   (Pinnacle devig + consensus fallback)
  * compute_ev           -> fact_ev_opportunity (replace) + _history (append)
  * compute_line_movement-> fact_line_movement
  * mark_methodology_complete (asset outlet)

Empty upstream frames raise AirflowSkipException (mirrors bg_unified_analysis).
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset

# dags dir is on sys.path in Airflow.
from bg_analytics_lib.common import (
    METHODOLOGY_COMPLETE,
    MODEL_LOADED,
    get_bankroll,
    get_engine,
    sharp_books,
    soft_legal_books,
)
from bg_arb_pipeline_lib.db import bulk_append_new, bulk_replace
from bountygate.analytics import consensus, devig, ev, kelly, signals
from bountygate.utils.db_connection import fetch_data

# Window (days) of fact_odds_snapshot history read for methodology.
_FAIR_LOOKBACK_DAYS = int(os.environ.get("BG_METHODOLOGY_LOOKBACK_DAYS", "2"))
_MOVEMENT_LOOKBACK_DAYS = int(os.environ.get("BG_LINE_MOVEMENT_LOOKBACK_DAYS", "2"))


def _latest_snapshot(lookback_days: int) -> pd.DataFrame:
    """Return the latest quote per (event, book, market, player, side, line).

    Reads only the recent window of fact_odds_snapshot, keeping the most recent
    fetched_at_utc row in each quote group.
    """
    df = fetch_data(
        "SELECT bg_event_id, sport_key, bookmaker_key, market_key, base_market_key, "
        "       player_name, side, line, price_decimal, implied_prob, "
        "       commence_at_utc, fetched_at_utc, is_sharp "
        "FROM fact_odds_snapshot "
        f"WHERE fetched_at_utc >= now() - interval '{int(lookback_days)} days'"
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["price_decimal"] = pd.to_numeric(df["price_decimal"], errors="coerce")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["commence_at_utc"] = pd.to_datetime(df["commence_at_utc"], errors="coerce")
    df["fetched_at_utc"] = pd.to_datetime(df["fetched_at_utc"], errors="coerce")
    # is_sharp may arrive as object/str; coerce to bool.
    df["is_sharp"] = df["is_sharp"].astype(bool)
    df = df.dropna(subset=["price_decimal", "fetched_at_utc"]).copy()

    group_cols = ["bg_event_id", "bookmaker_key", "market_key", "player_name", "side", "line"]
    df.sort_values("fetched_at_utc", inplace=True)
    df = df.drop_duplicates(subset=group_cols, keep="last")
    return df


@dag(
    dag_id="bg_methodology",
    description="Devig fair probabilities, build +EV opportunities, and line-movement signals",
    schedule=[MODEL_LOADED],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "methodology"],
)
def bg_methodology_pipeline() -> None:

    @task()
    def compute_fair_probs() -> int:
        latest = _latest_snapshot(_FAIR_LOOKBACK_DAYS)
        if latest.empty:
            raise AirflowSkipException("No recent snapshots for fair-prob computation")

        sharp = set(sharp_books())

        # Pivot to over/under price per (event, market, player, line, book).
        latest["side"] = latest["side"].astype("string").str.lower()
        sided = latest[latest["side"].isin(["over", "under"])].copy()
        if sided.empty:
            raise AirflowSkipException("No two-way quotes for fair-prob computation")

        key_cols = ["bg_event_id", "sport_key", "market_key", "base_market_key", "player_name", "line"]
        rows: list[dict] = []

        for key_vals, grp in sided.groupby(key_cols, dropna=False, sort=False):
            (bg_event_id, sport_key, market_key, base_market_key, player_name, line) = key_vals

            commence = grp["commence_at_utc"].dropna()
            commence_val = commence.iloc[0] if not commence.empty else pd.NaT
            fetched_val = grp["fetched_at_utc"].max()

            # Pinnacle (sharp) two-way.
            pinn = grp[grp["bookmaker_key"].isin(sharp)]
            pinn_over = pinn[pinn["side"] == "over"]["price_decimal"]
            pinn_under = pinn[pinn["side"] == "under"]["price_decimal"]
            pinn_over_price = float(pinn_over.iloc[0]) if not pinn_over.empty else None
            pinn_under_price = float(pinn_under.iloc[0]) if not pinn_under.empty else None

            devig_result = devig.devig_all(pinn_over_price, pinn_under_price)

            # Consensus fallback: a WIDE market consensus across ALL non-sharp books
            # (not just the 4 legal ones) — a broader, sharper sample. Pinnacle is the
            # primary anchor above and is excluded here. Edge is still measured vs the
            # legal soft books downstream; this is just the "fair price" estimate.
            cons_grp = grp[~grp["bookmaker_key"].isin(sharp)]
            over_list = cons_grp[cons_grp["side"] == "over"]["price_decimal"].tolist()
            under_list = cons_grp[cons_grp["side"] == "under"]["price_decimal"].tolist()
            n = min(len(over_list), len(under_list))
            cons = (
                consensus.no_vig_consensus(over_list[:n], under_list[:n])
                if n > 0
                else None
            )
            cons_over = float(cons[0]) if cons else None
            cons_under = float(cons[1]) if cons else None

            # Require either a Pinnacle devig or a consensus fallback.
            if devig_result is None and cons is None:
                continue

            if devig_result is not None:
                fp_over_mult = devig_result["fair_prob_over_mult"]
                fp_under_mult = devig_result["fair_prob_under_mult"]
                fp_over_power = devig_result["fair_prob_over_power"]
                fp_under_power = devig_result["fair_prob_under_power"]
                fp_over_shin = devig_result["fair_prob_over_shin"]
                fp_under_shin = devig_result["fair_prob_under_shin"]
                method_disagreement = devig_result["method_disagreement"]
                disagreement_flag = bool(devig_result["disagreement_flag"])
                overround = devig_result["overround"]
            else:
                # Pinnacle absent/one-sided: fall back to consensus for the devig pairs
                # so downstream EV (which reads the *_mult/_shin columns) still works.
                fp_over_mult = cons_over
                fp_under_mult = cons_under
                fp_over_power = cons_over
                fp_under_power = cons_under
                fp_over_shin = cons_over
                fp_under_shin = cons_under
                method_disagreement = 0.0
                disagreement_flag = False
                overround = None

            fetched_str = "" if pd.isna(fetched_val) else fetched_val.isoformat()
            fair_hash = hashlib.md5(
                f"{bg_event_id}|{market_key}|{player_name}|{line}|{fetched_str}".encode()
            ).hexdigest()

            rows.append(
                {
                    "fair_hash": fair_hash,
                    "bg_event_id": bg_event_id,
                    "sport_key": sport_key,
                    "market_key": market_key,
                    "base_market_key": base_market_key,
                    "player_name": player_name,
                    "player_id": None,
                    "line": line,
                    "pinnacle_over_price": pinn_over_price,
                    "pinnacle_under_price": pinn_under_price,
                    "fair_prob_over_mult": fp_over_mult,
                    "fair_prob_under_mult": fp_under_mult,
                    "fair_prob_over_power": fp_over_power,
                    "fair_prob_under_power": fp_under_power,
                    "fair_prob_over_shin": fp_over_shin,
                    "fair_prob_under_shin": fp_under_shin,
                    "consensus_fair_prob_over": cons_over,
                    "consensus_fair_prob_under": cons_under,
                    "method_disagreement": method_disagreement,
                    "disagreement_flag": disagreement_flag,
                    "overround": overround,
                    "fetched_at_utc": fetched_val,
                    "commence_at_utc": commence_val,
                }
            )

        if not rows:
            raise AirflowSkipException("No fair probabilities produced")

        out = pd.DataFrame(rows)
        out["fetched_at_utc"] = pd.to_datetime(out["fetched_at_utc"], errors="coerce")
        out["commence_at_utc"] = pd.to_datetime(out["commence_at_utc"], errors="coerce")
        # fact_fair_prob.commence_at_utc / fetched_at_utc are NOT NULL; a NaT would
        # serialize to NULL under the COPY loader and violate the constraint. Drop
        # any row missing either timestamp before loading.
        out = out.dropna(subset=["commence_at_utc", "fetched_at_utc"])
        out = out.drop_duplicates(subset=["fair_hash"])
        if out.empty:
            raise AirflowSkipException("No fair probabilities with valid timestamps")
        return int(bulk_append_new(out, "fact_fair_prob", "fair_hash"))

    @task()
    def compute_ev(_n_fair: int) -> int:
        soft = soft_legal_books()

        # Soft-book quotes (latest) for the legal books only.
        latest = _latest_snapshot(_FAIR_LOOKBACK_DAYS)
        if latest.empty:
            raise AirflowSkipException("No recent snapshots for EV computation")
        soft_quotes = latest[latest["bookmaker_key"].isin(soft)].copy()
        if soft_quotes.empty:
            raise AirflowSkipException("No soft-book quotes for EV computation")
        soft_quotes["side"] = soft_quotes["side"].astype("string").str.lower()

        # Fair probs: latest per natural key from fact_fair_prob.
        fair = fetch_data(
            "SELECT bg_event_id, market_key, player_name, line, "
            "       fair_prob_over_shin, fair_prob_under_shin, "
            "       fair_prob_over_mult, fair_prob_under_mult, "
            "       disagreement_flag, fetched_at_utc "
            "FROM fact_fair_prob "
            f"WHERE fetched_at_utc >= now() - interval '{int(_FAIR_LOOKBACK_DAYS)} days'"
        )
        if fair is None or fair.empty:
            raise AirflowSkipException("No fair probabilities available for EV")
        fair = fair.copy()
        fair["line"] = pd.to_numeric(fair["line"], errors="coerce")
        fair["fetched_at_utc"] = pd.to_datetime(fair["fetched_at_utc"], errors="coerce")
        for c in (
            "fair_prob_over_shin",
            "fair_prob_under_shin",
            "fair_prob_over_mult",
            "fair_prob_under_mult",
        ):
            fair[c] = pd.to_numeric(fair[c], errors="coerce")
        fair.sort_values("fetched_at_utc", inplace=True)
        fair = fair.drop_duplicates(
            subset=["bg_event_id", "market_key", "player_name", "line"], keep="last"
        )

        threshold = float(os.environ.get("BG_EV_EDGE_THRESHOLD", "0.025"))
        ev_df = ev.build_ev_opportunities(
            soft_quotes,
            fair,
            soft_books=soft,
            threshold=threshold,
        )
        if ev_df is None or ev_df.empty:
            raise AirflowSkipException("No actionable EV opportunities")

        ev_df = kelly.size_opportunities(ev_df, bankroll=get_bankroll())

        # Map to fact_ev_opportunity columns (migration 012). build_ev_opportunities
        # emits soft_book / soft_price_decimal / pinnacle_no_vig_price already.
        ev_df = ev_df.copy()
        ev_df["player_id"] = None
        ev_df["commence_at_utc"] = pd.to_datetime(ev_df["commence_at_utc"], errors="coerce")
        ev_df["fetched_at_utc"] = pd.to_datetime(ev_df["fetched_at_utc"], errors="coerce")

        ev_cols = [
            "ev_hash",
            "bg_event_id",
            "sport_key",
            "market_key",
            "player_name",
            "player_id",
            "side",
            "line",
            "soft_book",
            "soft_price_decimal",
            "fair_prob",
            "fair_method",
            "edge",
            "edge_pct",
            "kelly_full",
            "kelly_quarter",
            "stake_capped",
            "pinnacle_no_vig_price",
            "disagreement_flag",
            "commence_at_utc",
            "hours_until_commence",
            "fetched_at_utc",
        ]
        for c in ev_cols:
            if c not in ev_df.columns:
                ev_df[c] = pd.NA
        current = ev_df[ev_cols].drop_duplicates(subset=["ev_hash"]).copy()

        bulk_replace(current, "fact_ev_opportunity")

        # History twin: append-only on ev_history_hash = md5(ev_hash|fetched_at).
        hist = current.copy()

        def _hist_hash(row: pd.Series) -> str:
            fetched = row["fetched_at_utc"]
            fetched_str = "" if pd.isna(fetched) else pd.Timestamp(fetched).isoformat()
            return hashlib.md5(f"{row['ev_hash']}|{fetched_str}".encode()).hexdigest()

        hist["ev_history_hash"] = hist.apply(_hist_hash, axis=1)
        hist_cols = ["ev_history_hash"] + ev_cols
        hist = hist[hist_cols].drop_duplicates(subset=["ev_history_hash"])
        bulk_append_new(hist, "fact_ev_opportunity_history", "ev_history_hash")

        return int(len(current))

    @task()
    def compute_line_movement(_n_ev: int) -> int:
        history = fetch_data(
            "SELECT bg_event_id, market_key, player_name, side, line, "
            "       bookmaker_key, is_sharp, price_decimal, fetched_at_utc "
            "FROM fact_odds_snapshot "
            f"WHERE fetched_at_utc >= now() - interval '{int(_MOVEMENT_LOOKBACK_DAYS)} days'"
        )
        if history is None or history.empty:
            raise AirflowSkipException("No snapshot history for line movement")

        history = history.copy()
        history["price_decimal"] = pd.to_numeric(history["price_decimal"], errors="coerce")
        history["line"] = pd.to_numeric(history["line"], errors="coerce")
        history["fetched_at_utc"] = pd.to_datetime(history["fetched_at_utc"], errors="coerce")
        history["is_sharp"] = history["is_sharp"].astype(bool)
        history = history.dropna(subset=["price_decimal", "fetched_at_utc"]).copy()
        if history.empty:
            raise AirflowSkipException("No priced snapshot history for line movement")

        moves = signals.compute_line_movement(history)
        if moves is None or moves.empty:
            raise AirflowSkipException("No line-movement rows produced")

        # Map signals output -> fact_line_movement columns (migration 012).
        moves = moves.rename(
            columns={
                "window_start": "window_start_utc",
                "window_end": "window_end_utc",
                "soft_open": "soft_open_price",
                "soft_close": "soft_close_price",
                "sharp_open": "pinnacle_open_price",
                "sharp_close": "pinnacle_close_price",
                "reverse_line_movement": "reverse_line_movement",
            }
        )
        moves["computed_at_utc"] = pd.Timestamp.utcnow().tz_localize(None)

        lm_cols = [
            "movement_hash",
            "bg_event_id",
            "market_key",
            "player_name",
            "side",
            "line",
            "window_start_utc",
            "window_end_utc",
            "soft_open_price",
            "soft_close_price",
            "pinnacle_open_price",
            "pinnacle_close_price",
            "soft_vs_sharp_gap",
            "reverse_line_movement",
            "computed_at_utc",
        ]
        for c in lm_cols:
            if c not in moves.columns:
                moves[c] = pd.NA
        out = moves[lm_cols].drop_duplicates(subset=["movement_hash"]).copy()
        for c in ("window_start_utc", "window_end_utc"):
            out[c] = pd.to_datetime(out[c], errors="coerce")
        out["reverse_line_movement"] = out["reverse_line_movement"].astype(bool)
        return int(bulk_append_new(out, "fact_line_movement", "movement_hash"))

    @task(outlets=[METHODOLOGY_COMPLETE])
    def mark_methodology_complete(_n_fair: int, _n_ev: int, _n_move: int) -> None:
        return None

    n_fair = compute_fair_probs()
    n_ev = compute_ev(n_fair)
    n_move = compute_line_movement(n_ev)
    mark_methodology_complete(n_fair, n_ev, n_move)


dag = bg_methodology_pipeline()
