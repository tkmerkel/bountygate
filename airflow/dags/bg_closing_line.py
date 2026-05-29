"""DAG: bg_closing_line — capture sharp closing lines and compute CLV.

Independent */5 cron (not asset-triggered). Two tasks:

  * capture_closing_lines: for events commencing within the next ~5 minutes (and
    not already captured), snapshot the latest sharp (is_sharp) two-way prices
    from fact_odds_snapshot, devig them (Shin) into closing_fair_prob, and
    bulk_append_new into fact_closing_line (dedup on closing_hash).
  * compute_clv_task: join prior +EV bets (fact_ev_opportunity, treated as
    tracked bets: bet_price_decimal = soft_price_decimal, bet_fair_prob =
    fair_prob) against the captured closing lines via clv.compute_clv, then
    bulk_append_new into fact_clv (dedup on clv_hash).

closing_hash = md5(bg_event_id|market_key|player_name|side|line) per migration 010.
Empty inputs raise AirflowSkipException.
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

# dags dir is on sys.path in Airflow.
from bg_arb_pipeline_lib.db import bulk_append_new
from bountygate.analytics import clv, devig
from bountygate.utils.db_connection import fetch_data

# How far ahead (minutes) of commence we treat an event as "closing now".
_CLOSING_WINDOW_MINUTES = int(os.environ.get("BG_CLOSING_WINDOW_MINUTES", "5"))
# Lookback (days) for matching prior EV bets to the captured close.
_CLV_LOOKBACK_DAYS = int(os.environ.get("BG_CLV_LOOKBACK_DAYS", "2"))


def _closing_hash(bg_event_id, market_key, player_name, side, line) -> str:
    raw = f"{bg_event_id}|{market_key}|{player_name}|{side}|{line}"
    return hashlib.md5(raw.encode()).hexdigest()


@dag(
    dag_id="bg_closing_line",
    description="Capture sharp closing lines at commence and compute CLV vs prior EV bets",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "clv"],
)
def bg_closing_line_pipeline() -> None:

    @task()
    def capture_closing_lines() -> int:
        # Latest sharp two-way quotes for events commencing within the window and
        # not yet captured. We pull the most recent fetched_at per quote group.
        snaps = fetch_data(
            "SELECT bg_event_id, sport_key, bookmaker_key, market_key, base_market_key, "
            "       player_name, side, line, price_decimal, commence_at_utc, fetched_at_utc "
            "FROM fact_odds_snapshot "
            "WHERE is_sharp = true "
            "  AND commence_at_utc <= now() + interval "
            f"      '{int(_CLOSING_WINDOW_MINUTES)} minutes' "
            "  AND commence_at_utc >= now() - interval '1 day' "
            "  AND bg_event_id NOT IN (SELECT bg_event_id FROM fact_closing_line)"
        )
        if snaps is None or snaps.empty:
            raise AirflowSkipException("No sharp snapshots for events closing now")

        snaps = snaps.copy()
        snaps["price_decimal"] = pd.to_numeric(snaps["price_decimal"], errors="coerce")
        snaps["line"] = pd.to_numeric(snaps["line"], errors="coerce")
        snaps["commence_at_utc"] = pd.to_datetime(snaps["commence_at_utc"], errors="coerce")
        snaps["fetched_at_utc"] = pd.to_datetime(snaps["fetched_at_utc"], errors="coerce")
        snaps["side"] = snaps["side"].astype("string").str.lower()
        snaps = snaps.dropna(subset=["price_decimal", "fetched_at_utc"]).copy()
        snaps = snaps[snaps["side"].isin(["over", "under"])].copy()
        if snaps.empty:
            raise AirflowSkipException("No two-way sharp closing quotes")

        # Keep the latest quote per (event, book, market, player, side, line).
        group_cols = [
            "bg_event_id",
            "bookmaker_key",
            "market_key",
            "player_name",
            "side",
            "line",
        ]
        snaps.sort_values("fetched_at_utc", inplace=True)
        snaps = snaps.drop_duplicates(subset=group_cols, keep="last")

        # Group each two-way market and devig the over/under pair for fair probs.
        key_cols = [
            "bg_event_id",
            "sport_key",
            "bookmaker_key",
            "market_key",
            "base_market_key",
            "player_name",
            "line",
        ]
        rows: list[dict] = []
        captured_at = pd.Timestamp.utcnow().tz_localize(None)

        for key_vals, grp in snaps.groupby(key_cols, dropna=False, sort=False):
            (
                bg_event_id,
                sport_key,
                bookmaker_key,
                market_key,
                base_market_key,
                player_name,
                line,
            ) = key_vals

            over = grp[grp["side"] == "over"]["price_decimal"]
            under = grp[grp["side"] == "under"]["price_decimal"]
            over_price = float(over.iloc[0]) if not over.empty else None
            under_price = float(under.iloc[0]) if not under.empty else None

            commence = grp["commence_at_utc"].dropna()
            commence_val = commence.iloc[0] if not commence.empty else pd.NaT

            devig_result = devig.devig_all(over_price, under_price)

            # Per-side closing rows so CLV can match either side of the bet.
            side_specs = []
            if over_price is not None:
                fp_over = (
                    devig_result["fair_prob_over_shin"] if devig_result else None
                )
                side_specs.append(("over", over_price, fp_over))
            if under_price is not None:
                fp_under = (
                    devig_result["fair_prob_under_shin"] if devig_result else None
                )
                side_specs.append(("under", under_price, fp_under))

            for side, price, fair_prob in side_specs:
                rows.append(
                    {
                        "closing_hash": _closing_hash(
                            bg_event_id, market_key, player_name, side, line
                        ),
                        "bg_event_id": bg_event_id,
                        "sport_key": sport_key,
                        "bookmaker_key": bookmaker_key,
                        "market_key": market_key,
                        "base_market_key": base_market_key,
                        "player_name": player_name,
                        "player_id": None,
                        "side": side,
                        "line": line,
                        "closing_price_decimal": price,
                        "closing_fair_prob": fair_prob,
                        "captured_at_utc": captured_at,
                        "commence_at_utc": commence_val,
                    }
                )

        if not rows:
            raise AirflowSkipException("No closing-line rows produced")

        out = pd.DataFrame(rows)
        out["commence_at_utc"] = pd.to_datetime(out["commence_at_utc"], errors="coerce")
        out = out.drop_duplicates(subset=["closing_hash"])
        return int(bulk_append_new(out, "fact_closing_line", "closing_hash"))

    @task()
    def compute_clv_task(_n_closing: int) -> int:
        # Prior EV bets treated as tracked bets.
        ev = fetch_data(
            "SELECT bg_event_id, market_key, player_name, side, line, "
            "       soft_price_decimal AS bet_price_decimal, "
            "       fair_prob AS bet_fair_prob "
            "FROM fact_ev_opportunity "
            f"WHERE fetched_at_utc >= now() - interval '{int(_CLV_LOOKBACK_DAYS)} days'"
        )
        if ev is None or ev.empty:
            raise AirflowSkipException("No tracked EV bets for CLV")

        closing = fetch_data(
            "SELECT bg_event_id, market_key, player_name, side, line, "
            "       closing_price_decimal, closing_fair_prob "
            "FROM fact_closing_line "
            "WHERE closing_fair_prob IS NOT NULL"
        )
        if closing is None or closing.empty:
            raise AirflowSkipException("No closing lines available for CLV")

        ev = ev.copy()
        closing = closing.copy()
        for df_ in (ev, closing):
            df_["line"] = pd.to_numeric(df_["line"], errors="coerce")
            df_["side"] = df_["side"].astype("string").str.lower()
            df_["player_name"] = df_["player_name"].astype("string")
            df_["market_key"] = df_["market_key"].astype("string")
        ev["bet_price_decimal"] = pd.to_numeric(ev["bet_price_decimal"], errors="coerce")
        ev["bet_fair_prob"] = pd.to_numeric(ev["bet_fair_prob"], errors="coerce")
        closing["closing_price_decimal"] = pd.to_numeric(
            closing["closing_price_decimal"], errors="coerce"
        )
        closing["closing_fair_prob"] = pd.to_numeric(
            closing["closing_fair_prob"], errors="coerce"
        )
        ev = ev.dropna(subset=["bet_fair_prob"]).copy()
        closing = closing.dropna(subset=["closing_fair_prob"]).copy()
        if ev.empty or closing.empty:
            raise AirflowSkipException("No priced tracked/closing rows for CLV")

        # One tracked row per natural key (latest wins on dedupe; order-stable).
        ev = ev.drop_duplicates(
            subset=["bg_event_id", "market_key", "player_name", "side", "line"],
            keep="last",
        )
        closing = closing.drop_duplicates(
            subset=["bg_event_id", "market_key", "player_name", "side", "line"],
            keep="last",
        )

        merged = clv.compute_clv(ev, closing)
        if merged is None or merged.empty:
            raise AirflowSkipException("No CLV rows produced after join")

        merged = merged.copy()
        merged["recorded_at_utc"] = pd.Timestamp.utcnow().tz_localize(None)
        merged["beat_close"] = merged["beat_close"].astype(bool)

        clv_cols = [
            "clv_hash",
            "bg_event_id",
            "market_key",
            "player_name",
            "side",
            "line",
            "bet_price_decimal",
            "bet_fair_prob",
            "closing_price_decimal",
            "closing_fair_prob",
            "clv_pct",
            "beat_close",
            "recorded_at_utc",
        ]
        for c in clv_cols:
            if c not in merged.columns:
                merged[c] = pd.NA
        out = merged[clv_cols].drop_duplicates(subset=["clv_hash"]).copy()
        return int(bulk_append_new(out, "fact_clv", "clv_hash"))

    n_closing = capture_closing_lines()
    compute_clv_task(n_closing)


dag = bg_closing_line_pipeline()
