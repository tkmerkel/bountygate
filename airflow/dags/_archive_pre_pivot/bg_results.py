"""DAG: bg_results — settle finished games + player props from free feeds.

Independent */30 cron. For events that have recently commenced (so they may now
be final), pull results from the free feeds and append:

  * fact_game_result          — game-level score/winner (ESPN scoreboard, all
                                sports). PK is bg_event_id; we only write FINAL
                                games, so insert-new-only is correct (a final
                                result is immutable).
  * fact_player_stat_result   — per-player box-score stats for prop grading
                                (MLB StatsAPI + NHL api-web). result_hash =
                                md5(bg_event_id|player_id|stat_key).

Matching is name+date based (dim_team is empty) via
bountygate.enrichment.match. Empty inputs raise AirflowSkipException.

Once player stats land, mart_bet_performance (migration 016) grades prior +EV
picks against them automatically (it's a view).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

# dags dir is on sys.path in Airflow; bountygate is on app/shared/python.
from bg_arb_pipeline_lib.db import bulk_append_new
from bountygate.enrichment import clients, match, results, statmap
from bountygate.utils.db_connection import fetch_data


def _result_hash(bg_event_id, player_id, stat_key) -> str:
    raw = f"{bg_event_id}|{player_id}|{stat_key}"
    return hashlib.md5(raw.encode()).hexdigest()


def _recent_event_dates() -> list:
    """The UTC calendar dates we should poll (yesterday + today UTC)."""
    today = datetime.now(timezone.utc).date()
    return [today - timedelta(days=1), today]


@dag(
    dag_id="bg_results",
    description="Settle finished games + player props from ESPN/MLB/NHL free feeds",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "enrichment", "results"],
)
def bg_results_pipeline() -> None:

    @task()
    def load_recent_events() -> list[dict]:
        df = fetch_data(
            "SELECT bg_event_id, sport_key, home_team_name, away_team_name, "
            "       commence_at_utc "
            "FROM dim_event "
            "WHERE commence_at_utc >= now() - interval '2 days' "
            "  AND commence_at_utc <= now() + interval '6 hours'"
        )
        if df is None or df.empty:
            raise AirflowSkipException("No recent events to settle")
        df = df.copy()
        df["commence_at_utc"] = df["commence_at_utc"].astype(str)
        return df.to_dict("records")

    @task()
    def settle_game_results(events: list[dict]) -> int:
        if not events:
            raise AirflowSkipException("No events")
        sports = sorted({str(e["sport_key"]) for e in events if e.get("sport_key")})
        dates = _recent_event_dates()
        captured_at = pd.Timestamp.utcnow().tz_localize(None)
        rows: list[dict] = []

        for sport_key in sports:
            if sport_key not in clients.ESPN_PATHS:
                continue
            for on_date in dates:
                payload = clients.fetch_json(
                    clients.build_espn_scoreboard_url(sport_key, on_date)
                )
                if not payload:
                    continue
                for g in results.parse_espn_scoreboard(payload, sport_key):
                    if not g["completed"]:
                        continue
                    bg_event_id = match.match_game_to_event(
                        sport_key, g["home_team_name"], g["away_team_name"],
                        g["commence_at_utc"], events,
                    )
                    if not bg_event_id:
                        continue
                    rows.append(
                        {
                            "bg_event_id": bg_event_id,
                            "sport_key": sport_key,
                            "home_score": g["home_score"],
                            "away_score": g["away_score"],
                            "winner_team_id": None,
                            "status": g["status"],
                            "result_source": "espn",
                            "settled_at_utc": captured_at,
                            "raw": None,
                        }
                    )

        if not rows:
            raise AirflowSkipException("No final games matched to events")
        out = pd.DataFrame(rows).drop_duplicates(subset=["bg_event_id"])
        return int(bulk_append_new(out, "fact_game_result", "bg_event_id"))

    @task()
    def settle_player_stats(events: list[dict]) -> int:
        if not events:
            raise AirflowSkipException("No events")
        dates = _recent_event_dates()
        captured_at = pd.Timestamp.utcnow().tz_localize(None)
        rows: list[dict] = []

        has_mlb = any(e.get("sport_key") == "baseball_mlb" for e in events)
        has_nhl = any(e.get("sport_key") == "icehockey_nhl" for e in events)

        # --- MLB box scores ---
        if has_mlb:
            for on_date in dates:
                sched = clients.fetch_json(clients.build_mlb_schedule_url(on_date))
                if not sched:
                    continue
                for game in results.parse_mlb_schedule(sched):
                    if not game["final"]:
                        continue
                    bg_event_id = match.match_game_to_event(
                        "baseball_mlb", game["home_team_name"],
                        game["away_team_name"], game["commence_at_utc"], events,
                    )
                    if not bg_event_id:
                        continue
                    box = clients.fetch_json(
                        clients.build_mlb_boxscore_url(game["game_pk"])
                    )
                    if not box:
                        continue
                    rows.extend(
                        _stat_rows(
                            statmap.extract_mlb_player_stats(box),
                            bg_event_id, "mlb-statsapi", captured_at,
                        )
                    )

        # --- NHL box scores ---
        if has_nhl:
            for on_date in dates:
                sched = clients.fetch_json(clients.build_nhl_schedule_url(on_date))
                if not sched:
                    continue
                for game in results.parse_nhl_schedule(sched):
                    if not game["final"]:
                        continue
                    bg_event_id = match.match_game_to_event(
                        "icehockey_nhl", game["home_team_name"],
                        game["away_team_name"], game["commence_at_utc"], events,
                    )
                    if not bg_event_id:
                        continue
                    box = clients.fetch_json(
                        clients.build_nhl_boxscore_url(game["game_id"])
                    )
                    if not box:
                        continue
                    rows.extend(
                        _stat_rows(
                            statmap.extract_nhl_player_stats(box),
                            bg_event_id, "nhl-api", captured_at,
                        )
                    )

        if not rows:
            raise AirflowSkipException("No player-stat rows for finished games")
        out = pd.DataFrame(rows).drop_duplicates(subset=["result_hash"])
        return int(bulk_append_new(out, "fact_player_stat_result", "result_hash"))

    ev = load_recent_events()
    settle_game_results(ev)
    settle_player_stats(ev)


def _stat_rows(stat_rows, bg_event_id, source, captured_at) -> list[dict]:
    out = []
    for r in stat_rows:
        out.append(
            {
                "result_hash": _result_hash(bg_event_id, r["player_id"], r["stat_key"]),
                "bg_event_id": bg_event_id,
                "player_id": r["player_id"],
                "player_name": r["player_name"],
                "sport_key": r["sport_key"],
                "stat_key": r["stat_key"],
                "stat_value": r["stat_value"],
                "result_source": source,
                "settled_at_utc": captured_at,
            }
        )
    return out


dag = bg_results_pipeline()
