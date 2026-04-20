from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from bountygate.utils.etl_assets import resolve_market_key

LOGGER = logging.getLogger(__name__)
UNDERDOG_LINES_URL = "https://api.underdogfantasy.com/beta/v6/over_under_lines"
DEFAULT_TIMEOUT = 15


@dataclass(frozen=True)
class RegressionResult:
    coefficient_a: float
    coefficient_b: float
    r_squared: float


def fetch_underdog_payload(
    session: Optional[requests.Session] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    """Fetch the raw Underdog over/under payload."""
    session = session or requests.Session()
    LOGGER.info("Fetching Underdog payload from %s", UNDERDOG_LINES_URL)
    response = session.get(UNDERDOG_LINES_URL, timeout=timeout)
    response.raise_for_status()
    return response.json()


def normalize_underdog_payload(payload: Mapping[str, Any]) -> Dict[str, pd.DataFrame]:
    """Normalize the Underdog payload into tabular DataFrames."""
    games_df = pd.json_normalize(payload.get("games", []))
    if not games_df.empty:
        games_df = games_df[
            [
                "id",
                "scheduled_at",
                "sport_id",
                "abbreviated_title",
                "full_team_names_title",
            ]
        ]

    appearances_df = pd.DataFrame(payload.get("appearances", []))
    if not appearances_df.empty and "badges" in appearances_df.columns:
        appearances_df = appearances_df.drop(columns=["badges"])

    over_under_lines_df = pd.json_normalize(payload.get("over_under_lines", []))
    options_series = over_under_lines_df.get("options")
    options_df = pd.json_normalize(options_series.explode()) if options_series is not None else pd.DataFrame()
    if not options_df.empty and "parameters" in options_df.columns:
        options_df["parameters"] = options_df["parameters"].apply(
            lambda value: json.dumps(value, sort_keys=True) if isinstance(value, dict) and value else None
        )
        if options_df["parameters"].isna().all():
            options_df = options_df.drop(columns=["parameters"])
    if not over_under_lines_df.empty:
        over_under_lines_df = over_under_lines_df.drop(columns=["options", "over_under.id"], errors="ignore")

    players_df = pd.DataFrame(payload.get("players", []))

    return {
        "ud_games": games_df,
        "ud_appearances": appearances_df,
        "ud_over_under_lines": over_under_lines_df,
        "ud_options": options_df,
        "ud_players": players_df,
    }


def prepare_ud_analysis_frame(staging_tables: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    """Rebuild the ud_analysis frame joining normalized staging tables."""
    options_df = staging_tables.get("ud_options", pd.DataFrame()).copy()
    over_under_lines_df = staging_tables.get("ud_over_under_lines", pd.DataFrame()).copy()
    appearances_df = staging_tables.get("ud_appearances", pd.DataFrame()).copy()
    players_df = staging_tables.get("ud_players", pd.DataFrame()).copy()
    games_df = staging_tables.get("ud_games", pd.DataFrame()).copy()

    if options_df.empty or over_under_lines_df.empty:
        LOGGER.warning("Underdog options or over_under_lines DataFrames are empty")
        return pd.DataFrame()

    oul_rename_mapping = {
        "over_under.appearance_stat.id": "appearance_stat_id",
        "over_under.appearance_stat.appearance_id": "appearance_id",
        "over_under.appearance_stat.display_stat": "display_stat",
        "over_under.appearance_stat.graded_by": "graded_by",
        "over_under.appearance_stat.pickem_stat_id": "pickem_stat_id",
        "over_under.appearance_stat.stat": "stat",
        "over_under.boost": "boost",
        "over_under.has_alternates": "has_alternates",
        "over_under.option_priority": "option_priority",
        "over_under.scoring_type_id": "scoring_type_id",
        "over_under.title": "title",
    }
    over_under_lines_df = over_under_lines_df.rename(columns=oul_rename_mapping)
    over_under_lines_df = over_under_lines_df.rename(columns={"id": "over_under_line_id"})

    appearances_df = appearances_df.rename(columns={"id": "appearance_id"})
    players_df = players_df.rename(columns={"id": "player_id"})
    games_df = games_df.rename(columns={"id": "game_id"})

    appearances_enriched = appearances_df.merge(
        games_df[["game_id", "abbreviated_title", "full_team_names_title", "scheduled_at", "sport_id"]],
        left_on="match_id",
        right_on="game_id",
        how="left",
    )
    appearances_enriched = appearances_enriched.merge(
        players_df[
            [
                "player_id",
                "first_name",
                "last_name",
                "position_id",
                "team_id",
                "sport_id",
                "image_url",
            ]
        ],
        on="player_id",
        how="left",
        suffixes=("", "_player"),
    )

    over_under_enriched = over_under_lines_df.merge(
        appearances_enriched,
        on="appearance_id",
        how="left",
        suffixes=("", "_appearance"),
    )

    ud_df = options_df.merge(
        over_under_enriched,
        on="over_under_line_id",
        how="left",
        suffixes=("", "_oul"),
    )

    return ud_df


def build_ud_unified_lines(
    ud_df: pd.DataFrame,
    league_to_sport: Mapping[str, str],
    fetched_at: Optional[datetime] = None,
) -> pd.DataFrame:
    """Format Underdog lines into the COMMON_COLUMNS schema."""
    if ud_df.empty:
        return pd.DataFrame()

    fetched_at = fetched_at or datetime.now(timezone.utc)
    formatted = ud_df.copy()
    formatted["player_name"] = (
        formatted["first_name"].fillna("").str.strip()
        + " "
        + formatted["last_name"].fillna("").str.strip()
    ).str.strip()
    formatted["bm_market_key"] = formatted.get("display_stat")
    formatted["market_key"] = formatted.apply(
        lambda row: resolve_market_key(
            "underdog",
            row.get("sport_key"),
            row.get("bm_market_key"),
            fallback=row.get("stat"),
        ),
        axis=1,
    )
    formatted["line"] = formatted.get("stat_value")
    formatted["outcome"] = formatted.get("choice").map({"higher": "Over", "lower": "Under"})
    formatted["price"] = formatted.get("decimal_price")
    formatted["multiplier"] = formatted.get("payout_multiplier")
    formatted["bookmaker_key"] = "underdog"
    formatted["sport_title"] = formatted.get("sport_id")
    formatted["sport_key"] = formatted["sport_title"].map(league_to_sport)
    formatted["commence_time"] = formatted.get("scheduled_at")
    formatted["home_team"] = formatted.get("full_team_names_title").str.split(" @ ").str[1]
    formatted["away_team"] = formatted.get("full_team_names_title").str.split(" @ ").str[0]
    formatted["event_id"] = formatted.get("match_id")
    formatted["fetched_at_utc"] = fetched_at

    columns = [
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

    return formatted[columns].dropna(subset=["sport_key"])


def calc_avg_spread_pct_change(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate average spread change grouped by Underdog id."""
    if df.empty:
        return pd.DataFrame(columns=["id", "first_spread", "last_spread", "pct_change", "delta"])

    working = df.copy()
    working["update_time"] = pd.to_datetime(working["update_time"])

    idx_first = working.groupby("id")["update_time"].idxmin()
    idx_last = working.groupby("id")["update_time"].idxmax()

    first_spreads = working.loc[idx_first, ["id", "score"]].set_index("id")["score"]
    last_spreads = working.loc[idx_last, ["id", "score"]].set_index("id")["score"]

    spread_by_id = pd.DataFrame({
        "first_spread": first_spreads,
        "last_spread": last_spreads,
    })
    spread_by_id["pct_change"] = (
        (spread_by_id["last_spread"] - spread_by_id["first_spread"]) / spread_by_id["first_spread"]
    ) * 100
    spread_by_id["delta"] = spread_by_id["last_spread"] - spread_by_id["first_spread"]
    spread_by_id.reset_index(inplace=True)
    return spread_by_id


def fit_exponential_regression(ud_df: pd.DataFrame) -> Tuple[pd.DataFrame, RegressionResult]:
    """Fit an exponential regression on payout_multiplier vs implied probability."""
    working = ud_df.copy()
    working["decimal_price"] = working["decimal_price"].astype(float)
    working["payout_multiplier"] = working["payout_multiplier"].astype(float)
    working = working[working["payout_multiplier"] < 1.5].copy()

    working["ud_impl_prob"] = np.round(1 / working["decimal_price"] * 100, 2)

    x = working["ud_impl_prob"].values
    y = working["payout_multiplier"].values

    if len(x) < 2:
        LOGGER.warning("Not enough data points to perform regression")
        return working, RegressionResult(coefficient_a=0.0, coefficient_b=0.0, r_squared=0.0)

    coefficient_b, loga = np.polyfit(x, np.log(y), 1)
    coefficient_a = float(np.exp(loga))

    y_pred = coefficient_a * np.exp(coefficient_b * x)
    ss_res = float(np.sum((y - y_pred) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2)) if len(y) else 0.0
    r_squared = 1 - ss_res / ss_tot if ss_tot else 0.0

    working["ud_predicted"] = np.round(coefficient_a * np.exp(coefficient_b * working["ud_impl_prob"]), 2)
    working["predict_delta"] = working["payout_multiplier"] - working["ud_predicted"]

    return working, RegressionResult(
        coefficient_a=coefficient_a,
        coefficient_b=coefficient_b,
        r_squared=r_squared,
    )


def build_ud_scorecard(
    ud_df: pd.DataFrame,
    props_df: pd.DataFrame,
    hd_ud_df: pd.DataFrame,
    sport_market_map: Mapping[str, Mapping[str, str]],
    sport_league_map: Mapping[str, str],
) -> Dict[str, pd.DataFrame]:
    """Recreate the Underdog scorecard outputs using unified DataFrames."""
    if ud_df.empty or props_df.empty:
        LOGGER.warning("Missing data for Underdog scorecard generation")
        return {
            "ud_analysis": pd.DataFrame(),
            "ud_ou_lines_details": pd.DataFrame(),
            "hd_ud_analysis": pd.DataFrame(),
            "hd_ud_trend": pd.DataFrame(),
            "bg_reference": pd.DataFrame(),
        }

    working, regression = fit_exponential_regression(ud_df)

    sport_joined_market_map = {
        ud_stat: market
        for sport, market_map in sport_market_map.items()
        if sport in {"NFL", "NHL", "MLB", "WNBA", "NBA", "CFB", "FIFA", "CBB"}
        for ud_stat, market in market_map.items()
    }

    working["market_key"] = working["stat"].map(sport_joined_market_map)
    working["stat_value"] = working["stat_value"].astype(str)

    props = props_df.copy()
    props["choice"] = props["name"].map({"Over": "higher", "Under": "lower"})
    props["stat_value"] = props["point"].astype(str)
    name_split = props["description"].str.split(" ", n=1, expand=True)
    props["first_name"] = name_split[0]
    props["last_name"] = name_split[1]
    props["sport_id"] = props["sport"].map(sport_league_map)

    odds_df = props[
        [
            "sport_id",
            "market_key",
            "choice",
            "stat_value",
            "first_name",
            "last_name",
            "bookmaker_key",
            "price",
            "price_mean",
            "impl_prob",
            "impl_prob_mean",
            "bookmaker_count_sum",
            "event_id",
            "update_time",
        ]
    ].copy()

    output_df = odds_df.merge(
        working[
            [
                "id",
                "sport_id",
                "first_name",
                "last_name",
                "choice",
                "stat_value",
                "display_stat",
                "payout_multiplier",
                "stat",
                "abbreviated_title",
                "decimal_price",
                "ud_impl_prob",
                "ud_predicted",
                "predict_delta",
                "market_key",
            ]
        ],
        on=["sport_id", "market_key", "choice", "stat_value", "first_name", "last_name"],
        how="left",
    )

    output_df.dropna(subset=["payout_multiplier"], inplace=True)
    output_df["impl_prob_mean"] = output_df["impl_prob_mean"] * 100

    output_df["bm_predicted_mult"] = regression.coefficient_a * np.exp(
        regression.coefficient_b * output_df["impl_prob_mean"]
    )
    output_df["mult_delta"] = output_df["payout_multiplier"] - output_df["bm_predicted_mult"]
    output_df["score"] = (output_df["mult_delta"] / (output_df["payout_multiplier"] ** 2)) * 100
    output_df["score"] = output_df["score"].round(2)

    output_df["bm_spread"] = (1 + output_df["payout_multiplier"]) - output_df["price"]
    output_df["avg_spread"] = (1 + output_df["payout_multiplier"]) - output_df["price_mean"]
    output_df = output_df[output_df["payout_multiplier"] < 5]
    output_df.sort_values(by="score", ascending=False, inplace=True)

    hd_output_df = output_df[
        [
            "id",
            "sport_id",
            "first_name",
            "last_name",
            "choice",
            "stat_value",
            "display_stat",
            "price_mean",
            "impl_prob_mean",
            "bookmaker_count_sum",
            "ud_impl_prob",
            "ud_predicted",
            "predict_delta",
            "bm_predicted_mult",
            "mult_delta",
            "score",
            "payout_multiplier",
            "abbreviated_title",
            "decimal_price",
            "avg_spread",
            "update_time",
        ]
    ].drop_duplicates()

    hd_ud_df = pd.concat([hd_ud_df, hd_output_df], ignore_index=True)
    trend_df = calc_avg_spread_pct_change(hd_ud_df)

    merged_output = output_df.merge(trend_df, on="id", how="left")

    bg_reference_df = pd.DataFrame(
        [
            {
                "bg_reference_id": "ud_regression",
                "bg_reference_value": (
                    f"Exp fit: M = {regression.coefficient_a:.4f}·e^({regression.coefficient_b:.4f}·impl_prob), "
                    f"R²={regression.r_squared:.4f}"
                ),
                "update_time": datetime.now(timezone.utc),
            }
        ]
    )

    return {
        "ud_analysis": merged_output[
            [
                "id",
                "sport_id",
                "first_name",
                "last_name",
                "bookmaker_key",
                "choice",
                "stat_value",
                "display_stat",
                "price",
                "price_mean",
                "impl_prob",
                "impl_prob_mean",
                "bookmaker_count_sum",
                "ud_impl_prob",
                "ud_predicted",
                "predict_delta",
                "bm_predicted_mult",
                "mult_delta",
                "score",
                "payout_multiplier",
                "bm_spread",
                "avg_spread",
                "abbreviated_title",
                "decimal_price",
                "event_id",
                "update_time",
                "first_spread",
                "last_spread",
                "pct_change",
                "delta",
            ]
        ],
        "ud_ou_lines_details": working,
        "hd_ud_analysis": hd_ud_df,
        "hd_ud_trend": trend_df,
        "bg_reference": bg_reference_df,
    }
