from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import gspread
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from gspread_dataframe import set_with_dataframe

from bountygate.utils.db_connection import fetch_data

analysis_complete_asset = Asset("bg_unified_analysis_complete")
arbitrage_complete_asset = Asset("bg_unified_arbitrage_complete")

EXPORT_COLUMNS: List[str] = [
    "sport_title",
    "bookmaker_key",
    "player_name",
    "outcome",
    "line",
    "market_key",
    "price",
    "price_delta",
    "price_mean",
    "price_std",
    "price_count",
    "home_team_name",
    "away_team_name",
    "commence_at_utc",
    "fetched_at_utc",
    "bg_event_id",
]

DEFAULT_TARGET_BOOKMAKERS = ["draftkings", "fanduel", "fanatics", "underdog", "betmgm"]

CREDENTIALS_FILE = Path(__file__).with_name("service_account.json")
SHEET_ID = os.environ.get(
    "BG_UNIFIED_ANALYSIS_SHEET_ID",
    "1Lw0Z3cvQiXG_K-SxI8mhkyTRFX38A0aXyIa5H1YVJaA",
)

INSTRUCTIONS_CONTENT = [
    ["BountyGate Analysis Tool Instructions"],
    [""],
    ["1. Summary Sheet"],
    ["   - check 'Last Updated' to ensure data is fresh."],
    ["   - Review 'Top 10 Value Plays' for immediate opportunities."],
    [""],
    ["2. bg_unified_analysis Sheet"],
    ["   - Contains ALL opportunities across all bookmakers."],
    ["   - Sorted by 'price_delta' (Value) descending."],
    [""],
    ["3. Bookmaker Sheets (e.g., bg_analysis_draftkings)"],
    ["   - Filtered views specific to each bookmaker."],
    [""],
    ["Key Metrics:"],
    ["   - price: The odds offered by the bookmaker."],
    ["   - price_mean: Average odds across the market."],
    ["   - price_delta: (price - price_mean). Higher positive value = better opportunity."],
    ["   - line: The handicap or total line (e.g., 22.5 points)."],
]


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


def _coerce_datetimes(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)
    return df


def get_gspread_client(credentials_path: Path) -> gspread.Client:
    if not credentials_path.exists():
        raise FileNotFoundError(f"Credential file not found at: {credentials_path}")
    try:
        return gspread.service_account(filename=str(credentials_path))
    except Exception as exc:  # pragma: no cover - depends on Google client internals
        raise ConnectionError(f"Failed to authenticate with Google: {exc}") from exc


def get_spreadsheet(client: gspread.Client, identifier: str) -> gspread.Spreadsheet:
    try:
        return client.open_by_key(identifier)
    except SpreadsheetNotFound as exc:
        raise ValueError(
            f"Spreadsheet with identifier '{identifier}' not found. Share it with the service account."
        ) from exc


def get_or_create_worksheet(spreadsheet: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(title)
    except WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=100, cols=20)


def upload_df_to_sheet(
    spreadsheet: gspread.Spreadsheet,
    dataframe: pd.DataFrame,
    sheet_name: str,
    *,
    create_if_missing: bool = True,
    max_retries: int = 5,
) -> Dict[str, Any]:
    if dataframe.empty:
        return {"status": "skipped", "reason": "empty_dataframe", "worksheet_name": sheet_name}

    if create_if_missing:
        worksheet = get_or_create_worksheet(spreadsheet, sheet_name)
    else:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound:
            raise ValueError(f"Worksheet '{sheet_name}' not found and create_if_missing=False")

    clean_df = dataframe.fillna("")

    for attempt in range(max_retries):
        try:
            worksheet.clear()
            set_with_dataframe(
                worksheet,
                clean_df,
                include_index=False,
                include_column_header=True,
                resize=True,
            )
            return {
                "status": "success",
                "rows_uploaded": len(clean_df),
                "worksheet_name": sheet_name,
            }
        except APIError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code in {429, 503} and attempt < max_retries - 1:
                wait_time = 2**attempt
                time.sleep(wait_time)
                continue
            raise
    raise ConnectionError("Max retries exceeded while uploading to Google Sheets")


def update_dashboard_sheet(spreadsheet: gspread.Spreadsheet, df: pd.DataFrame) -> Dict[str, Any]:
    sheet_name = "Dashboard View"
    worksheet = get_or_create_worksheet(spreadsheet, sheet_name)
    
    # 1. Ensure the grid is large enough (e.g., 100 rows, 26 columns)
    # This prevents the "exceeds grid limits" error
    worksheet.resize(rows=100, cols=26)
    worksheet.clear()

    # 2. Summary Statistics
    last_fetched = df["fetched_at_utc"].max() if "fetched_at_utc" in df.columns else "N/A"
    total_player_props = len(df)
    
    stats_data = [
        ["Summary Statistics"],
        ["Last Updated (UTC)", str(last_fetched)],
        ["Total Player Props Found", total_player_props],
    ]
    
    worksheet.update(values=stats_data, range_name="A1")
    worksheet.format("A1", {"textFormat": {"bold": True, "fontSize": 12}})
    worksheet.format("A2:A3", {"textFormat": {"bold": True}})

    # 3. Dynamic Bookmaker Columns
    # Filter columns to only what we need for the dashboard
    summary_cols = ["bookmaker_key", "player_prop", "price", "risk_level"]
    cols_to_use = [c for c in summary_cols if c in df.columns]
    df_clean = df[cols_to_use]

    for i, bookmaker in enumerate(DEFAULT_TARGET_BOOKMAKERS):
        # Calculate the starting column index (1-based: 1, 4, 7, 10...)
        start_col_index = (i * 3) + 1
        
        # Convert index to Letter (1 -> A, 4 -> D, 7 -> G)
        col_letter = gspread.utils.rowcol_to_a1(6, start_col_index).replace("6", "")
        
        bm_df = df_clean.loc[df_clean['bookmaker_key'] == bookmaker]
        bm_df = bm_df[['player_prop', 'price', 'risk_level']].head(25)

        if bm_df.empty:
            continue

        # Set Bookmaker Header
        header_cell = f"{col_letter}6"
        worksheet.update(values=[[bookmaker.capitalize()]], range_name=header_cell)
        worksheet.format(header_cell, {"textFormat": {"bold": True, "fontSize": 12}, "horizontalAlignment": "CENTER"})

        # Write the dataframe 
        # resize=False here because we already resized the sheet manually at the top
        set_with_dataframe(
            worksheet,
            bm_df.fillna(""),
            row=7,
            col=start_col_index,
            include_index=False,
            include_column_header=True,
            resize=False, 
        )

    return {"status": "success", "worksheet_name": sheet_name}


def build_analysis_export_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    working_df = df.copy()
    working_df = _stringify_uuid_columns(working_df)
    working_df = _coerce_datetimes(working_df, ["commence_at_utc", "fetched_at_utc"])

    for column in EXPORT_COLUMNS:
        if column not in working_df.columns:
            working_df[column] = pd.NA

    working_df = working_df[EXPORT_COLUMNS].copy()
    working_df.dropna(subset=["price_delta", "line"], inplace=True)
    working_df.sort_values(
        by=["price_delta", "player_name", "bookmaker_key"],
        ascending=[False, True, True],
        inplace=True,
    )
    working_df.reset_index(drop=True, inplace=True)

    if "price_delta" in working_df.columns:
        working_df["price_delta"] = working_df["price_delta"].astype(float).round(3)
    for column in ("price_mean", "price_std"):
        if column in working_df.columns:
            working_df[column] = working_df[column].astype(float).round(2)

    return working_df


def _resolve_target_bookmakers() -> List[str]:
    env_value = os.environ.get("BG_SHEETS_BOOKMAKERS")
    if env_value:
        return [item.strip() for item in env_value.split(",") if item.strip()]
    return DEFAULT_TARGET_BOOKMAKERS


@dag(
    dag_id="bg_analysis_sheets",
    description="Push bg_unified_analysis outputs to Google Sheets for manual review",
    schedule=[analysis_complete_asset, arbitrage_complete_asset],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["google_sheets", "analysis"],
)
def bg_analysis_sheets_pipeline() -> None:
    # DataFrames are not pushed through XCom — Airflow 3's serializer rejects them.
    # Fetch + transform + upload all happen inside one task; only the small summary
    # dict crosses the XCom boundary.

    @task()
    def update_google_sheets() -> Dict[str, Any]:
        df = fetch_data("SELECT * FROM bg_unified_analysis")
        if df is None or df.empty:
            raise AirflowSkipException("bg_unified_analysis returned no rows")
        df = _stringify_uuid_columns(df)
        df = _coerce_datetimes(df, ["commence_at_utc", "fetched_at_utc"])

        arbitrage_query = """
        SELECT *
        FROM bg_unified_arbitrage
        WHERE under_line = over_line
          AND under_bookmaker_key NOT IN ('underdog', 'betonlineag', 'betrivers', 'bovada')
          AND over_bookmaker_key NOT IN ('underdog', 'betonlineag', 'betrivers', 'bovada')
          AND arb_ev > -1.01;
        """
        arbitrage_df = fetch_data(arbitrage_query)
        if arbitrage_df is None or arbitrage_df.empty:
            arbitrage_df = pd.DataFrame()
        else:
            arbitrage_df = _stringify_uuid_columns(arbitrage_df)

        working_df = build_analysis_export_frame(df)
        if working_df.empty:
            raise AirflowSkipException("No rows with price deltas available for Google Sheets upload")

        arbitrage_export_df = arbitrage_df.copy()
        arbitrage_export_df.drop(
            columns=[
                "under_bookmaker_key_low",
                "under_price_low",
                "over_bookmaker_key_low",
                "over_price_low",
            ],
            errors="ignore",
            inplace=True,
        )

        client = get_gspread_client(CREDENTIALS_FILE)
        spreadsheet = get_spreadsheet(client, SHEET_ID)

        summary: Dict[str, Any] = {}

        working_df = working_df[(working_df['price'] <= 2.5) & (working_df['price_count'] >= 3)]
        working_df['test_score_1'] = working_df['price_delta'] / (working_df['price_mean'] * 1.5)
        working_df.sort_values(by='test_score_1', ascending=False, inplace=True)

        dashboard_df = working_df.copy()

        outcome_map = {
            'Over': '⬆️',
            'Under': '⬇️'
            }

        dashboard_df['market_display'] = dashboard_df['market_key'].str.replace('player_', '').str.replace('_', ' ')

        dashboard_df['outcome_arrow'] = dashboard_df['outcome'].map(outcome_map)
        dashboard_df['player_prop'] = dashboard_df['player_name'] + " " + dashboard_df['outcome_arrow'] + " " + dashboard_df['line'].astype(str) + " " + dashboard_df['market_display']

        temp_df = dashboard_df[['bookmaker_key', 'player_prop', 'price', 'price_delta', 'fetched_at_utc']].copy()

        def categorize_risk(price: float) -> str:
            if price >= 2.2:
                return 'Very High'
            elif 2.05 <= price < 2.2:
                return 'High'
            elif 1.80 <= price < 2.05:
                return 'Medium'
            elif 1.65 <= price < 1.80:
                return 'Low'
            else:
                return 'Very Low'

        temp_df['risk_level'] = temp_df['price'].apply(categorize_risk)

        update_dashboard_sheet(spreadsheet, temp_df)

        summary["bg_unified_analysis"] = upload_df_to_sheet(
            spreadsheet,
            working_df,
            sheet_name="data_detail",
        )

        summary["arbitrage"] = upload_df_to_sheet(
            spreadsheet,
            arbitrage_export_df,
            sheet_name="Arbitrage",
        )

        for bookmaker in _resolve_target_bookmakers():
            bookmaker_df = working_df[working_df["bookmaker_key"] == bookmaker]
            if bookmaker_df.empty:
                continue
            sheet_name = f"{bookmaker}_analysis"
            summary[sheet_name] = upload_df_to_sheet(
                spreadsheet,
                bookmaker_df,
                sheet_name=sheet_name,
            )
        return summary

    update_google_sheets()


dag = bg_analysis_sheets_pipeline()
