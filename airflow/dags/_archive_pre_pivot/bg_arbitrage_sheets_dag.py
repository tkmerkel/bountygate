from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import gspread
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from gspread_dataframe import set_with_dataframe

from bountygate.utils.db_connection import fetch_data


CREDENTIALS_FILE = Path(__file__).with_name("service_account.json")
SHEET_ID = os.environ.get(
    "BG_ARBITRAGE_SHEET_ID",
    "1J7CdXLzjLNorzQPdG86Wns4X_XEOfTU-6HnBzVixwcg",
)

OPPORTUNITIES_SHEET = "Opportunities"
OPPS_TRANSPOSED_SHEET = "Opps Transposed"
ALT_OPPORTUNITIES_SHEET = "Alt Opportunities"
ALT_OPPS_TRANSPOSED_SHEET = "Alt Opps Transposed"

player_props_arbitrage_complete_asset = Asset("bg_arbitrage_player_props_complete")
arbitrage_sheets_complete_asset = Asset("bg_arbitrage_sheets_complete")


SQL_QUERY = """
select
    player_name,
    market_key,
    under_line,
    over_line,
    under_bookmaker_key,
    over_bookmaker_key,
    under_price,
    over_price,
    wager_under,
    wager_over,
    payout,
    arb_ev,
    roi,
    hours_until_commence,
    fetched_at_utc
from bg_arbitrage_player_props
where under_line = over_line
and under_bookmaker_key not in ('underdog', 'betonlineag', 'betrivers', 'bovada')
and over_bookmaker_key not in ('underdog', 'betonlineag', 'betrivers', 'bovada')
and arb_ev > -2.01
and hours_until_commence > -0.01
order by roi DESC;
""".strip()


SQL_QUERY_ALT = SQL_QUERY.replace("from bg_arbitrage_player_props", "from bg_arbitrage_player_props_alt")


def get_gspread_client(credentials_path: Path) -> gspread.Client:
    if not credentials_path.exists():
        raise FileNotFoundError(f"Credential file not found at: {credentials_path}")
    try:
        return gspread.service_account(filename=str(credentials_path))
    except Exception as exc:  # pragma: no cover
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
    max_retries: int = 5,
) -> Dict[str, Any]:
    if dataframe is None or dataframe.empty:
        return {"status": "skipped", "reason": "empty_dataframe", "worksheet_name": sheet_name}

    worksheet = get_or_create_worksheet(spreadsheet, sheet_name)
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
            return {"status": "success", "rows_uploaded": len(clean_df), "worksheet_name": sheet_name}
        except APIError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code in {429, 503} and attempt < max_retries - 1:
                time.sleep(2**attempt)
                continue
            raise
    raise ConnectionError("Max retries exceeded while uploading to Google Sheets")


def build_transposed_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    working = df.copy().reset_index(drop=True)
    transposed = working.transpose().reset_index()
    transposed.rename(columns={"index": "field"}, inplace=True)
    return transposed


def _fetch_opportunity_frame(query: str) -> pd.DataFrame:
    df = fetch_data(query)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if "fetched_at_utc" in df.columns:
        df["fetched_at_utc"] = pd.to_datetime(df["fetched_at_utc"], errors="coerce", utc=True)
    return df


@dag(
    dag_id="bg_arbitrage_sheets",
    description="Update Google Sheet with player-prop arbitrage opportunities",
    schedule=[player_props_arbitrage_complete_asset],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["sheets", "arbitrage", "player-props"],
)
def bg_arbitrage_sheets_pipeline() -> None:
    # DataFrames are not pushed through XCom — Airflow 3's serializer rejects them.
    # Both queries + uploads happen inside a single task; only the summary dict
    # crosses the XCom boundary.

    @task(outlets=[arbitrage_sheets_complete_asset])
    def update_google_sheets() -> Dict[str, Any]:
        df = _fetch_opportunity_frame(SQL_QUERY)
        alt_df = _fetch_opportunity_frame(SQL_QUERY_ALT)

        if df.empty and alt_df.empty:
            raise AirflowSkipException("No opportunities to upload")

        client = get_gspread_client(CREDENTIALS_FILE)
        spreadsheet = get_spreadsheet(client, SHEET_ID)

        results: Dict[str, Any] = {"rows": int(len(df))}
        if not df.empty:
            results["opportunities"] = upload_df_to_sheet(spreadsheet, df, OPPORTUNITIES_SHEET)
            results["opps_transposed"] = upload_df_to_sheet(
                spreadsheet, build_transposed_frame(df), OPPS_TRANSPOSED_SHEET
            )

        results["alt_rows"] = int(len(alt_df))
        if not alt_df.empty:
            results["alt_opportunities"] = upload_df_to_sheet(
                spreadsheet, alt_df, ALT_OPPORTUNITIES_SHEET
            )
            results["alt_opps_transposed"] = upload_df_to_sheet(
                spreadsheet, build_transposed_frame(alt_df), ALT_OPPS_TRANSPOSED_SHEET
            )

        return results

    update_google_sheets()


dag = bg_arbitrage_sheets_pipeline()


