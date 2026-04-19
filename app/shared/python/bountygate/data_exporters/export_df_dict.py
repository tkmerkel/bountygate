from collections.abc import Mapping
from typing import Any

from pandas import DataFrame
from sqlalchemy import create_engine

from app.shared.python.bountygate.utils.db_connection import DATABASE_URL
from app.shared.python.bountygate.utils.mage import data_exporter


@data_exporter
def export_data_to_postgres(df_dict: dict, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    if not isinstance(df_dict, Mapping):
        raise TypeError("df_dict must be a mapping of table names to DataFrames")

    engine = create_engine(DATABASE_URL)
    try:
        for table_name, df in df_dict.items():
            if not isinstance(df, DataFrame):
                raise TypeError(f"Expected DataFrame for table '{table_name}', got {type(df)}")

            if_exists = "append" if str(table_name).startswith("temporary") else "replace"
            df.to_sql(
                name=table_name,
                con=engine,
                schema="public",
                if_exists=if_exists,
                index=False,
            )
    finally:
        engine.dispose()