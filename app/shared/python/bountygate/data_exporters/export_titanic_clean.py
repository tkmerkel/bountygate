from pathlib import Path
from pandas import DataFrame

from app.shared.python.bountygate.utils.mage import data_exporter


@data_exporter
def export_data_to_file(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to filesystem.

    Docs: https://docs.mage.ai/design/data-loading#example-loading-data-from-a-file
    """
    filepath = Path("titanic_clean.csv")
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False)
