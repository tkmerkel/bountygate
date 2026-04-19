import io
import pandas as pd
import requests
from pandas import DataFrame
from app.shared.python.bountygate.utils.mage import data_loader, test


@data_loader
def load_data_from_api(**kwargs) -> DataFrame:
    """
    Template for loading data from API
    """
    url = 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv?raw=True'

    return pd.read_csv(url)


@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
