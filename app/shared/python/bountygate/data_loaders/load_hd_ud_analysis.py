import pandas as pd
from app.shared.python.bountygate.utils.db_connection import fetch_data
from app.shared.python.bountygate.utils.mage import data_loader, test


@data_loader
def load_data_from_postgres(*args, **kwargs):
    try:
        hd_ud_query = 'SELECT * FROM hd_ud_analysis'
        output_df = fetch_data(hd_ud_query)
    except:
        output_df = pd.DataFrame()
    return output_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'