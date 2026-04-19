import pandas as pd
from app.shared.python.bountygate.utils.etl_assets import odds_url, odds_apiKey, active_sports
from app.shared.python.bountygate.utils.db_connection import fetch_data
from app.shared.python.bountygate.utils.mage import data_loader, test

@data_loader
def load_ud_analysis(**kwargs) -> pd.DataFrame:

    props_query = """select * from odds_player_props;"""

    output_df = fetch_data(props_query)
    return output_df


# @test
# def test_output(output_df) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output_df is not None, 'The output is undefined'