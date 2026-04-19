import pandas as pd
from app.shared.python.bountygate.utils.etl_assets import odds_url, odds_apiKey, active_sports
from app.shared.python.bountygate.utils.mage import data_loader, test

@data_loader
def get_odds_sports(**kwargs) -> pd.DataFrame:

    output_df = pd.DataFrame()
    for sport in active_sports:
        events_endpoint = f"{odds_url}/v4/sports/{sport}/events?apiKey={odds_apiKey}"
        events = pd.read_json(events_endpoint)
        output_df = pd.concat([output_df, events], ignore_index=True)
    return output_df

@test
def test_output(output_df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output_df is not None, 'The output is undefined'