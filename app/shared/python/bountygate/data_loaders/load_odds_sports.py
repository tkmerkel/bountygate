import pandas as pd
from app.shared.python.bountygate.utils.etl_assets import odds_url, odds_apiKey
from app.shared.python.bountygate.utils.mage import data_loader, test


@data_loader
def load_odds_sports(**kwargs) -> pd.DataFrame:

    base_url = 'https://api.the-odds-api.com'
    endpoint = f"{base_url}/v4/sports/?apiKey={odds_apiKey}"
    return pd.read_json(endpoint)


@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'