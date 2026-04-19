import json
import requests
from app.shared.python.bountygate.utils.mage import data_loader, test


@data_loader
def load_underdog_ou_lines(**kwargs) -> dict:

    # Fetch data from the API
    overunderlines_url = 'https://api.underdogfantasy.com/beta/v5/over_under_lines'
    r = requests.get(overunderlines_url)
    return json.loads(r.text)


# @test
# def test_output(dict) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert dict is not None, 'The output is undefined'