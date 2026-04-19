import pandas as pd
import requests
from datetime import datetime
from app.shared.python.bountygate.utils.etl_assets import (
    odds_url,
    odds_apiKey,
    active_sports,
    get_sport_market_list,
    sport_region_map,
)
from app.shared.python.bountygate.utils.mage import data_loader, test
import concurrent.futures
from itertools import chain

def fetch_event_odds(args):
    sport, event_id, now_str = args
    market_list = get_sport_market_list(sport)
    regions = sport_region_map.get(sport, "")
    
    props_endpoint = f"{odds_url}/v4/sports/{sport}/events/{event_id}/odds"
    params = {
        "apiKey": odds_apiKey,
        "regions": regions,
    }
    if market_list:
        params["markets"] = ",".join(market_list)
    
    try:
        # Adding a timeout to prevent hanging requests
        response = requests.get(props_endpoint, params=params, timeout=10)
        if response.status_code != 200:
            return []
        props = response.json()
    except (requests.exceptions.RequestException, ValueError):
        return []

    # Check if event is not found or other API messages
    if isinstance(props, dict) and 'message' in props:
        return []

    records = []
    for bookmaker in props.get('bookmakers', []):
        bookmaker_key = bookmaker.get('key')
        for market in bookmaker.get('markets', []):
            market_key = market.get('key')
            for outcome in market.get('outcomes', []):
                records.append({
                    'name': outcome.get('name'),
                    'description': outcome.get('description'),
                    'price': outcome.get('price'),
                    'point': outcome.get('point'),
                    'sport': sport,
                    'market_key': market_key,
                    'bookmaker_key': bookmaker_key,
                    'event_id': event_id,
                    'update_time': now_str
                })
    return records

@data_loader
def get_odds_sports(events_df, *kwargs):
    now_str = datetime.now()
    
    tasks = []
    for sport in active_sports:
        sport_events_df = events_df[events_df['sport_key'] == sport]
        if not sport_events_df.empty:
            for event_id in sport_events_df['id'].unique():
                tasks.append((sport, event_id, now_str))

    all_records = []
    # Use ThreadPoolExecutor to fetch data in parallel for I/O-bound tasks
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # map will apply fetch_event_odds to each item in tasks
        results = executor.map(fetch_event_odds, tasks)
        # results is an iterator of lists, chain.from_iterable flattens it
        all_records = list(chain.from_iterable(results))

    # Convert all records to a single DataFrame at the end
    df = pd.DataFrame(all_records)

    if df.empty:
        output_df = pd.DataFrame()
    else:
        df['impl_prob'] = 1 / df['price']
        df['bookmaker_count'] = 1

        # Create a unique identifier for each proposition
        df['prop_id'] = (df['event_id'].astype(str) + '_' +
                         df['market_key'].astype(str) + '_' +
                         df['bookmaker_key'].astype(str) + '_' +
                         df['description'].astype(str) + '_' +
                         df['point'].astype(str))

        # Calculate sum of implied probabilities for each prop_id using transform for efficiency
        df['impl_prob_sum'] = df.groupby('prop_id')['impl_prob'].transform('sum')
        df['scaling_factor'] = 1 / df['impl_prob_sum']
        df['adj_impl_prob'] = df['impl_prob'] * df['scaling_factor']

        # Define columns for grouping to calculate mean price, probability, etc.
        grouping_cols = ['event_id', 'market_key', 'name', 'description', 'point', 'update_time', 'sport']
        
        grouped_stats = df.groupby(grouping_cols)
        
        # Calculate mean stats across bookmakers using transform
        df['price_mean'] = grouped_stats['price'].transform('mean')
        df['impl_prob_mean'] = grouped_stats['impl_prob'].transform('mean')
        df['adj_impl_prob_mean'] = grouped_stats['adj_impl_prob'].transform('mean')
        df['bookmaker_count_sum'] = grouped_stats['bookmaker_count'].transform('count')

        # Calculate arbitrage metrics
        df['arb_added_prob'] = df['impl_prob_mean'] - df['impl_prob']
        df['arb_added_return'] = (df['price'] - df['price_mean']) / df['price_mean']
        
        df['update_time'] = pd.to_datetime(df['update_time'])
        
        output_df = df
        
    df_dict = {
        'odds_player_props': output_df,
        'odds_events': events_df
        }
    return df_dict

# @test
# def test_output(df_dict) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert df_dict['odds_events'] is not None, 'The output is undefined'
