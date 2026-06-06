from datetime import datetime
from airflow.decorators import dag, task
import requests
import json
import pandas as pd
import concurrent.futures
from itertools import chain
import numpy as np
import time
import os
# from sklearn.metrics import r2_score
from bountygate.utils.db_connection import insert_data, fetch_data
from bountygate.utils.etl_assets import (
    odds_url,
    odds_apiKey,
    active_sports,
    get_sport_market_list,
    sport_region_map,
    ud_market_map_dict,
    sport_league_map,
)

def fetch_event_odds(sport, event_id, now_str):
    market_list = get_sport_market_list(sport)
    regions = sport_region_map.get(sport, "")
    params = {
        "apiKey": odds_apiKey,
        "regions": regions,
    }
    if market_list:
        params["markets"] = ",".join(market_list)
    props_endpoint = f"{odds_url}/v4/sports/{sport}/events/{event_id}/odds"
    
    try:
        # Adding a timeout to prevent hanging requests
        print(f"Fetching props for {sport} event {event_id} from {props_endpoint}")
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


def calc_avg_spread_pct_change(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the percentage change in 'avg_spread' by 'id' between
    the earliest (min) and latest (max) timestamp.

    The returned DataFrame has columns:
    - 'id'
    - 'first_spread':  the 'avg_spread' at the earliest timestamp
    - 'last_spread':   the 'avg_spread' at the latest timestamp
    - 'pct_change':    percentage change from first_spread to last_spread

    pct_change = ((last_spread - first_spread) / first_spread) * 100
    """
    # Ensure 'update_time' is a datetime type
    df['update_time'] = pd.to_datetime(df['update_time'])

    # Get the index of the first and last entry for each 'id'
    idx_first = df.groupby('id')['update_time'].idxmin()
    idx_last = df.groupby('id')['update_time'].idxmax()

    # Get the 'score' for the first and last entries
    first_spreads = df.loc[idx_first, ['id', 'score']].set_index('id')['score']
    last_spreads = df.loc[idx_last, ['id', 'score']].set_index('id')['score']

    # Create a DataFrame with the results
    spread_by_id = pd.DataFrame({
        'first_spread': first_spreads,
        'last_spread': last_spreads
    })

    # Compute percentage change and delta
    spread_by_id['pct_change'] = (
        (spread_by_id['last_spread'] - spread_by_id['first_spread']) / spread_by_id['first_spread']
    ) * 100
    spread_by_id['delta'] = spread_by_id['last_spread'] - spread_by_id['first_spread']

    spread_by_id.reset_index(inplace=True)

    return spread_by_id


@dag(
    default_args={'owner': 'airflow'},
    schedule='*/15 * * * *',
    start_date= datetime(2025, 7, 16, 17, 0, 0),
    max_active_runs=1,
    catchup=False,
    tags=['Underdog', 'Odds']
)
def update_underdog_outlier_analysis():
    @task()
    def load_ud_ou_lines() -> dict:
        # Fetch data from the API
        overunderlines_url = 'https://api.underdogfantasy.com/beta/v6/over_under_lines'
        r = requests.get(overunderlines_url)
        return json.loads(r.text)

    @task()
    def extract_ud_data(json_data: dict) -> pd.DataFrame:
        try:
            games_data = json_data.get('games', [])
            games_df = pd.json_normalize(games_data)
            games_df = games_df[['id', 'scheduled_at', 'sport_id', 'abbreviated_title', 'full_team_names_title']]
        except Exception:
            games_df = pd.DataFrame(columns=['id', 'scheduled_at', 'sport_id', 'abbreviated_title', 'full_team_names_title'])
        print(games_df.head())

        # Appearances table
        appearances_data = json_data['appearances']
        appearances_df = pd.DataFrame(appearances_data)
        appearances_df = appearances_df.drop(columns=['badges'])

        # OverUnderLines table
        over_under_lines_data = json_data['over_under_lines']
        over_under_lines_df = pd.json_normalize(over_under_lines_data)
        options_data = over_under_lines_df['options']

        # Normalize the nested 'options' column to create a separate DataFrame
        options_df = pd.json_normalize(options_data.explode())
        over_under_lines_df = over_under_lines_df.drop(columns=['options','over_under.id'])

        oul_rename_mapping = {
            'over_under.appearance_stat.id': 'appearance_stat_id',
            'over_under.appearance_stat.appearance_id': 'appearance_id',
            'over_under.appearance_stat.display_stat': 'display_stat',
            'over_under.appearance_stat.graded_by': 'graded_by',
            'over_under.appearance_stat.pickem_stat_id': 'pickem_stat_id',
            'over_under.appearance_stat.stat': 'stat',
            'over_under.boost': 'boost',
            'over_under.has_alternates': 'has_alternates',
            'over_under.option_priority': 'option_priority',
            'over_under.scoring_type_id': 'scoring_type_id',
            'over_under.title': 'title'
        }

        over_under_lines_df = over_under_lines_df.rename(columns=oul_rename_mapping)

        # Players table
        players_data = json_data['players']
        players_df = pd.DataFrame(players_data)

        df_dict = {
            'ud_games': games_df,
            'ud_appearances': appearances_df,
            'ud_over_under_lines': over_under_lines_df,
            'ud_options': options_df,
            'ud_players': players_df
        }

        return df_dict

    @task()
    def export_data_to_postgres(df_dict: dict) -> None:
        
        for key, df in df_dict.items():
            # start timer
            start_time = time.time()
            if key in ['market_lines_1', 'betting_markets']:
                # Use custom upsert function for market_lines
                # upsert_market_lines(df)
                    insert_data(df, key, if_exists='append')
            else:
                insert_data(df, key, if_exists='replace')
            # log duration
            duration = time.time() - start_time
            print(f"Exported {key} to Postgres in {duration:.2f} seconds")

    @task()
    def get_odds_events() -> pd.DataFrame:
        output_df = pd.DataFrame()
        for sport in active_sports:
            events_endpoint = f"{odds_url}/v4/sports/{sport}/events?apiKey={odds_apiKey}"
            events = pd.read_json(events_endpoint)
            output_df = pd.concat([output_df, events], ignore_index=True)
        return output_df
    
    @task()
    def get_odds_player_props(events_df: pd.DataFrame) -> pd.DataFrame:

        now_str = datetime.now()
        
        tasks = []
        for sport in active_sports:
            sport_events_df = events_df[events_df['sport_key'] == sport]
            if not sport_events_df.empty:
                for event_id in sport_events_df['id'].unique():
                    tasks.append((sport, event_id, now_str))

        all_records = []

        for task in tasks:
            sport, event_id, now_str = task
            event_odds = fetch_event_odds(sport, event_id, now_str)
            all_records.extend(event_odds)

        # Convert all records to a single DataFrame at the end
        df = pd.DataFrame(all_records)

        if df.empty:
            output_df = pd.DataFrame()
            market_lines_df = pd.DataFrame()
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
            
            
            # # Prepare data for market_lines table upsert
            # market_lines_df = df[['event_id', 'market_key', 'description', 'point', 'name', 'bookmaker_key', 'price', 'update_time']].copy()
            
            # market_lines_df = market_lines_df.rename(columns={
            #     'event_id': 'betting_event_id',
            #     'market_key': 'betting_market_id',
            #     'description': 'outcome_name',
            #     'point': 'line_value',
            #     'name': 'sportsbook_id',
            #     'bookmaker_key': 'bookmaker_id',
            #     'price': 'price_decimal',
            #     'update_time': 'fetched_at_utc'
            # })

            # # Convert decimal odds to American odds
            # market_lines_df['price_american'] = market_lines_df['price_decimal'].apply(
            #     lambda x: int((x - 1) * 100) if x >= 2.0 else int(-100 / (x - 1)) if x > 1.0 else None
            # )
            
            # # Select only the columns that match the market_lines table structure
            # market_lines_df = market_lines_df[['fetched_at_utc', 'betting_market_id', 'sportsbook_id', 
            #                                  'line_value', 'price_decimal', 'price_american', 'outcome_name']]
            
            # market_lines_df['sportsbook_id'] = market_lines_df['sportsbook_id'].astype(str)
            
            
            # print(f"Prepared {len(market_lines_df)} market lines records for upsert")
        

        df_dict = {
            'odds_player_props': output_df,
            'odds_events': events_df,
            # 'market_lines_1': market_lines_df,
            }
        return df_dict
    
    @task()
    def fetch_prepared_sql(file_path: str) -> pd.DataFrame:
        # Get the directory where this DAG file is located
        dag_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the full path to the SQL file
        full_path = os.path.join(dag_dir, file_path)
        
        with open(full_path, 'r') as file:
            sql_query = file.read()
        return fetch_data(sql_query)
    
    @task()
    def create_ud_analysis(ud_df: pd.DataFrame, props_df: pd.DataFrame, hd_ud_df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        sport_joined_market_map = {
            ud_stat: market
            for sport, market_map in ud_market_map_dict.items()
            if sport in {'NFL', 'NHL', 'MLB', 'WNBA', 'NBA', 'CFB', 'FIFA', 'CBB'}
            for ud_stat, market in market_map.items()
        }
        
        ud_df['decimal_price'] = ud_df['decimal_price'].astype(float)
        ud_df['ud_impl_prob'] = round((1 / ud_df['decimal_price'] * 100), 2)
        ud_df['payout_multiplier'] = ud_df['payout_multiplier'].astype(float)

        ud_df = ud_df[ud_df['payout_multiplier'] < 1.5].copy()

        # Load data
        x = ud_df['ud_impl_prob'].values
        y = ud_df['payout_multiplier'].values

        # Fit log-linear model: ln(y) = ln(a) + b*x
        b, loga = np.polyfit(x, np.log(y), 1)
        a = np.exp(loga)

        # Predictions
        y_pred = a * np.exp(b * x)
        # r2_exp = r2_score(y, y_pred)
        r2_exp = 1.2345678

        print(f'Exp fit: M = {a:.4f}·e^({b:.4f}·impl_prob), R²={r2_exp:.4f}')

        bg_reference_dict = {'bg_reference_id': 'ud_regression', 
                            'bg_reference_value': f'Exp fit: M = {a:.4f}·e^({b:.4f}·impl_prob), R²={r2_exp:.4f}'}
        
        bg_reference_df = pd.DataFrame([bg_reference_dict])
        bg_reference_df['update_time'] = pd.to_datetime('now')

        ud_df['ud_predicted'] = a * np.exp(b * ud_df['ud_impl_prob'])
        ud_df['ud_predicted'] = ud_df['ud_predicted'].round(2)
        ud_df['predict_delta'] =  ud_df['payout_multiplier'] - ud_df['ud_predicted']

        df = ud_df[['id', 'sport_id', 'first_name', 'last_name', 'choice', 'stat_value', 'display_stat', 'payout_multiplier', 
                    'stat', 'abbreviated_title', 'decimal_price', 'ud_impl_prob', 'ud_predicted', 'predict_delta']].copy()
        df = df[df['sport_id'].isin({'NFL', 'NHL', 'MLB', 'WNBA', 'NBA', 'CFB', 'FIFA', 'CBB'})]
        df['market_key'] = df['stat'].map(sport_joined_market_map)

        props_df['choice'] = props_df['name'].map({'Over': 'higher', 'Under': 'lower'})
        props_df['stat_value'] = props_df['point']
        
        # More efficient way to split string column
        name_split = props_df['description'].str.split(' ', n=1, expand=True)
        props_df['first_name'] = name_split[0]
        props_df['last_name'] = name_split[1]
        
        props_df['sport_id'] = props_df['sport'].map(sport_league_map)

        odds_df = props_df[['sport_id', 'market_key', 'choice', 'stat_value', 'first_name', 'last_name', 
                            'bookmaker_key', 'price', 'price_mean', 'impl_prob', 'impl_prob_mean', 'bookmaker_count_sum', 'event_id', 'update_time']].copy()
        
        # Ensure merge keys are the same type
        df['stat_value'] = df['stat_value'].astype(str)
        odds_df['stat_value'] = odds_df['stat_value'].astype(str)

        output_df = pd.merge(odds_df, df, on=['sport_id', 'market_key', 'choice', 'stat_value', 'first_name', 'last_name'], how='left')

        output_df.dropna(subset=['payout_multiplier'], inplace=True)

        output_df['impl_prob_mean'] = output_df['impl_prob_mean'] * 100
        output_df['bm_predicted_mult'] = a * np.exp(b * output_df['impl_prob_mean'])
        output_df['mult_delta'] =  output_df['payout_multiplier'] - output_df['bm_predicted_mult']
        output_df['score'] = (output_df['mult_delta'] / output_df['payout_multiplier'] ** 2) * 100
        output_df['score'] = output_df['score'].round(2)

        output_df['bm_spread'] = (1 + output_df['payout_multiplier']) - output_df['price'] 
        output_df['avg_spread'] = (1 + output_df['payout_multiplier']) - output_df['price_mean']

        output_df = output_df[output_df['payout_multiplier'] < 5]

        output_df.sort_values(by='score', ascending=False, inplace=True)

        output_df = output_df[['id', 'sport_id', 'first_name', 'last_name', 'bookmaker_key', 'choice', 'stat_value', 
                            'display_stat', 'price', 'price_mean', 'impl_prob', 'impl_prob_mean', 'bookmaker_count_sum', 
                                'ud_impl_prob', 'ud_predicted', 'predict_delta', 'bm_predicted_mult', 'mult_delta', 'score',
                            'payout_multiplier', 'bm_spread', 'avg_spread', 'abbreviated_title' , 'decimal_price', 'event_id', 'update_time']]
        
        hd_output_df = output_df[[
            'id', 'sport_id', 'first_name', 'last_name', 'choice', 'stat_value', 
            'display_stat', 'price_mean', 'impl_prob_mean', 'bookmaker_count_sum', 
            'ud_impl_prob', 'ud_predicted', 'predict_delta', 'bm_predicted_mult', 'mult_delta', 'score',
            'payout_multiplier', 'abbreviated_title' , 'decimal_price', 'avg_spread', 'update_time'
        ]].drop_duplicates()

        hd_ud_df = pd.concat([hd_ud_df, hd_output_df], ignore_index=True)

        trend_df = calc_avg_spread_pct_change(hd_ud_df)

        output_df = pd.merge(output_df, trend_df, on='id', how='left')

        df_dict = {
            'ud_analysis': output_df,
            'ud_ou_lines_details': ud_df,
            'hd_ud_analysis': hd_ud_df,
            'hd_ud_trend': trend_df,
            'bg_reference': bg_reference_df
            }

        return df_dict

    # Define tasks
    ud_lines = load_ud_ou_lines()
    extracted_data = extract_ud_data(ud_lines)
    export_extracted_data = export_data_to_postgres(extracted_data)
    
    events_df = get_odds_events()
    odds_player_props_data = get_odds_player_props(events_df)
    export_odds_data = export_data_to_postgres(odds_player_props_data)
    
    # Define SQL tasks that depend on the export tasks
    ud_analysis_df = fetch_prepared_sql('prepared_sql/ud_analysis.sql')
    odds_player_props_df = fetch_prepared_sql('prepared_sql/odds_player_props.sql')
    hd_ud_analysis_df = fetch_prepared_sql('prepared_sql/hd_ud_analysis.sql')

    # Final analysis task
    new_ud_analysis_data = create_ud_analysis(ud_analysis_df, odds_player_props_df, hd_ud_analysis_df)
    export_final_data = export_data_to_postgres(new_ud_analysis_data)
    
    # Set up dependencies - SQL tasks wait for both export tasks to complete
    [export_extracted_data, export_odds_data] >> ud_analysis_df
    [export_extracted_data, export_odds_data] >> odds_player_props_df
    [export_extracted_data, export_odds_data] >> hd_ud_analysis_df
    
    # Final tasks depend on SQL tasks
    [ud_analysis_df, odds_player_props_df, hd_ud_analysis_df] >> new_ud_analysis_data >> export_final_data


# create DAG instance
dag = update_underdog_outlier_analysis()
