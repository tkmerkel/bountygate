import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
from app.shared.python.bountygate.utils.etl_assets import ud_market_map_dict, sport_league_map
from app.shared.python.bountygate.utils.mage import transformer, test

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

@transformer
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
    r2_exp = r2_score(y, y_pred)

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

    props_df['choice'] = props_df['_name'].map({'Over': 'higher', 'Under': 'lower'})
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


# @test
# def test_output(df_dict) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert df_dict['ud_analysis'] is not None, 'The output is undefined'