import pandas as pd
from app.shared.python.bountygate.utils.mage import transformer, test


@transformer
def enrich_player_prop(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        df (DataFrame): Data frame from parent block.

    Returns:
        DataFrame: Transformed data frame
    """
    # Specify your transformation logic here

    output_df = pd.DataFrame()

    df['impl_prob'] = 1 / df['price']
    df['bookmaker_count'] = 1

    for sport in df['sport'].unique():
        props_df = df[df['sport'] == sport]
        props_df['prop_id'] = props_df['event_id'].astype(str) + '_' + props_df['market_key'] + '_' + props_df['bookmaker_key'] + '_' + props_df['description'] + '_' + props_df['point'].astype(str)
        imp_prob_margin_df = props_df.groupby('prop_id').agg({'impl_prob': 'sum'}).reset_index()
        props_df = pd.merge(props_df, imp_prob_margin_df, on='prop_id', suffixes=('', '_sum'))
        props_df['scaling_factor'] = 1 / props_df['impl_prob_sum']
        props_df['adj_impl_prob'] = props_df['impl_prob'] * props_df['scaling_factor']
        grouped = props_df.groupby(['event_id', 'market_key', 'name', 'description', 'point', 'update_time']).agg({'price': 'mean', 
                                                                                                                   'impl_prob': 'mean', 
                                                                                                                   'adj_impl_prob': 'mean',
                                                                                                                   'bookmaker_count': 'count'}).reset_index()
        props_df = pd.merge(props_df, grouped, on=['event_id', 'market_key', 'name', 'description', 'point', 'update_time'], suffixes=('', '_mean'))
        props_df['arb_added_prob'] = props_df['impl_prob_mean'] - props_df['impl_prob']
        props_df['arb_added_return'] = (props_df['price'] - props_df['price_mean']) / props_df['price_mean']
        props_df['sport'] = sport
        props_df['update_time'] = pd.to_datetime(props_df['update_time'])
        output_df = pd.concat([output_df, props_df], ignore_index=True)

    output_df.rename(columns={'bookmaker_count_mean': 'bookmaker_count_sum'}, inplace=True)
        
    df_dict = {
        'odds_player_props': output_df
        }

    return df_dict


# @test
# def test_output(df_dict) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert df_dict['odds_player_props'] is not None, 'The output is undefined'