import pandas as pd
from app.shared.python.bountygate.utils.mage import transformer, test


@transformer
def extract_ud_data(json_data: dict, *args, **kwargs) -> pd.DataFrame:
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

    # Games table
    games_data = json_data['games']

    # Convert games data to a DataFrame
    games_df = pd.DataFrame(games_data)

    # Appearances table
    appearances_data = json_data['appearances']

    # Convert appearances data to a DataFrame
    appearances_df = pd.DataFrame(appearances_data)

    # drop the 'badges' column
    appearances_df = appearances_df.drop(columns=['badges'])

    # OverUnderLines table
    over_under_lines_data = json_data['over_under_lines']

    # Convert over_under_lines data to a DataFrame
    over_under_lines_df = pd.json_normalize(over_under_lines_data)

    # Options Table
    options_data = over_under_lines_df['options']

    # Normalize the nested 'options' column to create a separate DataFrame
    options_df = pd.json_normalize(options_data.explode())

    # drop the 'options' column
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

    # Convert players data to a DataFrame
    players_df = pd.DataFrame(players_data)

    df_dict = {
        'ud_games': games_df,
        'ud_appearances': appearances_df,
        'ud_over_under_lines': over_under_lines_df,
        'ud_options': options_df,
        'ud_players': players_df
    }

    return df_dict


# @test
# def test_output(df_dict) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert df_dict['ud_games'] is not None, 'The output is undefined'
#     assert df_dict['ud_appearances'] is not None, 'The output is undefined'
#     assert df_dict['ud_over_under_lines'] is not None, 'The output is undefined'
#     assert df_dict['ud_options'] is not None, 'The output is undefined'
#     assert df_dict['ud_players'] is not None, 'The output is undefined'