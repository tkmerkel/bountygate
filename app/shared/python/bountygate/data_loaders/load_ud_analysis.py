import pandas as pd
from app.shared.python.bountygate.utils.etl_assets import odds_url, odds_apiKey, active_sports
from app.shared.python.bountygate.utils.db_connection import fetch_data
from app.shared.python.bountygate.utils.mage import data_loader, test

@data_loader
def load_ud_analysis(**kwargs) -> pd.DataFrame:

    ud_query = """select  opt.id,
        opt.over_under_line_id,
        oul.appearance_id,
        _type, 
        choice, 
        payout_multiplier,
        opt.decimal_price,
        oul.title,
        oul.display_stat,
        oul.abbreviated_title,
        oul.stat,
        oul.stat_value,
        oul.player_id,
        oul.lineup_status_id,
        oul.match_id,
        oul.sport_id,
        oul.first_name,
        oul.last_name,
        oul.position_id,
        oul.team_id,
        oul.image_url
        from ud_options as opt
    left join (
        select  overunder.id as oul_id,
                overunder.appearance_id,
                overunder.over_under_id,
                overunder._status,
                overunder.stat_value,
                overunder.stat,
                overunder.display_stat,
                overunder.title,
                app.id,
                app.abbreviated_title,
                app.player_id,
                app.lineup_status_id,
                app.match_id,
                app.sport_id,
                app.first_name,
                app.last_name,
                app.position_id,
                app.team_id,
                app.image_url
                from ud_over_under_lines as overunder
        left join (
            select  appearances.id,
                    games.abbreviated_title,
                    appearances.player_id,
                    appearances.lineup_status_id,
                    appearances.match_id,
                    players.sport_id,
                    players.first_name,
                    players.last_name,
                    players.position_id,
                    players.team_id,
                    players.image_url
                    from ud_appearances as appearances
            left join (
                select  id,
                        sport_id,
                        first_name,
                        last_name,
                        position_id,
                        team_id,
                        image_url
                        from ud_players
            ) as players
            on appearances.player_id = players.id
            left join (
                select  id,
                        abbreviated_title
                        from ud_games
            ) as games
            on appearances.match_id = games.id
        ) as app
        on overunder.appearance_id = app.id
    ) as oul
    on opt.over_under_line_id = oul.oul_id
    ;
    """

    output_df = fetch_data(ud_query)
    return output_df


# @test
# def test_output(output_df) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output_df is not None, 'The output is undefined'