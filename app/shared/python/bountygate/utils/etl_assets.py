import os
from collections import defaultdict
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import create_engine, text

from bountygate.utils import db_connection as dbc

odds_url = 'https://api.the-odds-api.com'
odds_apiKey = '9bc17b9e4d48606c2d72a95b7a7ac77a'

# https://api.the-odds-api.com/v4/sports/?apiKey=9bc17b9e4d48606c2d72a95b7a7ac77a

active_sports = [
    'americanfootball_nfl',
    'baseball_mlb',
    'icehockey_nhl',
    'basketball_nba',
    'basketball_ncaab',
    # 'basketball_wnba',
    'americanfootball_ncaaf'
    ]

def dict_values_to_string(data_dict):
    return ','.join(data_dict.values())


def _normalize_token(value: Optional[str]) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip().lower()
    text = str(value).strip()
    if text.lower() in {"", "nan", "none"}:
        return ""
    return text.lower()


def _database_url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


@lru_cache(maxsize=1)
def _load_market_alias_cache() -> Tuple[Dict[Tuple[str, str, str], str], Dict[str, Tuple[str, ...]]]:
    url = _database_url()
    if not url:
        return {}, {}
    try:
        engine = create_engine(url)
    except Exception:
        return {}, {}

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT bookmaker_key, sport_key, bm_market_key, canonical_market_key "
                    "FROM market_aliases"
                )
            ).fetchall()
    except Exception:
        return {}, {}
    finally:
        engine.dispose()

    alias_lookup: Dict[Tuple[str, str, str], str] = {}
    sport_map_accumulator: Dict[str, Set[str]] = defaultdict(set)

    for row in rows:
        bookmaker_key, sport_key, bm_market_key, canonical_market_key = row[0], row[1], row[2], row[3]
        canonical_clean = (canonical_market_key or "").strip()
        if not canonical_clean:
            continue
        key = (
            _normalize_token(bookmaker_key),
            _normalize_token(sport_key),
            _normalize_token(bm_market_key),
        )
        alias_lookup[key] = canonical_clean
        sport_map_accumulator[_normalize_token(sport_key)].add(canonical_clean)

    sport_map: Dict[str, Tuple[str, ...]] = {
        sport: tuple(sorted(values))
        for sport, values in sport_map_accumulator.items()
        if values
    }
    return alias_lookup, sport_map


def refresh_market_alias_cache() -> None:
    _load_market_alias_cache.cache_clear()

ud_market_map_dict = {
    "NFL": {
        "passing_yds": "player_pass_yds",
        "passing_tds": "player_pass_tds",
        "rush_rec_tds": "player_rush_reception_tds",
        "passing_and_rushing_yds": "player_pass_rush_reception_yds",
        "passing_att": "player_pass_attempts",
        "passing_comps": "player_pass_completions",
        "rushing_yds": "player_rush_yds",
        "rushing_att": "player_rush_attempts",
        "passing_ints": "player_pass_interceptions",
        "passing_long": "player_pass_longest_completion",
        "rushing_long": "player_rush_longest",
        # "period_first_touchdown_scored": None,
        # "fantasy_points": None,
        # "passing_first_downs": None,
        # "passing_comp_pct": None,
        # "fumbles_lost": None,
        "receiving_yds": "player_reception_yds",
        "receiving_rec": "player_receptions",
        # "receiving_tgts": None,
        "receiving_long": "player_reception_longest",
        # "first_downs": None,
        "rush_rec_yds": "player_rush_reception_yds",
        # "rushing_tds": None,
        # "period_1_2_rushing_yds": None,
        "field_goals_made": "player_field_goals",
        "extra_points_made": "player_pats",
        "kicking_points": "player_kicking_points",
        "tackles": "player_solo_tackles",
        "sacks": "player_sacks",
        "assists": "player_assists",
        "tackles_and_assists": "player_tackles_assists",
        # "defensive_ints": None,
        # "period_1_2_receiving_yds": None,
        # "period_1_2_passing_yds": None,
    },
    "CFB": {
        "passing_yds": "player_pass_yds",
        "passing_tds": "player_pass_tds",
        "rush_rec_tds": "player_rush_reception_tds",
        "passing_and_rushing_yds": "player_pass_rush_reception_yds",
        "passing_att": "player_pass_attempts",
        "passing_comps": "player_pass_completions",
        "rushing_yds": "player_rush_yds",
        "rushing_att": "player_rush_attempts",
        "passing_ints": "player_pass_interceptions",
        "passing_long": "player_pass_longest_completion",
        "rushing_long": "player_rush_longest",
        # "period_first_touchdown_scored": None,
        # "fantasy_points": None,
        # "passing_first_downs": None,
        # "passing_comp_pct": None,
        # "fumbles_lost": None,
        "receiving_yds": "player_reception_yds",
        "receiving_rec": "player_receptions",
        # "receiving_tgts": None,
        "receiving_long": "player_reception_longest",
        # "first_downs": None,
        "rush_rec_yds": "player_rush_reception_yds",
        # "rushing_tds": None,
        # "period_1_2_rushing_yds": None,
        "field_goals_made": "player_field_goals",
        "extra_points_made": "player_pats",
        "kicking_points": "player_kicking_points",
        "tackles": "player_solo_tackles",
        "sacks": "player_sacks",
        "assists": "player_assists",
        "tackles_and_assists": "player_tackles_assists",
        # "defensive_ints": None,
        # "period_1_2_receiving_yds": None,
        # "period_1_2_passing_yds": None,
    },
    "NHL": {
        "goals": "player_goals",
        "points": "player_points",
        "shots": "player_shots_on_goal",
        "assists": "player_assists",
        "power_play_points": "player_power_play_points",
        # "period_1_shots": None,
        # "goals_against": None,
        "saves": "player_total_saves",
        "blocked_shots": "player_blocked_shots"
    },
    "MLB": {
        "total_bases": "batter_total_bases",
        "runs": "batter_runs_scored",
        "rbis": "batter_rbis",
        "hits_runs_rbis": "batter_hits_runs_rbis",
        "home_runs": "batter_home_runs",
        "stolen_bases": "batter_stolen_bases",
        "hits": "batter_hits",
        "singles": "batter_singles",
        "batter_strikeouts": "batter_strikeouts",
        "doubles": "batter_doubles",
        "hits_allowed": "pitcher_hits_allowed",
        "pitch_outs": "pitcher_outs",
        "runs_allowed": "pitcher_earned_runs",
        "strikeouts": "pitcher_strikeouts",
        "walks": "batter_walks",
        "walks_allowed": "pitcher_walks"
    },
    "NBA": {
        "points": "player_points",
        "pts_rebs_asts": "player_points_rebounds_assists",
        "rebounds": "player_rebounds",
        "three_points_made": "player_threes",
        "pts_asts": "player_points_assists",
        "rebs_asts": "player_rebounds_assists",
        "pts_rebs": "player_points_rebounds",
        "assists": "player_assists"
    },
    "WNBA": {
        "points": "player_points",
        "pts_rebs_asts": "player_points_rebounds_assists",
        "rebounds": "player_rebounds",
        "three_points_made": "player_threes",
        "pts_asts": "player_points_assists",
        "rebs_asts": "player_rebounds_assists",
        "pts_rebs": "player_points_rebounds",
        "assists": "player_assists"
    }
}

sleeper_market_map_dict = {
    # fantasy
    'fantasy_points': None,

    # MLB batting
    'hits': 'batter_hits',
    'hits_runs_rbis': 'batter_hits_runs_rbis',
    'rbis': 'batter_rbis',
    'runs': 'batter_runs_scored',
    'stolen_bases': 'batter_stolen_bases',
    'total_bases': 'batter_total_bases',
    'home_runs': 'batter_home_runs',
    'singles': 'batter_singles',
    'bat_strike_outs': 'batter_strikeouts',
    'bat_walks': 'batter_walks',
    'walks': 'batter_walks',

    # MLB pitching
    'earned_runs': 'pitcher_earned_runs',
    'hits_allowed': 'pitcher_hits_allowed',
    'outs': 'pitcher_outs',
    'strike_outs': 'pitcher_strikeouts',
    'first_inning_runs': None,

    # NFL
    'passing_touchdowns': 'player_pass_tds',
    'passing_yards': 'player_pass_yds',
    'receiving_yards': 'player_reception_yds',
    'rushing_yards': 'player_rush_yds',
    'anytime_touchdowns': 'player_anytime_td',
    'receiving_touchdowns': None,
    'rushing_touchdowns': None,
    'passing_and_rushing_yards': 'player_pass_rush_reception_yds',
    'rushing_and_receiving_yards': 'player_rush_reception_yds',
    'receptions': 'player_receptions',
    'passing_attempts': 'player_pass_attempts',
    'rushing_attempts': 'player_rush_attempts',
    'tackles': 'player_solo_tackles',
    'tackles_and_assists': 'player_tackles_assists',
    'interceptions': 'player_pass_interceptions',
    'sacks': 'player_sacks',

    # NHL
    'goals': 'player_goals',
    'shots': 'player_shots_on_goal',
    'assists': 'player_assists',
    'shots_on_target': None,
    'goals_against': None,

    # other / e-sports
    'headshots_maps_1_2': None,
    'kills_maps_1_2': None,
    'games_won': None,
    'games_played': None,
    'aces': None,
    'breakpts_won': None,
}

splash_market_map_dict = {
    "longest_reception": "player_reception_longest",
    "receiving_yards": "player_reception_yds",
    "rushing_yards": "player_rush_yds",
    "longest_rush": "player_rush_longest",
    "tackles_plus_defensive_assists": "player_tackles_assists",
    "rushing_attempts": "player_rush_attempts",
    "rushing_plus_receiving_yards": "player_rush_reception_yds",
    "tackles": "player_solo_tackles",
    "receiving_receptions": "player_receptions",
    "total_kicking_points": "player_kicking_points",
    "field_goals_made": "player_field_goals",
    "passing_plus_rushing_yards": "player_pass_rush_reception_yds",
    "passing_yards": "player_pass_yds",
    "completions": "player_pass_completions",
    "passing_attempts": "player_pass_attempts",
    "interceptions": "player_pass_interceptions",
    "passing_touchdowns": "player_pass_tds",
}

pp_market_map_dict = {
    "NFL": {
        "Pass Yards": "player_pass_yds",
        "Pass TDs": "player_pass_tds",
        "Rush+Rec TDs": "player_rush_reception_tds",
        "Rush+Rec Yds": "player_rush_reception_yds",
        "Pass+Rush Yds": "player_pass_rush_reception_yds",
        "Pass Attempts": "player_pass_attempts",
        "Pass Completions": "player_pass_completions",
        "Rush Yards": "player_rush_yds",
        "Rush Attempts": "player_rush_attempts",
        "INT": "player_pass_interceptions",
        "Longest Rush": "player_rush_longest",
        "Receiving Yards": "player_reception_yds",
        "Receptions": "player_receptions",
        "Longest Reception": "player_reception_longest",
        "FG Made": "player_field_goals",
        "Kicking Points": "player_kicking_points",
    },
     "NBA": {
        "Points": "player_points",
        "Pts+Rebs+Asts": "player_points_rebounds_assists",
        "Rebounds": "player_rebounds",
        "Blks+Stls": "player_blocks_steals",
        "3-PT Made": "player_threes",
        "Pts+Rebs": "player_points_rebounds",
        "Pts+Asts": "player_points_assists",
        "Rebs+Asts": "player_rebounds_assists",
     }
}

# Map of sports to their respective market keys for calls to the Odds API
sport_market_map = {
    'basketball_nba': dict_values_to_string(ud_market_map_dict['NBA']),
    'basketball_wnba': dict_values_to_string(ud_market_map_dict['WNBA']),
    'baseball_mlb': dict_values_to_string(ud_market_map_dict['MLB']),
    'americanfootball_nfl': dict_values_to_string(ud_market_map_dict['NFL']),
    'americanfootball_ncaaf': dict_values_to_string(ud_market_map_dict['CFB']),
    'icehockey_nhl': dict_values_to_string(ud_market_map_dict['NHL'])
}

sport_region_map = {
    'basketball_nba': 'us',
    'basketball_ncaab': 'us',
    'basketball_wnba': 'us',
    'baseball_mlb': 'us',
    'americanfootball_nfl': 'us',
    'americanfootball_nfl_preseason': 'us',
    'golf_masters_tournament_winner': 'us',
    'icehockey_nhl': 'us',
    'americanfootball_ncaaf': 'us',
}

sport_league_map = {
    'basketball_nba': 'NBA',
    'basketball_ncaab': 'NCAAB',
    'basketball_wnba': 'WNBA',
    'baseball_mlb': 'MLB',
    'golf_masters_tournament_winner': 'PGA+TOUR',
    'americanfootball_nfl': 'NFL',
    'americanfootball_nfl_preseason': 'NFL',
    'icehockey_nhl': 'NHL',
    'americanfootball_ncaaf': 'CFB',
}

league_sport_map_inverted = {
    'NBA': 'basketball_nba',
    'NCAAB': 'basketball_ncaab',
    'WNBA': 'basketball_wnba',
    'MLB': 'baseball_mlb',
    'PGA+TOUR': 'golf_masters_tournament_winner',
    'NFL': 'americanfootball_nfl',
    'NHL': 'icehockey_nhl',
    'CFB': 'americanfootball_ncaaf',
}


@lru_cache(maxsize=1)
def _default_sport_market_map() -> Dict[str, Tuple[str, ...]]:
    fallback: Dict[str, Tuple[str, ...]] = {}
    for league, market_map in ud_market_map_dict.items():
        sport_key = league_sport_map_inverted.get(league)
        if not sport_key:
            continue
        canonical_values = sorted(set(market_map.values()))
        if canonical_values:
            fallback[_normalize_token(sport_key)] = tuple(canonical_values)
    return fallback


def resolve_market_key(
    bookmaker_key: Optional[str],
    sport_key: Optional[str],
    bm_market_key: Optional[str],
    *,
    fallback: Optional[str] = None,
) -> Optional[str]:
    if not bookmaker_key or not bm_market_key:
        return fallback
    alias_lookup, _ = _load_market_alias_cache()
    lookup_key = (
        _normalize_token(bookmaker_key),
        _normalize_token(sport_key),
        _normalize_token(bm_market_key),
    )
    canonical = alias_lookup.get(lookup_key)
    if canonical:
        return canonical
    return fallback


def get_sport_market_list(sport_key: Optional[str]) -> List[str]:
    if not sport_key:
        return []
    _, sport_map = _load_market_alias_cache()
    normalized = _normalize_token(sport_key)
    # fallback_map = _default_sport_market_map()

    # defaults = fallback_map.get(normalized)
    # if defaults:
    #     return list(defaults)

    aliases = sport_map.get(normalized)
    if aliases:
        return sorted(set(aliases))

    return []


def get_sport_market_string(sport_key: Optional[str]) -> str:
    markets = get_sport_market_list(sport_key)
    return ",".join(markets)


dk_market_map = {
'baseball_mlb': {
    'Total_Bases': 'batter_total_bases',
    'Hits': 'batter_hits',
    'Runs': 'batter_runs_scored',
    'RBIs': 'batter_rbis',
    'Hits + Runs + RBIs': 'batter_hits_runs_rbis',
    'Strikeouts_Thrown': 'pitcher_strikeouts',
    'Outs': 'pitcher_outs',
    'Earned_Runs_Allowed': 'pitcher_earned_runs',
    'Singles': 'batter_singles',
    'Strikeouts': 'batter_strikeouts',
    'Walks': 'batter_walks',
    'Hits_Against': 'pitcher_hits_allowed',
    'Walks_Allowed': 'pitcher_walks',
    'Total Bases (From Hits)': 'batter_total_bases',
    'Strikeouts Thrown': 'pitcher_strikeouts', 
    'Earned Runs Allowed': 'pitcher_earned_runs',
    'Hits Against': 'pitcher_hits_allowed',
    'Runs Batted In': 'batter_rbi',
    'Walks Allowed': 'pitcher_walks',
    },
'basketball_nba': {
    'Points': 'player_points',
    'Assists': 'player_assists',
    'Rebounds': 'player_rebounds',
    '3-Pointers Made': 'player_threes',
    'Points + Assists + Rebounds': 'player_points_rebounds_assists',
    'Points + Rebounds': 'player_points_rebounds',
    'Points + Assists': 'player_points_assists',
    'Rebounds + Assists': 'player_rebounds_assists',
    'Blocks + Steals': 'player_blocks_steals',
    'Blocks': 'player_blocks',
    'Steals': 'player_steals',
    },
'basketball_wnba': {
    'Points': 'player_points',
    'Assists': 'player_assists',
    'Rebounds': 'player_rebounds',
    '3-Pointers Made': 'player_threes',
    'Points + Assists + Rebounds': 'player_points_rebounds_assists',
    'Points + Rebounds': 'player_points_rebounds',
    'Points + Assists': 'player_points_assists',
    'Rebounds + Assists': 'player_rebounds_assists',
    'Blocks + Steals': 'player_blocks_steals',
    'Blocks': 'player_blocks',
    'Steals': 'player_steals',
    },
'americanfootball_nfl': {
    'Passing Yards': 'player_pass_yds',
    'Passing Touchdowns': 'player_pass_tds',
    'Passing Attempts': 'player_pass_attempts',
    'Passing Longest Completion': 'player_pass_longest_completion',
    'Interceptions Thrown': 'player_pass_interceptions',
    'Rushing Yards': 'player_rush_yds',
    'Rushing Attempts': 'player_rush_attempts',
    'Longest Rush': 'player_rush_longest',
    'Receptions': 'player_receptions',
    'Receiving Yards': 'player_reception_yds',
    'Longest Reception': 'player_reception_longest',
    'Kicking Points': 'player_kicking_points',
    'Field Goals Made': 'player_field_goals',
    'Tackles + Assists': 'player_tackles_assists',
    '1st Touchdown Scorer': 'player_1st_td',
    'Last Touchdown Scorer': 'player_last_td',
    'Anytime Touchdown Scorer': 'player_anytime_td'
    },
}

team_name_map = {
'basketball_nba': {
    'ATL': 'Atlanta Hawks',
    'BOS': 'Boston Celtics',
    'BKN': 'Brooklyn Nets',
    'CHA': 'Charlotte Hornets',
    'CHI': 'Chicago Bulls',
    'CLE': 'Cleveland Cavaliers',
    'DAL': 'Dallas Mavericks',
    'DEN': 'Denver Nuggets',
    'DET': 'Detroit Pistons',
    'GSW': 'Golden State Warriors',
    'HOU': 'Houston Rockets',
    'IND': 'Indiana Pacers',
    'LAC': 'Los Angeles Clippers',
    'LAL': 'Los Angeles Lakers',
    'MEM': 'Memphis Grizzlies',
    'MIA': 'Miami Heat',
    'MIL': 'Milwaukee Bucks',
    'MIN': 'Minnesota Timberwolves',
    'NOP': 'New Orleans Pelicans',
    'NYK': 'New York Knicks',
    'OKC': 'Oklahoma City Thunder',
    'ORL': 'Orlando Magic',
    'PHI': 'Philadelphia 76ers',
    'PHX': 'Phoenix Suns',
    'POR': 'Portland Trail Blazers',
    'SAC': 'Sacramento Kings',
    'SAS': 'San Antonio Spurs',
    'TOR': 'Toronto Raptors',
    'UTA': 'Utah Jazz',
    'WAS': 'Washington Wizards',
    },
'basketball_wnba': {
    'ATL': 'Atlanta Dream',
    'CHI': 'Chicago Sky',
    'CON': 'Connecticut Sun',
    'DAL': 'Dallas Wings',
    'IND': 'Indiana Fever',
    'LAS': 'Las Vegas Aces',
    'LAC': 'Los Angeles Sparks',
    'MIN': 'Minnesota Lynx',
    'NYL': 'New York Liberty',
    'PHO': 'Phoenix Mercury',
    'SEA': 'Seattle Storm',
    'WAS': 'Washington Mystics',
    },
'baseball_mlb': {
    'ARI': 'Arizona Diamondbacks',
    'ATL': 'Atlanta Braves',
    'BAL': 'Baltimore Orioles',
    'BOS': 'Boston Red Sox',
    'CHC': 'Chicago Cubs',
    'CIN': 'Cincinnati Reds',
    'CLE': 'Cleveland Indians',
    'COL': 'Colorado Rockies',
    'CWS': 'Chicago White Sox',
    'DET': 'Detroit Tigers',
    'HOU': 'Houston Astros',
    'KC': 'Kansas City Royals',
    'LAA': 'Los Angeles Angels',
    'LAD': 'Los Angeles Dodgers',
    'MIA': 'Miami Marlins',
    'MIL': 'Milwaukee Brewers',
    'MIN': 'Minnesota Twins',
    'NYM': 'New York Mets',
    'NYY': 'New York Yankees',
    'OAK': 'Oakland Athletics',
    'PHI': 'Philadelphia Phillies',
    'PIT': 'Pittsburgh Pirates',
    'SD': 'San Diego Padres',
    'SEA': 'Seattle Mariners',
    'SF': 'San Francisco Giants',
    'STL': 'St. Louis Cardinals',
    'TB': 'Tampa Bay Rays',
    'TEX': 'Texas Rangers',
    'TOR': 'Toronto Blue Jays',
    'WSH': 'Washington Nationals',
    },
'americanfootball_nfl_preseason': {
    'ARI': 'Arizona Cardinals',
    'ATL': 'Atlanta Falcons',
    'BAL': 'Baltimore Ravens',
    'BUF': 'Buffalo Bills',
    'CAR': 'Carolina Panthers',
    'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals',
    'CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos',
    'DET': 'Detroit Lions',
    'GB': 'Green Bay Packers',
    'HOU': 'Houston Texans',
    'IND': 'Indianapolis Colts',
    'JAX': 'Jacksonville Jaguars',
    'KC': 'Kansas City Chiefs',
    'LAC': 'Los Angeles Chargers',
    'LAR': 'Los Angeles Rams',
    'LV': 'Las Vegas Raiders',
    'MIA': 'Miami Dolphins',
    'MIN': 'Minnesota Vikings',
    'NE': 'New England Patriots',
    'NO': 'New Orleans Saints',
    'NYG': 'New York Giants',
    'NYJ': 'New York Jets',
    'PHI': 'Philadelphia Eagles',
    'PIT': 'Pittsburgh Steelers',
    'SEA': 'Seattle Seahawks',
    'SF': 'San Francisco 49ers',
    'TB': 'Tampa Bay Buccaneers',
    'TEN': 'Tennessee Titans',
    'WAS': 'Washington Commanders',
    },
'americanfootball_nfl': {
    'ARI': 'Arizona Cardinals',
    'ATL': 'Atlanta Falcons',
    'BAL': 'Baltimore Ravens',
    'BUF': 'Buffalo Bills',
    'CAR': 'Carolina Panthers',
    'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals',
    'CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos',
    'DET': 'Detroit Lions',
    'GB': 'Green Bay Packers',
    'HOU': 'Houston Texans',
    'IND': 'Indianapolis Colts',
    'JAX': 'Jacksonville Jaguars',
    'KC': 'Kansas City Chiefs',
    'LAC': 'Los Angeles Chargers',
    'LAR': 'Los Angeles Rams',
    'LV': 'Las Vegas Raiders',
    'MIA': 'Miami Dolphins',
    'MIN': 'Minnesota Vikings',
    'NE': 'New England Patriots',
    'NO': 'New Orleans Saints',
    'NYG': 'New York Giants',
    'NYJ': 'New York Jets',
    'PHI': 'Philadelphia Eagles',
    'PIT': 'Pittsburgh Steelers',
    'SEA': 'Seattle Seahawks',
    'SF': 'San Francisco 49ers',
    'TB': 'Tampa Bay Buccaneers',
    'TEN': 'Tennessee Titans',
    'WAS': 'Washington Commanders',
    },
}
