-- Seed data for dim_sport and dim_bookmaker.
-- All inserts are idempotent (ON CONFLICT DO NOTHING).
-- No PL/pgSQL; no DO $$ blocks; no CREATE FUNCTION/TRIGGER.

INSERT INTO dim_sport (sport_key, sport_title, league, is_outdoor, active)
VALUES ('americanfootball_nfl', 'NFL', 'NFL', true, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_sport (sport_key, sport_title, league, is_outdoor, active)
VALUES ('baseball_mlb', 'MLB', 'MLB', true, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_sport (sport_key, sport_title, league, is_outdoor, active)
VALUES ('icehockey_nhl', 'NHL', 'NHL', false, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_sport (sport_key, sport_title, league, is_outdoor, active)
VALUES ('basketball_nba', 'NBA', 'NBA', false, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_sport (sport_key, sport_title, league, is_outdoor, active)
VALUES ('basketball_ncaab', 'NCAAB', 'NCAA Basketball', false, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_sport (sport_key, sport_title, league, is_outdoor, active)
VALUES ('americanfootball_ncaaf', 'NCAAF', 'NCAA Football', true, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_bookmaker (bookmaker_key, bookmaker_title, is_sharp, is_soft, is_legal_for_betting)
VALUES ('pinnacle', 'Pinnacle', true, false, false)
ON CONFLICT DO NOTHING;

INSERT INTO dim_bookmaker (bookmaker_key, bookmaker_title, is_sharp, is_soft, is_legal_for_betting)
VALUES ('fanduel', 'FanDuel', false, true, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_bookmaker (bookmaker_key, bookmaker_title, is_sharp, is_soft, is_legal_for_betting)
VALUES ('betmgm', 'BetMGM', false, true, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_bookmaker (bookmaker_key, bookmaker_title, is_sharp, is_soft, is_legal_for_betting)
VALUES ('draftkings', 'DraftKings', false, true, true)
ON CONFLICT DO NOTHING;

INSERT INTO dim_bookmaker (bookmaker_key, bookmaker_title, is_sharp, is_soft, is_legal_for_betting)
VALUES ('williamhill_us', 'Caesars Sportsbook', false, true, true)
ON CONFLICT DO NOTHING;
