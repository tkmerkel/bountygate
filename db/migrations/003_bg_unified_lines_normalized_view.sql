CREATE OR REPLACE VIEW bg_unified_lines_normalized AS
SELECT
  bul.player_name,
  bul.outcome,
  bul.line,
  bul.market_key,
  bul.bm_market_key,
  bul.price,
  bul.multiplier,
  bul.bookmaker_key,
  bul.sport_key,
  bul.sport_title,
  bul.commence_at_utc,
  bul.home_team_id,
  bul.away_team_id,
  th.display_name AS home_team_name,
  ta.display_name AS away_team_name,
  bul.event_id,
  bul.fetched_at_utc
FROM bg_unified_lines bul
LEFT JOIN team_reference th ON th.team_id = bul.home_team_id
LEFT JOIN team_reference ta ON ta.team_id = bul.away_team_id;

