-- Game finals + scoring outputs. winner is 'home' or 'away' (resolved against the
-- event's own team names, immune to feed-vs-odds naming differences).
CREATE TABLE game_results (
  event_id     uuid PRIMARY KEY REFERENCES sports_events(event_id),
  home_score   int,
  away_score   int,
  winner       text,
  completed_at timestamptz,
  source       text
);

CREATE TABLE venue_sharpness (
  venue_key    text NOT NULL,
  sport_key    text NOT NULL,
  score_window text NOT NULL,
  n_games      int,
  brier        numeric,
  logloss      numeric,
  avg_clv      numeric,
  computed_at  timestamptz,
  UNIQUE (venue_key, sport_key, score_window)
);

CREATE TABLE mart_calibration (
  source      text NOT NULL,
  sport_key   text NOT NULL,
  prob_bucket numeric NOT NULL,
  n           int,
  predicted_mean numeric,
  realized_rate  numeric,
  computed_at timestamptz,
  UNIQUE (source, sport_key, prob_bucket)
);
