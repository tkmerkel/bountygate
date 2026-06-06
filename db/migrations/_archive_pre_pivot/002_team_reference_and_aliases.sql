-- Canonical team reference and alias mapping
CREATE TABLE IF NOT EXISTS team_reference (
  team_id uuid PRIMARY KEY,
  sport_key text NOT NULL,
  display_name text NOT NULL,
  abbreviation text,
  aliases jsonb DEFAULT '[]'::jsonb,
  active boolean DEFAULT true
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_team_reference_sport_display
  ON team_reference (sport_key, display_name);

CREATE TABLE IF NOT EXISTS team_aliases (
  sport_key text NOT NULL,
  alias text NOT NULL,
  team_id uuid NOT NULL REFERENCES team_reference(team_id) ON DELETE CASCADE,
  source_bookmaker text,
  PRIMARY KEY (sport_key, alias)
);

-- Extend unified lines with canonical IDs (to be backfilled later)
ALTER TABLE bg_unified_lines
  ADD COLUMN IF NOT EXISTS home_team_id uuid,
  ADD COLUMN IF NOT EXISTS away_team_id uuid;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_bg_unified_lines_home_team_id ON bg_unified_lines(home_team_id);
CREATE INDEX IF NOT EXISTS idx_bg_unified_lines_away_team_id ON bg_unified_lines(away_team_id);
