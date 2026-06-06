-- Add normalized UTC timestamp for commence_time and backfill
ALTER TABLE bg_unified_lines
  ADD COLUMN IF NOT EXISTS commence_at_utc timestamptz;

-- Backfill from text-based commence_time handling common ISO variants
UPDATE bg_unified_lines
SET commence_at_utc = CASE
  WHEN commence_time IS NULL OR commence_time = '' THEN NULL
  -- ISO with Z
  WHEN commence_time ~ 'Z$' THEN (commence_time)::timestamptz
  -- ISO with timezone offset like +00:00
  WHEN commence_time ~ '\+\d{2}:\d{2}$' THEN (commence_time)::timestamptz
  -- ISO compact offset like +0000 → convert to +00:00 then cast
  WHEN commence_time ~ '\+\d{4}$' THEN (
    regexp_replace(commence_time, '(\+\d{2})(\d{2})$', '\1:\2')
  )::timestamptz
  ELSE NULL
END
WHERE commence_at_utc IS NULL;

CREATE INDEX IF NOT EXISTS idx_bg_unified_lines_commence_at_utc
  ON bg_unified_lines (commence_at_utc);

