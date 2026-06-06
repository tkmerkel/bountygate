-- Watermark store so the normalize DAG processes only new raw rows.
CREATE TABLE IF NOT EXISTS transform_state (
  name      text PRIMARY KEY,
  watermark timestamptz NOT NULL
);
