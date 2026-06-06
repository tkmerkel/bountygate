-- Table to store data quality metrics over time
CREATE TABLE IF NOT EXISTS dq_metrics (
  id bigserial PRIMARY KEY,
  captured_at timestamptz NOT NULL DEFAULT now(),
  metric_name text NOT NULL,
  metric_value numeric,
  dimensions jsonb DEFAULT '{}'::jsonb,
  notes text
);

CREATE INDEX IF NOT EXISTS idx_dq_metrics_metric_time ON dq_metrics(metric_name, captured_at);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_dims_gin ON dq_metrics USING GIN (dimensions);

