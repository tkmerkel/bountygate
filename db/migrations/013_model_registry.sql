-- Common prediction shape all model tiers write to. consensus_v1 is the first source.
CREATE TABLE model_versions (
  model_key   text NOT NULL,
  version     text NOT NULL,
  created_at  timestamptz DEFAULT now(),
  description text,
  PRIMARY KEY (model_key, version)
);

CREATE TABLE model_predictions (
  model_key    text NOT NULL,
  version      text NOT NULL,
  event_id     uuid NOT NULL,
  market_type  text NOT NULL,
  outcome_name text NOT NULL,
  prob         numeric NOT NULL,
  predicted_at timestamptz NOT NULL,
  UNIQUE (model_key, version, event_id, market_type, outcome_name, predicted_at)
);
CREATE INDEX ix_model_predictions_event ON model_predictions (event_id, predicted_at);
