-- Support upsert-by-market in the market-history mart.
CREATE UNIQUE INDEX IF NOT EXISTS uq_mart_hist_market ON mart_market_history (market_id);
