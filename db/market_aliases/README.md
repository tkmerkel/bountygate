Market alias curation CSVs

Overview
- Map bookmaker-specific market labels (`bm_market_key`) to canonical `market_key` values used across the repo.
- Loaders and the normalization DAG can use these mappings to keep analytics consistent.

Conventions
- UTF-8 CSV with header.
- Columns: `bookmaker_key,sport_key,bm_market_key,canonical_market_key,notes`
- Leave `canonical_market_key` blank if unknown; fill over time.

Workflow
1) Generate a draft from the DB: `python scripts/generate_market_aliases.py` (writes `db/market_aliases/market_aliases.csv`).
2) Edit unmapped rows and refine canonical assignments.
3) Apply to DB and backfill: `python scripts/load_market_aliases.py --dir db/market_aliases --apply --backfill`.

