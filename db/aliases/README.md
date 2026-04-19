Alias curation CSVs

Overview
- CSV-driven aliases allow contributors to propose/curate mappings without code changes.
- Files here are the source of truth for TEAM aliases. A loader will upsert them into the DB.

Conventions
- UTF-8, comma-separated, quoted only when necessary.
- Columns are fixed; leave unknown values blank.

Team aliases CSV columns
- sport_key: e.g., americanfootball_nfl, basketball_nba
- alias: the raw team string seen in data (e.g., KC, GSW)
- canonical_name: the display name in team_reference (e.g., Kansas City Chiefs)
- source_bookmaker: optional source (e.g., sleeper, underdog)
- abbreviation: optional canonical abbreviation to store in team_reference
- team_id: optional; if blank, the loader resolves by canonical_name (or creates if allowed)
- notes: optional comments

Workflow
1) Edit CSVs locally; run a dry-run loader to see planned changes.
2) Apply and verify; commit CSVs and open a PR for review.
3) Airflow can load CSVs on a schedule once vetted (after review).
