"""bountygate.enrichment — Phase 2 free-API enrichment.

Pure, importable, test-against-fixtures logic for matching external feed data
(ESPN / MLB StatsAPI / NHL / Open-Meteo) to the analytics dimensional model and
for grading player props. The DAGs in airflow/dags/ orchestrate; this package
holds the parsing/matching/grading logic so it can be unit-tested without
Airflow or a live network.
"""
