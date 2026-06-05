.PHONY: help migrate compose-up

help:
	@echo "bountygate task runner (analytics-aggregator pivot in progress). Targets:"
	@echo "  make migrate      — apply DB migrations"
	@echo "  make compose-up   — start Airflow stack via docker compose"

migrate:
	python scripts/migrate.py up

compose-up:
	cd airflow && docker compose up --build
