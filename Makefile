.PHONY: help doctor worker smoke migrate compose-up test

help:
	@echo "bountygate task runner. Targets:"
	@echo "  make doctor       — run toolkit/doctor.py (DB, Chrome, Discord checks)"
	@echo "  make worker       — start the arbitrage executor worker"
	@echo "  make smoke        — run the selector smoke test"
	@echo "  make migrate      — apply DB migrations"
	@echo "  make test         — run pytest in arbitrage_executor/"
	@echo "  make compose-up   — start Airflow stack via docker compose"

doctor:
	python toolkit/doctor.py

worker:
	cd arbitrage_executor && python task_worker.py

smoke:
	python toolkit/selector_smoke_test.py

migrate:
	python scripts/migrate.py up

test:
	cd arbitrage_executor && python -m pytest tests/ -v

compose-up:
	cd airflow && docker compose up --build
