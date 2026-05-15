web: uvicorn app.web.main:app --host 0.0.0.0 --port $PORT --workers ${WEB_CONCURRENCY:-2}
release: python scripts/migrate.py up
