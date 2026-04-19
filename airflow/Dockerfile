# https://airflow.apache.org/docs/docker-stack/build.html
FROM apache/airflow:3.0.6
USER root
# Install shared packages (copy from app/shared in repo root context)
COPY app/shared /opt/bountygate-shared
# Include curated alias CSVs for runtime DAG use
COPY db/aliases /opt/bountygate-aliases
COPY db/market_aliases /opt/bountygate-market-aliases
RUN chown -R airflow:0 /opt/bountygate-shared
RUN chown -R airflow:0 /opt/bountygate-aliases || true
RUN chown -R airflow:0 /opt/bountygate-market-aliases || true
ENV PYTHONPATH="/opt/bountygate-shared:${PYTHONPATH}"
# Create a writable cache directory for uv
RUN mkdir -p /tmp/uv_cache && chmod -R 777 /tmp/uv_cache
# Set environment variable for uv cache
ENV UV_CACHE_DIR=/tmp/uv_cache

USER airflow
RUN pip install --no-cache-dir /opt/bountygate-shared/python
COPY airflow/requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
