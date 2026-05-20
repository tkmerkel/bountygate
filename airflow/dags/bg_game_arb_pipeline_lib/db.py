"""DB helpers for the game-arb pipeline.

Re-exports bulk_replace / bulk_append_new from bg_arb_pipeline_lib.db so we
don't fork the Postgres COPY implementation. Both lib packages live under
the same dags/ root that the Airflow runtime puts on PYTHONPATH.
"""
from __future__ import annotations

from bg_arb_pipeline_lib.db import bulk_append_new, bulk_replace

__all__ = ["bulk_append_new", "bulk_replace"]
