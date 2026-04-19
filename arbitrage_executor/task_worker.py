"""
Task queue worker — polls bot_execution_queue for PENDING tasks and runs execute_arb.main().
"""

import logging
import time
import traceback

from db_connection import claim_pending_task, complete_task

POLL_INTERVAL = 15  # seconds between polls when idle
POST_TASK_DELAY = 2  # seconds between consecutive tasks

# ---------------------------------------------------------------------------
# Logging setup: console + file
# ---------------------------------------------------------------------------
logger = logging.getLogger("task_worker")
logger.setLevel(logging.INFO)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
logger.addHandler(_sh)

_fh = logging.FileHandler("logs/worker.log", encoding="utf-8")
_fh.setFormatter(_fmt)
logger.addHandler(_fh)


def run_worker() -> None:
    """Main polling loop — runs forever until interrupted."""
    logger.info("Task worker started. Polling for pending tasks...")

    while True:
        task_id = claim_pending_task()

        if task_id is None:
            time.sleep(POLL_INTERVAL)
            continue

        logger.info("Claimed task %s — executing", task_id)

        try:
            from execute_arb import main as execute_arb_main
            success = execute_arb_main()
            if success:
                complete_task(task_id, success=True)
                logger.info("Task %s completed successfully", task_id)
            else:
                complete_task(task_id, success=False, error_msg="No viable opportunity executed")
                logger.warning("Task %s finished with no successful execution", task_id)
        except Exception:
            tb = traceback.format_exc()
            complete_task(task_id, success=False, error_msg=tb)
            logger.error("Task %s failed:\n%s", task_id, tb)

        time.sleep(POST_TASK_DELAY)


if __name__ == "__main__":
    try:
        run_worker()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
