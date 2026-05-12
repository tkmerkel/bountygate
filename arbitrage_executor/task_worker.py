"""
Task queue worker — polls bot_execution_queue for PENDING tasks and runs execute_arb.main().

Halts on OrphanedBetError (BetMGM placed but FanDuel hedge failed). The
CRITICAL Discord alert is already in flight by the time we see that
exception; the worker just needs to stop polling so we don't place more
bets while the human is intervening on the unhedged one.

Posts a Discord heartbeat every HEARTBEAT_INTERVAL_MINUTES (default 30) so
the user knows the bot is alive even when no opportunities are being placed.
"""

import logging
import os
import sys
import time
import traceback

from db_connection import claim_pending_task, complete_task, pending_task_count
from execute_arb import OrphanedBetError, WorkerHaltError

# Path setup so we can import the shared Discord notifier (executor isn't a package).
_SHARED_PY = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app", "shared", "python")
)
if _SHARED_PY not in sys.path:
    sys.path.insert(0, _SHARED_PY)
from bountygate.utils.discord_notify import notify  # noqa: E402

POLL_INTERVAL = 15  # seconds between polls when idle
POST_TASK_DELAY = 2  # seconds between consecutive tasks
HEARTBEAT_INTERVAL_MINUTES = int(os.getenv("HEARTBEAT_INTERVAL_MINUTES", "30"))
_HEARTBEAT_INTERVAL_SECONDS = HEARTBEAT_INTERVAL_MINUTES * 60

# Circuit breaker — halt after this many consecutive execution failures
# (does not count "no candidates" tasks, which are benign). Without this the
# worker can burn through every queued task replaying the same broken
# selector. Tune via env if needed.
CONSECUTIVE_FAILURE_LIMIT = int(os.getenv("CONSECUTIVE_FAILURE_LIMIT", "3"))

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


def _send_heartbeat(counters: dict) -> None:
    """Post the heartbeat summary to Discord. Resets counters in place."""
    queue_depth = pending_task_count()
    window = f"{HEARTBEAT_INTERVAL_MINUTES}m"

    if counters["attempts"] == 0 and queue_depth == 0:
        body = f"Bot alive — no opportunities in last {window} (queue empty)"
    else:
        body = (
            f"Bot alive — {window} window: {counters['attempts']} attempted, "
            f"{counters['succeeded']} placed, {counters['no_opportunity']} no-opp, "
            f"{counters['errored']} errored\n"
            f"queue: {queue_depth} PENDING"
        )

    notify(body, level="info", source="arbitrage_executor")
    logger.info("Heartbeat posted: %s", body.replace("\n", " | "))

    for key in counters:
        counters[key] = 0


def run_worker() -> None:
    """Main polling loop — runs forever until interrupted."""
    logger.info(
        "Task worker started. Polling for pending tasks (heartbeat every %dm)...",
        HEARTBEAT_INTERVAL_MINUTES,
    )

    counters = {"attempts": 0, "succeeded": 0, "no_opportunity": 0, "errored": 0}
    last_heartbeat = time.monotonic()
    consecutive_failures = 0
    last_failure_summary = ""

    # Initial "I just started" ping so the user knows the worker came up.
    notify(
        f"Worker started (heartbeat every {HEARTBEAT_INTERVAL_MINUTES}m, "
        f"circuit breaker at {CONSECUTIVE_FAILURE_LIMIT} consecutive failures)",
        level="info",
        source="arbitrage_executor",
    )

    while True:
        # Heartbeat check — fires regardless of queue activity.
        if time.monotonic() - last_heartbeat >= _HEARTBEAT_INTERVAL_SECONDS:
            _send_heartbeat(counters)
            last_heartbeat = time.monotonic()

        task_id = claim_pending_task()

        if task_id is None:
            time.sleep(POLL_INTERVAL)
            continue

        counters["attempts"] += 1
        logger.info("Claimed task %s — executing", task_id)

        # `failure_kind` drives the circuit breaker:
        #   "none"        -> success or empty queue, reset counter
        #   "attempted"   -> bot tried and failed, increment counter
        #   "exception"   -> bot raised, increment counter
        failure_kind = "none"

        try:
            from execute_arb import main as execute_arb_main
            success, attempted = execute_arb_main()
            if success:
                counters["succeeded"] += 1
                complete_task(task_id, success=True)
                logger.info("Task %s completed successfully", task_id)
            elif attempted:
                failure_kind = "attempted"
                counters["errored"] += 1
                last_failure_summary = "execute_arb attempted a viable candidate and returned failure"
                complete_task(task_id, success=False, error_msg=last_failure_summary)
                logger.warning("Task %s — execution attempted but failed", task_id)
            else:
                # No viable candidates (all unmapped, wrong book pair, etc.).
                # This is normal on quiet windows — do NOT trip the breaker.
                counters["no_opportunity"] += 1
                complete_task(task_id, success=False, error_msg="No viable opportunity")
                logger.info("Task %s — no viable candidates", task_id)
        except OrphanedBetError as e:
            err_msg = (
                f"ORPHANED BET — worker halting. {e}\n"
                "BetMGM was placed but the FanDuel hedge failed. Check Discord "
                "for the CRITICAL alert with manual-hedge instructions, resolve "
                "the bet on FanDuel by hand, then restart the worker."
            )
            complete_task(task_id, success=False, error_msg=err_msg)
            logger.critical("Task %s ORPHANED BET — worker halting:\n%s", task_id, err_msg)
            sys.exit(1)
        except WorkerHaltError as e:
            # Operator intervention required (e.g. 2FA on login). The
            # CRITICAL Discord alert was already sent by the executor; the
            # worker just has to stop polling so we don't pile up failed
            # login attempts that could lock the account.
            err_msg = f"Worker halt requested: {e}"
            complete_task(task_id, success=False, error_msg=err_msg)
            logger.critical("Task %s halt requested: %s", task_id, err_msg)
            sys.exit(1)
        except Exception:
            failure_kind = "exception"
            counters["errored"] += 1
            tb = traceback.format_exc()
            last_failure_summary = tb.strip().splitlines()[-1] if tb.strip() else "uncaught exception"
            complete_task(task_id, success=False, error_msg=tb)
            logger.error("Task %s failed:\n%s", task_id, tb)

        # Circuit breaker bookkeeping
        if failure_kind == "none":
            if consecutive_failures > 0:
                logger.info("Consecutive-failure counter reset (was %d)", consecutive_failures)
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            logger.warning(
                "Consecutive failures: %d/%d (kind=%s)",
                consecutive_failures, CONSECUTIVE_FAILURE_LIMIT, failure_kind,
            )
            if consecutive_failures >= CONSECUTIVE_FAILURE_LIMIT:
                halt_msg = (
                    f"CIRCUIT BREAKER TRIPPED — worker halting after "
                    f"{consecutive_failures} consecutive execution failures.\n"
                    f"Last failure: {last_failure_summary}\n\n"
                    "Likely causes: sportsbook UI changed (selectors stale), "
                    "session was logged out, or Chrome lost CDP connection. "
                    "Inspect logs/execution_failures.log and the most recent "
                    "audit_logs/ subdir, fix the root cause, then restart the worker."
                )
                notify(halt_msg, level="critical", source="arbitrage_executor")
                logger.critical(halt_msg)
                sys.exit(1)

        time.sleep(POST_TASK_DELAY)


if __name__ == "__main__":
    try:
        run_worker()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
