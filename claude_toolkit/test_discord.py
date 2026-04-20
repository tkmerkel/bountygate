"""Send a single test message to the project Discord webhook.

Uses the shared notify() from app/shared/python/bountygate/utils/discord_notify.py
so behavior matches what the executor and DAGs actually use.

No secrets in the payload -- just hostname, timestamp, and the fact that the
toolkit triggered it.

Usage:
    python claude_toolkit/test_discord.py
    python claude_toolkit/test_discord.py --strict   # fail if env var unset
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
from datetime import datetime, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SHARED = os.path.join(_REPO_ROOT, "app", "shared", "python")
if _SHARED not in sys.path:
    sys.path.insert(0, _SHARED)

from bountygate.utils.discord_notify import WEBHOOK_ENV_VAR, notify  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--strict", action="store_true",
                        help=f"Exit non-zero if {WEBHOOK_ENV_VAR} is not set.")
    args = parser.parse_args()

    webhook_set = bool(os.environ.get(WEBHOOK_ENV_VAR))
    if not webhook_set:
        msg = (
            f"{WEBHOOK_ENV_VAR} is not set. discord_notify.notify() will fall "
            "back to the hardcoded default webhook (see CRITIQUE.md P0 #3)."
        )
        if args.strict:
            print(f"ERROR: {msg}", file=sys.stderr)
            return 2
        print(f"WARNING: {msg}")

    host = socket.gethostname()
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = f"toolkit test from {host} at {ts} -- safe to ignore"
    print(f"Posting: {body!r}")
    notify(body, level="info", source="claude_toolkit:test_discord")
    print("Done. Check the Discord channel for an INFO message.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
