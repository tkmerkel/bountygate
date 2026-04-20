"""
Execution Logger
Handles logging for arbitrage execution: unmapped markets, failures, successes.
Failures and unmapped markets also page via Discord (shared notifier).
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, Optional

# Make bountygate.utils.discord_notify importable from this script (the
# arbitrage_executor isn't a package and shared utilities live under
# app/shared/python). Idempotent.
_SHARED_PY = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app", "shared", "python")
)
if _SHARED_PY not in sys.path:
    sys.path.insert(0, _SHARED_PY)

from bountygate.utils.discord_notify import notify  # noqa: E402


_SOURCE = "arbitrage_executor"


def _opportunity_summary(opportunity: Dict) -> str:
    """One-line summary safe for Discord message bodies."""
    player = opportunity.get("player_name") or "?"
    market = (
        opportunity.get("market_key")
        or opportunity.get("over_market_key")
        or opportunity.get("under_market_key")
        or "?"
    )
    line = opportunity.get("over_line") if opportunity.get("over_line") is not None else opportunity.get("under_line")
    line_str = f"@ {line}" if line is not None else ""
    over_book = opportunity.get("over_bookmaker_key") or "?"
    under_book = opportunity.get("under_bookmaker_key") or "?"
    roi = opportunity.get("roi")
    roi_str = f"ROI {float(roi) * 100:.2f}%" if roi is not None else ""
    return f"{player} | {market} {line_str} | {over_book} vs {under_book} | {roi_str}".strip()


class ExecutionLogger:
    """Centralized logging for arbitrage bot execution."""

    UNMAPPED_MARKETS_LOG = "logs/unmapped_markets.log"
    EXECUTION_FAILURES_LOG = "logs/execution_failures.log"
    EXECUTION_SUCCESS_LOG = "logs/execution_success.log"

    @staticmethod
    def _log_entry(log_file: str, message: str, data: Optional[Dict] = None):
        """Write a log entry with timestamp."""
        timestamp = datetime.now().isoformat()
        entry = f"[{timestamp}] {message}"

        if data:
            entry += f"\n{json.dumps(data, indent=2)}"

        entry += "\n" + "-" * 80 + "\n"

        os.makedirs("logs", exist_ok=True)

        with open(log_file, "a") as f:
            f.write(entry)

        print(entry)

    @staticmethod
    def log_unmapped_market(site: str, market_key: str, opportunity: Dict):
        """Log when a market selector is not found."""
        message = f"UNMAPPED MARKET: {site} - {market_key}"
        data = {
            "site": site,
            "market_key": market_key,
            "player_name": opportunity.get("player_name"),
            "sport": opportunity.get("sport_title"),
            "event": f"{opportunity.get('away_team')} @ {opportunity.get('home_team')}",
            "suggestion": f"Run: python map_selectors.py --site {site} --market {market_key}"
        }

        ExecutionLogger._log_entry(ExecutionLogger.UNMAPPED_MARKETS_LOG, message, data)
        notify(
            f"Unmapped market — {site}/{market_key}\n{_opportunity_summary(opportunity)}\n"
            f"Fix: python map_selectors.py --site {site} --market {market_key}",
            level="warning",
            source=_SOURCE,
        )

    @staticmethod
    def log_execution_failure(reason: str, opportunity: Dict, site: Optional[str] = None,
                             error: Optional[Exception] = None):
        """Log when an execution attempt fails."""
        message = f"EXECUTION FAILURE: {reason}"

        data = {
            "reason": reason,
            "site": site,
            "opportunity": {
                "player": opportunity.get("player_name"),
                "market": opportunity.get("market_key") or opportunity.get("over_market_key"),
                "sport": opportunity.get("sport_title"),
                "event": f"{opportunity.get('away_team')} @ {opportunity.get('home_team')}",
                "line": opportunity.get("over_line"),
                "roi": opportunity.get("roi"),
                "books": f"{opportunity.get('over_bookmaker_key')} vs {opportunity.get('under_bookmaker_key')}"
            }
        }

        if error:
            data["error"] = str(error)
            data["error_type"] = type(error).__name__

        ExecutionLogger._log_entry(ExecutionLogger.EXECUTION_FAILURES_LOG, message, data)

        site_str = f" [{site}]" if site else ""
        err_str = f"\nerror: {type(error).__name__}: {error}" if error else ""
        notify(
            f"Execution failure{site_str}: {reason}\n{_opportunity_summary(opportunity)}{err_str}",
            level="warning",
            source=_SOURCE,
        )

    @staticmethod
    def log_critical(reason: str, opportunity: Dict, action_required: str,
                     details: Optional[Dict] = None):
        """Log a CRITICAL event — used for orphaned-bet scenarios.

        Always pages Discord at CRITICAL severity. The body MUST contain
        everything the user needs to manually intervene (what was placed,
        on which book, at what odds, what the unhedged exposure is).
        """
        message = f"CRITICAL: {reason}"

        data = {
            "reason": reason,
            "action_required": action_required,
            "opportunity": {
                "player": opportunity.get("player_name"),
                "market": opportunity.get("market_key") or opportunity.get("over_market_key"),
                "sport": opportunity.get("sport_title"),
                "event": f"{opportunity.get('away_team')} @ {opportunity.get('home_team')}",
                "line": opportunity.get("over_line"),
                "roi": opportunity.get("roi"),
                "books": f"{opportunity.get('over_bookmaker_key')} vs {opportunity.get('under_bookmaker_key')}",
            },
        }
        if details:
            data["details"] = details

        ExecutionLogger._log_entry(ExecutionLogger.EXECUTION_FAILURES_LOG, message, data)

        details_lines = ""
        if details:
            details_lines = "\n" + "\n".join(f"  {k}: {v}" for k, v in details.items())
        notify(
            f"{reason}\n{_opportunity_summary(opportunity)}{details_lines}\n\n"
            f"ACTION REQUIRED: {action_required}",
            level="critical",
            source=_SOURCE,
        )

    @staticmethod
    def log_execution_success(opportunity: Dict, fanduel_details: Dict, betmgm_details: Dict,
                              audit_dir: str):
        """Log successful execution."""
        market_display = opportunity.get('market_key') or opportunity.get('over_market_key')
        message = f"EXECUTION SUCCESS: {opportunity.get('player_name')} - {market_display}"

        data = {
            "opportunity": {
                "player": opportunity.get("player_name"),
                "market": market_display,
                "sport": opportunity.get("sport_title"),
                "event": f"{opportunity.get('away_team')} @ {opportunity.get('home_team')}",
                "line": opportunity.get("over_line"),
                "roi": opportunity.get("roi"),
            },
            "fanduel": fanduel_details,
            "betmgm": betmgm_details,
            "audit_trail": audit_dir
        }

        ExecutionLogger._log_entry(ExecutionLogger.EXECUTION_SUCCESS_LOG, message, data)

    @staticmethod
    def log_skip(reason: str, opportunity: Dict):
        """Log when an opportunity is skipped (wrapper for execution_failure)."""
        ExecutionLogger.log_execution_failure(f"SKIPPED: {reason}", opportunity)
