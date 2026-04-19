"""
Execution Logger
Handles logging for arbitrage execution: unmapped markets, failures, successes.
"""

import json
import os
from datetime import datetime
from typing import Dict, Optional


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
