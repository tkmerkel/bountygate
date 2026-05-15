"""Phase 1 parity check between old arb tables and the new unified table.

Run during the 24-48 hour parallel-write window before flipping bot reads.
Expects new pipeline to be a SUPERSET of old: zero rows missing from new,
plus new alt_std and alt_alt rows.

Usage:
    python scripts/dq_checks_arb_parity.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make the shared package importable from repo root.
_repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root / "app" / "shared" / "python"))

from bountygate.utils.db_connection import fetch_data  # noqa: E402

CHECKS = {
    "std_std parity (old std rows missing from new)": """
        SELECT player_name, market_key AS under_mk, market_key AS over_mk,
               under_bookmaker_key, over_bookmaker_key
        FROM bg_arbitrage_player_props
        WHERE fetched_at_utc >= now() - INTERVAL '1 day'
        EXCEPT
        SELECT player_name, under_market_key, over_market_key,
               under_book, over_book
        FROM bg_arbitrage_opportunities
        WHERE pairing_type = 'std_std'
          AND fetched_at_utc >= now() - INTERVAL '1 day';
    """,
    "std_alt parity (old alt rows missing from new)": """
        SELECT player_name, under_market_key, over_market_key,
               under_bookmaker_key, over_bookmaker_key
        FROM bg_arbitrage_player_props_alt
        WHERE fetched_at_utc >= now() - INTERVAL '1 day'
        EXCEPT
        SELECT player_name, under_market_key, over_market_key,
               under_book, over_book
        FROM bg_arbitrage_opportunities
        WHERE pairing_type = 'std_alt'
          AND fetched_at_utc >= now() - INTERVAL '1 day';
    """,
    "pairing_type breakdown (new pipeline)": """
        SELECT pairing_type, COUNT(*) AS cnt,
               AVG(roi)::numeric(6,4) AS avg_roi,
               MAX(roi)::numeric(6,4) AS max_roi
        FROM bg_arbitrage_opportunities
        WHERE fetched_at_utc >= now() - INTERVAL '1 day'
        GROUP BY pairing_type
        ORDER BY pairing_type;
    """,
}


def main() -> int:
    failures = 0
    for label, sql in CHECKS.items():
        print(f"\n=== {label} ===")
        df = fetch_data(sql)
        if df is None or df.empty:
            if "parity" in label:
                print("OK — zero rows.")
            else:
                print("WARN — no rows. New pipeline produced nothing in the last day.")
                failures += 1
            continue
        print(df.to_string(index=False))
        if "parity" in label:
            print(f"FAIL — {len(df)} rows in old but not in new.")
            failures += 1
        elif "pairing_type" in label:
            missing = {"std_std", "std_alt", "alt_std", "alt_alt"} - set(df["pairing_type"])
            if missing:
                print(f"WARN — missing pairing_types: {sorted(missing)}")
    return failures


if __name__ == "__main__":
    sys.exit(main())
