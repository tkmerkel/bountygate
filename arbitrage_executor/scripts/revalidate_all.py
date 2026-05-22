"""Run validate_selector for every (site, market_key) in the YAML
configs and report a regression summary.

Usage:
    python -m scripts.revalidate_all
    python -m scripts.revalidate_all --site fanduel
    python -m scripts.revalidate_all --testing-mode

Exits 0 if all probed markets passed or were skipped for no-candidate
reasons. Exits 1 if any market that has recent candidates failed
validation.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Repo root is one level above this file (scripts/ sits next to the
# flat-layout modules: selector_finder.py, validate_selector.py, ...).
EXECUTOR_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(EXECUTOR_DIR))
os.chdir(EXECUTOR_DIR)

from selector_finder import SelectorManager  # noqa: E402
from validate_selector import (  # noqa: E402
    ValidationError,
    fetch_validation_opportunities,
    validate_selector,
)
from bet_placer import BetPlacerError  # noqa: E402


def _iter_markets(filter_site: str | None) -> list[tuple[str, str]]:
    """Return [(site, market_key), ...] for every entry in the YAML configs.

    Markets in selectors/*_markets.yaml live at the YAML root (one key per
    market) — see selectors/SCHEMA.md. SelectorManager.load_market_config
    returns that dict already filtered to mapping-valued entries, so we
    iterate its keys.
    """
    sites = [filter_site] if filter_site else ["fanduel", "betmgm"]
    pairs: list[tuple[str, str]] = []
    for site in sites:
        config = SelectorManager.load_market_config(site)
        for market_key in config.keys():
            pairs.append((site, market_key))
    return pairs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", choices=["fanduel", "betmgm"], default=None)
    parser.add_argument("--testing-mode", action="store_true")
    args = parser.parse_args()

    pairs = _iter_markets(args.site)
    print(f"[revalidate_all] {len(pairs)} markets to probe")

    passed: list[tuple[str, str]] = []
    failed: list[tuple[str, str, str]] = []
    no_candidates: list[tuple[str, str]] = []

    for site, market in pairs:
        print(f"\n[revalidate_all] === {site}/{market} ===")
        try:
            opps = fetch_validation_opportunities(
                site, market, testing_mode=args.testing_mode,
            )
            if not opps:
                no_candidates.append((site, market))
                print(f"  SKIP: no candidate opportunities")
                continue
            validate_selector(site, market, opps[0], save=True)
            passed.append((site, market))
        except (ValidationError, BetPlacerError) as e:
            failed.append((site, market, f"{type(e).__name__}: {e}"))
            print(f"  FAIL: {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print(f"[revalidate_all] passed:        {len(passed)}")
    print(f"[revalidate_all] failed:        {len(failed)}")
    print(f"[revalidate_all] no candidates: {len(no_candidates)}")
    print("=" * 60)
    if failed:
        print("\nRegressions:")
        for site, market, err in failed:
            print(f"  - {site}/{market}: {err}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
