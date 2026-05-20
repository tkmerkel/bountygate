"""Shared helpers for bet_placer site implementations.

Kept intentionally tiny. The only reusable pattern in the legacy
bet_placer.py was the selector-cascade locator search; everything else
(modal dismissal, slip inspection, wager-entry quirks) is site-specific
and lives in the respective subclass.
"""

import os
from datetime import datetime
from typing import Iterable, Optional

from playwright.sync_api import Page, Locator

def screenshot(page: Page, audit_dir: str, site: str, tag: str) -> str:
    """Save screenshot for audit trail. Never raises — logs and returns
    the intended path even if capture fails."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(audit_dir, f"{site}_{tag}_{timestamp}.png")
    try:
        page.screenshot(path=filename, full_page=True)
    except Exception as e:
        print(f"⚠ Screenshot failed: {e}")
    return filename


def first_visible(
    page: Page,
    selectors: Iterable[str],
    *,
    label: str = "",
    site: str = "",
) -> Optional[Locator]:
    """Try each CSS selector in order; return the first locator whose
    `.first` is visible, or None. Logs which selector matched on success
    so the audit trail keeps parity with legacy inline logging.

    Caller is responsible for any selector with placement-sensitive
    semantics (e.g. picking the LAST empty stake input, not the first
    visible one) — that logic stays inline in the caller.
    """
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                if label:
                    print(f"[{site.upper()}] {label} via {sel}")
                return loc.first
        except Exception:
            continue
    return None
