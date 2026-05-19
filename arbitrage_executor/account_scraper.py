"""Scrape current balance + 7d P&L from each sportsbook's account page.

Called by execute_arb at the end of a successful run (just before browser
tabs are closed), reusing the warm Playwright pages. Writes results to the
account_stats Postgres table and emits an 'account-scraper' heartbeat.

Public API:
    scrape_all(page_fd, page_mgm) -> None
        Run both scrapers, upsert account_stats, append history, heartbeat.

    parse_balance_text(text) -> Optional[float]
        Pure function exposed for unit testing.

CSS SELECTOR NOTE
-----------------
The selectors below are placeholders. The real selectors must be captured
against each book's live account page (use map_selectors.py-style workflow
or DevTools inspection). Until the placeholders are replaced, each scrape
will fail and write scrape_status='error' to the dashboard — visible in the
Watchers + Accounts cards.
"""
from __future__ import annotations

import os
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Import the shared heartbeat helper (executor isn't a package).
_SHARED = Path(__file__).resolve().parents[1] / "app" / "shared" / "python"
if str(_SHARED) not in sys.path:
    sys.path.insert(0, str(_SHARED))
from bountygate.watcher_heartbeat import heartbeat  # type: ignore  # noqa: E402


_MONEY_RE = re.compile(r"-?\$?\d[\d,]*\.?\d*")


def parse_balance_text(s: str) -> Optional[float]:
    """Extract a float from a money-formatted string. Returns None on garbage."""
    if not s:
        return None
    m = _MONEY_RE.search(s.strip())
    if not m:
        return None
    raw = m.group(0).replace("$", "").replace(",", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _engine() -> Engine:
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url, pool_pre_ping=True)  # type: ignore[return-value]


# --- Per-book scrapers ---------------------------------------------------
# These selectors are PLACEHOLDERS. Replace with real selectors captured
# against each book's live account page before relying on the data.

def _scrape_fanduel(page) -> dict:
    page.goto("https://account.fanduel.com/balance", wait_until="domcontentloaded")
    page.wait_for_selector('[data-test-id="balance-total"]', timeout=15000)
    balance = parse_balance_text(page.locator('[data-test-id="balance-total"]').first.inner_text())
    pending = parse_balance_text(page.locator('[data-test-id="pending-wagers"]').first.inner_text())
    page.goto("https://account.fanduel.com/wagers/settled?range=7d", wait_until="domcontentloaded")
    page.wait_for_selector('[data-test-id="settled-pnl-total"]', timeout=15000)
    pnl = parse_balance_text(page.locator('[data-test-id="settled-pnl-total"]').first.inner_text())
    avail = (balance - pending) if (balance is not None and pending is not None) else None
    return {"balance": balance, "pending_wagers": pending, "available_liquidity": avail, "pnl_7d": pnl}


def _scrape_betmgm(page) -> dict:
    page.goto("https://account.betmgm.com/balance", wait_until="domcontentloaded")
    page.wait_for_selector(".balance-amount", timeout=15000)
    balance = parse_balance_text(page.locator(".balance-amount").first.inner_text())
    pending = parse_balance_text(page.locator(".pending-wagers-amount").first.inner_text())
    page.goto("https://account.betmgm.com/wager-history?range=7d", wait_until="domcontentloaded")
    page.wait_for_selector(".pnl-7d", timeout=15000)
    pnl = parse_balance_text(page.locator(".pnl-7d").first.inner_text())
    avail = (balance - pending) if (balance is not None and pending is not None) else None
    return {"balance": balance, "pending_wagers": pending, "available_liquidity": avail, "pnl_7d": pnl}


def _write(eng: Engine, book: str, data: Optional[dict], err: Optional[str], now: datetime) -> None:
    """Upsert account_stats + append account_stats_history. err is None on success."""
    status = "error" if err else "ok"
    payload = {
        "book": book,
        "balance": (data or {}).get("balance"),
        "pending": (data or {}).get("pending_wagers"),
        "avail": (data or {}).get("available_liquidity"),
        "pnl": (data or {}).get("pnl_7d"),
        "scraped_at": now,
        "status": status,
        "err": err[:500] if err else None,
    }
    with eng.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO account_stats (
                book, balance, pending_wagers, available_liquidity, pnl_7d,
                scrape_status, last_error, scraped_at
            ) VALUES (
                :book, :balance, :pending, :avail, :pnl, :status, :err, :scraped_at
            )
            ON CONFLICT (book) DO UPDATE SET
                balance = EXCLUDED.balance,
                pending_wagers = EXCLUDED.pending_wagers,
                available_liquidity = EXCLUDED.available_liquidity,
                pnl_7d = EXCLUDED.pnl_7d,
                scrape_status = EXCLUDED.scrape_status,
                last_error = EXCLUDED.last_error,
                scraped_at = EXCLUDED.scraped_at
            """
        ), payload)
        if data:
            conn.execute(text(
                "INSERT INTO account_stats_history (book, scraped_at, balance, pnl_7d) "
                "VALUES (:book, :scraped_at, :balance, :pnl) "
                "ON CONFLICT (book, scraped_at) DO NOTHING"
            ), {"book": book, "scraped_at": now, "balance": payload["balance"], "pnl": payload["pnl"]})


def scrape_all(page_fd=None, page_mgm=None) -> None:
    """Scrape both books, write results, heartbeat.

    Either page may be None (e.g. one book's session is dead) — that book is
    skipped silently for this run; the other still scrapes. Caller is the
    bot's execute_arb at end of a successful run.
    """
    eng = _engine()
    now = datetime.now(timezone.utc)
    errors = 0

    scrapers = []
    if page_fd is not None:
        scrapers.append(("fanduel", page_fd, _scrape_fanduel))
    if page_mgm is not None:
        scrapers.append(("betmgm", page_mgm, _scrape_betmgm))

    for book, page, fn in scrapers:
        try:
            data = fn(page)
            _write(eng, book, data, None, now)
        except Exception as e:
            errors += 1
            err_msg = f"{type(e).__name__}: {e}"
            try:
                _write(eng, book, None, err_msg, now)
            except Exception:
                traceback.print_exc()

    heartbeat(
        "account-scraper",
        is_running=True,
        expected_interval_s=3600,
        pending_count=0,
        errors_24h=errors,
    )
