"""Session-recovery login for FanDuel and BetMGM.

The Chrome profile in ``chrome_profile/`` persists cookies, but sportsbook
sessions expire on a rolling basis (typically 24-72h of inactivity). When a
session lapses, the bet flow lands on a login page instead of the search /
event page and dies with a misleading "could not find search input" error.

``ensure_logged_in(page, site, audit_dir)`` is the entry point: it does a
cheap navigation to the book's home, checks for logged-in indicators, and
runs the login flow only if needed. Idempotent — safe to call before every
bet attempt.

Credentials come from env (loaded from ``.env`` via db_connection's
bootstrap). Required vars: FANDUEL_USERNAME, FANDUEL_PASSWORD,
BETMGM_USERNAME, BETMGM_PASSWORD. The module never logs the password value;
the username is logged with most chars masked.

Caveats:
- Selectors here are best-guess. The sportsbook login pages don't change as
  often as the bet flow but they do change. If login fails, check the
  per-attempt screenshots in ``audit_dir/`` — the file names point at which
  step (email input, password input, submit, post-submit verification)
  failed. Update selectors in ``_FD_*`` / ``_MGM_*`` constants below.
- CAPTCHA and 2FA cannot be solved here. Detected and raised as
  ``LoginInterventionRequired``, which the executor surfaces as CRITICAL
  so the operator can intervene.
- This runs in the same Chrome profile as the bet flow. There is no
  separate browser context — by design, so cookies set by login propagate.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from playwright.sync_api import Page, Locator


# Site home pages used as the "warm up" navigation to check session state.
# Using the actual bet-flow entry point means a successful warm-up doubles
# as a no-op for the next navigate call.
_HOME_URLS = {
    "fanduel": "https://mo.sportsbook.fanduel.com/",
    "betmgm": "https://www.mo.betmgm.com/en/sports",
}

# URL fragments that indicate the page is the login page (not logged in).
_LOGIN_URL_MARKERS = ("/login", "/auth", "/account/login", "account.")

# Header-area selectors that, when visible, mean "you are not logged in."
# Filter to anchors/buttons containing common login labels.
_LOGGED_OUT_INDICATORS = (
    'a:has-text("Log In")',
    'a:has-text("LOG IN")',
    'button:has-text("Log In")',
    'button:has-text("LOG IN")',
    'a:has-text("Sign In")',
    'button:has-text("Sign In")',
)

# FanDuel login-form selectors, tried in order.
_FD_EMAIL_SELECTORS = (
    'input[type="email"]',
    'input[name="email"]',
    'input[autocomplete="email"]',
    'input[autocomplete="username"]',
    'input[id*="email" i]',
    'input[id*="username" i]',
    'input[placeholder*="email" i]',
)
_FD_PASSWORD_SELECTORS = (
    'input[type="password"]',
    'input[name="password"]',
    'input[autocomplete="current-password"]',
    'input[id*="password" i]',
)
_FD_SUBMIT_SELECTORS = (
    'button[type="submit"]:has-text("Log In")',
    'button[type="submit"]:has-text("LOG IN")',
    'button:has-text("Log In")',
    'button:has-text("LOG IN")',
    'button[type="submit"]',
)

# BetMGM login-form selectors. BetMGM historically uses a modal that
# overlays the sports page; sometimes navigated to a separate domain
# (e.g. account.betmgm.com). The same selector families work either way.
_MGM_EMAIL_SELECTORS = (
    'input[type="email"]',
    'input[name="email"]',
    'input[name="username"]',
    'input[autocomplete="username"]',
    'input[id*="email" i]',
    'input[id*="username" i]',
    'input[formcontrolname="email"]',
    'input[formcontrolname="username"]',
)
_MGM_PASSWORD_SELECTORS = (
    'input[type="password"]',
    'input[name="password"]',
    'input[autocomplete="current-password"]',
    'input[formcontrolname="password"]',
)
_MGM_SUBMIT_SELECTORS = (
    'button[type="submit"]:has-text("Log In")',
    'button[type="submit"]:has-text("LOGIN")',
    'button:has-text("Log In")',
    'button:has-text("LOGIN")',
    'button[type="submit"]',
)

# Text fragments on post-submit pages that indicate operator intervention
# is required. Conservative on purpose — false positives only delay one
# bet attempt; false negatives could pile up failed-login attempts and
# trigger account locks.
_INTERVENTION_KEYWORDS = (
    "verify",
    "verification code",
    "two-factor",
    "2fa",
    "captcha",
    "recaptcha",
    "security check",
    "are you human",
    "press and hold",
    "puzzle",
)


class LoginError(Exception):
    """Login could not complete. Counts as a normal execution failure —
    the circuit breaker will halt the worker after the configured number
    of consecutive failures."""


class LoginInterventionRequired(LoginError):
    """2FA, CAPTCHA, or another human-required step was detected. The
    executor should surface this as CRITICAL so the operator stops what
    they're doing and finishes the login by hand."""


def _mask_username(user: str) -> str:
    """Mask a username for logs — never print the whole thing."""
    if "@" in user:
        local, _, domain = user.partition("@")
        local_masked = local[:2] + "***" if len(local) > 2 else "***"
        return f"{local_masked}@{domain}"
    return (user[:2] + "***") if len(user) > 2 else "***"


def _safe_screenshot(page: Page, audit_dir: str, tag: str) -> None:
    try:
        os.makedirs(audit_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(audit_dir, f"auth_{tag}_{ts}.png")
        page.screenshot(path=path, full_page=True)
    except Exception as e:
        print(f"[AUTH] screenshot failed ({tag}): {e}")


def _first_visible(page: Page, selectors) -> Optional[Locator]:
    for sel in selectors:
        try:
            loc = page.locator(sel)
            n = loc.count()
            for i in range(min(n, 5)):
                cand = loc.nth(i)
                try:
                    if cand.is_visible():
                        return cand
                except Exception:
                    continue
        except Exception:
            continue
    return None


def _looks_logged_in(page: Page) -> bool:
    """Heuristic: not on a login URL AND no log-in button visible."""
    url = (page.url or "").lower()
    if any(marker in url for marker in _LOGIN_URL_MARKERS):
        return False
    if _first_visible(page, _LOGGED_OUT_INDICATORS) is not None:
        return False
    return True


def _detect_intervention(page: Page) -> Optional[str]:
    """Return a short reason string if the page demands human action."""
    try:
        body = (page.content() or "").lower()
    except Exception:
        return None
    for kw in _INTERVENTION_KEYWORDS:
        if kw in body:
            return f"page contains '{kw}' — manual step required"
    return None


def _do_login(
    page: Page,
    site: str,
    audit_dir: str,
    user: str,
    password: str,
    email_selectors,
    password_selectors,
    submit_selectors,
) -> bool:
    print(f"[AUTH] {site}: logging in as {_mask_username(user)}")

    # If we're not on a login-shaped URL, try clicking a "Log In" button to
    # navigate there. The home page typically has one in the header.
    if not any(m in (page.url or "").lower() for m in _LOGIN_URL_MARKERS):
        login_link = _first_visible(page, _LOGGED_OUT_INDICATORS)
        if login_link is not None:
            try:
                login_link.click()
                page.wait_for_timeout(3000)
            except Exception as e:
                print(f"[AUTH] {site}: could not click header login link: {e}")

    email = _first_visible(page, email_selectors)
    if email is None:
        _safe_screenshot(page, audit_dir, f"{site}_email_input_missing")
        raise LoginError(f"{site}: email input not found on login page")

    pw = _first_visible(page, password_selectors)
    if pw is None:
        _safe_screenshot(page, audit_dir, f"{site}_password_input_missing")
        raise LoginError(f"{site}: password input not found on login page")

    try:
        email.click()
        email.fill("")
        email.type(user, delay=20)
        pw.click()
        pw.fill("")
        pw.type(password, delay=20)
    except Exception as e:
        _safe_screenshot(page, audit_dir, f"{site}_fill_failed")
        raise LoginError(f"{site}: failed to fill credentials: {e}")

    submit = _first_visible(page, submit_selectors)
    if submit is None:
        # Try Enter on password field.
        try:
            pw.press("Enter")
        except Exception as e:
            _safe_screenshot(page, audit_dir, f"{site}_submit_missing")
            raise LoginError(f"{site}: submit button not found and Enter failed: {e}")
    else:
        try:
            submit.click()
        except Exception as e:
            _safe_screenshot(page, audit_dir, f"{site}_submit_click_failed")
            raise LoginError(f"{site}: submit click failed: {e}")

    page.wait_for_timeout(6000)

    intervention = _detect_intervention(page)
    if intervention is not None:
        _safe_screenshot(page, audit_dir, f"{site}_intervention_required")
        raise LoginInterventionRequired(f"{site}: {intervention}")

    if not _looks_logged_in(page):
        _safe_screenshot(page, audit_dir, f"{site}_login_did_not_take")
        raise LoginError(
            f"{site}: still on a login-shaped URL after submit "
            f"(url={page.url[:120]!r}); check credentials and audit screenshots"
        )

    print(f"[AUTH] {site}: login successful")
    return True


def ensure_logged_in(page: Page, site: str, audit_dir: str) -> bool:
    """Warm up the book's home page; if not logged in, run the login flow.

    Idempotent. Returns True on success.

    Raises:
        LoginError: credentials missing, selectors broken, or login did not
          take. Treat as a normal execution failure — circuit breaker handles
          repeats.
        LoginInterventionRequired: 2FA / CAPTCHA / "press and hold" detected.
          Caller should escalate to CRITICAL.
    """
    if site not in _HOME_URLS:
        raise LoginError(f"unknown site: {site!r}")

    try:
        page.goto(_HOME_URLS[site], wait_until="domcontentloaded", timeout=30_000)
    except Exception as e:
        # Network blip or redirect storm. Treat as login failure so the
        # circuit breaker engages instead of hammering.
        raise LoginError(f"{site}: warm-up navigation failed: {e}")

    page.wait_for_timeout(2500)

    if _looks_logged_in(page):
        print(f"[AUTH] {site}: session valid (already logged in)")
        return True

    if site == "fanduel":
        user = os.getenv("FANDUEL_USERNAME")
        pw = os.getenv("FANDUEL_PASSWORD")
        if not user or not pw:
            raise LoginError("FANDUEL_USERNAME / FANDUEL_PASSWORD not set in .env")
        return _do_login(
            page, "fanduel", audit_dir, user, pw,
            _FD_EMAIL_SELECTORS, _FD_PASSWORD_SELECTORS, _FD_SUBMIT_SELECTORS,
        )

    user = os.getenv("BETMGM_USERNAME")
    pw = os.getenv("BETMGM_PASSWORD")
    if not user or not pw:
        raise LoginError("BETMGM_USERNAME / BETMGM_PASSWORD not set in .env")
    return _do_login(
        page, "betmgm", audit_dir, user, pw,
        _MGM_EMAIL_SELECTORS, _MGM_PASSWORD_SELECTORS, _MGM_SUBMIT_SELECTORS,
    )
