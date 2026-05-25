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
#
# `:not()` exclusions: BetMGM's Place Bet button (class `.place-button`,
# attr `vnhaptics="ms-betplacement-start"`) is a `<button>` element that
# the loose `button:has-text("Log In")` was matching when the slip
# happened to contain the substring — observed 2026-05-21 when the worker
# fired on the negative-ROI tail with active betslips on the page.
# Exclude it explicitly.
_LOGGED_OUT_INDICATORS = (
    'a:has-text("Log In")',
    'a:has-text("LOG IN")',
    'button:not(.place-button):not([vnhaptics*="betplacement"]):has-text("Log In")',
    'button:not(.place-button):not([vnhaptics*="betplacement"]):has-text("LOG IN")',
    'a:has-text("Sign In")',
    'button:not(.place-button):not([vnhaptics*="betplacement"]):has-text("Sign In")',
)

# FanDuel login-form selectors, tried in order. FD presents login as a
# modal overlay (not a separate URL), and the username field is typed as
# `input[type="text"]` with `name="email"` or `name="UserName"`.
_FD_EMAIL_SELECTORS = (
    'input[type="email"]',
    'input[name="email"]',
    'input[name="UserName"]',
    'input[name="userName"]',
    'input[name="username"]',
    'input[autocomplete="email"]',
    'input[autocomplete="username"]',
    'input[id*="email" i]',
    'input[id*="username" i]',
    'input[placeholder*="email" i]',
    'input[placeholder*="username" i]',
    '[role="dialog"] input[type="text"]',
    'form input[type="text"]:not([placeholder*="search" i])',
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
    # Scope to the login form/dialog first — most specific
    'form button[type="submit"]:has-text("Log In")',
    'form button[type="submit"]:has-text("LOGIN")',
    '[role="dialog"] button[type="submit"]:has-text("Log In")',
    '[role="dialog"] button:has-text("Log In")',
    # type=submit by itself rules out Angular Place Bet buttons (which
    # are plain <button> with no type=submit and never live in a <form>).
    'button[type="submit"]:has-text("Log In")',
    'button[type="submit"]:has-text("LOGIN")',
    # Loose text-match fallbacks, with explicit exclusion of Place Bet
    # (class `.place-button`, attr `vnhaptics="ms-betplacement-start"`)
    # which was the wrong match observed 2026-05-21.
    'button:not(.place-button):not([vnhaptics*="betplacement"]):has-text("Log In")',
    'button:not(.place-button):not([vnhaptics*="betplacement"]):has-text("LOGIN")',
    'form button[type="submit"]',
)

# Structural signals that a verification / CAPTCHA challenge is active.
# Substring matches on raw HTML gave false positives (BetMGM has "2fa" in
# marketing copy on logged-in pages) — these signals require an actual
# challenge element to be present in the DOM.
_INTERVENTION_STRUCTURAL = (
    ('input[autocomplete="one-time-code"]', "OTP input present"),
    ('input[inputmode="numeric"][maxlength="1"]', "single-digit OTP input present"),
    ('input[name*="otp" i]', "OTP-named input present"),
    ('input[name*="code" i]', "code-named input present"),
    ('input[name*="verification" i]', "verification-named input present"),
    ('input[id*="otp" i]', "OTP-id input present"),
    ('input[id*="code" i]', "code-id input present"),
    ('input[aria-label*="verification" i]', "verification aria input present"),
    ('input[aria-label*="code" i]', "code aria input present"),
    ('iframe[src*="recaptcha"]', "reCAPTCHA iframe present"),
    ('iframe[src*="hcaptcha"]', "hCaptcha iframe present"),
    ('iframe[src*="arkose"]', "Arkose challenge iframe present"),
    ('iframe[src*="funcaptcha"]', "FunCaptcha iframe present"),
)

# Visible-text phrases that are specific enough to indicate a live
# challenge, not just marketing copy.
_INTERVENTION_TEXT_PHRASES = (
    "press and hold",
    "we sent a code",
    "we sent you a code",
    "enter the code we sent",
    "enter your code",
    "verification code",
    "two-factor authentication required",
    "secure your account",
    "confirm it's you",
    "confirm its you",
    "verify it's you",
    "verify its you",
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


def _all_frames(page: Page):
    """Yield (label, frame_locator_target). The page itself plus each
    iframe — needed because FanDuel renders its login modal inside an
    iframe whose contents are invisible to top-level page.locator()."""
    yield ("main", page)
    for i, frame in enumerate(page.frames):
        if frame == page.main_frame:
            continue
        try:
            yield (f"frame[{i}]:{frame.url[:60]}", frame)
        except Exception:
            continue


def _dump_visible_inputs(page: Page, site: str, context: str) -> None:
    """Log every visible input on the page (and inside iframes) so we can
    update selectors with real data. Triggered only on failure paths."""
    for label, target in _all_frames(page):
        try:
            all_inputs = target.locator("input")
            dump = []
            for i in range(min(all_inputs.count(), 25)):
                elem = all_inputs.nth(i)
                try:
                    if not elem.is_visible():
                        continue
                    dump.append({
                        "type": elem.get_attribute("type"),
                        "name": elem.get_attribute("name"),
                        "id": elem.get_attribute("id"),
                        "autocomplete": elem.get_attribute("autocomplete"),
                        "placeholder": elem.get_attribute("placeholder"),
                    })
                except Exception:
                    continue
            if dump:
                print(f"[AUTH] {site}: inputs in {label} ({context}, {len(dump)}): {dump!r}")
        except Exception as e:
            print(f"[AUTH] {site}: dump in {label} failed: {e}")


def _first_visible(page: Page, selectors) -> Optional[Locator]:
    """Find the first visible match for any selector, scanning the main
    page and every iframe. FD's login form lives inside an iframe."""
    for label, target in _all_frames(page):
        for sel in selectors:
            try:
                loc = target.locator(sel)
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


def _modal_sweep_then_click(locator, *, page: Page, site: str,
                            attempts: int = 3) -> None:
    """Dismiss any blocking modal, then click ``locator``. Used in the
    auth flow where bare ``locator.click()`` gets intercepted by FD's
    Reality Check overlay — the ModalWatcher sweep in human.mouse.click
    doesn't apply here because auth doesn't go through that path.

    Calls ``check_all_active()`` (driving any registered ModalWatcher
    on this page; no-op if none registered) AND a direct
    ``_dismiss_blocking_modal`` for belt-and-suspenders coverage,
    then attempts the click. On a Playwright timeout (likely from a
    re-opened modal), sweeps again and retries up to ``attempts``.
    """
    # Lazy import to avoid a module-level cycle — human.modals doesn't
    # import auth, but importing eagerly here would shift module init
    # order on a clean tree.
    try:
        from human.modals import check_all_active
    except Exception:
        check_all_active = lambda: 0  # noqa: E731

    last_exc: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            check_all_active()
        except Exception:
            pass
        try:
            _dismiss_blocking_modal(page, site)
        except Exception:
            pass
        try:
            locator.click(timeout=10_000)
            return
        except Exception as e:
            last_exc = e
            # On retry, give the page a beat for the new modal state to
            # settle. Avoid sub-second sleeps that don't actually change
            # anything; the prior dismiss path waits 1s after named-name
            # clicks anyway.
            try:
                page.wait_for_timeout(500)
            except Exception:
                pass
            print(f"[AUTH] {site}: click attempt {attempt + 1} failed: {e}")
    if last_exc is not None:
        raise last_exc


def _dismiss_blocking_modal(page: Page, site: str) -> None:
    """Dismiss a non-credential modal that blocks login/header clicks.

    FanDuel can show a responsible-gambling modal before the login link is
    clickable. Do not close dialogs that already contain login fields; those
    are the credential form we need to fill.
    """
    try:
        modal = page.locator(
            'div[role="dialog"][aria-modal="true"], '
            '.ReactModal__Overlay.ReactModal__Overlay--after-open'
        )
        if modal.count() == 0 or not modal.first.is_visible():
            return

        if modal.first.locator("input").count() > 0:
            return

        buttons = modal.first.locator("button")
        if buttons.count() == 0:
            return

        # No "Close" — it's ambiguous (sometimes dismisses, sometimes
        # navigates back). Sole-button fallback below still handles
        # Close-only modals. Sweep #8 (2026-05-25) caught Close
        # detaching the email-input frame on an FD login interstitial.
        for label in (
            "Continue Playing", "I Understand", "Acknowledge",
            "Done", "Got it", "OK",
        ):
            try:
                named = modal.first.get_by_role("button", name=label)
                if named.count() > 0 and named.first.is_visible():
                    print(f"[AUTH] {site}: dismissing blocking modal via {label!r}")
                    named.first.click()
                    page.wait_for_timeout(1000)
                    return
            except Exception:
                continue

        if buttons.count() == 1 and buttons.first.is_visible():
            print(f"[AUTH] {site}: dismissing blocking modal via sole button")
            buttons.first.click()
            page.wait_for_timeout(1000)
    except Exception as e:
        print(f"[AUTH] {site}: modal-dismiss probe failed: {e}")


def _looks_logged_in(page: Page) -> bool:
    """Heuristic: not on a login URL AND no log-in button visible."""
    url = (page.url or "").lower()
    if any(marker in url for marker in _LOGIN_URL_MARKERS):
        return False
    if _first_visible(page, _LOGGED_OUT_INDICATORS) is not None:
        return False
    return True


def _detect_intervention(page: Page) -> Optional[str]:
    """Return a short reason string if the page demands human action.

    Conservative two-layer check:
      1. Structural — a verification input or challenge iframe is visible.
         Scans every frame (FanDuel renders the 2FA challenge in an iframe).
      2. Visible-text phrases specific enough that they cannot appear in
         marketing copy on a logged-in page (e.g. "we sent a code").

    Returns None when both layers are quiet — the caller's job is to then
    decide between "logged in" and "login failed (try again)".
    """
    for _label, target in _all_frames(page):
        for sel, reason in _INTERVENTION_STRUCTURAL:
            try:
                loc = target.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    return reason
            except Exception:
                continue

    for _label, target in _all_frames(page):
        try:
            text = (target.locator("body").inner_text() or "").lower()
        except Exception:
            continue
        for phrase in _INTERVENTION_TEXT_PHRASES:
            if phrase in text:
                return f"page shows '{phrase}'"
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
    _dismiss_blocking_modal(page, site)

    # If we're not on a login-shaped URL, try clicking a "Log In" button to
    # navigate there. The home page typically has one in the header.
    if not any(m in (page.url or "").lower() for m in _LOGIN_URL_MARKERS):
        login_link = _first_visible(page, _LOGGED_OUT_INDICATORS)
        if login_link is not None:
            try:
                _modal_sweep_then_click(login_link, page=page, site=site)
                page.wait_for_timeout(3000)
            except Exception as e:
                print(f"[AUTH] {site}: could not click header login link: {e}")

    email = _first_visible(page, email_selectors)
    if email is None:
        _dump_visible_inputs(page, site, "before email lookup")
        _safe_screenshot(page, audit_dir, f"{site}_email_input_missing")
        raise LoginError(f"{site}: email input not found on login page")

    pw = _first_visible(page, password_selectors)
    if pw is None:
        _dump_visible_inputs(page, site, "before password lookup")
        _safe_screenshot(page, audit_dir, f"{site}_password_input_missing")
        raise LoginError(f"{site}: password input not found on login page")

    try:
        _modal_sweep_then_click(email, page=page, site=site)
        email.fill("")
        email.type(user, delay=20)
        _modal_sweep_then_click(pw, page=page, site=site)
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
            _modal_sweep_then_click(submit, page=page, site=site)
        except Exception as e:
            _safe_screenshot(page, audit_dir, f"{site}_submit_click_failed")
            raise LoginError(f"{site}: submit click failed: {e}")

    page.wait_for_timeout(6000)

    # 2FA / CAPTCHA detection FIRST. _looks_logged_in is too lenient when
    # FD's 2FA modal overlays a partial home page (no Log In button to
    # see, no /login URL marker) — it would short-circuit to "logged in"
    # and we'd never raise the CRITICAL alert. The intervention detector
    # now uses tight signals (visible OTP input or specific text phrases)
    # so a false positive on a genuinely logged-in page is very unlikely.
    intervention = _detect_intervention(page)
    if intervention is not None:
        _safe_screenshot(page, audit_dir, f"{site}_intervention_required")
        raise LoginInterventionRequired(f"{site}: {intervention}")

    if _looks_logged_in(page):
        print(f"[AUTH] {site}: login successful")
        return True

    # Stuck-credential-modal detector. If the password input is STILL
    # visible 6s after submit, the modal didn't dismiss — usually because
    # the session/cookie state is corrupted on the persistent profile.
    # Treat as intervention (halt + page operator) rather than LoginError
    # (silent retry on the next opp), because retry-thrashing through a
    # stuck modal looks like a non-human attack pattern to risk teams and
    # burns Chrome cycles. Observed 2026-05-15 on BetMGM: modal stayed
    # 39s static across 27 frames while the bot kept clicking.
    if _first_visible(page, password_selectors) is not None:
        _safe_screenshot(page, audit_dir, f"{site}_credential_modal_stuck")
        raise LoginInterventionRequired(
            f"{site}: credential modal still visible 6s after submit — "
            f"refresh the {site} session in the bot's Chrome profile by "
            f"logging in manually, then restart the worker"
        )

    _safe_screenshot(page, audit_dir, f"{site}_login_did_not_take")
    raise LoginError(
        f"{site}: still on a login-shaped URL after submit "
        f"(url={page.url[:120]!r}); check credentials and audit screenshots"
    )


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
    _dismiss_blocking_modal(page, site)

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
