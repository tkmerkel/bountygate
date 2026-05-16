"""Structured error taxonomy for the arbitrage executor.

All custom exceptions raised during bet placement inherit from ``BetPlacerError``.
The subclasses exist so that upstream handlers — ``execute_arb.ArbExecutor``,
``task_worker.py``, and the Discord notifier — can make different decisions
based on the *kind* of failure instead of parsing string messages.

Status: SKELETON
----------------
This file currently defines the taxonomy only. The existing
``bet_placer.BetPlacerError`` remains the single exception type actually raised
throughout ``bet_placer.py``. A contractor should migrate the raises
incrementally, one call site at a time, with paper-trade validation between
changes. See the per-subclass ``MIGRATION`` notes for where each should be
adopted.

Severity -> Discord mapping
----------------------------
Each subclass declares a ``severity`` class attribute that the notifier can
read to pick a Discord prefix, matching the existing convention in
``arbitrage_executor/CLAUDE.md`` § "Operator runbook":

- ``"CRITICAL"`` -> 🚨  Orphaned bet possible. Halt worker, page operator.
- ``"WARNING"``  -> ⚠️  Opportunity skipped. No money at risk.
- ``"INFO"``     -> ℹ️  Notable event, non-error.

The skeleton intentionally does not wire severity into the notifier yet; that
is a follow-up refactor.
"""

from __future__ import annotations


class BetPlacerError(Exception):
    """Base class for all bet-placement failures.

    Any code that currently does ``except BetPlacerError`` will continue to
    work after subclasses are adopted — that's the whole point of keeping this
    as the root.
    """

    severity: str = "WARNING"


class SelectorMissing(BetPlacerError):
    """The market has no entry in ``selectors/{site}_markets.yaml``.

    This is a *configuration* failure, not a UI failure. The market was never
    mapped in the first place. Fix by running
    ``python map_selectors.py --site <site> --market <key>``.

    Severity: WARNING. Opportunity is skipped; no bet was placed on either
    leg.

    MIGRATION: raise this from ``execute_arb.py`` where the YAML lookup
    returns None / empty, and in ``bet_placer.py:86`` / similar spots where
    the executor asserts a market config is present before calling into
    Playwright.
    """

    severity = "WARNING"


class SelectorMatchedNothing(BetPlacerError):
    """A mapped selector ran against the live DOM and found 0 elements.

    This is the classic "UI changed" signal — the YAML was valid, the market
    is in scope, but the sportsbook redesigned or renamed the element. Fix
    by re-running ``map_selectors.py`` for the affected market (see SOP.md).

    Severity depends on which leg we're on:

    - If raised *before* the BetMGM leg places: WARNING (opportunity skipped).
    - If raised *after* BetMGM placed but while trying to hedge on FanDuel:
      CRITICAL (orphaned bet possible).

    Callers are responsible for upgrading to CRITICAL at the hedge site. The
    default declared here is WARNING.

    MIGRATION: replace raises in ``bet_placer.py`` at lines like 107, 253,
    274, 797, 840 where ``BetPlacerError`` currently reports "not found" /
    "Accordion not found" / "Place Bet button not found".
    """

    severity = "WARNING"


class MarketSuspended(BetPlacerError):
    """The bookmaker accepted the click but rejected the wager because the
    market is suspended, the line moved, or limits were hit.

    Not a selector problem. The YAML is fine. The book simply doesn't want
    this bet right now.

    Severity: WARNING if raised on BetMGM (first leg, no money committed).
    CRITICAL if raised on FanDuel after a successful BetMGM placement.

    MIGRATION: replace the ``return "REJECTED", msg`` branches in
    ``_place_bet_fanduel`` and ``_place_bet_betmgm`` with a raise of this
    exception, so the executor and notifier can react uniformly instead of
    inspecting tuple return values.
    """

    severity = "WARNING"


class BookRateLimited(BetPlacerError):
    """The bookmaker or its WAF (Cloudflare, Akamai) throttled or blocked
    requests. Continuing to hammer the site will only escalate the block.

    Symptoms: HTTP 429, Cloudflare 1020 Access Denied, repeated timeouts
    against selectors that are known-good, or geofence errors.

    Severity: WARNING (single-opportunity) rising to CRITICAL if the
    ``task_worker`` circuit-breaker sees it N times in a rolling window.

    MIGRATION: there is currently no code path that distinguishes rate-limit
    from generic timeout. A contractor adding this should detect
    ``PlaywrightTimeoutError`` combined with a page-content probe for
    Cloudflare / rate-limit text, then raise this instead of the generic
    timeout.
    """

    severity = "WARNING"


class UnknownUIState(BetPlacerError):
    """The bet was clicked but the bot cannot determine the outcome —
    no success message, no error message, no recognizable state.

    This is the most dangerous class: the bet may or may not have been
    placed. Current code returns ``"UNKNOWN", "Could not determine bet
    status"`` for this case in ``bet_placer.py:826, 877``. A contractor
    should raise this instead so the executor treats it as a potential
    orphan-bet situation.

    Severity: CRITICAL. Always. Operator must verify both books manually.

    MIGRATION: replace the ``return "UNKNOWN", ...`` branches in
    ``_place_bet_fanduel`` and ``_place_bet_betmgm``. Be careful — the
    ``ArbExecutor`` currently treats UNKNOWN as a halt-and-page state via
    tuple inspection, so swap in the exception and the tuple handling
    together.
    """

    severity = "CRITICAL"
