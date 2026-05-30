"""BetMGM humanized bet placer.

Rewrite of the legacy bet_placer_betmgm.py against the new
``arbitrage_executor/human/`` primitives. Public interface preserved
(see ``bet_placer.BetPlacer`` ABC).

Task 12 surface: ``__init__``, ``navigate_and_expand_market``,
``clear_betslip``, ``assert_betslip_has_bet``, ``assert_betslip_empty``.
Task 13a adds ``find_and_click_bet`` (+ alt/std dispatch helpers).
Wager/place/odds/limit-check land in Task 13b.
"""

import os
import re
from typing import Dict, Optional, Tuple

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from _bet_placer_helpers import (
    dump_miss_context,
    first_visible,
    with_screenshot_on_error,
)
from bet_placer import (
    BetPlacer,
    BetPlacerError,
    BetPlacerSkipError,
    ShadowAbortError,
)
from human.mouse import CursorState, click as mouse_click
from human.typing import TypingProfile, humanized_type
from human.waiting import settle, step_timer
from text_match import fuzzy_contains
from pick_matcher import parse_pick, select_unique, NoPickError


# Slip-pill regex selector — matches "Bet slip", "Bet slip (N)", and
# "N Bet slip" variants the prod page ships. Used three times: lazy
# short-circuit, post-clear verification, and slip-has-bet probe.
_SLIP_PILL_SELECTOR = (
    'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i'
)


def _pill_count(text: str) -> Optional[int]:
    """Parse the slip-pill text (e.g. ``"Bet slip (3)"`` / ``"3 Bet slip"``).
    Returns the parsed count, or None if the text doesn't match either form.
    """
    m = re.search(r"\((\d+)\)|^\s*(\d+)\s+Bet slip", text or "", re.I)
    if not m:
        return None
    return int(m.group(1) or m.group(2))


# Returns just the player name text for the row/group containing the given
# pick. Two BetMGM DOM shapes are handled, both scoped so they cannot leak an
# adjacent player's name (the bug fixed earlier where a loose walkup matched a
# wrapping container holding the full player list):
#
#   Shape A — legacy NBA player-prop rows: pick lives inside an
#   `.option-group-row` that also holds `.player-props-player-name`.
#
#   Shape B — MLB milestone O/U (e.g. "Batter doubles O/U"): there is NO row
#   wrapper. Inside `.option-group-container` the player name
#   (`.attribute-key.player-statistics` > `.group-title` > `.title`) and the
#   player's two `<ms-option>` picks are FLAT SIBLINGS. The owning name is the
#   nearest PRECEDING `.attribute-key.player-statistics` sibling of the pick's
#   `<ms-option>`. We stop at the first one, so it can't reach into the prior
#   player's group. (Confirmed against live DOM 2026-05-29 — Giants@Rockies
#   batter doubles; the Shape-A-only lookup returned null for all 103 picks and
#   no player ever matched.)
#
# In both shapes we read the name element's leading text only, so the
# "Avg: 0.2" stat suffix (a sibling/child node) is excluded.
_PLAYER_NAME_FROM_PICK_JS = """
(el) => {
    const leadingText = (node) => {
        if (!node) return '';
        let name = '';
        for (const child of node.childNodes) {
            if (child.nodeType === Node.TEXT_NODE) name += child.textContent;
        }
        name = name.trim();
        return name || (node.textContent || '').trim();
    };

    // Shape A: legacy NBA .option-group-row wrapper.
    const row = el.closest('.option-group-row');
    if (row) {
        const nameEl = row.querySelector('.player-props-player-name');
        const n = leadingText(nameEl);
        if (n) return n;
    }

    // Shape B: flat siblings inside .option-group-container — walk back from
    // this pick's <ms-option> to the nearest preceding player-name sibling.
    const opt = el.closest('ms-option') || el;
    let sib = opt.previousElementSibling;
    while (sib) {
        if (sib.matches && sib.matches('.attribute-key.player-statistics')) {
            const titleEl = sib.querySelector('.group-title .title')
                         || sib.querySelector('.title');
            const n = leadingText(titleEl);
            return n || null;
        }
        sib = sib.previousElementSibling;
    }
    return null;
}
"""


# Tight-walkup JS for non-NBA / unknown DOM shapes. max_text_len=150 keeps
# the scan inside a single player row; the prior 600-char cap allowed the
# wrapping container holding the full 10-player list to slip through, which
# is how every pick fuzzy-matched any target player and the first
# direction+line match always won.
_WALKUP_JS = """
(el, args) => {
    const max_depth = args.max_depth;
    const max_text_len = args.max_text_len;
    const out = [];
    let cur = el;
    for (let i = 0; i < max_depth && cur && cur.parentElement; i++) {
        cur = cur.parentElement;
        const text = (cur.innerText || cur.textContent || '').trim();
        if (text.length <= max_text_len) out.push(text);
    }
    return out;
}
"""


class BetmgmBetPlacer(BetPlacer):
    """Handles bet placement on BetMGM."""

    def __init__(self, page, site, audit_dir):
        super().__init__(page, site, audit_dir)
        # Per-session humanization state. CursorState carries cursor
        # position across calls so successive Bezier paths stitch
        # naturally; TypingProfile rotates daily so typing rhythm is
        # consistent within a day but drifts across days.
        self._cursor = CursorState()
        self._typing = TypingProfile.for_today()

    # ------------------------------------------------------------------
    # Static helpers — preserved from legacy unchanged (LOGIC.md hard rule)
    # ------------------------------------------------------------------

    @staticmethod
    def _alt_sibling_if_std_missing(accordion_name: str, visible_names):
        """Return the alt-merged sibling name if ``accordion_name`` is a
        " O/U"-suffixed std accordion AND its unsuffixed sibling is in
        ``visible_names``; else return None.

        Used to distinguish a structural-skip case (BetMGM doesn't ship
        the std O/U accordion on this event, but does ship the merged
        alt) from a genuine selector regression (YAML drift). See
        LOGIC.md.
        """
        if not accordion_name.endswith(" O/U"):
            return None
        sibling = accordion_name[: -len(" O/U")]
        target_norm = " ".join(sibling.lower().split())
        for name in visible_names:
            if " ".join((name or "").lower().split()) == target_norm:
                return sibling
        return None

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def navigate_and_expand_market(self, opportunity: Dict, market_config: Dict,
                                   direction: str = None) -> None:
        """Navigate BetMGM to the event and expand the market accordion.

        Four-phase orchestrator: load homepage (with auth probe + slip
        clear), search the betfinder for the event, navigate to the
        event page, then select the sub-tab + expand the accordion.
        Humanization (``settle()`` cadence, ``humanized_type``) lives
        in the phase methods.
        """
        home_team = opportunity['home_team']
        away_team = opportunity['away_team']
        sport = (opportunity.get('sport_title') or '').upper()
        accordion_name = market_config.get('accordion_name', '')
        is_alternate = (
            market_config.get('is_alternate', False)
            or market_config.get('has_threshold_tabs', False)
        )

        with step_timer("  p2_mgm_load_homepage"):
            self._load_betmgm_homepage(sport)
        with step_timer("  p2_mgm_search_event"):
            self._search_betmgm_for_event(home_team, sport)
        with step_timer("  p2_mgm_goto_event_page"):
            self._navigate_to_event_page_betmgm(home_team, away_team)
        with step_timer("  p2_mgm_select_sub_tab"):
            self._select_market_sub_tab_betmgm(market_config)
        with step_timer("  p2_mgm_expand_accordion"):
            self._expand_accordion_betmgm(accordion_name, is_alternate,
                                          opportunity, market_config, direction)

    def _load_betmgm_homepage(self, sport: str) -> None:
        """Goto the plain homepage, auth-probe, clear any leftover slip,
        then open the betfinder popup. The slip pill is only reachable
        from the plain homepage — the betfinder popup overlays it and
        would race a subsequent slip-clear; clear first, then open
        search."""
        print(f"[BETMGM] Loading homepage... (sport: {sport})")
        self.page.goto(
            "https://www.mo.betmgm.com/en/sports",
            wait_until="domcontentloaded",
        )
        settle(self.page, "page_load", rng=self._typing.rng)

        # Auth probe — fail loud if the session expired.
        try:
            login_link = self.page.locator(
                'a[href*="/login"]:has-text("Log in")'
            )
            if login_link.count() > 0 and login_link.first.is_visible():
                self._screenshot("session_expired")
                raise BetPlacerError(
                    "BetMGM session expired (Log in link visible on homepage)"
                )
        except BetPlacerError:
            raise
        except Exception as e:
            print(f"[BETMGM] ⚠ Auth probe failed: {e} (continuing — assuming logged in)")

        # Clean any leftover bets BEFORE opening the betfinder popup.
        self.clear_betslip()

        # Now open the betfinder popup for the actual team lookup.
        print(f"[BETMGM] Opening betfinder search...")
        self.page.goto(
            "https://www.mo.betmgm.com/en/sports?popup=betfinder",
            wait_until="domcontentloaded",
        )
        settle(self.page, "page_load", rng=self._typing.rng)

    def _search_betmgm_for_event(self, home_team: str, sport: str) -> None:
        """Humanized-type the home-team name into the betfinder search
        input, then trigger results. MLB goes via the autocomplete
        suggestion (BetMGM injects Futures markets that confuse Enter);
        other sports press Enter."""
        try:
            search_input = self.page.locator(
                'div.cdk-overlay-container input, '
                'input[placeholder*="Search"], '
                'input[placeholder*="Find"]'
            ).first
            search_input.wait_for(state="visible", timeout=10000)
            print(f"[BETMGM] Searching for: {home_team}")
            humanized_type(self.page, search_input, home_team, profile=self._typing)

            if sport == 'MLB':
                # MLB: click autocomplete suggestion (BetMGM shows Futures
                # that interfere with Enter).
                settle(self.page, "search_results", rng=self._typing.rng)
                suggestion_clicked = False
                try:
                    suggestions = self.page.locator(
                        'ms-search-suggestions-list-item'
                    )
                    suggestions.first.wait_for(state="visible", timeout=5000)
                    for i in range(suggestions.count()):
                        item = suggestions.nth(i)
                        item_text = (item.text_content() or "").lower()
                        if (home_team.lower() in item_text
                                and "future" not in item_text):
                            print(
                                f"[BETMGM] Clicking suggestion: "
                                f"{item_text.strip()[:60]}"
                            )
                            mouse_click(self.page, item, state=self._cursor,
                                        rng=self._typing.rng)
                            suggestion_clicked = True
                            settle(self.page, "search_results",
                                   rng=self._typing.rng)
                            break
                except Exception:
                    pass

                if not suggestion_clicked:
                    print(f"[BETMGM] No suggestion found, pressing Enter...")
                    settle(self.page, "pre_submit_dwell", rng=self._typing.rng)
                    self.page.keyboard.press("Enter")
                    settle(self.page, "search_results", rng=self._typing.rng)
            else:
                # NBA/NHL/NFL: standard Enter-based search.
                settle(self.page, "pre_submit_dwell", rng=self._typing.rng)
                self.page.keyboard.press("Enter")
                settle(self.page, "search_results", rng=self._typing.rng)
        except BetPlacerSkipError:
            raise
        except Exception as e:
            raise BetPlacerError(f"Search failed: {e}")

        # Detect "No results found" — BetMGM's betfinder doesn't index
        # every event on every regional site (mo.betmgm.com missed
        # Carolina Hurricanes NHL game on 2026-05-23, sweep #5). Without
        # this check the anchor-scan downstream falls back to stale
        # anchors from the previous page state and raises a misleading
        # "Could not find event link" error.
        try:
            no_results = self.page.get_by_text(
                "No results found", exact=False
            )
            if no_results.count() > 0 and no_results.first.is_visible():
                self._screenshot("betfinder_no_results")
                raise BetPlacerSkipError(
                    f"BetMGM betfinder: 'No results found' for "
                    f"{home_team!r} — event not indexed on this region. "
                    f"Skipping."
                )
        except BetPlacerSkipError:
            raise
        except Exception:
            pass  # diagnostic best-effort

    def _navigate_to_event_page_betmgm(self, home_team: str,
                                       away_team: str) -> None:
        """Resolve the event-page URL from the search-result anchors and
        goto it. BetMGM moved to whole-card anchors mid-2026 — scan
        every ``/sports/events/`` anchor and pick the one whose text +
        href slug covers both teams. After landing, append
        ``?market=PlayerProps`` if the URL doesn't already carry it."""
        with with_screenshot_on_error(
            self, "navigation_failed", "Event navigation failed"
        ):
            current_url = self.page.url.lower()
            home_slug = home_team.lower().replace(" ", "-")
            away_slug = away_team.lower().replace(" ", "-")

            if "/events/" in current_url and (
                home_slug in current_url or away_slug in current_url
            ):
                print(f"[BETMGM] Already on event page: {current_url}")
            else:
                anchors = self.page.locator('a[href*="/sports/events/"]')
                n = anchors.count()
                print(
                    f"[BETMGM] Scanning {n} event anchor(s) for "
                    f"{away_team} @ {home_team}"
                )

                target_href = None
                best_score = 0
                home_l = home_team.lower()
                away_l = away_team.lower()

                for i in range(n):
                    a = anchors.nth(i)
                    try:
                        text = (a.text_content() or "").lower()
                        href = a.get_attribute("href") or ""
                    except Exception:
                        continue
                    if "future" in text:
                        continue
                    score = int(home_l in text) + int(away_l in text)
                    href_l = href.lower()
                    if home_slug in href_l:
                        score += 1
                    if away_slug in href_l:
                        score += 1
                    if score > best_score:
                        best_score = score
                        target_href = href

                if not target_href or best_score < 2:
                    self._screenshot("event_link_not_found")
                    raise BetPlacerError(
                        f"Could not find event link: {away_team} @ "
                        f"{home_team} (scanned {n} anchors, "
                        f"best score={best_score})"
                    )

                # Direct goto, not click_through. BetMGM's betfinder
                # results render inside a modal overlay that intercepts
                # pointer events on the underlying event anchors —
                # force=True bypasses Playwright's check but the
                # browser still routes the click to the overlay, so
                # the nav never fires. Cost sweep #3 17 failures on
                # 2026-05-23.
                event_url = (
                    target_href
                    if target_href.startswith("http")
                    else "https://www.mo.betmgm.com" + target_href
                )
                # Brief "reading the results" beat before nav.
                settle(self.page, "reading_panel", rng=self._typing.rng)
                self.page.goto(event_url, wait_until="domcontentloaded")
                settle(self.page, "page_load", rng=self._typing.rng)

            # Add ?market=PlayerProps to land on the full props view.
            current_url = self.page.url
            if "market=PlayerProps" not in current_url:
                new_url = current_url + (
                    "&" if "?" in current_url else "?"
                ) + "market=PlayerProps"
                print(f"[BETMGM] Navigating to player props: {new_url}")
                self.page.goto(new_url, wait_until="domcontentloaded")
                settle(self.page, "page_load", rng=self._typing.rng)

    def _select_market_sub_tab_betmgm(self, market_config: Dict) -> None:
        """Click a market sub-tab (e.g. 'Combo stats') if configured.

        No-op if the market has no ``sub_tab_label``.
        """
        sub_tab = market_config.get("sub_tab_label")
        if not sub_tab:
            return
        print(f"[BETMGM] Selecting market sub-tab: {sub_tab}")
        sub_tab_loc = first_visible(
            self.page,
            [
                f'div[role="tablist"] button:has-text("{sub_tab}")',
                f'[role="tab"]:has-text("{sub_tab}")',
                f'button:has-text("{sub_tab}")',
            ],
            label=f"Sub-tab '{sub_tab}'",
            site=self.site,
        )
        if sub_tab_loc is not None:
            mouse_click(self.page, sub_tab_loc, state=self._cursor,
                        rng=self._typing.rng)
            settle(self.page, "ui_expansion", rng=self._typing.rng)
            print(f"[BETMGM] ✓ Sub-tab '{sub_tab}' selected")
            self._screenshot("sub_tab_selected")
            return
        self._screenshot("sub_tab_not_found")
        raise BetPlacerError(f"Could not find BetMGM sub-tab '{sub_tab}'")

    def _expand_accordion_betmgm(self, accordion_name: str, is_alternate: bool,
                                 opportunity: Dict, market_config: Dict,
                                 direction: str) -> None:
        """Locate the accordion by exact text, expand it, raise SkipError
        when only the merged-alt sibling is visible (LOGIC.md)."""
        with with_screenshot_on_error(
            self, "accordion_expansion_failed", "Accordion expansion failed"
        ):
            print(f"[BETMGM] Expanding accordion: {accordion_name}")
            # Exact-text match — :has-text would match the alt sibling
            # by substring and silently expand the wrong accordion.
            exact_selector = (
                f'button[dsaccordiontoggle]:text-is("{accordion_name}")'
            )
            accordion = self.page.locator(exact_selector)

            target = None
            if accordion.count() > 0:
                target = accordion.first
            else:
                # BetMGM ships its accordion list in two render waves:
                # merged-alt accordions ('Player assists', 'Player
                # points', etc.) first, then the std O/U variants
                # ('Player assists O/U', etc.) further down the page a
                # moment later. The settle('page_load') above is a
                # plain sleep and can fire BEFORE the std wave lands,
                # so the count() check above misses it even though the
                # accordion is about to render. Sweep #10 (2026-05-25)
                # raised a false BetPlacerSkipError on 'Player assists
                # O/U' on Cavs@Knicks for exactly this reason — the
                # accordion was at DOM idx 50 after the page finished
                # rendering, but the bot scanned before it appeared.
                #
                # Wait up to 6s for the exact target to attach. If it
                # arrives, use it. If it doesn't, fall through to the
                # iterate-and-skip-or-loud-fail path below — which is
                # the right path for genuinely missing std accordions.
                try:
                    accordion.first.wait_for(state="attached", timeout=6000)
                    if accordion.count() > 0:
                        target = accordion.first
                except Exception:
                    pass

            if target is None:
                # No fuzzy fallback — iterate visible accordions with a
                # normalized exact match, then on miss either skip
                # (alt sibling present) or raise loud (real drift).
                need_norm = " ".join((accordion_name or "").lower().split())
                visible_names: list = []
                for btn in self.page.locator('button[dsaccordiontoggle]').all():
                    try:
                        btn_text = (btn.text_content() or "").strip()
                    except Exception:
                        continue
                    if not btn_text:
                        continue
                    visible_names.append(btn_text)
                    if " ".join(btn_text.lower().split()) == need_norm:
                        target = btn
                        break

                if target is None:
                    # Structural skip: std O/U missing, merged-alt sibling
                    # present → SkipError (worker classifies as SKIPPED).
                    alt_sibling = self._alt_sibling_if_std_missing(
                        accordion_name, visible_names
                    )
                    if alt_sibling is not None:
                        raise BetPlacerSkipError(
                            f"BetMGM std accordion {accordion_name!r} "
                            f"not on this event; merged-alt accordion "
                            f"{alt_sibling!r} is present but Yes-only, "
                            f"so a std×std arb is unbettable here. "
                            f"Skipping (see LOGIC.md)."
                        )
                    self._screenshot("accordion_not_found")
                    raise BetPlacerError(
                        f"BetMGM accordion not found: {accordion_name!r}. "
                        f"Visible ({len(visible_names)}): "
                        f"{sorted(set(visible_names))!r}. "
                        f"Update selectors/betmgm_markets.yaml to match one "
                        f"of these."
                    )

            # Accordion buttons toggle — skip the click if already expanded.
            try:
                already_expanded = (
                    target.get_attribute("aria-expanded") == "true"
                )
            except Exception:
                already_expanded = False
            if already_expanded:
                print(f"[BETMGM] Accordion already expanded; skipping click.")
            else:
                mouse_click(self.page, target, state=self._cursor,
                            rng=self._typing.rng)
                settle(self.page, "ui_expansion", rng=self._typing.rng)

            # Fast-fail: wait for at least one ms-event-pick row.
            try:
                self.page.wait_for_selector(
                    "ms-event-pick", timeout=5000, state="visible",
                )
            except PlaywrightTimeoutError as e:
                self._screenshot("betmgm_accordion_empty")
                raise BetPlacerError(
                    "BetMGM accordion expanded but no ms-event-pick rows "
                    "in 5s — likely wrong sub-tab, market not offered, or "
                    "stuck search overlay"
                ) from e

            # Click "Show More" until all players visible.
            self._click_show_more_repeatedly_betmgm()

            print(f"[BETMGM] ✓ Market expanded")
            self._screenshot("market_expanded")

            # Alt markets: select the threshold tab.
            if is_alternate and direction:
                self._select_alternate_tab_betmgm(opportunity, market_config,
                                                  direction)

    # Selector cascade for the BetMGM "Show more" / "Show More" pagination
    # button at the bottom of a player-list accordion. Tried in order;
    # first match wins. The first entry (``ms-option-panel-bottom-action``)
    # was the historical wrapper element; BetMGM's Angular bundle drops
    # it intermittently and the role-only / button-only fallbacks rescue
    # us when that happens. Without this, the player list stays paginated
    # at ~11 visible rows and players past the fold never get scanned.
    _SHOW_MORE_SELECTORS = (
        'ms-option-panel-bottom-action:has-text("Show More")',
        'ms-option-panel-bottom-action:has-text("Show more")',
        'button:has-text("Show More")',
        'button:has-text("Show more")',
        '[role="button"]:has-text("Show More")',
        '[role="button"]:has-text("Show more")',
    )

    def _click_show_more_repeatedly_betmgm(self, *, max_attempts: int = 5) -> int:
        """Click whichever "Show more" pagination button is currently on
        the page, up to ``max_attempts`` times. Returns the click count
        for diagnostics.

        Each iteration re-probes the selector cascade because BetMGM
        sometimes re-renders the wrapper between expansions.
        """
        clicks = 0
        for _ in range(max_attempts):
            matched_selector: Optional[str] = None
            show_more = None
            for sel in self._SHOW_MORE_SELECTORS:
                try:
                    loc = self.page.locator(sel)
                    if loc.count() == 0:
                        continue
                    if not loc.first.is_visible():
                        continue
                    show_more = loc.first
                    matched_selector = sel
                    break
                except Exception:
                    continue
            if show_more is None:
                break
            try:
                print(f"[BETMGM] Show more via: {matched_selector}")
                mouse_click(self.page, show_more, state=self._cursor,
                            rng=self._typing.rng)
                settle(self.page, "ui_expansion", rng=self._typing.rng)
                clicks += 1
            except Exception as e:
                print(f"[BETMGM] Show more click failed ({matched_selector}): {e}")
                break
        if clicks == 0:
            print("[BETMGM] No 'Show more' button found — list may be fully expanded")
        return clicks

    def _select_alternate_tab_betmgm(self, opportunity: Dict,
                                     market_config: Dict,
                                     direction: str) -> None:
        """Select the alt threshold tab (e.g. ``5+``) matching the line.
        Best-effort — logs and continues on miss; the find/click path is
        what ultimately raises if the wrong picks rendered.
        """
        from selector_finder import calculate_alternate_tab_value
        line = (opportunity.get('over_line') if direction == 'over'
                else opportunity.get('under_line'))
        if line is None:
            print(f"[BETMGM] ⚠ No line for direction {direction}; skip tab")
            return
        tab_value = calculate_alternate_tab_value(line)
        tab_text = f"{tab_value}+"
        print(f"[BETMGM] Selecting alternate tab: {tab_text} (line: {line})")

        tab_selectors = [
            f'button:has-text("{tab_text}")',
            f'[role="tab"]:has-text("{tab_text}")',
            f'div[role="tablist"] button:has-text("{tab_text}")',
        ]
        pattern = market_config.get('tab_selector_pattern')
        if pattern:
            tab_selectors.append(pattern.format(threshold=tab_value))

        tab_loc = first_visible(
            self.page,
            tab_selectors,
            label="Found tab",
            site=self.site,
        )
        if tab_loc is not None:
            mouse_click(self.page, tab_loc, state=self._cursor,
                        rng=self._typing.rng)
            settle(self.page, "ui_expansion", rng=self._typing.rng)
            self._screenshot("alternate_tab_selected")

            # Log which threshold tab is currently active so the audit
            # captures it. Pre-fix, a 4-player "No bet found" sweep on
            # the Knicks game (sweep #5) couldn't distinguish "wrong
            # threshold scanned" from "player not at this threshold".
            # If we wanted "3+" but the page settled on "2+", the
            # downstream pick scan silently looks at the wrong picks.
            try:
                active = self.page.locator(
                    '[role="tab"][aria-selected="true"], '
                    'button[aria-selected="true"], '
                    'button.active, button.selected'
                )
                active_names = []
                for i in range(min(active.count(), 5)):
                    txt = (active.nth(i).text_content() or "").strip()
                    if txt:
                        active_names.append(txt[:30])
                print(f"[BETMGM] Active tabs after click: {active_names!r} "
                      f"(wanted '{tab_text}')")
                if active_names and not any(tab_text in n for n in active_names):
                    print(f"[BETMGM] ⚠ '{tab_text}' not in active set — "
                          f"pick scan may target wrong threshold")
            except Exception:
                pass

            # Re-expand the player list after tab switch.
            self._click_show_more_repeatedly_betmgm()
            return
        print(f"[BETMGM] ⚠ Could not find tab '{tab_text}'; continuing")
        self._screenshot("alternate_tab_not_found")

    # ------------------------------------------------------------------
    # Slip clearing — lazy fast-path
    # ------------------------------------------------------------------

    def clear_betslip(self) -> None:
        """Empty the BetMGM betslip; fail if it remains non-empty.

        **Lazy fast-path:** the very first action is the cheap pill read.
        If the pill reads ``(0)`` / ``0 Bet slip``, return immediately —
        no slip open, no sweep, no extra settle. Stale-bet cleanup only
        runs when the pill says we actually have items to clear.
        """
        # Fast-path: pill==0 → done.
        try:
            slip_pill = self.page.locator(_SLIP_PILL_SELECTOR)
            if slip_pill.count() > 0:
                pill_text = (slip_pill.first.text_content() or "").strip()
                count = _pill_count(pill_text)
                if count == 0:
                    print(f"[BETMGM] Slip already empty.")
                    return
        except Exception:
            # If the probe blew up, fall through to the full clear path.
            pass

        # Full clear dance — slip is (probably) non-empty.
        try:
            self._open_betmgm_slip()

            clicked_clear_all = False
            clear_loc = first_visible(
                self.page,
                [
                    'span:has-text("Clear All")',
                    'button:has-text("Clear All")',
                    'button:has-text("Remove all")',
                    'button:has-text("Clear all")',
                    'button[aria-label*="remove all" i]',
                    'div[role="button"]:has-text("Clear All")',
                    '[role="button"]:has-text("Clear All")',
                ],
                label="Clearing slip",
                site=self.site,
            )
            if clear_loc is not None:
                mouse_click(self.page, clear_loc, state=self._cursor,
                            rng=self._typing.rng)
                settle(self.page, "slip_update", rng=self._typing.rng)
                # Confirm-dialog dismissal — some BetMGM flows
                # gate Clear All behind a Yes/Remove/Confirm.
                confirm_loc = first_visible(
                    self.page,
                    [
                        'button:has-text("Yes")',
                        'button:has-text("Remove")',
                        'button:has-text("Confirm")',
                    ],
                )
                if confirm_loc is not None:
                    mouse_click(self.page, confirm_loc, state=self._cursor,
                                rng=self._typing.rng)
                    settle(self.page, "modal_dismiss", rng=self._typing.rng)
                clicked_clear_all = True

            # Per-bet remove sweep if Clear All wasn't available.
            if not clicked_clear_all:
                for _ in range(10):
                    removed = False
                    for sel in (
                        'bs-bet-slip-item button[aria-label*="remove" i]',
                        'button[aria-label*="remove" i]',
                        'button[aria-label*="delete" i]',
                    ):
                        try:
                            loc = self.page.locator(sel)
                            n = loc.count()
                            for i in range(n):
                                cand = loc.nth(i)
                                try:
                                    if cand.is_visible():
                                        mouse_click(self.page, cand,
                                                    state=self._cursor,
                                                    rng=self._typing.rng)
                                        settle(self.page, "slip_update",
                                               rng=self._typing.rng)
                                        removed = True
                                        break
                                except Exception:
                                    continue
                            if removed:
                                break
                        except Exception:
                            continue
                    if not removed:
                        break
            print(f"[BETMGM] Slip cleared (best-effort).")
        except Exception as e:
            print(f"[BETMGM] ⚠ Slip clear failed: {e} (continuing).")

        # Post-clear verification: pill must read 0 (or be absent).
        try:
            slip_pill = self.page.locator(_SLIP_PILL_SELECTOR)
            if slip_pill.count() > 0:
                pill_text = (slip_pill.first.text_content() or "").strip()
                count = _pill_count(pill_text)
                if count is not None and count > 0:
                    raise BetPlacerError(
                        f"BetMGM slip-clear failed: pill still reads "
                        f"{pill_text!r}"
                    )
        except BetPlacerError:
            raise
        except Exception:
            pass

    def _open_betmgm_slip(self) -> None:
        """Click the bottom slip pill to expand the slip panel.

        Idempotent — if a stake input is already visible, return.
        """
        try:
            if first_visible(
                self.page,
                [
                    'app-stake-input input',
                    'bs-stake-input input',
                    'input[inputmode="decimal"]',
                ],
            ) is not None:
                return

            for sel in (
                'div:has-text("pays out")',
                'span:has-text("pays out")',
                '*[role="button"]:has-text("Bet slip")',
                'button:has-text("Bet slip")',
                'a:has-text("Bet slip")',
                'span:has-text("Bet slip")',
            ):
                try:
                    loc = self.page.locator(sel)
                    if loc.count() == 0:
                        continue
                    for i in range(min(loc.count(), 5)):
                        cand = loc.nth(i)
                        try:
                            if cand.is_visible():
                                print(f"[BETMGM] Opening slip via {sel}")
                                mouse_click(self.page, cand,
                                            state=self._cursor,
                                            rng=self._typing.rng)
                                settle(self.page, "ui_expansion",
                                       rng=self._typing.rng)
                                return
                        except Exception:
                            continue
                except Exception:
                    continue
            print("[BETMGM] ⚠ No slip-pill affordance found; continuing.")
        except Exception as e:
            print(f"[BETMGM] ⚠ Slip-open probe failed: {e} (continuing)")

    # ------------------------------------------------------------------
    # Slip assertions
    # ------------------------------------------------------------------

    def assert_betslip_has_bet(self) -> None:
        """Assert a selected bet actually reached the slip."""
        self._open_betmgm_slip()
        if not self._betmgm_slip_has_bet():
            self._screenshot("validation_slip_empty")
            raise BetPlacerError("BetMGM slip is empty after bet click")

    def assert_betslip_empty(self) -> None:
        """Assert the slip is empty after cleanup."""
        self._open_betmgm_slip()
        if self._betmgm_slip_has_bet():
            self._screenshot("validation_slip_not_empty")
            raise BetPlacerError("BetMGM slip still appears to contain a bet")

    def _betmgm_slip_has_bet(self) -> bool:
        """Return True if BetMGM's slip appears to contain at least one bet.
        Conservative on ambiguous states.
        """
        try:
            for text in ("No bet selections", "Betslip empty"):
                empty_marker = self.page.get_by_text(text, exact=False)
                if (empty_marker.count() > 0
                        and empty_marker.first.is_visible()):
                    return False
        except Exception:
            pass
        try:
            slip_pill = self.page.locator(_SLIP_PILL_SELECTOR)
            if slip_pill.count() > 0:
                pill_text = (slip_pill.first.text_content() or "").strip()
                count = _pill_count(pill_text)
                if count is not None:
                    return count > 0
        except Exception:
            pass
        try:
            for sel in (
                'bs-bet-slip-item',
                'bs-betslip-item',
                'button[aria-label*="remove" i]',
                'button[aria-label*="delete" i]',
                'span:has-text("Clear All")',
            ):
                loc = self.page.locator(sel)
                for i in range(min(loc.count(), 5)):
                    try:
                        if loc.nth(i).is_visible():
                            return True
                    except Exception:
                        continue
        except Exception:
            pass
        return False

    # ------------------------------------------------------------------
    # Find + click — Task 13a
    # ------------------------------------------------------------------

    @staticmethod
    def _player_name_for_pick(pick) -> Optional[str]:
        """Return the player name from the ``.option-group-row`` containing
        ``pick``, or None if the BetMGM player-row DOM shape isn't present
        (e.g., non-NBA accordions, or DOM changed). Scoped to a single row,
        so it cannot leak adjacent players' names — this is the primary
        defense against the cross-row matching bug where a loose ancestor
        walkup matched the wrapping container that held all 10 players."""
        try:
            name = pick.evaluate(_PLAYER_NAME_FROM_PICK_JS)
            if not isinstance(name, str):
                return None
            return name.strip() or None
        except Exception:
            return None

    @staticmethod
    def _nearby_row_texts_for_pick(pick) -> list:
        """Best-effort sibling/nearby row texts for ``pick`` when the
        primary row-player resolution returns None. Augments the ancestor
        walkup for unusual DOM shapes. Returns an empty list on any failure
        (the ancestor walkup still carries the fuzzy player match)."""
        try:
            texts = pick.evaluate(_WALKUP_JS,
                                  {"max_depth": 8, "max_text_len": 150})
            return list(texts or [])
        except Exception:
            return []

    def _accordion_root_locator(self, accordion_name: str):
        """Return a Locator scoped to the ``ds-accordion`` whose toggle
        button text equals ``accordion_name`` (normalized).

        Why this exists instead of a plain string selector: Playwright's
        ``:text-is()`` and ``:text-matches("^X$")`` engines don't match
        BetMGM's toggle buttons reliably — the button's rendered text
        contains hidden child content (avatars, period chips, etc.) that
        breaks Playwright's text normalization. We iterate
        ``button[dsaccordiontoggle]`` and filter by normalized
        ``text_content`` on the Python side, then walk up to the
        surrounding ``ds-accordion`` via ``xpath=ancestor::``. Picks
        scoped to that accordion are guaranteed to live inside one
        market panel, not bleed across siblings.

        Returns ``None`` if no matching button is found.
        """
        need_norm = " ".join((accordion_name or "").lower().split())
        candidates = self.page.locator(
            f'button[dsaccordiontoggle]:has-text("{accordion_name}")'
        )
        try:
            count = candidates.count()
        except Exception:
            return None
        for i in range(count):
            try:
                txt = (candidates.nth(i).text_content() or "").strip()
            except Exception:
                continue
            if " ".join(txt.lower().split()) == need_norm:
                return candidates.nth(i).locator(
                    'xpath=ancestor::ds-accordion[1]'
                )
        return None

    def _detect_pick_format(self, accordion_name: str) -> str:
        """Inspect the first few picks in the panel to decide ``'std'``
        vs ``'alt'``.

        Returns ``'std'`` if any pick starts with ``O `` or ``U `` (a
        line is in the pick text); ``'alt'`` otherwise. ``'std'`` is the
        default when no picks are visible — that route raises a useful
        downstream error if the accordion is genuinely empty, while
        defaulting to ``'alt'`` would silently misclick under direction
        on a panel that just hadn't rendered yet.
        """
        acc = self._accordion_root_locator(accordion_name)
        if acc is None:
            print(f"[BETMGM] _detect_pick_format: no accordion match for "
                  f"{accordion_name!r}; defaulting to 'std'")
            return 'std'
        picks = acc.locator('ms-event-pick')
        n = picks.count()
        if n == 0:
            print(f"[BETMGM] _detect_pick_format: 0 picks inside "
                  f"{accordion_name!r}; defaulting to 'std'")
            return 'std'
        # Sample up to 5 picks; even one std-format pick is enough to
        # commit to the std path (NHL panels rarely mix formats).
        for i in range(min(n, 5)):
            try:
                txt = (picks.nth(i).text_content() or "").strip()
                norm = " ".join(txt.split())
                if re.match(r'^[OU]\s', norm):
                    return 'std'
            except Exception:
                continue
        return 'alt'

    def find_and_click_bet(self, opportunity, direction, market_config):
        """Find and click the bet for the specified player/line/direction.

        Dispatches between two pick-matching strategies based on what
        BetMGM actually rendered in the (already-expanded) accordion
        panel:

        * **std O/U** — picks read ``O 11.5 1.92`` / ``U 11.5 1.92``.
          The pick's own text contains the line, so we filter to the
          target line first, then walk up to find the matching player
          row. Covers NHL, MLB std, NFL, and NBA Player blocks /
          quarter markets.

        * **alt Yes-only** — picks read ``Yes 1.07`` (or just a price
          for some Yes/No markets). No line in the pick text — there's
          one ``Yes`` pick per player row at the currently-selected
          threshold tab. Find the matching player row and click its
          lone pick.

        ``has_threshold_tabs: true`` in the market config is a *hint*
        that the alt path is plausible, but we still confirm by
        inspecting the rendered picks. NHL player_points sets this
        flag too (the threshold tab click degrades gracefully when
        there's no ``5+`` tab), and the panel comes up showing std
        O/U picks — we'd misclick if we blindly trusted the flag.
        """
        player_name = opportunity['player_name']
        line = (opportunity['over_line'] if direction == 'over'
                else opportunity['under_line'])
        accordion_name = market_config.get('accordion_name', '')

        print(f"[BETMGM] Finding bet: {player_name} {direction} {line}")

        pick_format = 'std'
        if (market_config.get('has_threshold_tabs')
                or market_config.get('is_alternate')):
            pick_format = self._detect_pick_format(accordion_name)
            print(f"[BETMGM] Detected pick format: {pick_format!r}")

        if pick_format == 'alt':
            if direction != 'over':
                # BetMGM alt-only accordions ship one Yes pick per row
                # (= "achieves the threshold"). There is no symmetric
                # "No" pick, so under-direction can't be expressed here.
                # If the arb pipeline produced such an opp, something's
                # wrong upstream — fail loud rather than misclick.
                self._screenshot("alt_under_direction")
                raise BetPlacerError(
                    f"BetMGM alt-only accordion can't take "
                    f"direction={direction!r}; only 'over' (Yes pick) is "
                    f"supported. Market: {accordion_name!r}"
                )
            clicked = self._click_betmgm_alt_yes_pick_for_player(
                player_name, accordion_name
            )
        else:
            clicked = self._click_betmgm_pick_for_player(
                player_name, line, direction
            )

        if clicked:
            # Slip-phase pin: intentionally clobbers the orchestrator's
            # per-session viewport noise from viewport_from_cdp. Below
            # ~958px wide, BetMGM flips to a mobile-takeover slip layout
            # where "Clear All" lives in a position the placer's
            # selectors miss; 1920x945 is the smallest known-good
            # desktop layout. The navigation-phase nudge applied earlier
            # still carries most of the cross-session fingerprint
            # variability.
            print(f"[BETMGM] Pinning viewport to 1920x945 for slip phase...")
            self.page.set_viewport_size({"width": 1920, "height": 945})
            settle(self.page, "micro_pause", rng=self._typing.rng)
            return True

        # Miss-path diagnostic + raise (mirrors legacy lines 686-734).
        dump_miss_context(
            self.page,
            site=self.site,
            player_name=player_name,
            extra_locators=[
                (
                    "ms-event-pick elements",
                    f'ms-event-pick:has-text("{player_name}")',
                ),
            ],
        )
        self._screenshot("bet_not_found")
        # Hold the page in view long enough for the recording to capture
        # what BetMGM actually shipped — the bot otherwise navigates away
        # within milliseconds and the watcher can't verify whether the
        # player/market was on the page or whether the bot just missed it.
        # 5s at the top, then scroll to bottom, then 5s at the bottom
        # surfaces both the visible accordion and any below-fold rows
        # (e.g. virtual-scrolled picks that only render once they enter
        # the viewport).
        try:
            self.page.wait_for_timeout(5000)
            self.page.evaluate(
                "window.scrollTo(0, document.body.scrollHeight)"
            )
            self.page.wait_for_timeout(5000)
        except Exception:
            pass
        self._screenshot("bet_not_found_after_scroll")
        raise BetPlacerError(
            f"No bet found for {player_name} {direction} {line}"
        )

    def _click_betmgm_pick_for_player(self, player_name: str, line: float,
                                      direction: str) -> bool:
        """Find and click the BetMGM ms-event-pick that matches this
        player+line+direction.

        BetMGM's player-prop rows lay out as:
            [avatar][player name][stat avg][chart icon][ms-event-pick: "O 11.5  2.00"]

        The bet button (``ms-event-pick``) text contains only the direction
        letter + line + odds (e.g. "O 11.5"). The player name lives in a
        sibling/ancestor element.

        Strategy: enumerate every ms-event-pick on the page, check its text
        matches ``"<O|U> <line>"``, then walk up the DOM and pull the
        innerText of each ancestor row container. The player-name match runs
        Python-side via ``fuzzy_contains`` so apostrophe variants
        (curly vs straight: "De'Aaron" vs "De'Aaron"), abbreviation forms,
        and case differences all resolve cleanly.

        Returns True on a successful click, False if no match found.
        """
        # Best-effort: coax virtual-scroll / lazy-load picks into the DOM.
        try:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            settle(self.page, "micro_pause", rng=self._typing.rng)
            self.page.evaluate("window.scrollTo(0, 0)")
            settle(self.page, "micro_pause", rng=self._typing.rng)
        except Exception:
            pass

        all_picks = self.page.locator("ms-event-pick")
        pick_count = all_picks.count()
        print(f"[BETMGM] scanning {pick_count} ms-event-pick(s) for "
              f"{direction} {line} on player {player_name!r}")

        # Collect (pick, text) for picks that belong to the target player's
        # row. Player name is the ONLY fuzzy match; line/side is decided
        # deterministically by select_unique below.
        player_picks = []
        for i in range(pick_count):
            try:
                pick = all_picks.nth(i)
                txt = " ".join((pick.text_content() or "").split())

                row_player = self._player_name_for_pick(pick)
                if row_player is not None:
                    if not fuzzy_contains(row_player, player_name, threshold=90):
                        continue
                else:
                    ancestor_texts = pick.evaluate(
                        _WALKUP_JS, {"max_depth": 8, "max_text_len": 150}
                    )
                    row_texts = list(ancestor_texts or [])
                    row_texts.extend(self._nearby_row_texts_for_pick(pick))
                    if not any(fuzzy_contains(t, player_name, threshold=90)
                               for t in row_texts):
                        continue

                player_picks.append((pick, txt))
            except Exception as e:
                print(f"[BETMGM] pick #{i} scan error: {e}")
                continue

        try:
            matched = select_unique(player_picks, line, direction)
        except NoPickError:
            # Not found for this player — let find_and_click_bet emit the full
            # miss diagnostics (dump_miss_context + screenshots) and raise.
            print(f"[BETMGM] no unique pick for {player_name!r} {direction} "
                  f"{line}; parsed row picks: "
                  f"{[t for _, t in player_picks]}")
            return False
        # AmbiguousPickError intentionally propagates — refuse to guess.

        matched_option_id = matched.get_attribute("data-test-option-id")
        matched_text = " ".join((matched.text_content() or "").split())
        print(f"[BETMGM] matched bet: text={matched_text!r} "
              f"option_id={matched_option_id!r}")

        with with_screenshot_on_error(
            self, "click_failed", "Failed to click BetMGM bet"
        ):
            if matched_option_id:
                target = self.page.locator(
                    f'ms-event-pick[data-test-option-id="{matched_option_id}"]'
                )
                mouse_click(self.page, target.first, state=self._cursor,
                            rng=self._typing.rng)
            else:
                mouse_click(self.page, matched, state=self._cursor,
                            rng=self._typing.rng)
            settle(self.page, "slip_update", rng=self._typing.rng)
            self._screenshot("bet_clicked")
            print(f"[BETMGM] ✓ Bet added to slip")
            return True

    def _click_betmgm_alt_yes_pick_for_player(self, player_name: str,
                                              accordion_name: str) -> bool:
        """Click the lone ``Yes <price>`` pick on the row for ``player_name``.

        Used when the expanded accordion ships alternate-only picks
        (NBA Player points, Player assists, etc. with the threshold tab
        already selected by ``_select_alternate_tab_betmgm``). Each
        player row has exactly one ``ms-event-pick`` — find the row
        whose ancestor text contains ``player_name`` and click that
        pick.

        Returns True on a successful click, False if no match found.
        Raises BetPlacerError if a matching pick was found but the click
        itself failed (mirrors the std path's behavior).
        """
        acc = self._accordion_root_locator(accordion_name)
        if acc is None:
            print(f"[BETMGM] alt-mode: no accordion match for "
                  f"{accordion_name!r}")
            return False

        # Coax virtual-scrolled picks into the DOM by scrolling to bottom
        # then back to top — same dance as std-mode. Without this, alt
        # accordions with more rows than fit in the viewport (e.g. the
        # 2+ threes tab on Thunder@Spurs) only render the top few player
        # rows; the scan counts those few, misses the player we want,
        # and reports "no row matched" even though the row would have
        # appeared on its own a second later. Watcher caught this with
        # the Wembanyama miss on 2026-05-21.
        try:
            self.page.evaluate(
                "window.scrollTo(0, document.body.scrollHeight)"
            )
            settle(self.page, "micro_pause", rng=self._typing.rng)
            self.page.evaluate("window.scrollTo(0, 0)")
            settle(self.page, "micro_pause", rng=self._typing.rng)
        except Exception:
            pass

        all_picks = acc.locator('ms-event-pick')
        pick_count = all_picks.count()
        print(f"[BETMGM] alt-mode: scanning {pick_count} pick(s) inside "
              f"{accordion_name!r} for player {player_name!r}")

        matched_handle = None
        matched_option_id = None
        matched_meta = None

        for i in range(pick_count):
            try:
                pick = all_picks.nth(i)
                txt = (pick.text_content() or "").strip()

                row_player = self._player_name_for_pick(pick)
                if row_player is not None:
                    if not fuzzy_contains(row_player, player_name,
                                          threshold=90):
                        continue
                else:
                    ancestor_texts = pick.evaluate(
                        _WALKUP_JS,
                        {"max_depth": 8, "max_text_len": 150},
                    )
                    player_found = any(
                        fuzzy_contains(t, player_name, threshold=90)
                        for t in ancestor_texts
                    )
                    if not player_found:
                        continue

                option_id = pick.get_attribute("data-test-option-id")
                matched_handle = pick
                matched_option_id = option_id
                row_player_meta = (
                    f" row_player={row_player!r}" if row_player
                    else " (walkup-fallback)"
                )
                matched_meta = (
                    f"text={txt!r} option_id={option_id!r}{row_player_meta}"
                )
                break
            except Exception as e:
                print(f"[BETMGM] alt pick #{i} scan error: {e}")
                continue

        if matched_handle is None:
            print(f"[BETMGM] alt-mode: no row matched {player_name!r} "
                  f"(scanned {pick_count} pick(s))")
            return False

        print(f"[BETMGM] alt-mode matched: {matched_meta}")
        with with_screenshot_on_error(
            self, "alt_click_failed", "Failed to click BetMGM alt bet"
        ):
            if matched_option_id:
                target = self.page.locator(
                    f'ms-event-pick[data-test-option-id="{matched_option_id}"]'
                )
                mouse_click(self.page, target.first, state=self._cursor,
                            rng=self._typing.rng)
            else:
                mouse_click(self.page, matched_handle, state=self._cursor,
                            rng=self._typing.rng)
            settle(self.page, "slip_update", rng=self._typing.rng)
            self._screenshot("alt_bet_clicked")
            print(f"[BETMGM] ✓ Alt-mode bet added to slip")
            return True

    # ------------------------------------------------------------------
    # Task 13b — wager entry, place, odds, limit
    # ------------------------------------------------------------------

    def enter_wager(self, amount: float) -> bool:
        """Enter wager amount in the BetMGM slip via humanized typing.

        Replaces the legacy ``keyboard.type(amount_str, delay=80)`` with
        ``humanized_type``, which emits per-character keystrokes through
        the active ``TypingProfile`` (lognormal inter-key delays + the
        occasional typo-and-correct). The pre-submit dwell that the
        legacy code wrote as a 1000ms fixed wait becomes a categorized
        ``settle(..., "pre_submit_dwell")`` so the cadence drifts daily.
        """
        print(f"[BETMGM] Entering wager: ${amount:.2f}")
        with with_screenshot_on_error(
            self, "wager_entry_failed", "Failed to enter wager"
        ):
            # The wager input only mounts once the slip is expanded;
            # idempotent if it's already open.
            self._open_betmgm_slip()

            # Cascade of selectors — see legacy notes. Durable signals
            # first (inputmode=decimal, aria-label patterns), the more
            # brittle component-prefixed selectors next, and a text
            # last-resort.
            wager_selectors = [
                'app-stake-input input',
                'bs-stake-input input',
                'input[inputmode="decimal"]',
                'input[type="number"]',
                'input[aria-label*="stake" i]',
                'input[aria-label*="wager" i]',
                'input[aria-label*="amount" i]',
                'input[placeholder*="stake" i]',
                'input[placeholder*="enter amount" i]',
                '[data-testid*="stake" i] input',
                'input[type="text"]',  # last-resort fallback
            ]

            # If slip-clear failed and prior bets accumulated, the slip
            # has multiple stake inputs. Prefer the LAST visible EMPTY
            # input (= the just-added bet); only fall back to the last
            # filled one if every visible input is non-empty.
            wager_input = None
            wager_input_empty = False
            for selector in wager_selectors:
                try:
                    locator = self.page.locator(selector)
                    if locator.count() == 0:
                        continue
                    visible_inputs = []
                    for i in range(locator.count()):
                        elem = locator.nth(i)
                        try:
                            if not elem.is_visible():
                                continue
                            try:
                                value = elem.input_value() or ""
                            except Exception:
                                value = ""
                            visible_inputs.append((elem, value))
                        except Exception:
                            continue
                    if not visible_inputs:
                        continue
                    empty_inputs = [el for el, v in visible_inputs
                                    if not v.strip()]
                    if empty_inputs:
                        wager_input = empty_inputs[-1]
                        wager_input_empty = True
                        print(f"[BETMGM] Found wager input via {selector} "
                              f"(picked last empty of {len(visible_inputs)})")
                    else:
                        wager_input = visible_inputs[-1][0]
                        print(f"[BETMGM] Found wager input via {selector} "
                              f"(all {len(visible_inputs)} filled; picked last)")
                    break
                except Exception:
                    continue

            if wager_input is None:
                # Diagnostic dump — what visible inputs DO exist?
                try:
                    inputs = self.page.locator("input")
                    dump = []
                    for i in range(min(inputs.count(), 12)):
                        el = inputs.nth(i)
                        try:
                            if not el.is_visible():
                                continue
                            dump.append({
                                "type": el.get_attribute("type"),
                                "inputmode": el.get_attribute("inputmode"),
                                "aria-label": el.get_attribute("aria-label"),
                                "placeholder": el.get_attribute("placeholder"),
                                "data-testid": el.get_attribute("data-testid"),
                            })
                        except Exception:
                            continue
                    print(f"[BETMGM] visible inputs ({len(dump)}): {dump!r}")
                except Exception:
                    pass
                self._screenshot("wager_input_not_found")
                raise BetPlacerError("Could not find BetMGM wager input")

            # Focus + clear the input, then humanized-type the amount.
            # The Angular numpad widget listens on keydown events; we
            # must NOT use .fill() (which sets DOM value but doesn't
            # fire keydown), or the Place Bet button will stay
            # aria-disabled='true'.
            mouse_click(self.page, wager_input, state=self._cursor,
                        rng=self._typing.rng, fast=True)
            settle(self.page, "micro_pause", rng=self._typing.rng)
            # Clear existing content so the new digits don't append.
            # Use locator.press, NOT page.keyboard.press — settle()'s
            # pre-sleep modal sweep can drift focus off the input, and
            # page.keyboard.press fires on whatever currently has focus.
            # locator.press auto-refocuses the element. Same root cause
            # as the FD fix on 2026-05-25.
            # Skip the clear entirely when we picked an already-empty input
            # (the common case — the just-added bet). When non-empty, clear
            # with a SHORT timeout: BetMGM's slip re-renders constantly so the
            # element-stability actionability check otherwise burns the full
            # 30s default before this best-effort press gives up.
            if not wager_input_empty:
                try:
                    wager_input.press("Control+A", timeout=2000)
                    wager_input.press("Delete", timeout=2000)
                except Exception:
                    pass
            settle(self.page, "micro_pause", rng=self._typing.rng)

            amount_str = f"{amount:.2f}"
            # mouse_click above already focused the input; this is a belt-and-
            # suspenders refocus. Short timeout for the same stability reason.
            try:
                wager_input.focus(timeout=2000)
            except Exception:
                pass
            humanized_type(self.page, wager_input, amount_str,
                           profile=self._typing)

            # Blur the input so the form-state machine validates and
            # the Place Bet button transitions disabled→enabled.
            self.page.keyboard.press("Tab")
            settle(self.page, "pre_submit_dwell", rng=self._typing.rng)

            self._screenshot("wager_entered")
            print(f"[BETMGM] ✓ Wager entered: ${amount:.2f}")
            return True

    def place_bet(self) -> Tuple[str, str]:
        """Click the Place Bet button and poll for the success/failure
        confirmation. In shadow mode (``BG_SHADOW_MODE=1``), aborts
        BEFORE the click so a recorded run can validate the whole
        pre-submit flow without actually placing real money.
        """
        print(f"[BETMGM] Placing bet...")
        with with_screenshot_on_error(
            self, "place_bet_failed", "Place bet failed"
        ):
            place_btn = self.page.get_by_role(
                "button", name=re.compile(r"Place\s+Bet", re.I)
            )

            if place_btn.count() == 0:
                self._screenshot("place_bet_not_found")
                raise BetPlacerError("Place Bet button not found")

            # Shadow-mode short-circuit: the slip is loaded, the button
            # is visible — we've validated the pre-submit flow end-to-end.
            # Abort here, BEFORE the humanized click, so no real money
            # changes hands. ShadowAbortError subclasses BetPlacerError;
            # the worker classifies it as SKIPPED, not FAILED.
            if os.getenv("BG_SHADOW_MODE") == "1":
                raise ShadowAbortError(
                    "BG_SHADOW_MODE=1: aborted before Place Bet click "
                    "(shadow run)"
                )

            print(f"[BETMGM] Clicking Place Bet...")
            mouse_click(self.page, place_btn.first, state=self._cursor,
                        rng=self._typing.rng, fast=True)
            settle(self.page, "slip_update", rng=self._typing.rng)

            # Poll for confirmation — accepted / alt-success / rejected.
            # 10 attempts × slip_update settle ≈ 5s window (matches
            # legacy 10×500ms).
            for _ in range(10):
                accepted_msg = self.page.get_by_text(
                    "Your bet has been accepted"
                )
                if (accepted_msg.count() > 0
                        and accepted_msg.first.is_visible()):
                    self._screenshot("bet_placed_success")
                    print(f"[BETMGM] ✓ Bet ACCEPTED")
                    self._close_betslip_betmgm()
                    return "ACCEPTED", "Your bet has been accepted"

                alt_success = self.page.get_by_text(
                    re.compile(r"Bet Placed|Wager Accepted", re.I)
                )
                if (alt_success.count() > 0
                        and alt_success.first.is_visible()):
                    self._screenshot("bet_placed_success")
                    print(f"[BETMGM] ✓ Bet ACCEPTED")
                    self._close_betslip_betmgm()
                    return "ACCEPTED", "Bet placed successfully"

                error_msg = self.page.get_by_text(
                    re.compile(r"limit exceeded|Error|rejected", re.I)
                )
                if (error_msg.count() > 0
                        and error_msg.first.is_visible()):
                    msg = error_msg.first.text_content() or "Unknown error"
                    self._screenshot("bet_rejected")
                    print(f"[BETMGM] ✗ Bet REJECTED: {msg}")
                    return "REJECTED", msg

                settle(self.page, "slip_update", rng=self._typing.rng)

            # Unknown state — neither success nor rejection observed.
            self._screenshot("bet_status_unknown")
            print(f"[BETMGM] ? Bet status UNKNOWN")
            return "UNKNOWN", "Could not determine bet status"

    def _close_betslip_betmgm(self) -> None:
        """Close the slip after a successful bet. Best-effort — failures
        here are non-fatal; the next iteration's slip-clear will reset.
        """
        try:
            close_selectors = [
                'bs-linear-result-summary button',
                '[aria-label="Close"]',
            ]
            for selector in close_selectors:
                try:
                    close_btn = self.page.locator(selector)
                    if (close_btn.count() > 0
                            and close_btn.first.is_visible()):
                        print(f"[BETMGM] Closing betslip...")
                        mouse_click(self.page, close_btn.first,
                                    state=self._cursor,
                                    rng=self._typing.rng)
                        settle(self.page, "modal_dismiss",
                               rng=self._typing.rng)
                        print(f"[BETMGM] ✓ Betslip closed")
                        return
                except Exception:
                    continue
            print(f"[BETMGM] ⚠ Could not find close button, "
                  f"continuing anyway...")
        except Exception as e:
            print(f"[BETMGM] ⚠ Error closing betslip: {e}")

    def get_actual_odds(self) -> Optional[float]:
        """Extract the decimal odds rendered in the BetMGM slip.

        Pure DOM probe — no clicks, so no humanized mouse path. Selector
        cascade and regex are preserved verbatim from the legacy
        implementation; the regex is load-bearing for parsing prices
        like "1.75" out of the odds span.
        """
        try:
            odds_selectors = [
                'span.odds-indicator__lite--default',
                'span[class*="odds-indicator"]',
                '.odds-indicator',
            ]

            for selector in odds_selectors:
                try:
                    odds_elem = self.page.locator(selector)
                    if odds_elem.count() > 0:
                        text = odds_elem.first.text_content() or ""
                        text = text.strip()
                        decimal_match = re.search(r'(\d+\.?\d*)', text)
                        if decimal_match:
                            decimal_odds = float(decimal_match.group(1))
                            print(f"[BETMGM] Extracted odds: "
                                  f"{decimal_odds:.3f}")
                            return decimal_odds
                except Exception:
                    continue

            print(f"[BETMGM] ⚠ Could not extract odds from betslip")
            return None
        except Exception as e:
            print(f"[BETMGM] ⚠ Error extracting odds: {e}")
            return None

    def check_limit_alert(self) -> Tuple[bool, Optional[float]]:
        """Detect BetMGM's "over the allowed limit" alert and parse the
        adjusted stake.

        Pure text/DOM probe — no clicks. Returns ``(True, adjusted)`` if
        the alert fired and the adjusted stake parsed cleanly,
        ``(True, None)`` if the alert fired but the stake couldn't be
        parsed, ``(False, None)`` otherwise.

        Regex patterns are preserved verbatim — the ``"$6.76"`` /
        ``"6,762.50"`` shape parsing is load-bearing.
        """
        try:
            alert_selectors = [
                'p.alert-content__message',
                '.alert-content__message',
                'p:has-text("over the allowed limit")',
            ]

            for selector in alert_selectors:
                try:
                    alert_elem = self.page.locator(selector)
                    if alert_elem.count() > 0:
                        alert_text = alert_elem.first.text_content() or ""
                        if "over the allowed limit" in alert_text.lower():
                            print(f"[BETMGM] ⚠ Max limit alert detected!")

                            stake_selectors = [
                                'span.betslip-summary-value',
                                '.betslip-summary-value',
                            ]

                            for stake_selector in stake_selectors:
                                stake_elem = self.page.locator(
                                    stake_selector
                                ).first
                                if stake_elem.count() > 0:
                                    stake_text = (
                                        stake_elem.text_content() or ""
                                    )
                                    stake_match = re.search(
                                        r'\$?([\d,]+\.?\d*)', stake_text
                                    )
                                    if stake_match:
                                        adjusted_stake = float(
                                            stake_match.group(1)
                                            .replace(',', '')
                                        )
                                        print(f"[BETMGM] Adjusted stake: "
                                              f"${adjusted_stake:.2f}")
                                        self._screenshot(
                                            "limit_alert_detected"
                                        )
                                        return True, adjusted_stake

                            print(f"[BETMGM] ⚠ Could not parse "
                                  f"adjusted stake")
                            self._screenshot("limit_alert_no_stake")
                            return True, None
                except Exception:
                    continue

            return False, None
        except Exception as e:
            print(f"[BETMGM] ⚠ Error checking limit alert: {e}")
            return False, None
