"""BetMGM-specific bet placement implementation."""

import re
from typing import Dict, Tuple

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from selector_finder import calculate_alternate_tab_value
from text_match import fuzzy_contains
from bet_placer import BetPlacer, BetPlacerError, BetPlacerSkipError


class BetmgmBetPlacer(BetPlacer):
    """Handles bet placement on BetMGM."""

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

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        """Navigate BetMGM to event and expand the market accordion."""
        self._navigate_betmgm(opportunity, market_config, direction)

    def _navigate_betmgm(self, opportunity: Dict, market_config: Dict, direction: str = None):
        """Navigate BetMGM to event and expand market accordion."""
        home_team = opportunity['home_team']
        away_team = opportunity['away_team']
        sport = (opportunity.get('sport_title') or '').upper()
        accordion_name = market_config.get('accordion_name', '')
        is_alternate = market_config.get('is_alternate', False) or market_config.get('has_threshold_tabs', False)

        # Start on the regular homepage, not the betfinder popup. The
        # ?popup=betfinder querystring overlays a search modal on top of
        # the slip widget, which makes the subsequent slip-clear race
        # against the modal (observed: slip-clear silently no-op'd while
        # the modal sat on top, leaving stale bets in the slip). From the
        # plain /en/sports page the slip pill at the bottom is reliably
        # clickable and the page state is unambiguous. The betfinder
        # popup opens later, after the slip is clean.
        print(f"[BETMGM] Loading homepage... (sport: {sport})")
        self.page.goto("https://www.mo.betmgm.com/en/sports", wait_until="domcontentloaded")
        self.page.wait_for_timeout(2000)

        # Auth-and-render probe: when the BetMGM session is dead, the
        # homepage renders a "Log in" link in the header instead of the
        # account/balance widget. Cheap visual check — if the link is
        # there, we'd otherwise click around as logged-out and the run
        # would degrade silently. Fail loud, let the operator log back in.
        try:
            login_link = self.page.locator('a[href*="/login"]:has-text("Log in")')
            if login_link.count() > 0 and login_link.first.is_visible():
                self._screenshot("session_expired")
                raise BetPlacerError(
                    "BetMGM session expired (Log in link visible on homepage)"
                )
        except BetPlacerError:
            raise
        except Exception as e:
            print(f"[BETMGM] ⚠ Auth probe failed: {e} (continuing — assuming logged in)")

        # Clear any leftover bets from a previous failed run. The slip
        # icon at the bottom shows "(N)" when items are queued — those
        # must come out before our click adds a new one cleanly. Done
        # from the homepage so the slip drawer isn't fighting the search
        # modal for pointer events.
        self._clear_betslip_betmgm_precheck()

        # Now open the betfinder popup for the actual team lookup. After
        # this nav the page renders the search modal on top of the
        # (already-cleaned) homepage.
        print(f"[BETMGM] Opening betfinder search...")
        self.page.goto("https://www.mo.betmgm.com/en/sports?popup=betfinder", wait_until="domcontentloaded")
        self.page.wait_for_timeout(2000)

        # Search for team — MLB needs autocomplete suggestion click, others use Enter
        try:
            search_input = self.page.locator(
                'div.cdk-overlay-container input, '
                'input[placeholder*="Search"], '
                'input[placeholder*="Find"]'
            ).first
            search_input.wait_for(state="visible", timeout=10000)
            print(f"[BETMGM] Searching for: {home_team}")
            search_input.fill(home_team)

            if sport == 'MLB':
                # MLB: click autocomplete suggestion (BetMGM shows Futures that interfere with Enter)
                self.page.wait_for_timeout(2000)
                suggestion_clicked = False
                try:
                    suggestions = self.page.locator('ms-search-suggestions-list-item')
                    suggestions.first.wait_for(state="visible", timeout=5000)
                    for i in range(suggestions.count()):
                        item = suggestions.nth(i)
                        item_text = (item.text_content() or "").lower()
                        if home_team.lower() in item_text and "future" not in item_text:
                            print(f"[BETMGM] Clicking search suggestion: {item_text.strip()[:60]}")
                            item.click()
                            suggestion_clicked = True
                            self.page.wait_for_timeout(3000)
                            break
                except Exception:
                    pass

                if not suggestion_clicked:
                    print(f"[BETMGM] No suggestion found, pressing Enter...")
                    search_input.press("Enter")
                    self.page.wait_for_timeout(3000)
            else:
                # NBA/NHL/NFL: standard Enter-based search
                self.page.keyboard.press("Enter")
                self.page.wait_for_timeout(3000)

        except Exception as e:
            raise BetPlacerError(f"Search failed: {e}")

        # Navigate to the event page.
        #
        # BetMGM removed the "All Wagers" intermediate link from event cards
        # in mid-2026 — the whole card is now a single
        # <a href="/en/sports/events/<slug>-<id>"> anchor. The previous code
        # path of "find the All Wagers span inside the card" stopped finding
        # anything; the fallback that hunted `ms-event-footer` likewise
        # misses because that footer element no longer ships.
        #
        # New strategy: scan every `a[href*="/sports/events/"]` on the page,
        # pick the one whose text contains both team names (or whose href
        # slug matches both), and navigate to its href directly. This skips
        # the click-and-wait dance and is more robust to layout changes
        # (search-results, team-detail page, betfinder grid all share the
        # same anchor pattern).
        try:
            current_url = self.page.url.lower()
            home_slug = home_team.lower().replace(" ", "-")
            away_slug = away_team.lower().replace(" ", "-")

            if "/events/" in current_url and (home_slug in current_url or away_slug in current_url):
                print(f"[BETMGM] Already on event page: {current_url}")
            else:
                anchors = self.page.locator('a[href*="/sports/events/"]')
                n = anchors.count()
                print(f"[BETMGM] Scanning {n} event anchor(s) for {away_team} @ {home_team}")

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

                # Require both teams represented somewhere (text or href slug).
                # Score >= 2 covers: both names in text, OR one name in text +
                # one slug in href, OR both slugs in href.
                if not target_href or best_score < 2:
                    self._screenshot("event_link_not_found")
                    raise BetPlacerError(
                        f"Could not find event link: {away_team} @ {home_team} "
                        f"(scanned {n} anchors, best score={best_score})"
                    )

                # Normalize relative href; BetMGM mixes absolute and relative.
                if target_href.startswith("/"):
                    target_href = "https://www.mo.betmgm.com" + target_href
                print(f"[BETMGM] Navigating to event: {target_href}")
                self.page.goto(target_href, wait_until="domcontentloaded")
                self.page.wait_for_timeout(2000)

            # Add ?market=PlayerProps to land directly on the full props view.
            current_url = self.page.url
            if "market=PlayerProps" not in current_url:
                new_url = current_url + ("&" if "?" in current_url else "?") + "market=PlayerProps"
                print(f"[BETMGM] Navigating to player props: {new_url}")
                self.page.goto(new_url, wait_until="domcontentloaded")
                self.page.wait_for_timeout(2000)

        except BetPlacerError:
            raise
        except Exception as e:
            self._screenshot("navigation_failed")
            raise BetPlacerError(f"Event navigation failed: {e}")

        # Click market sub-tab first (e.g. "Combo stats" for player_points_rebounds)
        # — the sub-tab determines which accordion is visible at all.
        self._select_market_sub_tab_betmgm(market_config)

        # Expand accordion
        try:
            print(f"[BETMGM] Expanding accordion: {accordion_name}")
            # Use :text-is for an EXACT match — BetMGM ships sibling
            # accordions like "Player rebounds + assists" and "Player
            # rebounds + assists O/U" at the same level. :has-text would
            # match both via substring and pick whichever came first in
            # DOM order, often expanding the wrong market silently.
            exact_selector = (
                f'button[dsaccordiontoggle]:text-is("{accordion_name}")'
            )
            accordion = self.page.locator(exact_selector)

            target = None
            if accordion.count() > 0:
                target = accordion.first
            else:
                # No fuzzy fallback. The previous fuzzy-score path silently
                # masked YAML/UI drift — e.g. PR #12 found 10 batter_*
                # YAML entries with a wrong "Player" prefix that fuzzy-
                # matched onto neighboring accordions, putting the bot in
                # the wrong market context and surfacing two layers later
                # as a misleading "No bet found for X under 0.5" error.
                #
                # Instead: iterate visible accordions with a normalized
                # exact match (whitespace + case insensitive) to absorb
                # BetMGM's minor label wobble, and on miss raise loudly
                # with the full visible list so the operator's next move
                # is one obvious YAML edit.
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
                    # Structural-skip path: this std opp's "O/U" accordion
                    # isn't on the live page, but the merged-alt sibling
                    # IS. BetMGM ships per-event variance — a lower-profile
                    # game may only carry the alt-merged accordion. Falling
                    # back to it can't satisfy direction='under' (alt is
                    # Yes-only), so the opp is structurally unbettable.
                    # Mark SKIPPED, not FAILED. See LOGIC.md.
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

            # Accordion buttons are toggles — if a prior session left it
            # expanded, clicking again COLLAPSES. Check aria-expanded and
            # skip the click in that case.
            try:
                already_expanded = (target.get_attribute("aria-expanded") == "true")
            except Exception:
                already_expanded = False
            if already_expanded:
                print(f"[BETMGM] Accordion already expanded; skipping click.")
            else:
                target.click()

            # Fast-fail: wait up to 5s for at least one ms-event-pick row.
            # Replaces a fixed 1.5s sleep that previously let stuck-search /
            # wrong-sub-tab cases stall ~67s downstream before surfacing.
            try:
                self.page.wait_for_selector("ms-event-pick", timeout=5000, state="visible")
            except PlaywrightTimeoutError as e:
                self._screenshot("betmgm_accordion_empty")
                raise BetPlacerError(
                    "BetMGM accordion expanded but no ms-event-pick rows in 5s "
                    "— likely wrong sub-tab, market not offered, or stuck search overlay"
                ) from e

            # Click "Show More" until all players visible
            show_more_selector = 'ms-option-panel-bottom-action:has-text("Show More")'
            attempts = 0
            while attempts < 5:
                show_more = self.page.locator(show_more_selector)
                if show_more.count() == 0:
                    break
                show_more.first.click()
                self.page.wait_for_timeout(1000)
                attempts += 1

            print(f"[BETMGM] ✓ Market expanded")
            self._screenshot("market_expanded")

            # For alternate markets, select the threshold tab
            if is_alternate and direction:
                self._select_alternate_tab_betmgm(opportunity, market_config, direction)

        except BetPlacerSkipError:
            # Structural skip — surface to caller unwrapped so the worker
            # can classify the task as SKIPPED, not FAILED.
            raise
        except Exception as e:
            self._screenshot("accordion_expansion_failed")
            raise BetPlacerError(f"Accordion expansion failed: {e}")

    def _select_market_sub_tab_betmgm(self, market_config: Dict) -> None:
        """Click a market sub-tab (e.g. 'Combo stats') if configured.

        For non-default BetMGM markets like player_points_rebounds, the
        correct accordion lives under a sub-tab that isn't selected by
        default. No-op if the market has no `sub_tab_label`.
        """
        sub_tab = market_config.get("sub_tab_label")
        if not sub_tab:
            return

        print(f"[BETMGM] Selecting market sub-tab: {sub_tab}")
        for selector in (
            f'div[role="tablist"] button:has-text("{sub_tab}")',
            f'[role="tab"]:has-text("{sub_tab}")',
            f'button:has-text("{sub_tab}")',
        ):
            try:
                loc = self.page.locator(selector)
                if loc.count() > 0 and loc.first.is_visible():
                    loc.first.click()
                    self.page.wait_for_timeout(800)
                    print(f"[BETMGM] ✓ Sub-tab '{sub_tab}' selected via {selector}")
                    self._screenshot("sub_tab_selected")
                    return
            except Exception as e:
                print(f"[BETMGM] Sub-tab selector failed ({selector}): {e}")
                continue

        self._screenshot("sub_tab_not_found")
        raise BetPlacerError(f"Could not find BetMGM sub-tab '{sub_tab}'")

    def _select_alternate_tab_betmgm(self, opportunity: Dict, market_config: Dict, direction: str):
        """Select threshold tab in BetMGM alternate accordion.

        For alternate markets, BetMGM shows tabs like "5+", "7+", "9+" for different thresholds.
        This method selects the correct tab based on the betting line.
        """
        line = opportunity.get('over_line') if direction == 'over' else opportunity.get('under_line')
        if line is None:
            print(f"[BETMGM] ⚠ No line found for direction {direction}, skipping tab selection")
            return

        tab_value = calculate_alternate_tab_value(line)
        tab_text = f"{tab_value}+"

        print(f"[BETMGM] Selecting alternate tab: {tab_text} (line: {line})")

        # Try multiple tab selector patterns
        tab_selectors = [
            f'button:has-text("{tab_text}")',
            f'[role="tab"]:has-text("{tab_text}")',
            f'div[role="tablist"] button:has-text("{tab_text}")',
            market_config.get('tab_selector_pattern', '').format(threshold=tab_value) if market_config.get('tab_selector_pattern') else None,
        ]

        for selector in tab_selectors:
            if not selector:
                continue
            try:
                tab = self.page.locator(selector)
                if tab.count() > 0 and tab.first.is_visible():
                    print(f"[BETMGM] Found tab using: {selector}")
                    tab.first.click()
                    self.page.wait_for_timeout(1000)
                    print(f"[BETMGM] ✓ Tab {tab_text} selected")
                    self._screenshot("alternate_tab_selected")
                    # Tab selection re-renders the player list to the first ~5
                    # players. The next bet-find step looks for a specific
                    # player by aria-label; if that player isn't in the first
                    # render, the lookup misses. Click Show More until the
                    # full list is on the page. Mirrors the post-expand Show
                    # More loop in _expand_accordion_betmgm.
                    show_more_selector = (
                        'ms-option-panel-bottom-action:has-text("Show More")'
                    )
                    attempts = 0
                    while attempts < 5:
                        show_more = self.page.locator(show_more_selector)
                        if show_more.count() == 0:
                            break
                        try:
                            if not show_more.first.is_visible():
                                break
                            show_more.first.click()
                            self.page.wait_for_timeout(1000)
                        except Exception:
                            break
                        attempts += 1
                    if attempts:
                        print(f"[BETMGM] ✓ Expanded player list ({attempts}× Show More)")
                    return
            except Exception as e:
                print(f"[BETMGM] Tab selector failed: {selector} - {e}")
                continue

        # If we couldn't find the tab, log warning but continue
        print(f"[BETMGM] ⚠ Could not find tab '{tab_text}', continuing without tab selection")
        self._screenshot("alternate_tab_not_found")

    def clear_betslip(self):
        """Empty the BetMGM betslip; fail if it remains non-empty."""
        self._clear_betslip_betmgm_precheck()

    def _clear_betslip_betmgm_precheck(self) -> None:
        """Empty the BetMGM betslip if it isn't already.

        Same rationale as the FanDuel version: stale items leave the
        accordion-click path in an ambiguous state and cause toggle-off
        side-effects. Best-effort, never raises.
        """
        try:
            # BetMGM shows a "Bet slip (N)" pill at the bottom. If N == 0
            # there's nothing to clear and the icon is just "Bet slip".
            try:
                slip_pill = self.page.locator(
                    'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i'
                )
                text = ""
                if slip_pill.count() > 0:
                    text = (slip_pill.first.text_content() or "").strip()
                # Match "(0)" -> empty; "(1)" -> has bets.
                m = re.search(r"\((\d+)\)|^\s*(\d+)\s+Bet slip", text, re.I)
                if m and int(m.group(1) or m.group(2)) == 0:
                    print(f"[BETMGM] Slip already empty.")
                    return
            except Exception:
                pass

            clicked_clear_all = False

            # Open the slip to access remove controls. The pill at the
            # bottom is a non-button element ("0 Bet slip" / "1 Bet slip"
            # in a span/div). Earlier `button:has-text("Bet slip")`
            # selectors matched nothing and the subsequent "Clear all"
            # click hit a wrong button on the page (or no-op'd) while
            # bets accumulated across iterations.
            self._open_betmgm_slip()

            # "Remove all" sweep. In desktop right-rail layout the actual
            # clickable Clear All element is a <span> (not a <button>).
            # Mobile-layout variants render it as a button or div[role=button].
            for sel in (
                'span:has-text("Clear All")',
                'button:has-text("Clear All")',
                'button:has-text("Remove all")',
                'button:has-text("Clear all")',
                'button[aria-label*="remove all" i]',
                'div[role="button"]:has-text("Clear All")',
                '[role="button"]:has-text("Clear All")',
            ):
                try:
                    loc = self.page.locator(sel)
                    if loc.count() > 0 and loc.first.is_visible():
                        print(f"[BETMGM] Clearing slip via {sel}")
                        loc.first.click()
                        self.page.wait_for_timeout(800)
                        # Some BetMGM flows show a confirmation dialog.
                        for confirm_sel in (
                            'button:has-text("Yes")',
                            'button:has-text("Remove")',
                            'button:has-text("Confirm")',
                        ):
                            try:
                                cloc = self.page.locator(confirm_sel)
                                if cloc.count() > 0 and cloc.first.is_visible():
                                    cloc.first.click()
                                    self.page.wait_for_timeout(500)
                                    break
                            except Exception:
                                continue
                        clicked_clear_all = True
                        break
                except Exception:
                    continue

            # Otherwise individual remove icons.
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
                                        cand.click(timeout=2000)
                                        self.page.wait_for_timeout(400)
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

        # Post-clear verification: re-read the slip pill. If "(N)" with N > 0
        # remains, the clear didn't take and we should halt before placing.
        try:
            slip_pill = self.page.locator(
                'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i'
            )
            if slip_pill.count() > 0:
                text = (slip_pill.first.text_content() or "").strip()
                m = re.search(r"\((\d+)\)|^\s*(\d+)\s+Bet slip", text, re.I)
                if m and int(m.group(1) or m.group(2)) > 0:
                    raise BetPlacerError(
                        f"BetMGM slip-clear failed: pill still reads {text!r}"
                    )
        except BetPlacerError:
            raise
        except Exception:
            pass

    def _open_betmgm_slip(self) -> None:
        """Click the bottom-docked Bet slip pill to expand the slip panel.

        BetMGM's slip is collapsed by default after a bet is added —
        the wager input doesn't exist in the DOM until the slip
        expands. Tries multiple selectors (text variants seen in
        production: 'pays out', 'to win', plain 'Bet slip').
        Idempotent — if the slip is already open, returns silently.
        """
        try:
            # If a stake input is already visible, slip is already expanded.
            for probe in ('app-stake-input input', 'bs-stake-input input',
                          'input[inputmode="decimal"]'):
                try:
                    loc = self.page.locator(probe)
                    if loc.count() > 0 and loc.first.is_visible():
                        return
                except Exception:
                    continue

            # Click any visible slip-pill affordance.
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
                    # Pick the FIRST visible candidate; bottom-pill is
                    # usually the only visible match.
                    for i in range(min(loc.count(), 5)):
                        cand = loc.nth(i)
                        try:
                            if cand.is_visible():
                                print(f"[BETMGM] Opening slip via {sel}")
                                cand.click()
                                self.page.wait_for_timeout(1500)
                                return
                        except Exception:
                            continue
                except Exception:
                    continue
            print("[BETMGM] ⚠ No slip-pill affordance found; continuing.")
        except Exception as e:
            print(f"[BETMGM] ⚠ Slip-open probe failed: {e} (continuing)")

    def assert_betslip_has_bet(self):
        """Assert a selected bet actually reached the slip."""
        self._open_betmgm_slip()
        if not self._betmgm_slip_has_bet():
            self._screenshot("validation_slip_empty")
            raise BetPlacerError("BetMGM slip is empty after bet click")

    def assert_betslip_empty(self):
        """Assert the slip is empty after cleanup."""
        self._open_betmgm_slip()
        if self._betmgm_slip_has_bet():
            self._screenshot("validation_slip_not_empty")
            raise BetPlacerError("BetMGM slip still appears to contain a bet")

    def _betmgm_slip_has_bet(self) -> bool:
        """Return True if BetMGM's slip appears to contain at least one bet.

        Conservative on ambiguous states: if the page exposes remove controls
        or a non-zero slip pill, report True. If a clear empty marker is
        visible, report False.
        """
        try:
            for text in ("No bet selections", "Betslip empty"):
                empty_marker = self.page.get_by_text(text, exact=False)
                if empty_marker.count() > 0 and empty_marker.first.is_visible():
                    return False
        except Exception:
            pass

        try:
            slip_pill = self.page.locator(
                'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i'
            )
            if slip_pill.count() > 0:
                text = (slip_pill.first.text_content() or "").strip()
                m = re.search(r"\((\d+)\)|^\s*(\d+)\s+Bet slip", text, re.I)
                if m:
                    return int(m.group(1) or m.group(2)) > 0
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

    def find_and_click_bet(self, opportunity, direction, market_config):
        """Find and click the bet for the specified player/line/direction.

        Dispatches between two pick-matching strategies based on what
        BetMGM actually rendered in the (already-expanded) accordion
        panel:

        * **std O/U** — picks read ``O 11.5 1.92`` / ``U 11.5 1.92``.
          The pick's own text contains the line, so we filter to the
          target line first, then walk up to find the matching player
          row. This is the original path; covers NHL, MLB std, NFL,
          and NBA Player blocks / quarter markets.

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
        line = opportunity['over_line'] if direction == 'over' else opportunity['under_line']
        accordion_name = market_config.get('accordion_name', '')

        print(f"[BETMGM] Finding bet: {player_name} {direction} {line}")

        pick_format = 'std'
        if market_config.get('has_threshold_tabs') or market_config.get('is_alternate'):
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
                    f"BetMGM alt-only accordion can't take direction={direction!r}; "
                    f"only 'over' (Yes pick) is supported. Market: {accordion_name!r}"
                )
            clicked = self._click_betmgm_alt_yes_pick_for_player(player_name, accordion_name)
        else:
            clicked = self._click_betmgm_pick_for_player(player_name, line, direction)

        if clicked:
            # Expand viewport for betslip interaction
            print(f"[BETMGM] Expanding viewport to 1920x945...")
            self.page.set_viewport_size({"width": 1920, "height": 945})
            self.page.wait_for_timeout(500)
            return True

        # Miss-path diagnostic + raise (mirrors legacy lines 686-734)
        try:
            aria_loc = self.page.locator(f'[aria-label*="{player_name}"]')
            aria_dump = []
            for i in range(min(aria_loc.count(), 10)):
                try:
                    aria_dump.append(aria_loc.nth(i).get_attribute("aria-label"))
                except Exception:
                    continue
            print(f"[BETMGM] aria-labels mentioning {player_name!r} "
                  f"({len(aria_dump)}): {aria_dump!r}")
        except Exception:
            pass
        try:
            btn_dump = []
            btn_loc = self.page.locator(f'button:has-text("{player_name}")')
            for i in range(min(btn_loc.count(), 10)):
                try:
                    txt = (btn_loc.nth(i).text_content() or "").strip()[:120]
                    btn_dump.append(txt)
                except Exception:
                    continue
            print(f"[BETMGM] buttons mentioning {player_name!r} "
                  f"({len(btn_dump)}): {btn_dump!r}")
        except Exception:
            pass
        try:
            pick_loc = self.page.locator("ms-event-pick")
            pick_count = pick_loc.count()
            pick_dump = []
            for i in range(min(pick_count, 20)):
                try:
                    txt = (pick_loc.nth(i).text_content() or "").strip()[:80]
                    if player_name.lower() in txt.lower():
                        pick_dump.append(txt)
                except Exception:
                    continue
            print(f"[BETMGM] ms-event-pick elements mentioning "
                  f"{player_name!r} ({len(pick_dump)}): {pick_dump!r}")
        except Exception:
            pass
        self._screenshot("bet_not_found")
        raise BetPlacerError(f"No bet found for {player_name} {direction} {line}")

    def _click_betmgm_pick_for_player(self, player_name: str, line: float, direction: str) -> bool:
        """Find and click the BetMGM ms-event-pick that matches this player+line+direction.

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
        direction_letter = "O" if direction == "over" else "U"
        target_text = f"{direction_letter} {line}"
        target_text_alt = f"{direction_letter} {int(line)}" if float(line).is_integer() else None

        # Best-effort: scroll the page to its bottom to coax virtual-scroll
        # / lazy-load picks into the DOM. Some BetMGM pages keep below-fold
        # picks unrendered until they enter the viewport — we previously
        # missed Fox's Under-3.5 pick this way. Cheap, idempotent.
        try:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            self.page.wait_for_timeout(400)
            self.page.evaluate("window.scrollTo(0, 0)")
            self.page.wait_for_timeout(200)
        except Exception:
            pass

        all_picks = self.page.locator("ms-event-pick")
        pick_count = all_picks.count()
        print(f"[BETMGM] scanning {pick_count} ms-event-pick(s) for "
              f"{target_text!r} on player {player_name!r}")

        # Walk-up returns each ancestor's innerText (with a length cap so we
        # don't pay for serializing the whole document). Python-side, we
        # apply fuzzy_contains across the returned texts — the JS-side
        # `.includes(player)` is character-exact and silently misses curly-
        # vs-straight-apostrophe rows like "De'Aaron Fox".
        walkup_js = """
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

        matched_option_id = None
        matched_handle = None
        matched_meta = None

        for i in range(pick_count):
            try:
                pick = all_picks.nth(i)
                # Deliberately NOT calling pick.is_visible() — the previous
                # impl skipped DOM-attached but not-yet-rendered picks (e.g.
                # below the fold), which is exactly how Fox's Under-3.5
                # disappeared. Playwright's click() will auto-scroll into
                # view, so unrendered-but-attached is fine here.
                txt = (pick.text_content() or "").strip()
                norm = " ".join(txt.split())
                if target_text not in norm and (
                    target_text_alt is None or target_text_alt not in norm
                ):
                    continue
                # Pull ancestor texts; fuzzy-match the player Python-side.
                ancestor_texts = pick.evaluate(
                    walkup_js,
                    {"max_depth": 15, "max_text_len": 600},
                )
                player_found = any(
                    fuzzy_contains(t, player_name, threshold=90)
                    for t in ancestor_texts
                )
                if not player_found:
                    continue
                option_id = pick.get_attribute("data-test-option-id")
                matched_option_id = option_id
                matched_handle = pick
                matched_meta = f"text={norm!r} option_id={option_id!r}"
                break
            except Exception as e:
                print(f"[BETMGM] pick #{i} scan error: {e}")
                continue

        if matched_handle is None:
            print(f"[BETMGM] no ms-event-pick matched {target_text!r} for "
                  f"{player_name!r} (scanned {pick_count})")
            return False

        print(f"[BETMGM] matched bet: {matched_meta}")
        # Prefer clicking via the stable data-test-option-id selector when
        # one is present — the locator handle can go stale across the click's
        # auto-scroll if Angular re-renders the row.
        try:
            if matched_option_id:
                target = self.page.locator(
                    f'ms-event-pick[data-test-option-id="{matched_option_id}"]'
                )
                target.first.click(timeout=10000)
            else:
                matched_handle.click(timeout=10000)
            self.page.wait_for_timeout(1500)
            self._screenshot("bet_clicked")
            print(f"[BETMGM] ✓ Bet added to slip")
            return True
        except Exception as e:
            self._screenshot("click_failed")
            raise BetPlacerError(f"Failed to click BetMGM bet: {e}")

    def _accordion_root_locator(self, accordion_name: str):
        """Return a Locator scoped to the ``ds-accordion`` whose toggle
        button text equals ``accordion_name`` (normalized).

        Why this exists instead of a plain string selector:

        Playwright's ``:text-is()`` and ``:text-matches("^X$")``
        engines don't match BetMGM's toggle buttons reliably — the
        button's rendered text contains hidden child content (avatars,
        period chips, etc.) that breaks Playwright's text
        normalization. Verified 2026-05-20 against Spurs @ Thunder:
        ``button[dsaccordiontoggle]:text-is("Player points")`` returns
        0 matches even though ``button.innerText.trim() === "Player
        points"`` in the JS evaluator.

        The accordion expander already works around this by iterating
        ``button[dsaccordiontoggle]`` and filtering by normalized
        ``text_content`` on the Python side. We do the same here, then
        walk up to the surrounding ``ds-accordion`` via
        ``xpath=ancestor::``. Picks scoped to that accordion are
        guaranteed to live inside one market panel, not bleed across
        siblings.

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
                return candidates.nth(i).locator('xpath=ancestor::ds-accordion[1]')
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

    def _click_betmgm_alt_yes_pick_for_player(self, player_name: str, accordion_name: str) -> bool:
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
            print(f"[BETMGM] alt-mode: no accordion match for {accordion_name!r}")
            return False
        all_picks = acc.locator('ms-event-pick')
        pick_count = all_picks.count()
        print(f"[BETMGM] alt-mode: scanning {pick_count} pick(s) inside "
              f"{accordion_name!r} for player {player_name!r}")

        # Same walkup helper as the std path — fuzzy-match the player
        # name against ancestor row text to absorb apostrophe variants
        # (curly vs straight) and casing differences.
        walkup_js = """
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

        matched_handle = None
        matched_option_id = None
        matched_meta = None

        for i in range(pick_count):
            try:
                pick = all_picks.nth(i)
                txt = (pick.text_content() or "").strip()
                ancestor_texts = pick.evaluate(
                    walkup_js,
                    {"max_depth": 15, "max_text_len": 600},
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
                matched_meta = f"text={txt!r} option_id={option_id!r}"
                break
            except Exception as e:
                print(f"[BETMGM] alt pick #{i} scan error: {e}")
                continue

        if matched_handle is None:
            print(f"[BETMGM] alt-mode: no row matched {player_name!r} "
                  f"(scanned {pick_count} pick(s))")
            return False

        print(f"[BETMGM] alt-mode matched: {matched_meta}")
        try:
            if matched_option_id:
                target = self.page.locator(
                    f'ms-event-pick[data-test-option-id="{matched_option_id}"]'
                )
                target.first.click(timeout=10000)
            else:
                matched_handle.click(timeout=10000)
            self.page.wait_for_timeout(1500)
            self._screenshot("alt_bet_clicked")
            print(f"[BETMGM] ✓ Alt-mode bet added to slip")
            return True
        except Exception as e:
            self._screenshot("alt_click_failed")
            raise BetPlacerError(f"Failed to click BetMGM alt bet: {e}")

    def enter_wager(self, amount):
        """Enter wager amount in the betslip."""
        print(f"[BETMGM] Entering wager: ${amount:.2f}")
        return self._enter_wager_betmgm(amount)

    def _enter_wager_betmgm(self, amount: float) -> bool:
        """Enter wager on BetMGM."""
        try:
            # BetMGM docks the slip at the bottom of the screen, collapsed
            # to a "1 Bet slip — $X.XX pays out $Y.YY" pill. The wager
            # input only mounts when the slip is expanded. Click the pill
            # (or any visible "pays out"/"Bet slip" affordance) to expand.
            self._open_betmgm_slip()

            # BetMGM stake input — historically `app-stake-input input`,
            # but the prefix and component name drift. Broaden the cascade
            # to durable signals: inputmode=decimal, aria-label / placeholder
            # patterns, and the data-testid families we've seen.
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

            # When slip-clear fails and multiple bets accumulate, the slip
            # has multiple stake inputs and we must enter the wager into
            # the one for the just-added bet. Heuristic: prefer the LAST
            # visible EMPTY stake input (just-added bets have no value;
            # previously-filled bets retain their value across iterations).
            wager_input = None
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
                    empty_inputs = [el for el, v in visible_inputs if not v.strip()]
                    if empty_inputs:
                        wager_input = empty_inputs[-1]  # last empty = just-added bet
                        print(f"[BETMGM] Found wager input via {selector} "
                              f"(picked last empty of {len(visible_inputs)})")
                    else:
                        wager_input = visible_inputs[-1][0]  # last filled — best guess
                        print(f"[BETMGM] Found wager input via {selector} "
                              f"(all {len(visible_inputs)} filled; picked last)")
                    break
                except Exception:
                    continue

            if wager_input is None:
                # Diagnostic dump on miss — what visible inputs DO exist?
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

            # Enter the amount via individual keystrokes. BetMGM uses a
            # custom Angular numpad widget; .fill() sets the input value
            # in DOM but doesn't fire the keydown events the widget's
            # form-state machine listens for, so the Place Bet button
            # stays aria-disabled='true'. .pw_type() generates real
            # keystroke events that the widget accepts.
            wager_input.click()
            self.page.wait_for_timeout(200)
            # Clear any existing content first (Ctrl+A, Delete) so the
            # new digits don't get appended.
            self.page.keyboard.press("Control+A")
            self.page.keyboard.press("Delete")
            self.page.wait_for_timeout(200)
            amount_str = f"{amount:.2f}"
            # Use keyboard.type with a small per-char delay so each
            # digit triggers a clean keypress that the numpad widget
            # processes individually.
            self.page.keyboard.type(amount_str, delay=80)
            self.page.wait_for_timeout(500)

            # Press Tab/ArrowDown to blur the input and trigger validation
            # — needed so the Place Bet button transitions from disabled
            # to enabled.
            self.page.keyboard.press("Tab")
            self.page.wait_for_timeout(1000)

            self._screenshot("wager_entered")
            print(f"[BETMGM] ✓ Wager entered: ${amount:.2f}")
            return True

        except Exception as e:
            self._screenshot("wager_entry_failed")
            raise BetPlacerError(f"Failed to enter wager: {e}")

    def place_bet(self):
        """Click the Place Bet button and check for success/failure."""
        print(f"[BETMGM] Placing bet...")
        return self._place_bet_betmgm()

    def _place_bet_betmgm(self) -> Tuple[str, str]:
        """Place bet on BetMGM."""
        try:
            # BetMGM: button with "Place Bet" text
            place_btn = self.page.get_by_role("button", name=re.compile(r"Place\s+Bet", re.I))

            if place_btn.count() == 0:
                self._screenshot("place_bet_not_found")
                raise BetPlacerError("Place Bet button not found")

            print(f"[BETMGM] Clicking Place Bet...")
            place_btn.first.click()
            self.page.wait_for_timeout(2000)

            # Check for success/failure - poll for result
            for _ in range(10):  # 5 seconds max
                # Check for success: "Your bet has been accepted" in pc-richtext section
                accepted_msg = self.page.get_by_text("Your bet has been accepted")
                if accepted_msg.count() > 0 and accepted_msg.first.is_visible():
                    self._screenshot("bet_placed_success")
                    print(f"[BETMGM] ✓ Bet ACCEPTED")
                    self._close_betslip_betmgm()
                    return "ACCEPTED", "Your bet has been accepted"

                # Alternative success messages
                alt_success = self.page.get_by_text(re.compile(r"Bet Placed|Wager Accepted", re.I))
                if alt_success.count() > 0 and alt_success.first.is_visible():
                    self._screenshot("bet_placed_success")
                    print(f"[BETMGM] ✓ Bet ACCEPTED")
                    self._close_betslip_betmgm()
                    return "ACCEPTED", "Bet placed successfully"

                # Check for error messages
                error_msg = self.page.get_by_text(re.compile(r"limit exceeded|Error|rejected", re.I))
                if error_msg.count() > 0 and error_msg.first.is_visible():
                    msg = error_msg.first.text_content() or "Unknown error"
                    self._screenshot("bet_rejected")
                    print(f"[BETMGM] ✗ Bet REJECTED: {msg}")
                    return "REJECTED", msg

                self.page.wait_for_timeout(500)

            # Unknown state
            self._screenshot("bet_status_unknown")
            print(f"[BETMGM] ? Bet status UNKNOWN")
            return "UNKNOWN", "Could not determine bet status"

        except Exception as e:
            self._screenshot("place_bet_failed")
            raise BetPlacerError(f"Place bet failed: {e}")

    def _close_betslip_betmgm(self):
        """Close the betslip after a successful bet on BetMGM."""
        try:
            # From recording: aria/Close or bs-linear-result-summary button
            close_selectors = [
                'bs-linear-result-summary button',
                '[aria-label="Close"]',
            ]

            for selector in close_selectors:
                try:
                    close_btn = self.page.locator(selector)
                    if close_btn.count() > 0 and close_btn.first.is_visible():
                        print(f"[BETMGM] Closing betslip...")
                        close_btn.first.click()
                        self.page.wait_for_timeout(500)
                        print(f"[BETMGM] ✓ Betslip closed")
                        return
                except Exception:
                    continue

            print(f"[BETMGM] ⚠ Could not find close button, continuing anyway...")
        except Exception as e:
            print(f"[BETMGM] ⚠ Error closing betslip: {e}")

    def get_actual_odds(self):
        """Extract actual odds from BetMGM betslip.

        BetMGM displays decimal odds in: span.odds-indicator__lite--default

        Returns:
            Decimal odds as float, or None if not found
        """
        try:
            # Primary selector for BetMGM odds
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

                        # Parse decimal odds (e.g., "1.75")
                        decimal_match = re.search(r'(\d+\.?\d*)', text)
                        if decimal_match:
                            decimal_odds = float(decimal_match.group(1))
                            print(f"[BETMGM] Extracted odds: {decimal_odds:.3f}")
                            return decimal_odds
                except Exception:
                    continue

            print(f"[BETMGM] ⚠ Could not extract odds from betslip")
            return None

        except Exception as e:
            print(f"[BETMGM] ⚠ Error extracting odds: {e}")
            return None

    def check_limit_alert(self):
        """Check if BetMGM shows the max limit alert and get adjusted stake.

        When the requested bet exceeds BetMGM's limit, they show an alert:
        "Your requested bet is over the allowed limit. The maximum stake has been adjusted..."

        Returns:
            (limit_hit: bool, adjusted_stake: float or None)
        """
        try:
            # Check for the limit alert message
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

                            # Extract the adjusted stake from betslip summary
                            stake_selectors = [
                                'span.betslip-summary-value',
                                '.betslip-summary-value',
                            ]

                            for stake_selector in stake_selectors:
                                stake_elem = self.page.locator(stake_selector).first
                                if stake_elem.count() > 0:
                                    stake_text = stake_elem.text_content() or ""
                                    # Parse "$6.76" format
                                    stake_match = re.search(r'\$?([\d,]+\.?\d*)', stake_text)
                                    if stake_match:
                                        adjusted_stake = float(stake_match.group(1).replace(',', ''))
                                        print(f"[BETMGM] Adjusted stake: ${adjusted_stake:.2f}")
                                        self._screenshot("limit_alert_detected")
                                        return True, adjusted_stake

                            # Alert found but couldn't parse stake
                            print(f"[BETMGM] ⚠ Could not parse adjusted stake")
                            self._screenshot("limit_alert_no_stake")
                            return True, None
                except Exception:
                    continue

            return False, None

        except Exception as e:
            print(f"[BETMGM] ⚠ Error checking limit alert: {e}")
            return False, None
