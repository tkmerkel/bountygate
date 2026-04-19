"""
Semi-Automated Selector Mapping Tool
Discovers and validates element selectors for sportsbook sites.

Usage:
    python map_selectors.py --site betmgm --market player_points
    python map_selectors.py --site fanduel --market player_assists
"""

import argparse
import time
from typing import Optional, Dict

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from selector_finder import SelectorFinder, SelectorManager, is_alternate_market, calculate_alternate_tab_value, get_base_market_key
from db_connection import fetch_data
from chrome_helpers import CDP_PORT, profile_dir, ensure_chrome_cdp


# Site-specific navigation patterns
SITE_CONFIG = {
    "fanduel": {
        "base_url": "https://mo.sportsbook.fanduel.com/search",
        "search_method": "player_name",
        "market_display_names": {
            "player_points": ["Points", "Player Points"],
            "player_assists": ["Assists", "Player Assists"],
            "player_rebounds": ["Rebounds"],
            "player_threes": ["Made Threes"],
            "player_blocks": ["Blocks", "Player Blocks"],
            "player_steals": ["Steals", "Player Steals"],
            "player_turnovers": ["Turnovers", "Player Turnovers"],
            "player_points_rebounds": ["Pts + Reb", "Points + Rebounds"],
            "player_points_assists": ["Pts + Ast"],
            "player_rebounds_assists": ["Reb + Ast", "Rebounds + Assists"],
            "player_points_rebounds_assists": ["Pts + Reb + Ast", "Points + Rebounds + Assists"],
            "player_receptions": ["Total Receptions"],
            "player_rush_attempts": ["Rush Attempts"],
            "player_tackles_assists": ["Tackles + Assists"],
            "player_pass_completions": ["Pass Completions"],
            # Alternate markets (use same display names, different search strategy)
            "player_points_alternate": ["Points"],
            "player_assists_alternate": ["Assists"],
            "player_rebounds_alternate": ["Rebounds"],
            "player_threes_alternate": ["Made Threes"],
            "player_blocks_alternate": ["Blocks"],
            "player_steals_alternate": ["Steals"],
            # ----- MLB Batter markets (standard O/U) -----
            "batter_hits": ["Hits"],
            "batter_singles": ["Singles"],
            "batter_doubles": ["Doubles"],
            "batter_triples": ["Triples"],
            "batter_home_runs": ["Home Runs"],
            "batter_total_bases": ["Total Bases"],
            "batter_rbis": ["RBIs"],
            "batter_runs": ["Runs"],
            "batter_stolen_bases": ["Stolen Bases"],
            "batter_strikeouts": ["Strikeouts"],
            "batter_walks": ["Walks"],
            "batter_hits_runs_rbis": ["Hits + Runs + RBIs"],
            # ----- MLB Batter markets (alternate) -----
            "batter_hits_alternate": ["Hits"],
            "batter_singles_alternate": ["Singles"],
            "batter_doubles_alternate": ["Doubles"],
            "batter_triples_alternate": ["Triples"],
            "batter_home_runs_alternate": ["Home Runs"],
            "batter_total_bases_alternate": ["Total Bases"],
            "batter_rbis_alternate": ["RBIs"],
            "batter_runs_alternate": ["Runs"],
            "batter_stolen_bases_alternate": ["Stolen Bases"],
            "batter_strikeouts_alternate": ["Strikeouts"],
            "batter_walks_alternate": ["Walks"],
            # ----- MLB Pitcher markets (standard O/U) -----
            "pitcher_strikeouts": ["Pitching Strikeouts", "Strikeouts"],
            "pitcher_hits_allowed": ["Hits Allowed"],
            "pitcher_walks": ["Pitching Walks", "Walks Allowed"],
            "pitcher_outs": ["Pitching Outs", "Outs Recorded"],
            "pitcher_earned_runs": ["Earned Runs"],
            # ----- MLB Pitcher markets (alternate) -----
            "pitcher_strikeouts_alternate": ["Pitching Strikeouts", "Strikeouts"],
            "pitcher_hits_allowed_alternate": ["Hits Allowed"],
            "pitcher_walks_alternate": ["Pitching Walks", "Walks Allowed"],
            "pitcher_outs_alternate": ["Pitching Outs", "Outs Recorded"],
            "pitcher_earned_runs_alternate": ["Earned Runs"],
        }
    },
    "betmgm": {
        "base_url": "https://www.mo.betmgm.com/en/sports",
        "search_method": "team_then_player",
        "market_display_names": {
            "player_points": ["Player points O/U"],
            "player_assists": ["Player assists O/U", "Assists"],
            "player_rebounds": ["Player rebounds O/U"],
            "player_threes": ["Player three-pointers O/U"],
            "player_blocks": ["Player blocks"],
            "player_steals": ["Total Steals", "Player Total Steals", "Steals"],
            "player_points_rebounds": ["Player points + rebounds"],
            "player_points_assists": ["Player points + assists"],
            "player_rebounds_assists": ["Player rebounds + assists"],
            "player_points_rebounds_assists": ["Player points + rebounds + assists"],
            "player_receptions": ["Receptions made"],
            "player_rush_attempts": ["Rushing Attempts"],
            "player_tackles_assists": ["Tackles + Assists"],
            "player_pass_completions": ["Pass completions"],
            # Alternate markets (use different accordion names, have threshold tabs)
            "player_points_alternate": ["Points"],
            "player_assists_alternate": ["Assists"],
            "player_rebounds_alternate": ["Rebounds"],
            "player_threes_alternate": ["Three-pointers"],
            "player_blocks_alternate": ["Blocks"],
            "player_steals_alternate": ["Steals"],
            # ----- MLB Batter markets (standard O/U) -----
            "batter_hits": ["Player hits O/U"],
            "batter_singles": ["Player singles O/U"],
            "batter_doubles": ["Player doubles O/U"],
            "batter_triples": ["Player triples O/U"],
            "batter_home_runs": ["Player home runs O/U"],
            "batter_total_bases": ["Player total bases O/U"],
            "batter_rbis": ["Player RBIs O/U"],
            "batter_runs": ["Player runs O/U"],
            "batter_stolen_bases": ["Player stolen bases O/U"],
            "batter_strikeouts": ["Player strikeouts O/U", "Batter strikeouts O/U"],
            "batter_walks": ["Player walks O/U"],
            "batter_hits_runs_rbis": ["Player hits + runs + RBIs"],
            # ----- MLB Batter markets (alternate) -----
            "batter_hits_alternate": ["Hits"],
            "batter_singles_alternate": ["Singles"],
            "batter_doubles_alternate": ["Doubles"],
            "batter_triples_alternate": ["Triples"],
            "batter_home_runs_alternate": ["Home Runs"],
            "batter_total_bases_alternate": ["Total Bases"],
            "batter_rbis_alternate": ["RBIs"],
            "batter_runs_alternate": ["Runs"],
            "batter_stolen_bases_alternate": ["Stolen Bases"],
            "batter_strikeouts_alternate": ["Strikeouts"],
            "batter_walks_alternate": ["Walks"],
            # ----- MLB Pitcher markets (standard O/U) -----
            "pitcher_strikeouts": ["Pitcher strikeouts O/U"],
            "pitcher_hits_allowed": ["Pitcher hits allowed O/U"],
            "pitcher_walks": ["Pitcher walks O/U"],
            "pitcher_outs": ["Pitcher outs O/U"],
            "pitcher_earned_runs": ["Pitcher earned runs O/U"],
            # ----- MLB Pitcher markets (alternate) -----
            "pitcher_strikeouts_alternate": ["Pitcher Strikeouts"],
            "pitcher_hits_allowed_alternate": ["Hits Allowed"],
            "pitcher_walks_alternate": ["Pitcher Walks"],
            "pitcher_outs_alternate": ["Pitcher Outs"],
            "pitcher_earned_runs_alternate": ["Earned Runs"],
        },
        # Alternate market accordion names (different from O/U markets)
        "alternate_accordion_names": {
            "player_points_alternate": "Points",
            "player_assists_alternate": "Assists",
            "player_rebounds_alternate": "Rebounds",
            "player_threes_alternate": "Three-pointers",
            "player_blocks_alternate": "Blocks",
            "player_steals_alternate": "Steals",
            # MLB batter alternates
            "batter_hits_alternate": "Hits",
            "batter_singles_alternate": "Singles",
            "batter_doubles_alternate": "Doubles",
            "batter_triples_alternate": "Triples",
            "batter_home_runs_alternate": "Home Runs",
            "batter_total_bases_alternate": "Total Bases",
            "batter_rbis_alternate": "RBIs",
            "batter_runs_alternate": "Runs",
            "batter_stolen_bases_alternate": "Stolen Bases",
            "batter_strikeouts_alternate": "Strikeouts",
            "batter_walks_alternate": "Walks",
            # MLB pitcher alternates
            "pitcher_strikeouts_alternate": "Pitcher Strikeouts",
            "pitcher_hits_allowed_alternate": "Hits Allowed",
            "pitcher_walks_alternate": "Pitcher Walks",
            "pitcher_outs_alternate": "Pitcher Outs",
            "pitcher_earned_runs_alternate": "Earned Runs",
        }
    }
}

def fetch_opportunity_for_market(market_key: str, bookmaker: str) -> Optional[Dict]:
    """
    Fetch a real opportunity from DB for the specified market and bookmaker.

    Args:
        market_key: Market to fetch (e.g., 'player_points')
        bookmaker: Bookmaker to filter for (e.g., 'fanduel', 'betmgm')

    Returns:
        Opportunity dict or None if not found
    """
    # Query for recent opportunity with this market and bookmaker
    query = f"""
    SELECT player_name,
           sport_title,
           home_team,
           away_team,
           market_key,
           under_line,
           over_line,
           under_bookmaker_key,
           over_bookmaker_key,
           under_price,
           over_price
    FROM bg_arbitrage_player_props
    WHERE market_key = '{market_key}'
      AND (under_bookmaker_key = '{bookmaker}' OR over_bookmaker_key = '{bookmaker}')
      AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '4 hours'
      AND hours_until_commence > 0
      AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
    LIMIT 1;
    """

    print(f"Fetching opportunity for market: {market_key}, bookmaker: {bookmaker}")
    df = fetch_data(query)

    if df is None or df.empty:
        # Try alt table
        query_alt = query.replace("bg_arbitrage_player_props", "bg_arbitrage_player_props_alt")
        df = fetch_data(query_alt)

    if df is None or df.empty:
        print(f"❌ No recent opportunities found for market: {market_key}, bookmaker: {bookmaker}")
        print(f"   Try expanding the time window or check if this market is currently active")
        return None

    opportunity = df.iloc[0].to_dict()
    print(f"✓ Found opportunity: {opportunity['player_name']} - {opportunity['market_key']}")
    print(f"  {opportunity['away_team']} @ {opportunity['home_team']}")
    print(f"  Line: {opportunity['over_line']}")
    print(f"  Bookmakers: {opportunity['under_bookmaker_key']} vs {opportunity['over_bookmaker_key']}")
    return opportunity


def navigate_fanduel(page, player_name: str):
    """Navigate FanDuel to player props."""
    print(f"Navigating to FanDuel search page...")
    page.goto("https://mo.sportsbook.fanduel.com/search", wait_until="domcontentloaded")

    # Wait for search input (using selector from DevTools recording)
    try:
        search_input = page.locator('input[placeholder="Search"], div.aq input').first
        search_input.wait_for(state="visible", timeout=15000)
        print(f"✓ Found search input")
    except Exception:
        raise RuntimeError("Could not find FanDuel search input")

    # Search for player
    print(f"Searching for: {player_name}")
    search_input.fill(player_name)
    page.keyboard.press("Enter")

    # Wait for results to load
    page.wait_for_timeout(3000)
    print("✓ Search results loaded")


def navigate_betmgm(page, home_team: str, away_team: str):
    """Navigate BetMGM to player props for event."""
    print(f"Navigating to BetMGM...")
    page.goto("https://www.mo.betmgm.com/en/sports?popup=betfinder", wait_until="domcontentloaded")

    page.wait_for_timeout(2000)

    # Try to find and click search/bet finder
    try:
        # Look for search input or bet finder
        search_input = page.locator('input[placeholder*="Search"], input[placeholder*="Find"]').first
        if search_input.is_visible():
            print(f"Searching for: {home_team}")
            search_input.fill(home_team)
            page.keyboard.press("Enter")
            page.wait_for_timeout(3000)
            print("✓ Search results loaded")
    except Exception as e:
        print(f"Search method failed: {e}")
        print("You may need to manually navigate to the event...")

    # Look for "All Wagers" link in the event results
    try:
        # Strategy 1: Find search result cards and match by team names
        print(f"Looking for event: {away_team} @ {home_team}")
        time.sleep(2)
        # Find all search result cards
        result_cards = page.locator('ms-grid-search-result-card')
        card_count = result_cards.count()

        print(f"Found {card_count} search result card(s)")

        if card_count > 0:
            # Score cards: prefer both teams, skip futures
            scored_cards = []
            for i in range(card_count):
                card = result_cards.nth(i)
                card_text = (card.text_content() or "").lower()

                if "future" in card_text:
                    print(f"  Skipping futures card #{i+1}")
                    continue

                has_home = home_team.lower() in card_text
                has_away = away_team.lower() in card_text
                score = int(has_home) + int(has_away)
                if score > 0:
                    scored_cards.append((score, i, card))

            # Sort by score descending — prefer cards with both teams
            scored_cards.sort(key=lambda x: x[0], reverse=True)

            for score, idx, card in scored_cards:
                print(f"✓ Found matching card #{idx+1} (score={score})")

                # Try multiple selectors for "All Wagers" link
                all_wagers_selectors = [
                    'span.title:has-text("All Wagers")',
                    'ms-event-footer span:has-text("All Wagers")',
                    'span:has-text("All Wagers")',
                ]
                all_wagers_in_card = None
                for aw_selector in all_wagers_selectors:
                    loc = card.locator(aw_selector)
                    if loc.count() > 0:
                        all_wagers_in_card = loc
                        print(f"  Found 'All Wagers' using: {aw_selector}")
                        break

                if all_wagers_in_card is not None:
                    print(f"Clicking 'All Wagers' in matching card...")
                    all_wagers_in_card.first.click()
                    page.wait_for_timeout(2000)

                    # Add ?market=PlayerProps to URL
                    current_url = page.url
                    if "?market=PlayerProps" not in current_url and "market=PlayerProps" not in current_url:
                        new_url = current_url + ("&" if "?" in current_url else "?") + "market=PlayerProps"
                        print(f"Navigating to: {new_url}")
                        page.goto(new_url, wait_until="domcontentloaded")
                        page.wait_for_timeout(2000)

                    print("✓ Navigated to player props")
                    return
                else:
                    print(f"⚠ Card matched but no 'All Wagers' link found inside")

            # If we get here, no card worked
            print(f"⚠ No usable cards found for: {home_team} or {away_team}")
            print(f"Card 1 preview: {result_cards.first.text_content()[:100] if card_count > 0 else 'N/A'}")

    except Exception as e:
        print(f"Strategy 1 failed: {e}")

    # Strategy 2: Fallback - look for clickable event elements
    try:
        print("Trying alternate navigation method...")
        event_containers = page.locator(f'*:has-text("{away_team}"):has-text("{home_team}")')

        if event_containers.count() > 0:
            print(f"Found event container, looking for clickable element...")
            # Try to find a clickable element within
            clickable = event_containers.first.locator('a, button, [role="button"]').first
            if clickable.count() > 0:
                clickable.click()
                page.wait_for_timeout(2000)

                # Add ?market=PlayerProps to URL
                current_url = page.url
                if "?market=PlayerProps" not in current_url:
                    new_url = current_url + ("&" if "?" in current_url else "?") + "market=PlayerProps"
                    print(f"Navigating to: {new_url}")
                    page.goto(new_url, wait_until="domcontentloaded")
                    page.wait_for_timeout(2000)

                print("✓ Navigated to player props")
                return

    except Exception as e:
        print(f"Strategy 2 failed: {e}")

    # If all strategies fail, ask user to navigate manually
    print("\n❌ Could not auto-navigate to event")
    print("Please manually:")
    print("  1. Click on the event (All Wagers link)")
    print("  2. Add ?market=PlayerProps to the URL or select 'Players' from the market filter")
    print("  3. Press Enter to continue...")
    input()


def expand_betmgm_accordions(page, display_names: list, player_name: str):
    """Expand BetMGM accordion sections and 'Show More' buttons to reveal all props."""
    print("Expanding BetMGM accordions and 'Show More' sections...")

    try:
        # Find accordion headers matching our market display names
        for display_name in display_names:
            try:
                # Look for accordion with this market name
                accordion_header = page.locator(f'button[dsaccordiontoggle]:has-text("{display_name}")')

                if accordion_header.count() > 0:
                    print(f"  Found accordion: '{display_name}'")

                    # Always click to expand (BetMGM accordion state detection is unreliable)
                    print(f"    Expanding...")
                    accordion_header.first.click()
                    page.wait_for_timeout(1500)

                    # Now look for "Show More" button within this accordion
                    # Find the parent accordion container
                    parent_accordion = accordion_header.first.locator('xpath=ancestor::ds-accordion[1]')

                    if parent_accordion.count() > 0:
                        # Look for "Show More" within this accordion
                        show_more = parent_accordion.locator('ms-option-panel-bottom-action:has-text("Show More")')

                        attempts = 0
                        while show_more.count() > 0 and attempts < 5:
                            print(f"    Clicking 'Show More' (attempt {attempts + 1})...")
                            show_more.first.click()
                            page.wait_for_timeout(1000)

                            # Check if there's still a "Show More" button (some markets have multiple pages)
                            show_more = parent_accordion.locator('ms-option-panel-bottom-action:has-text("Show More")')
                            attempts += 1

                        print(f"    ✓ All players revealed")

                    break  # Found and expanded the right accordion

            except Exception as e:
                print(f"    Error with '{display_name}': {e}")
                continue

        print("✓ Accordion expansion complete\n")

    except Exception as e:
        print(f"⚠ Error expanding accordions: {e}")


def interactive_selector_mapping(site: str, market_key: str):
    """Main interactive flow for mapping selectors."""
    print("\n" + "=" * 60)
    print(f"SELECTOR MAPPING: {site.upper()} - {market_key}")
    print("=" * 60 + "\n")

    # Check if already mapped
    if SelectorManager.has_market(site, market_key):
        existing = SelectorManager.get_market(site, market_key)
        print(f"⚠ Market already mapped!")
        print(f"  Validated at: {existing.get('validated_at', 'unknown')}")
        print(f"  Selector: {existing.get('selector_pattern', 'N/A')}")
        response = input("\nRe-map this market? (y/n): ").strip().lower()
        if response != 'y':
            print("Skipping.")
            return

    # Fetch opportunity for this bookmaker
    opportunity = fetch_opportunity_for_market(market_key, site)
    if not opportunity:
        print(f"\n❌ Cannot map without a real opportunity for {site} - {market_key}.")
        print(f"   Make sure there are recent opportunities involving {site} in the database.")
        return

    player_name = opportunity['player_name']
    line = opportunity['over_line']
    home_team = opportunity['home_team']
    away_team = opportunity['away_team']

    # Get display names for this market
    site_config = SITE_CONFIG[site]
    display_names = site_config['market_display_names'].get(market_key, [market_key])

    print(f"\nWill search for terms: {display_names}")
    print(f"Player: {player_name}, Line: {line}")

    # Setup browser
    endpoint_url = ensure_chrome_cdp(profile_dir, CDP_PORT)

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(endpoint_url)
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = context.new_page()

        # Set viewport based on site (from DevTools recordings)
        if site == "betmgm":
            page.set_viewport_size({"width": 958, "height": 944})
            print("Set BetMGM viewport: 958x944")
        elif site == "fanduel":
            page.set_viewport_size({"width": 943, "height": 944})
            print("Set FanDuel viewport: 943x944")

        # Navigate to site
        print("\n" + "-" * 60)
        if site == "fanduel":
            navigate_fanduel(page, player_name)
        elif site == "betmgm":
            navigate_betmgm(page, home_team, away_team)
            # BetMGM-specific: expand accordions and "Show More" buttons
            expand_betmgm_accordions(page, display_names, player_name)
        else:
            print(f"❌ Unknown site: {site}")
            return

        print("-" * 60 + "\n")

        # Check if this is an alternate market
        is_alternate = is_alternate_market(market_key)

        # Site-specific mapping logic
        if site == "betmgm":
            if is_alternate:
                # BetMGM alternate market: test accordion + tab selection flow
                print("Mapping BetMGM ALTERNATE market...\n")
                config = _map_betmgm_alternate(page, market_key, display_names, player_name, line)
            else:
                # BetMGM regular: Map the accordion, not individual bets
                print("Mapping BetMGM market accordion...\n")
                config = _map_betmgm_regular(page, market_key, display_names, player_name, line)

        elif site == "fanduel":
            if is_alternate:
                # FanDuel alternate market: test threshold-based selector pattern
                print("Mapping FanDuel ALTERNATE market...\n")
                config = _map_fanduel_alternate(page, market_key, display_names, player_name, line)
            else:
                # FanDuel regular: Direct selector mapping
                print("Mapping FanDuel selector...\n")
                config = _map_fanduel_regular(page, market_key, display_names, player_name, line)

        if config is None:
            return

        # Save to YAML
        print("\nSaving configuration...")
        success = SelectorManager.save_market_config(site, market_key, config)

        if success:
            print(f"✓ Saved to selectors/{site}_markets.yaml")
            print(f"\nYou can now execute arbitrage opportunities for {site} {market_key}")
        else:
            print("❌ Failed to save configuration")


def _map_betmgm_regular(page, market_key: str, display_names: list, player_name: str, line: float) -> Optional[Dict]:
    """Map a regular BetMGM market (accordion-based O/U markets)."""
    # Find the accordion selector
    accordion_name = display_names[0]  # e.g., "Player points O/U"
    accordion_selector = f'button[dsaccordiontoggle]:has-text("{accordion_name}")'

    print(f"Accordion: {accordion_name}")
    print(f"Selector: {accordion_selector}\n")

    # Test: Can we find a specific player + line dynamically?
    print(f"Testing dynamic search for: {player_name}, line {line}...")

    # Try to find the bet using the search strategy
    test_candidates = SelectorFinder.find_candidates_by_text(
        page, display_names, player_name, line
    )

    if not test_candidates:
        print("❌ Could not find bet buttons for test player/line.")
        print("The accordion may not be fully expanded or search strategy needs adjustment.")
        response = input("Save accordion info anyway? (y/n): ").strip().lower()
        if response != 'y':
            return None
        search_validated = False
    else:
        print(f"✓ Found {len(test_candidates)} bet button(s) for {player_name}")

        # Show first candidate as example
        example = test_candidates[0]
        print(f"\nExample bet button found:")
        print(f"  Type: {example.selector_type}")
        print(f"  Preview: {example.preview_text[:80]}")

        # Test clicking it
        print("\nTesting click on example bet...")
        click_success, click_msg = SelectorFinder.test_click_selector(page, example.selector)

        if click_success:
            print(f"✓ {click_msg}")
            search_validated = True
        else:
            print(f"⚠ {click_msg}")
            response = input("Save anyway? (y/n): ").strip().lower()
            if response != 'y':
                return None
            search_validated = False

    # Return BetMGM market config
    return {
        "accordion_name": accordion_name,
        "accordion_selector": accordion_selector,
        "show_more_selector": 'ms-option-panel-bottom-action:has-text("Show More")',
        "bet_element_type": "ms-event-pick",
        "search_strategy": "player_container_then_line",
        "search_validated": search_validated,
        "test_player": player_name,
        "test_line": float(line)
    }


def _map_betmgm_alternate(page, market_key: str, display_names: list, player_name: str, line: float) -> Optional[Dict]:
    """Map a BetMGM alternate market (accordion + threshold tabs)."""
    accordion_name = display_names[0]  # e.g., "Assists" (not "Player assists O/U")
    accordion_selector = f'button[dsaccordiontoggle]:has-text("{accordion_name}")'

    print(f"Alternate Accordion: {accordion_name}")
    print(f"Selector: {accordion_selector}\n")

    # Calculate threshold tab value
    tab_value = calculate_alternate_tab_value(line)
    tab_text = f"{tab_value}+"

    print(f"Testing threshold tab: {tab_text} (line: {line})")

    # Test tab selector patterns
    tab_selectors = [
        f'button:has-text("{tab_text}")',
        f'[role="tab"]:has-text("{tab_text}")',
        f'div[role="tablist"] button:has-text("{tab_text}")',
    ]

    tab_found = False
    working_tab_selector = None

    for selector in tab_selectors:
        try:
            tab = page.locator(selector)
            if tab.count() > 0 and tab.first.is_visible():
                print(f"✓ Found tab using: {selector}")
                working_tab_selector = selector
                tab_found = True
                break
        except Exception as e:
            print(f"  Tab selector failed: {selector} - {e}")
            continue

    if not tab_found:
        print(f"⚠ Could not find tab '{tab_text}'")
        response = input("Save config anyway? (y/n): ").strip().lower()
        if response != 'y':
            return None
        search_validated = False
    else:
        # Click the tab and test finding a player bet
        try:
            page.locator(working_tab_selector).first.click()
            page.wait_for_timeout(1000)
            print(f"✓ Tab clicked successfully")

            # Now test finding a player bet
            test_candidates = SelectorFinder.find_candidates_by_text(
                page, display_names, player_name, tab_value
            )

            if test_candidates:
                print(f"✓ Found {len(test_candidates)} bet button(s) for {player_name}")
                search_validated = True
            else:
                print(f"⚠ Could not find bets after tab selection")
                search_validated = False

        except Exception as e:
            print(f"⚠ Tab click/search failed: {e}")
            search_validated = False

    base_market = get_base_market_key(market_key)

    return {
        "accordion_name": accordion_name,
        "accordion_selector": accordion_selector,
        "tab_selector_pattern": f'button:has-text("{{threshold}}+")',
        "show_more_selector": 'ms-option-panel-bottom-action:has-text("Show More")',
        "bet_element_type": "ms-event-pick",
        "search_strategy": "alternate_tab_then_player",
        "is_alternate": True,
        "has_threshold_tabs": True,
        "base_market": base_market,
        "search_validated": search_validated,
        "test_player": player_name,
        "test_line": float(line)
    }


def _map_fanduel_regular(page, market_key: str, display_names: list, player_name: str, line: float) -> Optional[Dict]:
    """Map a regular FanDuel market (aria-label based selection)."""
    # Discover candidate selectors
    print("Scanning page for candidate selectors...")
    candidates = SelectorFinder.find_candidates_by_text(
        page, display_names, player_name, line
    )

    if not candidates:
        print("❌ No candidates found. The page may not have loaded correctly.")
        return None

    print(f"\n✓ Found {len(candidates)} candidate(s):\n")

    # Display candidates
    for i, cand in enumerate(candidates, 1):
        print(f"[{i}] Confidence: {cand.confidence}%")
        print(f"    Type: {cand.selector_type}")
        print(f"    Selector: {cand.selector}")
        print(f"    Preview: {cand.preview_text[:80]}")
        print()

    # User selection
    while True:
        choice = input(f"Select candidate (1-{len(candidates)}) or 'q' to quit: ").strip()

        if choice.lower() == 'q':
            print("Cancelled.")
            return None

        try:
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(candidates):
                selected = candidates[choice_idx]
                break
            else:
                print(f"Invalid choice. Enter 1-{len(candidates)}")
        except ValueError:
            print("Invalid input. Enter a number or 'q'")

    print(f"\n✓ Selected: {selected.selector}\n")

    # Test clicking
    print("Testing click...")
    click_success, click_msg = SelectorFinder.test_click_selector(page, selected.selector)

    if not click_success:
        print(f"⚠ {click_msg}")
        response = input("Save anyway? (y/n): ").strip().lower()
        if response != 'y':
            return None

    print(f"✓ {click_msg}")

    return {
        "display_names": display_names,
        "selector_type": selected.selector_type,
        "selector_pattern": selected.selector,
        "search_strategy": "aria_label_match",
        "test_player": player_name,
        "test_line": float(line)
    }


def _map_fanduel_alternate(page, market_key: str, display_names: list, player_name: str, line: float) -> Optional[Dict]:
    """Map a FanDuel alternate market (threshold-based selection like "4+ Points")."""
    threshold = calculate_alternate_tab_value(line)
    base_display = display_names[0] if display_names else 'Points'

    print(f"Testing alternate pattern: {player_name} {threshold}+ {base_display}")

    # Try various selector patterns for threshold-based bets
    selector_patterns = [
        f'[aria-label*="{player_name}"][aria-label*="{threshold}+"]',
        f'[aria-label*="{player_name}"][aria-label*="{threshold} or more"]',
        f'[aria-label*="{player_name}"][aria-label*="{threshold}"][aria-label*="{base_display}"]',
    ]

    found_selector = None
    for selector in selector_patterns:
        try:
            locator = page.locator(selector)
            if locator.count() > 0:
                print(f"✓ Found using: {selector}")
                found_selector = selector
                break
        except Exception as e:
            print(f"  Pattern failed: {selector} - {e}")
            continue

    if not found_selector:
        # Fallback: try standard search with threshold
        print("Direct patterns failed, trying standard search...")
        candidates = SelectorFinder.find_candidates_by_text(
            page, display_names, player_name, threshold
        )

        if candidates:
            print(f"✓ Found {len(candidates)} candidate(s) via standard search")
            found_selector = candidates[0].selector
        else:
            print("❌ No candidates found for alternate market")
            return None

    # Test clicking
    print(f"\nTesting click on: {found_selector[:80]}...")
    click_success, click_msg = SelectorFinder.test_click_selector(page, found_selector)

    if not click_success:
        print(f"⚠ {click_msg}")
        response = input("Save anyway? (y/n): ").strip().lower()
        if response != 'y':
            return None

    print(f"✓ {click_msg}")

    base_market = get_base_market_key(market_key)

    return {
        "display_names": display_names,
        "selector_type": "aria_label",
        "search_strategy": "alternate_threshold_match",
        "is_alternate": True,
        "base_market": base_market,
        "test_player": player_name,
        "test_line": float(line)
    }


def main():
    parser = argparse.ArgumentParser(description="Semi-automated selector mapping tool")
    parser.add_argument("--site", required=True, choices=["fanduel", "betmgm"],
                       help="Sportsbook site to map")
    parser.add_argument("--market", required=True,
                       help="Market key to map (e.g., player_points, player_assists)")

    args = parser.parse_args()

    interactive_selector_mapping(args.site, args.market)


if __name__ == "__main__":
    main()
