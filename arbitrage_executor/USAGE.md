# Arbitrage Bot Usage Guide

## System Overview

Your arbitrage bot uses a **tease-probe-execute** strategy to safely place bet pairs on FanDuel and BetMGM:

1. **Tease**: Discover FanDuel's max wager limit (enter 99999)
2. **Probe**: Calculate safe BetMGM stake based on FanDuel limit
3. **Execute**: Place BetMGM bet first, then hedge on FanDuel

## Quick Start

### 1. Map Selectors (One-Time Setup)

Before you can execute bets, you need to map each market on each site:

```bash
# Map BetMGM markets
python map_selectors.py --site betmgm --market player_points
python map_selectors.py --site betmgm --market player_assists
python map_selectors.py --site betmgm --market player_rebounds

# Map FanDuel markets
python map_selectors.py --site fanduel --market player_points
python map_selectors.py --site fanduel --market player_assists
python map_selectors.py --site fanduel --market player_rebounds
```

The script will:
- Fetch a real opportunity for that market
- Navigate to the site
- Expand accordions (BetMGM) or search player (FanDuel)
- Show you candidate selectors
- Let you test-click to verify
- Save to `selectors/{site}_markets.yaml`

### 2. Execute Arbitrage

Once selectors are mapped:

```bash
# Set testing mode to find any recent opportunity
set TESTING_MODE=true

# Run execution
python execute_arb.py
```

The script will:
1. Fetch the best FanDuel/BetMGM opportunity from DB
2. Check if selectors are mapped (skip if not)
3. Open both sites in stealth Chrome
4. Discover FanDuel max wager
5. Place BetMGM bet
6. Place FanDuel hedge
7. Save audit trail to `audit_logs/{timestamp}_{player}_{market}/`

## File Structure

```
dev_bg_browser/
├── execute_arb.py              # Main orchestrator
├── bet_placer.py               # Site-specific bet placement logic
├── selector_finder.py          # Selector discovery utilities
├── map_selectors.py            # Selector mapping tool
├── opportunity.py              # DB fetching logic
├── execution_logger.py         # Logging utilities
├── selectors/
│   ├── fanduel_markets.yaml    # FanDuel market configs
│   └── betmgm_markets.yaml     # BetMGM market configs
├── logs/
│   ├── unmapped_markets.log    # Markets that need mapping
│   ├── execution_failures.log  # Failed executions
│   └── execution_success.log   # Successful trades
└── audit_logs/
    └── {timestamp}_{player}_{market}/
        ├── opportunity_info.json
        ├── fanduel_*.png
        └── betmgm_*.png
```

## Execution Flow Details

### Phase 1: Discover FanDuel Max Wager

```
1. Navigate to FanDuel search
2. Search for player
3. Click the bet (e.g., "Over 25.5 Points")
4. Enter 99999 in wager field
5. Parse "MAX WAGER $X" message
6. Save max wager amount
```

**Why?** FanDuel limits vary by market/player. We need to know the limit before calculating BetMGM stake.

### Phase 2: Place BetMGM Bet

```
1. Calculate safe BetMGM stake:
   max_mgm_stake = fd_max_wager / hedge_ratio
   actual_stake = min(planned_wager, max_mgm_stake)

2. Navigate to BetMGM event
3. Expand market accordion
4. Click "Show More" until all players visible
5. Find and click bet (e.g., "Under 25.5 Points")
6. Enter stake
7. Click "Place Bet"
8. Check for acceptance/rejection
```

**Why BetMGM first?** FanDuel prices move faster. Place the slower site first, then hedge on FanDuel.

### Phase 3: Place FanDuel Hedge

```
1. Calculate hedge stake:
   hedge_stake = (mgm_stake * mgm_price) / fd_price

2. Update FanDuel wager (bet already in slip from Phase 1)
3. Click "Place Bet"
4. Check for acceptance
```

**If hedge fails:** BetMGM bet is already placed! Manual intervention required.

## Handling Unmapped Markets

If execution encounters an unmapped market:

```
❌ SKIPPED: Selectors not mapped for betmgm - player_threes

Logged to: logs/unmapped_markets.log
```

To map it:
```bash
python map_selectors.py --site betmgm --market player_threes
```

## Troubleshooting

### "Could not find search input"
- Viewport may be wrong size (should be 943x944 for FanDuel, 958x944 for BetMGM)
- Site UI may have changed - re-run mapping tool

### "Accordion not found"
- Market name in YAML may not match site display name
- Check `selectors/betmgm_markets.yaml` accordion_name matches site

### "Bet not found for {player}"
- Player may not be in the market (injured, not playing)
- Line may have changed since opportunity was fetched
- Expand "Show More" may have failed - check screenshots in audit_logs/

### "MAX WAGER not found"
- FanDuel may not show limit for small amounts
- Bot assumes $99,999 unlimited if no limit detected

### "Hedge failure - manual intervention required"
- BetMGM bet placed but FanDuel hedge failed
- Check FanDuel manually and place hedge
- Review audit screenshots to see what happened

## Environment Variables

```bash
# Testing mode - fetch old opportunities
set TESTING_MODE=true

# Minimum ROI threshold (production mode only)
set MIN_ROI_THRESHOLD=0.01

# Wager scale factor (reduce stakes for testing)
set WAGER_SCALE_FACTOR=0.01
```

## Production Checklist

Before running in production with real money:

- [ ] All common markets mapped on both sites
- [ ] Testing mode successful execution (small stakes)
- [ ] `WAGER_SCALE_FACTOR=1.0` for full stakes
- [ ] `TESTING_MODE=false` for real-time opportunities
- [ ] `MIN_ROI_THRESHOLD` set appropriately (e.g., 0.01 = 1%)
- [ ] Monitoring set up for hedge failures
- [ ] Sufficient balance on both sites

## Tips

**Mapping efficiency:**
- Map markets in batches (all NBA markets at once)
- Run mapping when games are about to start (more opportunities)
- Validators validate on real data, so mapping during active hours is best

**Execution speed:**
- Keep Chrome open between executions (faster startup)
- Mapping validates selectors work, so execution should be fast (<10 seconds per leg)
- If too slow, check for "Show More" loops or navigation issues

**Risk management:**
- Start with `WAGER_SCALE_FACTOR=0.01` (1% of calculated stakes)
- Test each market with small stakes before going full size
- Always have enough balance for hedge (FanDuel max wager ≥ hedge amount)
