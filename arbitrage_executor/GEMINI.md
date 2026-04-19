# Arbitrage Betting Bot (dev-bg-browser)

## Project Overview
This project is an automated betting arbitrage bot designed to execute a "tease-probe-execute" strategy between FanDuel and BetMGM sportsbooks. It uses Playwright for browser automation, SQLAlchemy for database interactions, and a YAML-based selector mapping system to handle UI changes.

### Key Technologies
- **Python 3.11+**: Core logic.
- **Playwright**: Browser automation (using Chrome with CDP).
- **SQLAlchemy & PostgreSQL**: Data persistence (AWS RDS).
- **YAML**: Selector and market alias configurations.
- **Pandas**: Data manipulation for opportunities.

### Architecture
- **Orchestration**: `execute_arb.py` manages the end-to-end flow of finding and placing arbitrage bets.
- **Site Logic**: `bet_placer.py` contains site-specific logic for FanDuel and BetMGM.
- **Selector Management**: `map_selectors.py` is a tool for mapping and validating UI selectors for different markets.
- **Data Access**: `db_connection.py` handles all PostgreSQL interactions.
- **Business Logic**: `opportunity.py` fetches and prepares arbitrage opportunities from the database.

---

## Building and Running

### Prerequisites
- Python 3.11+
- Google Chrome installed in standard locations.
- PostgreSQL database (credentials are currently hardcoded in `db_connection.py`).

### Setup
1.  **Install dependencies**:
    ```bash
    uv sync
    # or
    pip install playwright sqlalchemy psycopg2-binary pandas pyyaml
    playwright install chrome
    ```

2.  **Chrome Profile**:
    The bot uses a local Chrome profile directory (`chrome_profile`) to maintain sessions and bypass bot detection.

### Mapping Selectors (Required for new markets)
Before executing bets on a specific market (e.g., player points), the selectors must be mapped:
```bash
python map_selectors.py --site [fanduel|betmgm] --market [market_name]
```
Example: `python map_selectors.py --site betmgm --market player_points`

### Executing Arbitrage
To run the main execution loop:
```bash
python execute_arb.py
```

### Testing Mode
To test the bot without placing real bets or to find historical opportunities:
```bash
# Windows
set TESTING_MODE=true
# Linux/macOS
export TESTING_MODE=true
```

---

## Development Conventions

### Selector Strategy
- UI selectors are stored in `selectors/{site}_markets.yaml`.
- The bot uses strict `aria-label` matching for FanDuel and a combination of accordion names and text matching for BetMGM.
- Always use `map_selectors.py` to update selectors when site UIs change.

### Logging and Auditing
- **Execution Logs**: `logs/execution_success.log` and `logs/execution_failures.log`.
- **Audit Trails**: Every execution attempt creates a folder in `audit_logs/` containing `opportunity_info.json` and screenshots of each step.
- **Unmapped Markets**: Markets encountered during execution that lack selectors are logged to `logs/unmapped_markets.log`.

### Error Handling
- The bot employs a "fail-fast" strategy. If an element isn't found or a price changes beyond a threshold, it captures a screenshot and aborts the execution (or the specific leg).
- **Hedge Failures**: If the first leg (BetMGM) succeeds but the hedge (FanDuel) fails, manual intervention is required.

### Environment Variables
- `TESTING_MODE`: Set to `true` to fetch old opportunities and bypass some production checks.
- `MIN_ROI_THRESHOLD`: Minimum ROI required to execute a real trade (default is often 0.01).
- `WAGER_SCALE_FACTOR`: Scale down calculated wagers (e.g., `0.01` for 1% stakes during testing).
