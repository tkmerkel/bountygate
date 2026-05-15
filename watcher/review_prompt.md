Review this screen recording of an automated sports-betting arbitrage execution. The bot places paired bets across FanDuel and BetMGM in three phases:

1. **Phase 1** — Open FanDuel, find the player prop market, tease the max-wager limit
2. **Phase 2** — Open BetMGM, place the actual bet at the discovered stake
3. **Phase 3** — Return to FanDuel and place the hedge bet

A normal run is 20–35 seconds. The bot has no human-in-the-loop — every wait, every click, every wager entry happens automatically. The sportsbooks actively profile users to detect and limit bots, so timing patterns matter.

For each of the five axes below, list **concrete observations with frame timestamps**. Quote what's on screen and reference the frame number. Be specific — if you can't see something clearly, say so. Don't speculate.

### 1. Wasted wait time
Frames where the page looks fully loaded and idle but the bot hasn't acted yet. Look for cursor sitting still, betslip ready but no click, page content stable for >1s without action. Estimate total seconds saveable across the run.

### 2. Selector misses + retries
Cursor hovering near but not on a target element. Repeated clicks on the same area. Modals interrupting the flow:
- **FanDuel Reality Check** modal (~270 minute trigger)
- **Geolocation** verification prompts
- **Cookie/consent** banners
- **Login expiry** warnings

Note which selectors looked unreliable based on visible behavior.

### 3. Slip-state issues
- Betslip showing leftover bets from a prior run that needed clearing
- Wager input field rejecting input, showing wrong value, or being typed into too fast
- Odds visibly changing between when the bot reads them and when it clicks place
- Bet showing "suspended" or "unavailable" mid-flow

### 4. Login/auth/geo drift
Early-warning signals that don't break the run yet but predict future failure:
- Session-expired banners about to appear
- Location verification prompts
- "Verify your identity" overlays
- Captchas
- Account-restriction notices

### 5. Stealth risks (avoid getting limited)
Patterns a sportsbook risk team would flag for limiting the account:
- Perfectly identical click cadence across phases
- Instant clicks immediately after page load with no hover/scroll/idle
- Repeated identical wager amounts run after run
- Wager amounts that round suspiciously (always $10.00, $25.00, etc.)
- Interaction patterns no human would produce (clicking before the page is fully painted, no mouse movement between distant clicks)
- Both books targeting the exact same market/player simultaneously — easy correlation signal

## Output format

For each axis, use this format:
```
### 1. Wasted wait time
- t=00:04 — FanDuel page fully loaded, betslip visible, bot idle for 1.8s before clicking. Saveable: ~1.5s.
- t=00:18 — BetMGM stake field accepted input by t=00:18.2, "Place Bet" button stable, but click didn't fire until t=00:19.6. Saveable: ~1.4s.

Estimated total wasted wait: ~2.9s
```

If you see nothing for an axis, write "None observed in this recording." Don't pad with speculation.

End with:

```
## Top Finding
<One sentence — the single highest-leverage change. Examples: "Replace the 2s wait after stake entry on BetMGM (t=00:18) with a wait-for-button-enabled — saves ~1.4s." or "Wager amount $87.50 has been identical for 6 consecutive runs — randomize ±$2 to reduce limiting risk.">
```
