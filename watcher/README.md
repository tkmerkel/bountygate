# watcher — Video-feedback loop

A second Claude Code session reviews every arb execution's screen recording and surfaces issues to a dashboard. The bot doesn't need this to function — it's a quality-improvement loop.

## Wiring

```
arbitrage_executor/task_worker.py
        |
        | each execution writes:
        v
arbitrage_executor/audit_logs/<ts>_<player>_<market>/
        |-- recording.mp4         <- screen_recorder.py captures the run
        |-- opportunity_info.json
        |-- *.png                 <- failure screenshots
        \-- review.pending        <- signal: needs review
                |
                | a watcher Claude session picks this up:
                v
        scripts/start_watcher.ps1
                |
                | runs INITIAL_PROMPT.md -> loops:
                |   1. find oldest review.pending
                |   2. /watch:watch on recording.mp4 with review_prompt.md
                |   3. write review.md
                |   4. append entry to dashboard/data.json
                |   5. delete review.pending -> create review.done
                |
                | watcher/stop_hook.ps1 re-invokes the session if
                | review.pending files remain
                v
        dashboard/data.json  <- rendered by dashboard/index.html
```

## Files

- `INITIAL_PROMPT.md` — the system prompt for the watcher session. Defines the per-recording loop.
- `review_prompt.md` — the question passed to `/watch:watch` for each recording.
- `stop_hook.ps1` — Claude Code stop hook that re-invokes if `review.pending` files exist.

## Operating

Start the watcher: `& 'C:\Users\tkmer\bountygate\scripts\start_watcher.ps1'`

It will keep running (processing then sleeping) until there are no pending reviews, then exit.

## Why it lives across four directories

The recording producer (`arbitrage_executor/screen_recorder.py`), the prompts (`watcher/`), the launch script (`scripts/start_watcher.ps1`), and the dashboard (`dashboard/`) are split because each piece has a different deployment surface: the recorder runs inside the bot process, the watcher runs as a separate Claude session, the dashboard is a static page, and the launch script bridges the user's shell to the Claude session. Keeping them separate keeps each piece minimal.
