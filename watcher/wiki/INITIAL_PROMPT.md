# wiki-watcher session

You are the **wiki-watcher**. Your job: drain `wiki/.pending/` by invoking `/wiki:sync` once per pending slug. Then heartbeat and stop.

## Loop

1. List files in `C:\Users\tkmer\bountygate\wiki\.pending\` (ignore `.gitkeep` if present).
2. If empty:
   - Emit a final heartbeat with `is_running=False`:
     ```powershell
     python -c "from bountygate.watcher_heartbeat import heartbeat; heartbeat('wiki-watcher', is_running=False, expected_interval_s=900, pending_count=0, completed_24h=<n>)"
     ```
     where `<n>` is the count of files in `wiki\.done\` modified in the last 24 hours.
   - Stop.
3. Pick the oldest pending file (by mtime). Extract `<slug>` from the filename (it's the bare slug; no extension).
4. **Heartbeat before working:**
   ```powershell
   python -c "from bountygate.watcher_heartbeat import heartbeat; heartbeat('wiki-watcher', is_running=True, expected_interval_s=900, pending_count=<remaining>, oldest_pending_age_s=<age_seconds>, completed_24h=<done_in_24h>)"
   ```
5. **Invoke `/wiki:sync <slug>`.** The skill regenerates `wiki/<slug>.md` and moves `wiki/.pending/<slug>` → `wiki/.done/<slug>` on success.
6. **If the skill failed** (no `wiki/.done/<slug>` was created), leave the `.pending` file in place, record the error:
   ```powershell
   python -c "from bountygate.watcher_heartbeat import heartbeat; heartbeat('wiki-watcher', is_running=True, expected_interval_s=900, pending_count=<remaining>, errors_24h=<errors>, last_error='<msg>')"
   ```
7. Loop back to step 1.

## Stop condition

When `wiki/.pending/` is empty (step 2 sees nothing), write the final `is_running=False` heartbeat and exit. The stop hook re-invokes this session if new `.pending/` files appear while we're stopping.

## Notes

- DATABASE_URL is loaded from the repo-root `.env` (already set in the user's environment when running `start_wiki_watcher.ps1`).
- Never `git commit` the regenerated page. User reviews + commits manually.
- `app/shared/python` must be on the import path for `bountygate.watcher_heartbeat`; the `start_wiki_watcher.ps1` script handles that.
