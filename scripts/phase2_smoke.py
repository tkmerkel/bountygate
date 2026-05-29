"""Read-only Phase 2 integration smoke test.

Proves the live chain works end to end against TODAY's data:
  1. Read recent dim_event rows from the live DB.
  2. Fetch live MLB schedule + ESPN MLB scoreboard.
  3. Measure how many feed games match a dim_event (the crux — name+date join).
  4. For one matched final MLB game, fetch the boxscore and grade stats.

No writes. External calls are GETs to keyless public APIs.
"""
import pathlib
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "app" / "shared" / "python"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from bountygate.enrichment import clients, match, results, statmap  # noqa: E402
from bountygate.utils.db_connection import fetch_data  # noqa: E402

today = datetime.now(timezone.utc).date()
dates = [today - timedelta(days=1), today]

print("== dim_event (recent) ==")
df = fetch_data(
    "SELECT bg_event_id, sport_key, home_team_name, away_team_name, commence_at_utc "
    "FROM dim_event "
    "WHERE commence_at_utc >= now() - interval '1 day' "
    "  AND commence_at_utc <= now() + interval '2 days'"
)
if df is None or df.empty:
    print("  (no recent events — try when games are scheduled)")
    sys.exit(0)
df = df.copy()
df["commence_at_utc"] = df["commence_at_utc"].astype(str)
events = df.to_dict("records")
by_sport = {}
for e in events:
    by_sport.setdefault(e["sport_key"], 0)
    by_sport[e["sport_key"]] += 1
print(f"  {len(events)} events:", by_sport)

print("\n== MLB schedule match rate ==")
matched = total = 0
example_final = None
for d in dates:
    sched = clients.fetch_json(clients.build_mlb_schedule_url(d))
    if not sched:
        continue
    for g in results.parse_mlb_schedule(sched):
        total += 1
        bg = match.match_game_to_event(
            "baseball_mlb", g["home_team_name"], g["away_team_name"],
            g["commence_at_utc"], events,
        )
        if bg:
            matched += 1
            if g["final"] and example_final is None:
                example_final = (g, bg)
print(f"  matched {matched}/{total} MLB feed games to dim_event")

print("\n== ESPN scoreboard match rate (MLB) ==")
em = et = 0
for d in dates:
    sb = clients.fetch_json(clients.build_espn_scoreboard_url("baseball_mlb", d))
    if not sb:
        continue
    for g in results.parse_espn_scoreboard(sb, "baseball_mlb"):
        et += 1
        if match.match_game_to_event(
            "baseball_mlb", g["home_team_name"], g["away_team_name"],
            g["commence_at_utc"], events,
        ):
            em += 1
print(f"  matched {em}/{et} ESPN MLB games to dim_event")

if example_final:
    g, bg = example_final
    print(f"\n== boxscore grade for {g['away_team_name']} @ {g['home_team_name']} (event {bg}) ==")
    box = clients.fetch_json(clients.build_mlb_boxscore_url(g["game_pk"]))
    rows = statmap.extract_mlb_player_stats(box) if box else []
    print(f"  {len(rows)} player-stat rows")
    for r in rows[:6]:
        print(f"    {r['player_name']:24} {r['stat_key']:22} {r['stat_value']}")
else:
    print("\n(no final MLB game matched yet — results grading is time-of-day dependent)")
