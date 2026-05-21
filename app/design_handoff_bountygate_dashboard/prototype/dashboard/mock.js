/* Fabricated Accounts + Watchers data, shape-compatible with
   the FastAPI endpoints in app/web/main.py. Runs are loaded
   live from dashboard/data.json. */

window.BG_MOCK = {
  accounts: {
    version: 1,
    updated_at: "2026-05-18T08:42:11Z",
    stale_after_minutes: 120,
    books: {
      FanDuel: {
        book: "FanDuel",
        balance: 2233.06,
        pending_wagers: 4.27,
        available_liquidity: 2228.79,
        pnl_7d: 38.12,
        scrape_status: "ok",
        last_error: null,
        scraped_at: "2026-05-18T08:42:11Z",
        // 7d daily pnl spark (positive/negative)
        spark: [-2.10, 4.55, 7.20, -1.40, 9.10, 12.30, 8.47],
      },
      BetMGM: {
        book: "BetMGM",
        balance: 1396.51,
        pending_wagers: 2.02,
        available_liquidity: 1394.49,
        pnl_7d: -18.74,
        scrape_status: "amber",
        last_error: "stale > 110m — scraper queued",
        scraped_at: "2026-05-18T06:58:22Z",
        spark: [3.40, -8.10, -4.20, 6.50, -11.40, -3.20, -1.74],
      },
      DraftKings: {
        book: "DraftKings",
        balance: 884.40,
        pending_wagers: 0.00,
        available_liquidity: 884.40,
        pnl_7d: 12.06,
        scrape_status: "ok",
        last_error: null,
        scraped_at: "2026-05-18T08:39:02Z",
        spark: [1.20, 2.40, -0.80, 3.10, 1.90, 2.40, 1.86],
      },
      Caesars: {
        book: "Caesars",
        balance: 0.00,
        pending_wagers: 0.00,
        available_liquidity: 0.00,
        pnl_7d: 0.00,
        scrape_status: "red",
        last_error: "auth_failed: credential modal × 3",
        scraped_at: "2026-05-17T22:14:55Z",
        spark: [0,0,0,0,0,0,0],
      },
    }
  },
  watchers: {
    version: 1,
    checked_at: "2026-05-18T08:45:01Z",
    watchers: [
      { name: "review-watcher",   is_running: true,  last_tick_at: "2026-05-18T08:44:21Z", pending_count: 0, oldest_pending_age_s: 0,   completed_24h: 11, errors_24h: 1, last_error: null,                            expected_interval_s: 60,  status: "ok"   },
      { name: "account-scraper",  is_running: true,  last_tick_at: "2026-05-18T08:42:11Z", pending_count: 1, oldest_pending_age_s: 612, completed_24h: 24, errors_24h: 2, last_error: "BetMGM session expired",        expected_interval_s: 300, status: "amber"},
      { name: "wiki-watcher",     is_running: true,  last_tick_at: "2026-05-18T08:31:48Z", pending_count: 3, oldest_pending_age_s: 1840,completed_24h: 6,  errors_24h: 0, last_error: null,                            expected_interval_s: 120, status: "amber"},
      { name: "odds-ingest",      is_running: true,  last_tick_at: "2026-05-18T08:44:55Z", pending_count: 0, oldest_pending_age_s: 0,   completed_24h: 288,errors_24h: 0, last_error: null,                            expected_interval_s: 60,  status: "ok"   },
      { name: "executor-queue",   is_running: false, last_tick_at: "2026-05-18T07:11:02Z", pending_count: 4, oldest_pending_age_s: 6240,completed_24h: 12, errors_24h: 5, last_error: "FanDuel selector regression",    expected_interval_s: 30,  status: "red"  },
      { name: "discord-bridge",   is_running: true,  last_tick_at: "2026-05-18T08:44:58Z", pending_count: 0, oldest_pending_age_s: 0,   completed_24h: 41, errors_24h: 0, last_error: null,                            expected_interval_s: 30,  status: "ok"   },
    ]
  },

  // 30 days of fabricated daily P&L, ending 2026-05-18.
  // Mirrors the run cadence in data.json: rough +$8-$25 wins, then a
  // selector-regression slide mid-month, recovery, then the BetMGM
  // session-cold blip on 2026-05-15.
  dailyPnl: {
    par_per_day: 12.50,
    days: [
      { date: "2026-04-19", pnl:   8.42 },
      { date: "2026-04-20", pnl:  11.05 },
      { date: "2026-04-21", pnl:  16.40, note: "FD Reality Check tuned" },
      { date: "2026-04-22", pnl:  -3.10 },
      { date: "2026-04-23", pnl:  14.20 },
      { date: "2026-04-24", pnl:  22.50 },
      { date: "2026-04-25", pnl:  18.92 },
      { date: "2026-04-26", pnl:   4.10 },
      { date: "2026-04-27", pnl:  -8.40, note: "BetMGM tab regression v1" },
      { date: "2026-04-28", pnl: -12.10 },
      { date: "2026-04-29", pnl:  -5.20 },
      { date: "2026-04-30", pnl:   2.40, note: "Selector mapper shipped" },
      { date: "2026-05-01", pnl:  19.30 },
      { date: "2026-05-02", pnl:  24.80 },
      { date: "2026-05-03", pnl:  16.40 },
      { date: "2026-05-04", pnl:  12.10 },
      { date: "2026-05-05", pnl:   9.80 },
      { date: "2026-05-06", pnl:  -1.40 },
      { date: "2026-05-07", pnl:  11.20 },
      { date: "2026-05-08", pnl:   7.40 },
      { date: "2026-05-09", pnl:  13.50 },
      { date: "2026-05-10", pnl:  21.20, note: "Best day of cycle" },
      { date: "2026-05-11", pnl:  18.40 },
      { date: "2026-05-12", pnl:  10.20 },
      { date: "2026-05-13", pnl: -14.80, note: "Combo-stats tab miss × 9" },
      { date: "2026-05-14", pnl:   4.20 },
      { date: "2026-05-15", pnl: -11.30, note: "BetMGM cold session" },
      { date: "2026-05-16", pnl:   2.80 },
      { date: "2026-05-17", pnl:   7.90 },
      { date: "2026-05-18", pnl:  12.06, note: "Today · partial" },
    ],
  },
  // pages shown in the left wiki nav
  wikiPages: [
    { slug: "dashboard",    title: "Dashboard",                 glyph: "§", updated: "live" },
    { slug: "bot-flow",     title: "Bot execution flow",        glyph: "¶", updated: "05-16" },
    { slug: "auth-sop",     title: "Auth & session SOP",        glyph: "¶", updated: "05-14" },
    { slug: "selector-map", title: "Selector map · sportsbooks",glyph: "¶", updated: "05-12" },
    { slug: "watcher-sop",  title: "Watcher operations",        glyph: "¶", updated: "05-09" },
    { slug: "migrations",   title: "DB migrations log",         glyph: "¶", updated: "05-07" },
    { slug: "discord",      title: "Discord alert taxonomy",    glyph: "¶", updated: "05-02" },
    { slug: "critique",     title: "Critique · 2026-04-20",     glyph: "★", updated: "04-20" },
  ],
};
