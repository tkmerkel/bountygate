# Handoff: BountyGate Dashboard redesign ("The Daily Hedge")

## Overview

A full redesign of the internal BountyGate dashboard at `dashboard/index.html` plus a worked example of how a wiki page (the existing `wiki/bot-flow.md`) should render in the new chrome. The redesign reskins the operator surface in a custom "Pixel Augusta" design system — WSJ-style serif headlines and double rules wrapped around SimCity 3000 / Windows 95-flavored advisor windows.

Replaces:
- `dashboard/index.html` (the static dashboard rendered at `GET /`)
- `app/web/main.py::_WIKI_TEMPLATE` + `app/web/wiki.py` rendering for `GET /wiki/{slug}` (specifically the bot-flow page)

The four FastAPI endpoints that feed the dashboard (`/api/runs`, `/api/account-stats`, `/api/watchers`, `/api/wiki/{slug}.json`) stay as-is — this is **frontend only**. No schema changes, no new tables.

## About the Design Files

The files under `prototype/` are a **high-fidelity HTML reference design**, not production code. They were built as a single React-via-Babel page so a designer could iterate quickly. They are not what should ship.

The task is to **recreate these designs in the target codebase**: the existing static frontend at `dashboard/`, served by FastAPI from `app/web/main.py`. The current production stack is vanilla HTML + a single `<script>` block — no React, no build step. **Do not add React or a bundler** unless you talk to the operator first; instead, port the layout and styles to plain HTML/CSS and replace the polling code in `dashboard/index.html` with the equivalent DOM construction.

The Pixel Augusta CSS (`colors_and_type.css`, `kit.css`, `dash.css`) is portable as-is — those files can be lifted straight into `dashboard/` (or split into the FastAPI `static/` mount). The `.jsx` files are reference for layout, copy, and behavior; their structure can be translated to template-literal rendering or a small DOM helper.

## Fidelity

**High-fidelity.** Colors, type, spacing, bevels, and chrome are intentional and pixel-perfect. Recreate visuals exactly. Specifically:

- The masthead is *not* a generic dashboard header — it's a deliberate broadsheet pastiche with vol/no, byline, date, and a thin "strip" row underneath carrying live status.
- The card chrome (titled `BOT_RUNS.LOG`, `ACCOUNT_LEDGER.EXE`, etc.) is an SC3K window with hard 1px bevels, no border-radius, no shadow blur. Don't soften it.
- Numbers use `VT323` (mono pixel font). Headlines and player names use `Playfair Display` italic. Body copy uses `Merriweather`.
- The 30-day chart is drawn directly in SVG, not Chart.js — keep it that way for hand-control over the WSJ aesthetic.

## Screens / Views

### 1. Dashboard (`/`)

The main operator surface. Two-column shell with a persistent left wiki sidebar (240px) and a main column (max 1280px).

#### 1a. Sidebar (`.bg-side` in `prototype/dashboard/dash-components.jsx::Sidebar`)
- Fixed 240px column, `--newsprint-white` bg, right-edge `1px solid --ink-black`.
- Masthead block at top: "Bounty*Gate*" (italic Playfair, last word in `--augusta-green`) over `EST. MMXXVI · INTERNAL` in VT323.
- "The Wiki" section: list of pages with glyph (§ for active page, ¶ for normal). Active item has `--augusta-green` background, white text, and a `--masters-yellow` left-border accent.
- "Health" section: `/health`, Postgres status, Heroku build — dummy items for now; populate from existing endpoints when porting.
- "Officers" section: list of operators. Static.

#### 1b. Masthead (`.bg-mast` in `Mast`)
Three-column flex on the WSJ broadsheet model:
- **Left col** (VT323, `--rule-gray`): `VOL. MMXXVI · NO. NNN`, `MEMBER T-MERKEL`, `FILED FROM REDMOND, WA`. Numbers come from runs.length.
- **Center**: `<h1>The Daily Hedge</h1>` — Playfair Black italic, clamp(38px, 4.2vw, 60px).
- **Right col**: today's date, `PAIRED · HEDGED · BOOKED`, `PRICE FREE TO PATRONS`.
Below it, a 30px strip bordered top + bottom with `1px solid --ink-black`:
- Left: blinking red square + `ALL WATCHERS NOMINAL` or `N WATCHERS OFF-NOMINAL`.
- Center: `LAST RUN · <player> · <outcome>`.
- Right: `EXPOSURE · $<total balance>`.

#### 1c. Profit chart hero (`.bg-hero`, `.bg-chart` in `ProfitChart`)
30-day daily P&L with cumulative overlay. WSJ-style chart panel on `--newsprint-white` with a 1px ink border and inset bevel.

Anatomy:
- **Hed strip** at top: `FILED · 2026-05-18 · NET PROFIT, DAILY` kicker, "The House Account, Day by Day" italic Playfair hed, single-line dek. To the right, two KPI cards: `30D NET` and `LAST 7D` with red/green colored numbers.
- **SVG chart**, 880×300 viewBox, `preserveAspectRatio="none"` so it scales. Drawn elements, back to front:
  - White inner panel with 1px ink border.
  - Horizontal grid lines every $10 or $20 (chosen by range). Zero line is solid 1px ink; others are 0.5px black @ 10% with `2 3` dasharray.
  - Y-axis labels at left in VT323 12px, `--rule-gray`, right-aligned.
  - Cumulative area: `rgba(11,91,64,0.10)` fill, polygon from baseline up.
  - Cumulative line: `--augusta-green`, 2.4px stroke, round join/cap.
  - Daily bars: 4-6px wide (`xStep - 4`), `--fg-positive` (#1F7A4D) for wins, `--ledger-red` (#B0211A) for losses.
  - Run-day dots: 2.5px circle at `H - M.b + 14`, below the x-axis baseline, marking days that had bot runs.
  - Cumulative line dots: 2.2px green circles on each cum point; the hover point is 5px with a 2px white stroke.
  - Annotations: dashed leader from the cum point up to a label band near the top, alternating two rows so labels don't collide. Currently used for: "FD Reality Check tuned", "BetMGM tab regression v1", "Selector mapper shipped", "Best day of cycle", "Combo-stats tab miss × 9", "BetMGM cold session", "Today · partial".
  - X-axis ticks at Sundays + first + last, with `MAY 4`-style VT323 labels.
  - Crosshair: dashed vertical line at hoverIdx, 50% opacity.
- **Readout strip** under the chart: 5-column grid (`DATE`, `DAILY`, `CUMULATIVE`, `RUNS`, optional `EDITORIAL NOTE`). The RUNS cell lists clickable run-pills (one per bot run that landed that day); clicking jumps to that run in the runs table. Pill colors match outcome.
- **Legend strip**: cumulative line swatch, daily win/loss swatches, run-filed dot. VT323 13px caps.
- **Stats meta row** (`.bg-hero__meta`): 4-column grid below the panel: `30D NET`, `WIN DAYS / total`, `BEST · WORST`, `LOSS DAYS`.

#### 1d. Accounts (`.bg-books` in `Accounts`)
2-column grid of "book cards" (FanDuel, BetMGM, DraftKings, Caesars). Each card:
- Row 1: book name in VT323 caps + status pill (green `● OK`, yellow `▲ STALE`, red `✕ DOWN`).
- Balance in Playfair 30px bold, tabular-nums.
- Sub: `avail $X · pending $X` in VT323.
- 7d P&L in VT323, green positive / red negative.
- 7-day sparkline: 7 bars 1-2px gap, green up / red down, max 22px height.
- If `last_error` present, ledger-red error footer.
Currently 4 books shown; data shape mirrors `GET /api/account-stats` exactly.

#### 1e. Watchers (`.bg-table` in `Watchers`)
4-column table inside a Window card: WATCHER (dot + name), TICK (age), BACKLOG (count + oldest age), 24H (completed + errors). Dots map to status: ok=green, amber=yellow, red=red, idle=newsprint-white. Errors render in ledger-red. Data shape mirrors `GET /api/watchers`.

#### 1f. Runs table (`Runs` component, the biggest piece)
Window-card titled `BOT_RUNS.LOG` with subtitle `SORT: BY <SORT>`.
- **Filter bar**: chip buttons for ALL / SUCCESS / FAILURE / SKIPPED, each showing count. Selected chip is bevel-in with colored background. To the right: `N OF M RUNS` counter.
- **Table** with fixed col widths: WHEN (110) / PLAYER (22%) / MARKET (19%) / OUTCOME (90) / DUR (80) / METERS (130) / ACTIONS (60).
  - WHEN: date stacked above time, VT323.
  - PLAYER: Playfair 16px bold.
  - MARKET: VT323 14px, ellipsis truncation.
  - OUTCOME: small caps pill, green/red/gray bg by outcome.
  - DUR: tabular-nums; if `estimated_wasted_wait_s > 0`, show `−Ns waste` in red beneath.
  - METERS: 5 stacked vertical pixel meters (one per issue axis) — each 14×22px with 4 segments, lit segments use the axis color (`wasted=blue`, `selector=orange`, `slip=red`, `auth=purple`, `stealth=yellow`).
  - ACTIONS: `[+]` / `[−]` caret button.
- **Expanded row** (`.bg-runs__expand-cell`, on click):
  - Top-finding callout in a bevel-out window: `EDITORIAL · TOP FINDING` lbl, then the `top_finding` body text.
  - Three buttons under it: `▶ OPEN RECORDING` (links `r.video_url`), `VIEW REVIEW.MD` (links `r.review_url`), `FILE TO ARCHIVE` (no-op placeholder).
  - 5-column grid of axis cards, one per issue key. Each card has a black title bar with the axis label + count in a yellow chip; body lists findings as `<li>` with leading `t=hh:mm` timestamps highlighted in `--augusta-green` VT323. Empty axes get a gray header and `— NO FINDINGS —`.

Sort options (driven by the Tweaks panel):
- `recency` (default): `new Date(b.timestamp) - new Date(a.timestamp)`.
- `wasted`: descending `estimated_wasted_wait_s`.
- `outcome`: failure → skipped → success, then by recency.

#### 1g. Ticker (`.pa-ticker` in `Ticker`, the kit component)
Fixed-bottom 40px marquee across the whole viewport. `--augusta-green` background. `LIVE` badge in yellow on the left. Content scrolls right→left at `280s` (intentionally slow; was 28s originally — operator wanted readable, not flashy). Items shown rotate based on the `tickerMode` tweak:
- `stats` (default): book balances + 7d P&L for each book, last run outcome, watcher health, total Phase-2 waste.
- `findings`: same plus top-findings from latest 4 runs as "EDITORIAL · <text>".
- `mixed`: union.

#### 1h. Tweaks panel
Floating bottom-right panel (`tweaks-panel.jsx`) showing:
- Density: comfort / compact.
- Profit chart on/off.
- Ticker on/off.
- Runs sort: recent / waste / failure.
- Ticker content: stats / edits / mix.
Persisted via `__edit_mode_set_keys` in the prototype; in production this should just be `localStorage`.

### 2. Wiki page · Bot execution flow (`/wiki/bot-flow`)

Same shell (sidebar + main column) but the main column renders a long-form "article."

Top to bottom:
- **Breadcrumb** (`.wk-breadcrumb`): `← BountyGate · The Daily Hedge · Wiki · Bot execution flow`. VT323 14px, single line.
- **Masthead** (`.wk-mast`): `INTERNAL · ENGINEERING WIKI · BOT-FLOW` kicker, Playfair italic title, meta line with `UPDATED <date> · WATCHED BY N SOURCE FILES · FILED BY wiki-watcher`.
- **Lede paragraph** with a `dropcap` first letter in `--augusta-green`. Verbatim from the existing `wiki/bot-flow.md` lede.
- **Watched-files card** (`.wk-watches`): list of `arbitrage_executor/*.py` paths in VT323, each with a leading `¶` glyph. Footer: `HOOK · post-commit · idempotent | NEXT SYNC · on next push`. Source: the `watches:` array from the page's YAML front-matter.
- **Sequence diagram** (`.wk-seq`): hand-drawn SVG version of the mermaid `sequenceDiagram` block currently in bot-flow.md. Four lanes (`bot_execution_queue`, `task_worker`, `FanDuel`, `BetMGM`) with black headers, dashed lifelines, request (solid black) / response (dashed gray) / alert (red) arrows. Three `alt` bands: ROI passes (green dashed), ROI fails (gray), MG rejects (red). This replaces the Mermaid render — keep the SVG hand-built so it visually fits the rest of the page.
- **Decision graph** (`.wk-dg`): the live React Flow island, drawn as SVG inside a `min-width: 1100px` horizontal scroll container. Replaces the `:::reactflow` block. Anatomy:
  - **Legend row**: chip buttons per layer (Execution, Decisions, Value stream, Recent failures). Toggling visibility hides nodes in that layer and any edges touching them. Right side shows `COMPUTED HH:MM` from `metrics.computed_at`.
  - **Phase brackets** across the top: QUEUE, PHASE 1 · PROBE, PHASE 2 · BETMGM, PHASE 3 · HEDGE.
  - **Nodes**: 132×56px SC3K-style window boxes. Title bar fill varies by kind: green=step, yellow=decision, red=alert, black=outcome. Body shows the wrapped label in Playfair bold + a `runs_24h · avg_duration_s` line in VT323. Hover swaps the body to `--masters-yellow`.
  - **Edges**: orthogonal step paths with arrowheads (defs marker). Yes/no decision labels render in tiny pill boxes near the source. Failure-layer edges are red with dashed stroke.
  - Coordinates and metric numbers come from `BOT_FLOW` + `BOT_FLOW_METRICS` constants in `wiki-botflow.jsx`, which mirror the shape of the existing `/api/wiki/bot-flow.json` endpoint.
- **Editor's note callout** (`.wk-callout`): inset-bevel block with a black `v1 NOTE · EDITOR` tag on the left and italic body on the right. Verbatim from the existing markdown blockquote.
- **Recurring issues table** (`.wk-issues`): one row per issue axis, each row has a colored axis tag, a pattern description, and a `N× in 30` flagged count. Replaces the markdown table at the bottom of bot-flow.md.
- **Footer**: italic centered, double-rule above. Link back to the dashboard's Watchers card.

## Interactions & Behavior

- **Polling**: dashboard cards refresh on a 30s timer (the existing `dashboard/index.html` does this). Keep that behavior. The freshness pill in each card title's subtitle should re-render from `updated_at` / `checked_at`.
- **Run row expand/collapse**: clicking a row (or the `[+]` caret) toggles its expanded panel. Only one row open at a time (`openId` state). Clicking again collapses.
- **Filter chips**: single-select. Default `all`.
- **Sort**: read from the Tweaks panel. In production, persist via `localStorage.bg.sort`.
- **Chart hover**: `onMouseMove` over the SVG snaps to the nearest `xStep` index and updates the readout strip. Default hover idx = last day so the readout has content on initial render.
- **Chart run-pill click**: scrolls/jumps to the corresponding run in the runs table and expands it (`onPinClick(run_id)` → sets `openId`).
- **Sidebar nav**: clicking a page slug swaps the main column between Dashboard / WikiBotFlowPage / WikiPlaceholder. In production, this should be real routing (`/`, `/wiki/bot-flow`, `/wiki/<slug>`). The current FastAPI already serves these endpoints — just preserve the new chrome.
- **Decision graph layer toggles**: independent on/off. Default-on for Execution/Decisions/Value-stream; default-off for Recent-failures. State is component-local in the prototype; can stay local in production.
- **Ticker**: pure CSS animation, 280s linear infinite. Don't speed up.

## State Management

For a straight port to vanilla JS (recommended), state is minimal:
- `openRunId: string | null` — runs table expansion.
- `runFilter: "all" | "success" | "failure" | "skipped"`.
- `runSort: "recency" | "wasted" | "outcome"`.
- `tickerMode: "stats" | "findings" | "mixed"`.
- `density: "comfortable" | "compact"`.
- `showChart, showTicker: boolean`.
- `chartHoverIdx: number | null`.
- `decisionLayerState: { [layerId]: boolean }`.

Persist sort/filter/density/showChart/showTicker/tickerMode in `localStorage` under `bg.tweaks`. Don't persist `openRunId` or `chartHoverIdx`.

All data is fetched from existing endpoints; no new backend work is required. Re-fetch every 30s; on focus, re-fetch immediately.

## Design Tokens

All defined in `prototype/dashboard/colors_and_type.css`. Lift the file as-is.

### Colors
```
--augusta-green:   #0B5B40   /* primary chrome, card titles, links */
--augusta-green-2: #106B4D
--augusta-green-3: #084632
--augusta-green-4: #C9DBD2
--newsprint-white: #F4F1EA   /* page background */
--crisp-white:     #FFFFFF   /* card background */
--masters-yellow:  #F1C40F   /* accent, highlights, ticker text */
--masters-yellow-2:#FFD84A
--ink-black:       #1A1A1A   /* body text, borders */
--rule-gray:       #6B6B66   /* meta text, dividers */
--ledger-red:      #B0211A   /* negative figures, failures */
--fg-positive:     #1F7A4D   /* positive figures */
--bg-inset:        #EEEAE0   /* recessed wells, expanded rows */
```

### Typography
```
--font-serif:      'Playfair Display', 'Merriweather', 'Times New Roman', serif
--font-serif-body: 'Merriweather', 'Georgia', serif
--font-mono-pixel: 'VT323', 'Courier New', monospace
```
Type scale: `11 / 12 / 14 / 16 / 20 / 26 / 34 / 46 / 60 px` (modular, 1.25).

### Spacing
4px base scale: 0 / 4 / 8 / 12 / 16 / 24 / 32 / 48 / 64.

### Borders & bevels
- All borders: `1px solid --ink-black`. No border-radius anywhere except `--radius-pill: 999px` on tickers/dots.
- Bevel-out (raised SC3K chrome):
  ```css
  box-shadow: inset 1px 1px 0 rgba(255,255,255,0.40),
              inset -1px -1px 0 rgba(0,0,0,0.40);
  ```
- Bevel-in (pressed/recessed): reverse.
- Hard shadow (used sparingly): `2px 2px 0 --ink-black`.

## Assets

- **Google Fonts** (already imported in `colors_and_type.css`):
  `Playfair Display`, `VT323`, `Merriweather`.
- **No image assets.** The hedcut portraits, isometric tiles, sparklines, profit chart, sequence diagram, and decision graph are all CSS or inline SVG. Don't add raster images.
- **Crest glyph** in the wiki masthead is the literal `⛳` (U+26F3) — Apple Color Emoji on macOS / Segoe UI Emoji on Windows. Fine as a placeholder; replace with a custom SVG if you want platform-consistent rendering.

## Implementation guidance

Recommended port path, given the production stack is vanilla:
1. Copy `colors_and_type.css`, `kit.css`, `dash.css` into `dashboard/` next to `index.html`. Link them from `index.html`.
2. Rewrite `dashboard/index.html`:
   - Build the sidebar + masthead as static HTML.
   - Replace the existing card-rendering JS with new functions that render the new structure. Existing fetches (`fetch("/api/runs?...")`, etc.) stay; just swap the render targets.
   - Move the runs-table render to produce the new layout (chips, meters, expandable rows). Move issue-meter render into a small function that takes an `issues` object.
3. Build the profit chart as a self-contained `<svg>` constructed in JS from a new `account_stats_history` query. **New backend work needed here**: the chart wants 30 days of daily P&L, and `account_stats` currently only stores point-in-time scrape results. Either:
   - Add a `daily_pnl_history` materialized view rolled up from the bet history, exposed at `GET /api/pnl-history?days=30`, or
   - Compute it client-side from settled bets if those are queryable.
   The prototype uses 30 hardcoded days in `mock.js::dailyPnl` — use that as the contract shape (`{ par_per_day, days: [{date, pnl, note?}] }`).
4. Port the wiki page rendering. The existing `app/web/wiki.py` already parses `:::reactflow` blocks into `<div class="reactflow-mount">` — keep that mechanism, but change the reactflow bootstrap script in `dashboard/wiki/reactflow-bootstrap.js` to render the new SC3K-style nodes (or replace React Flow with a hand-built SVG, which is what the prototype does). The mermaid sequence diagram can stay as `pre.mermaid` if you want Mermaid's rendering; the prototype hand-draws it for visual consistency, which is the higher-fidelity option.
5. Reuse the existing `app/web/main.py` endpoints unchanged. The prototype's mock data shapes are deliberately wire-compatible.

## Files

Reference implementation under `prototype/`:

- `prototype/BountyGate Dashboard.html` — entry point. Loads React via CDN with Babel for `.jsx` files. **Do not ship this.**
- `prototype/dashboard/colors_and_type.css` — design tokens. Ship as-is.
- `prototype/dashboard/kit.css` — Pixel Augusta UI kit primitives (window cards, buttons, ticker, hedcut). Ship as-is.
- `prototype/dashboard/dash.css` — dashboard- and wiki-page-specific styles. Ship as-is.
- `prototype/dashboard/components.jsx` — `<WindowCard>`, `<Button>`, `<Ticker>`, `<IsometricField>` primitives. Translate the structures to plain HTML; the CSS classes are already in `kit.css`.
- `prototype/dashboard/dash-components.jsx` — `<Sidebar>`, `<Mast>`, `<ProfitChart>`, `<Accounts>`, `<Watchers>`, `<Runs>`, `<IssueMeters>`. The bulk of the dashboard.
- `prototype/dashboard/wiki-botflow.jsx` — the bot-flow wiki page (breadcrumb, sequence diagram, decision graph, issues table). Contains `BOT_FLOW` and `BOT_FLOW_METRICS` reference data shapes.
- `prototype/dashboard/app.jsx` — top-level composition + Tweaks panel wiring.
- `prototype/dashboard/tweaks-panel.jsx` — design-time control panel, **not for production**.
- `prototype/dashboard/mock.js` — fabricated Accounts, Watchers, and 30-day P&L data. Use for shape reference only; the production endpoints already exist for Accounts and Watchers.
- `prototype/dashboard/bg-data.js` — the runs from `dashboard/data.json`, JS-wrapped. Production should keep using `GET /api/runs`.

## What to ask the operator before shipping

1. **30-day P&L source**: is the data already queryable from `bot_execution_queue` settlement data, or does the chart need a new `daily_pnl` rollup view?
2. **Books beyond FD + MGM**: the prototype shows DraftKings and Caesars cards because they appear in the mock. If those books aren't actually scraped yet, render only the books that come back from `/api/account-stats`.
3. **Wiki page strategy**: do you want all wiki pages to adopt the new chrome, or only bot-flow as the demo? The other slugs in the sidebar (`auth-sop`, `selector-map`, etc.) are placeholders; they'll need their own content or should hide until written.
4. **Sidebar items** (Health / Officers sections): real data sources, or keep as static decoration?
