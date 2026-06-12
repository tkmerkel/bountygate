# Next.js Foundation (Stage 2) — Public Product Shell

**Status:** Approved for autonomous execution (user delegated design decisions 2026-06-11:
"Loop on it until you're 100% satisfied with end product").
**Blueprint:** `2026-06-10-analytics-platform-blueprint.md`, build-order stage 2.

## 1. Context: what exists and is reused

- **API (FastAPI, Heroku `bountygate`):** ten read endpoints — `/markets`,
  `/markets/{id}/history`, `/edges`, `/cross-market`, `/history`, `/fair-odds`, `/sharpness`,
  `/calibration`, `/movement/{event_id}`, `/closing-lines` — plus `/health`. CORS already
  allows `*` for GET. All router SQL runs on both Postgres and sqlite (test seam).
- **Vanilla MVP (`app/web/static/`):** two views — cross-venue compare (`/cross-market`,
  sortable) and market browser (`/markets`, venue/status filter + title search). This page
  retires at the end of this stage.
- **Pixel Augusta design system (`app/design_handoff_bountygate_dashboard/`):**
  `colors_and_type.css` is a complete, portable token sheet (palette, type scale, spacing,
  bevels, dithers). `kit.css`/`dash.css` and the `.jsx` prototypes are layout/chrome reference.
  The handoff README's "no React" rule applied to the old internal dashboard port; the
  blueprint explicitly supersedes it for the public product (Next.js + TS + Tailwind).
- **Host tooling:** Node 22 / npm 10 available. No package.json exists anywhere yet.

## 2. Decisions (made autonomously, with rationale)

1. **Location: `app/frontend/`** — sibling of `app/web`, matching the repo's app-per-surface
   layout. Vercel supports a monorepo root directory setting.
2. **Stack: Next.js (App Router) + TypeScript + Tailwind CSS + Recharts** — blueprint mandate.
   No other runtime dependencies; data fetching is plain `fetch` in a small typed client.
3. **API access: Next.js rewrites, not CORS.** `next.config.ts` rewrites `/api/*` to
   `${API_BASE_URL}/*` (env; `http://localhost:8000` in dev, the Heroku app URL in prod).
   The browser only ever talks same-origin; the backend origin stays swappable; FastAPI's
   existing permissive CORS becomes irrelevant to the product (left as-is). This resolves the
   blueprint's deferred CORS-vs-proxy decision in favor of the proxy: fewer moving parts and
   no preflight latency.
4. **Theme: tokens as CSS custom properties, Tailwind mapped onto them.** Lift the
   `colors_and_type.css` `:root` block into `globals.css` verbatim (single source of truth for
   the palette), expose the vars to Tailwind via theme config (`augusta-green`,
   `newsprint-white`, `masters-yellow`, `ink-black`, `rule-gray`, `ledger-red`, font families).
   Fonts load via `next/font/google` (Playfair Display, Merriweather, VT323) instead of the
   CSS `@import` — self-hosted, no layout shift. The SC3K chrome (1px bevels, no radius, hard
   offset shadows) and WSJ rules (double rule, kickers) come over as utility classes.
5. **Pages (App Router):**
   - `/` — **Fair odds screen** (new flagship): `mart_fair_odds` table via `/api/fair-odds`;
     sport + market-type filters; consensus prob, best price/book, edge (ledger red/green);
     each row links to its event page.
   - `/events/[eventId]` — **Movement page**: Recharts line chart of decimal price over time
     per (bookmaker, outcome) series from `/api/movement/{eventId}`, market-type toggle
     (h2h/totals), closing-line markers from `/api/closing-lines`. Renders sensibly when
     closing lines don't exist yet (chart only).
   - `/cross-venue` — port of the cross-venue compare view (same columns/sort behavior).
   - `/markets` — port of the market browser (venue/status filters, title search).
   - Shared chrome: Pixel Augusta masthead ("The Daily Hedge" broadsheet pastiche with nav
     strip), nav links to the four screens, footer rule. No sidebar (that was internal-dashboard
     chrome; the public product is masthead + content column, max-width 1280px).
6. **Rendering model:** server-component shells, client components for data tables/charts with
   plain client-side fetch + loading/error/empty states (the data is volatile; ISR/SEO work is
   stage 4's game pages). One shared `useApi<T>(path)` hook: loading → error-with-retry → data.
7. **Vercel:** the app is Vercel-ready (build passes, `API_BASE_URL` is the only env var,
   README documents the two-step connect: import repo, set root dir `app/frontend` + env var).
   Actually connecting the Vercel account is a user action; this stage's acceptance is a clean
   production build + Playwright smoke locally. No vercel.json needed (defaults suffice).
8. **Retiring the vanilla page (last task, after parity verified):** delete `app/web/static/`
   and the StaticFiles mount; `GET /` returns `{"service": ..., "docs": "/docs"}`. The three
   static-serving web tests are replaced by one root-JSON test.

## 3. Scope guards

- No accounts, no alerts, no SEO/ISR work, no game pages, no model-hub/calibration pages
  (stages 3–4), no NFL filter chips beyond the three blueprint sports + whatever sports the
  API returns (the sport filter is populated from data, not hardcoded).
- No state library, no component library, no CSS-in-JS. Tailwind + tokens only.
- `/sharpness` and `/calibration` endpoints stay API-only this stage (their pages are stage 3,
  when there is scored data to show).
- The FastAPI app changes only in the final retire task (static removal). No schema changes,
  no new endpoints.

## 4. Structure

```
app/frontend/
  package.json, next.config.ts, tsconfig.json, postcss.config.mjs, .gitignore
  src/app/layout.tsx          — fonts, masthead, nav, footer
  src/app/page.tsx            — fair odds screen
  src/app/events/[eventId]/page.tsx — movement page
  src/app/cross-venue/page.tsx
  src/app/markets/page.tsx
  src/app/globals.css         — Pixel Augusta tokens + base + utilities (bevels, rules)
  src/lib/api.ts              — typed endpoint client (FairOdds, MovementPoint, ClosingLine,
                                CrossMarketRow, MarketRow) + useApi hook
  src/components/             — Masthead, DataTable (sortable), FilterBar, MovementChart,
                                LedgerNumber, states (Loading/Error/Empty)
  e2e/                        — Playwright smoke (one spec per page) + seed fixtures
  e2e/seed_api.py             — builds a sqlite file with all five tables + rows, serves the
                                real FastAPI app against it (uvicorn, DATABASE_URL=sqlite:///…)
```

## 5. Testing

- **Playwright smoke per page** (blueprint quality bar): boots the seeded FastAPI (sqlite) on
  a fixed port, `next dev`-or-build server with `API_BASE_URL` pointed at it, asserts each page
  renders rows/chart and the error state appears when the API is down. Runs headless locally
  (`npm run e2e`).
- **Production build gate:** `npm run build` must pass with zero type errors.
- Existing FastAPI tests keep passing; the retire task swaps three static tests for one.

## 6. Success criteria

1. `npm run build` clean; `npm run e2e` green (4 page smokes + error-state).
2. All four screens render live data from the local API with Pixel Augusta chrome (masthead,
   WSJ rules, bevel cards, VT323 figures, Playfair heds).
3. Movement chart shows multi-series price history with closing-line markers for a real event.
4. Vanilla page removed; FastAPI tests green; README documents Vercel connect + env.
5. A fresh `git clone` + documented commands reproduce dev and prod builds.
