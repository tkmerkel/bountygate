# Frontend MVP — Cross-Venue + Market Browser (data-first)

**Date:** 2026-06-10
**Status:** Design — approved, pending implementation plan
**Spec #5 of the downstream queue** (`docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`):
connectors ✅ → Postgres backend ✅ → transform pipeline ✅ → cross-venue matching ✅ → **frontend (this)**.

This is the **first, data-first slice** of the frontend item. It deliberately ships two read-only data
views with minimal styling so the operator (new to frontend work) can see real data on screen, then
layer styling and more views afterward.

---

## 1. Context & goal

The read API now serves real data end-to-end (`/markets`, `/edges`, `/cross-market`, `/history`). There
is no UI: the only way to see the data is curl. This spec adds a **single static page, served by the
existing FastAPI app**, that renders two tables:

1. **Cross-venue comparison** — `GET /cross-market`: each game-side's win probability on Kalshi vs
   Polymarket vs sportsbook-consensus, sorted by disagreement. The marquee view.
2. **Market browser** — `GET /markets`: a filterable/searchable list of all Kalshi + Polymarket markets.

**Audience:** intended to be viewable by others (not operator-only), but the MVP is unauthenticated,
read-only, and unstyled-beyond-legible. Polish, more views (edges, calibration), and the existing
"Pixel Augusta" design system are explicitly deferred to follow-up specs.

### Locked decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Stack | **Vanilla HTML + CSS + plain JS**, no framework, no build step, no `node_modules`. Approach A. |
| Serving | Served by the **existing FastAPI app** (`app/web/main.py`) — `GET /` returns the page; `/static/*` serves JS/CSS. Same dyno, same origin (no CORS concern). |
| MVP views | **Cross-venue comparison** + **Market browser** only. Edges and calibration deferred. |
| Styling | **Minimal/legible** only (system font, bordered tables). Pixel Augusta design system deferred. |
| Auth | **None** for the MVP — public, read-only. |
| Refresh | **Manual** per-view Refresh button + "loaded at" stamp. No auto-polling (data changes ~every 5 min). |
| JS tests | **None** for the MVP (would require adding a Node test runner). Backend route tests + manual browser verification instead. |

### Environment facts
- FastAPI app at `app/web/main.py`; routers under `app/web/routers/`; engine via `app/web/db.py`
  (`DATABASE_URL`). Run locally with `py -3.12 -m uvicorn app.web.main:app` from the repo root.
- Heroku `Procfile`: `web: uvicorn app.web.main:app --host 0.0.0.0 --port $PORT` (unchanged — the page
  ships on this same dyno).
- FastAPI already depends on `starlette` (provides `StaticFiles`, `FileResponse`). No new dependency.
- A prior "Pixel Augusta" design system exists at `app/design_handoff_bountygate_dashboard/` — **not used
  here**, referenced only as the future styling source.

### API shapes consumed (already live, unchanged)
- `GET /cross-market?limit&offset` → `[{question_key, captured_at, kalshi_prob, polymarket_prob,
  sportsbook_consensus_prob, max_spread}]` (probs are 0–1 floats or null).
- `GET /markets?venue&status&limit&offset` → `[{market_id, venue_key, external_id, title, category,
  status, open_time, close_time, resolved_outcome, resolution_time, updated_at}]`.

---

## 2. Deliverables

1. `app/web/static/index.html` — page shell: header, two tab buttons, two view containers.
2. `app/web/static/styles.css` — minimal legible styling.
3. `app/web/static/app.js` — fetch + render for both views, plain JS (no framework).
4. `app/web/main.py` — `GET /` (serves index.html) + `/static` mount.
5. Backend tests in `app/web/tests/` for the new routes.

**Explicitly out of scope:** the edges and calibration views; auth/login; the Pixel Augusta styling;
auto-refresh/websockets; pagination UI beyond a fixed `limit`; any JS build tooling or test runner; any
backend/schema change beyond the two serving routes.

---

## 3. Architecture & serving

```
app/web/
  main.py            # + GET "/" (FileResponse index.html), + mount /static
  static/
    index.html       # shell: <header>, tab buttons, #view-cross, #view-markets
    app.js           # fetch + render; hash routing (#cross / #markets)
    styles.css       # minimal
  routers/…          # unchanged
```

- `main.py` resolves the static dir from `__file__`
  (`STATIC_DIR = Path(__file__).parent / "static"`), mounts `app.mount("/static",
  StaticFiles(directory=STATIC_DIR))`, and adds `@app.get("/")` returning
  `FileResponse(STATIC_DIR / "index.html")`. Registered **after** the existing routers so nothing is
  shadowed (API paths `/markets`, `/cross-market`, … keep priority; the page owns only `/` and
  `/static/*`).
- Same-origin: the page is served by the same app whose API it calls, so `fetch('/cross-market')`
  needs no CORS handling locally or on Heroku.
- Data flow: load `/` → `app.js` runs → fetches the relevant endpoint for the active tab → builds a
  `<table>` in the DOM.

---

## 4. Views

### 4a. Cross-venue comparison (default tab, `#cross`)

Source: `GET /cross-market?limit=200`. Each row's `question_key` (e.g. `nba:2026-06-09:SAS@NYK:NYK`) is
parsed by a pure helper `parseQuestionKey(key)` → `{sport, date, matchup, side}` where the key splits on
`:` into `[sport, date, "AWAY@HOME", side]`; `matchup` renders the `AWAY@HOME` part as `AWAY @ HOME`.

Columns: **Sport · Date · Matchup · Side · Kalshi · Polymarket · Sportsbook · Spread**.
- Probabilities via `formatPct(x)` → `(x*100).toFixed(1) + '%'`; `null` → `—`.
- **Default sort: `max_spread` descending** (biggest disagreement first). Clicking a numeric column
  header (Kalshi/Polymarket/Sportsbook/Spread) sorts by it, toggling asc/desc; nulls sort last.
- A row counter ("N rows") and the "loaded at HH:MM:SS" stamp render above the table.

### 4b. Market browser (`#markets`)

Source: `GET /markets?venue=&status=&limit=200`.

Columns: **Title · Venue · Status · Category · Close** (`close_time`, `—` if null).
- **Venue** dropdown (`All / kalshi / polymarket`) and **Status** dropdown (`All / active / closed`,
  plus any distinct statuses present in the first fetch) re-fetch from the API with the chosen query
  params.
- **Title search** box filters the currently-loaded rows client-side (case-insensitive `includes`).
- Row counter + "loaded at" stamp as above.

---

## 5. Interactions & states

- **Tabs:** two buttons set `location.hash` to `#cross` / `#markets`; a `hashchange` handler shows the
  matching container and hides the other, and lazily loads that view's data on first show. Default
  `#cross` when no hash.
- **Refresh:** each view has a Refresh button that re-runs its fetch and updates the "loaded at" stamp.
- **Sort (cross view):** client-side comparator over the loaded rows; re-renders the table body.
- **Filters (market view):** venue/status change → re-fetch; search → re-filter loaded rows (no fetch).
- **Three states per view**, rendered into the view container:
  - **Loading:** "Loading…" shown while the fetch is in flight.
  - **Error:** non-2xx or thrown fetch → "Failed to load — status `<N>`" (or the error message) with a
    Retry button.
  - **Empty:** a `200` with `[]` → "No rows." (so an out-of-season empty `/cross-market` reads as
    intentional, not broken).

---

## 6. Styling scope

Minimal and legible only, in `styles.css`:
- System font stack; page max-width ~1100px centered.
- Tables: full-width, 1px borders, sticky `<thead>`, zebra rows, right-aligned numeric cells with
  tabular figures.
- Tab buttons: simple active/inactive states.
- No color system, no fonts to load, no images. The Pixel Augusta tokens
  (`app/design_handoff_bountygate_dashboard/prototype/dashboard/colors_and_type.css`) are the intended
  source for the **later** styling pass and are not used here.

---

## 7. Testing & verification

- **Backend (extend `app/web/tests/`, FastAPI `TestClient`):**
  - `GET /` → `200`, `content-type` starts with `text/html`, body contains the app root marker
    (e.g. `id="view-cross"`).
  - `GET /static/app.js` → `200`, JS content-type (`javascript` in the `content-type`).
  - Existing endpoint tests stay green (the new routes don't touch them).
- **Frontend JS:** no automated tests in the MVP (no Node/test-runner added). The pure helpers
  (`parseQuestionKey`, `formatPct`, the sort comparator) are written as standalone functions so a test
  harness can be added later without restructuring.
- **Manual verification:** run `py -3.12 -m uvicorn app.web.main:app` from the repo root with
  `DATABASE_URL` set; open `http://localhost:8000/`; confirm:
  - Cross tab shows real rows sorted by Spread desc; a numeric header re-sorts.
  - Market tab shows rows; venue/status filters re-fetch; search filters live.
  - Refresh updates the "loaded at" stamp; an empty/erroring fetch shows the right state.
  - Capture a screenshot of each tab.

---

## 8. Success criteria

- Visiting `/` on the running app shows a two-tab page.
- The cross-venue tab renders real `/cross-market` rows with parsed Sport/Date/Matchup/Side columns,
  probabilities as percentages (nulls as `—`), default-sorted by Spread desc, with working header sort.
- The market browser tab renders real `/markets` rows with working venue/status filters and title
  search.
- Loading, empty, and error states each render correctly.
- Backend route tests pass; existing web tests stay green.
- No new runtime dependency, no build step, no `node_modules`; the page ships on the existing Heroku web
  dyno.
