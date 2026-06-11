# Frontend MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single vanilla HTML/JS page, served by the existing FastAPI app, with two tabbed data tables — cross-venue comparison (`/cross-market`) and a market browser (`/markets`).

**Architecture:** `app/web/main.py` serves `static/index.html` at `/` and mounts `/static` for `app.js`/`styles.css`. The page is plain HTML + a single `app.js` (no framework, no build step) that `fetch()`es the same-origin API and builds tables in the DOM. No backend/schema change beyond the two serving routes; no new runtime dependency (StaticFiles/FileResponse ship with FastAPI/starlette).

**Tech Stack:** FastAPI/starlette (`StaticFiles`, `FileResponse`), vanilla HTML/CSS/JS, pytest (`fastapi.testclient`). Host commands use `py -3.12` (the project's Python; see project memory).

**Spec:** `docs/superpowers/specs/2026-06-10-frontend-mvp-design.md`

**Test command (web, host):** `cd /c/Users/tkmer/bountygate && py -3.12 -m pytest app/web/tests -q`

---

## File Structure

| Path | Responsibility | Status |
|---|---|---|
| `app/web/main.py` | add `GET /` (serve index.html) + `/static` mount | Modify |
| `app/web/static/index.html` | page shell: header, two tab buttons, two view containers, toolbars | Create |
| `app/web/static/styles.css` | minimal legible styling | Create |
| `app/web/static/app.js` | fetch + render for both views; tab routing; pure helpers | Create |
| `app/web/tests/test_static.py` | backend route tests (`/`, `/static/app.js`) | Create |

---

## Task 1: Serving routes + page shell

**Files:**
- Modify: `app/web/main.py`
- Create: `app/web/static/index.html`, `app/web/static/styles.css`, `app/web/static/app.js`
- Create: `app/web/tests/test_static.py`

- [ ] **Step 1: Write the failing test**

Create `app/web/tests/test_static.py`:

```python
from fastapi.testclient import TestClient

from app.web.main import app

client = TestClient(app)


def test_index_served_at_root():
    r = client.get("/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert 'id="view-cross"' in r.text
    assert 'id="view-markets"' in r.text


def test_app_js_served():
    r = client.get("/static/app.js")
    assert r.status_code == 200
    assert "javascript" in r.headers["content-type"]


def test_styles_css_served():
    r = client.get("/static/styles.css")
    assert r.status_code == 200
    assert "css" in r.headers["content-type"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate && py -3.12 -m pytest app/web/tests/test_static.py -q`
Expected: FAIL — `GET /` returns 404 (route not defined) / static files missing.

- [ ] **Step 3: Add the serving routes to `main.py`**

In `app/web/main.py`, add these imports at the top (with the existing imports):

```python
from pathlib import Path

from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
```

Then, **after** the existing `app.include_router(...)` lines at the bottom of the file, append:

```python
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")
```

- [ ] **Step 4: Create the page shell**

Create `app/web/static/index.html`:

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BountyGate — market data</title>
  <link rel="stylesheet" href="/static/styles.css">
</head>
<body>
  <header>
    <h1>BountyGate</h1>
    <nav class="tabs">
      <button class="tab" data-view="cross">Cross-venue</button>
      <button class="tab" data-view="markets">Markets</button>
    </nav>
  </header>

  <main>
    <section id="view-cross" class="view">
      <div class="toolbar">
        <button class="refresh" data-view="cross">Refresh</button>
        <span class="meta" id="meta-cross"></span>
      </div>
      <div id="body-cross" class="view-body"></div>
    </section>

    <section id="view-markets" class="view" hidden>
      <div class="toolbar">
        <label>Venue
          <select id="filter-venue">
            <option value="">All</option>
            <option value="kalshi">kalshi</option>
            <option value="polymarket">polymarket</option>
          </select>
        </label>
        <label>Status
          <select id="filter-status"><option value="">All</option></select>
        </label>
        <input id="search-markets" type="search" placeholder="Search title…">
        <button class="refresh" data-view="markets">Refresh</button>
        <span class="meta" id="meta-markets"></span>
      </div>
      <div id="body-markets" class="view-body"></div>
    </section>
  </main>

  <script src="/static/app.js"></script>
</body>
</html>
```

Create `app/web/static/styles.css`:

```css
:root { --border: #ccc; --ink: #1a1a1a; --muted: #666; --red: #b0211a; }
* { box-sizing: border-box; }
body { font-family: -apple-system, "Segoe UI", Roboto, sans-serif; color: var(--ink); margin: 0; }
header { padding: 12px 20px; border-bottom: 1px solid var(--border);
         display: flex; align-items: baseline; gap: 20px; }
h1 { font-size: 20px; margin: 0; }
.tabs { display: flex; gap: 6px; }
.tab { padding: 6px 12px; border: 1px solid var(--border); background: #f4f4f4; cursor: pointer; }
.tab.active { background: var(--ink); color: #fff; }
main { max-width: 1100px; margin: 0 auto; padding: 20px; }
.toolbar { display: flex; gap: 12px; align-items: center; margin-bottom: 12px; flex-wrap: wrap; }
.toolbar label { font-size: 13px; color: var(--muted); }
.meta { color: var(--muted); font-size: 13px; margin-left: auto; }
table { width: 100%; border-collapse: collapse; font-size: 14px; }
th, td { border: 1px solid var(--border); padding: 6px 8px; text-align: left; }
thead th { position: sticky; top: 0; background: #f4f4f4; cursor: pointer; white-space: nowrap; }
tbody tr:nth-child(even) { background: #fafafa; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
.state { padding: 24px; color: var(--muted); }
.state.error { color: var(--red); }
```

Create `app/web/static/app.js` (stub — replaced with the full implementation in Task 2):

```javascript
'use strict';
// UI boot — implemented in Task 2.
console.log('bountygate ui boot');
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd /c/Users/tkmer/bountygate && py -3.12 -m pytest app/web/tests/test_static.py -q`
Expected: PASS (3 passed).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/web/main.py app/web/static/index.html app/web/static/styles.css app/web/static/app.js app/web/tests/test_static.py
git commit -m "feat(web): serve static frontend shell (index + two-tab layout)"
```

---

## Task 2: `app.js` — both views (fetch, render, routing)

**Files:**
- Modify (replace stub): `app/web/static/app.js`

> No automated JS test in the MVP (the spec defers a JS test runner). Verified by loading the page in Task 3. The pure helpers (`parseQuestionKey`, `formatPct`) are standalone so a harness can be added later.

- [ ] **Step 1: Replace `app/web/static/app.js` with the full implementation**

Overwrite `app/web/static/app.js`:

```javascript
'use strict';

// ---------- pure helpers ----------
function formatPct(x) {
  return (x === null || x === undefined) ? '—' : (x * 100).toFixed(1) + '%';
}

function parseQuestionKey(key) {
  // "nba:2026-06-09:SAS@NYK:NYK" -> {sport, date, matchup, side}
  const [sport = '', date = '', pair = '', side = ''] = (key || '').split(':');
  const matchup = pair.includes('@') ? pair.replace('@', ' @ ') : pair;
  return { sport: sport.toUpperCase(), date, matchup, side };
}

function fmtTime(d) {
  return d.toTimeString().slice(0, 8); // HH:MM:SS
}

// ---------- generic fetch + state rendering ----------
async function loadInto(bodyEl, metaEl, url, render) {
  bodyEl.innerHTML = '<div class="state">Loading…</div>';
  const fail = (msg) => {
    bodyEl.innerHTML = `<div class="state error">Failed to load — ${msg} `
      + `<button class="retry">Retry</button></div>`;
    bodyEl.querySelector('.retry').onclick = () => loadInto(bodyEl, metaEl, url, render);
  };
  try {
    const resp = await fetch(url);
    if (!resp.ok) { fail('status ' + resp.status); return; }
    const rows = await resp.json();
    if (metaEl) metaEl.textContent = `${rows.length} rows · loaded at ${fmtTime(new Date())}`;
    if (!rows.length) { bodyEl.innerHTML = '<div class="state">No rows.</div>'; return; }
    render(bodyEl, rows);
  } catch (e) {
    fail(String(e));
  }
}

function tableEl(headers) {
  const t = document.createElement('table');
  const tr = document.createElement('tr');
  headers.forEach(h => {
    const th = document.createElement('th');
    th.textContent = h.label;
    if (h.sortKey) th.dataset.sort = h.sortKey;
    tr.appendChild(th);
  });
  const thead = document.createElement('thead');
  thead.appendChild(tr);
  t.appendChild(thead);
  t.appendChild(document.createElement('tbody'));
  return t;
}

// ---------- cross-venue view ----------
const CROSS_HEADERS = [
  { label: 'Sport' }, { label: 'Date' }, { label: 'Matchup' }, { label: 'Side' },
  { label: 'Kalshi', sortKey: 'kalshi_prob' },
  { label: 'Polymarket', sortKey: 'polymarket_prob' },
  { label: 'Sportsbook', sortKey: 'sportsbook_consensus_prob' },
  { label: 'Spread', sortKey: 'max_spread' },
];
let crossRows = [];
let crossSort = { key: 'max_spread', dir: -1 };

function renderCross(bodyEl, rows) {
  crossRows = rows;
  bodyEl.innerHTML = '';
  const t = tableEl(CROSS_HEADERS);
  t.querySelectorAll('th[data-sort]').forEach(th => {
    th.onclick = () => {
      const key = th.dataset.sort;
      crossSort = { key, dir: crossSort.key === key ? -crossSort.dir : -1 };
      paintCrossBody(t.querySelector('tbody'));
    };
  });
  bodyEl.appendChild(t);
  paintCrossBody(t.querySelector('tbody'));
}

function paintCrossBody(tbody) {
  const { key, dir } = crossSort;
  const sorted = [...crossRows].sort((a, b) => {
    const av = a[key], bv = b[key];
    if (av === null || av === undefined) return 1;   // nulls last
    if (bv === null || bv === undefined) return -1;
    return (av - bv) * dir;
  });
  tbody.innerHTML = '';
  sorted.forEach(r => {
    const k = parseQuestionKey(r.question_key);
    const cells = [
      [k.sport, ''], [k.date, ''], [k.matchup, ''], [k.side, ''],
      [formatPct(r.kalshi_prob), 'num'], [formatPct(r.polymarket_prob), 'num'],
      [formatPct(r.sportsbook_consensus_prob), 'num'], [formatPct(r.max_spread), 'num'],
    ];
    const tr = document.createElement('tr');
    cells.forEach(([val, cls]) => {
      const td = document.createElement('td');
      td.textContent = val;
      if (cls) td.className = cls;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

function loadCross() {
  loadInto(
    document.getElementById('body-cross'),
    document.getElementById('meta-cross'),
    '/cross-market?limit=200',
    renderCross,
  );
}

// ---------- market browser view ----------
const MARKET_HEADERS = [
  { label: 'Title' }, { label: 'Venue' }, { label: 'Status' },
  { label: 'Category' }, { label: 'Close' },
];
let marketRows = [];

function renderMarkets(bodyEl, rows) {
  marketRows = rows;
  const statusSel = document.getElementById('filter-status');
  const have = new Set([...statusSel.options].map(o => o.value));
  [...new Set(rows.map(r => r.status).filter(Boolean))].forEach(s => {
    if (!have.has(s)) {
      const o = document.createElement('option');
      o.value = s; o.textContent = s;
      statusSel.appendChild(o);
    }
  });
  bodyEl.innerHTML = '';
  const t = tableEl(MARKET_HEADERS);
  bodyEl.appendChild(t);
  paintMarketBody(t.querySelector('tbody'));
}

function paintMarketBody(tbody) {
  const q = document.getElementById('search-markets').value.trim().toLowerCase();
  const shown = q ? marketRows.filter(r => (r.title || '').toLowerCase().includes(q)) : marketRows;
  tbody.innerHTML = '';
  shown.forEach(r => {
    const tr = document.createElement('tr');
    [r.title || '', r.venue_key || '', r.status || '', r.category || '', r.close_time || '—']
      .forEach(v => {
        const td = document.createElement('td');
        td.textContent = v;
        tr.appendChild(td);
      });
    tbody.appendChild(tr);
  });
}

function loadMarkets() {
  const venue = document.getElementById('filter-venue').value;
  const status = document.getElementById('filter-status').value;
  const params = new URLSearchParams({ limit: '200' });
  if (venue) params.set('venue', venue);
  if (status) params.set('status', status);
  loadInto(
    document.getElementById('body-markets'),
    document.getElementById('meta-markets'),
    '/markets?' + params.toString(),
    renderMarkets,
  );
}

// ---------- tab routing + wiring ----------
const LOADERS = { cross: loadCross, markets: loadMarkets };
const loaded = {};

function showView(name) {
  if (!LOADERS[name]) name = 'cross';
  document.querySelectorAll('.view').forEach(v => { v.hidden = v.id !== 'view-' + name; });
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.view === name));
  if (!loaded[name]) { loaded[name] = true; LOADERS[name](); }
}

function currentView() {
  return (location.hash || '#cross').replace('#', '');
}

window.addEventListener('hashchange', () => showView(currentView()));
document.querySelectorAll('.tab').forEach(t => {
  t.onclick = () => { location.hash = t.dataset.view; };
});
document.querySelectorAll('.refresh').forEach(b => {
  b.onclick = () => LOADERS[b.dataset.view]();
});
document.getElementById('filter-venue').onchange = loadMarkets;
document.getElementById('filter-status').onchange = loadMarkets;
document.getElementById('search-markets').oninput = () => {
  const tbody = document.querySelector('#body-markets tbody');
  if (tbody) paintMarketBody(tbody);
};

showView(currentView());
```

- [ ] **Step 2: Re-run the backend suite (no regressions)**

Run: `cd /c/Users/tkmer/bountygate && py -3.12 -m pytest app/web/tests -q`
Expected: all green (the static + existing endpoint tests; `app.js` change doesn't affect them).

- [ ] **Step 3: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add app/web/static/app.js
git commit -m "feat(web): cross-venue + market browser views (vanilla JS)"
```

---

## Task 3: End-to-end verification

**Files:** none (verification only).

- [ ] **Step 1: Start the app against the live database**

```bash
cd /c/Users/tkmer/bountygate
export DATABASE_URL="$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"'"'"'' | tr -d '\r')"
py -3.12 -m uvicorn app.web.main:app --port 8000
```
(Run in the background, or in a separate shell, so the next steps can hit it.)

- [ ] **Step 2: Confirm the page and assets serve**

```bash
curl -s -o /dev/null -w "%{http_code} %{content_type}\n" http://localhost:8000/
curl -s -o /dev/null -w "%{http_code} %{content_type}\n" http://localhost:8000/static/app.js
curl -s "http://localhost:8000/cross-market?limit=2"
```
Expected: `200 text/html...` for `/`, `200 ...javascript...` for `app.js`, and a JSON array of cross-market rows.

- [ ] **Step 3: Verify both views in a browser**

Open `http://localhost:8000/` (use the project browser tooling — e.g. the `mcp__chrome-devtools__*` or `mcp__playwright__*` tools — or a real browser). Confirm:
- **Cross-venue tab** (default): a table of real rows with Sport/Date/Matchup/Side + three percentage columns + Spread; rows are sorted by **Spread descending**; clicking the "Kalshi" or "Spread" header re-sorts; the meta line shows "N rows · loaded at …".
- **Markets tab**: switch via the tab button (URL hash becomes `#markets`); a table of markets renders; changing the **Venue** dropdown to `kalshi` re-fetches and the rows update; typing in the search box filters the visible rows live.
- **Refresh** updates the "loaded at" stamp.
- Capture a screenshot of each tab.

- [ ] **Step 4: Check the empty/error states (quick spot-check)**

Briefly confirm graceful states (either by observation or reasoning from the code): an out-of-season empty `/cross-market` would render "No rows."; a failed fetch renders "Failed to load — …" with a Retry button. No code change — just confirm the page doesn't render a blank/broken view in those cases.

- [ ] **Step 5: Report completion**

Summarize against the spec's §8 success criteria: `/` shows a two-tab page; cross-venue tab renders parsed columns with percentages (nulls as `—`), default-sorted by Spread desc, header sort works; market browser tab renders with working venue/status filters and title search; loading/empty/error states render; backend route tests pass and existing web tests stay green; no new dependency, no build step, page ships on the existing Heroku web dyno.
