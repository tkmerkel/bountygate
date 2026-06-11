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
