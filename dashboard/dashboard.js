/* BountyGate · The Daily Hedge — vanilla JS port of the Pixel Augusta
   design handoff. Single file, no build step. Polls four endpoints
   every 30s and renders into static section anchors in index.html. */

(() => {
"use strict";

// ────────────────────────────────────────────────────────────
// Constants
// ────────────────────────────────────────────────────────────
const ISSUE_AXES = [
  { key: "wasted_wait",   short: "wasted",   label: "Wasted" },
  { key: "selector_miss", short: "selector", label: "Selector" },
  { key: "slip_state",    short: "slip",     label: "Slip" },
  { key: "auth_geo",      short: "auth",     label: "Auth / Geo" },
  { key: "stealth",       short: "stealth",  label: "Stealth" },
];

// Sidebar wiki page list — mirrors mock.js::wikiPages. v1 hardcodes
// these because the wiki backend (/wiki/<slug>) exists already and we
// don't have a /api/wiki-pages endpoint yet. Add one when the wiki port
// lands.
const WIKI_PAGES = [
  { slug: "dashboard",    title: "Dashboard",                  glyph: "§", updated: "live"  },
  { slug: "bot-flow",     title: "Bot execution flow",         glyph: "¶", updated: "—"     },
  { slug: "auth-sop",     title: "Auth & session SOP",         glyph: "¶", updated: "—"     },
  { slug: "selector-map", title: "Selector map · sportsbooks", glyph: "¶", updated: "—"     },
  { slug: "watcher-sop",  title: "Watcher operations",         glyph: "¶", updated: "—"     },
  { slug: "migrations",   title: "DB migrations log",          glyph: "¶", updated: "—"     },
  { slug: "discord",      title: "Discord alert taxonomy",     glyph: "¶", updated: "—"     },
];

// Mutable client state — fetches stash their latest response here so
// the mast/ticker can recompute from a single source of truth.
const state = {
  runs: [],
  accounts: { books: {}, updated_at: null },
  watchers: { watchers: [], checked_at: null },
  pnl: { days: [], par_per_day: 12.50, updated_at: null },
  health: { app: "—", db: "—" },
  goodBets: { bets: [], updated_at: null },
  clv: { n_bets: 0, avg_clv_pct: null, beat_close_rate: null, updated_at: null },
  openRunId: null,
  filter: "all",
  betType: "all",
};

// ────────────────────────────────────────────────────────────
// Utilities
// ────────────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

function fmtUsd(n, signed) {
  if (n == null || isNaN(n)) return "—";
  const sign = n < 0 ? "−" : (signed ? "+" : "");
  return sign + "$" + Math.abs(n).toLocaleString("en-US", {
    minimumFractionDigits: 2, maximumFractionDigits: 2
  });
}

function fmtSecs(s) {
  if (s == null) return "—";
  return s < 60 ? Math.round(s) + "s" : Math.round(s / 60) + "m";
}

// fraction -> "x.x%" (input already a percent, e.g. edge_pct=2.5 -> "2.5%")
function fmtPctVal(n, signed) {
  if (n == null || isNaN(n)) return "—";
  const sign = n < 0 ? "−" : (signed ? "+" : "");
  return sign + Math.abs(n).toFixed(1) + "%";
}

// fraction in [0,1] -> "x.x%"
function fmtFrac(n) {
  if (n == null || isNaN(n)) return "—";
  return (n * 100).toFixed(1) + "%";
}

function fmtHours(h) {
  if (h == null || isNaN(h)) return "—";
  if (h < 0) return "live";
  if (h < 1) return Math.round(h * 60) + "m";
  return h.toFixed(1) + "h";
}

function ageLabel(iso) {
  if (!iso) return "—";
  const sec = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (sec < 60) return Math.round(sec) + "s ago";
  if (sec < 3600) return Math.round(sec / 60) + "m ago";
  if (sec < 86400) return (sec / 3600).toFixed(1) + "h ago";
  return (sec / 86400).toFixed(1) + "d ago";
}

function fmtTime(iso) {
  const d = new Date(iso);
  return {
    date: d.toLocaleDateString("en-US", { month: "short", day: "2-digit" }).toUpperCase(),
    time: d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false }),
  };
}

function issueCount(v) {
  return Array.isArray(v) ? v.length : (v && v !== "none" ? 1 : 0);
}

// Highlight leading "t=hh:mm" timestamps in finding strings.
function renderFinding(s) {
  const m = String(s).match(/^(t=\d\d:\d\d(?:\s*[→\-]\s*t=\d\d:\d\d)?)(.*)$/);
  if (m) return `<span class="t">${esc(m[1])}</span>${esc(m[2])}`;
  return esc(s);
}

// SC3K WindowCard wrapper — title bar + body.
function windowCard(title, subtitle, glyph, bodyHtml) {
  return `
    <div class="pa-card">
      <div class="pa-card__title">
        <span>${esc(title)}${subtitle ? `<span style="margin-left:10px;color:rgba(255,255,255,.6);font-size:14px;letter-spacing:.08em;">${esc(subtitle)}</span>` : ""}</span>
        <span style="display:flex;align-items:center;gap:6px;">
          <button class="pa-card__glyph">${esc(glyph || "[?]")}</button>
        </span>
      </div>
      <div class="pa-card__body">${bodyHtml}</div>
    </div>
  `;
}

// ────────────────────────────────────────────────────────────
// Sidebar wiki list (one-shot render)
// ────────────────────────────────────────────────────────────
function renderSidebarWikiList() {
  const ul = document.getElementById("side-wiki-list");
  if (!ul) return;
  // The "dashboard" entry is the active page; the rest link to /wiki/<slug>.
  ul.innerHTML = WIKI_PAGES.map(p => {
    const isActive = p.slug === "dashboard";
    const href = isActive ? "#" : `/wiki/${p.slug}`;
    return `<a class="bg-side__item${isActive ? " is-active" : ""}" href="${esc(href)}">
      <span class="bg-side__item-glyph">${esc(p.glyph)}</span>
      <span>${esc(p.title)}</span>
      <span class="bg-side__item-meta">${esc(p.updated)}</span>
    </a>`;
  }).join("");
}

// ────────────────────────────────────────────────────────────
// Mast & mast strip
// ────────────────────────────────────────────────────────────
function renderMast() {
  const dateStr = new Date().toLocaleDateString("en-US", {
    weekday: "long", month: "long", day: "numeric", year: "numeric"
  }).toUpperCase();
  document.getElementById("mast-date").textContent = dateStr;
  document.getElementById("mast-runno").textContent = String(state.runs.length).padStart(3, "0");

  const ws = state.watchers.watchers || [];
  const offNominal = ws.filter(w => w.status !== "ok").length;
  document.getElementById("mast-strip-watchers").innerHTML =
    `<span class="live-dot"></span>` +
    (offNominal === 0
      ? "ALL WATCHERS NOMINAL"
      : `${offNominal} WATCHER${offNominal === 1 ? "" : "S"} OFF-NOMINAL`);

  const lastRun = state.runs[0];
  document.getElementById("mast-strip-lastrun").textContent =
    `LAST RUN · ${lastRun ? lastRun.player.toUpperCase() : "—"} · ${lastRun ? (lastRun.outcome || "—").toUpperCase() : "—"}`;

  const exposure = Object.values(state.accounts.books || {})
    .reduce((s, b) => s + (b.balance || 0), 0);
  document.getElementById("mast-strip-exposure").textContent =
    `EXPOSURE · ${fmtUsd(exposure)}`;

  // Sidebar health pills
  document.getElementById("side-health-app").textContent = state.health.app;
  document.getElementById("side-health-db").textContent = state.health.db;
}

// ────────────────────────────────────────────────────────────
// Hero / profit chart (SVG)
// ────────────────────────────────────────────────────────────
function renderHero() {
  const root = document.getElementById("sec-hero");
  if (!root) return;

  const days = state.pnl.days || [];
  if (days.length === 0) {
    root.innerHTML = windowCard(
      "PROFIT_LINE.EXE",
      "30-DAY DAILY P&L · NO DATA YET",
      "[–]",
      `<div style="padding:40px;text-align:center;color:var(--rule-gray);font-family:var(--font-mono-pixel);">
        — NO P&amp;L HISTORY YET. account_stats_history populates as the scraper runs. —
      </div>`
    );
    return;
  }

  // Cumulative series
  let s = 0;
  const cum = days.map(d => +(s += d.pnl).toFixed(2));

  // Map dates → list of runs that landed that day
  const runsByDay = {};
  state.runs.forEach(r => {
    const key = (r.occurred_at || "").slice(0, 10);
    (runsByDay[key] = runsByDay[key] || []).push(r);
  });

  // Chart geometry
  const W = 880, H = 300;
  const M = { t: 18, r: 18, b: 42, l: 56 };
  const innerW = W - M.l - M.r;
  const innerH = H - M.t - M.b;
  const n = days.length;
  const xStep = n > 1 ? innerW / (n - 1) : innerW;
  const x = i => M.l + i * xStep;

  const dailyPnls = days.map(d => d.pnl);
  const yMaxRaw = Math.max(Math.max(...dailyPnls, 0), Math.max(...cum, 0));
  const yMinRaw = Math.min(Math.min(...dailyPnls, 0), Math.min(...cum, 0));
  const pad = (yMaxRaw - yMinRaw) * 0.12 || 10;
  const yMax = Math.ceil((yMaxRaw + pad) / 10) * 10;
  const yMin = Math.floor((yMinRaw - pad) / 10) * 10;
  const y = v => M.t + (1 - (v - yMin) / (yMax - yMin)) * innerH;
  const y0 = y(0);

  const yStep = (yMax - yMin) > 200 ? 50 : (yMax - yMin) > 80 ? 20 : 10;
  const yTicks = [];
  for (let v = Math.ceil(yMin / yStep) * yStep; v <= yMax; v += yStep) yTicks.push(v);

  // X-axis ticks: every Sunday + first + last
  const xTickIdxs = [];
  days.forEach((d, i) => {
    if (i === 0 || i === n - 1) { xTickIdxs.push(i); return; }
    if (new Date(d.date + "T00:00:00").getUTCDay() === 0) xTickIdxs.push(i);
  });

  const linePts = cum.map((v, i) => `${x(i)},${y(v)}`).join(" ");
  const areaPts = `${x(0)},${y0} ${linePts} ${x(n - 1)},${y0}`;

  // Stats
  const total = +cum[cum.length - 1].toFixed(2);
  const last7 = days.slice(-7).reduce((acc, d) => acc + d.pnl, 0);
  const winDays = days.filter(d => d.pnl > 0).length;
  const lossDays = days.filter(d => d.pnl < 0).length;
  const bestDay = days.reduce((a, b) => (a == null || b.pnl > a.pnl) ? b : a, null);
  const worstDay = days.reduce((a, b) => (a == null || b.pnl < a.pnl) ? b : a, null);

  // Annotations
  const annotations = days.map((d, i) => d.note ? { i, d } : null).filter(Boolean);

  const fmtDateShort = iso => {
    const d = new Date(iso + "T12:00:00");
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" }).toUpperCase();
  };

  const yTickHtml = yTicks.map(v => `
    <g>
      <line x1="${M.l}" x2="${M.l + innerW}" y1="${y(v)}" y2="${y(v)}"
            stroke="${v === 0 ? "var(--ink-black)" : "rgba(0,0,0,0.10)"}"
            stroke-width="${v === 0 ? 1 : 0.5}"
            stroke-dasharray="${v === 0 ? "" : "2 3"}" />
      <text x="${M.l - 8}" y="${y(v) + 4}" text-anchor="end" class="bg-chart__yt" fill="var(--rule-gray)">
        ${(v >= 0 ? "" : "−") + "$" + Math.abs(v)}
      </text>
    </g>
  `).join("");

  const barsHtml = days.map((d, i) => {
    const bw = Math.max(4, xStep - 4);
    const yTop = Math.min(y(d.pnl), y0);
    const yBot = Math.max(y(d.pnl), y0);
    const cls = d.pnl >= 0 ? "is-pos" : "is-neg";
    return `<rect class="bg-chart__bar ${cls}" x="${x(i) - bw / 2}" y="${yTop}" width="${bw}" height="${Math.max(1, yBot - yTop)}" />`;
  }).join("");

  const runDotsHtml = days.map((d, i) => {
    const has = (runsByDay[d.date] || []).length > 0;
    if (!has) return "";
    return `<circle cx="${x(i)}" cy="${H - M.b + 14}" r="2.5" fill="var(--ink-black)" />`;
  }).join("");

  const cumDotsHtml = cum.map((v, i) =>
    `<circle cx="${x(i)}" cy="${y(v)}" r="2.2" fill="var(--augusta-green)" stroke="#fff" stroke-width="1" />`
  ).join("");

  const annoHtml = annotations.map(({ i, d }, k) => {
    const tx = x(i);
    const tyLbl = M.t + 4 + (k % 2) * 14;
    return `<g class="bg-chart__anno">
      <line x1="${tx}" y1="${Math.min(y(cum[i]), y(d.pnl))}" x2="${tx}" y2="${tyLbl + 4}"
            stroke="var(--ink-black)" stroke-width="0.6" stroke-dasharray="2 2" />
      <text x="${tx + 4}" y="${tyLbl + 11}" class="bg-chart__anno-t" fill="var(--ink-black)">
        ${esc(d.note)}
      </text>
    </g>`;
  }).join("");

  const xTickHtml = xTickIdxs.map(i => `
    <g>
      <line x1="${x(i)}" x2="${x(i)}" y1="${M.t + innerH}" y2="${M.t + innerH + 4}"
            stroke="var(--ink-black)" stroke-width="0.8" />
      <text x="${x(i)}" y="${M.t + innerH + 17}" text-anchor="middle"
            class="bg-chart__xt" fill="var(--rule-gray)">${fmtDateShort(days[i].date)}</text>
    </g>
  `).join("");

  const filedDate = state.pnl.updated_at ? state.pnl.updated_at.slice(0, 10) : "—";

  const inner = `
    <div class="bg-chart">
      <div class="bg-chart__hd">
        <div class="bg-chart__hd-left">
          <div class="bg-chart__kicker">FILED · ${esc(filedDate)} · NET PROFIT, DAILY</div>
          <div class="bg-chart__hed">The House Account, Day by Day</div>
          <div class="bg-chart__dek">Cumulative ledger across FanDuel, BetMGM, DraftKings &amp; Caesars · paired arbitrage wagers only · settled USD</div>
        </div>
        <div class="bg-chart__hd-right">
          <div class="bg-chart__kpi">
            <div class="bg-chart__kpi-lbl">${days.length}D NET</div>
            <div class="bg-chart__kpi-val ${total >= 0 ? "is-pos" : "is-neg"}">
              ${total >= 0 ? "+" : "−"}$${Math.abs(total).toFixed(2)}
            </div>
          </div>
          <div class="bg-chart__kpi">
            <div class="bg-chart__kpi-lbl">LAST 7D</div>
            <div class="bg-chart__kpi-val ${last7 >= 0 ? "is-pos" : "is-neg"}">
              ${last7 >= 0 ? "+" : "−"}$${Math.abs(last7).toFixed(2)}
            </div>
          </div>
        </div>
      </div>

      <svg class="bg-chart__svg" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
        <rect x="${M.l}" y="${M.t}" width="${innerW}" height="${innerH}" fill="#FFFFFF" stroke="var(--ink-black)" />
        ${yTickHtml}
        <polygon points="${areaPts}" fill="rgba(11,91,64,0.10)" />
        <polyline points="${linePts}" fill="none" stroke="var(--augusta-green)" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round" />
        ${barsHtml}
        ${runDotsHtml}
        ${cumDotsHtml}
        ${annoHtml}
        ${xTickHtml}
      </svg>

      <div class="bg-chart__legend">
        <span><i class="sw line"></i>CUMULATIVE P&amp;L</span>
        <span><i class="sw bar-pos"></i>DAILY · WIN</span>
        <span><i class="sw bar-neg"></i>DAILY · LOSS</span>
        <span><i class="sw dot"></i>RUN FILED</span>
      </div>
    </div>
  `;

  const metaHtml = `
    <div class="bg-hero__meta">
      <div class="bg-hero__stat">
        <div class="bg-hero__stat-label">${days.length}D NET</div>
        <div class="bg-hero__stat-val ${total >= 0 ? "is-pos" : "is-neg"}">
          ${total >= 0 ? "+" : "−"}$${Math.abs(total).toFixed(2)}
        </div>
      </div>
      <div class="bg-hero__stat">
        <div class="bg-hero__stat-label">WIN DAYS</div>
        <div class="bg-hero__stat-val is-pos">${winDays} <span style="color:var(--rule-gray);font-size:18px;">/ ${days.length}</span></div>
      </div>
      <div class="bg-hero__stat">
        <div class="bg-hero__stat-label">BEST · WORST</div>
        <div class="bg-hero__stat-val">
          <span class="is-pos">+$${(bestDay ? bestDay.pnl : 0).toFixed(2)}</span>
          <span style="color:var(--rule-gray);"> · </span>
          <span class="is-neg">−$${Math.abs(worstDay ? worstDay.pnl : 0).toFixed(2)}</span>
        </div>
      </div>
      <div class="bg-hero__stat">
        <div class="bg-hero__stat-label">LOSS DAYS</div>
        <div class="bg-hero__stat-val is-neg">${lossDays}</div>
      </div>
    </div>
  `;

  root.innerHTML = windowCard(
    "PROFIT_LINE.EXE",
    `${days.length}-DAY DAILY P&L · CUMULATIVE OVERLAY`,
    "[–]",
    inner
  ) + metaHtml;
}

// ────────────────────────────────────────────────────────────
// Accounts
// ────────────────────────────────────────────────────────────
function renderAccounts() {
  const root = document.getElementById("sec-accounts");
  if (!root) return;
  const books = Object.values(state.accounts.books || {});

  if (books.length === 0) {
    root.innerHTML = windowCard("ACCOUNT_LEDGER.EXE", "NO DATA", "[?]",
      `<div style="padding:24px;text-align:center;color:var(--rule-gray);font-family:var(--font-mono-pixel);">
        — No account stats yet. The scraper populates this. —
      </div>`);
    return;
  }

  const cards = books.map(b => {
    const status = b.scrape_status || "unknown";
    const statusLabel = status === "ok" ? "● OK"
      : status === "amber" ? "▲ STALE"
      : status === "red" ? "✕ DOWN"
      : "· —";
    const statusCls = status === "ok" ? "ok" : status === "amber" ? "amber" : "red";
    const spark = b.spark || [];
    const sparkHtml = spark.length === 0 ? "" : spark.map(v => {
      const mag = Math.min(1, Math.abs(v) / 14);
      return `<div class="b${v < 0 ? " neg" : ""}" style="height:${Math.max(2, mag * 22)}px;"></div>`;
    }).join("");
    return `
      <div class="bg-book">
        <div class="bg-book__row1">
          <span class="bg-book__name">${esc((b.book || "—").toUpperCase())}</span>
          <span class="bg-book__status ${statusCls}">${esc(statusLabel)}</span>
        </div>
        <div class="bg-book__balance">${esc(fmtUsd(b.balance))}</div>
        <div class="bg-book__sub">
          avail ${esc(fmtUsd(b.available_liquidity))} · pending ${esc(fmtUsd(b.pending_wagers))}
        </div>
        <div class="bg-book__pnl ${b.pnl_7d >= 0 ? "pos" : "neg"}">
          7D P&amp;L · ${esc(fmtUsd(b.pnl_7d, true))}
        </div>
        ${sparkHtml ? `<div class="bg-book__spark" aria-hidden="true">${sparkHtml}</div>` : ""}
        ${b.last_error ? `<div class="bg-book__error">⚠ ${esc(b.last_error)}</div>` : ""}
      </div>
    `;
  }).join("");

  root.innerHTML = windowCard(
    "ACCOUNT_LEDGER.EXE",
    "UPDATED " + ageLabel(state.accounts.updated_at),
    "[?]",
    `<div class="bg-books">${cards}</div>`
  );
}

// ────────────────────────────────────────────────────────────
// Watchers
// ────────────────────────────────────────────────────────────
function renderWatchers() {
  const root = document.getElementById("sec-watchers");
  if (!root) return;
  const ws = state.watchers.watchers || [];

  if (ws.length === 0) {
    root.innerHTML = windowCard("WATCHER_STATUS.EXE", "NO DATA", "[?]",
      `<div style="padding:24px;text-align:center;color:var(--rule-gray);font-family:var(--font-mono-pixel);">
        — No watcher heartbeats yet. —
      </div>`);
    return;
  }

  const rows = ws.map(w => {
    const dotCls = !w.is_running ? "idle"
      : w.status === "ok" ? "ok"
      : w.status === "amber" ? "amber" : "red";
    const oldest = w.oldest_pending_age_s > 0
      ? `<span style="color:var(--rule-gray);"> · ${esc(fmtSecs(w.oldest_pending_age_s))}</span>`
      : "";
    const errBadge = w.errors_24h > 0
      ? `<span style="color:var(--ledger-red);"> / ${w.errors_24h}e</span>`
      : "";
    return `
      <tr>
        <td>
          <span class="bg-dot ${dotCls}"></span>
          <span style="font-family:var(--font-mono-pixel);font-size:16px;letter-spacing:.04em;">${esc(w.name)}</span>
          ${!w.is_running ? `<span style="color:var(--rule-gray);font-family:var(--font-mono-pixel);margin-left:6px;font-size:13px;">· IDLE</span>` : ""}
          ${w.last_error ? `<div style="font-size:12px;color:var(--ledger-red);font-family:var(--font-mono-pixel);margin-top:2px;">⚠ ${esc(w.last_error)}</div>` : ""}
        </td>
        <td class="mono">${esc(ageLabel(w.last_tick_at))}</td>
        <td class="num mono">${w.pending_count || 0}${oldest}</td>
        <td class="num mono">${w.completed_24h || 0}${errBadge}</td>
      </tr>
    `;
  }).join("");

  const tableHtml = `
    <table class="bg-table">
      <thead>
        <tr><th>WATCHER</th><th>TICK</th><th class="num">BACKLOG</th><th class="num">24H</th></tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;

  root.innerHTML = windowCard(
    "WATCHER_STATUS.EXE",
    "CHECKED " + ageLabel(state.watchers.checked_at),
    "[?]",
    tableHtml
  );
}

// ────────────────────────────────────────────────────────────
// Issue meters
// ────────────────────────────────────────────────────────────
function metersHtml(issues, max) {
  max = max || 4;
  return `<div class="bg-meters" title="Findings per axis: wasted · selector · slip · auth · stealth">` +
    ISSUE_AXES.map(a => {
      const ct = issueCount(issues && issues[a.key]);
      let segs = "";
      for (let i = 0; i < max; i++) {
        segs += `<span class="seg${i < ct ? " on" : ""}"></span>`;
      }
      return `<div class="bg-meter" data-key="${esc(a.short)}" title="${esc(a.label)}: ${ct}">${segs}</div>`;
    }).join("") + `</div>`;
}

// ────────────────────────────────────────────────────────────
// Runs table
// ────────────────────────────────────────────────────────────
function renderRuns() {
  const root = document.getElementById("sec-runs");
  if (!root) return;
  const runs = state.runs || [];
  const filtered = state.filter === "all"
    ? runs
    : runs.filter(r => r.outcome === state.filter);
  const counts = {
    all:     runs.length,
    success: runs.filter(r => r.outcome === "success").length,
    failure: runs.filter(r => r.outcome === "failure").length,
    skipped: runs.filter(r => r.outcome === "skipped").length,
  };

  const filterChips = [
    ["all",     "ALL",     "default"],
    ["success", "SUCCESS", "success"],
    ["failure", "FAILURE", "failure"],
    ["skipped", "SKIPPED", "skipped"],
  ].map(([key, label, cls]) =>
    `<button class="bg-chip ${cls}${state.filter === key ? " is-active" : ""}" data-filter="${esc(key)}">
      ${esc(label)} · ${counts[key]}
    </button>`
  ).join("");

  const rowsHtml = filtered.map(r => {
    const isOpen = state.openRunId === r.run_id;
    const t = fmtTime(r.occurred_at);
    const wasted = (r.estimated_wasted_wait_s || 0);
    const dur = r.duration_s != null
      ? `${r.duration_s.toFixed(1)}s${wasted > 0 ? `<span class="wasted">−${wasted}s waste</span>` : ""}`
      : "—";

    let expandedHtml = "";
    if (isOpen) {
      const axesHtml = ISSUE_AXES.map(a => {
        const items = Array.isArray(r.issues && r.issues[a.key]) ? r.issues[a.key] : [];
        const body = items.length === 0
          ? `<div class="none">— NO FINDINGS —</div>`
          : `<ul>${items.map(it => `<li>${renderFinding(it)}</li>`).join("")}</ul>`;
        return `
          <div class="bg-axis${items.length === 0 ? " is-empty" : ""}">
            <div class="bg-axis__hd"><span>${esc(a.label.toUpperCase())}</span><span class="ct">${items.length}</span></div>
            <div class="bg-axis__body">${body}</div>
          </div>
        `;
      }).join("");

      const recBtn = r.video_url
        ? `<a class="pa-btn pa-btn--primary" href="${esc(r.video_url)}" target="_blank" rel="noopener">▶ OPEN RECORDING</a>`
        : `<button class="pa-btn pa-btn--primary is-disabled">▶ OPEN RECORDING</button>`;
      const revBtn = r.review_url
        ? `<a class="pa-btn" href="${esc(r.review_url)}" target="_blank" rel="noopener">VIEW REVIEW.MD</a>`
        : `<button class="pa-btn is-disabled">VIEW REVIEW.MD</button>`;

      expandedHtml = `
        <tr>
          <td colspan="7" class="bg-runs__expand-cell">
            <div class="bg-runs__expand">
              <div class="bg-runs__top-finding">
                <div class="lbl">EDITORIAL · TOP FINDING</div>
                <div class="body">${esc(r.top_finding || "— no finding recorded —")}</div>
                <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
                  ${recBtn} ${revBtn}
                  <button class="pa-btn pa-btn--ghost is-disabled">FILE TO ARCHIVE</button>
                </div>
              </div>
              <div class="bg-axes">${axesHtml}</div>
            </div>
          </td>
        </tr>
      `;
    }

    return `
      <tr class="is-row${isOpen ? " is-open" : ""}" data-run-id="${esc(r.run_id)}">
        <td><span class="bg-runs__when"><span class="date">${esc(t.date)}</span>${esc(t.time)}</span></td>
        <td><div class="bg-runs__player">${esc(r.player)}</div></td>
        <td><div class="bg-runs__market">${esc(r.market)}</div></td>
        <td class="ctr"><span class="bg-runs__outcome ${esc(r.outcome)}">${esc(r.outcome || "—")}</span></td>
        <td class="num"><div class="bg-runs__dur">${dur}</div></td>
        <td class="ctr"><div style="display:inline-flex;">${metersHtml(r.issues)}</div></td>
        <td class="ctr"><button class="bg-runs__caret" data-run-id="${esc(r.run_id)}">${isOpen ? "[–]" : "[+]"}</button></td>
      </tr>
      ${expandedHtml}
    `;
  }).join("");

  const tableInner = `
    <div class="bg-filterbar">
      ${filterChips}
      <span class="bg-filterbar__spacer"></span>
      <span class="bg-filterbar__count">${filtered.length} OF ${runs.length} RUNS</span>
    </div>
    <table class="bg-runs">
      <colgroup>
        <col class="when"><col class="player"><col class="market">
        <col class="outcome"><col class="dur"><col class="meters"><col class="actions">
      </colgroup>
      <thead>
        <tr>
          <th>WHEN</th><th>PLAYER</th><th>MARKET</th>
          <th class="ctr">OUTCOME</th><th class="num">DUR</th>
          <th class="ctr">W S S A S</th><th class="ctr"></th>
        </tr>
      </thead>
      <tbody>
        ${rowsHtml}
        ${filtered.length === 0
          ? `<tr><td colspan="7" style="text-align:center;padding:24px;color:var(--rule-gray);font-family:var(--font-mono-pixel);">— NO RUNS MATCH THIS FILTER —</td></tr>`
          : ""}
      </tbody>
    </table>
  `;

  root.innerHTML = windowCard("BOT_RUNS.LOG", "SORT: BY RECENCY", "[X]", tableInner);

  // Wire up event listeners on the new DOM
  root.querySelectorAll(".bg-chip[data-filter]").forEach(btn => {
    btn.addEventListener("click", () => {
      state.filter = btn.dataset.filter;
      renderRuns();
    });
  });
  root.querySelectorAll(".is-row[data-run-id]").forEach(tr => {
    tr.addEventListener("click", e => {
      // Don't toggle if a button or link inside the row was clicked
      if (e.target.closest("a, button.pa-btn")) return;
      const id = tr.dataset.runId;
      state.openRunId = state.openRunId === id ? null : id;
      renderRuns();
    });
  });
}

// ────────────────────────────────────────────────────────────
// Good Bets board + CLV scorecard
// ────────────────────────────────────────────────────────────
const BET_TYPE_META = {
  ev:  { label: "+EV",  color: "var(--augusta-green)" },
  arb: { label: "ARB",  color: "var(--ink-black)" },
  clv: { label: "CLV",  color: "#7a5cff" },
};

function renderGoodBets() {
  const root = document.getElementById("sec-good-bets");
  if (!root) return;

  const allBets = state.goodBets.bets || [];
  const bets = state.betType === "all"
    ? allBets
    : allBets.filter(b => b.opportunity_type === state.betType);

  // CLV scorecard KPI strip (driven by /api/clv-summary).
  const clv = state.clv || {};
  const beatRate = clv.beat_close_rate;
  const beatCls = beatRate == null ? "" : (beatRate >= 0.5 ? "is-pos" : "is-neg");
  const avgClvCls = clv.avg_clv_pct == null ? "" : (clv.avg_clv_pct >= 0 ? "is-pos" : "is-neg");
  const kpiStrip = `
    <div class="bg-hero__meta" style="margin:0 0 14px;">
      <div class="bg-hero__stat">
        <div class="bg-hero__stat-label">LIVE PLAYS</div>
        <div class="bg-hero__stat-val">${allBets.length}</div>
      </div>
      <div class="bg-hero__stat">
        <div class="bg-hero__stat-label">CLV · BEAT-CLOSE</div>
        <div class="bg-hero__stat-val ${beatCls}">${beatRate == null ? "—" : fmtFrac(beatRate)}</div>
      </div>
      <div class="bg-hero__stat">
        <div class="bg-hero__stat-label">AVG CLV</div>
        <div class="bg-hero__stat-val ${avgClvCls}">${clv.avg_clv_pct == null ? "—" : fmtPctVal(clv.avg_clv_pct, true)}</div>
      </div>
      <div class="bg-hero__stat">
        <div class="bg-hero__stat-label">CLV SAMPLE</div>
        <div class="bg-hero__stat-val">${clv.n_bets || 0}<span style="color:var(--rule-gray);font-size:18px;"> bets</span></div>
      </div>
    </div>
  `;

  const counts = {
    all: allBets.length,
    ev:  allBets.filter(b => b.opportunity_type === "ev").length,
    arb: allBets.filter(b => b.opportunity_type === "arb").length,
    clv: allBets.filter(b => b.opportunity_type === "clv").length,
  };
  const filterChips = [
    ["all", "ALL"], ["ev", "+EV"], ["arb", "ARB"], ["clv", "CLV"],
  ].map(([key, label]) =>
    `<button class="bg-chip${state.betType === key ? " is-active" : ""}" data-bettype="${esc(key)}">
      ${esc(label)} · ${counts[key]}
    </button>`
  ).join("");

  const rowsHtml = bets.map(b => {
    const tm = BET_TYPE_META[b.opportunity_type] || { label: (b.opportunity_type || "—").toUpperCase(), color: "var(--rule-gray)" };
    const game = (b.home_team_name && b.away_team_name)
      ? `${esc(b.away_team_name)} @ ${esc(b.home_team_name)}`
      : esc(b.sport_title || b.sport_key || "—");
    const sideLine = `${esc((b.side || "").toUpperCase())}${b.line != null ? " " + esc(b.line) : ""}`;
    const edge = b.edge_pct != null ? b.edge_pct : (b.roi != null ? b.roi * 100 : null);
    const edgeLbl = b.opportunity_type === "arb" ? "ROI" : "EDGE";
    const warn = b.disagreement_flag
      ? `<span title="Devig methods disagree — lower confidence" style="color:var(--ledger-red);margin-left:5px;">⚠</span>`
      : "";
    return `
      <tr>
        <td>
          <div class="bg-runs__player">${esc(b.player_name || b.market_key || "—")}${warn}</div>
          <div style="font-size:12px;color:var(--rule-gray);font-family:var(--font-mono-pixel);">${esc(b.market_key || "")} · ${sideLine}</div>
          <div style="font-size:12px;color:var(--rule-gray);">${game}</div>
        </td>
        <td><span style="font-weight:700;color:${tm.color};font-family:var(--font-mono-pixel);">${esc(tm.label)}</span></td>
        <td><div class="bg-runs__market">${esc((b.soft_book || "—").toUpperCase())}</div></td>
        <td class="num mono">${b.soft_price_decimal != null ? Number(b.soft_price_decimal).toFixed(2) : "—"}</td>
        <td class="num mono">${b.fair_prob != null ? fmtFrac(b.fair_prob) : "—"}</td>
        <td class="num mono"><span class="${edge != null && edge >= 0 ? "is-pos" : "is-neg"}">${fmtPctVal(edge, true)}</span><div style="font-size:10px;color:var(--rule-gray);">${edgeLbl}</div></td>
        <td class="num mono">${b.stake_capped != null ? fmtUsd(b.stake_capped) : (b.kelly_quarter != null ? fmtFrac(b.kelly_quarter) : "—")}</td>
        <td class="num mono">${fmtHours(b.hours_until_commence)}</td>
      </tr>
    `;
  }).join("");

  const tableInner = `
    ${kpiStrip}
    <div class="bg-filterbar">
      ${filterChips}
      <span class="bg-filterbar__spacer"></span>
      <span class="bg-filterbar__count">${bets.length} OF ${allBets.length} · UPDATED ${ageLabel(state.goodBets.updated_at)}</span>
    </div>
    <table class="bg-table">
      <thead>
        <tr>
          <th>PLAY</th><th>TYPE</th><th>BOOK</th>
          <th class="num">PRICE</th><th class="num">FAIR</th><th class="num">EDGE</th>
          <th class="num">¼-KELLY</th><th class="num">START</th>
        </tr>
      </thead>
      <tbody>
        ${rowsHtml}
        ${bets.length === 0
          ? `<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--rule-gray);font-family:var(--font-mono-pixel);">— NO QUALIFYING BETS. The analytics DAGs populate mart_good_bets each cycle. —</td></tr>`
          : ""}
      </tbody>
    </table>
  `;

  root.innerHTML = windowCard("GOOD_BETS.EXE", "DEVIG · +EV · CLV · RANKED", "[$]", tableInner);

  root.querySelectorAll(".bg-chip[data-bettype]").forEach(btn => {
    btn.addEventListener("click", () => {
      state.betType = btn.dataset.bettype;
      renderGoodBets();
    });
  });
}

// ────────────────────────────────────────────────────────────
// Ticker
// ────────────────────────────────────────────────────────────
function renderTicker() {
  const el = document.getElementById("ticker-content");
  if (!el) return;
  const items = [];
  items.push("MARKET OPEN");
  Object.values(state.accounts.books || {}).forEach(b => {
    items.push(`${(b.book || "—").toUpperCase()} BAL ${fmtUsd(b.balance)}  ·  7D ${fmtUsd(b.pnl_7d, true)}`);
  });
  const lastRun = state.runs[0];
  if (lastRun) {
    items.push(`LAST RUN · ${lastRun.player.toUpperCase()} · ${(lastRun.market || "").toUpperCase()} · ${(lastRun.outcome || "—").toUpperCase()} IN ${(lastRun.duration_s || 0).toFixed(1)}s`);
  }
  const offNominal = (state.watchers.watchers || []).filter(w => w.status !== "ok");
  if (offNominal.length === 0) {
    items.push("WATCHERS · ALL NOMINAL");
  } else {
    offNominal.forEach(w => {
      items.push(`WATCHER ${w.name.toUpperCase()} · ${(w.status || "—").toUpperCase()}` +
        (w.last_error ? ` · ${w.last_error.toUpperCase()}` : ""));
    });
  }
  const text = items.join("  ···  ") + "  ···  ";
  // Duplicate the text so the marquee loops cleanly (matches Ticker.jsx)
  el.textContent = text + text;
}

// ────────────────────────────────────────────────────────────
// Fetch helpers
// ────────────────────────────────────────────────────────────
async function refreshAccounts() {
  try {
    const r = await fetch("/api/account-stats?cb=" + Date.now());
    state.accounts = await r.json();
    renderAccounts();
    renderMast();
    renderTicker();
  } catch (e) { console.error("accounts fetch failed", e); }
}

async function refreshWatchers() {
  try {
    const r = await fetch("/api/watchers?cb=" + Date.now());
    state.watchers = await r.json();
    renderWatchers();
    renderMast();
    renderTicker();
  } catch (e) { console.error("watchers fetch failed", e); }
}

async function refreshRuns() {
  try {
    const r = await fetch("/api/runs?limit=100&cb=" + Date.now());
    state.runs = (await r.json()).runs || [];
    renderRuns();
    renderMast();
    renderTicker();
    renderHero();   // run-day dots on chart depend on runs
  } catch (e) { console.error("runs fetch failed", e); }
}

async function refreshChart() {
  try {
    const r = await fetch("/api/pnl-history?days=30&cb=" + Date.now());
    state.pnl = await r.json();
    renderHero();
  } catch (e) { console.error("pnl-history fetch failed", e); }
}

async function refreshGoodBets() {
  try {
    const r = await fetch("/api/good-bets?limit=100&cb=" + Date.now());
    state.goodBets = await r.json();
    renderGoodBets();
  } catch (e) { console.error("good-bets fetch failed", e); }
}

async function refreshClv() {
  try {
    const r = await fetch("/api/clv-summary?days=30&cb=" + Date.now());
    state.clv = await r.json();
    renderGoodBets();   // CLV scorecard lives in the good-bets card header
  } catch (e) { console.error("clv-summary fetch failed", e); }
}

async function refreshHealth() {
  try {
    const r = await fetch("/health?cb=" + Date.now());
    const data = await r.json();
    state.health.app = data.status === "ok" ? "OK" : (data.status || "—").toUpperCase();
    state.health.db  = data.db ? "OK" : "DOWN";
    renderMast();
  } catch (e) {
    state.health.app = "DOWN"; state.health.db = "—";
    renderMast();
  }
}

function refreshAll() {
  refreshHealth();
  refreshAccounts();
  refreshWatchers();
  refreshRuns();
  refreshChart();
  refreshGoodBets();
  refreshClv();
}

// ────────────────────────────────────────────────────────────
// Boot
// ────────────────────────────────────────────────────────────
renderSidebarWikiList();
renderMast();        // paint date immediately so the page isn't blank
refreshAll();
window.addEventListener("focus", refreshAll);
setInterval(refreshAll, 30000);

})();
