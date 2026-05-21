/* BountyGate Internal Dashboard — Pixel Augusta theme.
   Single-file React app. Mock data sourced from window.BG_DATA + window.BG_MOCK. */

const ISSUE_AXES = [
  { key: "wasted_wait",   short: "wasted",   label: "Wasted" },
  { key: "selector_miss", short: "selector", label: "Selector" },
  { key: "slip_state",    short: "slip",     label: "Slip" },
  { key: "auth_geo",      short: "auth",     label: "Auth / Geo" },
  { key: "stealth",       short: "stealth",  label: "Stealth" },
];

const fmtUsd = (n, signed = false) => {
  if (n == null || isNaN(n)) return "—";
  const sign = n < 0 ? "\u2212" : (signed ? "+" : "");
  return sign + "$" + Math.abs(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};
const fmtSecs = s => s == null ? "—" : (s < 60 ? Math.round(s) + "s" : Math.round(s/60) + "m");
const ageLabel = (iso, nowIso) => {
  if (!iso) return "—";
  const sec = Math.max(0, (new Date(nowIso || Date.now()) - new Date(iso))/1000);
  if (sec < 60) return Math.round(sec) + "s ago";
  if (sec < 3600) return Math.round(sec/60) + "m ago";
  if (sec < 86400) return (sec/3600).toFixed(1) + "h ago";
  return (sec/86400).toFixed(1) + "d ago";
};
const fmtTime = iso => {
  const d = new Date(iso);
  return {
    date: d.toLocaleDateString("en-US", { month: "short", day: "2-digit" }).toUpperCase(),
    time: d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false }),
  };
};
const issueCount = v => Array.isArray(v) ? v.length : (v && v !== "none" ? 1 : 0);

/* ─────────────────────────────────────────────────────────── */
/* Sidebar                                                     */
/* ─────────────────────────────────────────────────────────── */
function Sidebar({ active, onSelect, pages }) {
  return (
    <aside className="bg-side">
      <div className="bg-side__brand">
        <div className="bg-side__brand-mast">Bounty<i style={{ fontStyle: "italic", color: "var(--augusta-green)" }}>Gate</i></div>
        <div className="bg-side__brand-est">EST. MMXXVI · INTERNAL</div>
      </div>
      <h6>The Wiki</h6>
      <ul className="bg-side__list">
        {pages.map(p => (
          <a key={p.slug}
             className={"bg-side__item" + (active === p.slug ? " is-active" : "")}
             onClick={e => { e.preventDefault(); onSelect(p.slug); }}
             href={"#/" + p.slug}>
            <span className="bg-side__item-glyph">{p.glyph}</span>
            <span>{p.title}</span>
            <span className="bg-side__item-meta">{p.updated}</span>
          </a>
        ))}
      </ul>
      <hr className="bg-side__divider" />
      <h6>Health</h6>
      <ul className="bg-side__list">
        <a className="bg-side__item" href="#"><span className="bg-side__item-glyph">·</span><span>/health</span><span className="bg-side__item-meta">OK</span></a>
        <a className="bg-side__item" href="#"><span className="bg-side__item-glyph">·</span><span>Postgres</span><span className="bg-side__item-meta">OK</span></a>
        <a className="bg-side__item" href="#"><span className="bg-side__item-glyph">·</span><span>Heroku build</span><span className="bg-side__item-meta">#412</span></a>
      </ul>
      <hr className="bg-side__divider" />
      <h6>Officers</h6>
      <ul className="bg-side__list">
        <a className="bg-side__item" href="#"><span className="bg-side__item-glyph">§</span><span>T. Merkel</span><span className="bg-side__item-meta">CHM</span></a>
        <a className="bg-side__item" href="#"><span className="bg-side__item-glyph">§</span><span>Claude.exe</span><span className="bg-side__item-meta">REV</span></a>
      </ul>
    </aside>
  );
}

/* ─────────────────────────────────────────────────────────── */
/* Mast (WSJ-style header)                                     */
/* ─────────────────────────────────────────────────────────── */
function Mast({ runs, watchers, accounts }) {
  const now = new Date();
  const dateStr = now.toLocaleDateString("en-US", {
    weekday: "long", month: "long", day: "numeric", year: "numeric"
  }).toUpperCase();
  const allOk = (watchers?.watchers || []).every(w => w.status === "ok");
  const issues = (watchers?.watchers || []).filter(w => w.status !== "ok").length;
  const lastRun = runs[0];
  return (
    <header>
      <div className="bg-mast">
        <div className="bg-mast__col">
          <div>VOL. <span className="v">MMXXVI</span> · NO. <span className="v">{String(runs.length).padStart(3,"0")}</span></div>
          <div>MEMBER <span className="v">T-MERKEL</span></div>
          <div>FILED FROM <span className="v">REDMOND, WA</span></div>
        </div>
        <h1 className="bg-mast__title">The Daily Hedge</h1>
        <div className="bg-mast__col right">
          <div>{dateStr}</div>
          <div>PAIRED · HEDGED · BOOKED</div>
          <div>PRICE <span className="v">FREE TO PATRONS</span></div>
        </div>
      </div>
      <div className="bg-mast__strip">
        <span><span className="live-dot"></span>{allOk ? "ALL WATCHERS NOMINAL" : issues + " WATCHER" + (issues === 1 ? "" : "S") + " OFF-NOMINAL"}</span>
        <span className="center">LAST RUN · {lastRun ? lastRun.player.toUpperCase() : "—"} · {lastRun ? lastRun.outcome.toUpperCase() : "—"}</span>
        <span>EXPOSURE · {fmtUsd(Object.values(accounts.books).reduce((s,b) => s + (b.balance||0), 0))}</span>
      </div>
    </header>
  );
}

/* ─────────────────────────────────────────────────────────── */
/* Daily profit line chart hero                                */
/* ─────────────────────────────────────────────────────────── */
//
// 30-day daily P&L bars + cumulative line, WSJ-financial-chart style
// drawn in SVG on newsprint. Hover a day for the read-out; bot-run dates
// get a subtle marker so you can correlate runs ↔ profit.
function ProfitChart({ data, runs, activeRun, onPinClick }) {
  const days = data.days;
  const [hoverIdx, setHoverIdx] = useState(days.length - 1);

  // Cumulative series
  const cum = useMemo(() => {
    let s = 0;
    return days.map(d => (s += d.pnl, +s.toFixed(2)));
  }, [days]);

  // Map dates → list of runs that landed that day (for run dots on x-axis)
  const runsByDay = useMemo(() => {
    const m = {};
    (runs || []).forEach(r => {
      const key = r.timestamp.slice(0, 10);
      (m[key] = m[key] || []).push(r);
    });
    return m;
  }, [runs]);

  // Chart geometry — drawn in 100×100 viewBox-ish units mapped to a wider SVG
  const W = 880, H = 300;
  const M = { t: 18, r: 18, b: 42, l: 56 };
  const innerW = W - M.l - M.r;
  const innerH = H - M.t - M.b;
  const n = days.length;
  const xStep = innerW / (n - 1);
  const x = i => M.l + i * xStep;

  // Y range covers BOTH bars (daily) and line (cumulative). Add headroom.
  const yMaxRaw = Math.max(Math.max(...days.map(d => d.pnl), 0), Math.max(...cum, 0));
  const yMinRaw = Math.min(Math.min(...days.map(d => d.pnl), 0), Math.min(...cum, 0));
  const pad = (yMaxRaw - yMinRaw) * 0.12 || 10;
  const yMax = Math.ceil((yMaxRaw + pad) / 10) * 10;
  const yMin = Math.floor((yMinRaw - pad) / 10) * 10;
  const y = v => M.t + (1 - (v - yMin) / (yMax - yMin)) * innerH;
  const y0 = y(0);

  // Y axis ticks at $10 steps (or wider if range is big)
  const yStep = (yMax - yMin) > 200 ? 50 : (yMax - yMin) > 80 ? 20 : 10;
  const yTicks = [];
  for (let v = Math.ceil(yMin / yStep) * yStep; v <= yMax; v += yStep) yTicks.push(v);

  // X axis ticks: every Sunday (≈ every 7 days), plus first + last
  const xTickIdxs = [];
  days.forEach((d, i) => {
    if (i === 0 || i === n - 1) { xTickIdxs.push(i); return; }
    if (new Date(d.date + "T00:00:00").getUTCDay() === 0) xTickIdxs.push(i);
  });

  // Smooth-ish cumulative line via simple monotonic cubic? Polyline is fine.
  const linePts = cum.map((v, i) => `${x(i)},${y(v)}`).join(" ");
  const areaPts = `${x(0)},${y0} ${linePts} ${x(n-1)},${y0}`;

  // Stats below the chart
  const total = +cum[cum.length - 1].toFixed(2);
  const last7 = days.slice(-7).reduce((s, d) => s + d.pnl, 0);
  const winDays = days.filter(d => d.pnl > 0).length;
  const lossDays = days.filter(d => d.pnl < 0).length;
  const bestDay = days.reduce((a, b) => (a == null || b.pnl > a.pnl) ? b : a, null);
  const worstDay = days.reduce((a, b) => (a == null || b.pnl < a.pnl) ? b : a, null);

  // Annotation markers (labelled days)
  const annotations = days.map((d, i) => d.note ? { i, d } : null).filter(Boolean);

  const hov = hoverIdx != null && hoverIdx >= 0 ? days[hoverIdx] : null;
  const hovCum = hoverIdx != null && hoverIdx >= 0 ? cum[hoverIdx] : null;
  const hovRuns = hov ? (runsByDay[hov.date] || []) : [];
  const onMove = e => {
    const rect = e.currentTarget.getBoundingClientRect();
    const px = (e.clientX - rect.left) * (W / rect.width);
    const i = Math.round((px - M.l) / xStep);
    if (i >= 0 && i < n) setHoverIdx(i);
  };

  const fmtDateShort = iso => {
    const d = new Date(iso + "T12:00:00");
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" }).toUpperCase();
  };
  const fmtDateLong = iso => {
    const d = new Date(iso + "T12:00:00");
    return d.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" }).toUpperCase();
  };

  return (
    <section className="bg-hero">
      <WindowCard title="PROFIT_LINE.EXE" subtitle="30-DAY DAILY P&L · CUMULATIVE OVERLAY" glyph="[–]">

        <div className="bg-chart">
          <div className="bg-chart__hd">
            <div className="bg-chart__hd-left">
              <div className="bg-chart__kicker">FILED · 2026-05-18 · NET PROFIT, DAILY</div>
              <div className="bg-chart__hed">The House Account, Day by Day</div>
              <div className="bg-chart__dek">Cumulative ledger across FanDuel, BetMGM, DraftKings &amp; Caesars · paired arbitrage wagers only · settled USD</div>
            </div>
            <div className="bg-chart__hd-right">
              <div className="bg-chart__kpi">
                <div className="bg-chart__kpi-lbl">30D NET</div>
                <div className={"bg-chart__kpi-val " + (total >= 0 ? "is-pos" : "is-neg")}>
                  {total >= 0 ? "+" : "\u2212"}${Math.abs(total).toFixed(2)}
                </div>
              </div>
              <div className="bg-chart__kpi">
                <div className="bg-chart__kpi-lbl">LAST 7D</div>
                <div className={"bg-chart__kpi-val " + (last7 >= 0 ? "is-pos" : "is-neg")}>
                  {last7 >= 0 ? "+" : "\u2212"}${Math.abs(last7).toFixed(2)}
                </div>
              </div>
            </div>
          </div>

          <svg className="bg-chart__svg" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none"
               onMouseMove={onMove} onMouseLeave={() => setHoverIdx(days.length - 1)}>
            {/* Frame */}
            <rect x={M.l} y={M.t} width={innerW} height={innerH} fill="#FFFFFF" stroke="var(--ink-black)" />

            {/* Horizontal gridlines */}
            {yTicks.map(v => (
              <g key={v}>
                <line x1={M.l} x2={M.l + innerW} y1={y(v)} y2={y(v)}
                      stroke={v === 0 ? "var(--ink-black)" : "rgba(0,0,0,0.10)"}
                      strokeWidth={v === 0 ? 1 : 0.5}
                      strokeDasharray={v === 0 ? "" : "2 3"} />
                <text x={M.l - 8} y={y(v) + 4} textAnchor="end"
                      className="bg-chart__yt"
                      fill="var(--rule-gray)">
                  {(v >= 0 ? "" : "\u2212") + "$" + Math.abs(v)}
                </text>
              </g>
            ))}

            {/* Cumulative area + line */}
            <polygon points={areaPts} fill="rgba(11,91,64,0.10)" />
            <polyline points={linePts} fill="none" stroke="var(--augusta-green)"
                      strokeWidth={2.4} strokeLinejoin="round" strokeLinecap="round" />

            {/* Daily bars */}
            {days.map((d, i) => {
              const bw = Math.max(4, xStep - 4);
              const yTop = Math.min(y(d.pnl), y0);
              const yBot = Math.max(y(d.pnl), y0);
              const cls = d.pnl >= 0 ? "is-pos" : "is-neg";
              return (
                <rect key={i}
                      className={"bg-chart__bar " + cls}
                      x={x(i) - bw/2} y={yTop} width={bw} height={Math.max(1, yBot - yTop)} />
              );
            })}

            {/* Run-day dots beneath the bars */}
            {days.map((d, i) => {
              const has = (runsByDay[d.date] || []).length > 0;
              if (!has) return null;
              return (
                <circle key={"r"+i} cx={x(i)} cy={H - M.b + 14} r={2.5}
                        fill="var(--ink-black)" />
              );
            })}

            {/* Cumulative line dots over real bot-run days */}
            {cum.map((v, i) => (
              <circle key={"cd"+i} cx={x(i)} cy={y(v)} r={i === hoverIdx ? 5 : 2.2}
                      fill="var(--augusta-green)" stroke="#fff" strokeWidth={i === hoverIdx ? 2 : 1} />
            ))}

            {/* Annotations: dashed leaders */}
            {annotations.map(({ i, d }, k) => {
              const tx = x(i);
              const ty = Math.min(y(cum[i]), y(d.pnl)) - 18;
              const tyLbl = M.t + 4 + (k % 2) * 14;
              return (
                <g key={"a"+i} className="bg-chart__anno">
                  <line x1={tx} y1={Math.min(y(cum[i]), y(d.pnl))} x2={tx} y2={tyLbl + 4}
                        stroke="var(--ink-black)" strokeWidth={0.6} strokeDasharray="2 2" />
                  <text x={tx + 4} y={tyLbl + 11} className="bg-chart__anno-t" fill="var(--ink-black)">
                    {d.note}
                  </text>
                </g>
              );
            })}

            {/* X-axis ticks + labels */}
            {xTickIdxs.map(i => (
              <g key={"x"+i}>
                <line x1={x(i)} x2={x(i)} y1={M.t + innerH} y2={M.t + innerH + 4}
                      stroke="var(--ink-black)" strokeWidth={0.8} />
                <text x={x(i)} y={M.t + innerH + 17} textAnchor="middle"
                      className="bg-chart__xt" fill="var(--rule-gray)">
                  {fmtDateShort(days[i].date)}
                </text>
              </g>
            ))}

            {/* Hover crosshair */}
            {hoverIdx != null && (
              <g pointerEvents="none">
                <line x1={x(hoverIdx)} x2={x(hoverIdx)} y1={M.t} y2={M.t + innerH}
                      stroke="var(--ink-black)" strokeWidth={0.8} strokeDasharray="3 3" opacity={0.5}/>
              </g>
            )}
          </svg>

          {/* Readout strip — selected day */}
          {hov && (
            <div className="bg-chart__readout">
              <div className="bg-chart__ro-cell">
                <span className="bg-chart__ro-lbl">DATE</span>
                <span className="bg-chart__ro-val">{fmtDateLong(hov.date)}</span>
              </div>
              <div className="bg-chart__ro-cell">
                <span className="bg-chart__ro-lbl">DAILY</span>
                <span className={"bg-chart__ro-val " + (hov.pnl >= 0 ? "is-pos" : "is-neg")}>
                  {(hov.pnl >= 0 ? "+" : "\u2212") + "$" + Math.abs(hov.pnl).toFixed(2)}
                </span>
              </div>
              <div className="bg-chart__ro-cell">
                <span className="bg-chart__ro-lbl">CUMULATIVE</span>
                <span className={"bg-chart__ro-val " + (hovCum >= 0 ? "is-pos" : "is-neg")}>
                  {(hovCum >= 0 ? "+" : "\u2212") + "$" + Math.abs(hovCum).toFixed(2)}
                </span>
              </div>
              <div className="bg-chart__ro-cell wide">
                <span className="bg-chart__ro-lbl">RUNS</span>
                <span className="bg-chart__ro-val small">
                  {hovRuns.length === 0
                    ? <em>— no runs filed —</em>
                    : hovRuns.map((r, i) => (
                        <button key={r.run_id}
                                className={"bg-chart__ro-run " + r.outcome + (activeRun === r.run_id ? " is-active" : "")}
                                onClick={() => onPinClick(r.run_id)}>
                          {r.player.split(" ").slice(-1)[0]} · {r.outcome[0].toUpperCase()}
                        </button>
                      ))}
                </span>
              </div>
              {hov.note && (
                <div className="bg-chart__ro-cell wide">
                  <span className="bg-chart__ro-lbl">EDITORIAL NOTE</span>
                  <span className="bg-chart__ro-val small">{hov.note}</span>
                </div>
              )}
            </div>
          )}

          {/* Legend */}
          <div className="bg-chart__legend">
            <span><i className="sw line"></i>CUMULATIVE P&amp;L</span>
            <span><i className="sw bar-pos"></i>DAILY · WIN</span>
            <span><i className="sw bar-neg"></i>DAILY · LOSS</span>
            <span><i className="sw dot"></i>RUN FILED</span>
          </div>
        </div>

      </WindowCard>

      <div className="bg-hero__meta">
        <div className="bg-hero__stat">
          <div className="bg-hero__stat-label">30D NET</div>
          <div className={"bg-hero__stat-val " + (total >= 0 ? "is-pos" : "is-neg")}>
            {(total >= 0 ? "+" : "\u2212") + "$" + Math.abs(total).toFixed(2)}
          </div>
        </div>
        <div className="bg-hero__stat">
          <div className="bg-hero__stat-label">WIN DAYS</div>
          <div className="bg-hero__stat-val is-pos">{winDays} <span style={{ color: "var(--rule-gray)", fontSize: 18 }}>/ {days.length}</span></div>
        </div>
        <div className="bg-hero__stat">
          <div className="bg-hero__stat-label">BEST · WORST</div>
          <div className="bg-hero__stat-val">
            <span className="is-pos">+${bestDay.pnl.toFixed(2)}</span>
            <span style={{ color: "var(--rule-gray)" }}> · </span>
            <span className="is-neg">{"\u2212$" + Math.abs(worstDay.pnl).toFixed(2)}</span>
          </div>
        </div>
        <div className="bg-hero__stat">
          <div className="bg-hero__stat-label">LOSS DAYS</div>
          <div className="bg-hero__stat-val is-neg">{lossDays}</div>
        </div>
      </div>
    </section>
  );
}

/* ─────────────────────────────────────────────────────────── */
/* Accounts                                                    */
/* ─────────────────────────────────────────────────────────── */
function Accounts({ data }) {
  const books = Object.values(data.books);
  return (
    <WindowCard title="ACCOUNT_LEDGER.EXE" subtitle={"UPDATED " + ageLabel(data.updated_at)} glyph="[?]">
      <div className="bg-books">
        {books.map(b => (
          <div className="bg-book" key={b.book}>
            <div className="bg-book__row1">
              <span className="bg-book__name">{b.book.toUpperCase()}</span>
              <span className={"bg-book__status " + (b.scrape_status === "ok" ? "ok" : b.scrape_status === "amber" ? "amber" : "red")}>
                {b.scrape_status === "ok" ? "● OK" : b.scrape_status === "amber" ? "▲ STALE" : "✕ DOWN"}
              </span>
            </div>
            <div className="bg-book__balance">{fmtUsd(b.balance)}</div>
            <div className="bg-book__sub">
              avail {fmtUsd(b.available_liquidity)} · pending {fmtUsd(b.pending_wagers)}
            </div>
            <div className={"bg-book__pnl " + (b.pnl_7d >= 0 ? "pos" : "neg")}>
              7D P&L · {fmtUsd(b.pnl_7d, true)}
            </div>
            <div className="bg-book__spark" aria-hidden="true">
              {(b.spark || []).map((v, i) => {
                const mag = Math.min(1, Math.abs(v) / 14);
                return <div key={i} className={"b" + (v < 0 ? " neg" : "")} style={{ height: Math.max(2, mag * 22) + "px" }}></div>;
              })}
            </div>
            {b.last_error && <div className="bg-book__error">⚠ {b.last_error}</div>}
          </div>
        ))}
      </div>
    </WindowCard>
  );
}

/* ─────────────────────────────────────────────────────────── */
/* Watchers                                                    */
/* ─────────────────────────────────────────────────────────── */
function Watchers({ data }) {
  return (
    <WindowCard title="WATCHER_STATUS.EXE" subtitle={"CHECKED " + ageLabel(data.checked_at)} glyph="[?]">
      <table className="bg-table">
        <thead>
          <tr><th>WATCHER</th><th>TICK</th><th className="num">BACKLOG</th><th className="num">24H</th></tr>
        </thead>
        <tbody>
          {data.watchers.map(w => {
            const dotCls = !w.is_running ? "idle" : w.status === "ok" ? "ok" : w.status === "amber" ? "amber" : "red";
            return (
              <tr key={w.name}>
                <td>
                  <span className={"bg-dot " + dotCls}></span>
                  <span style={{ fontFamily: "var(--font-mono-pixel)", fontSize: 16, letterSpacing: ".04em" }}>{w.name}</span>
                  {!w.is_running && <span style={{ color: "var(--rule-gray)", fontFamily: "var(--font-mono-pixel)", marginLeft: 6, fontSize: 13 }}>· IDLE</span>}
                  {w.last_error && <div style={{ fontSize: 12, color: "var(--ledger-red)", fontFamily: "var(--font-mono-pixel)", marginTop: 2 }}>⚠ {w.last_error}</div>}
                </td>
                <td className="mono">{ageLabel(w.last_tick_at)}</td>
                <td className="num mono">
                  {w.pending_count}
                  {w.oldest_pending_age_s > 0 && <span style={{ color: "var(--rule-gray)" }}> · {fmtSecs(w.oldest_pending_age_s)}</span>}
                </td>
                <td className="num mono">
                  {w.completed_24h}
                  {w.errors_24h > 0 && <span style={{ color: "var(--ledger-red)" }}> / {w.errors_24h}e</span>}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </WindowCard>
  );
}

/* ─────────────────────────────────────────────────────────── */
/* Issue meters (5 stacked pixel bars)                         */
/* ─────────────────────────────────────────────────────────── */
function IssueMeters({ issues, max = 4 }) {
  return (
    <div className="bg-meters" title="Findings per axis: wasted · selector · slip · auth · stealth">
      {ISSUE_AXES.map(a => {
        const ct = issueCount(issues?.[a.key]);
        const segs = [];
        for (let i = 0; i < max; i++) segs.push(i < ct);
        return (
          <div className="bg-meter" key={a.key} data-key={a.short} title={`${a.label}: ${ct}`}>
            {segs.map((on, i) => <span key={i} className={"seg " + (on ? "on" : "")}></span>)}
          </div>
        );
      })}
    </div>
  );
}

/* ─────────────────────────────────────────────────────────── */
/* Runs section                                                */
/* ─────────────────────────────────────────────────────────── */
function Runs({ runs, openId, onOpen, filter, onFilter, sort }) {
  const filtered = useMemo(() => {
    let xs = filter === "all" ? runs : runs.filter(r => r.outcome === filter);
    if (sort === "wasted") {
      xs = [...xs].sort((a,b) => (b.estimated_wasted_wait_s || 0) - (a.estimated_wasted_wait_s || 0));
    } else if (sort === "outcome") {
      const ord = { failure: 0, skipped: 1, success: 2 };
      xs = [...xs].sort((a,b) => (ord[a.outcome] ?? 3) - (ord[b.outcome] ?? 3) || new Date(b.timestamp) - new Date(a.timestamp));
    } else {
      xs = [...xs].sort((a,b) => new Date(b.timestamp) - new Date(a.timestamp));
    }
    return xs;
  }, [runs, filter, sort]);

  const counts = useMemo(() => ({
    all:     runs.length,
    success: runs.filter(r => r.outcome === "success").length,
    failure: runs.filter(r => r.outcome === "failure").length,
    skipped: runs.filter(r => r.outcome === "skipped").length,
  }), [runs]);

  return (
    <WindowCard title="BOT_RUNS.LOG" subtitle={`SORT: BY ${sort.toUpperCase()}`} glyph="[X]">
      <div className="bg-filterbar">
        {[
          ["all",     "ALL",     "default"],
          ["success", "SUCCESS", "success"],
          ["failure", "FAILURE", "failure"],
          ["skipped", "SKIPPED", "skipped"],
        ].map(([key, label, cls]) => (
          <button key={key}
                  className={"bg-chip " + cls + (filter === key ? " is-active" : "")}
                  onClick={() => onFilter(key)}>
            {label} · {counts[key]}
          </button>
        ))}
        <span className="bg-filterbar__spacer"></span>
        <span className="bg-filterbar__count">{filtered.length} OF {runs.length} RUNS</span>
      </div>

      <table className="bg-runs">
        <colgroup>
          <col className="when" /><col className="player" /><col className="market" />
          <col className="outcome" /><col className="dur" /><col className="meters" /><col className="actions" />
        </colgroup>
        <thead>
          <tr>
            <th>WHEN</th><th>PLAYER</th><th>MARKET</th>
            <th className="ctr">OUTCOME</th><th className="num">DUR</th>
            <th className="ctr">W S S A S</th>
            <th className="ctr"></th>
          </tr>
        </thead>
        <tbody>
          {filtered.map(r => {
            const isOpen = openId === r.run_id;
            const t = fmtTime(r.timestamp);
            return (
              <React.Fragment key={r.run_id}>
                <tr className={"is-row" + (isOpen ? " is-open" : "")} onClick={() => onOpen(isOpen ? null : r.run_id)}>
                  <td>
                    <span className="bg-runs__when">
                      <span className="date">{t.date}</span>
                      {t.time}
                    </span>
                  </td>
                  <td><div className="bg-runs__player">{r.player}</div></td>
                  <td><div className="bg-runs__market">{r.market}</div></td>
                  <td className="ctr"><span className={"bg-runs__outcome " + r.outcome}>{r.outcome}</span></td>
                  <td className="num">
                    <div className="bg-runs__dur">
                      {r.duration_s != null ? r.duration_s.toFixed(1) + "s" : "—"}
                      {r.estimated_wasted_wait_s > 0 && <span className="wasted">−{r.estimated_wasted_wait_s}s waste</span>}
                    </div>
                  </td>
                  <td className="ctr"><div style={{ display: "inline-flex" }}><IssueMeters issues={r.issues} /></div></td>
                  <td className="ctr">
                    <button className="bg-runs__caret" onClick={e => { e.stopPropagation(); onOpen(isOpen ? null : r.run_id); }}>
                      {isOpen ? "[–]" : "[+]"}
                    </button>
                  </td>
                </tr>
                {isOpen && (
                  <tr>
                    <td colSpan={7} className="bg-runs__expand-cell">
                      <div className="bg-runs__expand">
                        <div className="bg-runs__top-finding">
                          <div className="lbl">EDITORIAL · TOP FINDING</div>
                          <div className="body">{r.top_finding}</div>
                          <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
                            <Button variant="primary">▶ OPEN RECORDING</Button>
                            <Button variant="default">VIEW REVIEW.MD</Button>
                            <Button variant="ghost">FILE TO ARCHIVE</Button>
                          </div>
                        </div>
                        <div className="bg-axes">
                          {ISSUE_AXES.map(a => {
                            const items = Array.isArray(r.issues?.[a.key]) ? r.issues[a.key] : [];
                            return (
                              <div key={a.key} className={"bg-axis" + (items.length === 0 ? " is-empty" : "")}>
                                <div className="bg-axis__hd">
                                  <span>{a.label.toUpperCase()}</span>
                                  <span className="ct">{items.length}</span>
                                </div>
                                <div className="bg-axis__body">
                                  {items.length === 0
                                    ? <div className="none">— NO FINDINGS —</div>
                                    : <ul>{items.map((it, i) => <li key={i}>{renderFinding(it)}</li>)}</ul>}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
          {filtered.length === 0 && (
            <tr><td colSpan={7} style={{ textAlign: "center", padding: 24, color: "var(--rule-gray)", fontFamily: "var(--font-mono-pixel)" }}>— NO RUNS MATCH THIS FILTER —</td></tr>
          )}
        </tbody>
      </table>
    </WindowCard>
  );
}

// Highlight leading "t=hh:mm" timestamps in finding strings.
function renderFinding(s) {
  const m = String(s).match(/^(t=\d\d:\d\d(?:\s*[→\-]\s*t=\d\d:\d\d)?)(.*)$/);
  if (m) return <><span className="t">{m[1]}</span>{m[2]}</>;
  return s;
}

Object.assign(window, { Sidebar, Mast, ProfitChart, Accounts, Watchers, Runs, IssueMeters, ISSUE_AXES });
