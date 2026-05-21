/* Wiki page · Bot execution flow.
   Mocks /wiki/bot-flow + /api/wiki/bot-flow.json into the SC3K + WSJ chrome.
   Mounted via the WikiPage switch in app.jsx when the active slug is bot-flow. */

const BOT_FLOW = {
  slug: "bot-flow",
  title: "Bot execution flow",
  updated_at: "2026-05-16T00:00:00Z",
  watches: [
    "arbitrage_executor/execute_arb.py",
    "arbitrage_executor/task_worker.py",
    "arbitrage_executor/opportunity.py",
    "arbitrage_executor/bet_placer.py",
  ],
  layers: [
    { id: "execution",    label: "Execution",       color: "var(--augusta-green)",  default: true  },
    { id: "decisions",    label: "Decisions",       color: "var(--masters-yellow)", default: true  },
    { id: "value_stream", label: "Value stream",    color: "var(--fg-positive)",    default: true  },
    { id: "failures",     label: "Recent failures", color: "var(--ledger-red)",     default: false },
  ],
  nodes: [
    { id: "queue_pick",          x:    0, y:  60, label: "Queue pick",                   layer: "execution", kind: "step"     },
    { id: "open_fd",             x:  160, y:  60, label: "Open FanDuel",                 layer: "execution", kind: "step"     },
    { id: "probe_decision",      x:  320, y:  60, label: "Probe limits\nROI > min?",     layer: "decisions", kind: "decision" },
    { id: "skip_audit",          x:  200, y: 220, label: "Skip + audit",                 layer: "execution", kind: "step"     },
    { id: "outcome_skipped",     x:   60, y: 220, label: "SKIPPED",                      layer: "execution", kind: "outcome", outcome: "skipped" },
    { id: "search_mgm",          x:  500, y:  60, label: "Search BetMGM",                layer: "execution", kind: "step"     },
    { id: "select_market_mgm",   x:  660, y:  60, label: "Select market tab",            layer: "execution", kind: "step"     },
    { id: "enter_wager_mgm",     x:  820, y:  60, label: "Enter wager",                  layer: "execution", kind: "step"     },
    { id: "place_mgm_decision",  x:  980, y:  60, label: "Place BetMGM\nconfirmed?",     layer: "decisions", kind: "decision" },
    { id: "halt_orphan",         x:  820, y: 220, label: "HALT + alert\norphaned bet",   layer: "failures",  kind: "alert"    },
    { id: "outcome_failure",     x:  980, y: 220, label: "FAILED",                       layer: "execution", kind: "outcome", outcome: "failure" },
    { id: "place_fd_hedge",      x: 1160, y:  60, label: "Place FD hedge",               layer: "execution", kind: "step"     },
    { id: "outcome_success",     x: 1320, y:  60, label: "COMPLETED",                    layer: "execution", kind: "outcome", outcome: "success" },
  ],
  edges: [
    { id: "e1",  s: "queue_pick",         t: "open_fd"                                              },
    { id: "e2",  s: "open_fd",            t: "probe_decision"                                       },
    { id: "e3",  s: "probe_decision",     t: "skip_audit",        label: "no",  layer: "decisions" },
    { id: "e4",  s: "probe_decision",     t: "search_mgm",        label: "yes", layer: "decisions" },
    { id: "e5",  s: "skip_audit",         t: "outcome_skipped"                                       },
    { id: "e6",  s: "search_mgm",         t: "select_market_mgm"                                     },
    { id: "e7",  s: "select_market_mgm",  t: "enter_wager_mgm"                                       },
    { id: "e8",  s: "enter_wager_mgm",    t: "place_mgm_decision"                                    },
    { id: "e9",  s: "place_mgm_decision", t: "halt_orphan",       label: "no",  layer: "decisions" },
    { id: "e10", s: "place_mgm_decision", t: "place_fd_hedge",    label: "yes", layer: "decisions" },
    { id: "e11", s: "halt_orphan",        t: "outcome_failure",   layer: "failures"                  },
    { id: "e12", s: "place_fd_hedge",     t: "outcome_success"                                       },
  ],
};

// Fabricated /api/wiki/bot-flow.json — 24h aggregates per node + per edge.
const BOT_FLOW_METRICS = {
  computed_at: "2026-05-18T08:45:00Z",
  node_metrics: {
    queue_pick:         { runs_24h: 47, avg_duration_s:  0.4 },
    open_fd:            { runs_24h: 47, avg_duration_s:  3.2 },
    probe_decision:     { runs_24h: 47, avg_duration_s:  8.5 },
    skip_audit:         { runs_24h: 12, avg_duration_s:  1.1 },
    outcome_skipped:    { runs_24h: 12, avg_duration_s:  0.0 },
    search_mgm:         { runs_24h: 35, avg_duration_s: 14.2 },
    select_market_mgm:  { runs_24h: 31, avg_duration_s:  9.1 },
    enter_wager_mgm:    { runs_24h: 22, avg_duration_s:  4.3 },
    place_mgm_decision: { runs_24h: 22, avg_duration_s:  6.8 },
    halt_orphan:        { runs_24h:  4, avg_duration_s:  0.0 },
    outcome_failure:    { runs_24h: 18, avg_duration_s:  0.0 },
    place_fd_hedge:     { runs_24h: 18, avg_duration_s: 11.6 },
    outcome_success:    { runs_24h: 17, avg_duration_s:  0.0 },
  },
  edge_metrics: {
    e3:  { runs_24h: 12, label: "ROI < min · skipped" },
    e4:  { runs_24h: 35, label: "ROI passes" },
    e9:  { runs_24h:  4, label: "MG rejected · orphan" },
    e10: { runs_24h: 18, label: "MG accepted" },
  },
};

const NODE_W = 132;
const NODE_H = 56;

function WikiBreadcrumb({ pages, activeSlug, onSelect }) {
  return (
    <nav className="wk-breadcrumb">
      <a href="#" onClick={e => { e.preventDefault(); onSelect("dashboard"); }}>← BountyGate</a>
      <span className="wk-breadcrumb__sep">·</span>
      <a href="#" onClick={e => { e.preventDefault(); onSelect("dashboard"); }}>The Daily Hedge</a>
      <span className="wk-breadcrumb__sep">·</span>
      <span>Wiki</span>
      <span className="wk-breadcrumb__sep">·</span>
      <span className="wk-breadcrumb__active">{pages.find(p => p.slug === activeSlug)?.title || activeSlug}</span>
    </nav>
  );
}

function WikiMast({ page }) {
  const updated = new Date(page.updated_at);
  const fmt = updated.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" }).toUpperCase();
  return (
    <header className="wk-mast">
      <div className="wk-mast__kicker">INTERNAL · ENGINEERING WIKI · {page.slug.toUpperCase()}</div>
      <h1 className="wk-mast__title">{page.title}</h1>
      <div className="wk-mast__meta">
        <span>UPDATED {fmt}</span>
        <span>·</span>
        <span>WATCHED BY {page.watches.length} SOURCE FILE{page.watches.length === 1 ? "" : "S"}</span>
        <span>·</span>
        <span>FILED BY <em>wiki-watcher</em></span>
      </div>
    </header>
  );
}

function WatchedFiles({ files, updatedAt }) {
  return (
    <WindowCard title="WATCHED_FILES.SYS" subtitle="REGENERATES ON COMMIT" glyph="[i]">
      <p className="wk-prose">This page subscribes to the source files below. On every commit, the post-commit hook touches <code>wiki/.pending/{"{slug}"}</code> if any of these changed; the wiki-watcher then re-runs <code>/wiki:sync bot-flow</code> and you review the regenerated diff.</p>
      <ul className="wk-watches">
        {files.map(f => (
          <li key={f}>
            <span className="wk-watches__glyph">¶</span>
            <code>{f}</code>
            <span className="wk-watches__age">touched ·  unchanged</span>
          </li>
        ))}
      </ul>
      <div className="wk-watches__foot">
        <span>HOOK · post-commit · idempotent</span>
        <span>NEXT SYNC · on next push</span>
      </div>
    </WindowCard>
  );
}

function SequencePixel() {
  // Four lanes: Queue, Worker, FanDuel, BetMGM. Messages stack top→bottom.
  const lanes = ["bot_execution_queue", "task_worker", "FanDuel", "BetMGM"];
  const rows = [
    { from: "bot_execution_queue", to: "task_worker", label: "PENDING task · FOR UPDATE SKIP LOCKED", kind: "msg" },
    { from: "task_worker",         to: "FanDuel",     label: "Phase 1 · probe limits",                 kind: "msg" },
    { from: "FanDuel",             to: "task_worker", label: "max_wager + ROI check",                  kind: "rsp" },
    { kind: "alt", label: "ROI passes & limits OK" },
    { from: "task_worker",         to: "BetMGM",      label: "Phase 2 · place wager",                  kind: "msg" },
    { from: "BetMGM",              to: "task_worker", label: "ACCEPTED",                               kind: "rsp" },
    { from: "task_worker",         to: "FanDuel",     label: "Phase 3 · place hedge",                  kind: "msg" },
    { from: "FanDuel",             to: "task_worker", label: "ACCEPTED",                               kind: "rsp" },
    { from: "task_worker",         to: "bot_execution_queue", label: "mark COMPLETED",                 kind: "msg" },
    { kind: "alt", label: "ROI fails",         color: "rule" },
    { from: "task_worker",         to: "bot_execution_queue", label: "skipped + audit",                kind: "msg" },
    { kind: "alt", label: "BetMGM rejects",    color: "fail" },
    { from: "task_worker",         to: "task_worker", label: "HALT + CRITICAL Discord alert · orphaned", kind: "alert" },
  ];
  const W = 880, H = 360, marg = { t: 40, r: 30, b: 20, l: 30 };
  const innerW = W - marg.l - marg.r;
  const laneX = i => marg.l + (i + 0.5) * (innerW / lanes.length);
  const laneIdx = name => lanes.indexOf(name);

  // Each non-alt row gets a fixed vertical step; alt rows get a band.
  const stepH = 24;
  const positions = [];
  let y = marg.t + 14;
  rows.forEach((r, i) => {
    positions[i] = y;
    y += r.kind === "alt" ? 22 : stepH;
  });
  const totalH = y + marg.b;

  return (
    <WindowCard title="SEQUENCE_DIAGRAM.EXE" subtitle="THREE-PHASE PIPELINE · execute_arb.ArbExecutor" glyph="[?]">
      <svg className="wk-seq" viewBox={`0 0 ${W} ${totalH}`} preserveAspectRatio="xMidYMid meet">
        {/* Lane headers */}
        {lanes.map((name, i) => (
          <g key={name}>
            <rect x={laneX(i) - 80} y={4} width={160} height={28}
                  fill="var(--ink-black)" stroke="var(--ink-black)" />
            <text x={laneX(i)} y={22} textAnchor="middle" className="wk-seq__lane">{name}</text>
          </g>
        ))}
        {/* Lane lines */}
        {lanes.map((name, i) => (
          <line key={"l"+name} x1={laneX(i)} x2={laneX(i)}
                y1={32} y2={totalH - 8}
                stroke="var(--ink-black)" strokeWidth="1" strokeDasharray="2 4" />
        ))}
        {/* Rows */}
        {rows.map((r, i) => {
          const ry = positions[i];
          if (r.kind === "alt") {
            const stroke = r.color === "fail" ? "var(--ledger-red)" : r.color === "rule" ? "var(--rule-gray)" : "var(--augusta-green)";
            return (
              <g key={"r"+i}>
                <line x1={marg.l} x2={W - marg.r} y1={ry + 6} y2={ry + 6}
                      stroke={stroke} strokeWidth="1" strokeDasharray="4 2" />
                <rect x={marg.l + 8} y={ry - 6} width={140} height={18}
                      fill="var(--newsprint-white)" stroke={stroke} />
                <text x={marg.l + 14} y={ry + 7} className="wk-seq__alt" fill={stroke}>
                  ALT · {r.label.toUpperCase()}
                </text>
              </g>
            );
          }
          const x1 = laneX(laneIdx(r.from));
          const x2 = laneX(laneIdx(r.to));
          const isSelf = x1 === x2;
          const stroke = r.kind === "alert" ? "var(--ledger-red)" : r.kind === "rsp" ? "var(--rule-gray)" : "var(--ink-black)";
          const dash = r.kind === "rsp" ? "3 3" : "";
          const labelX = isSelf ? x1 + 14 : (x1 + x2) / 2;
          const labelY = ry - 2;
          const arrowDir = x2 >= x1 ? 1 : -1;
          return (
            <g key={"m"+i}>
              {isSelf ? (
                <path d={`M ${x1},${ry + 6} h 36 v 10 h -36`}
                      fill="none" stroke={stroke} strokeWidth="1.2" />
              ) : (
                <line x1={x1} x2={x2 - arrowDir * 6} y1={ry + 6} y2={ry + 6}
                      stroke={stroke} strokeWidth="1.2" strokeDasharray={dash} />
              )}
              {/* Arrowhead */}
              {!isSelf && (
                <polygon
                  points={`${x2},${ry + 6} ${x2 - arrowDir * 6},${ry + 2} ${x2 - arrowDir * 6},${ry + 10}`}
                  fill={stroke}
                />
              )}
              <text x={labelX} y={labelY} textAnchor={isSelf ? "start" : "middle"} className="wk-seq__msg" fill={stroke}>
                {r.label}
              </text>
            </g>
          );
        })}
      </svg>
      <div className="wk-seq__legend">
        <span><i className="sw solid"></i>request</span>
        <span><i className="sw dashed"></i>response</span>
        <span><i className="sw alert"></i>alert / orphan</span>
      </div>
    </WindowCard>
  );
}

function DecisionGraph({ page, metrics }) {
  const [layerState, setLayerState] = useState(() => {
    const m = {};
    page.layers.forEach(l => { m[l.id] = l.default; });
    return m;
  });
  const [hoverId, setHoverId] = useState(null);

  // Resolve node positions to coords inside graph viewBox
  const W = 1480, H = 320;
  const X_OFF = 30, Y_OFF = 10;

  const nodeById = useMemo(() => {
    const m = {};
    page.nodes.forEach(n => { m[n.id] = { ...n, cx: n.x + X_OFF + NODE_W / 2, cy: n.y + Y_OFF + NODE_H / 2 }; });
    return m;
  }, [page]);

  // Edge routing: orthogonal step lines (right-angle).
  function edgePath(s, t) {
    const sn = nodeById[s], tn = nodeById[t];
    const sx = sn.cx + (tn.cx > sn.cx ? NODE_W / 2 : tn.cx < sn.cx ? -NODE_W / 2 : 0);
    const sy = sn.cy + (tn.cy > sn.cy && tn.cx === sn.cx ? NODE_H / 2 : tn.cy < sn.cy && tn.cx === sn.cx ? -NODE_H / 2 : 0);
    const tx = tn.cx + (tn.cx > sn.cx ? -NODE_W / 2 : tn.cx < sn.cx ? NODE_W / 2 : 0);
    const ty = tn.cy + (tn.cy > sn.cy && tn.cx === sn.cx ? -NODE_H / 2 : tn.cy < sn.cy && tn.cx === sn.cx ? NODE_H / 2 : 0);
    if (sn.cy === tn.cy) {
      return { d: `M ${sx} ${sy} L ${tx} ${ty}`, ax: tx, ay: ty };
    }
    // Step shape: out, then turn vertically, then in
    const midX = (sx + tx) / 2;
    return { d: `M ${sx} ${sy} L ${midX} ${sy} L ${midX} ${ty} L ${tx} ${ty}`, ax: tx, ay: ty };
  }

  const visibleNode = n => layerState[n.layer];
  const visibleEdge = e => {
    if (e.layer && !layerState[e.layer]) return false;
    const sn = nodeById[e.s], tn = nodeById[e.t];
    return visibleNode(sn) && visibleNode(tn);
  };

  return (
    <WindowCard title="DECISION_GRAPH.EXE" subtitle="LIVE · /api/wiki/bot-flow.json · 24h aggregates" glyph="[?]">
      <div className="wk-dg__legend">
        {page.layers.map(l => (
          <button key={l.id}
                  className={"wk-dg__lyr" + (layerState[l.id] ? " is-on" : "")}
                  onClick={() => setLayerState(s => ({ ...s, [l.id]: !s[l.id] }))}>
            <i className="sw" style={{ background: l.color }}></i>
            <span>{l.label.toUpperCase()}</span>
            <em>{layerState[l.id] ? "ON" : "OFF"}</em>
          </button>
        ))}
        <span className="wk-dg__legend-fill"></span>
        <span className="wk-dg__updated">
          COMPUTED {new Date(metrics.computed_at).toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" })}
        </span>
      </div>

      <div className="wk-dg__scroll">
        <svg className="wk-dg__svg" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="xMidYMid meet">
          {/* Defs: arrowhead */}
          <defs>
            <marker id="arrow" markerWidth="10" markerHeight="10" refX="7" refY="3" orient="auto" markerUnits="strokeWidth">
              <path d="M 0 0 L 7 3 L 0 6 z" fill="var(--ink-black)" />
            </marker>
            <marker id="arrow-red" markerWidth="10" markerHeight="10" refX="7" refY="3" orient="auto" markerUnits="strokeWidth">
              <path d="M 0 0 L 7 3 L 0 6 z" fill="var(--ledger-red)" />
            </marker>
            <marker id="arrow-yellow" markerWidth="10" markerHeight="10" refX="7" refY="3" orient="auto" markerUnits="strokeWidth">
              <path d="M 0 0 L 7 3 L 0 6 z" fill="var(--ink-black)" />
            </marker>
          </defs>

          {/* Edges first so nodes paint over their endpoints */}
          {page.edges.map(e => {
            if (!visibleEdge(e)) return null;
            const isDec = e.label === "yes" || e.label === "no";
            const isFail = e.layer === "failures";
            const path = edgePath(e.s, e.t);
            const stroke = isFail ? "var(--ledger-red)" : "var(--ink-black)";
            const mid = e.label && path.d;
            // Place label near source
            const sn = nodeById[e.s];
            const labelX = sn.cx + (isDec && e.label === "no" ? -10 : isDec && e.label === "yes" ? 90 : 0);
            const labelY = sn.cy + (e.layer === "failures" ? 56 : 56);
            const em = metrics.edge_metrics[e.id];
            return (
              <g key={e.id} className="wk-dg__edge">
                <path d={path.d} fill="none" stroke={stroke}
                      strokeWidth={isFail ? 1.8 : 1.4}
                      strokeDasharray={isFail ? "6 4" : ""}
                      markerEnd={isFail ? "url(#arrow-red)" : "url(#arrow)"} />
                {e.label && (
                  <g>
                    <rect x={labelX - 16} y={labelY - 11} width={32} height={16}
                          fill={e.label === "yes" ? "var(--masters-yellow)" : "var(--newsprint-white)"}
                          stroke="var(--ink-black)" />
                    <text x={labelX} y={labelY + 1} textAnchor="middle" className="wk-dg__edge-lbl">
                      {e.label.toUpperCase()}
                    </text>
                  </g>
                )}
                {em && (
                  <text x={(nodeById[e.s].cx + nodeById[e.t].cx) / 2}
                        y={(nodeById[e.s].cy + nodeById[e.t].cy) / 2 - 6}
                        textAnchor="middle" className="wk-dg__edge-ct" fill={stroke}>
                    {em.runs_24h} ·24h
                  </text>
                )}
              </g>
            );
          })}

          {/* Nodes */}
          {page.nodes.map(n => {
            if (!visibleNode(n)) return null;
            const m = metrics.node_metrics[n.id] || {};
            const x = n.x + X_OFF, y = n.y + Y_OFF;
            const cx = x + NODE_W / 2;
            const isHov = hoverId === n.id;
            // Short display ID so the title bar fits inside NODE_W.
            const shortId = n.id
              .replace(/_decision$/, "")
              .replace(/_mgm$/, "")
              .replace(/^outcome_/, "")
              .toUpperCase();
            return (
              <g key={n.id} className={"wk-dg__node wk-dg__node--" + n.kind + (isHov ? " is-hover" : "")}
                 onMouseEnter={() => setHoverId(n.id)} onMouseLeave={() => setHoverId(null)}>
                {/* Drop shadow / bevel */}
                <rect x={x + 2} y={y + 2} width={NODE_W} height={NODE_H}
                      fill="rgba(0,0,0,0.18)" />
                <rect x={x} y={y} width={NODE_W} height={NODE_H}
                      className="wk-dg__node-shell"
                      stroke="var(--ink-black)" strokeWidth="1" />
                {/* Title bar */}
                <rect x={x} y={y} width={NODE_W} height={16}
                      className="wk-dg__node-bar"
                      stroke="var(--ink-black)" strokeWidth="0.5" />
                <text x={x + 6} y={y + 12} className="wk-dg__node-title">{shortId}</text>
                <text x={x + NODE_W - 6} y={y + 12} textAnchor="end" className="wk-dg__node-glyph">
                  {n.kind === "decision" ? "[?]" : n.kind === "alert" ? "[!]" : n.kind === "outcome" ? "[→]" : "[·]"}
                </text>
                {/* Label */}
                {n.label.split("\n").map((line, i) => (
                  <text key={i} x={cx} y={y + 32 + i * 12} textAnchor="middle" className="wk-dg__node-lbl">
                    {line}
                  </text>
                ))}
                {/* 24h metrics */}
                <text x={cx} y={y + NODE_H - 4} textAnchor="middle" className="wk-dg__node-mt">
                  {m.runs_24h != null ? `${m.runs_24h} ·24h` : "—"}
                  {m.avg_duration_s ? `  ${m.avg_duration_s.toFixed(1)}s` : ""}
                </text>
              </g>
            );
          })}

          {/* Phase brackets above */}
          <PhaseBracket x1={nodeById.queue_pick.cx} x2={nodeById.open_fd.cx + NODE_W / 2} y={6} label="QUEUE" />
          <PhaseBracket x1={nodeById.open_fd.cx - NODE_W / 2} x2={nodeById.probe_decision.cx + NODE_W / 2} y={6} label="PHASE 1 · PROBE" />
          <PhaseBracket x1={nodeById.search_mgm.cx - NODE_W / 2} x2={nodeById.place_mgm_decision.cx + NODE_W / 2} y={6} label="PHASE 2 · BETMGM" />
          <PhaseBracket x1={nodeById.place_fd_hedge.cx - NODE_W / 2} x2={nodeById.outcome_success.cx + NODE_W / 2} y={6} label="PHASE 3 · HEDGE" />
        </svg>
      </div>
    </WindowCard>
  );
}

function PhaseBracket({ x1, x2, y, label }) {
  const midX = (x1 + x2) / 2;
  return (
    <g>
      <line x1={x1} x2={x2} y1={y} y2={y} stroke="var(--ink-black)" strokeWidth="0.8" />
      <line x1={x1} x2={x1} y1={y} y2={y + 4} stroke="var(--ink-black)" strokeWidth="0.8" />
      <line x1={x2} x2={x2} y1={y} y2={y + 4} stroke="var(--ink-black)" strokeWidth="0.8" />
      <rect x={midX - label.length * 3.6 - 4} y={y - 9} width={label.length * 7.2 + 8} height={14}
            fill="var(--newsprint-white)" stroke="var(--ink-black)" />
      <text x={midX} y={y + 2} textAnchor="middle" className="wk-dg__phase">{label}</text>
    </g>
  );
}

function RecurringIssues() {
  const rows = [
    { axis: "auth_geo",      cls: "auth",     pattern: "BetMGM credential modal triggers mid-Phase-2; session warmth keepalive needed.",                                    flagged: 4 },
    { axis: "wasted_wait",   cls: "wasted",   pattern: "BetMGM search overlay freezes (5–67s); Chrome audio-device modal at startup (~6s).",                                 flagged: 7 },
    { axis: "selector_miss", cls: "selector", pattern: "BetMGM market sub-tab routing fails when player metadata differs from canonical mapping.",                           flagged: 6 },
    { axis: "slip_state",    cls: "slip",     pattern: "Leftover bets occasionally seen at slip open; suspended-market banners flagged.",                                    flagged: 2 },
    { axis: "stealth",       cls: "stealth",  pattern: "Sub-dollar wagers (ROI ≤ 0.4%) reaching execution; same-player same-market within ~30s = canonical correlation signal.", flagged: 9 },
  ];
  return (
    <WindowCard title="RECURRING_ISSUES.LOG" subtitle="OBSERVED BY review-watcher · last 30 reviews" glyph="[X]">
      <table className="wk-issues">
        <thead>
          <tr><th>AXIS</th><th>PATTERN</th><th className="num">FLAGGED</th></tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.axis}>
              <td><span className={"wk-tag wk-tag--" + r.cls}>{r.axis}</span></td>
              <td>{r.pattern}</td>
              <td className="num">{r.flagged}× <span className="wk-issues__small">in 30</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </WindowCard>
  );
}

function PhaseNote() {
  return (
    <aside className="wk-callout">
      <div className="wk-callout__tag">v1 NOTE · EDITOR</div>
      <div className="wk-callout__body">
        This view collapses each phase into one node per step. The Phase D <code>/wiki:sync</code> skill will regenerate it with finer-grained UI-interaction sub-nodes (navigate, wait-for-element, dismiss-modal, etc.) once that pipeline is live; phase headers will become collapsible super-nodes to keep the dense view readable.
      </div>
    </aside>
  );
}

function WikiBotFlowPage({ pages, activeSlug, onSelect }) {
  return (
    <article className="wk-page">
      <WikiBreadcrumb pages={pages} activeSlug={activeSlug} onSelect={onSelect} />
      <WikiMast page={BOT_FLOW} />

      <p className="wk-lede dropcap">
        The bot processes one task at a time from <code>bot_execution_queue</code>. Each task runs the three-phase pipeline in <code>execute_arb.ArbExecutor</code>: probe FanDuel for limits, place a wager on BetMGM, then hedge back on FanDuel. A failure in phase 2 <em>after</em> phase 1 succeeded triggers a <code>CRITICAL</code> orphaned-bet alert and halts the worker.
      </p>

      <WatchedFiles files={BOT_FLOW.watches} updatedAt={BOT_FLOW.updated_at} />

      <h2 className="wk-h2">Three-phase pipeline</h2>
      <SequencePixel />

      <h2 className="wk-h2">Decision graph · live</h2>
      <p className="wk-prose">
        Hover any node for run counts and average duration over the last 24h. The metrics endpoint <code>/api/wiki/bot-flow.json</code> recomputes on a 5-minute cadence; toggle layers in the legend to focus on decision branches or recent failure paths.
      </p>
      <DecisionGraph page={BOT_FLOW} metrics={BOT_FLOW_METRICS} />

      <PhaseNote />

      <h2 className="wk-h2">Recurring issues observed</h2>
      <RecurringIssues />

      <footer className="wk-foot">
        See <a href="#" onClick={e => { e.preventDefault(); onSelect("dashboard"); }}>the Watchers card on the dashboard</a> for current backlog and last-tick freshness per watcher.
      </footer>
    </article>
  );
}

Object.assign(window, { WikiBotFlowPage, BOT_FLOW, BOT_FLOW_METRICS });
