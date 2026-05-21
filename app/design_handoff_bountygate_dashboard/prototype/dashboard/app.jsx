/* BountyGate · The Daily Hedge — main app. */

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "sort": "recency",
  "density": "comfortable",
  "showHero": true,
  "showTicker": true,
  "tickerMode": "stats"
}/*EDITMODE-END*/;

function App() {
  const runs = (window.BG_DATA?.runs || []);
  const accounts = window.BG_MOCK.accounts;
  const watchers = window.BG_MOCK.watchers;
  const pages = window.BG_MOCK.wikiPages;

  const [openId, setOpenId] = useState(runs[0]?.run_id || null);
  const [filter, setFilter] = useState("all");
  const [activeSlug, setActiveSlug] = useState("dashboard");

  const [tweaks, setTweak] = window.useTweaks(TWEAK_DEFAULTS);

  // Build live-stats ticker items
  const tickerItems = useMemo(() => {
    const books = Object.values(accounts.books);
    const items = [];
    items.push("MARKET OPEN");
    books.forEach(b => {
      items.push(`${b.book.toUpperCase()} BAL ${fmtUsd(b.balance)}  ·  7D ${fmtUsd(b.pnl_7d, true)}`);
    });
    const lastRun = runs[0];
    if (lastRun) {
      items.push(`LAST RUN · ${lastRun.player.toUpperCase()} · ${lastRun.market.toUpperCase()} · ${lastRun.outcome.toUpperCase()} IN ${lastRun.duration_s?.toFixed(1)}s`);
    }
    const offNominal = watchers.watchers.filter(w => w.status !== "ok");
    if (offNominal.length === 0) {
      items.push("WATCHERS · ALL NOMINAL");
    } else {
      offNominal.forEach(w => items.push(`WATCHER ${w.name.toUpperCase()} · ${w.status.toUpperCase()}` + (w.last_error ? ` · ${w.last_error.toUpperCase()}` : "")));
    }
    if (tweaks.tickerMode === "findings" || tweaks.tickerMode === "mixed") {
      runs.slice(0, 4).forEach(r => {
        const t = r.top_finding.split(/[.\u2014—]/)[0].trim().slice(0, 140).toUpperCase();
        items.push(`EDITORIAL · ${t}`);
      });
    }
    const wasted = runs.reduce((s,r) => s + (r.estimated_wasted_wait_s || 0), 0);
    items.push(`PHASE-2 WASTE LAST ${runs.length} RUNS · −${wasted}s`);
    return items;
  }, [accounts, watchers, runs, tweaks.tickerMode]);

  // If active page isn't dashboard, show a tasteful "wiki page" placeholder.
  const isDashboard = activeSlug === "dashboard";

  return (
    <div className={"bg-shell" + (tweaks.density === "compact" ? " is-compact" : "")}>
      <Sidebar active={activeSlug} onSelect={setActiveSlug} pages={pages} />
      <main className="bg-main">
        <Mast runs={runs} watchers={watchers} accounts={accounts} />

        {isDashboard ? (
          <>
            {tweaks.showHero && (
              <ProfitChart
                data={window.BG_MOCK.dailyPnl}
                runs={runs}
                activeRun={openId}
                onPinClick={id => setOpenId(id)}
              />
            )}

            <h2 className="bg-sec">Treasury &amp; Operations</h2>
            <div className="bg-row-2">
              <Accounts data={accounts} />
              <Watchers data={watchers} />
            </div>

            <h2 className="bg-sec">From the Audit Desk</h2>
            <Runs runs={runs} openId={openId} onOpen={setOpenId}
                  filter={filter} onFilter={setFilter}
                  sort={tweaks.sort} />

            <footer className="bg-foot">
              The Pixel Augusta is a publication of the BountyGate internal review committee. Data refreshes every 30s from <code>/api/runs</code>, <code>/api/account-stats</code>, <code>/api/watchers</code>. Patrons of the club only.
            </footer>
          </>
        ) : activeSlug === "bot-flow" ? (
          <window.WikiBotFlowPage pages={pages} activeSlug={activeSlug} onSelect={setActiveSlug} />
        ) : (
          <WikiPlaceholder slug={activeSlug} title={pages.find(p => p.slug === activeSlug)?.title} onBack={() => setActiveSlug("dashboard")} />
        )}

      </main>

      {tweaks.showTicker && <Ticker items={tickerItems} badge="● LIVE" />}

      <window.TweaksPanel title="Tweaks">
        <window.TweakSection label="Display">
          <window.TweakRadio
            label="Density"
            value={tweaks.density}
            options={[{value:"comfortable",label:"COMFORT"},{value:"compact",label:"COMPACT"}]}
            onChange={v => setTweak("density", v)} />
          <window.TweakToggle label="Profit chart" value={tweaks.showHero} onChange={v => setTweak("showHero", v)} />
          <window.TweakToggle label="Ticker" value={tweaks.showTicker} onChange={v => setTweak("showTicker", v)} />
        </window.TweakSection>
        <window.TweakSection label="Runs">
          <window.TweakRadio
            label="Sort by"
            value={tweaks.sort}
            options={[{value:"recency",label:"RECENT"},{value:"wasted",label:"WASTE"},{value:"outcome",label:"FAILURE"}]}
            onChange={v => setTweak("sort", v)} />
        </window.TweakSection>
        <window.TweakSection label="Ticker">
          <window.TweakRadio
            label="Content"
            value={tweaks.tickerMode}
            options={[{value:"stats",label:"STATS"},{value:"findings",label:"EDITS"},{value:"mixed",label:"MIX"}]}
            onChange={v => setTweak("tickerMode", v)} />
        </window.TweakSection>
      </window.TweaksPanel>
    </div>
  );
}

function WikiPlaceholder({ slug, title, onBack }) {
  return (
    <div style={{ padding: "20px 0 60px" }}>
      <WindowCard title={(slug || "page").toUpperCase().replace(/-/g, "_") + ".MD"} subtitle="WIKI" glyph="[X]" onGlyph={onBack}>
        <p style={{ fontFamily: "var(--font-serif-body)", fontSize: 16, lineHeight: 1.55 }}>
          <em>Wiki page <strong>{title}</strong> would render here.</em> The live dashboard FastAPI app renders these from <code>wiki/*.md</code> with Mermaid + React Flow support. For the redesign mock, see the <a href="#" onClick={e => { e.preventDefault(); onBack(); }} style={{ color: "var(--augusta-green)", fontWeight: 700 }}>Dashboard</a>.
        </p>
        <Button variant="primary" onClick={onBack}>← BACK TO DASHBOARD</Button>
      </WindowCard>
    </div>
  );
}

/* mount */
ReactDOM.createRoot(document.getElementById("root")).render(<App />);
