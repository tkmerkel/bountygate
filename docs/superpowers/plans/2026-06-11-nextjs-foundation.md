# Next.js Foundation (Stage 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the public product shell: a Pixel Augusta–themed Next.js app at `app/frontend/` with four screens (fair odds, movement, cross-venue, markets) proxying the existing FastAPI, Playwright-smoked, with the vanilla page retired.

**Architecture:** Next.js App Router + TypeScript + Tailwind v4; tokens live as CSS custom properties with Tailwind mapped onto them; `/api/*` rewrites to `API_BASE_URL` (no CORS dependency); client components fetch through one `useApi` hook; Playwright boots the real FastAPI against a seeded sqlite file.

**Tech Stack:** Next.js (latest via create-next-app), TypeScript, Tailwind CSS v4, Recharts ^3, @playwright/test, Python 3.12 (seed API), Node 22/npm 10.

**Spec:** `docs/superpowers/specs/2026-06-11-nextjs-foundation-design.md`

**Conventions:**
- All `npm`/`npx` commands run from `app/frontend/` unless stated otherwise; Python commands run from the repo root with global `py -3.12`.
- Windows host. In PowerShell, `cd app/frontend` once per command block.
- Accept whatever current versions `create-next-app@latest` scaffolds (Next 15/16, Tailwind 4) — do NOT downgrade; if a scaffolded API differs from this plan (e.g. config file name), adapt minimally and note it in your report.
- Commit messages end with the trailer line: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Scaffold the app

**Files:**
- Create: `app/frontend/**` (scaffolded), `app/frontend/package.json` (recharts added)

- [ ] **Step 1: Scaffold**

From the repo root:

```powershell
cd app; npx create-next-app@latest frontend --ts --tailwind --app --src-dir --import-alias "@/*" --use-npm --no-eslint --yes; cd ..
```

If prompted anyway (older npx cache), answer: TypeScript yes, ESLint no, Tailwind yes, `src/` yes, App Router yes, import alias `@/*`, Turbopack default.

- [ ] **Step 2: Add recharts**

```powershell
cd app/frontend; npm install recharts@^3
```

(recharts 2.x is end-of-life and renders unreliably under React 19; v3 required.)

- [ ] **Step 3: Verify the scaffold builds**

Run (in `app/frontend`): `npm run build`
Expected: `✓ Compiled successfully`, exit 0.

- [ ] **Step 4: Commit**

```bash
git add app/frontend
git commit -m "feat(frontend): scaffold Next.js app (TS, Tailwind, App Router) + recharts"
```

---

### Task 2: Pixel Augusta theme + chrome

**Files:**
- Replace: `app/frontend/src/app/globals.css`
- Replace: `app/frontend/src/app/layout.tsx`
- Create: `app/frontend/src/components/Masthead.tsx`
- Delete: `app/frontend/src/app/favicon.ico` retained as-is; delete any scaffolded `page.tsx` styling later (Task 4 replaces it). Remove `public/*.svg` scaffold art.

- [ ] **Step 1: Write `globals.css`** (tokens lifted from `app/design_handoff_bountygate_dashboard/prototype/dashboard/colors_and_type.css`; fonts come from next/font variables, not @import):

```css
@import "tailwindcss";

:root {
  --augusta-green:   #0B5B40;
  --augusta-green-2: #106B4D;
  --augusta-green-3: #084632;
  --augusta-green-4: #C9DBD2;
  --newsprint-white: #F4F1EA;
  --crisp-white:     #FFFFFF;
  --masters-yellow:  #F1C40F;
  --masters-yellow-2:#FFD84A;
  --ink-black:       #1A1A1A;
  --rule-gray:       #6B6B66;
  --ledger-red:      #B0211A;

  --bg-page:   var(--newsprint-white);
  --bg-card:   var(--crisp-white);
  --bg-inset:  #EEEAE0;
  --fg-1:      var(--ink-black);
  --fg-2:      #3A3A36;
  --fg-3:      var(--rule-gray);
  --fg-positive: #1F7A4D;

  --bevel-light: rgba(255, 255, 255, 0.40);
  --bevel-dark:  rgba(0, 0, 0, 0.40);
  --bevel-out: inset 1px 1px 0 var(--bevel-light), inset -1px -1px 0 var(--bevel-dark);
  --bevel-in:  inset 1px 1px 0 var(--bevel-dark),  inset -1px -1px 0 var(--bevel-light);
  --shadow-card: 2px 2px 0 var(--ink-black);
}

@theme inline {
  --color-augusta-green: var(--augusta-green);
  --color-augusta-green-2: var(--augusta-green-2);
  --color-augusta-green-3: var(--augusta-green-3);
  --color-augusta-green-4: var(--augusta-green-4);
  --color-newsprint: var(--newsprint-white);
  --color-crisp: var(--crisp-white);
  --color-masters-yellow: var(--masters-yellow);
  --color-ink: var(--ink-black);
  --color-rule-gray: var(--rule-gray);
  --color-ledger-red: var(--ledger-red);
  --color-positive: var(--fg-positive);
  --color-inset: var(--bg-inset);
  --font-serif: var(--font-playfair);
  --font-body: var(--font-merriweather);
  --font-pixel: var(--font-vt323);
}

html, body {
  background: var(--bg-page);
  color: var(--fg-1);
  font-family: var(--font-merriweather), Georgia, serif;
  font-size: 16px;
  line-height: 1.55;
}

h1, h2, h3, h4 {
  font-family: var(--font-playfair), "Times New Roman", serif;
  font-weight: 800;
  letter-spacing: -0.01em;
  line-height: 1.1;
}

/* WSJ double rule */
.wsj-rule { border-bottom: 3px double var(--ink-black); }

/* SC3K bevel chrome — crisp, no radius, no blur */
.bevel-out { border: 1px solid var(--ink-black); box-shadow: var(--bevel-out); }
.bevel-in  { border: 1px solid var(--ink-black); box-shadow: var(--bevel-in); background: var(--bg-inset); }
.card-hard { border: 1px solid var(--ink-black); box-shadow: var(--shadow-card); background: var(--bg-card); }

/* Ledger figures */
.ledger-pos { color: var(--fg-positive); font-family: var(--font-vt323), monospace; font-size: 1.15em; }
.ledger-neg { color: var(--ledger-red);  font-family: var(--font-vt323), monospace; font-size: 1.15em; }

/* Pixel figures (VT323 reads small — bump it) */
.pixel { font-family: var(--font-vt323), monospace; font-size: 1.15em; letter-spacing: 0; }

.kicker {
  font-family: var(--font-vt323), monospace;
  font-size: 14px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--fg-3);
}
```

- [ ] **Step 2: Write `layout.tsx`**

```tsx
import type { Metadata } from "next";
import { Merriweather, Playfair_Display, VT323 } from "next/font/google";
import { Masthead } from "@/components/Masthead";
import "./globals.css";

const playfair = Playfair_Display({
  subsets: ["latin"],
  style: ["normal", "italic"],
  variable: "--font-playfair",
});
const merriweather = Merriweather({
  subsets: ["latin"],
  weight: ["300", "400", "700"],
  style: ["normal", "italic"],
  variable: "--font-merriweather",
});
const vt323 = VT323({
  subsets: ["latin"],
  weight: "400",
  variable: "--font-vt323",
});

export const metadata: Metadata = {
  title: "The Daily Hedge — BountyGate",
  description: "Fair odds, line movement, and cross-venue prices for MLB, NBA, and NHL.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${playfair.variable} ${merriweather.variable} ${vt323.variable}`}>
      <body className="min-h-screen">
        <div className="mx-auto max-w-[1280px] px-4">
          <Masthead />
          <main className="py-6">{children}</main>
          <footer className="wsj-rule mb-2" />
          <footer className="kicker pb-6">
            BOUNTYGATE · ANALYTICS ONLY · NOT BETTING ADVICE
          </footer>
        </div>
      </body>
    </html>
  );
}
```

- [ ] **Step 3: Write `Masthead.tsx`** (client component — needs the active path):

```tsx
"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV = [
  { href: "/", label: "FAIR ODDS" },
  { href: "/cross-venue", label: "CROSS-VENUE" },
  { href: "/markets", label: "MARKETS" },
];

export function Masthead() {
  const pathname = usePathname();
  const today = new Date().toLocaleDateString("en-US", {
    weekday: "long", year: "numeric", month: "long", day: "numeric",
  });

  return (
    <header className="pt-4">
      <div className="flex items-end justify-between gap-4">
        <div className="kicker hidden w-48 sm:block">
          VOL. MMXXVI
          <br />
          FILED FROM REDMOND, WA
        </div>
        <h1 className="grow text-center font-serif text-[clamp(38px,4.2vw,60px)] italic">
          The Daily Hedge
        </h1>
        <div className="kicker hidden w-48 text-right sm:block">
          {today.toUpperCase()}
          <br />
          PRICE FREE TO PATRONS
        </div>
      </div>
      <nav className="mt-3 flex items-center gap-1 border-y border-ink py-1">
        {NAV.map(({ href, label }) => {
          const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={`pixel px-3 py-0.5 ${
                active
                  ? "bg-augusta-green text-crisp shadow-[inset_0_-2px_0_var(--masters-yellow)]"
                  : "text-ink hover:bg-inset"
              }`}
            >
              {label}
            </Link>
          );
        })}
        <span className="kicker ml-auto hidden sm:inline">PAIRED · HEDGED · BOOKED</span>
      </nav>
    </header>
  );
}
```

Also delete the scaffolded `public/*.svg` files (vercel/next logos) — unused.

- [ ] **Step 4: Verify build**

Run (in `app/frontend`): `npm run build`
Expected: compiles clean. (`/` still shows scaffold content until Task 4 — fine.)

- [ ] **Step 5: Commit**

```bash
git add app/frontend
git commit -m "feat(frontend): pixel augusta theme tokens + broadsheet masthead chrome"
```

---

### Task 3: API proxy + typed client + shared components

**Files:**
- Modify: `app/frontend/next.config.ts`
- Create: `app/frontend/src/lib/api.ts`
- Create: `app/frontend/src/lib/format.ts`
- Create: `app/frontend/src/components/DataTable.tsx`
- Create: `app/frontend/src/components/states.tsx`

- [ ] **Step 1: Rewrites in `next.config.ts`**

```ts
import type { NextConfig } from "next";

const API_BASE_URL = process.env.API_BASE_URL ?? "http://localhost:8000";

const nextConfig: NextConfig = {
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${API_BASE_URL}/:path*` }];
  },
};

export default nextConfig;
```

- [ ] **Step 2: Write `src/lib/api.ts`**

```ts
"use client";

import { useCallback, useEffect, useState } from "react";

export type FairOddsRow = {
  event_id: string;
  sport_key: string | null;
  commence_time: string | null;
  home_team: string | null;
  away_team: string | null;
  market_type: string;
  outcome_name: string;
  consensus_prob: number | null;
  best_price: number | null;
  best_bookmaker: string | null;
  edge: number | null;
  computed_at: string | null;
};

export type MovementPoint = {
  market_type: string;
  bookmaker: string;
  outcome_name: string;
  decimal_price: number | null;
  captured_at: string;
};

export type ClosingLine = {
  event_id: string;
  market_type: string;
  bookmaker: string;
  outcome_name: string;
  decimal_price: number | null;
  fair_prob: number | null;
  captured_at: string | null;
  staleness_minutes: number | null;
};

export type CrossMarketRow = {
  question_key: string;
  captured_at: string | null;
  kalshi_prob: number | null;
  polymarket_prob: number | null;
  sportsbook_consensus_prob: number | null;
  max_spread: number | null;
};

export type MarketRow = {
  market_id: string;
  venue_key: string | null;
  external_id: string | null;
  title: string | null;
  category: string | null;
  status: string | null;
  open_time: string | null;
  close_time: string | null;
  resolved_outcome: string | null;
  resolution_time: string | null;
  updated_at: string | null;
};

export type ApiState<T> = {
  data: T | null;
  error: string | null;
  loading: boolean;
  reload: () => void;
};

export function useApi<T>(path: string): ApiState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  const reload = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(path)
      .then(async (resp) => {
        if (!resp.ok) throw new Error(`status ${resp.status}`);
        return (await resp.json()) as T;
      })
      .then((body) => {
        if (!cancelled) setData(body);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [path, tick]);

  return { data, error, loading, reload };
}
```

- [ ] **Step 3: Write `src/lib/format.ts`**

```ts
export function formatPct(x: number | null | undefined): string {
  return x === null || x === undefined ? "—" : (x * 100).toFixed(1) + "%";
}

export function formatPrice(x: number | null | undefined): string {
  return x === null || x === undefined ? "—" : x.toFixed(2);
}

export function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d.getTime()) ? iso : d.toLocaleString();
}

// "nba:2026-06-09:SAS@NYK:NYK" -> {sport, date, matchup, side}
export function parseQuestionKey(key: string | null | undefined) {
  const [sport = "", date = "", pair = "", side = ""] = (key ?? "").split(":");
  return {
    sport: sport.toUpperCase(),
    date,
    matchup: pair.includes("@") ? pair.replace("@", " @ ") : pair,
    side,
  };
}
```

- [ ] **Step 4: Write `src/components/states.tsx`**

```tsx
export function Loading() {
  return <div className="kicker py-8 text-center">LOADING…</div>;
}

export function ErrorState({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="bevel-in my-4 p-4 text-center" data-testid="error-state">
      <span className="ledger-neg">FAILED TO LOAD</span>
      <span className="px-2 text-rule-gray">{message}</span>
      <button className="pixel bevel-out cursor-pointer bg-crisp px-3" onClick={onRetry}>
        RETRY
      </button>
    </div>
  );
}

export function Empty({ note }: { note: string }) {
  return <div className="kicker py-8 text-center">{note}</div>;
}
```

- [ ] **Step 5: Write `src/components/DataTable.tsx`** (generic, client-sortable, nulls last):

```tsx
"use client";

import { useMemo, useState } from "react";

export type Column<T> = {
  key: string;
  label: string;
  numeric?: boolean;
  sortValue?: (row: T) => number | string | null;
  render: (row: T) => React.ReactNode;
};

export function DataTable<T>({
  columns,
  rows,
  initialSort,
  rowKey,
}: {
  columns: Column<T>[];
  rows: T[];
  initialSort?: { key: string; dir: 1 | -1 };
  rowKey: (row: T, i: number) => string;
}) {
  const [sort, setSort] = useState(initialSort ?? null);

  const sorted = useMemo(() => {
    if (!sort) return rows;
    const col = columns.find((c) => c.key === sort.key);
    if (!col?.sortValue) return rows;
    return [...rows].sort((a, b) => {
      const av = col.sortValue!(a);
      const bv = col.sortValue!(b);
      if (av === null || av === undefined) return 1;
      if (bv === null || bv === undefined) return -1;
      if (av < bv) return -sort.dir;
      if (av > bv) return sort.dir;
      return 0;
    });
  }, [rows, sort, columns]);

  return (
    <div className="card-hard overflow-x-auto">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="bg-augusta-green text-left text-crisp">
            {columns.map((c) => (
              <th
                key={c.key}
                onClick={
                  c.sortValue
                    ? () =>
                        setSort((s) => ({
                          key: c.key,
                          dir: s?.key === c.key ? ((-s.dir) as 1 | -1) : -1,
                        }))
                    : undefined
                }
                className={`pixel px-3 py-1 font-normal ${c.sortValue ? "cursor-pointer select-none" : ""} ${
                  c.numeric ? "text-right" : ""
                }`}
              >
                {c.label}
                {sort?.key === c.key ? (sort.dir === -1 ? " ▼" : " ▲") : ""}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr key={rowKey(row, i)} className="border-t border-ink/20 odd:bg-crisp even:bg-newsprint">
              {columns.map((c) => (
                <td key={c.key} className={`px-3 py-1 ${c.numeric ? "text-right" : ""}`}>
                  {c.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 6: Verify build + commit**

Run (in `app/frontend`): `npm run build` → clean.

```bash
git add app/frontend
git commit -m "feat(frontend): /api rewrite proxy, typed api client, DataTable + states"
```

---

### Task 4: Fair odds screen (`/`)

**Files:**
- Replace: `app/frontend/src/app/page.tsx`

- [ ] **Step 1: Write the page**

```tsx
"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { FairOddsRow, useApi } from "@/lib/api";
import { formatPct, formatPrice, formatTime } from "@/lib/format";
import { Column, DataTable } from "@/components/DataTable";
import { Empty, ErrorState, Loading } from "@/components/states";

const COLUMNS: Column<FairOddsRow>[] = [
  {
    key: "matchup",
    label: "MATCHUP",
    render: (r) => (
      <Link href={`/events/${r.event_id}`} className="font-serif italic underline decoration-dotted">
        {r.away_team && r.home_team ? `${r.away_team} @ ${r.home_team}` : r.event_id.slice(0, 8)}
      </Link>
    ),
  },
  { key: "sport", label: "SPORT", render: (r) => <span className="kicker">{r.sport_key ?? "—"}</span> },
  { key: "market", label: "MKT", render: (r) => <span className="pixel">{r.market_type}</span> },
  { key: "outcome", label: "OUTCOME", render: (r) => r.outcome_name },
  {
    key: "consensus",
    label: "CONSENSUS",
    numeric: true,
    sortValue: (r) => r.consensus_prob,
    render: (r) => <span className="pixel">{formatPct(r.consensus_prob)}</span>,
  },
  {
    key: "best",
    label: "BEST PRICE",
    numeric: true,
    sortValue: (r) => r.best_price,
    render: (r) => <span className="pixel">{formatPrice(r.best_price)}</span>,
  },
  { key: "book", label: "BOOK", render: (r) => <span className="kicker">{r.best_bookmaker ?? "—"}</span> },
  {
    key: "edge",
    label: "EDGE",
    numeric: true,
    sortValue: (r) => r.edge,
    render: (r) =>
      r.edge === null ? "—" : <span className={r.edge >= 0 ? "ledger-pos" : "ledger-neg"}>{formatPct(r.edge)}</span>,
  },
  {
    key: "commence",
    label: "COMMENCE",
    sortValue: (r) => r.commence_time,
    render: (r) => <span className="kicker">{formatTime(r.commence_time)}</span>,
  },
];

export default function FairOddsPage() {
  const { data, error, loading, reload } = useApi<FairOddsRow[]>("/api/fair-odds?limit=1000");
  const [sport, setSport] = useState("");
  const [market, setMarket] = useState("");

  const sports = useMemo(() => [...new Set((data ?? []).map((r) => r.sport_key).filter(Boolean))] as string[], [data]);
  const rows = useMemo(
    () =>
      (data ?? []).filter(
        (r) => (!sport || r.sport_key === sport) && (!market || r.market_type === market),
      ),
    [data, sport, market],
  );

  return (
    <section>
      <div className="wsj-rule mb-4 flex items-end justify-between pb-2">
        <div>
          <div className="kicker">FILED · CONSENSUS FAIR PRICES · SHIN DEVIG, SHARPNESS-WEIGHTED</div>
          <h2 className="font-serif text-3xl italic">The Fair Price of Everything</h2>
        </div>
        <div className="flex gap-2">
          <select value={sport} onChange={(e) => setSport(e.target.value)} className="pixel bevel-in px-2 py-0.5">
            <option value="">ALL SPORTS</option>
            {sports.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
          <select value={market} onChange={(e) => setMarket(e.target.value)} className="pixel bevel-in px-2 py-0.5">
            <option value="">ALL MARKETS</option>
            <option value="h2h">h2h</option>
            <option value="totals">totals</option>
          </select>
        </div>
      </div>
      {loading ? (
        <Loading />
      ) : error ? (
        <ErrorState message={error} onRetry={reload} />
      ) : rows.length === 0 ? (
        <Empty note="NO FAIR ODDS YET — THE PRESSES ARE WARMING UP." />
      ) : (
        <DataTable columns={COLUMNS} rows={rows} initialSort={{ key: "edge", dir: -1 }} rowKey={(r, i) => `${r.event_id}-${r.market_type}-${r.outcome_name}-${i}`} />
      )}
    </section>
  );
}
```

- [ ] **Step 2: Verify against the live local API**

With the FastAPI running (`$env:DATABASE_URL` set; `py -3.12 -m uvicorn app.web.main:app --port 8000`) and `npm run dev` in `app/frontend`: `http://localhost:3000/` renders fair-odds rows. Then stop both.
If you can't run both servers, `npm run build` clean is the minimum gate for this task; the Playwright task covers rendering.

- [ ] **Step 3: Commit**

```bash
git add app/frontend/src/app/page.tsx
git commit -m "feat(frontend): fair odds screen with sport/market filters + edge sort"
```

---

### Task 5: Movement page (`/events/[eventId]`)

**Files:**
- Create: `app/frontend/src/app/events/[eventId]/page.tsx`
- Create: `app/frontend/src/components/MovementChart.tsx`

- [ ] **Step 1: Write `MovementChart.tsx`**

```tsx
"use client";

import { useMemo } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { ClosingLine, MovementPoint } from "@/lib/api";

const SERIES_COLORS = ["#0B5B40", "#B0211A", "#1A1A1A", "#106B4D", "#F1C40F", "#6B6B66", "#084632", "#FFD84A"];

type SeriesPoint = { t: number; price: number };
type Series = { name: string; points: SeriesPoint[] };

function buildSeries(points: MovementPoint[]): Series[] {
  const byKey = new Map<string, SeriesPoint[]>();
  for (const p of points) {
    if (p.decimal_price === null) continue;
    const t = new Date(p.captured_at).getTime();
    if (isNaN(t)) continue;
    const key = `${p.bookmaker} · ${p.outcome_name}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key)!.push({ t, price: p.decimal_price });
  }
  return [...byKey.entries()]
    .map(([name, pts]) => ({ name, points: pts.sort((a, b) => a.t - b.t) }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

export function MovementChart({ points, closing }: { points: MovementPoint[]; closing: ClosingLine[] }) {
  const series = useMemo(() => buildSeries(points), [points]);
  const fmtTick = (t: number) =>
    new Date(t).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });

  return (
    <div className="card-hard p-3" data-testid="movement-chart">
      <ResponsiveContainer width="100%" height={360}>
        <LineChart margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
          <CartesianGrid strokeDasharray="2 3" stroke="rgba(0,0,0,0.15)" />
          <XAxis
            dataKey="t"
            type="number"
            domain={["auto", "auto"]}
            tickFormatter={fmtTick}
            stroke="#1A1A1A"
            tick={{ fontFamily: "var(--font-vt323)", fontSize: 14 }}
          />
          <YAxis
            dataKey="price"
            domain={["auto", "auto"]}
            stroke="#1A1A1A"
            tick={{ fontFamily: "var(--font-vt323)", fontSize: 14 }}
            width={48}
          />
          <Tooltip
            labelFormatter={(t) => fmtTick(Number(t))}
            formatter={(value) => [value == null ? "—" : Number(value).toFixed(3), "price"]}
          />
          {series.length <= 10 && <Legend wrapperStyle={{ fontFamily: "var(--font-vt323)", fontSize: 14 }} />}
          {series.map((s, i) => (
            <Line
              key={s.name}
              data={s.points}
              dataKey="price"
              name={s.name}
              stroke={SERIES_COLORS[i % SERIES_COLORS.length]}
              // single-snapshot series are invisible without a dot
              dot={s.points.length < 2 ? { r: 3, fill: SERIES_COLORS[i % SERIES_COLORS.length] } : false}
              strokeWidth={1.5}
              isAnimationActive={false}
            />
          ))}
          {closing
            .filter((c) => c.decimal_price !== null && c.captured_at)
            .map((c) => (
              <ReferenceDot
                key={`${c.bookmaker}-${c.outcome_name}`}
                x={new Date(c.captured_at!).getTime()}
                y={c.decimal_price!}
                r={4}
                fill="#F1C40F"
                stroke="#1A1A1A"
              />
            ))}
        </LineChart>
      </ResponsiveContainer>
      <div className="kicker mt-1">YELLOW DOTS · CLOSING LINES</div>
    </div>
  );
}
```

- [ ] **Step 2: Write the page** (`src/app/events/[eventId]/page.tsx`):

```tsx
"use client";

import { use, useMemo, useState } from "react";
import { ClosingLine, MovementPoint, useApi } from "@/lib/api";
import { formatPct, formatPrice, formatTime } from "@/lib/format";
import { MovementChart } from "@/components/MovementChart";
import { Empty, ErrorState, Loading } from "@/components/states";

export default function EventPage({ params }: { params: Promise<{ eventId: string }> }) {
  const { eventId } = use(params);
  const [market, setMarket] = useState("h2h");
  const movement = useApi<MovementPoint[]>(`/api/movement/${eventId}?market_type=${market}`);
  const closing = useApi<ClosingLine[]>(`/api/closing-lines?event_id=${eventId}`);

  const closingForMarket = useMemo(
    () => (closing.data ?? []).filter((c) => c.market_type === market),
    [closing.data, market],
  );

  return (
    <section>
      <div className="wsj-rule mb-4 flex items-end justify-between pb-2">
        <div>
          <div className="kicker">FILED · LINE MOVEMENT · EVENT {eventId.slice(0, 8).toUpperCase()}</div>
          <h2 className="font-serif text-3xl italic">How the Line Moved</h2>
        </div>
        <div className="flex gap-1">
          {["h2h", "totals"].map((m) => (
            <button
              key={m}
              onClick={() => setMarket(m)}
              className={`pixel px-3 py-0.5 ${m === market ? "bg-augusta-green text-crisp" : "bevel-out bg-crisp"}`}
            >
              {m.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      {movement.loading ? (
        <Loading />
      ) : movement.error ? (
        <ErrorState message={movement.error} onRetry={movement.reload} />
      ) : (movement.data ?? []).length === 0 ? (
        <Empty note="NO SNAPSHOTS FOR THIS EVENT/MARKET." />
      ) : (
        <MovementChart points={movement.data!} closing={closingForMarket} />
      )}

      <h3 className="mt-6 font-serif text-xl italic">Closing Lines</h3>
      {closing.loading ? (
        <Loading />
      ) : closing.error ? (
        <ErrorState message={closing.error} onRetry={closing.reload} />
      ) : closingForMarket.length === 0 ? (
        <Empty note="NOT CLOSED YET — CLOSING LINES DERIVE AFTER COMMENCE." />
      ) : (
        <div className="card-hard mt-2 overflow-x-auto">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="bg-augusta-green text-left text-crisp">
                {["BOOK", "OUTCOME", "PRICE", "FAIR PROB", "CAPTURED", "STALENESS"].map((h) => (
                  <th key={h} className="pixel px-3 py-1 font-normal">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {closingForMarket.map((c, i) => (
                <tr key={i} className="border-t border-ink/20 odd:bg-crisp even:bg-newsprint">
                  <td className={`px-3 py-1 ${c.bookmaker === "consensus" ? "font-bold" : ""}`}>{c.bookmaker}</td>
                  <td className="px-3 py-1">{c.outcome_name}</td>
                  <td className="pixel px-3 py-1">{formatPrice(c.decimal_price)}</td>
                  <td className="pixel px-3 py-1">{formatPct(c.fair_prob)}</td>
                  <td className="kicker px-3 py-1">{formatTime(c.captured_at)}</td>
                  <td className="pixel px-3 py-1">
                    {c.staleness_minutes === null ? "—" : `${Math.round(c.staleness_minutes)}m`}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
```

(Note: `params` is a Promise in current Next App Router client pages — unwrap with `use()`. If the scaffolded Next version passes plain objects, drop the `Promise<>`/`use()` and take `params.eventId` directly; keep whichever typechecks.)

- [ ] **Step 3: Verify build, spot-check with a real event id** (as in Task 4 Step 2; `/events/<uuid from /fair-odds>` shows the chart). Commit:

```bash
git add app/frontend/src/app/events app/frontend/src/components/MovementChart.tsx
git commit -m "feat(frontend): movement page - multi-series price chart + closing lines"
```

---

### Task 6: Cross-venue + markets ports

**Files:**
- Create: `app/frontend/src/app/cross-venue/page.tsx`
- Create: `app/frontend/src/app/markets/page.tsx`

- [ ] **Step 1: Write `cross-venue/page.tsx`**

```tsx
"use client";

import { CrossMarketRow, useApi } from "@/lib/api";
import { formatPct, parseQuestionKey } from "@/lib/format";
import { Column, DataTable } from "@/components/DataTable";
import { Empty, ErrorState, Loading } from "@/components/states";

const COLUMNS: Column<CrossMarketRow>[] = [
  { key: "sport", label: "SPORT", render: (r) => <span className="kicker">{parseQuestionKey(r.question_key).sport}</span> },
  { key: "date", label: "DATE", render: (r) => parseQuestionKey(r.question_key).date },
  { key: "matchup", label: "MATCHUP", render: (r) => <span className="font-serif italic">{parseQuestionKey(r.question_key).matchup}</span> },
  { key: "side", label: "SIDE", render: (r) => parseQuestionKey(r.question_key).side },
  { key: "kalshi", label: "KALSHI", numeric: true, sortValue: (r) => r.kalshi_prob, render: (r) => <span className="pixel">{formatPct(r.kalshi_prob)}</span> },
  { key: "poly", label: "POLYMARKET", numeric: true, sortValue: (r) => r.polymarket_prob, render: (r) => <span className="pixel">{formatPct(r.polymarket_prob)}</span> },
  { key: "book", label: "SPORTSBOOK", numeric: true, sortValue: (r) => r.sportsbook_consensus_prob, render: (r) => <span className="pixel">{formatPct(r.sportsbook_consensus_prob)}</span> },
  { key: "spread", label: "SPREAD", numeric: true, sortValue: (r) => r.max_spread, render: (r) => <span className="pixel">{formatPct(r.max_spread)}</span> },
];

export default function CrossVenuePage() {
  const { data, error, loading, reload } = useApi<CrossMarketRow[]>("/api/cross-market?limit=200");

  return (
    <section>
      <div className="wsj-rule mb-4 pb-2">
        <div className="kicker">FILED · SAME GAME, THREE PRICES · KALSHI v POLYMARKET v BOOKS</div>
        <h2 className="font-serif text-3xl italic">The Cross-Venue Ledger</h2>
      </div>
      {loading ? (
        <Loading />
      ) : error ? (
        <ErrorState message={error} onRetry={reload} />
      ) : (data ?? []).length === 0 ? (
        <Empty note="NO LINKED QUESTIONS YET." />
      ) : (
        <DataTable columns={COLUMNS} rows={data!} initialSort={{ key: "spread", dir: -1 }} rowKey={(r) => r.question_key} />
      )}
    </section>
  );
}
```

- [ ] **Step 2: Write `markets/page.tsx`**

```tsx
"use client";

import { useMemo, useState } from "react";
import { MarketRow, useApi } from "@/lib/api";
import { formatTime } from "@/lib/format";
import { Column, DataTable } from "@/components/DataTable";
import { Empty, ErrorState, Loading } from "@/components/states";

const COLUMNS: Column<MarketRow>[] = [
  { key: "title", label: "TITLE", render: (r) => <span className="font-serif italic">{r.title ?? "—"}</span> },
  { key: "venue", label: "VENUE", render: (r) => <span className="kicker">{r.venue_key ?? "—"}</span> },
  { key: "status", label: "STATUS", render: (r) => <span className="pixel">{r.status ?? "—"}</span> },
  { key: "category", label: "CATEGORY", render: (r) => r.category ?? "—" },
  { key: "close", label: "CLOSE", sortValue: (r) => r.close_time, render: (r) => <span className="kicker">{formatTime(r.close_time)}</span> },
];

export default function MarketsPage() {
  const [venue, setVenue] = useState("");
  const [status, setStatus] = useState("");
  const [search, setSearch] = useState("");
  const qs = new URLSearchParams({ limit: "200" });
  if (venue) qs.set("venue", venue);
  if (status) qs.set("status", status);
  const { data, error, loading, reload } = useApi<MarketRow[]>(`/api/markets?${qs.toString()}`);

  const statuses = useMemo(() => [...new Set((data ?? []).map((r) => r.status).filter(Boolean))] as string[], [data]);
  const rows = useMemo(() => {
    const q = search.trim().toLowerCase();
    return q ? (data ?? []).filter((r) => (r.title ?? "").toLowerCase().includes(q)) : data ?? [];
  }, [data, search]);

  return (
    <section>
      <div className="wsj-rule mb-4 flex items-end justify-between pb-2">
        <div>
          <div className="kicker">FILED · EVERY LISTED QUESTION · KALSHI + POLYMARKET</div>
          <h2 className="font-serif text-3xl italic">The Market Directory</h2>
        </div>
        <div className="flex gap-2">
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="SEARCH TITLES…"
            className="pixel bevel-in px-2 py-0.5"
            data-testid="market-search"
          />
          <select value={venue} onChange={(e) => setVenue(e.target.value)} className="pixel bevel-in px-2 py-0.5">
            <option value="">ALL VENUES</option>
            <option value="kalshi">kalshi</option>
            <option value="polymarket">polymarket</option>
          </select>
          <select value={status} onChange={(e) => setStatus(e.target.value)} className="pixel bevel-in px-2 py-0.5">
            <option value="">ALL STATUS</option>
            {statuses.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>
      </div>
      {loading ? (
        <Loading />
      ) : error ? (
        <ErrorState message={error} onRetry={reload} />
      ) : rows.length === 0 ? (
        <Empty note="NO MARKETS MATCH." />
      ) : (
        <DataTable columns={COLUMNS} rows={rows} rowKey={(r) => r.market_id} />
      )}
    </section>
  );
}
```

- [ ] **Step 3: Verify build + commit**

`npm run build` clean.

```bash
git add app/frontend/src/app/cross-venue app/frontend/src/app/markets
git commit -m "feat(frontend): cross-venue + market browser ports"
```

---

### Task 7: Playwright smoke + seeded API

**Files:**
- Create: `app/frontend/e2e/seed_api.py`
- Create: `app/frontend/e2e/pages.spec.ts`
- Create: `app/frontend/playwright.config.ts`
- Modify: `app/frontend/package.json` (scripts + devDependency)

- [ ] **Step 1: Install Playwright**

```powershell
cd app/frontend; npm install -D @playwright/test; npx playwright install chromium
```

- [ ] **Step 2: Write `e2e/seed_api.py`** (run from the REPO ROOT; serves the real FastAPI against a seeded sqlite file):

```python
"""Seeded sqlite + real FastAPI for Playwright. Usage: py -3.12 app/frontend/e2e/seed_api.py"""
import os
import pathlib
import sys
import tempfile

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "app" / "shared" / "python"))

db_path = pathlib.Path(tempfile.gettempdir()) / "bg_e2e_seed.db"
if db_path.exists():
    db_path.unlink()
os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

from sqlalchemy import create_engine, text  # noqa: E402

EID = "11111111-1111-1111-1111-111111111111"

DDL = [
    "CREATE TABLE mart_fair_odds (event_id text, sport_key text, commence_time text, "
    "home_team text, away_team text, market_type text, outcome_name text, "
    "consensus_prob real, best_price real, best_bookmaker text, edge real, computed_at text)",
    "CREATE TABLE sportsbook_odds_history (event_id text, market_type text, bookmaker text, "
    "outcome_name text, captured_at text, decimal_price real)",
    "CREATE TABLE closing_lines (event_id text, market_type text, bookmaker text, "
    "outcome_name text, decimal_price real, fair_prob real, captured_at text, staleness_minutes real)",
    "CREATE TABLE mart_cross_market_prices (question_key text, captured_at text, kalshi_prob real, "
    "polymarket_prob real, sportsbook_consensus_prob real, max_spread real)",
    "CREATE TABLE markets (market_id text, venue_key text, external_id text, title text, "
    "category text, status text, open_time text, close_time text, resolved_outcome text, "
    "resolution_time text, updated_at text)",
]

INSERTS = [
    f"INSERT INTO mart_fair_odds VALUES ('{EID}','baseball_mlb','2026-06-10T19:00:00Z',"
    "'New York Yankees','Boston Red Sox','h2h','New York Yankees',0.62,1.72,'fanduel',0.0664,"
    "'2026-06-10T18:00:00Z')",
    f"INSERT INTO mart_fair_odds VALUES ('{EID}','baseball_mlb','2026-06-10T19:00:00Z',"
    "'New York Yankees','Boston Red Sox','h2h','Boston Red Sox',0.38,2.80,'draftkings',0.064,"
    "'2026-06-10T18:00:00Z')",
] + [
    f"INSERT INTO sportsbook_odds_history VALUES ('{EID}','h2h','fanduel','New York Yankees',"
    f"'2026-06-10T1{i}:00:00Z',1.{70 + i})"
    for i in range(5)
] + [
    # closing captured_at matches the last snapshot (mirrors derive_closing, and
    # keeps the chart's ReferenceDot inside the x-domain)
    f"INSERT INTO closing_lines VALUES ('{EID}','h2h','fanduel','New York Yankees',1.74,0.605,"
    "'2026-06-10T14:00:00Z',300.0)",
    f"INSERT INTO closing_lines VALUES ('{EID}','h2h','consensus','New York Yankees',NULL,0.61,"
    "'2026-06-10T14:00:00Z',300.0)",
    "INSERT INTO mart_cross_market_prices VALUES ('mlb:2026-06-10:BOS@NYY:NYY',"
    "'2026-06-10T18:00:00Z',0.60,0.59,0.62,0.03)",
    "INSERT INTO markets VALUES ('22222222-2222-2222-2222-222222222222','kalshi','KX1',"
    "'Yankees beat Red Sox','Sports','open','2026-06-09T00:00:00Z','2026-06-10T19:00:00Z',"
    "NULL,NULL,'2026-06-10T18:00:00Z')",
]

engine = create_engine(os.environ["DATABASE_URL"])
with engine.begin() as conn:
    for stmt in DDL + INSERTS:
        conn.execute(text(stmt))
engine.dispose()

import uvicorn  # noqa: E402

from app.web.main import app  # noqa: E402

uvicorn.run(app, host="127.0.0.1", port=8765)
```

- [ ] **Step 3: Write `playwright.config.ts`**

```ts
import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  use: { baseURL: "http://127.0.0.1:3100" },
  webServer: [
    {
      command: "py -3.12 app/frontend/e2e/seed_api.py",
      cwd: "../..",
      url: "http://127.0.0.1:8765/health",
      reuseExistingServer: false,
      timeout: 60_000,
    },
    {
      command: "npm run dev -- --port 3100",
      url: "http://127.0.0.1:3100",
      reuseExistingServer: false,
      timeout: 120_000,
      env: { API_BASE_URL: "http://127.0.0.1:8765" },
    },
  ],
});
```

- [ ] **Step 4: Write `e2e/pages.spec.ts`** (five smokes incl. error state via route abort):

```ts
import { expect, test } from "@playwright/test";

const EID = "11111111-1111-1111-1111-111111111111";

test("fair odds page renders rows and filters", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("h2")).toContainText("The Fair Price of Everything");
  await expect(page.getByRole("cell", { name: /Boston Red Sox @ New York Yankees/ }).first()).toBeVisible();
  await expect(page.locator("tbody tr")).toHaveCount(2);
});

test("movement page renders chart and closing lines", async ({ page }) => {
  await page.goto(`/events/${EID}`);
  await expect(page.getByTestId("movement-chart")).toBeVisible();
  // a drawn price line, not just a mounted svg (blank-chart regression guard)
  await expect(page.locator("path.recharts-curve").first()).toBeVisible();
  // the closing-line marker dot from the seeded fanduel close
  await expect(page.locator(".recharts-reference-dot").first()).toBeVisible();
  await expect(page.getByRole("cell", { name: "consensus" })).toBeVisible();
});

test("cross-venue page renders linked questions", async ({ page }) => {
  await page.goto("/cross-venue");
  await expect(page.locator("h2")).toContainText("The Cross-Venue Ledger");
  await expect(page.getByRole("cell", { name: "BOS @ NYY" })).toBeVisible();
});

test("markets page filters by search", async ({ page }) => {
  await page.goto("/markets");
  await expect(page.getByRole("cell", { name: "Yankees beat Red Sox" })).toBeVisible();
  await page.getByTestId("market-search").fill("zzz-no-match");
  await expect(page.getByText("NO MARKETS MATCH.")).toBeVisible();
});

test("api failure shows error state with retry", async ({ page }) => {
  await page.route("**/api/fair-odds*", (route) => route.abort());
  await page.goto("/");
  await expect(page.getByTestId("error-state")).toBeVisible();
  await expect(page.getByRole("button", { name: "RETRY" })).toBeVisible();
});
```

- [ ] **Step 5: Add npm script** — in `app/frontend/package.json` `"scripts"`, add `"e2e": "playwright test"`.

- [ ] **Step 6: Run the suite**

Run (in `app/frontend`): `npm run e2e`
Expected: 5 passed.

- [ ] **Step 7: Commit**

```bash
git add app/frontend/e2e app/frontend/playwright.config.ts app/frontend/package.json app/frontend/package-lock.json
git commit -m "test(frontend): playwright smoke per page against seeded sqlite API"
```

---

### Task 8: Retire the vanilla page

**Files:**
- Delete: `app/web/static/index.html`, `app/web/static/app.js`, `app/web/static/styles.css`
- Modify: `app/web/main.py`
- Delete: `app/web/tests/test_static.py`
- Create: `app/web/tests/test_root.py`

- [ ] **Step 1: Write the failing test** — `app/web/tests/test_root.py`:

```python
from fastapi.testclient import TestClient

from app.web.main import app


def test_root_returns_service_json():
    client = TestClient(app)
    body = client.get("/").json()
    assert body["service"] == "bountygate read API"
    assert body["docs"] == "/docs"
```

Run: `py -3.12 -m pytest app/web/tests/test_root.py -v` → FAIL (root currently serves HTML).

- [ ] **Step 2: Edit `app/web/main.py`** — remove the `FileResponse`/`StaticFiles` imports, the `STATIC_DIR` block, the mount, and the `index()` route; replace with:

```python
@app.get("/")
def index():
    return {"service": "bountygate read API", "docs": "/docs"}
```

Delete the three files under `app/web/static/` and `app/web/tests/test_static.py`.

- [ ] **Step 3: Run the full web suite**

Run: `py -3.12 -m pytest app/web/tests -v`
Expected: all pass (test_root replaces the three static tests; total = previous 14 − 3 + 1 = 12).

- [ ] **Step 4: Commit**

```bash
git add -A app/web
git commit -m "feat(web): retire vanilla static page; / returns service JSON"
```

---

### Task 9: README + full verification

**Files:**
- Create: `app/frontend/README.md` (replace scaffolded)

- [ ] **Step 1: Write the README**

```markdown
# The Daily Hedge — bountygate frontend

Next.js (App Router, TS, Tailwind v4) public product shell. Pixel Augusta theme.

## Develop

    # API (repo root; needs DATABASE_URL in env)
    py -3.12 -m uvicorn app.web.main:app --port 8000
    # frontend
    cd app/frontend && npm run dev      # http://localhost:3000

`/api/*` is rewritten to `API_BASE_URL` (default `http://localhost:8000`).

## Test

    npm run e2e     # boots seeded sqlite FastAPI + dev server, 5 smokes

## Build

    npm run build

## Deploy (Vercel)

1. Import the GitHub repo in Vercel; set **Root Directory** to `app/frontend`.
2. Set env var `API_BASE_URL` to the Heroku API origin (e.g. `https://bountygate.herokuapp.com`).
3. Deploy. Previews get the same env var.
```

- [ ] **Step 2: Full verification**

```powershell
cd app/frontend; npm run build; npm run e2e; cd ../..
py -3.12 -m pytest app/web/tests app/shared/python/bountygate/models/tests -q
```

Expected: build clean, 5 e2e passed, 31 python tests passed (12 web + 19 models).

- [ ] **Step 3: Commit**

```bash
git add app/frontend/README.md
git commit -m "docs(frontend): dev/test/build/vercel instructions"
```
