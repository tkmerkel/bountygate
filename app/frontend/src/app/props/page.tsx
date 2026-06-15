"use client";

import { useEffect, useMemo, useState } from "react";
import { PropRow, useApi } from "@/lib/api";
import { formatPrice, formatTime, minutesSince, newestTimestamp, relativeAge } from "@/lib/format";
import { Column, DataTable } from "@/components/DataTable";
import { Empty, ErrorState, Loading, StaleBanner } from "@/components/states";

// Feed runs every 15 min; flag the table once its freshest line is older than
// this many minutes (one fully-missed cycle plus normalize lag).
const STALE_AFTER_MIN = 30;

// Sport options for the filter select
const SPORT_OPTIONS: { value: string; label: string }[] = [
  { value: "", label: "ALL" },
  { value: "baseball_mlb", label: "MLB" },
  { value: "basketball_nba", label: "NBA" },
  { value: "basketball_wnba", label: "WNBA" },
  { value: "icehockey_nhl", label: "NHL" },
  { value: "americanfootball_nfl", label: "NFL" },
];

// Normalize market_key for best-price grouping: strip _alternate suffix so
// e.g. "player_points_alternate" and "player_points" share the same group.
function canonicalMarket(mk: string): string {
  return mk.replace(/_alternate$/, "");
}

// Build a map of best (max) decimal_price per (player, canonical_market, line, side).
// A cell is highlighted only when its group has ≥2 distinct bookmakers — a solo
// price is not a "best" in any meaningful competitive sense.
function buildBestPriceMap(rows: PropRow[]): Map<string, number> {
  // First pass: collect all prices per group key
  const groups = new Map<string, { prices: number[]; books: Set<string> }>();
  for (const r of rows) {
    if (r.decimal_price === null) continue;
    const key = `${r.player_name ?? ""}|${canonicalMarket(r.market_key)}|${r.line ?? ""}|${r.side ?? ""}`;
    if (!groups.has(key)) groups.set(key, { prices: [], books: new Set() });
    const g = groups.get(key)!;
    g.prices.push(r.decimal_price);
    if (r.bookmaker) g.books.add(r.bookmaker);
  }
  // Second pass: emit max price only for groups with ≥2 books
  const best = new Map<string, number>();
  for (const [key, { prices, books }] of groups) {
    if (books.size >= 2) {
      best.set(key, Math.max(...prices));
    }
  }
  return best;
}

export default function PropsPage() {
  // Raw text input — debounced before hitting the API
  const [playerInput, setPlayerInput] = useState("");
  const [playerDebounced, setPlayerDebounced] = useState("");
  const [sport, setSport] = useState("");
  const [marketKey, setMarketKey] = useState("");

  // Debounce player input: wait 300ms after last keystroke before querying
  useEffect(() => {
    const t = setTimeout(() => setPlayerDebounced(playerInput), 300);
    return () => clearTimeout(t);
  }, [playerInput]);

  // Build query path. Market filter is applied server-side via market_key param.
  // Market select options are derived from the current result set (see comment
  // below on market options strategy). limit=2000 is the server max.
  const path = useMemo(() => {
    const params = new URLSearchParams();
    params.set("limit", "2000");
    if (sport) params.set("sport", sport);
    if (playerDebounced.trim()) params.set("player", playerDebounced.trim());
    if (marketKey) params.set("market_key", marketKey);
    return `/api/props?${params.toString()}`;
  }, [sport, playerDebounced, marketKey]);

  const { data, error, loading, reload } = useApi<PropRow[]>(path);
  const rows = data ?? [];

  // Market key options: derived from the current result set (without the active
  // market_key filter applied — but since we're filtering server-side, the
  // options list will only reflect results matching the other active filters).
  // Trade-off: this keeps the UI simple and consistent with the rest of the app
  // (no separate unfiltered fetch). The selected value is always kept as an
  // option even if absent from current rows, so the control never silently resets.
  const marketOptions = useMemo(() => {
    const keys = Array.from(new Set(rows.map((r) => r.market_key))).sort();
    const options = [{ value: "", label: "ALL" }, ...keys.map((k) => ({ value: k, label: k }))];
    // Ensure the currently-selected key is present even if absent from rows
    if (marketKey && !keys.includes(marketKey)) {
      options.push({ value: marketKey, label: marketKey });
    }
    return options;
  }, [rows, marketKey]);

  // Freshness: the newest captured_at across the shown rows. If that is older
  // than the expected feed cadence, the prices on screen are stale (ingestion
  // outage) — surface a banner so aging lines don't read as live.
  const newestIso = useMemo(() => newestTimestamp(rows.map((r) => r.captured_at)), [rows]);
  const staleAgeLabel = useMemo(() => {
    const mins = minutesSince(newestIso);
    return mins !== null && mins > STALE_AFTER_MIN ? relativeAge(newestIso) : null;
  }, [newestIso]);

  // Best-price map for highlight logic
  const bestPriceMap = useMemo(() => buildBestPriceMap(rows), [rows]);

  const COLUMNS: Column<PropRow>[] = useMemo(
    () => [
      {
        key: "player",
        label: "PLAYER",
        sortValue: (r) => r.player_name ?? "",
        render: (r) => <span className="pixel">{r.player_name ?? "—"}</span>,
      },
      {
        key: "matchup",
        label: "MATCHUP",
        sortValue: (r) => r.commence_time ?? "",
        render: (r) => (
          <div>
            <div className="font-serif italic">
              {r.away_team && r.home_team ? `${r.away_team} @ ${r.home_team}` : r.event_id?.slice(0, 8) ?? "—"}
            </div>
            <div className="kicker">{r.sport_key ?? "—"}</div>
          </div>
        ),
      },
      {
        key: "market",
        label: "MARKET",
        sortValue: (r) => r.market_key,
        render: (r) => <span className="pixel">{r.market_key}</span>,
      },
      {
        key: "line",
        label: "LINE",
        numeric: true,
        sortValue: (r) => r.line ?? 0,
        render: (r) => <span className="pixel">{r.line !== null && r.line !== undefined ? String(r.line) : "—"}</span>,
      },
      {
        key: "side",
        label: "SIDE",
        render: (r) => (
          <div>
            <span className="pixel">{r.side ?? "—"}</span>
            {r.side ? <span className="kicker"> {r.side.toUpperCase() === "OVER" ? "OVR" : r.side.toUpperCase() === "UNDER" ? "UND" : ""}</span> : null}
          </div>
        ),
      },
      {
        key: "book",
        label: "BOOK",
        sortValue: (r) => r.bookmaker ?? "",
        render: (r) => <span>{r.bookmaker ?? "—"}</span>,
      },
      {
        key: "price",
        label: "PRICE",
        numeric: true,
        sortValue: (r) => r.decimal_price ?? 0,
        render: (r) => {
          const groupKey = `${r.player_name ?? ""}|${canonicalMarket(r.market_key)}|${r.line ?? ""}|${r.side ?? ""}`;
          const best = bestPriceMap.get(groupKey);
          const isBest = best !== undefined && r.decimal_price !== null && r.decimal_price === best;
          return (
            <span className={`pixel ${isBest ? "ledger-pos" : ""}`}>
              {formatPrice(r.decimal_price)}
            </span>
          );
        },
      },
      {
        key: "as_of",
        label: "AS OF",
        sortValue: (r) => r.captured_at ?? "",
        render: (r) => <span className="kicker">{formatTime(r.captured_at)}</span>,
      },
    ],
    [bestPriceMap],
  );

  return (
    <section>
      <div className="wsj-rule mb-4 flex flex-wrap items-end justify-between gap-3 pb-2">
        <div>
          <div className="kicker">PLAYER PROP LINES · BEST PRICE PER SIDE HIGHLIGHTED</div>
          <h2 className="font-serif text-3xl italic">The Props Counter</h2>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          {/* Player search — debounced 300ms */}
          <label className="flex items-center gap-1">
            <span className="kicker">PLAYER</span>
            <input
              type="text"
              placeholder="search…"
              value={playerInput}
              onChange={(e) => setPlayerInput(e.target.value)}
              className="pixel bevel-in w-36 px-2 py-0.5"
            />
          </label>
          {/* Sport filter */}
          <label className="flex items-center gap-1">
            <span className="kicker">SPORT</span>
            <select
              value={sport}
              onChange={(e) => setSport(e.target.value)}
              className="pixel bevel-in px-2 py-0.5"
            >
              {SPORT_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          {/* Market filter — options from current rows, server-side filtering */}
          <label className="flex items-center gap-1">
            <span className="kicker">MARKET</span>
            <select
              value={marketKey}
              onChange={(e) => setMarketKey(e.target.value)}
              className="pixel bevel-in px-2 py-0.5"
            >
              {marketOptions.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>
      {loading ? (
        <Loading />
      ) : error ? (
        <ErrorState message={error} onRetry={reload} />
      ) : rows.length === 0 ? (
        <Empty note="NO PROP LINES INSIDE 24 HOURS — FEED MAY BE DOWN." />
      ) : (
        <>
          {staleAgeLabel ? <StaleBanner ageLabel={staleAgeLabel} /> : null}
          <DataTable
            columns={COLUMNS}
            rows={rows}
            initialSort={{ key: "player", dir: 1 }}
            rowKey={(r, i) => `${r.event_id}-${r.market_key}-${r.player_name}-${r.bookmaker}-${r.side}-${i}`}
          />
        </>
      )}
    </section>
  );
}
