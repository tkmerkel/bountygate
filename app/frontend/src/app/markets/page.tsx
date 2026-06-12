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
