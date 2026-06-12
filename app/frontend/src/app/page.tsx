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
