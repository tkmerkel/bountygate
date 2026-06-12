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
