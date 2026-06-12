"use client";

import { useMemo, useState } from "react";
import { EdgeRow, useApi } from "@/lib/api";
import { formatPct, formatPrice, formatTime } from "@/lib/format";
import { Column, DataTable } from "@/components/DataTable";
import { Empty, ErrorState, Loading } from "@/components/states";

type SignalFilter = "" | "arb" | "ev";

const SIGNAL_PILLS: { value: SignalFilter; label: string }[] = [
  { value: "", label: "ALL" },
  { value: "arb", label: "ARB" },
  { value: "ev", label: "EV" },
];

function pillClass(active: boolean): string {
  return `pixel bevel-out cursor-pointer px-3 py-0.5 ${
    active ? "bg-augusta-green text-crisp" : "bg-crisp text-ink hover:bg-inset"
  }`;
}

const COLUMNS: Column<EdgeRow>[] = [
  {
    key: "detected",
    label: "DETECTED",
    sortValue: (r) => r.detected_at ?? "",
    render: (r) => <span className="kicker">{formatTime(r.detected_at)}</span>,
  },
  {
    key: "venue",
    label: "VENUE",
    sortValue: (r) => r.venue_key ?? "",
    render: (r) => <span className="pixel">{r.venue_key ?? "—"}</span>,
  },
  {
    key: "type",
    label: "TYPE",
    sortValue: (r) => r.signal_type ?? "",
    render: (r) => <span className="kicker">{(r.signal_type ?? "—").toUpperCase()}</span>,
  },
  {
    key: "fair_prob",
    label: "FAIR PROB",
    numeric: true,
    sortValue: (r) => r.fair_prob,
    render: (r) => <span className="pixel">{formatPct(r.fair_prob)}</span>,
  },
  {
    key: "venue_price",
    label: "VENUE PRICE",
    numeric: true,
    sortValue: (r) => r.venue_price,
    render: (r) => <span className="pixel">{formatPrice(r.venue_price)}</span>,
  },
  {
    key: "edge",
    label: "EDGE",
    numeric: true,
    sortValue: (r) => r.edge,
    render: (r) => <span className={(r.edge ?? 0) >= 0 ? "ledger-pos" : "ledger-neg"}>{formatPct(r.edge)}</span>,
  },
  {
    key: "kelly",
    label: "KELLY",
    numeric: true,
    sortValue: (r) => r.kelly_fraction,
    render: (r) => <span className="pixel">{formatPct(r.kelly_fraction)}</span>,
  },
];

export default function EdgesPage() {
  const [signalType, setSignalType] = useState<SignalFilter>("");

  const path = useMemo(() => {
    const params = new URLSearchParams();
    if (signalType) params.set("signal_type", signalType);
    params.set("limit", "1000");
    return `/api/edges?${params.toString()}`;
  }, [signalType]);

  const { data, error, loading, reload } = useApi<EdgeRow[]>(path);
  const rows = data ?? [];

  return (
    <section>
      <div className="wsj-rule mb-4 flex flex-wrap items-end justify-between gap-3 pb-2">
        <div>
          <div className="kicker">RAW +EV &amp; ARB SIGNALS FROM THE MART</div>
          <h2 className="font-serif text-3xl italic">The Edge Wire</h2>
        </div>
        <div className="flex items-center gap-1">
          {SIGNAL_PILLS.map((p) => (
            <button key={p.value} className={pillClass(signalType === p.value)} onClick={() => setSignalType(p.value)}>
              {p.label}
            </button>
          ))}
        </div>
      </div>
      {loading ? (
        <Loading />
      ) : error ? (
        <ErrorState message={error} onRetry={reload} />
      ) : rows.length === 0 ? (
        <Empty note="NO SIGNALS ON THE WIRE." />
      ) : (
        <DataTable
          columns={COLUMNS}
          rows={rows}
          initialSort={{ key: "edge", dir: -1 }}
          rowKey={(r, i) => `${r.signal_id}-${i}`}
        />
      )}
    </section>
  );
}
