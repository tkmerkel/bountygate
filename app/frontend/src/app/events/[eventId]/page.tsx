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
