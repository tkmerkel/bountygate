"use client";

import { useMemo, useState } from "react";
import { ArbRow, useApi } from "@/lib/api";
import { formatPct, formatPrice, formatTime, minutesSince, newestTimestamp, relativeAge as ageLabel } from "@/lib/format";
import { Column, DataTable } from "@/components/DataTable";
import { Empty, ErrorState, Loading, StaleBanner } from "@/components/states";

type KindFilter = "" | "game" | "prop";
type PairingFilter = "" | "book_book" | "book_venue";

const KIND_PILLS: { value: KindFilter; label: string }[] = [
  { value: "", label: "ALL" },
  { value: "game", label: "GAME" },
  { value: "prop", label: "PROP" },
];

const PAIRING_PILLS: { value: PairingFilter; label: string }[] = [
  { value: "", label: "ALL" },
  { value: "book_book", label: "BOOK×BOOK" },
  { value: "book_venue", label: "BOOK×VENUE" },
];

// An arb's last_seen_at older than this counts as stale (matches the API's live cutoff).
const STALE_AFTER_MIN = 30;
const STALE_CUTOFF_MS = STALE_AFTER_MIN * 60 * 1000;

function pillClass(active: boolean): string {
  return `pixel bevel-out cursor-pointer px-3 py-0.5 ${
    active ? "bg-augusta-green text-crisp" : "bg-crisp text-ink hover:bg-inset"
  }`;
}

function legLabel(source: string | null, outcome: string | null, point: number | null, price: number): React.ReactNode {
  const head = [source ?? "—", outcome ?? "—"].join(" · ");
  const pt = point === null || point === undefined ? "" : ` ${point}`;
  return (
    <span>
      {head}
      {pt} @ <span className="pixel">{formatPrice(price)}</span>
    </span>
  );
}

function relativeAge(iso: string | null): React.ReactNode {
  if (!iso) return <span className="kicker">—</span>;
  const t = new Date(iso).getTime();
  if (isNaN(t)) return <span className="kicker">—</span>;
  const ageMs = Date.now() - t;
  if (ageMs <= STALE_CUTOFF_MS) {
    return <span className="pixel text-augusta-green">LIVE</span>;
  }
  const mins = Math.floor(ageMs / 60000);
  let label: string;
  if (mins < 60) label = `${mins}m ago`;
  else if (mins < 60 * 24) label = `${Math.floor(mins / 60)}h ago`;
  else label = `${Math.floor(mins / (60 * 24))}d ago`;
  return <span className="kicker">{label}</span>;
}

const COLUMNS: Column<ArbRow>[] = [
  {
    key: "matchup",
    label: "MATCHUP",
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
    key: "segment",
    label: "SEGMENT",
    render: (r) => (
      <div>
        <span className="pixel">{r.market_segment ?? "—"}</span>
        {r.kind === "prop" && r.player_name ? (
          <div className="kicker">
            {r.player_name}
            {r.line === null || r.line === undefined ? "" : ` · ${r.line}`}
          </div>
        ) : null}
      </div>
    ),
  },
  {
    key: "leg_a",
    label: "LEG A",
    render: (r) => legLabel(r.leg_a_source, r.leg_a_outcome, r.leg_a_point, r.leg_a_price),
  },
  {
    key: "leg_b",
    label: "LEG B",
    render: (r) => legLabel(r.leg_b_source, r.leg_b_outcome, r.leg_b_point, r.leg_b_price),
  },
  {
    key: "roi",
    label: "ROI",
    numeric: true,
    sortValue: (r) => r.roi,
    render: (r) => <span className={r.roi >= 0 ? "ledger-pos" : "ledger-neg"}>{formatPct(r.roi)}</span>,
  },
  {
    key: "fee_adjusted_roi",
    label: "FEE-ADJ ROI",
    numeric: true,
    sortValue: (r) => r.fee_adjusted_roi,
    render: (r) => (
      <span className={r.fee_adjusted_roi >= 0 ? "ledger-pos" : "ledger-neg"}>{formatPct(r.fee_adjusted_roi)}</span>
    ),
  },
  {
    key: "freshness",
    label: "FRESHNESS",
    sortValue: (r) => r.last_seen_at,
    render: (r) => relativeAge(r.last_seen_at),
  },
  {
    key: "commence",
    label: "COMMENCE",
    sortValue: (r) => r.commence_time,
    render: (r) => <span className="kicker">{formatTime(r.commence_time)}</span>,
  },
];

export default function ArbitragePage() {
  const [kind, setKind] = useState<KindFilter>("");
  const [pairing, setPairing] = useState<PairingFilter>("");
  const [minRoi, setMinRoi] = useState("0");
  const [includeStale, setIncludeStale] = useState(false);

  const path = useMemo(() => {
    const params = new URLSearchParams();
    if (kind) params.set("kind", kind);
    if (pairing) params.set("pairing", pairing);
    const roiPct = parseFloat(minRoi);
    if (!isNaN(roiPct) && roiPct !== 0) params.set("min_roi", String(roiPct / 100));
    if (includeStale) params.set("include_stale", "true");
    params.set("limit", "1000");
    return `/api/arbs?${params.toString()}`;
  }, [kind, pairing, minRoi, includeStale]);

  const { data, error, loading, reload } = useApi<ArbRow[]>(path);
  const rows = data ?? [];

  // Freshest arb on the tape. If even that is older than the live cutoff, the
  // build_arbs pipeline (or its inputs) has stalled — flag it so aged-out arbs
  // (typically the STALE-ON view) don't read as actionable.
  const staleAgeLabel = useMemo(() => {
    const newest = newestTimestamp(rows.map((r) => r.last_seen_at));
    const mins = minutesSince(newest);
    return mins !== null && mins > STALE_AFTER_MIN ? ageLabel(newest) : null;
  }, [rows]);

  return (
    <section>
      <div className="wsj-rule mb-4 flex flex-wrap items-end justify-between gap-3 pb-2">
        <div>
          <div className="kicker">CROSS-BOOK &amp; CROSS-VENUE DISLOCATIONS · DETECTION ONLY</div>
          <h2 className="font-serif text-3xl italic">The Arbitrage Ledger</h2>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-1">
            {KIND_PILLS.map((p) => (
              <button key={p.value} className={pillClass(kind === p.value)} onClick={() => setKind(p.value)}>
                {p.label}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-1">
            {PAIRING_PILLS.map((p) => (
              <button key={p.value} className={pillClass(pairing === p.value)} onClick={() => setPairing(p.value)}>
                {p.label}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-1">
            <span className="kicker">MIN ROI %</span>
            <input
              type="number"
              step="0.5"
              value={minRoi}
              onChange={(e) => setMinRoi(e.target.value)}
              className="pixel bevel-in w-16 px-2 py-0.5"
            />
          </label>
          <button className={pillClass(includeStale)} onClick={() => setIncludeStale((s) => !s)}>
            STALE {includeStale ? "ON" : "OFF"}
          </button>
        </div>
      </div>
      {loading ? (
        <Loading />
      ) : error ? (
        <ErrorState message={error} onRetry={reload} />
      ) : rows.length === 0 ? (
        <Empty note="NO DISLOCATIONS ON THE TAPE." />
      ) : (
        <>
          {staleAgeLabel ? <StaleBanner ageLabel={staleAgeLabel} /> : null}
          <DataTable
            columns={COLUMNS}
            rows={rows}
            initialSort={{ key: "fee_adjusted_roi", dir: -1 }}
            rowKey={(r, i) => `${r.opportunity_hash}-${i}`}
          />
        </>
      )}
    </section>
  );
}
