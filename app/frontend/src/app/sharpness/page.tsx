"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { CalibrationRow, SharpnessRow, useApi } from "@/lib/api";
import { formatPct } from "@/lib/format";
import { Column, DataTable } from "@/components/DataTable";
import { Empty, ErrorState, Loading } from "@/components/states";

// Copied from src/components/MovementChart.tsx (not exported there)
const SERIES_COLORS = ["#0B5B40", "#B0211A", "#1A1A1A", "#106B4D", "#F1C40F", "#6B6B66", "#084632", "#FFD84A"];

const EMPTY_NOTE = "ACCUMULATING — SCORING BEGINS AFTER FIRST SETTLED GAMES.";

// ---- (a) The Sharpness Ledger ------------------------------------------------

const SHARP_COLUMNS: Column<SharpnessRow>[] = [
  {
    key: "venue",
    label: "VENUE",
    sortValue: (r) => r.venue_key ?? "",
    render: (r) => <span className="pixel">{r.venue_key ?? "—"}</span>,
  },
  {
    key: "sport",
    label: "SPORT",
    sortValue: (r) => r.sport_key ?? "",
    render: (r) => <span className="kicker">{r.sport_key ?? "—"}</span>,
  },
  {
    key: "window",
    label: "WINDOW",
    sortValue: (r) => r.score_window ?? "",
    render: (r) => <span className="kicker">{r.score_window ?? "—"}</span>,
  },
  {
    key: "n_games",
    label: "GAMES",
    numeric: true,
    sortValue: (r) => r.n_games ?? 0,
    render: (r) => <span className="pixel">{r.n_games ?? "—"}</span>,
  },
  {
    key: "brier",
    label: "BRIER",
    numeric: true,
    sortValue: (r) => r.brier,
    render: (r) => <span className="pixel">{r.brier === null || r.brier === undefined ? "—" : r.brier.toFixed(4)}</span>,
  },
  {
    key: "logloss",
    label: "LOG LOSS",
    numeric: true,
    sortValue: (r) => r.logloss,
    render: (r) => (
      <span className="pixel">{r.logloss === null || r.logloss === undefined ? "—" : r.logloss.toFixed(4)}</span>
    ),
  },
  {
    key: "avg_clv",
    label: "AVG CLV",
    numeric: true,
    sortValue: (r) => r.avg_clv,
    render: (r) => (
      <span className={`pixel ${(r.avg_clv ?? 0) >= 0 ? "ledger-pos" : "ledger-neg"}`}>{formatPct(r.avg_clv)}</span>
    ),
  },
];

function SharpnessLedger() {
  const { data, error, loading, reload } = useApi<SharpnessRow[]>("/api/sharpness");
  const rows = data ?? [];

  return (
    <div className="card-hard p-4">
      <div className="wsj-rule mb-4 pb-2">
        <div className="kicker">VENUE FORECAST ACCURACY · LOWER BRIER IS SHARPER</div>
        <h2 className="font-serif text-3xl italic">The Sharpness Ledger</h2>
      </div>
      {loading ? (
        <Loading />
      ) : error ? (
        <ErrorState message={error} onRetry={reload} />
      ) : rows.length === 0 ? (
        <Empty note={EMPTY_NOTE} />
      ) : (
        <DataTable
          columns={SHARP_COLUMNS}
          rows={rows}
          initialSort={{ key: "brier", dir: 1 }}
          rowKey={(r, i) => `${r.venue_key}-${r.sport_key}-${r.score_window}-${i}`}
        />
      )}
    </div>
  );
}

// ---- (b) Calibration ---------------------------------------------------------

const CAL_COLUMNS: Column<CalibrationRow>[] = [
  {
    key: "bucket",
    label: "BUCKET",
    numeric: true,
    sortValue: (r) => r.prob_bucket,
    render: (r) => (
      <span className="pixel">{r.prob_bucket === null || r.prob_bucket === undefined ? "—" : String(r.prob_bucket)}</span>
    ),
  },
  {
    key: "n",
    label: "N",
    numeric: true,
    sortValue: (r) => r.n ?? 0,
    render: (r) => <span className="pixel">{r.n ?? "—"}</span>,
  },
  {
    key: "predicted",
    label: "PREDICTED",
    numeric: true,
    sortValue: (r) => r.predicted_mean,
    render: (r) => <span className="pixel">{formatPct(r.predicted_mean)}</span>,
  },
  {
    key: "realized",
    label: "REALIZED",
    numeric: true,
    sortValue: (r) => r.realized_rate,
    render: (r) => <span className="pixel">{formatPct(r.realized_rate)}</span>,
  },
];

function CalibrationSection() {
  const { data, error, loading, reload } = useApi<CalibrationRow[]>("/api/calibration");
  const rows = useMemo(() => data ?? [], [data]);

  const [source, setSource] = useState("");
  const [sport, setSport] = useState("");

  const sourceOptions = useMemo(() => {
    const vals = Array.from(new Set(rows.map((r) => r.source).filter((s): s is string => !!s))).sort();
    return ["", ...vals];
  }, [rows]);

  const sportOptions = useMemo(() => {
    const vals = Array.from(new Set(rows.map((r) => r.sport_key).filter((s): s is string => !!s))).sort();
    return ["", ...vals];
  }, [rows]);

  // Filtered + null-safe points for the chart and the table.
  const points = useMemo(() => {
    return rows
      .filter((r) => (!source || r.source === source) && (!sport || r.sport_key === sport))
      .filter((r) => r.predicted_mean !== null && r.realized_rate !== null)
      .sort((a, b) => (a.predicted_mean ?? 0) - (b.predicted_mean ?? 0));
  }, [rows, source, sport]);

  // Group points by source; each group sorted by predicted_mean (already sorted above).
  const series = useMemo(() => {
    const bySource = new Map<string, CalibrationRow[]>();
    for (const r of points) {
      const key = r.source ?? "(unknown)";
      if (!bySource.has(key)) bySource.set(key, []);
      bySource.get(key)!.push(r);
    }
    return [...bySource.entries()]
      .map(([src, pts]) => ({ source: src, points: pts }))
      .sort((a, b) => a.source.localeCompare(b.source));
  }, [points]);

  const fmtTick = (v: number) => `${Math.round(v * 100)}%`;

  return (
    <div className="card-hard p-4">
      <div className="wsj-rule mb-4 flex flex-wrap items-end justify-between gap-3 pb-2">
        <div>
          <div className="kicker">PREDICTED VS REALIZED · THE 45° LINE IS PERFECT CALIBRATION</div>
          <h2 className="font-serif text-3xl italic">Calibration</h2>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-1">
            <span className="kicker">SOURCE</span>
            <select value={source} onChange={(e) => setSource(e.target.value)} className="pixel bevel-in px-2 py-0.5">
              {sourceOptions.map((o) => (
                <option key={o || "ALL"} value={o}>
                  {o ? o : "ALL"}
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-1">
            <span className="kicker">SPORT</span>
            <select value={sport} onChange={(e) => setSport(e.target.value)} className="pixel bevel-in px-2 py-0.5">
              {sportOptions.map((o) => (
                <option key={o || "ALL"} value={o}>
                  {o ? o : "ALL"}
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
      ) : points.length === 0 ? (
        <Empty note={EMPTY_NOTE} />
      ) : (
        <>
          <ResponsiveContainer width="100%" height={360}>
            <LineChart margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
              <CartesianGrid strokeDasharray="2 3" stroke="rgba(0,0,0,0.15)" />
              <XAxis
                dataKey="predicted_mean"
                type="number"
                domain={[0, 1]}
                tickFormatter={fmtTick}
                stroke="#1A1A1A"
                tick={{ fontFamily: "var(--font-vt323)", fontSize: 14 }}
              />
              <YAxis
                dataKey="realized_rate"
                type="number"
                domain={[0, 1]}
                tickFormatter={fmtTick}
                stroke="#1A1A1A"
                tick={{ fontFamily: "var(--font-vt323)", fontSize: 14 }}
                width={48}
              />
              <Tooltip
                labelFormatter={(v) => `PREDICTED ${fmtTick(Number(v))}`}
                formatter={(value) => [value == null ? "—" : fmtTick(Number(value)), "REALIZED"]}
              />
              {/* y=x ideal calibration line, segment (0,0) -> (1,1) */}
              {series.length >= 2 && series.length <= 10 && (
                <Legend wrapperStyle={{ fontFamily: "var(--font-vt323)", fontSize: 14 }} />
              )}
              <ReferenceLine
                segment={[
                  { x: 0, y: 0 },
                  { x: 1, y: 1 },
                ]}
                stroke="#6B6B66"
                strokeDasharray="4 4"
                ifOverflow="extendDomain"
              />
              {series.map((s, i) => (
                <Line
                  key={s.source}
                  data={s.points}
                  dataKey="realized_rate"
                  name={s.source}
                  stroke={SERIES_COLORS[i % SERIES_COLORS.length]}
                  dot={{ r: 3, fill: SERIES_COLORS[i % SERIES_COLORS.length] }}
                  strokeWidth={1.5}
                  isAnimationActive={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
          <div className="kicker mb-3 mt-1">DASHED · IDEAL (PREDICTED = REALIZED)</div>
          <DataTable
            columns={CAL_COLUMNS}
            rows={points}
            initialSort={{ key: "bucket", dir: 1 }}
            rowKey={(r, i) => `${r.source}-${r.sport_key}-${r.prob_bucket}-${i}`}
          />
        </>
      )}
    </div>
  );
}

export default function SharpnessPage() {
  return (
    <section className="flex flex-col gap-6">
      <SharpnessLedger />
      <CalibrationSection />
    </section>
  );
}
