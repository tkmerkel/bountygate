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
            formatter={(value, name) => [value == null ? "—" : Number(value).toFixed(3), name]}
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
