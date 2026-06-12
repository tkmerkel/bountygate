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
