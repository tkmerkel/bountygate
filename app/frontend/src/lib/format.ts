export function formatPct(x: number | null | undefined): string {
  return x === null || x === undefined ? "—" : (x * 100).toFixed(1) + "%";
}

export function formatPrice(x: number | null | undefined): string {
  return x === null || x === undefined ? "—" : x.toFixed(2);
}

export function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d.getTime()) ? iso : d.toLocaleString();
}

// Minutes elapsed since an ISO timestamp (float, clamped at 0). null if unparseable.
export function minutesSince(iso: string | null | undefined, now: number = Date.now()): number | null {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  return isNaN(t) ? null : Math.max(0, (now - t) / 60000);
}

// Compact human age like "12m", "3h", "3h 5m", "2d 4h" for a past ISO timestamp.
export function relativeAge(iso: string | null | undefined, now: number = Date.now()): string | null {
  const mins = minutesSince(iso, now);
  if (mins === null) return null;
  const m = Math.round(mins);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return m % 60 ? `${h}h ${m % 60}m` : `${h}h`;
  const d = Math.floor(h / 24);
  return h % 24 ? `${d}d ${h % 24}h` : `${d}d`;
}

// Newest (most recent) ISO timestamp among a set of rows, or null if none parse.
export function newestTimestamp(isos: (string | null | undefined)[]): string | null {
  let best: string | null = null;
  let bestMs = -Infinity;
  for (const iso of isos) {
    if (!iso) continue;
    const ms = new Date(iso).getTime();
    if (!isNaN(ms) && ms > bestMs) {
      bestMs = ms;
      best = iso;
    }
  }
  return best;
}

// "nba:2026-06-09:SAS@NYK:NYK" -> {sport, date, matchup, side}
export function parseQuestionKey(key: string | null | undefined) {
  const [sport = "", date = "", pair = "", side = ""] = (key ?? "").split(":");
  return {
    sport: sport.toUpperCase(),
    date,
    matchup: pair.includes("@") ? pair.replace("@", " @ ") : pair,
    side,
  };
}
