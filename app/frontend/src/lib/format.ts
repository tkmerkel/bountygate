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
