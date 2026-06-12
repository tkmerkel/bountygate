"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV = [
  { href: "/", label: "FAIR ODDS" },
  { href: "/cross-venue", label: "CROSS-VENUE" },
  { href: "/markets", label: "MARKETS" },
];

export function Masthead() {
  const pathname = usePathname();
  const today = new Date().toLocaleDateString("en-US", {
    weekday: "long", year: "numeric", month: "long", day: "numeric",
  });

  return (
    <header className="pt-4">
      <div className="flex items-end justify-between gap-4">
        <div className="kicker hidden w-48 sm:block">
          VOL. MMXXVI
          <br />
          FILED FROM REDMOND, WA
        </div>
        <h1 className="grow text-center font-serif text-[clamp(38px,4.2vw,60px)] italic">
          The Daily Hedge
        </h1>
        <div className="kicker hidden w-48 text-right sm:block">
          {today.toUpperCase()}
          <br />
          PRICE FREE TO PATRONS
        </div>
      </div>
      <nav className="mt-3 flex items-center gap-1 border-y border-ink py-1">
        {NAV.map(({ href, label }) => {
          const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={`pixel px-3 py-0.5 ${
                active
                  ? "bg-augusta-green text-crisp shadow-[inset_0_-2px_0_var(--masters-yellow)]"
                  : "text-ink hover:bg-inset"
              }`}
            >
              {label}
            </Link>
          );
        })}
        <span className="kicker ml-auto hidden sm:inline">PAIRED · HEDGED · BOOKED</span>
      </nav>
    </header>
  );
}
