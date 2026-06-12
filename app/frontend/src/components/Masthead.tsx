"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV = [
  { href: "/", label: "FAIR ODDS" },
  { href: "/cross-venue", label: "CROSS-VENUE" },
  { href: "/markets", label: "MARKETS" },
];

const EXPERIMENTAL_NAV = [
  { href: "/arbitrage", label: "ARBITRAGE" },
  { href: "/props", label: "PROPS" },
  { href: "/sharpness", label: "SHARPNESS" },
  { href: "/edges", label: "EDGES" },
];

function NavLink({ href, label, pathname }: { href: string; label: string; pathname: string }) {
  const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
  return (
    <Link
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
}

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
      <nav className="mt-3 flex items-center gap-1 border-t border-ink py-1">
        {NAV.map(({ href, label }) => (
          <NavLink key={href} href={href} label={label} pathname={pathname} />
        ))}
        <span className="kicker ml-auto hidden sm:inline">PAIRED · HEDGED · BOOKED</span>
      </nav>
      <nav className="flex items-center gap-1 border-b border-ink py-0.5 text-[0.85em]">
        <span className="kicker mr-2 text-augusta-green">EXPERIMENTAL</span>
        {EXPERIMENTAL_NAV.map(({ href, label }) => (
          <NavLink key={href} href={href} label={label} pathname={pathname} />
        ))}
      </nav>
    </header>
  );
}
