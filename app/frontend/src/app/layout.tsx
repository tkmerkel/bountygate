import type { Metadata } from "next";
import { Merriweather, Playfair_Display, VT323 } from "next/font/google";
import { Masthead } from "@/components/Masthead";
import "./globals.css";

const playfair = Playfair_Display({
  subsets: ["latin"],
  style: ["normal", "italic"],
  variable: "--font-playfair",
});
const merriweather = Merriweather({
  subsets: ["latin"],
  weight: ["300", "400", "700"],
  style: ["normal", "italic"],
  variable: "--font-merriweather",
});
const vt323 = VT323({
  subsets: ["latin"],
  weight: "400",
  variable: "--font-vt323",
});

export const metadata: Metadata = {
  title: "The Daily Hedge — BountyGate",
  description: "Fair odds, line movement, and cross-venue prices for MLB, NBA, and NHL.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${playfair.variable} ${merriweather.variable} ${vt323.variable}`}>
      <body className="min-h-screen">
        <div className="mx-auto max-w-[1280px] px-4">
          <Masthead />
          <main className="py-6">{children}</main>
          <footer className="wsj-rule mb-2" />
          <footer className="kicker pb-6">
            BOUNTYGATE · ANALYTICS ONLY · NOT BETTING ADVICE
          </footer>
        </div>
      </body>
    </html>
  );
}
