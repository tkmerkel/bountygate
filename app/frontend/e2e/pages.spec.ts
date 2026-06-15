import { expect, test } from "@playwright/test";

const EID = "11111111-1111-1111-1111-111111111111";

test("fair odds page renders rows and filters", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("h2")).toContainText("The Fair Price of Everything");
  await expect(page.getByRole("cell", { name: /Boston Red Sox @ New York Yankees/ }).first()).toBeVisible();
  await expect(page.locator("tbody tr")).toHaveCount(2);
});

test("movement page renders chart and closing lines", async ({ page }) => {
  await page.goto(`/events/${EID}`);
  await expect(page.getByTestId("movement-chart")).toBeVisible();
  // a drawn price line, not just a mounted svg (blank-chart regression guard)
  await expect(page.locator("path.recharts-curve").first()).toBeVisible();
  // the closing-line marker dot from the seeded fanduel close
  await expect(page.locator(".recharts-reference-dot").first()).toBeVisible();
  await expect(page.getByRole("cell", { name: "consensus" })).toBeVisible();
});

test("cross-venue page renders linked questions", async ({ page }) => {
  await page.goto("/cross-venue");
  await expect(page.locator("h2")).toContainText("The Cross-Venue Ledger");
  await expect(page.getByRole("cell", { name: "BOS @ NYY" })).toBeVisible();
});

test("markets page filters by search", async ({ page }) => {
  await page.goto("/markets");
  await expect(page.getByRole("cell", { name: "Yankees beat Red Sox" })).toBeVisible();
  await page.getByTestId("market-search").fill("zzz-no-match");
  await expect(page.getByText("NO MARKETS MATCH.")).toBeVisible();
});

test("arbitrage page renders ledger and filters by kind", async ({ page }) => {
  await page.goto("/arbitrage");
  await expect(page.locator("h2")).toContainText("The Arbitrage Ledger");
  // seeded game book×book matchup + its 5.0% ROI cell
  await expect(page.getByRole("cell", { name: /Boston Red Sox @ New York Yankees/ })).toBeVisible();
  await expect(page.getByRole("cell", { name: "5.0%" }).first()).toBeVisible();
  // seeded arbs have a fresh last_seen_at → no stale banner
  await expect(page.getByTestId("stale-banner")).toHaveCount(0);
  // PROP filter → only the prop row survives (player visible, game matchup gone)
  await page.getByRole("button", { name: "PROP", exact: true }).click();
  await expect(page.getByText("Jayson Tatum")).toBeVisible();
  await expect(page.getByRole("cell", { name: /Boston Red Sox @ New York Yankees/ })).toHaveCount(0);
});

test("arbitrage page shows stale banner when the freshest arb is old", async ({ page }) => {
  // last_seen_at 90 minutes old → past the 30-min cutoff
  const old = new Date(Date.now() - 90 * 60_000).toISOString();
  await page.route("**/api/arbs*", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify([
        {
          opportunity_hash: "stale-arb-1", first_detected_at: old, last_seen_at: old,
          kind: "game", pairing: "book_book", event_id: EID, sport_key: "baseball_mlb",
          home_team: "New York Yankees", away_team: "Boston Red Sox", commence_time: old,
          market_segment: "h2h", player_name: null, line: null, pairing_type: "book_book",
          leg_a_kind: "book", leg_a_source: "betmgm", leg_a_outcome: "Boston Red Sox",
          leg_a_point: null, leg_a_price: 2.1, leg_a_stake: 50,
          leg_b_kind: "book", leg_b_source: "draftkings", leg_b_outcome: "New York Yankees",
          leg_b_point: null, leg_b_price: 2.1, leg_b_stake: 50,
          payout: 105, arb_ev: 5, roi: 0.05, fee_adjusted_roi: 0.05,
          hours_until_commence: 2, details: null,
        },
      ]),
    }),
  );
  await page.goto("/arbitrage");
  const banner = page.getByTestId("stale-banner");
  await expect(banner).toBeVisible();
  await expect(banner).toContainText("DATA MAY BE STALE");
  await expect(banner).toContainText(/1h.*ago/);
});

test("props page renders and filters by search", async ({ page }) => {
  await page.goto("/props");
  await expect(page.locator("h2")).toContainText("The Props Counter");
  await expect(page.getByText("Jayson Tatum").first()).toBeVisible();
  // seeds are timestamped at runtime → fresh → no stale banner
  await expect(page.getByTestId("stale-banner")).toHaveCount(0);
  // search is debounced 300ms; an unmatched term empties the table
  await page.getByPlaceholder("search…").fill("zzz-no-match");
  await expect(page.getByText(/NO PROP LINES INSIDE 24 HOURS/)).toBeVisible();
  // clearing restores the rows
  await page.getByPlaceholder("search…").fill("");
  await expect(page.getByText("Jayson Tatum").first()).toBeVisible();
});

test("props page shows stale banner when the freshest line is old", async ({ page }) => {
  // 90 minutes old → past the 30-min staleness threshold
  const old = new Date(Date.now() - 90 * 60_000).toISOString();
  await page.route("**/api/props*", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify([
        {
          event_id: EID, sport_key: "baseball_mlb", commence_time: old,
          home_team: "Home", away_team: "Away", market_key: "batter_hits",
          player_name: "Stale Player", line: 1.5, side: "over",
          bookmaker: "betmgm", decimal_price: 2.1, captured_at: old,
        },
      ]),
    }),
  );
  await page.goto("/props");
  await expect(page.getByText("Stale Player").first()).toBeVisible();
  const banner = page.getByTestId("stale-banner");
  await expect(banner).toBeVisible();
  await expect(banner).toContainText("DATA MAY BE STALE");
  await expect(banner).toContainText(/1h.*ago/);
});

test("sharpness page renders ledger and calibration chart", async ({ page }) => {
  await page.goto("/sharpness");
  await expect(page.getByText("The Sharpness Ledger")).toBeVisible();
  await expect(page.getByRole("cell", { name: "pinnacle" })).toBeVisible();
  // a drawn calibration line, not just a mounted svg (blank-chart regression guard)
  await expect(page.locator("path.recharts-curve").first()).toBeVisible();
  // recharts v3 renders Line dots as <circle class="recharts-dot recharts-line-dot">
  await expect(page.locator("circle.recharts-dot").first()).toBeVisible();
});

test("edges page renders the wire", async ({ page }) => {
  await page.goto("/edges");
  await expect(page.locator("h2")).toContainText("The Edge Wire");
  await expect(page.getByRole("cell", { name: "kalshi" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "EV" })).toBeVisible();
});

test("api failure shows error state with retry", async ({ page }) => {
  await page.route("**/api/fair-odds*", (route) => route.abort());
  await page.goto("/");
  await expect(page.getByTestId("error-state")).toBeVisible();
  await expect(page.getByRole("button", { name: "RETRY" })).toBeVisible();
});
