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
  await expect(page.locator("svg.recharts-surface").first()).toBeVisible();
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

test("api failure shows error state with retry", async ({ page }) => {
  await page.route("**/api/fair-odds*", (route) => route.abort());
  await page.goto("/");
  await expect(page.getByTestId("error-state")).toBeVisible();
  await expect(page.getByRole("button", { name: "RETRY" })).toBeVisible();
});
