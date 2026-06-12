import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  use: { baseURL: "http://127.0.0.1:3100" },
  webServer: [
    {
      command: "py -3.12 app/frontend/e2e/seed_api.py",
      cwd: "../..",
      url: "http://127.0.0.1:8765/health",
      reuseExistingServer: false,
      timeout: 60_000,
    },
    {
      command: "npm run dev -- --port 3100",
      url: "http://127.0.0.1:3100",
      reuseExistingServer: false,
      timeout: 120_000,
      env: { API_BASE_URL: "http://127.0.0.1:8765" },
    },
  ],
});
