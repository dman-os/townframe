import { defineConfig, devices } from "@playwright/test";

// The ladder specs drive static pages that `bench/ladder/serve.ts` serves (ports below); no
// webServer entry: the harness starts and stops its servers by PID.
export default defineConfig({
  testDir: ".",
  testMatch: "ladder.spec.ts",
  fullyParallel: false,
  workers: 1,
  reporter: "list",
  timeout: 20 * 60_000,
  use: { browserName: "chromium", ...devices["Desktop Chrome"], viewport: { width: 1280, height: 900 } },
});
