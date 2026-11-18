import { defineConfig, devices } from "@playwright/test";

// `x/dayframe-web.ts` builds the shared composition + counter guest and then serves it, so
// the server's first start includes a cold wasm32 build.
const port = Number(process.env.DAYFRAME_WEB_PORT ?? "4518");

export default defineConfig({
  testDir: "./e2e",
  // The plain-serve spec runs under its own config (`playwright.unconfigured.config.ts`).
  testIgnore: "**/unconfigured.spec.ts",
  // A micro's first mount compiles its Wasm component in the page: the review specs wait on
  // that, so the default 30 s does not fit them.
  timeout: 90_000,
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  reporter: "list",
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    browserName: "chromium",
    ...devices["Desktop Chrome"],
    trace: "retain-on-failure",
  },
  webServer: {
    command: "deno run --allow-all ../../x/dayframe-web.ts",
    // The trapper package is what the sync-export trap spec opens (`e2e/trap.spec.ts`).
    env: {
      HOST: "127.0.0.1",
      PORT: String(port),
      DAYFRAME_WEB_TRAPPER: "1",
      // The review micro served over its sample node (`e2e/review.spec.ts`).
      DAYFRAME_WEB_REVIEW_FIXTURE: "1",
    },
    url: `http://127.0.0.1:${port}/`,
    reuseExistingServer: !process.env.CI,
    stdout: "pipe",
    stderr: "pipe",
    timeout: 20 * 60 * 1000,
  },
});
