import { defineConfig, devices } from "@playwright/test";

// `x/dayframe-web.ts` builds the shared composition + counter guest and then serves it, so
// the server's first start includes a cold wasm32 build.
const port = Number(process.env.DAYFRAME_WEB_PORT ?? "4518");

export default defineConfig({
  testDir: "./e2e",
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
    env: { HOST: "127.0.0.1", PORT: String(port) },
    url: `http://127.0.0.1:${port}/`,
    reuseExistingServer: !process.env.CI,
    stdout: "pipe",
    stderr: "pipe",
    timeout: 20 * 60 * 1000,
  },
});