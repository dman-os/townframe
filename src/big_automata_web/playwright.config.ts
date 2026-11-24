import { defineConfig, devices } from "@playwright/test";

// `x/big-automata-web.ts` builds the wasm32 host and the native peer (`big_automata_iroh`'s
// `web_fixture` example: an in-process relay and a node behind it), starts the peer, and serves
// the page with the peer's address as `/fixture.json`. The first start includes a cold wasm32
// build.
const port = Number(process.env.BIG_AUTOMATA_WEB_PORT ?? "4519");

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
    command: "deno run --allow-all ../../x/big-automata-web.ts",
    env: { HOST: "127.0.0.1", PORT: String(port) },
    url: `http://127.0.0.1:${port}/`,
    reuseExistingServer: !process.env.CI,
    stdout: "pipe",
    stderr: "pipe",
    timeout: 20 * 60 * 1000,
  },
});
