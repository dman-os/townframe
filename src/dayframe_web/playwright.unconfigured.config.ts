import { defineConfig } from "@playwright/test";
import base from "./playwright.config";

// A plain serve: no `DAYFRAME_WEB_REVIEW_FIXTURE`, so the review micro is the shipped guest and
// reads "Unconfigured". Its own port, so it never reuses the fixture server of the main config.
const port = Number(process.env.DAYFRAME_WEB_PORT ?? "4518") + 1;

export default defineConfig({
  ...base,
  testIgnore: undefined,
  testMatch: "**/unconfigured.spec.ts",
  use: { ...base.use, baseURL: `http://127.0.0.1:${port}` },
  webServer: {
    ...base.webServer,
    env: { HOST: "127.0.0.1", PORT: String(port) },
    url: `http://127.0.0.1:${port}/`,
  },
});
