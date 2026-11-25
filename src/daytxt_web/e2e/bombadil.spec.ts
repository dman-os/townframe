import { spawn } from "node:child_process";
import { once } from "node:events";
import path from "node:path";
import { chromium, expect, test } from "@playwright/test";

// Bombadil (property-based UI testing, ADR 032 section 9) explores the shell by clicking, typing
// and going back, and checks the invariants in `bombadil/shell.ts` in every state it reaches.
// Playwright is only the harness: its `webServer` serves the build, and its pinned Chromium (the
// flake's browser farm) is the browser Bombadil drives. Tagged so `pnpm run e2e` skips it and
// `pnpm run bombadil` runs it alone.
//
// Bombadil has no seed option. A run is reproduced from its trace, which lands in the test's
// output directory (uploaded as a CI artifact): `bombadil browser test --reproduce <dir> ...`.
const timeLimit = process.env.BOMBADIL_TIME_LIMIT ?? "60s";
const bombadil = path.join(
  import.meta.dirname,
  "../node_modules/.bin/bombadil",
);
const specification = path.join(import.meta.dirname, "../bombadil/shell.ts");

// The phone and the desktop arrangement differ structurally (one pane at a time versus
// side by side), so each width is its own exploration.
for (const width of [420, 1024]) {
  test(`@bombadil the shell keeps its invariants at ${width}px`, async ({
    baseURL,
  }, testInfo) => {
    // The exploration runs for `timeLimit`; the rest of the budget is browser start and the
    // shell's cold load.
    testInfo.setTimeout(5 * 60 * 1000);
    const output = testInfo.outputPath("bombadil");
    const child = spawn(
      bombadil,
      [
        "browser",
        "test",
        "--headless",
        "--no-sandbox",
        "--exit-on-violation",
        "--width",
        String(width),
        "--height",
        "800",
        "--time-limit",
        timeLimit,
        "--output-path",
        output,
        baseURL ?? "",
        specification,
      ],
      {
        env: { ...process.env, CHROME: chromium.executablePath() },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let log = "";
    child.stdout.on("data", (chunk) => {
      log += chunk;
    });
    child.stderr.on("data", (chunk) => {
      log += chunk;
    });
    const [code] = await once(child, "exit");
    await testInfo.attach("bombadil.log", { body: log });
    // 0: ran to the time limit, 2: a property was violated (the log names it and how to
    // reproduce), anything else: Bombadil itself failed.
    expect(code, log.slice(-6000)).toBe(0);
  });
}
