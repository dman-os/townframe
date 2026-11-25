import { expect, test } from "@playwright/test";

// The whole-sync workload in a browser: two in-memory nodes in one page, N objects, the real
// driver, timed by `performance.now()`. The page prints one `BENCH_RECORD` line per measurement
// (see `big_automata::bench`); this spec only checks they arrive and re-prints them for
// `x/bench.ts`, which records them. Nothing here is gated: browser timings are for looking at.
// There is no instruction counter in a browser, but allocation counts work and are reported.
for (const objects of [1000, 10000]) {
  test(`whole sync of ${objects} objects in the browser`, async ({ page }) => {
    const failures: string[] = [];
    page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
    page.on("console", (message) => {
      if (message.type() === "error") failures.push(`console: ${message.text()}`);
    });
    await page.goto(`/?mode=bench&n=${objects}&rtt_ms=0`);
    await expect(page.locator("#bench")).toContainText("BENCH_RECORD", {
      timeout: 10 * 60_000,
    });
    const lines = (await page.locator("#bench").innerText()).split("\n");
    for (const line of lines) {
      // `target` is filled in by x/bench.ts from the tier, not by the page.
      console.log(line);
    }
    expect(lines.some((line) => line.includes('"metric":"wall_ns"'))).toBe(true);
    expect(failures).toEqual([]);
  });
}
