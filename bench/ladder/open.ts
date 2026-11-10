import { expect, type Browser, type Page } from "@playwright/test";
import * as path from "node:path";
import { openLifecycle, watchFailures } from "../../src/dayframe_web/e2e/shell";

// Opens one rung's page, ready to be scrolled: shared by the ladder (X1/X4) and X5 specs.

const COMMON = path.join(import.meta.dirname, "common.js");

// r3 / r3a / r3b: the Clay stack; r3a / r3b apply the Pierre surface's batches with a direct
// receiver (X4): a JS receiver (one call per batch) or a Rust one (web-sys per op).
export const isR3 = (rung: string) => rung === "r3" || rung === "r3a" || rung === "r3b";
const directOf = (rung: string) => ({ r3a: "js", r3b: "rust" } as Record<string, string>)[rung];

const R3_BUTTONS: Record<string, string> = { syn7: "tokens-7", syn18: "tokens-18", corpus: "corpus-raw" };

export type OpenOptions = {
  rung: string;
  workload: string;
  base: string;
  baseR3: string;
  /** The micro's window profile for the R3 rungs (`pierre`, `code-view`, ...). */
  profile: string;
  startTop: number;
  viewport?: { width: number; height: number };
};

export async function openRung(
  browser: Browser,
  { rung: RUNG, workload, base: BASE, baseR3: BASE_R3, profile, startTop: START_TOP, viewport }: OpenOptions,
): Promise<{ page: Page; failures: string[] }> {
  const IS_R3 = isR3(RUNG);
  const DIRECT = directOf(RUNG);
  const context = await browser.newContext({ viewport: viewport ?? { width: 1280, height: 900 } });
  const page = await context.newPage();
  const failures = watchFailures(page);
  if (IS_R3) {
    await page.addInitScript({ path: COMMON });
    if (DIRECT) await page.addInitScript((kind) => { (window as any).__clayDirect = kind; }, DIRECT);
    if (process.env.LADDER_CAPTURE) await page.addInitScript(() => { (window as any).__clayCapture = true; });
    await page.addInitScript(() => { (window as any).__clayNoCoalesce = true; });
    await page.goto(`${BASE_R3}/`);
    await expect(page.getByRole("button", { name: "Open Settings", exact: true })).toBeVisible({ timeout: 120_000 });
    await openLifecycle(page);
    await page.getByRole("button", { name: "Enable dayframe.layout micro" }).click();
    await page.getByRole("button", { name: "Enable dayframe.pierre_spike_scroll micro" }).click();
    await expect(page.getByRole("button", { name: "Disable dayframe.pierre_spike_scroll micro" })).toBeVisible();
    await page.getByRole("button", { name: "Show Layout" }).click();
    await page.getByRole("button", { name: "Show Pierre scroll spike panel" }).click({ timeout: 120_000 });
    await expect(page.getByTestId("scroller")).toBeVisible({ timeout: 120_000 });
    await page.getByTestId("scroller").scrollIntoViewIfNeeded();
    // The micro's default window profile is `code-view`; every other profile is a button.
    if (profile !== "code-view") await page.getByTestId(`profile-${profile}`).click();
    await page.getByTestId(R3_BUTTONS[workload]).click();
    await page.evaluate(() => {
      const L = (window as any).Ladder;
      const win = () => document.querySelector('[data-testid="window"]')!;
      L.rung = {
        name: "r3",
        scroller: document.querySelector('[data-testid="scroller"]'),
        mutationRoot: document.querySelector('[data-testid="scroller"]'),
        censusRoot: win(),
        windowStart: () => Number(win().getAttribute("data-value")),
      };
    });
  } else {
    const url = {
      "r0": `${BASE}/r0/?workload=${workload}`,
      "r1-dom": `${BASE}/r1/?variant=dom&workload=${workload}`,
      "r1-html": `${BASE}/r1/?variant=html&workload=${workload}`,
      "r1-static": `${BASE}/r1/?variant=static&workload=${workload}`,
      "r2": `${BASE}/r2/?workload=${workload}`,
    }[RUNG];
    if (!url) throw new Error(`unknown rung ${RUNG}`);
    await page.goto(url);
    await page.waitForFunction(() => (window as any).__ready === true, null, { timeout: 120_000 });
  }
  // Start away from the file top (the window is clamped there) and settle.
  await page.evaluate(async (top) => {
    const L = (window as any).Ladder;
    L.rung.scroller.scrollTop = top;
    for (let i = 0; i < 6; i++) await L.nextFrame();
  }, START_TOP);
  return { page, failures };
}
