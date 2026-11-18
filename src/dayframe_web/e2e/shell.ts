import { expect, type Page } from "@playwright/test";

/// Collects page errors and console errors; a test ends with `expect(failures).toEqual([])`.
export function watchFailures(page: Page): string[] {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });
  return failures;
}

/// Opens Settings' "Micro lifecycle" section through the wide window's sidebar (the Playwright
/// default viewport is wide), replacing whatever the main and details positions showed. The
/// controls are a section of Settings, so they are never on screen beside another section or
/// beside another panel.
export async function openLifecycle(page: Page) {
  await page.getByRole("button", { name: "Show Settings" }).click();
  await page
    .getByRole("button", { name: "Open Micro lifecycle settings" })
    .click();
  await expect(
    page.getByRole("region", { name: "Micro lifecycle" }),
  ).toBeVisible();
}

/// Opens the review micro's panel: packages are admitted disabled, so it enables the layout and
/// the review micro like a user does, then shows the review panel in the layout. Returns the
/// micro's own "Review" section (the stack of screens), inside its panel.
export async function openReview(page: Page) {
  await page.goto("/");
  await expect(
    page.getByRole("button", { name: "Open Settings", exact: true }),
  ).toBeVisible({ timeout: 60_000 });
  await openLifecycle(page);
  for (const micro of ["layout", "review"]) {
    await page
      .getByRole("button", { name: `Enable dayframe.${micro} micro` })
      .click();
    await expect(
      page.getByRole("button", { name: `Disable dayframe.${micro} micro` }),
    ).toBeVisible();
  }
  await page.getByRole("button", { name: "Show Layout" }).click();
  await page.getByRole("button", { name: "Show Review panel" }).click({
    timeout: 60_000,
  });
  const panel = page.getByRole("region", { name: "Review panel" });
  return panel.getByRole("region", { name: "Review", exact: true });
}

/// Writes `<name>.png` into `$REVIEW_SCREENSHOT_DIR` when it is set (the starting point for a GUI
/// spec); a no-op otherwise.
export async function screenshot(page: Page, name: string) {
  const directory = process.env.REVIEW_SCREENSHOT_DIR;
  if (directory) await page.screenshot({ path: `${directory}/${name}.png` });
}
