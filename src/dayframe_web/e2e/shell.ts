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
