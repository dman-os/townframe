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

/// Installs `window.deepQueryAll(selector)`: `querySelectorAll` that also descends into open
/// shadow roots. Every Clay presentation is mounted inside a `data-dioxus-shadow` root, so a
/// plain `document.querySelectorAll("[data-clay-presentation]")` finds none of them.
export async function installDeepQuery(page: Page) {
  await page.addInitScript(() => {
    (window as any).deepQueryAll = (selector: string): Element[] => {
      const found: Element[] = [];
      const walk = (root: Document | ShadowRoot | Element) => {
        found.push(...root.querySelectorAll(selector));
        for (const el of root.querySelectorAll("*")) {
          if (el.shadowRoot) walk(el.shadowRoot);
        }
      };
      walk(document);
      return found;
    };
  });
}

/// Enables the layout and counter micros and shows the counter's panel, so a layout presentation
/// and the counter's nested presentation (each in its own mount shadow root) are on screen. The
/// shell's own chrome and Settings are native Dioxus and mount no Clay presentation, so a spec
/// that needs one has to bring a guest up like a user does.
export async function showCounterPanel(page: Page) {
  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Enable dayframe.layout micro" })
    .click();
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await expect(
    page.getByRole("button", { name: "Disable dayframe.counter micro" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Show Layout" }).click();
  await page
    .getByRole("button", { name: "Show Counter (Wasm) panel" })
    .click({ timeout: 60_000 });
  await expect(
    page
      .getByRole("region", { name: "Counter (Wasm) panel" })
      .getByRole("button", { name: "Increment Wasm counter" }),
  ).toBeVisible({ timeout: 60_000 });
}
