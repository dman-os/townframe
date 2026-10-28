import { expect, test } from "@playwright/test";

// ADR 018 migration step 1 in the browser: the real counter component compiled by Chromium's
// `WebAssembly` engine through `wcmp`, mounted and driven through `clay_dioxus` on `dioxus-web`.
// A guest call that suspends panics in `first_poll`, and a panic in the page's module surfaces
// as a page error, so the test fails on any page error or console error rather than waiting.
test("the wcmp-hosted counter mounts and dispatches clicks in the browser", async ({
  page,
}) => {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });

  await page.goto("/");
  const status = page.locator("#spike-status");
  await expect(status).not.toHaveText("loading", { timeout: 60_000 });
  await expect(status).toHaveText("ready");

  const counter = page
    .locator("#spike-surface")
    .getByRole("button", { name: "Increment Wasm counter" });
  await expect(counter).toHaveText("0");
  for (const expected of ["1", "2", "3"]) {
    await counter.click();
    await expect(counter).toHaveText(expected);
  }
  expect(failures).toEqual([]);
});
