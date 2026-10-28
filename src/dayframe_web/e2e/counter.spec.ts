import { expect, test } from "@playwright/test";

// ADR 018 migration step 7 in the browser: the shared composition on `dayframe_web` activates
// the real counter guest through `wcmp` over Chromium's `WebAssembly` engine, mounts it through
// `clay_dioxus` on `dioxus-web`, and the click round trip reaches the guest. A `first_poll`
// panic surfaces as a page error, so the test fails on any page or console error.
test("the composed wcmp counter mounts and dispatches clicks in the browser", async ({
  page,
}) => {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });

  await page.goto("/");
  const openCounter = page.getByRole("button", {
    name: "Open Counter (Wasm) settings",
  });
  await expect(openCounter).toBeVisible({ timeout: 60_000 });
  await openCounter.click();

  const counter = page.getByRole("button", { name: "Increment Wasm counter" });
  await expect(counter).toBeVisible({ timeout: 60_000 });
  await expect(counter).toHaveText("0");
  for (const expected of ["1", "2", "3"]) {
    await counter.click();
    await expect(counter).toHaveText(expected);
  }
  expect(failures).toEqual([]);
});
