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

  // A package from the index is admitted disabled: enable it through the lifecycle panel,
  // which writes its install datum, before its section can start the guest.
  await page.getByText("Micro lifecycle controls").click();
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await expect(
    page.getByRole("button", { name: "Disable dayframe.counter micro" }),
  ).toBeVisible();
  // Settings already shows the counter's section from boot, "unavailable" while the micro is
  // disabled. Nothing is reopened: the section's slot re-redeems by itself once the supervisor
  // records the enabled micro, so this waits only on the mounted counter below.

  const counter = page.getByRole("button", { name: "Increment Wasm counter" });
  await expect(counter).toBeVisible({ timeout: 60_000 });
  await expect(counter).toHaveText("0");
  for (const expected of ["1", "2", "3"]) {
    await counter.click();
    await expect(counter).toHaveText(expected);
  }

  // Disabling the micro withdraws its mounted section; its manifest row stays listed, and
  // re-enabling it remounts the still-open section on a new incarnation. The lifecycle panel is still
  // open from enabling the counter above.
  await page
    .getByRole("button", { name: "Disable dayframe.counter micro" })
    .click();
  await expect(counter).toBeHidden();
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await expect(counter).toBeVisible({ timeout: 60_000 });
  expect(failures).toEqual([]);
});

// The install datum lives in the durable document store (IndexedDB), so a disabled micro is
// still disabled after the page reloads.
test("a disabled counter stays disabled across a reload", async ({ page }) => {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });

  await page.goto("/");
  await page.getByText("Micro lifecycle controls").click();
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await page
    .getByRole("button", { name: "Disable dayframe.counter micro" })
    .click();
  // The toggle is inert until the datum reached IndexedDB; a reload before that would race
  // the write, since a reload does not run the persister's shutdown drain.
  await expect(
    page.getByRole("button", { name: "Enable dayframe.counter micro" }),
  ).toBeEnabled();

  await page.reload();
  await page.getByText("Micro lifecycle controls").click();
  await expect(
    page.getByRole("button", { name: "Enable dayframe.counter micro" }),
  ).toBeVisible({ timeout: 60_000 });
  expect(failures).toEqual([]);
});

// The counter package contributes an Increment button to the toolbar through its manifest: a
// durable row whose action is a lazy reference to the guest's `increment` call. With Settings
// closed nothing holds the guest, yet the pinned toolbar still lists the button; pressing it
// starts the guest on `wcmp` for the length of the call, and the new count shows once the
// counter's section is mounted again.
test("the toolbar button increments a stopped wcmp counter", async ({ page }) => {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });

  await page.goto("/");
  const increment = page
    .getByRole("toolbar", { name: "Toolbar" })
    .getByRole("button", { name: "Increment", exact: true });
  await expect(increment).toBeVisible({ timeout: 60_000 });

  await page.getByText("Micro lifecycle controls").click();
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await expect(
    page.getByRole("button", { name: "Disable dayframe.counter micro" }),
  ).toBeVisible();

  // Close Settings: the counter's section unmounts, and the toolbar stays.
  await page.getByRole("button", { name: "Close Settings" }).click();
  await expect(
    page.getByRole("button", { name: "Increment Wasm counter" }),
  ).toBeHidden();
  await expect(increment).toBeVisible();
  await increment.click();

  await page.getByRole("button", { name: "Open Settings" }).click();
  await expect(
    page.getByRole("button", { name: "Increment Wasm counter" }),
  ).toHaveText("1", { timeout: 60_000 });
  expect(failures).toEqual([]);
});
