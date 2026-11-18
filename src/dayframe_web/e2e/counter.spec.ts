import { expect, test } from "@playwright/test";
import { openLifecycle, watchFailures } from "./shell";

// ADR 018 migration step 7 in the browser: the shared composition on `dayframe_web` activates
// the real counter guest through `wcmp` over Chromium's `WebAssembly` engine, mounts it through
// `clay_dioxus` on `dioxus-web`, and the click round trip reaches the guest. A `first_poll`
// panic surfaces as a page error, so the test fails on any page or console error.
test("the composed wcmp counter mounts and dispatches clicks in the browser", async ({
  page,
}) => {
  const failures = watchFailures(page);

  await page.goto("/");
  // Home lists Settings; opening it lists the sections, and a section is the details.
  await page
    .getByRole("button", { name: "Open Settings", exact: true })
    .click({ timeout: 60_000 });
  const openCounter = page.getByRole("button", {
    name: "Open Counter (Wasm) settings",
  });
  await expect(openCounter).toBeVisible({ timeout: 60_000 });

  // A package from the index is admitted disabled: enable it through the lifecycle section,
  // which writes its install datum, before its section can start the guest.
  await page
    .getByRole("button", { name: "Open Micro lifecycle settings" })
    .click();
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await expect(
    page.getByRole("button", { name: "Disable dayframe.counter micro" }),
  ).toBeVisible();
  // The open section is the list's current row.
  await expect(
    page.getByRole("button", { name: "Open Micro lifecycle settings" }),
  ).toHaveAttribute("aria-current", "true");
  await expect(openCounter).not.toHaveAttribute("aria-current", "true");

  // The section mounts the counter's view as the details, replacing the lifecycle section.
  await openCounter.click();
  await expect(openCounter).toHaveAttribute("aria-current", "true");
  const counter = page.getByRole("button", { name: "Increment Wasm counter" });
  await expect(counter).toBeVisible({ timeout: 60_000 });
  await expect(counter).toHaveText("0");
  for (const expected of ["1", "2", "3"]) {
    await counter.click();
    await expect(counter).toHaveText(expected);
  }

  // Disabling the micro withdraws its mounted section (the lifecycle section replaces it in
  // the details position, so the section is reopened to see that); its manifest row stays
  // listed, and re-enabling it remounts the section on a new incarnation that kept the count.
  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Disable dayframe.counter micro" })
    .click();
  await expect(
    page.getByRole("button", { name: "Enable dayframe.counter micro" }),
  ).toBeVisible();
  await expect(counter).toBeHidden();
  await openCounter.click();
  await expect(counter).toBeHidden();
  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await openCounter.click();
  await expect(counter).toBeVisible({ timeout: 60_000 });
  await expect(counter).toHaveText("3");
  expect(failures).toEqual([]);
});

// The install datum lives in the durable document store (IndexedDB), so a disabled micro is
// still disabled after the page reloads.
test("a disabled counter stays disabled across a reload", async ({ page }) => {
  const failures = watchFailures(page);

  await page.goto("/");
  await openLifecycle(page);
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
  await openLifecycle(page);
  await expect(
    page.getByRole("button", { name: "Enable dayframe.counter micro" }),
  ).toBeVisible({ timeout: 60_000 });
  expect(failures).toEqual([]);
});

// The counter package contributes an Increment button to the toolbar through its manifest: a
// durable row whose action is a lazy reference to the guest's `increment` call. The toolbar is a
// panel in the shell's sidebar, so with home showing nothing holds the guest, yet the sidebar
// still lists the button; pressing it starts the guest on `wcmp` for the length of the call, and
// the new count shows once the counter's section is opened.
test("the toolbar button increments a stopped wcmp counter", async ({
  page,
}) => {
  const failures = watchFailures(page);

  await page.goto("/");
  const increment = page
    .getByRole("toolbar", { name: "Toolbar" })
    .getByRole("button", { name: "Increment", exact: true });
  await expect(increment).toBeVisible({ timeout: 60_000 });

  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await expect(
    page.getByRole("button", { name: "Disable dayframe.counter micro" }),
  ).toBeVisible();

  // Home is showing, so no section mounts the counter.
  await page.getByRole("button", { name: "Show Home" }).click();
  await expect(
    page.getByRole("button", { name: "Increment Wasm counter" }),
  ).toBeHidden();
  await expect(increment).toBeVisible();
  await increment.click();

  await page
    .getByRole("button", { name: "Open Settings", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Open Counter (Wasm) settings" })
    .click();
  await expect(
    page.getByRole("button", { name: "Increment Wasm counter" }),
  ).toHaveText("1", { timeout: 60_000 });
  expect(failures).toEqual([]);
});
