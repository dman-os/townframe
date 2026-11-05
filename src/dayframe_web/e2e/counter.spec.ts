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
  // Home lists Settings; opening it lists the sections, and a section is the details.
  await page
    .getByRole("button", { name: "Open Settings", exact: true })
    .click({ timeout: 60_000 });
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
  // The section is listed from boot and unavailable while the micro is disabled; opening it
  // now mounts the counter's view as the details, and its slot re-redeems by itself once the
  // supervisor records the micro enabled.
  await openCounter.click();
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
// durable row whose action is a lazy reference to the guest's `increment` call. With home
// showing nothing holds the guest, yet the pinned toolbar still lists the button; pressing it
// starts the guest on `wcmp` for the length of the call, and the new count shows once the
// counter's section is opened.
test("the toolbar button increments a stopped wcmp counter", async ({
  page,
}) => {
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

  // Home is showing, so no section mounts the counter.
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

// The shell's navigation in a real browser (ADR 032): home lists Settings, Settings lists its
// sections, a section is the details, and back unwinds the stack on a phone-width window, where
// the details come over the list, as on a desktop one, where they sit beside it. The browser's
// own back button is the platform back gesture.
test("home, Settings and a section unwind with back at a compact and a wide width", async ({
  page,
}) => {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });

  const sample = page.getByRole("button", {
    name: "Open Sample micro settings",
  });
  const flag = page.getByRole("button", {
    name: "Disable sample configuration flag",
  });
  const home = page.getByRole("region", { name: "Home" });

  // Compact: one pane at a time.
  await page.setViewportSize({ width: 420, height: 800 });
  await page.goto("/");
  await expect(home).toBeVisible({ timeout: 60_000 });
  await page
    .getByRole("button", { name: "Open Settings", exact: true })
    .click();
  await expect(home).toBeHidden();
  await sample.click();
  await expect(flag).toBeVisible();
  // The list is covered while the details show.
  await expect(sample).toBeHidden();
  await page.getByRole("button", { name: "Back", exact: true }).click();
  await expect(flag).toBeHidden();
  await expect(sample).toBeVisible();

  // The browser's back button is the platform back: it returns to home.
  await page.goBack();
  await expect(home).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Open sidebar" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Open sidebar" }).click();
  await expect(page.getByRole("dialog", { name: "Sidebar" })).toBeVisible();
  // The scrim is the strip the bottom sheet leaves uncovered at the top of the frame.
  await page
    .getByRole("button", { name: "Dismiss Sidebar" })
    .click({ position: { x: 10, y: 5 } });
  await expect(page.getByRole("dialog", { name: "Sidebar" })).toBeHidden();

  // Wide: the sidebar and the list sit beside the details.
  await page.setViewportSize({ width: 1280, height: 800 });
  await expect(page.getByRole("navigation", { name: "Sidebar" })).toBeVisible();
  await page
    .getByRole("button", { name: "Open Settings", exact: true })
    .click();
  await sample.click();
  await expect(flag).toBeVisible();
  await expect(sample).toBeVisible();
  await page.getByRole("button", { name: "Back", exact: true }).click();
  await expect(flag).toBeHidden();
  await expect(sample).toBeVisible();
  await page.getByRole("button", { name: "Show Home" }).click();
  await expect(home).toBeVisible();
  expect(failures).toEqual([]);
});
