import { expect, test } from "@playwright/test";
import { openLifecycle, watchFailures } from "./shell";

// ADR 031 §13 in the browser: the layout is a Wasm package run on `wcmp`. It is a list panel of
// the shell, and mounts the panels the viewer arranged (the counter's, a Wasm guest on its own
// `wcmp` instance, and the native sample's) through slots it redeems itself; only a shown panel
// starts its provider. The micro lifecycle controls are a Settings section, so they and the
// layout are never on screen together: enabling and disabling happen by leaving the layout, and
// the desktop e2e (`a_mounted_panel_is_interactive_and_disabling_one_provider_leaves_the_others_mounted`)
// is where a provider leaving beside a mounted sibling is observed live.
test("the wcmp layout shows only the arranged panels and a disabled provider leaves a placeholder", async ({
  page,
}) => {
  const failures = watchFailures(page);

  await page.goto("/");
  await expect(
    page.getByRole("button", { name: "Open Settings", exact: true }),
  ).toBeVisible({ timeout: 60_000 });

  // Packages are admitted disabled: enable the layout and the counter like a user does.
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

  // The layout lists the installed panels, but nothing is arranged yet, so the counter's guest
  // has never been started: its runtime is idle while no layout shows it.
  await page.getByRole("button", { name: "Show Layout" }).click();
  const showCounter = page.getByRole("button", {
    name: "Show Counter (Wasm) panel",
  });
  await expect(showCounter).toBeVisible({ timeout: 60_000 });
  const counterPanel = page.getByRole("region", {
    name: "Counter (Wasm) panel",
  });
  await expect(counterPanel).toBeHidden();
  await openLifecycle(page);
  await expect(page.getByText(/^dayframe\.counter runtime:/)).toHaveText(
    "dayframe.counter runtime: Idle",
  );
  await page.getByRole("button", { name: "Show Layout" }).click();

  // Showing the panels mounts them in place; showing the counter's starts its guest.
  await page.getByRole("button", { name: "Show Sample micro panel" }).click();
  const samplePanel = page.getByRole("region", { name: "Sample micro panel" });
  await expect(
    samplePanel.getByRole("button", {
      name: "Disable sample configuration flag",
    }),
  ).toBeVisible({ timeout: 60_000 });
  await showCounter.click();
  const counter = counterPanel.getByRole("button", {
    name: "Increment Wasm counter",
  });
  await expect(counter).toBeVisible({ timeout: 60_000 });
  await expect(counter).toHaveText("0");
  for (const expected of ["1", "2"]) {
    await counter.click();
    await expect(counter).toHaveText(expected);
  }

  // Disabling the counter turns its panel into a placeholder and leaves the sample's mounted
  // and interactive; enabling it mounts the counter in the same place again. The arrangement
  // is a stored datum, so it survives leaving the layout.
  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Disable dayframe.counter micro" })
    .click();
  // The toggle is inert until the datum is stored; leaving earlier would race the write.
  await expect(
    page.getByRole("button", { name: "Enable dayframe.counter micro" }),
  ).toBeEnabled();
  await page.getByRole("button", { name: "Show Layout" }).click();
  await expect(counterPanel.getByRole("alert")).toContainText("unavailable", {
    timeout: 60_000,
  });
  await expect(counter).toBeHidden();
  const flag = samplePanel.getByRole("button", {
    name: /sample configuration flag$/,
  });
  await expect(flag).toHaveAttribute(
    "aria-label",
    "Disable sample configuration flag",
  );
  await flag.click();
  await expect(flag).toHaveAttribute(
    "aria-label",
    "Enable sample configuration flag",
  );
  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await page.getByRole("button", { name: "Show Layout" }).click();
  await expect(counter).toBeVisible({ timeout: 60_000 });
  await expect(counter).toHaveText("2");

  // Hiding a panel withdraws it.
  await page.getByRole("button", { name: "Hide Counter (Wasm) panel" }).click();
  await expect(counterPanel).toBeHidden();
  expect(failures).toEqual([]);
});
