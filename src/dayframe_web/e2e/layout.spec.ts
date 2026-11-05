import { expect, test } from "@playwright/test";

// ADR 031 §13 in the browser: the layout is a Wasm package run on `wcmp`. It mounts the panels
// the viewer arranged (the counter's, a Wasm guest on its own `wcmp` instance, and the native
// sample's) through slots it redeems itself, and only a shown panel starts its provider.
test("the wcmp layout shows only the arranged panels and keeps the others mounted when one provider is disabled", async ({
  page,
}) => {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });

  await page.goto("/");
  // Home is showing: Settings is not open, so nothing else mounts a second view of the counter
  // and holds its guest live.
  await expect(
    page.getByRole("button", { name: "Open Settings", exact: true }),
  ).toBeVisible({ timeout: 60_000 });

  // Packages are admitted disabled: enable the layout and the counter like a user does.
  await page.getByText("Micro lifecycle controls").click();
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
  // has never been started: its runtime is idle while the layout is on screen.
  const showCounter = page.getByRole("button", {
    name: "Show Counter (Wasm) panel",
  });
  await expect(showCounter).toBeVisible({ timeout: 60_000 });
  const counterRuntime = page.getByText(/^dayframe\.counter runtime:/);
  await expect(counterRuntime).toHaveText("dayframe.counter runtime: Idle");
  const counterPanel = page.getByRole("region", {
    name: "Counter (Wasm) panel",
  });
  await expect(counterPanel).toBeHidden();

  // Showing the panels mounts them in place; showing the counter's starts its guest.
  await page.getByRole("button", { name: "Show Sample micro panel" }).click();
  const samplePanel = page.getByRole("region", { name: "Sample micro panel" });
  await expect(
    samplePanel.getByRole("button", {
      name: "Disable sample configuration flag",
    }),
  ).toBeVisible({ timeout: 60_000 });
  await expect(counterRuntime).toHaveText("dayframe.counter runtime: Idle");
  await showCounter.click();
  const counter = counterPanel.getByRole("button", {
    name: "Increment Wasm counter",
  });
  await expect(counter).toBeVisible({ timeout: 60_000 });
  await expect(counterRuntime).toHaveText("dayframe.counter runtime: Active");
  await expect(counter).toHaveText("0");
  for (const expected of ["1", "2"]) {
    await counter.click();
    await expect(counter).toHaveText(expected);
  }

  // Disabling the counter turns its panel into a placeholder and leaves the sample's mounted
  // and interactive; enabling it mounts the counter in the same place again.
  await page
    .getByRole("button", { name: "Disable dayframe.counter micro" })
    .click();
  await expect(counterPanel.getByRole("alert")).toContainText("unavailable");
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
  await page
    .getByRole("button", { name: "Enable dayframe.counter micro" })
    .click();
  await expect(counter).toBeVisible({ timeout: 60_000 });

  // Hiding a panel withdraws it and releases its guest.
  await page.getByRole("button", { name: "Hide Counter (Wasm) panel" }).click();
  await expect(counterPanel).toBeHidden();
  await expect(counterRuntime).toHaveText("dayframe.counter runtime: Idle");
  expect(failures).toEqual([]);
});
