import { expect, test } from "@playwright/test";
import { watchFailures } from "./shell";

// The shell's navigation in a real browser (ADR 032): home lists Settings, Settings lists its
// sections, a section is the details, and back unwinds the stack on a phone-width window, where
// the details come over the list, as on a desktop one, where they sit beside it. The browser's
// own back button is the platform back gesture.
test("home, Settings and a section unwind with back at a compact and a wide width", async ({
  page,
}) => {
  const failures = watchFailures(page);

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
  // The shell is the whole frame: no heading above it, and home spans the window edge to edge.
  await expect(page.getByRole("heading", { level: 1 })).toHaveCount(0);
  expect(await home.boundingBox()).toMatchObject({
    x: 0,
    y: 0,
    width: 420,
    height: 800,
  });
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
  // The row that was open is the list's current one only while its details are.
  await expect(sample).not.toHaveAttribute("aria-current", "true");

  // The browser's back button is the platform back: it returns to home.
  await page.goBack();
  await expect(home).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Open sidebar" }),
  ).toBeVisible();

  // The sidebar is the left drawer: dismissed by its scrim, and a destination closes it.
  await page.getByRole("button", { name: "Open sidebar" }).click();
  const drawer = page.getByRole("dialog", { name: "Sidebar" });
  await expect(drawer).toBeVisible();
  expect(await drawer.boundingBox()).toMatchObject({ x: 0, y: 0, height: 800 });
  // The scrim is what the drawer leaves uncovered at the right of the frame.
  await page
    .getByRole("button", { name: "Dismiss Sidebar" })
    .click({ position: { x: 390, y: 300 } });
  await expect(drawer).toBeHidden();
  await page.getByRole("button", { name: "Open sidebar" }).click();
  await drawer.getByRole("button", { name: "Show Settings" }).click();
  await expect(drawer).toBeHidden();
  await expect(sample).toBeVisible();
  await page.goBack();
  await expect(home).toBeVisible();

  // Wide: the sidebar and the list sit beside the details.
  await page.setViewportSize({ width: 1280, height: 800 });
  const sidebar = page.getByRole("navigation", { name: "Sidebar" });
  await expect(sidebar).toBeVisible();
  await page
    .getByRole("button", { name: "Open Settings", exact: true })
    .click();
  await sample.click();
  await expect(flag).toBeVisible();
  await expect(sample).toBeVisible();
  // The open row is marked in the list.
  await expect(sample).toHaveAttribute("aria-current", "true");
  await page.getByRole("button", { name: "Back", exact: true }).click();
  await expect(flag).toBeHidden();
  await expect(sample).toBeVisible();

  // The sidebar hides, the nav button then offers it back.
  await page.getByRole("button", { name: "Hide sidebar" }).click();
  await expect(sidebar).toBeHidden();
  await page.getByRole("button", { name: "Open sidebar" }).click();
  await expect(sidebar).toBeVisible();

  await page.getByRole("button", { name: "Show Home" }).click();
  await expect(home).toBeVisible();
  expect(failures).toEqual([]);
});

// A panel whose micro has no presentation (packages are admitted disabled, so the layout is one
// on a fresh install) shows a placeholder, and the micro that would place the sidebar/back button
// is the one that is absent. The shell supplies the button then, or a compact window is a dead
// end that only the platform back leaves. Found by the Bombadil exploration (`bombadil/shell.ts`).
test("a panel that is unavailable still offers the way back or the sidebar", async ({
  page,
}) => {
  const failures = watchFailures(page);
  const unavailable = page.getByRole("alert");

  await page.setViewportSize({ width: 420, height: 800 });
  await page.goto("/");
  await expect(page.getByRole("button", { name: "Open Layout" })).toBeVisible({
    timeout: 60_000,
  });
  await page.getByRole("button", { name: "Open Layout" }).click();
  await expect(unavailable).toContainText("Section unavailable");
  await page.getByRole("button", { name: "Back", exact: true }).click();
  await expect(page.getByRole("region", { name: "Home" })).toBeVisible();

  // Wide with the sidebar hidden is the same dead end one step later.
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.getByRole("button", { name: "Hide sidebar" }).click();
  await page.getByRole("button", { name: "Open Layout" }).click();
  await expect(unavailable).toContainText("Section unavailable");
  await page.getByRole("button", { name: "Open sidebar" }).click();
  await expect(page.getByRole("navigation", { name: "Sidebar" })).toBeVisible();
  expect(failures).toEqual([]);
});
