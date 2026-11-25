import { expect, test } from "@playwright/test";
import { readFrame } from "../bombadil/frame.ts";
import { openLifecycle, watchFailures } from "./shell";

// The reading Bombadil's properties are made of (`bombadil/frame.ts`) must see what a user sees.
// A wide window with the sidebar hidden and the layout shown has exactly one way to move
// between positions: the shell's navigation rack, which the layout mounts in its app bar.
// That button lives in the layout presentation's shadow root, which `document.querySelectorAll`
// does not cross; a reading that stopped there reported "no navigation" and failed
// `navigationAlwaysReachable` on a screen that had the button (CI run of #90, bombadil at 1024px,
// "Show Counter (Wasm) panel", back, "Hide sidebar").
test("the frame reading finds the navigation rack inside the layout's shadow root", async ({ page }) => {
  const failures = watchFailures(page);
  await page.setViewportSize({ width: 1024, height: 800 });
  await page.goto("/");
  await expect(
    page.getByRole("button", { name: "Open Settings", exact: true }),
  ).toBeVisible({ timeout: 60_000 });
  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Enable dayframe.layout micro" })
    .click();
  await page.getByRole("button", { name: "Show Layout" }).click();
  await expect(
    page.getByRole("button", { name: "Show Sample micro panel" }),
  ).toBeVisible({ timeout: 60_000 });

  await page.getByRole("button", { name: "Hide sidebar" }).click();
  await expect(
    page.getByRole("button", { name: "Open sidebar" }),
  ).toBeVisible();

  const frame = await page.evaluate(`(${readFrame})(document, window)`);
  expect(frame).toMatchObject({
    booted: true,
    mains: 1,
    lists: ["Layout"],
    details: [],
  });
  // The wide rack holds the toggle, back and forward; the two that have nowhere to go are
  // present and disabled, so the reading must list them as such.
  expect(frame.navigation).toEqual(
    expect.arrayContaining([{ name: "Open sidebar", enabled: true }]),
  );
  expect(frame.navigation.filter((button) => button.enabled)).toEqual([
    { name: "Open sidebar", enabled: true },
  ]);
  expect(failures).toEqual([]);
});
