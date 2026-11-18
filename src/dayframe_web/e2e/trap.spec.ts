import { expect, test } from "@playwright/test";
import { openLifecycle, watchFailures } from "./shell";

// A guest that traps inside a synchronous export (here `presentation.dispatch`, from a click)
// faults that micro and nothing else. Chromium reports a guest trap only through the rejection
// of the call's promise, on a microtask, so a host that polls the synchronous export once used
// to see it suspend, which `clay_rt_wcmp::first_poll` treats as an invariant break: it panicked,
// and the panic took the whole shell down with a page error. The `trapper` package (a fixture
// the e2e server serves, see `x/dayframe-packages.ts`) has one button, and its click traps.
test("a guest that traps in a sync export faults only its micro", async ({
  page,
}) => {
  const failures = watchFailures(page);

  await page.goto("/");
  await page
    .getByRole("button", { name: "Open Settings", exact: true })
    .click({ timeout: 60_000 });
  await page
    .getByRole("button", { name: "Open Micro lifecycle settings" })
    .click();
  for (const micro of ["trapper", "counter"]) {
    await page
      .getByRole("button", { name: `Enable dayframe.${micro} micro` })
      .click();
    await expect(
      page.getByRole("button", { name: `Disable dayframe.${micro} micro` }),
    ).toBeVisible();
  }

  // The trapper's section mounts its view, and the click dispatches into the guest, which traps.
  await page
    .getByRole("button", { name: "Open Trapper (fixture) settings" })
    .click();
  await page
    .getByRole("button", { name: "Trap the guest" })
    .click({ timeout: 60_000 });

  // The micro is faulted with the trap as the cause, and the shell lists it.
  await openLifecycle(page);
  await expect(
    page.getByRole("status").filter({ hasText: "dayframe.trapper runtime:" }),
  ).toContainText(/Faulted.*unreachable/i, { timeout: 60_000 });

  // The shell and the other micros keep working: the counter still mounts and counts.
  await page
    .getByRole("button", { name: "Open Counter (Wasm) settings" })
    .click();
  const counter = page.getByRole("button", { name: "Increment Wasm counter" });
  await expect(counter).toHaveText("0", { timeout: 60_000 });
  await counter.click();
  await expect(counter).toHaveText("1");
  await openLifecycle(page);
  await expect(
    page.getByRole("status").filter({ hasText: "dayframe.counter runtime:" }),
  ).not.toContainText("Faulted");

  expect(failures).toEqual([]);
});
