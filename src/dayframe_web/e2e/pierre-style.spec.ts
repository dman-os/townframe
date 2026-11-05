import { expect, type Page, test } from "@playwright/test";
import { openLifecycle, watchFailures } from "./shell";

// Style spike (docs/scratch/pierre-spikes.md, "P1 style"): `micro_pierre_spike` is a wasm guest
// that renders the 217 rows real Pierre 1.5.2 emitted for a unified diff, once per resolution of
// the stylesheet-delivery blocker, plus a probe of what the wire already lets a guest do.

/// Enables the spike micro and opens one of its Settings sections (a section is how the micro
/// mounts a presentation in the shell).
async function openSpikeSection(page: Page, url: string, title: string) {
  await page.goto(url);
  await expect(
    page.getByRole("button", { name: "Open Settings", exact: true }),
  ).toBeVisible({ timeout: 60_000 });
  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Enable dayframe.pierrespike micro" })
    .click();
  await page.getByRole("button", { name: "Show Settings" }).click();
  await page.getByRole("button", { name: `Open ${title} settings` }).click();
}

// The guest's raw `style` attribute is in the wire's attribute vocabulary, and the host applies it
// with `setAttribute("style", ...)`, so the style-property list (the `style` namespace) is not what
// bounds a guest's CSS: this guest covers the viewport and makes the page fetch a URL of its choice.
test("a guest's raw style attribute is applied unfiltered", async ({ page }) => {
  const failures = watchFailures(page);
  // The probe's URL is fulfilled here, so nothing leaves the machine; the request event is the
  // page asking the network for a URL the guest chose.
  const beacons: string[] = [];
  await page.route("http://127.0.0.1:9/pierre-probe-beacon", (route) => {
    beacons.push(route.request().url());
    return route.fulfill({ status: 204 });
  });
  await openSpikeSection(page, "/", "Pierre probe (raw style)");

  const probe = page.locator("#probe");
  await expect(probe).toHaveText("raw style probe", { timeout: 60_000 });
  const style = await probe.evaluate((element) => {
    const computed = getComputedStyle(element);
    return {
      position: computed.position,
      width: element.getBoundingClientRect().width,
      viewport: window.innerWidth,
    };
  });
  expect(style.position).toBe("fixed");
  expect(style.width).toBe(style.viewport);
  await expect.poll(() => beacons[0]).toBe(
    "http://127.0.0.1:9/pierre-probe-beacon",
  );
  expect(failures).toEqual([]);
});

