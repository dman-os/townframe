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

// Each option is styled only by the sheet its page asks for (`?pierre-css=`; (b) asks for none:
// its sheet is the host's, delivered from the manifest), so a presentation that renders without
// its own mechanism renders unstyled. `row` selects one row of the code column in the option's
// markup, `addition` an added one.
const options = [
  {
    id: "a",
    url: "/?pierre-css=verbatim",
    title: "Pierre rows (a) widened vocabulary",
    row: "[data-line]",
    addition: "[data-line][data-line-type='change-addition']",
  },
  {
    id: "b",
    url: "/",
    title: "Pierre rows (b) host stylesheet",
    row: ".pd-line",
    addition: ".pd-line.pd-line-type--change-addition",
  },
];

for (const option of options) {
  test(`option ${option.id}: the guest's rows are laid out by Pierre's own stylesheet`, async ({
    page,
  }) => {
    const failures = watchFailures(page);
    // Tall enough that the settings details scroller shows every row, so one screenshot per
    // option covers them all.
    await page.setViewportSize({ width: 1400, height: 4800 });
    await openSpikeSection(page, option.url, option.title);

    // One revision carries every row: they all appear at once.
    const rows = page.locator(option.row);
    await expect(rows).toHaveCount(217, { timeout: 120_000 });

    // Pierre's stylesheet decided the layout: a row is one 20px line, and an added row is
    // tinted (an unstyled row has a transparent background).
    expect((await rows.first().boundingBox())?.height).toBe(20);
    const background = await page
      .locator(option.addition)
      .first()
      .evaluate((row) => getComputedStyle(row).backgroundColor);
    expect(background).not.toBe("rgba(0, 0, 0, 0)");
    expect(failures).toEqual([]);
  });
}
