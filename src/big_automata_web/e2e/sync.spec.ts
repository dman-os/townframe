import { expect, test } from "@playwright/test";

// The first browser run of big_automata's sync stack: the wasm host dials the native peer by
// node id through the fixture's relay (relay-only: a browser has no UDP), pulls the published
// part with a `PeerDriver` into a memory backend, and reports `Synced` and its object count.
// A Rust panic surfaces as a page error, so any page or console error fails the test.
test("the browser host syncs a part from a native peer over iroh", async ({
  page,
  request,
}) => {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });

  const fixture = await (await request.get("/fixture.json")).json();
  expect(fixture.objects).toBeGreaterThan(1);

  await page.goto("/");
  await expect(page.locator("#status")).toHaveText("Synced", {
    timeout: 60_000,
  });
  await expect(page.locator("#expected")).toHaveText(String(fixture.objects));
  await expect(page.locator("#objects")).toHaveText(String(fixture.objects));
  expect(failures).toEqual([]);
});
