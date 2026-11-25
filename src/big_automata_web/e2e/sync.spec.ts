import { type Page, expect, test } from "@playwright/test";

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

// Can a browser endpoint ACCEPT through a relay? The first page binds with an echo ALPN and
// publishes its endpoint id; the native fixture, then a second page (own browser context),
// each dial it by id and relay only and check the echoed bytes.
const watchFailures = (page: Page) => {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });
  return failures;
};

test("a browser endpoint accepts dials from native and from another browser", async ({
  browser,
  request,
}) => {
  const acceptor = await (await browser.newContext()).newPage();
  const acceptorFailures = watchFailures(acceptor);
  await acceptor.goto("/?mode=accept");
  const nodeId = await expect
    .poll(async () => (await acceptor.locator("#node_id").textContent()) ?? "", {
      timeout: 60_000,
    })
    .toMatch(/^[0-9a-f]{64}$/)
    .then(async () => (await acceptor.locator("#node_id").textContent())!);

  // Native dials browser.
  const native = await request.post(`/dial/${nodeId}`);
  expect(await native.text()).toBe("ok");
  await expect(acceptor.locator("#echoed")).toHaveText("1");

  // Browser dials browser.
  const dialer = await (await browser.newContext()).newPage();
  const dialerFailures = watchFailures(dialer);
  await dialer.goto(`/?mode=dial&target=${nodeId}`);
  await expect(dialer.locator("#echo")).toHaveText("hello through the relay", {
    timeout: 60_000,
  });
  await expect(acceptor.locator("#echoed")).toHaveText("2");
  await expect(acceptor.locator("#closed")).toHaveText("2", { timeout: 30_000 });

  expect(acceptorFailures).toEqual([]);
  expect(dialerFailures).toEqual([]);
});

// The reverse of the first test: the page serves big_automata (the real replay and sync rpc
// service over its part store) through the relay, and the native fixture's `PeerDriver` pulls
// part 1 from it. `?mode=serve` publishes its endpoint id once its home relay is connected.
test("a native peer pulls a part from the browser host over iroh", async ({
  page,
  request,
}) => {
  test.setTimeout(90_000);
  const failures = watchFailures(page);
  page.on("console", (message) => console.log(`page ${message.type()}: ${message.text()}`));
  page.on("pageerror", (error) => console.log(`pageerror: ${error.message}`));
  await page.addInitScript(() => { Error.stackTraceLimit = 200; });
  await page.goto("/?mode=serve");
  await expect(page.locator("#node_id")).toHaveText(/^[0-9a-f]{64}$/, {
    timeout: 60_000,
  });
  const nodeId = (await page.locator("#node_id").textContent())!;
  const served = Number(await page.locator("#served").textContent());
  expect(served).toBeGreaterThan(1);

  const pulled = await request.post(`/pull/${nodeId}`, { timeout: 40_000 });
  expect(await pulled.text()).toBe(`synced ${served}`);
  expect(failures).toEqual([]);
});
