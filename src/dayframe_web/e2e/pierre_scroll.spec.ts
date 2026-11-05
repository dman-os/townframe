import { expect, type Locator, type Page, test } from "@playwright/test";
import { openLifecycle, watchFailures } from "./shell";

// Pierre scroll spike (docs/scratch/pierre-spikes.md, "P2 scroll"): the spike micro
// `dayframe.pierre_spike_scroll` virtualizes a synthetic 1M-line file at Pierre's geometry
// (20 px rows, a ~200 line window quantized to 50-line chunks). Every assertion here is on
// behaviour (what the guest rendered for the scroll position the DOM reported, where the DOM
// ended up after a command), and the profile tests print the measured numbers as
// `PIERRE_SPIKE <name> <json>` lines.

const LINE = 20;

// A scroller of 600 px. The default window profile is the code view's (overscroll 200 px, chunk
// 1 line: `CodeView.ts:763`, `constants.ts:67-70`): ceil((600 + 400) / 20) + 1 = 51 rows, centred
// on the viewport (the scroller's clientHeight is 598: its border is inside the 600 px) and
// clamped at the top of the file to 41.
test.describe.configure({ timeout: 240_000 });

function stats(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  const at = (q: number) =>
    sorted.length === 0
      ? null
      : sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
  const round = (v: number | null) => v === null ? null : Math.round(v * 100) / 100;
  return { n: sorted.length, median: round(at(0.5)), p95: round(at(0.95)), max: round(at(1)) };
}

/// `coalesce: false` is the host before the scroll coalescer (every browser scroll event is a
/// synchronous guest dispatch), which is what the baseline tests measure.
async function openSpike(page: Page, { coalesce = false } = {}) {
  await page.addInitScript((off) => { (window as any).__clayNoCoalesce = off; }, !coalesce);
  await page.goto("/");
  await expect(
    page.getByRole("button", { name: "Open Settings", exact: true }),
  ).toBeVisible({ timeout: 60_000 });
  await openLifecycle(page);
  await page
    .getByRole("button", { name: "Enable dayframe.layout micro" })
    .click();
  await page
    .getByRole("button", { name: "Enable dayframe.pierre_spike_scroll micro" })
    .click();
  await expect(
    page.getByRole("button", {
      name: "Disable dayframe.pierre_spike_scroll micro",
    }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Show Layout" }).click();
  await page
    .getByRole("button", { name: "Show Pierre scroll spike panel" })
    .click({ timeout: 60_000 });
  const scroller = page.getByTestId("scroller");
  await expect(scroller).toBeVisible({ timeout: 60_000 });
  await scroller.scrollIntoViewIfNeeded();
  return {
    scroller,
    window: page.getByTestId("window"),
  };
}

/// The first line and the line count of the window the guest last committed.
async function windowOf(win: Locator) {
  return {
    start: Number(await win.getAttribute("data-value")),
    count: Number(await win.getAttribute("data-state")),
  };
}

/// Installs the in-page probe: scroll events, DOM mutation batches, animation-frame intervals
/// and long tasks, all stamped with `performance.now()`. The host's own phase records are in
/// `window.__clayTiming` (feature `spike-timing` of `clay_dioxus`).
async function installProbe(page: Page) {
  await page.evaluate(() => {
    // deno-lint-ignore no-explicit-any
    const w = window as any;
    const scroller = document.querySelector('[data-testid="scroller"]')!;
    const probe = { clicks: [] as number[], scrolls: [] as number[][], mutations: [] as number[][], frames: [] as number[], longtasks: [] as number[], running: true };
    w.__probe = probe;
    w.__clayTiming = [];
    document.addEventListener("click", () => probe.clicks.push(performance.now()), { capture: true });
    scroller.addEventListener("scroll", () => {
      probe.scrolls.push([performance.now(), (scroller as HTMLElement).scrollTop]);
    }, { capture: true, passive: true });
    new MutationObserver((records) => {
      let added = 0, removed = 0;
      for (const r of records) { added += r.addedNodes.length; removed += r.removedNodes.length; }
      probe.mutations.push([performance.now(), records.length, added, removed]);
    }).observe(scroller, { subtree: true, childList: true, attributes: true, characterData: true });
    let last = performance.now();
    const tick = (now: number) => {
      if (!probe.running) return;
      probe.frames.push(now - last);
      last = now;
      requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
    new PerformanceObserver((list) => {
      for (const e of list.getEntries()) probe.longtasks.push(e.duration);
    }).observe({ entryTypes: ["longtask"] });
  });
}

async function frame(page: Page) {
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(resolve)));
}

/// Wheels `steps` times by `delta` px, one wheel event per animation frame, then reports what the
/// probe and the host's phase records saw.
async function wheelProfile(page: Page, scroller: Locator, steps: number, delta: number) {
  const box = (await scroller.boundingBox())!;
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await installProbe(page);
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Performance.enable");
  const metrics = async () =>
    Object.fromEntries(
      (await cdp.send("Performance.getMetrics")).metrics.map((m) => [m.name, m.value]),
    ) as Record<string, number>;
  const before = await metrics();
  for (let i = 0; i < steps; i++) {
    await page.mouse.wheel(0, delta);
    await frame(page);
  }
  // Let the last event's commit land before stopping the probe.
  await frame(page);
  await frame(page);
  const after = await metrics();
  const data = await page.evaluate(() => {
    // deno-lint-ignore no-explicit-any
    const w = window as any;
    w.__probe.running = false;
    return { probe: w.__probe, timing: w.__clayTiming as [string, number, number, number, number][] };
  });
  const duration = (kind: string) =>
    data.timing.filter((r) => r[0] === kind).map((r) => r[2] - r[1]);
  const applies = data.timing.filter((r) => r[0] === "apply");
  const renders = data.timing.filter((r) => r[0] === "render");
  // The window changed in a revision when the apply carried mutations.
  const committing = applies.filter((r) => r[3] > 0);
  // Scroll event to the DOM mutation batch that follows it (the committed window).
  const commitLatency: number[] = [];
  for (const [t] of data.probe.scrolls) {
    const next = data.probe.mutations.find((m) => m[0] >= t);
    const nextScroll = data.probe.scrolls.find((s) => s[0] > t);
    if (next && (!nextScroll || next[0] < nextScroll[0])) commitLatency.push(next[0] - t);
  }
  // From the end of each guest scroll dispatch (its batch is applied) to the first DOM mutation
  // batch after it: the host's projection, the VDOM diff and the DOM apply, which the host
  // schedules after the commit and which cannot be told apart without instrumenting dioxus-web.
  const hostTail: number[] = [];
  for (const r of data.timing.filter((r) => r[0] === "dispatch:scroll")) {
    const next = data.probe.mutations.find((m) => m[0] >= r[2]);
    if (next) hostTail.push(next[0] - r[2]);
  }
  const frames = data.probe.frames.slice(1);
  const metric = (k: string) => Math.round((after[k] - before[k]) * 1000) / 1000;
  return {
    steps,
    delta,
    scrollEvents: data.probe.scrolls.length,
    guestDispatchMs: stats(duration("dispatch:scroll")),
    hostRenderMs: stats(renders.map((r) => r[2] - r[1])),
    treeApplyMs: stats(applies.map((r) => r[2] - r[1])),
    mutationsPerCommit: stats(committing.map((r) => r[3])),
    retainedNodes: applies.length ? applies[applies.length - 1][4] : null,
    commitsWithMutations: committing.length,
    domMutationBatches: data.probe.mutations.length,
    scrollToCommitMs: stats(commitLatency),
    hostTailMs: stats(hostTail),
    frameIntervalMs: stats(frames),
    framesOver25ms: frames.filter((f) => f > 25).length,
    framesOver50ms: frames.filter((f) => f > 50).length,
    longTasks: data.probe.longtasks.length,
    chromium: {
      scriptMs: metric("ScriptDuration") * 1000,
      layoutMs: metric("LayoutDuration") * 1000,
      recalcStyleMs: metric("RecalcStyleDuration") * 1000,
      taskMs: metric("TaskDuration") * 1000,
    },
  };
}

function report(name: string, value: unknown) {
  console.log(`PIERRE_SPIKE ${name} ${JSON.stringify(value)}`);
}

test("scroll offsets reach the guest and its window follows the DOM scroll position", async ({ page }) => {
  const failures = watchFailures(page);
  const { scroller, window: win } = await openSpike(page);

  // 1M lines at 20 px: the guest's content box is 20,000,000 px tall and the DOM agrees.
  expect(await scroller.evaluate((el) => el.scrollHeight)).toBe(1_000_000 * LINE);
  expect(await windowOf(win)).toEqual({ start: 0, count: 41 });

  // The user (here: the test, writing the DOM) scrolls to line 500,000. The scroll event's
  // payload is the only thing the guest learns it from.
  await scroller.evaluate((el, top) => { el.scrollTop = top; }, 500_000 * LINE);
  await expect(win).toHaveAttribute("data-value", "499989");
  expect(await windowOf(win)).toEqual({ start: 499_989, count: 51 });
  await expect(win.getByText("let v500000 = step(")).toBeVisible();
  // The window is positioned by the model: its first row's top is `start * 20`.
  expect(await win.evaluate((el) => (el as HTMLElement).offsetTop)).toBe(499_989 * LINE);

  // And back to the top.
  await scroller.evaluate((el) => { el.scrollTop = 0; });
  await expect(win).toHaveAttribute("data-value", "0");
  await expect(win.getByText("let v0 = step(")).toBeVisible();
  expect(failures).toEqual([]);
});

test("the browser clamps a 3M line (60M px) content box below the file height", async ({ page }) => {
  const failures = watchFailures(page);
  const { scroller, window: win } = await openSpike(page);
  await page.getByTestId("lines-3000000").click();
  await expect(scroller).toBeVisible();
  const geometry = await scroller.evaluate((el) => ({ scrollHeight: el.scrollHeight, client: el.clientHeight }));
  report("clamp", geometry);
  expect(geometry.scrollHeight).toBeLessThan(3_000_000 * LINE);
  // The last reachable line is therefore far short of line 3,000,000.
  await scroller.evaluate((el) => { el.scrollTop = el.scrollHeight; });
  const reached = (await windowOf(win)).start + (await windowOf(win)).count;
  report("clamp-last-line", { reached });
  expect(reached).toBeLessThan(3_000_000);
  expect(failures).toEqual([]);
});

test("KNOWN BREAK: Pierre's own window profile (about 200 rows) overruns the batch budget and takes the shell's guest dispatch down with host panics", async ({ page }) => {
  const failures = watchFailures(page);
  const { scroller, window: win } = await openSpike(page);
  await page.getByTestId("profile-pierre").click();
  // The 100 row window at the top of the file still commits: it is a diff from 41 rows, 59 new.
  await expect(win).toHaveAttribute("data-state", "100");
  // A jump replaces all of it with a 200 row window: about 4,600 mutations in one batch, over the
  // 4,096 cap. The surface is withdrawn, and the guest's `expect("guest presentation commit
  // failed")` in `dispatch` aborts the guest inside a synchronous export, which the wcmp engine
  // reports as a host panic (`clay_rt_wcmp/lib.rs:208`) followed by a re-borrow panic (`:655`).
  await scroller.evaluate((el, top) => { el.scrollTop = top; }, 500_000 * LINE);
  const panel = page.getByRole("region", { name: "Pierre scroll spike panel" });
  await expect(panel).toHaveText("Component withdrawn.");
  expect(failures.some((f) => f.includes("suspended on its first poll"))).toBe(true);
  expect(failures.some((f) => f.includes("RefCell already borrowed"))).toBe(true);
});

for (const [name, steps, delta] of [["continuous-100px", 200, 100], ["chunk-per-frame-1000px", 200, 1000]] as const) {
  test(`baseline wheel profile ${name}`, async ({ page }) => {
    const failures = watchFailures(page);
    const { scroller, window: win } = await openSpike(page);
    const profile = await wheelProfile(page, scroller, steps, delta);
    report(`baseline-${name}`, profile);
    // The guest followed the wheel: the window left the top of the file.
    expect((await windowOf(win)).start).toBeGreaterThan(0);
    expect(failures).toEqual([]);
  });
}

test("element commands: the guest scrolls the scroller to line 500,000 and back with scrollTo, and reads its geometry", async ({ page }) => {
  const failures = watchFailures(page);
  const { scroller, window: win } = await openSpike(page);

  // `onmounted` handed the guest a handle; `scroll` is a guest-to-host element command. The DOM
  // is the witness that the host ran it, and the window shows the guest saw the scroll event.
  await page.getByTestId("jump-500000").click();
  await expect.poll(() => scroller.evaluate((el) => el.scrollTop)).toBe(500_000 * LINE);
  await expect(win).toHaveAttribute("data-value", "499989");
  await expect(win.getByText("let v500000 = step(")).toBeVisible();

  await page.getByTestId("jump-top").click();
  await expect.poll(() => scroller.evaluate((el) => el.scrollTop)).toBe(0);
  await expect(win).toHaveAttribute("data-value", "0");
  await expect(win.getByText("let v0 = step(")).toBeVisible();

  // Reads are answered by a later `element-reply` event; the values are the DOM's own.
  await page.getByTestId("jump-500000").click();
  await expect(win).toHaveAttribute("data-value", "499989");
  const dom = await scroller.evaluate((el) => {
    const rect = el.getBoundingClientRect();
    return { w: rect.width, h: rect.height, sw: el.scrollWidth, sh: el.scrollHeight, top: el.scrollTop };
  });
  await page.getByTestId("measure").click();
  await expect(page.getByTestId("measured")).toHaveText(
    `rect ${dom.w}x${dom.h} scroll-size ${dom.sw}x${dom.sh} scroll-offset 0,${dom.top}`,
  );
  expect(failures).toEqual([]);
});

test("element commands: the round trip of a jump to the committed window", async ({ page }) => {
  const failures = watchFailures(page);
  const { scroller, window: win } = await openSpike(page);
  const samples: number[] = [];
  for (let i = 0; i < 20; i++) {
    const line = i % 2 === 0 ? "500000" : "top";
    await installProbe(page);
    await page.getByTestId(`jump-${line}`).click();
    await expect(win).toHaveAttribute("data-value", line === "top" ? "0" : "499989");
    const { clicks, mutations } = await page.evaluate(() => (window as any).__probe as { clicks: number[]; mutations: number[][] });
    // From the click event in the page to the last DOM mutation batch of the jump's window.
    samples.push(mutations[mutations.length - 1][0] - clicks[0]);
    await page.evaluate(() => { (window as any).__probe.running = false; });
  }
  report("jump-roundtrip-ms", stats(samples));
  void scroller;
  expect(failures).toEqual([]);
});

// Chromium delivers at most one native scroll event per frame, so the coalescer has nothing to
// merge under a real wheel. A host that delivers every scroll position (synthesised here by
// dispatching a scroll event after each programmatic scrollTop write) is the case it exists for.
for (const [name, steps, delta] of [["continuous-100px", 200, 100]] as const) {
  test(`coalesced wheel profile ${name}`, async ({ page }) => {
    const failures = watchFailures(page);
    const { scroller } = await openSpike(page, { coalesce: true });
    report(`coalesced-${name}`, await wheelProfile(page, scroller, steps, delta));
    expect(failures).toEqual([]);
  });
}

for (const coalesce of [false, true]) {
  test(`scroll coalescing ${coalesce ? "on" : "off"}: a burst of scroll events per frame, and the last position wins`, async ({ page }) => {
    const failures = watchFailures(page);
    const burst = 8;
    const frames = 100;
    const { scroller, window: win } = await openSpike(page, { coalesce });
    await installProbe(page);
    const before = await page.evaluate(() => (window as any).__clayTiming.length);
    // Each frame: `burst` scrollTop writes of 20 px, each followed by a scroll event.
    const finalTop = await scroller.evaluate(async (el, { burst, frames }) => {
      for (let f = 0; f < frames; f++) {
        for (let b = 0; b < burst; b++) {
          el.scrollTop += 20;
          el.dispatchEvent(new Event("scroll"));
        }
        await new Promise((resolve) => requestAnimationFrame(resolve));
      }
      await new Promise((resolve) => requestAnimationFrame(resolve));
      await new Promise((resolve) => requestAnimationFrame(resolve));
      return el.scrollTop;
    }, { burst, frames });
    // The guest saw the last position, whether or not it saw the ones before it.
    const clientHeight = await scroller.evaluate((el) => el.clientHeight);
    const expected = Math.max(0, Math.floor((finalTop + clientHeight / 2) / LINE) - 25);
    await expect(win).toHaveAttribute("data-value", String(expected));
    const data = await page.evaluate(() => ({
      timing: (window as any).__clayTiming as [string, number, number, number, number][],
      frames: (window as any).__probe.frames as number[],
      longtasks: (window as any).__probe.longtasks as number[],
    }));
    const records = data.timing.slice(before);
    const scrollDispatches = records.filter((r) => r[0] === "dispatch:scroll");
    const intervals = data.frames.slice(1);
    report(`burst-coalesce-${coalesce ? "on" : "off"}`, {
      burst,
      frames,
      finalTop,
      scrollDispatches: scrollDispatches.length,
      // Events the host dropped because the guest had committed a newer revision than the one
      // the listener was rendered from (the host re-renders asynchronously after a commit).
      staleScrollsDropped: records.filter((r) => r[0] === "stale:scroll").length,
      dispatchesPerFrame: Math.round((scrollDispatches.length / frames) * 100) / 100,
      guestDispatchMs: stats(scrollDispatches.map((r) => r[2] - r[1])),
      coalesceFlushes: records.filter((r) => r[0] === "coalesce").length,
      frameIntervalMs: stats(intervals),
      framesOver25ms: intervals.filter((f) => f > 25).length,
      longTasks: data.longtasks.length,
    });
    if (coalesce) {
      // One delivery per frame, plus the frame that follows the last burst.
      expect(scrollDispatches.length).toBeLessThanOrEqual(frames + 3);
    } else {
      // Without it the guest is entered for the first event of a burst and the host drops the
      // rest as stale: it never sees most positions, and which one it sees is a race.
      expect(scrollDispatches.length + records.filter((r) => r[0] === "stale:scroll").length)
        .toBeGreaterThanOrEqual(burst * frames);
    }
    expect(failures).toEqual([]);
  });
}

// Budget-scale measurements need the raised surface budgets: build the site with
// DAYFRAME_RAISED_LIMITS=1 and run with PIERRE_RAISED=1. Rows carry `spans` token spans each in
// Pierre's 200 row window (centred, so 200 rows are committed by the jump).
if (process.env.PIERRE_RAISED) {
  for (const spans of [3, 7, 18, 38]) {
    test(`retained-tree scale: ${spans} spans per line, Pierre window, whole-tree vs incremental projection`, async ({ page }) => {
      test.setTimeout(900_000);
      const failures = watchFailures(page);
      const { scroller, window: win } = await openSpike(page, { coalesce: true });
      await page.getByTestId(`tokens-${spans}`).click();
      await page.getByTestId("profile-pierre").click();
      await page.getByTestId("jump-500000").click();
      await expect(win).toHaveAttribute("data-state", "200", { timeout: 120_000 });
      for (const incremental of [false, true]) {
        await page.evaluate((off) => { (window as any).__clayNoIncremental = off; }, !incremental);
        // One quiet frame so the projection switch itself is not in the measurement.
        await frame(page);
        const profile = await wheelProfile(page, scroller, 60, 1000);
        report(`scale-spans${spans}-${incremental ? "incremental" : "whole-tree"}`, profile);
      }
      expect(failures).toEqual([]);
    });
  }
}
