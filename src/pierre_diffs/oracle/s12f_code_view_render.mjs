// Differential scenarios for the render pass of `CodeView` (C5), end to end: the
// published class over the world of `s12e_code_view_shell.mjs` (jsdom, a fake clock
// that `performance.now` and the timers read, frames that run when a step says so,
// scripted `Math.random`, `ResizeObserver` entries delivered by steps), with the
// geometry of the render rules (`env.rules`: a line is 20px, every fifth is 40px, an
// annotation 30px; a root and the hosts are declared) and a root that clamps its
// scroll position like a browser (`env.clamp`: to the container's declared extent) or
// like the upstream harness's `createRoot` (`env.unclamped`).
//
// A scenario is a list of steps: `setup`, `setItems`/`addItems`/`updateItem`/
// `removeItem`/`updateItemId`, `setOptions`, `scroll` (the root's `scrollTop` and its
// scroll event), `scrollTo` (every target type, alignment and behavior), `render`,
// `frames` (rounds of queued renders at the clock's time), `advance` (the clock and
// its timers), `resize` and `viewport` (the root and the hosts), `clearPendingScroll`,
// `cleanUp`. Each step records its value, the host trace (the root's scroll writes,
// timers, observers, listeners, focus, and the frames requested and cancelled), the
// observable state (the render state, window specs, logical and DOM scroll position,
// page offset, container height, spring, pending target and anchor, the item list, the
// frames pending) and the outline of the tree under the root (every rendered item's
// shell, hashed). The Rust port replays them in `components/code_view/render_oracle_tests.rs`.
//
// What this establishes: the order of the render pass's decisions and writes, for the
// same geometry, through scrolls, far jumps (`fitPerfectly`), resizes, option changes,
// item churn, programmatic scrolls and the spring. What it cannot: a browser's layout
// and compositor (declared here; C6's captured run).

import { clone } from "./s11a_cases.mjs";
import { runScenarios } from "./s12e_code_view_shell.mjs";

const spec = (id, diff, extra = {}) => ({ id, diff, ...extra });
const world = (viewportHeight) => ({ op: "world", scrollTop: 0, scrollHeight: 0, viewportHeight });
const frames = (rounds = 1) => ({ op: "frames", rounds });
const scroll = (top) => ({ op: "scroll", top });
const scrollTo = (target) => ({ op: "scrollTo", target });
const render = (immediate = true) => ({ op: "render", immediate });
const settle = (count = 4) => [frames(count)];
const ENV = { rules: true, unclamped: true };
const CLAMP = { rules: true, clamp: true };
const MIX = ["twoHunks", "multi3", "gaps1", "multi8", "gaps3", "uneven", "random0", "mock0", "thresholds", "multi0"];

function items(count, pool = MIX, prefix = "i") {
  return Array.from({ length: count }, (_, index) => spec(`${prefix}${index}`, pool[index % pool.length]));
}

const align = (name) => ({ align: name, behavior: "instant" });
const targets = (id, line) => [
  { type: "position", position: 700, behavior: "instant" },
  { type: "item", id, ...align("start") },
  { type: "item", id, ...align("center") },
  { type: "item", id, ...align("end") },
  { type: "item", id, ...align("nearest") },
  { type: "item", id, align: "start", offset: 30, behavior: "instant" },
  { type: "line", id, lineNumber: line, side: "additions", ...align("center") },
  { type: "line", id, lineNumber: line, side: "deletions", ...align("start") },
  { type: "line", id, lineNumber: line + 2, ...align("nearest") },
  { type: "range", id, range: { start: line, end: line + 3 }, ...align("center") },
  { type: "range", id, range: { start: line, end: line + 40, side: "additions" }, ...align("center") },
  { type: "range", id, range: { start: 1, end: 2 }, ...align("nearest") },
];

function handwritten() {
  const scenarios = [];
  // The first render, near and far scrolls, the end, the top again.
  for (const [label, height] of [["short", 300], ["tall", 900]]) {
    scenarios.push({
      name: `scroll ${label}`,
      env: ENV,
      options: {},
      steps: [
        world(height),
        { op: "setItems", items: items(10) },
        { op: "setup" },
        ...settle(),
        scroll(120),
        render(false),
        ...settle(),
        scroll(900),
        ...settle(),
        scroll(4000),
        ...settle(),
        scroll(1e6),
        ...settle(),
        scroll(10),
        render(true),
        ...settle(),
      ],
    });
  }
  // Resizes of the root and the sticky container, a viewport change.
  scenarios.push({
    name: "resizes",
    env: ENV,
    options: {},
    steps: [
      world(500),
      { op: "setItems", items: items(8) },
      { op: "setup" },
      ...settle(),
      scroll(800),
      ...settle(),
      { op: "viewport", height: 300 },
      { op: "resize", entries: [{ target: "root", blockSize: 300 }] },
      ...settle(),
      { op: "resize", entries: [{ target: "sticky", blockSize: 900 }] },
      ...settle(),
      { op: "viewport", height: 700 },
      { op: "resize", entries: [{ target: "root", blockSize: 700 }, { target: "sticky", blockSize: 100 }] },
      ...settle(),
    ],
  });
  // Header and footer hosts, resized while scrolled.
  scenarios.push({
    name: "hosts",
    env: ENV,
    options: { header: "a", footer: "b" },
    steps: [
      world(500),
      { op: "setItems", items: items(8) },
      { op: "setup" },
      ...settle(),
      { op: "setHeight", target: "header", height: 90 },
      { op: "resize", entries: [{ target: "header", blockSize: 90 }] },
      ...settle(),
      scroll(600),
      ...settle(),
      { op: "setHeight", target: "header", height: 170 },
      { op: "resize", entries: [{ target: "header", blockSize: 170 }, { target: "footer", blockSize: 60 }] },
      ...settle(),
      scrollTo({ type: "position", position: 1e9, behavior: "instant" }),
      ...settle(),
      { op: "setOptions", options: { header: null, footer: "b" } },
      render(true),
      ...settle(),
    ],
  });
  // Option changes mid scroll (the layout anchor), with measured wrapping rows.
  for (const overflow of ["scroll", "wrap"]) {
    scenarios.push({
      name: `options ${overflow}`,
      env: ENV,
      options: { diff: { overflow } },
      steps: [
        world(500),
        { op: "setItems", items: items(8) },
        { op: "setup" },
        ...settle(),
        scroll(1200),
        ...settle(),
        { op: "setOptions", options: { diff: { overflow, diffStyle: "unified" } } },
        render(true),
        ...settle(),
        { op: "setOptions", options: { diff: { overflow, diffStyle: "split", expandUnchanged: true }, layout: "roomy" } },
        render(true),
        ...settle(),
        { op: "setOptions", options: { diff: { overflow }, itemMetrics: "codeView", layout: "tight", stickyHeaders: true } },
        render(true),
        ...settle(),
        { op: "setOptions", options: { diff: { overflow, disableFileHeader: true, themeType: "light" } } },
        render(true),
        ...settle(),
        // Options that change what an item renders and not its layout.
        { op: "setOptions", options: { diff: { overflow, disableFileHeader: true, themeType: "dark", disableBackground: true } } },
        render(true),
        ...settle(),
        { op: "setOptions", options: { diff: { overflow, disableFileHeader: true, themeType: "dark", disableBackground: false, disableLineNumbers: true } } },
        render(true),
        ...settle(),
      ],
    });
  }
  // Every target type, alignment and offset, instant, from the top and from mid-list.
  const list = items(10);
  for (const from of [0, 2500]) {
    scenarios.push({
      name: `targets from ${from}`,
      env: ENV,
      options: {},
      steps: [
        world(450),
        { op: "setItems", items: list },
        { op: "setup" },
        ...settle(),
        scroll(from),
        ...settle(),
        ...targets("i3", 5).flatMap((target) => [scrollTo(target), render(true), ...settle(2), scroll(from), ...settle(2)]),
        scrollTo({ type: "item", id: "missing", align: "start", behavior: "instant" }),
        ...settle(),
      ],
    });
  }
  // The spring: smooth scrolls over explicit frame times, smooth-auto near and far, a
  // target cancelled by user input, a target retargeted mid-flight.
  const frameLoop = (count, ms = 16) => Array.from({ length: count }, () => [{ op: "advance", ms }, frames(1)]).flat();
  scenarios.push({
    name: "spring",
    env: ENV,
    options: {},
    steps: [
      world(500),
      { op: "setItems", items: items(10) },
      { op: "setup" },
      ...settle(),
      scrollTo({ type: "item", id: "i4", align: "start", behavior: "smooth" }),
      ...frameLoop(30),
      scrollTo({ type: "position", position: 200, behavior: "smooth-auto" }),
      ...frameLoop(20, 16),
      scrollTo({ type: "position", position: 6000, behavior: "smooth-auto" }),
      ...settle(),
      scrollTo({ type: "item", id: "i7", align: "center", behavior: "smooth" }),
      ...frameLoop(6, 16),
      scrollTo({ type: "item", id: "i2", align: "start", behavior: "smooth" }),
      ...frameLoop(8, 40),
      { op: "advance", ms: 100000 },
      frames(2),
      ...frameLoop(30),
    ],
  });
  scenarios.push({
    name: "spring cancelled",
    env: ENV,
    options: {},
    steps: [
      world(500),
      { op: "setItems", items: items(10) },
      { op: "setup" },
      ...settle(),
      scrollTo({ type: "item", id: "i6", align: "start", behavior: "smooth" }),
      ...frameLoop(4),
      { op: "clearPendingScroll" },
      ...frameLoop(4),
      scroll(900),
      ...frameLoop(4),
      scrollTo({ type: "position", position: 3000, behavior: "smooth" }),
      ...frameLoop(3),
      { op: "setItems", items: items(10).slice(2) },
      ...frameLoop(40),
    ],
  });
  // Item churn under the viewport: order, replacement by version, removal, rename, collapse.
  scenarios.push({
    name: "churn",
    env: ENV,
    options: {},
    steps: [
      world(500),
      { op: "setItems", items: items(12).map((entry) => ({ ...entry, version: 0 })) },
      { op: "setup" },
      ...settle(),
      scroll(1500),
      ...settle(),
      { op: "setItems", items: items(12).filter((_, index) => index % 3 !== 1).map((entry) => ({ ...entry, version: 0 })) },
      render(true),
      ...settle(),
      { op: "updateItem", item: spec("i5", "gaps3", { version: 1, collapsed: true }) },
      render(true),
      ...settle(),
      { op: "updateItem", item: spec("i5", "gaps3", { version: 2, collapsed: false }) },
      ...settle(),
      { op: "updateItemId", from: "i6", to: "renamed" },
      ...settle(),
      { op: "removeItem", id: "i4" },
      render(true),
      ...settle(),
      { op: "addItems", items: items(3, MIX, "added") },
      ...settle(),
      { op: "setItems", items: [] },
      render(true),
      ...settle(),
      { op: "setItems", items: items(6, MIX, "again") },
      ...settle(),
    ],
  });
  scenarios.push({
    name: "collapse everything",
    env: ENV,
    options: {},
    steps: [
      world(500),
      { op: "setItems", items: items(30).map((entry) => ({ ...entry, version: 0 })) },
      { op: "setup" },
      ...settle(),
      scroll(6000),
      ...settle(),
      { op: "setItems", items: items(30).map((entry) => ({ ...entry, version: 1, collapsed: true })) },
      render(true),
      ...settle(),
    ],
  });
  // The rebase across the threshold, with a root that clamps like a browser.
  scenarios.push({
    name: "rebase",
    env: CLAMP,
    options: { layout: "huge" },
    steps: [
      world(500),
      { op: "setItems", items: items(40, ["oneLine"]) },
      { op: "setup" },
      ...settle(),
      scroll(11100000),
      ...settle(),
      scroll(2500000),
      ...settle(),
      scrollTo({ type: "item", id: "i39", align: "start", behavior: "instant" }),
      ...settle(),
      scrollTo({ type: "position", position: 11100000, behavior: "instant" }),
      ...settle(),
      scroll(1000000),
      ...settle(),
      { op: "setItems", items: [] },
      render(true),
      ...settle(),
    ],
  });
  scenarios.push({
    name: "rebase firefox",
    env: { ...CLAMP, userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:153.0) Gecko/20100101 Firefox/153.0", platform: "MacIntel" },
    options: { layout: "huge" },
    steps: [
      world(500),
      { op: "setItems", items: items(40, ["oneLine"]) },
      { op: "setup" },
      ...settle(),
      scroll(3800000),
      ...settle(),
      scroll(1000000),
      ...settle(),
    ],
  });
  return scenarios;
}

function randomScenario(random, index) {
  const { pick } = random;
  const clamp = random.next() < 0.5;
  const ids = Array.from({ length: 12 }, (_, at) => `i${at}`);
  const live = () => ids.filter((_, at) => at < 4 || random.next() < 0.75).map((id) => spec(id, pick(MIX), { version: 0 }));
  let current = live();
  const steps = [world(250 + random.int(700)), { op: "setItems", items: current }, { op: "setup" }, ...settle(2)];
  const count = 14 + random.int(14);
  const id = () => pick(current).id;
  for (let step = 0; step < count; step++) {
    const roll = random.int(20);
    if (roll < 4) steps.push(scroll(random.int(random.next() < 0.3 ? 40000 : 5000)), render(random.next() < 0.5));
    else if (roll < 6) steps.push(frames(1 + random.int(3)));
    else if (roll < 8) steps.push(scrollTo(pick(targets(id(), 1 + random.int(30)).concat([{ type: "item", id: id(), align: "end", behavior: random.next() < 0.4 ? "smooth" : "instant" }]))), render(true));
    else if (roll < 9) steps.push({ op: "advance", ms: 8 + random.int(60) }, frames(1));
    else if (roll < 10) steps.push({ op: "viewport", height: 200 + random.int(800) }, { op: "resize", entries: [{ target: "root", blockSize: 200 + random.int(800) }, { target: "sticky", blockSize: random.int(2000) }] });
    else if (roll < 12) {
      const overflow = pick(["scroll", "wrap"]);
      steps.push({ op: "setOptions", options: { diff: { overflow, diffStyle: pick(["split", "unified"]), expandUnchanged: random.next() < 0.3, disableBackground: random.next() < 0.5, themeType: pick(["light", "dark", "system"]) }, ...(random.next() < 0.5 ? { layout: pick(["tight", "roomy", "fractional"]) } : {}), ...(random.next() < 0.3 ? { header: "a" } : {}) } }, render(true));
    } else if (roll < 14) {
      current = live().map((entry) => ({ ...entry, version: random.int(3), ...(random.next() < 0.2 ? { collapsed: true } : {}) }));
      steps.push({ op: "setItems", items: current }, render(random.next() < 0.5));
    } else if (roll < 15 && current.length > 3) {
      const gone = id();
      current = current.filter((entry) => entry.id !== gone);
      steps.push({ op: "removeItem", id: gone });
    } else if (roll < 16) steps.push({ op: "clearPendingScroll" });
    else if (roll < 17) steps.push({ op: "setHeight", target: "header", height: random.int(200) }, { op: "resize", entries: [{ target: "header", blockSize: random.int(200) }] });
    else steps.push(frames(2));
  }
  steps.push({ op: "cleanUp" });
  return { name: `random${index}`, env: clamp ? CLAMP : ENV, options: { header: pick([undefined, "a"]), footer: pick([undefined, "b"]) }, steps };
}

export function buildS12fCodeViewRender(oracle, random, corpusMocks) {
  return runScenarios(oracle, random, corpusMocks, ({ random: draws }) => {
    const scenarios = handwritten();
    for (let index = 0; index < 60; index++) scenarios.push(randomScenario(draws, index));
    return scenarios;
  });
}
