// Differential scenarios for the scroll model of `CodeView` (C3): the paged
// scroll and rebase math (`getMaxScrollTopForHeight` ... `getPagedLayoutTop`,
// with the default and the Firefox `ScrollRebaseConfig`), the scroll targets
// (`getScrollTargetRect`, `normalizeScrollTarget`, `resolveScrollTargetTop`,
// `resolveAlignedScrollPosition`, `getStickyHeaderOffset`,
// `resolveEffectiveScrollBehavior`), the spring (`computeSpringStep`,
// `computeTargetScrollTopForFrame`, `advanceScrollAnimation`), the scroll
// anchors (`getScrollAnchor`, `getScrollAnchorViewportTop`,
// `resolveAnchoredScrollTop`, `isPendingTargetSettled`,
// `capturePendingLayoutAnchor`) and the cached reads (`getScrollTop`,
// `getHeight`).
//
// The published `CodeView` is constructed as in `s12c_code_view.mjs` (a bare
// `document`, never `setup`); the scroll model reads only `root.scrollTop` and
// `root.getBoundingClientRect().height`, so a `root` is installed directly as a
// two-member stand-in (`setRoot`), and the private methods are called through
// the `call` step (plain methods at runtime). State the model reads is written
// directly (`setState`: scroll position and page offset, the dirty flags, the
// header/footer heights, `scrollHeight`, the render state, the pending target,
// the animation, the pending anchor); the rebase thresholds are reached either
// by writing `scrollHeight` or, as the upstream test does, by item gaps of
// 1,000,000. The environment is per scenario: the user agent (the Firefox
// config is chosen at construction), `devicePixelRatio` and
// `prefers-reduced-motion`. Each step records its value and, after anything
// that can change it, the model's state. The Rust port replays them in
// `components/code_view/scroll_oracle_tests.rs`.
//
// What this establishes: that the scroll arithmetic, the target resolution, the
// spring and the anchor search return what the published class returns for the
// same state. What it cannot: anything that needs a render (`scrollTo`,
// `applyScrollFix`, `computeRenderRangeAndEmit`: C5) or the DOM (C4). The spring
// compares to a tolerance (`approx` scenarios): JavaScript's and Rust's `exp`
// need not agree to the last bit.

import { clone } from "./s11a_cases.mjs";
import { buildOptions } from "./s12a_file_diff.mjs";
import { METRICS, QUERY_DIFFS_SMALL, buildDiffPool } from "./s12b_layout.mjs";
import { LAYOUTS, LISTS } from "./s12c_code_view.mjs";

const CHROME = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const FIREFOX = "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0";
// The layouts of `s12c_code_view.mjs` and the two tall ones: the gap of the upstream rebase test
// (`CodeView.scrollAnchoring.test.ts`) and a taller one.
const D_LAYOUTS = {
  ...LAYOUTS,
  gapMillion: { paddingTop: 10, paddingBottom: 10, gap: 1_000_000 },
  gapFourMillion: { paddingTop: 10, paddingBottom: 10, gap: 4_000_000 },
};
const SMOOTH = {
  default: undefined,
  snappy: { omega: 0.03, positionEpsilon: 0.5, velocityEpsilon: 0.01 },
  slow: { omega: 0.004, positionEpsilon: 2, velocityEpsilon: 0.2 },
};

// Calls that change what the model caches or holds, so the state is recorded after them.
const MUTATING = new Set(["getScrollTop", "getHeight", "advanceScrollAnimation", "capturePendingLayoutAnchor"]);

function encode(value) {
  return value === undefined ? null : JSON.parse(JSON.stringify(value));
}

function encodeState(cv) {
  return {
    scrollTop: cv.scrollTop,
    scrollPageOffset: cv.scrollPageOffset,
    scrollDirty: cv.scrollDirty,
    height: cv.height,
    heightDirty: cv.heightDirty,
    scrollHeight: cv.scrollHeight,
    pendingScrollTarget: encode(cv.pendingScrollTarget),
    scrollAnimation: encode(cv.scrollAnimation),
    pendingLayoutAnchor: encode(cv.pendingLayoutAnchor),
    renderState: clone(cv.renderState),
    items: cv.items.map((record) => [record.item.id, record.top, record.height]),
  };
}

async function runScenario({ CodeView, resetPlatformDetection }, diffs, scenario) {
  const saved = {
    document: Object.getOwnPropertyDescriptor(globalThis, "document"),
    window: Object.getOwnPropertyDescriptor(globalThis, "window"),
    navigator: Object.getOwnPropertyDescriptor(globalThis, "navigator"),
  };
  const noop = () => {};
  const makeElement = () => ({ style: { setProperty: noop, removeProperty: noop }, appendChild: noop, remove: noop });
  const env = scenario.env ?? {};
  const define = (name, value) => Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
  define("document", { createElement: makeElement });
  define("window", {
    devicePixelRatio: env.dpr ?? 1,
    matchMedia: () => ({ matches: env.reducedMotion === true }),
  });
  define("navigator", { userAgent: env.userAgent ?? CHROME });
  resetPlatformDetection();
  const savedWarn = console.warn;
  const savedError = console.error;
  console.warn = noop;
  console.error = noop;
  try {
    const buildCodeViewOptions = (spec) => {
      const options = buildOptions(spec.diff ?? {}, new Map());
      if (spec.stickyHeaders !== undefined) options.stickyHeaders = spec.stickyHeaders;
      if (spec.itemMetrics !== undefined) options.itemMetrics = clone(METRICS[spec.itemMetrics]);
      if (spec.layout !== undefined) options.layout = clone(D_LAYOUTS[spec.layout]);
      if (spec.smoothScrollSettings !== undefined && SMOOTH[spec.smoothScrollSettings] !== undefined) {
        options.smoothScrollSettings = clone(SMOOTH[spec.smoothScrollSettings]);
      }
      return options;
    };
    const cv = new CodeView(buildCodeViewOptions(scenario.options));
    cv.render = noop;
    const item = (entry) => ({
      id: entry.id,
      type: "diff",
      fileDiff: diffs[entry.diff],
      ...(entry.annotations != null ? { annotations: LISTS[entry.annotations] } : {}),
    });
    // `available` names, per id, what the `availableItems` map holds for it:
    // the record itself, another record (a replaced one), or nothing.
    const availableMap = (available) => {
      if (available === undefined) return undefined;
      const map = new Map();
      for (const [id, how] of Object.entries(available)) {
        const record = cv.idToItem.get(id);
        if (how === "keep") map.set(id, record);
        else if (how === "stale") map.set(id, cv.items.find((other) => other !== record));
      }
      return map;
    };
    const apply = (step) => {
      switch (step.op) {
        case "setItems":
          cv.setItems(step.items.map(item));
          return null;
        case "recompute":
          if (cv.layoutDirtyIndex != null) {
            cv.recomputeLayout(cv.layoutDirtyIndex, cv.pendingLayoutReset);
            cv.layoutDirtyIndex = undefined;
            cv.pendingLayoutReset = undefined;
          }
          return null;
        case "setOptions":
          cv.setOptions(buildCodeViewOptions(step.options));
          return null;
        case "setRoot": {
          const root = { scrollTop: step.scrollTop, getBoundingClientRect: () => ({ height: root.height }) };
          root.height = step.height;
          cv.root = root;
          cv.scrollDirty = true;
          cv.heightDirty = true;
          return null;
        }
        case "writeRoot":
          if (step.scrollTop !== undefined) cv.root.scrollTop = step.scrollTop;
          if (step.height !== undefined) cv.root.height = step.height;
          return null;
        case "setState": {
          const { state } = step;
          for (const key of ["scrollTop", "scrollPageOffset", "scrollDirty", "height", "heightDirty", "scrollHeight"]) {
            if (state[key] !== undefined) cv[key] = state[key];
          }
          if (state.header !== undefined) cv.header.height = state.header;
          if (state.footer !== undefined) cv.footer.height = state.footer;
          if (state.renderState !== undefined) Object.assign(cv.renderState, state.renderState);
          for (const key of ["pendingScrollTarget", "scrollAnimation", "pendingLayoutAnchor"]) {
            if (key in state) cv[key] = state[key] === null ? undefined : clone(state[key]);
          }
          return null;
        }
        case "call": {
          const args = clone(step.args ?? []);
          if (step.fn === "getScrollAnchor" || step.fn === "capturePendingLayoutAnchor") args.push(availableMap(step.available));
          return encode(cv[step.fn](...args));
        }
        default:
          throw new Error(`unknown step ${step.op}`);
      }
    };

    const trace = [encodeState(cv)];
    const results = [];
    for (const step of scenario.steps) {
      let result;
      try {
        result = { value: apply(step) };
      } catch (error) {
        result = { throws: error.message };
      }
      results.push(result);
      trace.push(step.op === "call" && !MUTATING.has(step.fn) ? null : encodeState(cv));
    }
    return {
      config: encode(cv.scrollRebaseConfig),
      results,
      trace,
    };
  } finally {
    console.warn = savedWarn;
    console.error = savedError;
    for (const [name, descriptor] of Object.entries(saved)) {
      if (descriptor === undefined) delete globalThis[name];
      else Object.defineProperty(globalThis, name, descriptor);
    }
    resetPlatformDetection();
  }
}

// ---- scenarios ------------------------------------------------------------------

const spec = (id, diff, extra = {}) => ({ id, diff, ...extra });
const call = (fn, ...args) => ({ op: "call", fn, args });
const recompute = { op: "recompute" };
const ABC = [spec("a", "twoHunks"), spec("b", "multi3"), spec("c", "gaps1", { annotations: "A" })];
const ENVS = [
  { userAgent: CHROME, dpr: 1 },
  { userAgent: CHROME, dpr: 1.25 },
  { userAgent: FIREFOX, dpr: 2 },
  { userAgent: FIREFOX, dpr: 1.5 },
];
const envName = (env) => `${env.userAgent === FIREFOX ? "firefox" : "chrome"} dpr ${env.dpr}${env.reducedMotion ? " reduced" : ""}`;

function pagedMath() {
  const scenarios = [];
  const heights = [0, 700, 5_000, 11_000_000, 11_999_000, 12_000_000, 30_000_000];
  const offsets = [0, 1_000_000, 9_000_000, 25_000_000];
  const values = [-5, 0, 0.4, 1234.5, 499_999, 1_000_000, 2_500_000.3, 4_194_000, 11_000_001, 11_500_000, 29_999_999];
  for (const env of ENVS) {
    for (const scrollHeight of heights) {
      for (const scrollPageOffset of offsets) {
        const steps = [
          { op: "setRoot", scrollTop: 0, height: 800 },
          { op: "setState", state: { scrollHeight, scrollPageOffset, header: 37.5, footer: 12 } },
          call("getMaxScrollTop"),
          call("shouldRebaseScroll"),
          call("getPagedScrollHeight"),
          call("getMaxPagedScrollTop"),
          call("getMaxScrollPageOffset"),
          call("getMaxScrollTopForHeight", 123456.5),
        ];
        for (const value of values) {
          steps.push(
            call("clampScrollTop", value),
            call("clampPagedScrollTop", value),
            call("clampScrollPageOffset", value),
            call("resolvePagedScrollPosition", value),
            call("needsScrollPageUpdate", value),
            call("getPagedLayoutTop", value),
            call("resolveScrollPageWindow", value, 2_000_000),
            call("resolveScrollPageWindow", value, 9_999_999.7),
          );
        }
        scenarios.push({ name: `paged ${envName(env)} height ${scrollHeight} offset ${scrollPageOffset}`, env, options: {}, steps });
      }
    }
  }
  return scenarios;
}

function rebaseThroughItems() {
  const scenarios = [];
  const files = Array.from({ length: 12 }, (_, index) => spec(`f${index}`, QUERY_DIFFS_SMALL[index % QUERY_DIFFS_SMALL.length]));
    for (const env of [ENVS[0], ENVS[2]]) {
    for (const [name, layout] of [["gap 1M", "gapMillion"], ["gap 4M", "gapFourMillion"]]) {
      scenarios.push({
        name: `rebase through items ${envName(env)} ${name}`,
        env,
        options: { layout },
        steps: [
          { op: "setItems", items: files },
          recompute,
          { op: "setRoot", scrollTop: 0, height: 800 },
          call("getMaxScrollTop"),
          call("shouldRebaseScroll"),
          ...[0, 1_000_000, 6_000_000, 10_999_999, 11_000_001, 11_500_000, 12_000_000, 30_000_000, 40_000_000].flatMap((scrollTop) => [
            { op: "writeRoot", scrollTop },
            { op: "setState", state: { scrollDirty: true } },
            call("getScrollTop"),
            call("resolvePagedScrollPosition", scrollTop),
            call("needsScrollPageUpdate", scrollTop),
          ]),
          { op: "setState", state: { scrollPageOffset: 9_000_000 } },
          call("resolvePagedScrollPosition", 9_400_000),
          call("resolvePagedScrollPosition", 9_000_100),
          call("resolvePagedScrollPosition", 20_900_000),
          call("getPagedLayoutTop", 9_000_300),
          { op: "setState", state: { scrollPageOffset: 0 } },
          call("getPagedLayoutTop", 9_000_300),
        ],
      });
    }
  }
  return scenarios;
}

function cachedReads() {
  const scenarios = [];
  for (const env of [ENVS[0], ENVS[1]]) {
    scenarios.push({
      name: `cached reads ${envName(env)}`,
      env,
      options: {},
      steps: [
        { op: "setItems", items: ABC },
        recompute,
        call("getScrollTop"),
        call("getHeight"),
        { op: "setRoot", scrollTop: 123.3, height: 600 },
        call("getScrollTop"),
        call("getHeight"),
        { op: "writeRoot", scrollTop: 500, height: 900 },
        call("getScrollTop"),
        call("getHeight"),
        { op: "setState", state: { scrollDirty: true, heightDirty: true } },
        call("getScrollTop"),
        call("getHeight"),
        { op: "writeRoot", scrollTop: 1e9 },
        { op: "setState", state: { scrollDirty: true, scrollPageOffset: 40 } },
        call("getScrollTop"),
        { op: "writeRoot", scrollTop: -30 },
        { op: "setState", state: { scrollDirty: true } },
        call("getScrollTop"),
        { op: "setState", state: { header: 50, footer: 20 } },
        call("getMaxScrollTop"),
        { op: "setOptions", options: { layout: "roomy" } },
        recompute,
        call("getMaxScrollTop"),
        call("getMaxScrollTopForHeight", 0),
      ],
    });
  }
  return scenarios;
}

function targetList() {
  const aligns = [undefined, "start", "center", "end", "nearest"];
  const targets = [];
  for (const id of ["a", "b", "c"]) {
    for (const align of aligns) {
      for (const offset of [undefined, 25]) targets.push({ type: "item", id, ...(align ? { align } : {}), ...(offset !== undefined ? { offset } : {}) });
    }
  }
  for (const id of ["a", "c"]) {
    for (const lineNumber of [1, 12, 9999]) {
      for (const side of [undefined, "deletions"]) {
        for (const align of aligns) {
          targets.push({ type: "line", id, lineNumber, ...(side ? { side } : {}), ...(align ? { align } : {}), ...(lineNumber === 12 ? { offset: 7 } : {}) });
        }
      }
    }
  }
  const ranges = [{ start: 1, end: 1 }, { start: 3, end: 20 }, { start: 20, end: 3 }, { start: 5, end: 8, side: "deletions", endSide: "additions" }, { start: 2, end: 9999 }];
  for (const id of ["a", "b"]) {
    for (const range of ranges) {
      for (const align of aligns) targets.push({ type: "range", id, range, ...(align ? { align } : {}) });
    }
  }
  for (const position of [-50, 0, 120.4, 700, 1e7]) targets.push({ type: "position", position });
  targets.push({ type: "item", id: "zzz", align: "start" }, { type: "line", id: "zzz", lineNumber: 1 }, { type: "range", id: "zzz", range: { start: 1, end: 2 } });
  return targets;
}

function scrollTargets() {
  const scenarios = [];
  const targets = targetList();
  const setups = [
    { name: "plain", options: {} },
    { name: "sticky", options: { stickyHeaders: true } },
    { name: "sticky no file header", options: { stickyHeaders: true, diff: { disableFileHeader: true } } },
    { name: "roomy layout fractional metrics", options: { layout: "roomy", itemMetrics: "fractional", stickyHeaders: true } },
  ];
  for (const setup of setups) {
    for (const { scrollTop, height, header } of [
      { scrollTop: 0, height: 800, header: 0 },
      { scrollTop: 150, height: 300, header: 40 },
      { scrollTop: 400.5, height: 800, header: 0 },
    ]) {
      const steps = [
        { op: "setItems", items: ABC },
        recompute,
        { op: "setRoot", scrollTop, height },
        { op: "setState", state: { header } },
      ];
      for (const target of targets) {
        if (target.type !== "position") steps.push(call("getScrollTargetRect", target));
        steps.push(call("normalizeScrollTarget", target));
        if (target.align !== "nearest") steps.push(call("resolveScrollTargetTop", target));
      }
      scenarios.push({ name: `targets ${setup.name} scrollTop ${scrollTop} height ${height}`, options: setup.options, steps });
    }
  }
  // Behavior resolution: instant by default, smooth-auto by distance, reduced motion forces instant.
  for (const env of [{ dpr: 1 }, { dpr: 1, reducedMotion: true }]) {
    const steps = [{ op: "setRoot", scrollTop: 100, height: 800 }];
    for (const behavior of [undefined, "instant", "smooth", "smooth-auto"]) {
      for (const destination of [100, 5000, 8099, 8100, 8101, 1e6]) {
        steps.push(call("resolveEffectiveScrollBehavior", { type: "position", position: 0, ...(behavior ? { behavior } : {}) }, destination));
      }
    }
    scenarios.push({ name: `behavior ${envName({ userAgent: CHROME, ...env })}`, env, options: {}, steps });
  }
  // The sticky offset and the aligned position directly.
  scenarios.push({
    name: "sticky header offset and alignment",
    options: { stickyHeaders: true },
    steps: [
      { op: "setItems", items: ABC },
      recompute,
      { op: "setRoot", scrollTop: 0, height: 500 },
      call("getStickyHeaderOffset"),
      { op: "setOptions", options: { stickyHeaders: true, diff: { disableFileHeader: true } } },
      call("getStickyHeaderOffset"),
      { op: "setOptions", options: { stickyHeaders: false } },
      call("getStickyHeaderOffset"),
      { op: "setOptions", options: { stickyHeaders: true, itemMetrics: "upstreamTest" } },
      call("getStickyHeaderOffset"),
      ...["start", "center", "end", undefined].flatMap((align) =>
        [0, 30, -20].flatMap((offset) => [0, 24].map((stickyOffset) => call("resolveAlignedScrollPosition", 300, 40, align ?? null, offset, stickyOffset))),
      ),
      call("resolveAlignedScrollPosition", 300, 600, "center", 0, 0),
      call("resolveAlignedScrollPosition", 300, 499, "center", 1, 0),
    ],
  });
  return scenarios;
}

function spring() {
  const scenarios = [];
  for (const settings of Object.keys(SMOOTH)) {
    const steps = [];
    for (const animation of [
      { position: 0, velocity: 0, lastTimestamp: 1000 },
      { position: 500, velocity: 0.8, lastTimestamp: 1000 },
      { position: 100, velocity: -2, lastTimestamp: 50 },
    ]) {
      for (const destination of [0, 250.5, 1e4]) {
        for (const dt of [-30, 0, 1, 16, 1000, 100000]) steps.push(call("computeSpringStep", animation, destination, animation.lastTimestamp + dt));
      }
    }
    scenarios.push({ name: `spring step ${settings}`, approx: true, options: { smoothScrollSettings: settings }, steps });
  }
  // A smooth scroll to a position, frame by frame, until it settles; and the anchor correction.
  for (const settings of Object.keys(SMOOTH)) {
    for (const [name, frames, anchorDelta] of [
      ["60fps", Array.from({ length: 80 }, (_, index) => 1000 + (index + 1) * 16.67), 0],
      ["gappy", [1050, 1060, 2500, 2501, 9000, 90000], 0],
      ["anchored", Array.from({ length: 60 }, (_, index) => 1000 + (index + 1) * 16), 12.5],
    ]) {
      const steps = [
        { op: "setItems", items: ABC },
        recompute,
        { op: "setRoot", scrollTop: 0, height: 600 },
        { op: "setState", state: { pendingScrollTarget: { type: "position", position: 900 }, scrollAnimation: { position: 0, velocity: 0, lastTimestamp: 1000 } } },
      ];
      for (const [index, timestamp] of frames.entries()) {
        steps.push(call("computeTargetScrollTopForFrame", index * 7, timestamp));
        steps.push(call("advanceScrollAnimation", timestamp, index % 3 === 0 ? anchorDelta : 0));
      }
      scenarios.push({ name: `spring frames ${name} ${settings}`, approx: true, options: { smoothScrollSettings: settings }, steps });
    }
  }
  // No animation, no target, an unresolvable target.
  scenarios.push({
    name: "advance without animation or target",
    approx: true,
    options: {},
    steps: [
      { op: "setItems", items: ABC },
      recompute,
      { op: "setRoot", scrollTop: 40, height: 600 },
      call("advanceScrollAnimation", 100, 0),
      call("computeTargetScrollTopForFrame", 40, 100),
      { op: "setState", state: { pendingScrollTarget: { type: "item", id: "b", align: "center" } } },
      call("advanceScrollAnimation", 100, 0),
      call("computeTargetScrollTopForFrame", 40, 100),
      { op: "setState", state: { scrollAnimation: { position: 40, velocity: 0, lastTimestamp: 100 } } },
      call("computeTargetScrollTopForFrame", 40, 116),
      call("advanceScrollAnimation", 116, 0),
      call("advanceScrollAnimation", 100000, 0),
      { op: "setState", state: { pendingScrollTarget: { type: "item", id: "zzz" }, scrollAnimation: { position: 40, velocity: 0, lastTimestamp: 100 } } },
      call("computeTargetScrollTopForFrame", 40, 116),
      call("advanceScrollAnimation", 116, 0),
      { op: "setState", state: { pendingScrollTarget: { type: "line", id: "a", lineNumber: 30 } } },
      call("isPendingTargetSettled", { type: "line", id: "a", lineNumber: 30 }),
      call("isPendingTargetSettled", { type: "item", id: "zzz" }),
    ],
  });
  return scenarios;
}

function anchors() {
  const scenarios = [];
  const scrollTops = [-1, 0, 1, 15, 33, 60, 100, 137, 200, 333, 500, 777, 1000, 1500, 2500, 4000, 9000];
  const stateFor = (extra = {}) => ({ firstIndex: 0, lastIndex: 2, stickyTop: 0, stickyBottom: 900, stickyHeight: 900, scrollTop: 0, ...extra });
  for (const setup of [
    { name: "plain", options: {} },
    { name: "sticky", options: { stickyHeaders: true } },
    { name: "roomy", options: { layout: "roomy", stickyHeaders: true } },
  ]) {
    for (const header of [0, 44]) {
      const steps = [
        { op: "setItems", items: ABC },
        recompute,
        { op: "setRoot", scrollTop: 0, height: 700 },
        { op: "setState", state: { header, renderState: stateFor() } },
      ];
      for (const scrollTop of scrollTops) {
        steps.push(call("getScrollAnchor", scrollTop));
        steps.push(call("getScrollAnchorViewportTop", scrollTop + 20, scrollTop));
      }
      // The same positions with the first item replaced, removed, or all but the last gone.
      for (const available of [
        { a: "stale", b: "keep", c: "keep" },
        { a: "missing", b: "keep", c: "keep" },
        { a: "stale", b: "stale", c: "keep" },
        { a: "missing", b: "missing", c: "missing" },
        { a: "keep", b: "stale", c: "keep" },
      ]) {
        for (const scrollTop of scrollTops.filter((_, index) => index % 2 === 0)) steps.push({ ...call("getScrollAnchor", scrollTop), available });
      }
      // The render state gates the search.
      for (const renderState of [
        { firstIndex: -1 },
        { lastIndex: -1 },
        { stickyTop: -1 },
        { stickyBottom: -1 },
        { firstIndex: 1, lastIndex: 1 },
        { firstIndex: 0, lastIndex: 7 },
      ]) {
        steps.push({ op: "setState", state: { renderState: stateFor(renderState) } });
        steps.push(call("getScrollAnchor", 400), call("getScrollAnchor", 1000));
      }
      steps.push({ op: "setState", state: { renderState: stateFor() } });
      // Resolving anchors back to scroll positions.
      for (const anchor of [
        { type: "item", id: "a", viewportOffset: 0 },
        { type: "item", id: "b", viewportOffset: 33.5 },
        { type: "item", id: "c", viewportOffset: -12 },
        { type: "item", id: "zzz", viewportOffset: 0 },
        { type: "line", id: "a", lineNumber: 30, side: "additions", viewportOffset: 5 },
        { type: "line", id: "b", lineNumber: 2, viewportOffset: 0 },
        { type: "line", id: "c", lineNumber: 4, side: "deletions", viewportOffset: 90 },
        { type: "line", id: "a", lineNumber: 99999, viewportOffset: 0 },
        { type: "line", id: "zzz", lineNumber: 1, viewportOffset: 0 },
      ]) {
        steps.push(call("resolveAnchoredScrollTop", anchor));
      }
      // A pending layout anchor that is still available wins; a replaced one falls through to the walk.
      steps.push({ op: "setState", state: { pendingLayoutAnchor: { type: "item", id: "b", viewportOffset: 7 } } });
      steps.push(call("getScrollAnchor", 400), { ...call("getScrollAnchor", 400), available: { a: "keep", b: "stale", c: "keep" } });
      steps.push({ ...call("getScrollAnchor", 400), available: { a: "keep", b: "missing", c: "keep" } });
      steps.push({ ...call("getScrollAnchor", 0), available: { a: "keep", b: "stale", c: "keep" } });
      steps.push({ op: "setState", state: { pendingLayoutAnchor: null } });
      // Capturing: needs a root, items and no pending target.
      steps.push(call("capturePendingLayoutAnchor"));
      steps.push({ op: "writeRoot", scrollTop: 520 }, { op: "setState", state: { scrollDirty: true } });
      steps.push(call("capturePendingLayoutAnchor"));
      steps.push({ op: "setState", state: { pendingLayoutAnchor: null } });
      steps.push({ ...call("capturePendingLayoutAnchor"), available: { a: "stale", b: "keep", c: "keep" } });
      steps.push({ op: "setState", state: { pendingLayoutAnchor: null, pendingScrollTarget: { type: "item", id: "c", align: "start" } } });
      steps.push(call("capturePendingLayoutAnchor"));
      steps.push({ op: "setState", state: { pendingScrollTarget: null, pendingLayoutAnchor: null } });
      scenarios.push({ name: `anchors ${setup.name} header ${header}`, options: setup.options, steps });
    }
  }
  // Settled detection against the scroll position.
  for (const env of [ENVS[0], ENVS[1]]) {
    const steps = [{ op: "setItems", items: ABC }, recompute, { op: "setRoot", scrollTop: 0, height: 700 }];
    for (const target of [
      { type: "item", id: "b", align: "start" },
      { type: "item", id: "c", align: "end", offset: 3 },
      { type: "line", id: "a", lineNumber: 40, align: "center" },
      { type: "position", position: 321.37 },
    ]) {
      for (const scrollTop of [0, 100, 321.37, 321, 321.4, 600]) {
        steps.push({ op: "writeRoot", scrollTop }, { op: "setState", state: { scrollDirty: true } }, call("isPendingTargetSettled", target));
      }
      steps.push({ op: "setState", state: { scrollDirty: true } }, call("resolveScrollTargetTop", target));
    }
    scenarios.push({ name: `settled ${envName(env)}`, env, options: {}, steps });
  }
  return scenarios;
}

function randomScenario(random, index) {
  const pool = QUERY_DIFFS_SMALL;
  const ids = ["a", "b", "c", "d"];
  const items = () => ids.filter(() => random.next() < 0.75).map((id) => spec(id, random.pick(pool), random.next() < 0.3 ? { annotations: random.pick(["A", "B"]) } : {}));
  const env = random.pick(ENVS);
  const steps = [{ op: "setItems", items: items() }, recompute, { op: "setRoot", scrollTop: random.int(2000), height: 300 + random.int(700) }];
  const target = () => {
    const id = random.pick([...ids, "zzz"]);
    const align = random.pick([undefined, "start", "center", "end", "nearest"]);
    const base = { id, ...(align ? { align } : {}), ...(random.next() < 0.3 ? { offset: random.int(60) - 20 } : {}) };
    switch (random.int(4)) {
      case 0:
        return { type: "item", ...base };
      case 1:
        return { type: "line", ...base, lineNumber: random.int(80), ...(random.next() < 0.3 ? { side: random.pick(["additions", "deletions"]) } : {}) };
      case 2:
        return { type: "range", ...base, range: { start: random.int(40), end: random.int(60) } };
      default:
        return { type: "position", position: random.int(5000) - 100 };
    }
  };
  const count = 14 + random.int(10);
  for (let step = 0; step < count; step++) {
    const roll = random.int(10);
    if (roll < 2) steps.push({ op: "setItems", items: items() }, recompute);
    else if (roll < 3) steps.push({ op: "setOptions", options: { stickyHeaders: random.pick([undefined, true, false]), layout: random.pick([undefined, "tight", "roomy"]) } }, recompute);
    else if (roll < 4) steps.push({ op: "writeRoot", scrollTop: random.int(4000), height: 300 + random.int(700) }, { op: "setState", state: { scrollDirty: true, heightDirty: true } });
    else if (roll < 5) {
      steps.push({ op: "setState", state: { renderState: { firstIndex: 0, lastIndex: random.int(4), stickyTop: random.pick([-1, 0, 100]), stickyBottom: random.pick([-1, 500]) } } });
    } else if (roll < 7) {
      const available = Object.fromEntries(ids.map((id) => [id, random.pick(["keep", "keep", "stale", "missing"])]));
      steps.push({ ...call("getScrollAnchor", random.int(3000) - 50), available });
      steps.push({ ...call("capturePendingLayoutAnchor"), available });
    } else {
      const picked = target();
      steps.push(call("normalizeScrollTarget", picked));
      if (picked.type !== "position") steps.push(call("getScrollTargetRect", picked));
      if (picked.align !== "nearest") steps.push(call("resolveScrollTargetTop", picked));
    }
  }
  return { name: `random${index}`, env, options: {}, steps };
}

export async function buildS12dCodeViewScroll(oracle, random, corpusMocks) {
  const { CodeView } = await import(oracle.pierre("components/CodeView.js"));
  const { resetPlatformDetection } = await import(oracle.pierre("utils/platform.js"));
  const diffs = await buildDiffPool(oracle, random, corpusMocks);
  const scenarios = [
    ...pagedMath(),
    ...rebaseThroughItems(),
    ...cachedReads(),
    ...scrollTargets(),
    ...spring(),
    ...anchors(),
  ];
  for (let index = 0; index < 60; index++) scenarios.push(randomScenario(random, index));
  const cases = [];
  for (const scenario of scenarios) {
    const own = {};
    for (const step of scenario.steps) for (const entry of step.items ?? []) own[entry.diff] ??= clone(diffs[entry.diff]);
    cases.push({ scenario: clone(scenario), ...(await runScenario({ CodeView, resetPlatformDetection }, own, scenario)) });
  }
  const metrics = Object.fromEntries(Object.entries(METRICS).filter(([, value]) => value !== undefined));
  return { diffs, metrics, layouts: D_LAYOUTS, lists: LISTS, smooth: Object.fromEntries(Object.entries(SMOOTH).filter(([, value]) => value !== undefined)), cases };
}
