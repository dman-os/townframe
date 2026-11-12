// Differential scenarios for the integration of `VirtualizedFileDiff` with
// `FileDiff` and the `Virtualizer` (S12b-3): `render`, `rerender`, `setOptions`,
// `cleanUp`, `expandHunk`, `finalizeRender`, `onRender`, `setVisibility`,
// connecting to and disconnecting from the virtualizer, and the frames the
// virtualizer drives them with.
//
// The published `VirtualizedFileDiff` runs over the published `Virtualizer` and
// jsdom 26.1.0 (the world of `s12b_render.mjs`: the published `FileDiff`'s
// DOM, with its renderer replaced by the synchronous semantics of the Rust
// `DiffHunksRenderer`), in the scripted world of the S10 oracle: declared
// geometry (no layout; the heights and tops of elements are the functions of
// `ruleHeight` and `ruleTop` below, which `fake_virtualized_host.rs` repeats), the
// window as the scroll root (`scrollY`, `innerHeight`, `scrollHeight`,
// `scrollTo` clamped), observers and listeners that only record, and a
// `requestAnimationFrame` that runs when a step says so. A scenario is a list
// of steps over one or two instances: scripted scroll, resize, geometry,
// intersection and resize-observer deliveries, frames, and the instance's own
// API (`render`, `setOptions`, `setThemeType`, `setLineAnnotations`,
// `expandHunk`, `rerender`, `cleanUp`, `revealLine`, `isLineRenderable`,
// `reconcileHeights`). After every step the oracle records the step's value, the
// trace since the previous step (the host calls of the virtualizer, the
// managers and the callbacks the options carry, and the frames requested and
// cancelled), the instance's observable state (the layout cache, visibility,
// setup, render range, rendered, input and pending diffs, annotation lists,
// expanded hunks) and the virtualizer's (the observers and visible instances it
// holds, its window), and the tree of every named container. The Rust port replays the
// scenarios (`components/file_diff_render_oracle_tests/virtualized.rs`).
//
// What this establishes: the order of the instance's decisions and calls into
// the virtualizer, the managers and the document, for the same geometry. What it
// cannot: a browser's layout, observers and scroll behaviour (declared here).
// Generated before any oracle that measures the scrollbar gutter (the published
// module caches the measurement for the whole process; unconnected containers,
// as here, never measure it).

import { clone } from "./s11a_cases.mjs";
import { buildInstanceOptions, buildTemplates, freshPool, installSynchronousRenderer, orderedTrace, snapshotTree, withWorld } from "./s12b_render.mjs";

// ---- the geometry rules (repeated by fake_virtualized_host.rs) ---------------------

const leadingIndex = (element) => {
  const value = element.getAttribute("data-line-index");
  return value === null ? null : Number.parseInt(value, 10) || 0;
};

function ruleHeight(geo, element) {
  if (element.localName === "diffs-container") return geo.containerHeight;
  if (element.hasAttribute("data-line-annotation")) return 30;
  if (element.hasAttribute("data-no-newline")) return 16;
  const index = leadingIndex(element);
  if (index === null) return 0;
  return index % 5 === 0 ? 40 : 20;
}

function ruleTop(geo, element) {
  if (element.localName === "diffs-container") return geo.containerTop;
  const index = leadingIndex(element);
  return index === null ? geo.containerTop : geo.containerTop + 40 + 20 * index;
}

// ---- running a scenario ------------------------------------------------------------

function runScenario(JSDOM, modules, templates, scenario) {
  const { VirtualizedFileDiff, Virtualizer } = modules;
  return withWorld(JSDOM, modules, async (world) => {
    const pool = freshPool(templates);
    const names = new Map();
    for (const [name, diff] of Object.entries(pool.diffs)) names.set(diff, name);
    const annotationNames = new Map();
    for (const [name, list] of Object.entries(pool.annotations)) annotationNames.set(list, name);
    annotationNames.set(modules.EMPTY_LINE_ANNOTATIONS, "EMPTY");
    const run = {
      trace: world.trace,
      nameOf: world.nameOf,
      pendingLoad: undefined,
      callbacks: new Map(),
      callback(field, make) {
        if (!this.callbacks.has(field)) this.callbacks.set(field, make);
        return this.callbacks.get(field);
      },
      diffName: (diff) => names.get(diff) ?? "?",
    };
    const geo = { scrollTop: 0, viewportHeight: 600, scrollHeight: 5000, containerTop: 0, containerHeight: 0, ...scenario.geometry };
    const { window } = globalThis;

    // The window as the scroll root.
    const windowListeners = new Map();
    // `Virtualizer.getHeight` reads the global `innerHeight` (a bare `globalThis`).
    const savedInnerHeight = Object.getOwnPropertyDescriptor(globalThis, "innerHeight");
    Object.defineProperty(globalThis, "innerHeight", { configurable: true, get: () => geo.viewportHeight });
    Object.defineProperty(window, "scrollY", { configurable: true, get: () => geo.scrollTop });
    Object.defineProperty(window, "innerHeight", { configurable: true, get: () => geo.viewportHeight });
    Object.defineProperty(window.document.documentElement, "scrollHeight", { configurable: true, get: () => geo.scrollHeight });
    window.addEventListener = (type, listener) => {
      world.trace.push(["window.addEventListener", type]);
      windowListeners.set(type, listener);
    };
    window.removeEventListener = (type) => {
      world.trace.push(["window.removeEventListener", type]);
      windowListeners.delete(type);
    };
    // `Virtualizer.cleanUp` also removes a scroll listener from a document root
    // (`this.root?.removeEventListener`), which registered nothing; the port has no
    // such host call, so it is not traced.
    const documentRemove = window.document.removeEventListener;
    window.document.removeEventListener = function (type, ...rest) {
      if (type === "scroll") return;
      return documentRemove.call(this, type, ...rest);
    };
    window.scrollTo = ({ top, behavior }) => {
      world.trace.push(["window.scrollTo", top, behavior]);
      geo.scrollTop = Math.min(Math.max(top, 0), Math.max(0, geo.scrollHeight - geo.viewportHeight));
    };
    window.HTMLElement.prototype.getBoundingClientRect = function () {
      const height = ruleHeight(geo, this);
      const top = ruleTop(geo, this) - geo.scrollTop;
      return { top, height, bottom: top + height, left: 0, right: 0, width: 0, x: 0, y: top };
    };
    Object.defineProperty(window.HTMLElement.prototype, "offsetHeight", {
      configurable: true,
      get() {
        return ruleHeight(geo, this);
      },
    });

    // Frames run when a step says so.
    const frames = new Map();
    let nextFrameId = 0;
    world.requestAnimationFrame = (callback) => {
      const id = ++nextFrameId;
      frames.set(id, callback);
      world.trace.push(["raf.request", id]);
      return id;
    };
    world.cancelAnimationFrame = (id) => {
      world.trace.push(["raf.cancel", id]);
      frames.delete(id);
    };
    const runFrames = (limit) => {
      for (let round = 0; round < limit && frames.size > 0; round++) {
        const pending = [...frames.values()];
        frames.clear();
        for (const callback of pending) callback(16 * (round + 1));
      }
    };

    const containers = {};
    const container = (name) => {
      if (name === undefined) return undefined;
      if (!containers[name]) {
        containers[name] = document.createElement(name === "W" ? "div" : "diffs-container");
        world.labels.set(containers[name], name);
      }
      return containers[name];
    };
    const virtualizer = new Virtualizer();
    const instances = [];
    const makeInstance = (spec) => {
      const instance = new VirtualizedFileDiff(
        buildInstanceOptions(spec ?? {}, run),
        virtualizer,
        undefined,
        undefined,
        scenario.managed ? () => world.trace.push(["ownerRender"]) : undefined,
      );
      installSynchronousRenderer(instance, modules);
      instances.push(instance);
      return instance;
    };
    makeInstance(scenario.options);

    const input = (step) => {
      const props = {};
      if (step.diff !== undefined) props.fileDiff = pool.diffs[step.diff];
      if (step.files !== undefined) {
        const source = pool.files[step.files];
        props.oldFile = source.old;
        props.newFile = source.new;
      }
      if (step.annotations !== undefined) props.lineAnnotations = pool.annotations[step.annotations];
      if (step.range !== undefined) props.renderRange = { ...step.range, totalLines: step.range.totalLines ?? Number.POSITIVE_INFINITY };
      if (step.force) props.forceRender = true;
      if (step.defer) props.deferManagers = true;
      if (step.noEmit) props.preventEmit = true;
      if (step.container !== undefined) props.fileContainer = container(step.container);
      return props;
    };

    const nameOfDiff = (diff) => (diff == null ? null : (names.get(diff) ?? "parsed"));
    const stateOf = (instance) => {
      const rangeOf = (range) => (range ? [range.startingLine, Number.isFinite(range.totalLines) ? range.totalLines : null, range.bufferBefore, range.bufferAfter] : null);
      const cache = instance.cache;
      return {
        top: instance.top ?? null,
        height: instance.height,
        layoutDirty: instance.layoutDirty,
        isVisible: instance.isVisible,
        isSetup: instance.isSetup,
        measuredTotal: cache.measuredHeightDeltaTotal,
        fileAnnotationHeight: cache.fileAnnotationHeight,
        deltas: Array.from(cache.heightDeltas.entries()).sort((a, b) => a[0] - b[0]),
        estimates: [cache.estimatedSplitHeight ?? null, cache.estimatedUnifiedHeight ?? null],
        totalLines: cache.totalLines,
        checkpoints: cache.checkpoints.length,
        renderRange: rangeOf(instance.renderRange),
        forceRenderOverride: instance.forceRenderOverride === true,
        inputDiff: nameOfDiff(instance.fileDiff),
        renderedDiff: nameOfDiff(instance.renderedData?.fileDiff),
        renderedAnnotations: instance.renderedData ? (annotationNames.get(instance.renderedData.lineAnnotations) ?? "?") : null,
        pendingData: instance.pendingData ? [nameOfDiff(instance.pendingData.fileDiff), annotationNames.get(instance.pendingData.lineAnnotations) ?? "?"] : null,
        pendingRender: instance.pendingRender ? [nameOfDiff(instance.pendingRender.workingDiff), nameOfDiff(instance.pendingRender.diff)] : null,
        enabled: instance.enabled,
        mounted: instance.mounted,
        expanded: Array.from(instance.hunksRenderer.expandedHunks.entries())
          .map(([index, region]) => [index, region.fromStart, region.fromEnd])
          .sort((a, b) => a[0] - b[0]),
        virtualizer: {
          observers: virtualizer.observers.size,
          visible: virtualizer.visibleInstances.size,
          window: [virtualizer.windowSpecs.top, virtualizer.windowSpecs.bottom],
        },
      };
    };

    const snapshots = [];
    let previousTrees = "{}";
    const record = (step, ret, instance) => {
      const trees = {};
      for (const [name, element] of Object.entries(containers)) trees[name] = snapshotTree(element);
      const serialized = JSON.stringify(trees);
      const same = serialized === previousTrees;
      previousTrees = serialized;
      const trace = world.trace.splice(0);
      snapshots.push({
        ret,
        trace: orderedTrace(trace),
        state: stateOf(instance),
        trees: same ? "same" : trees,
      });
    };

    try {
    for (const step of scenario.steps) {
      const instance = instances[step.instance ?? 0] ?? makeInstance(step.options);
      let ret = null;
      switch (step.op) {
        case "setup":
          virtualizer.setup(window.document);
          break;
        case "virtualizerCleanUp":
          virtualizer.cleanUp();
          break;
        case "render":
          ret = instance.render(input(step));
          break;
        case "onRender":
          ret = instance.onRender(step.dirty);
          break;
        case "setOptions":
          instance.setOptions(buildInstanceOptions(step.spec, run));
          break;
        case "setThemeType":
          instance.setThemeType(step.themeType);
          break;
        case "setLineAnnotations":
          instance.setLineAnnotations(pool.annotations[step.annotations]);
          break;
        case "rerender":
          instance.rerender();
          break;
        case "expandHunk":
          instance.expandHunk(step.hunkIndex, step.direction, step.count);
          break;
        case "isLineRenderable":
          ret = instance.isLineRenderable(step.line);
          break;
        case "revealLine":
          ret = instance.revealLine(step.line);
          break;
        case "reconcile":
          ret = instance.reconcileHeights();
          break;
        case "filesLoaded": {
          const { resolve } = run.pendingLoad;
          run.pendingLoad = undefined;
          const source = pool.partials[step.files];
          resolve({ oldFile: source.old, newFile: source.new });
          await instance.pendingFiles?.promise;
          break;
        }
        case "cleanUp": {
          // The nodes the Rust instance frees and the published one leaves
          // behind (see `s12b_render.mjs`).
          const left = [instance.spriteSVG, instance.themeCSSStyle, instance.unsafeCSSStyle];
          instance.cleanUp(step.recycle === true);
          for (const node of left) node?.remove();
          break;
        }
        case "scroll":
          geo.scrollTop = Math.min(Math.max(step.top, 0), Math.max(0, geo.scrollHeight - geo.viewportHeight));
          windowListeners.get("scroll")?.();
          break;
        case "resize":
          geo.viewportHeight = step.height;
          windowListeners.get("resize")?.();
          break;
        case "geometry":
          for (const key of ["containerTop", "containerHeight", "scrollHeight"]) if (step[key] !== undefined) geo[key] = step[key];
          break;
        case "frames":
          runFrames(step.limit ?? 16);
          break;
        case "intersect":
          world.intersectionCallback?.([{ target: container(step.container ?? "A"), isIntersecting: step.visible }]);
          break;
        case "docResize":
          world.resizeCallback?.([{ target: window.document.documentElement, borderBoxSize: [{ blockSize: step.blockSize }] }]);
          break;
        default:
          throw new Error(`unknown step ${step.op}`);
      }
      record(step, ret, instance);
    }
    } finally {
      if (savedInnerHeight === undefined) delete globalThis.innerHeight;
      else Object.defineProperty(globalThis, "innerHeight", savedInnerHeight);
    }
    return snapshots;
  });
}

// ---- scenarios ---------------------------------------------------------------------

const SPECS = {
  base: {},
  unified: { diffStyle: "unified" },
  wrap: { overflow: "wrap" },
  annotated: { renderAnnotation: true },
  headers: { renderHeaderPrefix: true, renderHeaderMetadata: true },
  expanded: { expandUnchanged: true },
};

function handwritten() {
  const r = (extra) => ({ op: "render", container: undefined, ...extra });
  const first = (diff, extra) => ({ op: "render", container: "A", diff, ...extra });
  const settle = [{ op: "frames" }];
  const visible = { op: "intersect", visible: true };
  const hidden = { op: "intersect", visible: false };
  const scroll = (top) => ({ op: "scroll", top });
  return [
    { name: "connect before setup and the setup replays it", options: SPECS.base, geometry: { containerTop: 0 }, steps: [first("D9"), { op: "setup" }, ...settle, visible, ...settle] },
    { name: "setup first, then render", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, ...settle, scroll(800), ...settle, scroll(0), ...settle] },
    { name: "an off-screen instance renders a placeholder and a later scroll renders it", options: SPECS.base, geometry: { containerTop: 20000, scrollHeight: 30000 }, steps: [{ op: "setup" }, first("D9"), ...settle, scroll(18000), visible, ...settle, scroll(0), hidden, ...settle] },
    { name: "an off-screen placeholder is replaced by new data", options: SPECS.base, geometry: { containerTop: 20000 }, steps: [{ op: "setup" }, first("D6"), r({ diff: "D7" }), r({ diff: "D7" }), ...settle] },
    { name: "cleanUp mid scroll", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, scroll(400), { op: "cleanUp" }, ...settle, scroll(900), ...settle, { op: "render", instance: 1, options: SPECS.base, container: "A", diff: "D9" }, ...settle] },
    { name: "strict mode: cleanUp and mount again over the same container", options: SPECS.base, steps: [first("D9"), { op: "setup" }, visible, ...settle, { op: "cleanUp" }, { op: "virtualizerCleanUp" }, { op: "setup" }, { op: "render", instance: 1, options: SPECS.base, container: "A", diff: "D9" }, visible, ...settle, { op: "cleanUp", instance: 1 }, { op: "virtualizerCleanUp" }, ...settle] },
    { name: "recycling keeps the layout and the data", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, ...settle, { op: "cleanUp", recycle: true }, ...settle] },
    { name: "options that change the layout and ones that do not", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, ...settle, { op: "setOptions", spec: SPECS.unified }, ...settle, { op: "setOptions", spec: { ...SPECS.unified, diffIndicators: "classic" } }, ...settle, { op: "setOptions", spec: { ...SPECS.unified, diffIndicators: "classic", disableBackground: true } }, ...settle, { op: "setOptions", spec: SPECS.wrap }, ...settle, { op: "setThemeType", themeType: "dark" }, ...settle] },
    { name: "annotations arrive and leave", options: SPECS.annotated, steps: [{ op: "setup" }, first("D9", { annotations: "A0" }), visible, ...settle, { op: "setLineAnnotations", annotations: "A1" }, ...settle, { op: "setLineAnnotations", annotations: "E" }, ...settle, r({ diff: "D9", annotations: "A3" }), ...settle, { op: "reconcile" }] },
    { name: "expanding hunks", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, ...settle, { op: "expandHunk", hunkIndex: 1, direction: "up", count: 5 }, ...settle, { op: "expandHunk", hunkIndex: 1, direction: "down", count: 3 }, ...settle, { op: "expandHunk", hunkIndex: 3, direction: "up", count: 2 }, ...settle, { op: "revealLine", line: 100 }, ...settle, { op: "isLineRenderable", line: 100 }, { op: "isLineRenderable", line: 40 }] },
    { name: "expandHunk before anything rendered does nothing", options: SPECS.base, steps: [{ op: "expandHunk", hunkIndex: 0, direction: "up", count: 5 }, { op: "setup" }, first("D9"), visible, ...settle, { op: "rerender" }, ...settle] },
    { name: "a resize and a document resize", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, ...settle, { op: "resize", height: 900 }, ...settle, { op: "docResize", blockSize: 123 }, ...settle, { op: "geometry", containerHeight: 4000, scrollHeight: 7000 }, { op: "docResize", blockSize: 7000 }, ...settle, { op: "resize", height: 300 }, ...settle] },
    { name: "wrapped rows and annotation rows are measured", options: { ...SPECS.annotated, overflow: "wrap" }, steps: [{ op: "setup" }, first("D1", { annotations: "A1" }), visible, ...settle, scroll(100), ...settle, { op: "reconcile" }, { op: "setOptions", spec: { ...SPECS.annotated, overflow: "scroll" } }, ...settle, { op: "reconcile" }] },
    { name: "an owner-managed instance asks its owner to render", options: SPECS.annotated, managed: true, steps: [{ op: "setup" }, first("D9", { annotations: "A0" }), visible, ...settle, { op: "rerender" }, ...settle, { op: "setOptions", spec: { ...SPECS.annotated, diffStyle: "unified" } }, ...settle] },
    { name: "visibility toggles", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, ...settle, hidden, ...settle, visible, ...settle, hidden, hidden, ...settle, { op: "onRender", dirty: true }, { op: "onRender", dirty: false }] },
    { name: "a render that changes nothing", options: { theme: "pierre-dark" }, steps: [{ op: "setup" }, first("D9"), visible, ...settle, r({ diff: "D9" }), r({ diff: "D9" }), ...settle, r({ diff: "D9", force: true }), ...settle] },
    { name: "a new diff of the same shape renders", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, ...settle, r({ diff: "D11" }), ...settle, r({ diff: "D9" }), r({ diff: "D11", annotations: "A0" }), ...settle] },
    { name: "the caller's render range wins", options: SPECS.base, steps: [{ op: "setup" }, first("D9", { range: { startingLine: 0, totalLines: 30, bufferBefore: 0, bufferAfter: 100 } }), visible, ...settle, r({ diff: "D9", range: { startingLine: 5, totalLines: 40, bufferBefore: 10, bufferAfter: 0 } }), r({ diff: "D9", range: { startingLine: 5, totalLines: 40, bufferBefore: 10, bufferAfter: 0 } }), ...settle] },
    { name: "files render and compare by content", options: SPECS.base, steps: [{ op: "setup" }, { op: "render", container: "A", files: "F6" }, visible, ...settle, r({ files: "F6" }), r({ files: "F7" }), ...settle, r({ files: "F7" }), r({ files: "F3" }), ...settle] },
    { name: "files render off-screen as a placeholder", options: SPECS.base, geometry: { containerTop: 20000 }, steps: [{ op: "setup" }, { op: "render", container: "A", files: "F6" }, r({ files: "F7" }), r({ files: "F7" }), visible, ...settle] },
    { name: "loading the files of a partial diff", options: { loadDiffFiles: true, expandUnchanged: true }, steps: [{ op: "setup" }, first("P0"), visible, ...settle, { op: "filesLoaded", files: "P0" }, ...settle, r({ diff: "P0" }), ...settle] },
    { name: "expanding a hunk of a partial diff loads its files", options: { loadDiffFiles: true }, steps: [{ op: "setup" }, first("P0"), visible, ...settle, { op: "expandHunk", hunkIndex: 0, direction: "up", count: 3 }, ...settle, { op: "filesLoaded", files: "P0" }, ...settle] },
    { name: "a collapsed file", options: { collapsed: true }, steps: [{ op: "setup" }, first("D9"), visible, ...settle, { op: "setOptions", spec: {} }, ...settle, scroll(1000), ...settle] },
    { name: "the container moves", options: SPECS.base, steps: [{ op: "setup" }, first("D9"), visible, ...settle, { op: "geometry", containerTop: 3000 }, scroll(10), { op: "intersect", visible: false }, ...settle, { op: "geometry", containerTop: 100 }, scroll(0), visible, ...settle] },
  ];
}

function randomScenario(random, index) {
  const spec = { ...random.pick([SPECS.base, SPECS.base, SPECS.unified, SPECS.wrap, SPECS.annotated, SPECS.headers, SPECS.expanded]) };
  if (random.next() < 0.4) spec.theme = random.pick(["pierre-dark", { dark: "pierre-dark", light: "pierre-light" }]);
  const geometry = {
    containerTop: random.pick([0, 0, 200, 1500, 6000, 20000]),
    viewportHeight: random.pick([300, 600, 900]),
    scrollHeight: random.pick([3000, 8000, 30000]),
    containerHeight: random.pick([0, 0, 2000]),
  };
  const diffs = ["D9", "D9", "D10", "D11", "D6", "D7", "D1", "D0"];
  const steps = [];
  if (random.next() < 0.7) steps.push({ op: "setup" });
  steps.push({ op: "render", container: "A", diff: random.pick(diffs), ...(random.next() < 0.3 ? { annotations: random.pick(["A0", "A1", "E"]) } : {}) });
  if (random.next() < 0.7) steps.push({ op: "setup" });
  steps.push({ op: "intersect", visible: random.next() < 0.8 });
  steps.push({ op: "frames" });
  let cleaned = false;
  const count = 8 + random.int(22);
  for (let at = 0; at < count; at++) {
    const roll = random.next();
    const instance = cleaned ? 1 : undefined;
    const move = (step) => steps.push(cleaned ? { ...step, instance } : step);
    if (roll < 0.22) {
      steps.push({ op: "scroll", top: random.int(geometry.scrollHeight) });
      if (random.next() < 0.8) steps.push({ op: "frames" });
    } else if (roll < 0.28) {
      steps.push({ op: "resize", height: random.pick([300, 600, 900, 1200]) });
      steps.push({ op: "frames" });
    } else if (roll < 0.33) steps.push({ op: "geometry", containerTop: random.pick([0, 400, 3000, 20000]), containerHeight: random.pick([0, 1500, 4000]), scrollHeight: random.pick([3000, 8000, 30000]) });
    else if (roll < 0.43) steps.push({ op: "frames", limit: random.pick([1, 2, 16]) });
    else if (roll < 0.5) steps.push({ op: "intersect", visible: random.next() < 0.6 });
    else if (roll < 0.58) move({ op: "render", ...(random.next() < 0.12 ? { files: random.pick(["F6", "F7", "F0"]) } : { diff: random.pick(diffs) }), ...(random.next() < 0.1 ? { range: { startingLine: random.int(40), totalLines: 10 + random.int(40), bufferBefore: random.int(50), bufferAfter: random.int(50) } } : {}), ...(random.next() < 0.3 ? { annotations: random.pick(["A0", "A1", "A2", "E"]) } : {}), ...(random.next() < 0.15 ? { force: true } : {}), ...(random.next() < 0.1 ? { defer: true } : {}) });
    else if (roll < 0.66) {
      const next = { ...random.pick([SPECS.base, SPECS.unified, SPECS.wrap, SPECS.annotated]) };
      if (random.next() < 0.3) next.diffIndicators = random.pick(["bars", "classic", "none"]);
      if (random.next() < 0.2) next.disableBackground = true;
      if (random.next() < 0.2) next.expandUnchanged = true;
      if (random.next() < 0.1) next.collapsed = true;
      move({ op: "setOptions", spec: next });
    } else if (roll < 0.7) move({ op: "setThemeType", themeType: random.pick(["system", "light", "dark"]) });
    else if (roll < 0.75) move({ op: "setLineAnnotations", annotations: random.pick(["A0", "A1", "A2", "E"]) });
    else if (roll < 0.82) move({ op: "expandHunk", hunkIndex: random.int(5), direction: random.pick(["up", "down", "both"]), ...(random.next() < 0.5 ? { count: 1 + random.int(8) } : {}) });
    else if (roll < 0.86) move({ op: "rerender" });
    else if (roll < 0.9) move({ op: "revealLine", line: random.int(320) });
    else if (roll < 0.93) move({ op: "isLineRenderable", line: random.int(320) });
    else if (roll < 0.96) move({ op: "reconcile" });
    else if (roll < 0.98) steps.push({ op: "docResize", blockSize: random.pick([100, 5000, 9000]) });
    else if (!cleaned) {
      steps.push({ op: "cleanUp", recycle: random.next() < 0.25 });
      cleaned = true;
      steps.push({ op: "frames" });
      if (random.next() < 0.7) steps.push({ op: "render", instance: 1, options: spec, container: "A", diff: random.pick(diffs) });
    }
  }
  steps.push({ op: "frames" });
  steps.push({ op: "cleanUp", instance: cleaned ? 1 : 0 });
  steps.push({ op: "frames" });
  return { name: `random ${index}`, options: spec, geometry, managed: random.next() < 0.1, steps };
}

export async function buildS12bVirtualized(oracle, random) {
  const { JSDOM } = await import(oracle.jsdom);
  const modules = {
    ...(await import(oracle.pierre("components/VirtualizedFileDiff.js"))),
    ...(await import(oracle.pierre("components/Virtualizer.js"))),
    ...(await import(oracle.pierre("components/FileDiff.js"))),
    ...(await import(oracle.pierre("managers/ResizeManager.js"))),
    ...(await import(oracle.pierre("managers/UniversalRenderingManager.js"))),
    ...(await import(oracle.pierre("constants.js"))),
    ...(await import(oracle.pierre("utils/isDiffPlainText.js"))),
    ...(await import(oracle.pierre("utils/renderDiffWithHighlighter.js"))),
  };
  const templates = await buildTemplates(oracle, random);
  // Longer files than the render oracle's, so windows and buffers matter.
  const { parseDiffFromFile } = await import(oracle.pierre("utils/parseDiffFromFile.js"));
  const numbered = (count, edits) =>
    Array.from({ length: count }, (_, index) => (edits.includes(index + 1) ? `edited ${index + 1}` : `line ${index + 1}`)).join("\n") + "\n";
  for (const [name, count, edits] of [
    ["D9", 320, [20, 90, 150, 230, 300]],
    ["D10", 120, [10, 60, 100]],
    ["D11", 320, [21, 91, 151, 231, 301]],
  ]) {
    const old = { name: "long.txt", contents: numbered(count, []) };
    const changed = { name: "long.txt", contents: numbered(count, edits) };
    templates.diffs[name] = JSON.parse(JSON.stringify(parseDiffFromFile(old, changed)));
  }
  const scenarios = handwritten();
  for (let index = 0; index < 120; index++) scenarios.push(randomScenario(random, index));
  const cases = [];
  for (const scenario of scenarios) {
    const snapshots = await runScenario(JSDOM, modules, templates, scenario);
    cases.push({ scenario, snapshots });
  }
  return { templates: clone(templates), cases };
}
