// Differential scenarios for the DOM shell of `CodeView` (C4): `setup`, `reset`,
// `cleanUp`, the root handlers (`handleScroll`, `clearPendingScroll`,
// `handleResize`), scroll interaction suspension (pointer events, the mobile
// Safari overflow variable, the 120 ms restore timer), the header and footer
// hosts (`reconcileHeaderFooterHosts`, `measureMountedHosts`, host resizes),
// the sticky slab (`getStickyBounds`, `applyStickyPositioning` with a scripted
// `Math.random`, `syncPagedScrollScaffolding`, `updateStickyPositioning`,
// `syncContainerHeight`, `reconcileRenderedItems`, `applyScrollFix`) and the
// element pool (`acquireElement`, `releaseRenderedItem` over shells holding
// shared assets, junk and focus, the generation counter, the limit).
//
// The published `CodeView` runs over jsdom 26.1.0 (the world of
// `s12b_render.mjs`) with a fake clock for `setTimeout`, a recording
// `ResizeObserver`, `addEventListener` tracing for the five root events, a
// scripted `Math.random`, scripted geometry (`getBoundingClientRect`,
// `scrollTop`, `scrollTo` that clamps like a browser) and a scripted
// `document.activeElement`. `render` is observed on the instance (the calls are
// recorded) and forwarded: the render pass runs, over item instances with the
// synchronous renderer semantics of the port. Item shells can also be mounted
// by the `mount` step (`acquireElement` and `syncRenderedItemOrder`, which is a
// module function upstream, so it is copied here), to set up pool states the
// scripted geometry would not reach. Each step records its value,
// the trace slice, the observable state, the tree of the root and the pool. The
// Rust port replays them in `components/code_view/shell_oracle_tests.rs`.
//
// Debugging a mismatch: `S12E_ONLY=<scenario name>` runs one scenario, `S12_FULL=1`
// snapshots every shell in full instead of by hash (the port's `snapshot_outline`
// answers the same variable), so two runs' trees can be diffed.
//
// What this establishes: the shell's DOM writes, observer and listener calls,
// timers and state transitions for the same inputs. What it cannot: layout (the
// geometry is declared). The render pass as a whole is `s12f_code_view_render.mjs`.

import { clone } from "./s11a_cases.mjs";
import { buildOptions } from "./s12a_file_diff.mjs";
import { METRICS, QUERY_DIFFS_SMALL, buildDiffPool } from "./s12b_layout.mjs";
import { LAYOUTS } from "./s12c_code_view.mjs";
import { installSynchronousRenderer, snapshotOutline, withWorld } from "./s12b_render.mjs";

const CHROME = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const IPHONE =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1";
const SHADOW_KINDS = {
  sprite: ["svg", [["data-shell-sprite", ""]]],
  theme: ["style", [["data-theme-css", ""]]],
  unsafe: ["style", [["data-unsafe-css", ""]]],
  core: ["style", [["data-core-css", ""]]],
  junkStyle: ["style", [["data-junk", ""]]],
  junkDiv: ["div", [["data-junk", ""]]],
  header: ["div", [["data-diffs-header", ""]]],
};
// The header and footer callbacks a scenario names: by identity, as upstream.
// The diffs the render pass is given: a partial diff carries no line arrays to render
// (a loader hydrates it; none runs here), and the published renderer would read
// `undefined` lines from it.
let POOL = QUERY_DIFFS_SMALL;
// The layouts a scenario names; `huge` puts a million pixels between items, to cross the
// scroll rebase thresholds with a few of them.
const LAYOUT_TABLE = { ...LAYOUTS, huge: { paddingTop: 8, paddingBottom: 8, gap: 1000000 } };
const UNSCRIPTED_RANDOM = 0.25;
const INTERACTION_EVENTS = new Set(["click", "pointerdown", "pointermove", "pointerleave"]);
const SLOT_CALLBACKS = { a: 11, b: 12, c: null };

// The height of a rendered row in the render scenarios (`env.rules`): the line rules of
// `s12b_virtualized.mjs`, repeated by `FakeDom`'s `rule_height`.
function ruleHeight(element) {
  if (element.hasAttribute("data-line-annotation")) return 30;
  if (element.hasAttribute("data-no-newline")) return 16;
  const value = element.getAttribute("data-line-index");
  if (value === null) return 0;
  return (Number.parseInt(value, 10) || 0) % 5 === 0 ? 40 : 20;
}

function encode(value) {
  return value === undefined ? null : JSON.parse(JSON.stringify(value));
}

async function runScenario(modules, JSDOM, diffs, scenario, oracle) {
  const { resetPlatformDetection } = modules;
  return withWorld(JSDOM, modules, async (world) => {
    const env = scenario.env ?? {};
    const saved = new Map();
    const install = (name, value) => {
      saved.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
      Object.defineProperty(globalThis, name, { value, configurable: true, writable: true });
    };
    install("navigator", {
      userAgent: env.userAgent ?? CHROME,
      platform: env.platform ?? "Linux x86_64",
      maxTouchPoints: env.maxTouchPoints ?? 0,
    });
    // `getPropertyValue`-free world: `window.devicePixelRatio`, `matchMedia`.
    world.document.defaultView.devicePixelRatio = env.dpr ?? 1;
    install("window", Object.assign(Object.create(world.document.defaultView), { devicePixelRatio: env.dpr ?? 1, matchMedia: () => ({ matches: false }) }));
    resetPlatformDetection();
    // `MOBILE_SAFARI` is evaluated once at module load: a scenario with its own
    // user agent imports the module under a cache-busting query, as
    // `CodeView.pointerEvents.test.ts` does.
    const { CodeView } =
      env.userAgent === undefined ? modules : await import(`${oracle.pierre("components/CodeView.js")}?ua=${encodeURIComponent(env.userAgent)}`);

    // The fake clock.
    let clock = 0;
    let nextTimer = 0;
    const timers = new Map();
    install("setTimeout", (callback, ms) => {
      const id = ++nextTimer;
      timers.set(id, [clock + ms, callback]);
      world.trace.push(["setTimeout", id, ms]);
      return id;
    });
    install("clearTimeout", (id) => {
      timers.delete(id);
      world.trace.push(["clearTimeout", id]);
    });
    const advance = (ms) => {
      const target = clock + ms;
      for (;;) {
        let next;
        for (const [id, [due]] of timers) if (due <= target && (next === undefined || due < timers.get(next)[0])) next = id;
        if (next === undefined) break;
        const [due, callback] = timers.get(next);
        timers.delete(next);
        clock = due;
        callback();
      }
      clock = target;
    };

    install("performance", { now: () => clock });
    // Frames run when a step says so, at the clock's time.
    const frames = new Map();
    let nextFrame = 0;
    world.requestAnimationFrame = (callback) => {
      const id = ++nextFrame;
      frames.set(id, callback);
      world.trace.push(["raf.request", id]);
      return id;
    };
    world.cancelAnimationFrame = (id) => {
      world.trace.push(["raf.cancel", id]);
      frames.delete(id);
    };
    const runFrames = (rounds) => {
      for (let round = 0; round < rounds && frames.size > 0; round++) {
        const pending = [...frames.values()];
        frames.clear();
        for (const callback of pending) callback(clock);
      }
    };

    // Scripted `Math.random`.
    const randoms = [];
    const savedRandom = Math.random;
    // The render pass positions the slab, which draws a value per call; a
    // scenario that does not script it gets `UNSCRIPTED_RANDOM` (the port's
    // `FakeDom::random` answers the same).
    Math.random = () => (randoms.length === 0 ? UNSCRIPTED_RANDOM : randoms.shift());

    // Geometry.
    const geometry = { scrollTop: 0, scrollHeight: 0, viewportHeight: 0, heights: new Map() };
    const elementProto = world.document.defaultView.Element.prototype;
    const savedRect = elementProto.getBoundingClientRect;
    elementProto.getBoundingClientRect = function () {
      return { height: geometry.heights.get(this) ?? (env.rules ? ruleHeight(this) : 0) };
    };
    // The five root listeners (the world traces `scroll` adds and every removal).
    const proto = world.document.defaultView.EventTarget.prototype;
    const worldAdd = proto.addEventListener;
    proto.addEventListener = function (type, ...rest) {
      // The viewer's own five root events (the items' managers listen elsewhere).
      if (world.nameOf(this) === "root" && ["wheel", "touchstart", "pointerdown", "keydown"].includes(type)) world.trace.push(["listener.add", world.nameOf(this), type]);
      return worldAdd.call(this, type, ...rest);
    };
    // `document.activeElement` and a shadow root's.
    const focus = { active: null };
    Object.defineProperty(world.document, "activeElement", { configurable: true, get: () => focus.active });

    const savedWarn = console.warn;
    const savedError = console.error;
    console.warn = () => {};
    console.error = () => {};
    try {
      const options = (spec) => {
        const built = buildOptions(spec.diff ?? {}, new Map());
        // The port's word diff is `similar`'s, not jsdiff's (PARITY.md, diffWordsWithSpace), so
        // the renders compared here carry no intra-line diff spans.
        built.lineDiffType ??= "none";
        if (spec.pointerEventsOnScroll !== undefined) built.pointerEventsOnScroll = spec.pointerEventsOnScroll;
        if (spec.itemMetrics !== undefined) built.itemMetrics = clone(METRICS[spec.itemMetrics]);
        if (spec.layout !== undefined) built.layout = clone(LAYOUT_TABLE[spec.layout]);
        if (spec.stickyHeaders !== undefined) built.stickyHeaders = spec.stickyHeaders;
        return built;
      };
      // Callbacks are shared objects per name, so a scenario can set the same one twice.
      const slotFns = {};
      for (const [name, value] of Object.entries(SLOT_CALLBACKS)) slotFns[name] = () => value ?? undefined;
      const withSlots = (built, spec) => {
        if (spec.header !== undefined) built.renderCodeViewHeader = spec.header === null ? undefined : slotFns[spec.header];
        if (spec.footer !== undefined) built.renderCodeViewFooter = spec.footer === null ? undefined : slotFns[spec.footer];
        return built;
      };
      const cv = new CodeView(withSlots(options(scenario.options), scenario.options));
      // `render` is observed and forwarded: the render pass runs.
      const renders = [];
      const render = cv.render.bind(cv);
      cv.render = (immediate = false) => {
        renders.push(immediate);
        render(immediate);
      };
      // The highlighter-readiness gate is not ported (`isReady`, dropped.psv): the
      // port's highlights backend is synchronous, so the pass always runs.
      cv.isReady = () => true;
      // Item instances render with the synchronous renderer semantics of the port.
      const createItem = cv.createItem.bind(cv);
      cv.createItem = (...args) => {
        const created = createItem(...args);
        installSynchronousRenderer(created.instance, modules);
        return created;
      };
      let root;
      const nodes = {};
      const target = (name) => {
        switch (name) {
          case "root":
            return root;
          case "sticky":
            return cv.stickyContainer;
          case "offset":
            return cv.stickyOffset;
          case "container":
            return cv.container;
          case "header":
            return cv.header.element;
          case "footer":
            return cv.footer.element;
          default:
            throw new Error(`unknown target ${name}`);
        }
      };
      const itemAt = (index) => cv.items[index];
      const item = (entry) => ({
        id: entry.id,
        type: "diff",
        fileDiff: diffs[entry.diff],
        ...(entry.version !== undefined ? { version: entry.version } : {}),
        ...(entry.collapsed !== undefined ? { collapsed: entry.collapsed } : {}),
      });
      const sticky = (name) => target(name);
      const windowOf = (value) => (value == null ? undefined : { top: value.top, bottom: value.bottom });
      const syncRenderedItemOrder = (container, element, prevElement) => {
        if (prevElement == null) {
          if (container.firstChild !== element) container.prepend(element);
          return;
        }
        if (prevElement.nextSibling !== element) prevElement.after(element);
      };
      const updatedSet = (indexes) => (indexes == null ? undefined : new Set(indexes.map(itemAt)));
      const apply = (step) => {
        switch (step.op) {
          case "setup": {
            root = world.document.createElement("div");
            world.labels.set(root, "root");
            nodes.root = root;
            Object.defineProperty(root, "scrollTop", { configurable: true, get: () => geometry.scrollTop, set: (value) => (geometry.scrollTop = value) });
            root.scrollTo = ({ top, behavior }) => {
              world.trace.push(["element.scrollTo", "root", top, behavior]);
              const extent = env.clamp
                ? (() => {
                    const px = (name) => Number.parseFloat(cv.container.style[name] || "0");
                    return px("height") + px("marginTop") + px("marginBottom");
                  })()
                : env.unclamped
                  ? Number.POSITIVE_INFINITY
                  : geometry.scrollHeight;
              geometry.scrollTop = Math.min(Math.max(top, 0), Math.max(0, extent - geometry.viewportHeight));
            };
            geometry.heights.set(root, geometry.viewportHeight);
            root.focus = () => world.trace.push(["focus", "root"]);
            if (step.tabindex !== undefined) root.setAttribute("tabindex", String(step.tabindex));
            if (step.overflowAnchor !== undefined) root.style.overflowAnchor = step.overflowAnchor;
            cv.setup(root);
            return null;
          }
          case "world":
            for (const key of ["scrollTop", "scrollHeight", "viewportHeight"]) if (step[key] !== undefined) geometry[key] = step[key];
            if (step.viewportHeight !== undefined) geometry.heights.set(root, step.viewportHeight);
            return null;
          case "setHeight":
            geometry.heights.set(target(step.target), step.height);
            return null;
          case "setItems":
            cv.setItems(step.items.map(item));
            return null;
          case "addItems":
            cv.addItems(step.items.map(item));
            return null;
          case "updateItem":
            return cv.updateItem(item(step.item));
          case "removeItem":
            return cv.removeItem(step.id);
          case "updateItemId":
            return cv.updateItemId(step.from, step.to);
          case "scrollTo":
            cv.scrollTo(clone(step.target));
            return null;
          case "render":
            cv.render(step.immediate ?? false);
            return null;
          case "frames":
            runFrames(step.rounds ?? 1);
            return null;
          case "viewport":
            geometry.viewportHeight = step.height;
            geometry.heights.set(root, step.height);
            return null;
          case "recompute":
            if (cv.layoutDirtyIndex != null) {
              cv.recomputeLayout(cv.layoutDirtyIndex, cv.pendingLayoutReset);
              cv.layoutDirtyIndex = undefined;
              cv.pendingLayoutReset = undefined;
            }
            return null;
          case "setOptions":
            cv.setOptions(withSlots(options(step.options), step.options));
            return null;
          case "setState": {
            const { state } = step;
            for (const key of ["scrollTop", "scrollPageOffset", "scrollDirty", "height", "heightDirty", "scrollHeight"]) {
              if (state[key] !== undefined) cv[key] = state[key];
            }
            if (state.renderState !== undefined) Object.assign(cv.renderState, state.renderState);
            for (const key of ["pendingScrollTarget", "scrollAnimation", "pendingLayoutAnchor"]) {
              if (key in state) cv[key] = state[key] === null ? undefined : clone(state[key]);
            }
            return null;
          }
          case "random":
            randoms.push(...step.values);
            return null;
          case "scroll":
            geometry.scrollTop = step.top;
            cv.handleScroll();
            return null;
          case "clearPendingScroll":
            cv.clearPendingScroll();
            return null;
          case "resize":
            // An entry for a host that is not mounted is skipped: its `undefined`
            // target would equal `header.element` upstream (a quirk the port
            // cannot express).
            cv.handleResize(
              step.entries
                .filter((entry) => target(entry.target) !== undefined)
                .map((entry) => ({ target: target(entry.target), borderBoxSize: [{ blockSize: entry.blockSize }] })),
            );
            return null;
          case "advance":
            advance(step.ms);
            return null;
          case "mount": {
            const record = itemAt(step.index);
            if (record === undefined || record.element != null) return null;
            record.element = cv.acquireElement();
            syncRenderedItemOrder(cv.stickyContainer, record.element, step.after === undefined ? undefined : itemAt(step.after)?.element);
            record.instance.virtualizedSetup();
            if (cv.renderState.firstIndex === -1 || step.index < cv.renderState.firstIndex) cv.renderState.firstIndex = step.index;
            if (step.index > cv.renderState.lastIndex) cv.renderState.lastIndex = step.index;
            return null;
          }
          case "release":
            if (itemAt(step.index) === undefined) return null;
            cv.releaseRenderedItem(itemAt(step.index));
            return null;
          case "decorate": {
            const element = itemAt(step.index)?.element;
            if (element == null) return null;
            const shadow = element.shadowRoot ?? element.attachShadow({ mode: "open" });
            for (const kind of step.shadow ?? []) {
              const [tag, attributes] = SHADOW_KINDS[kind];
              const child = tag === "svg" ? world.document.createElementNS("http://www.w3.org/2000/svg", "svg") : world.document.createElement(tag);
              for (const [name, value] of attributes) child.setAttribute(name, value);
              shadow.appendChild(child);
            }
            for (let index = 0; index < (step.light ?? 0); index++) {
              const child = world.document.createElement("span");
              child.setAttribute("slot", `s${index}`);
              element.appendChild(child);
            }
            if (step.focus !== undefined) {
              const inner = step.focus === "light" ? element.lastElementChild : step.focus === "self" ? element : null;
              if (step.focus === "shadow") Object.defineProperty(shadow, "activeElement", { configurable: true, value: shadow.lastElementChild });
              focus.active = step.focus === "shadow" ? null : inner;
            }
            return null;
          }
          case "unfocus":
            focus.active = null;
            return null;
          case "call": {
            switch (step.fn) {
              case "getStickyBounds":
                return encode(cv.getStickyBounds(windowOf(step.window)));
              case "applyStickyPositioning":
                return encode(cv.applyStickyPositioning(step.bounds));
              case "syncPagedScrollScaffolding":
                return encode(cv.syncPagedScrollScaffolding(windowOf(step.window)));
              case "applyScrollFix":
                return encode(cv.applyScrollFix(step.target, step.synced, windowOf(step.window)));
              case "reconcileRenderedItems":
                return encode(cv.reconcileRenderedItems(updatedSet(step.updated)));
              case "acquireAndDrop": {
                const element = cv.acquireElement();
                world.labels.set(element, `acquired${step.name}`);
                return null;
              }
              case "getElementPoolLimit":
                return cv.getElementPoolLimit();
              default:
                return encode(cv[step.fn]());
            }
          }
          case "reset":
            cv.reset();
            return null;
          case "cleanUp":
            cv.cleanUp();
            return null;
          default:
            throw new Error(`unknown step ${step.op}`);
        }
      };
      void sticky;

      const state = () => ({
        scrollTop: cv.scrollTop,
        scrollPageOffset: cv.scrollPageOffset,
        scrollDirty: cv.scrollDirty,
        height: cv.height,
        heightDirty: cv.heightDirty,
        scrollHeight: cv.scrollHeight,
        containerHeight: cv.containerHeight,
        pointerEventsDisabled: cv.pointerEventsDisabled,
        codeOverflowFix: cv.codeOverflowFix,
        shouldFixContainerFocus: cv.shouldFixContainerFocus,
        timer: cv.scrollInteractionFixTimer ?? null,
        header: { height: cv.header.height, element: cv.header.element != null, render: cv.header.render != null },
        footer: { height: cv.footer.height, element: cv.footer.element != null, render: cv.footer.render != null },
        poolSize: cv.elementPool.length,
        poolVersion: cv.elementPoolVersion,
        renderState: clone(cv.renderState),
        windowSpecs: clone(cv.windowSpecs),
        pendingScrollTarget: encode(cv.pendingScrollTarget),
        scrollAnimation: encode(cv.scrollAnimation),
        pendingLayoutAnchor: encode(cv.pendingLayoutAnchor),
        items: cv.items.map((record) => [record.item.id, record.top, record.height, record.element != null]),
        root: root === undefined ? null : snapshotOutline(root),
        pool: cv.elementPool.map((element) => snapshotOutline(element)),
        hasContainer: cv.container != null,
        renders: [...renders],
        clock,
        domScrollTop: geometry.scrollTop,
        pendingFrames: frames.size,
      });

      const steps = [];
      for (const step of scenario.steps) {
        world.trace.length = 0;
        let result;
        try {
          result = { value: apply(step) };
        } catch (error) {
          result = { throws: error.message };
        }
        // The items' interaction managers remove their own listeners (`click`,
        // `pointerdown`, ...) too; the port records those in the vocabulary of the
        // interaction oracle, so the shell's trace is the viewer's own (the root's).
        const trace = world.trace.splice(0).filter(([kind, name, type]) => !(kind === "listener.remove" && name !== "root" && INTERACTION_EVENTS.has(type)));
        steps.push({ ...result, trace, state: state() });
      }
      return steps;
    } finally {
      console.warn = savedWarn;
      console.error = savedError;
      Math.random = savedRandom;
      elementProto.getBoundingClientRect = savedRect;
      proto.addEventListener = worldAdd;
      for (const [name, descriptor] of saved) {
        if (descriptor === undefined) delete globalThis[name];
        else Object.defineProperty(globalThis, name, descriptor);
      }
      resetPlatformDetection();
    }
  });
}

// ---- scenarios ------------------------------------------------------------------

const spec = (id, diff) => ({ id, diff });
const call = (fn, extra = {}) => ({ op: "call", fn, ...extra });
const recompute = { op: "recompute" };
const ABC = [spec("a", "twoHunks"), spec("b", "multi3"), spec("c", "gaps1")];
const WORLD = { op: "world", scrollTop: 0, scrollHeight: 5000, viewportHeight: 600 };
const SETUP = [WORLD, { op: "setup" }];

function pointerEvents() {
  const scenarios = [];
  for (const [label, opts, env] of [
    ["default", {}, {}],
    ["opted out", { pointerEventsOnScroll: true }, {}],
    ["mobile safari", {}, { userAgent: IPHONE, platform: "iPhone", maxTouchPoints: 5 }],
    ["ipados", {}, { userAgent: IPHONE.replace("iPhone", "Macintosh"), platform: "MacIntel", maxTouchPoints: 5 }],
    ["chrome ios", {}, { userAgent: IPHONE + " CriOS/126", platform: "iPhone", maxTouchPoints: 5 }],
  ]) {
    scenarios.push({
      name: `pointer events ${label}`,
      env,
      options: opts,
      steps: [
        ...SETUP,
        { op: "scroll", top: 100 },
        { op: "advance", ms: 60 },
        { op: "scroll", top: 140 },
        { op: "advance", ms: 119 },
        { op: "advance", ms: 1 },
        { op: "scroll", top: 90 },
        { op: "setOptions", options: { pointerEventsOnScroll: true } },
        { op: "advance", ms: 150 },
        { op: "scroll", top: 10 },
        call("suspendScrollInteractions"),
        call("restoreScrollInteractions"),
        { op: "cleanUp" },
      ],
    });
  }
  scenarios.push({
    name: "setup arms",
    options: {},
    steps: [WORLD, { op: "setup", tabindex: 3, overflowAnchor: "auto" }, { op: "cleanUp" }, { op: "setup" }, { op: "setup" }, { op: "cleanUp" }, { op: "cleanUp" }],
  });
  return scenarios;
}

function hosts() {
  const scenarios = [];
  const seqs = [
    ["a", "a"],
    ["a", "b"],
    ["a", null],
    [null, "a"],
    ["c", "a"],
    ["a", "c"],
  ];
  for (const [header, footer] of seqs) {
    scenarios.push({
      name: `hosts ${header}/${footer}`,
      options: { header: header ?? undefined, footer: footer ?? undefined },
      steps: [
        { op: "setItems", items: ABC },
        recompute,
        ...SETUP,
        call("reconcileHeaderFooterHosts"),
        { op: "setHeight", target: "header", height: 40 },
        { op: "setHeight", target: "footer", height: 25.5 },
        call("measureMountedHosts"),
        call("reconcileHeaderFooterHosts"),
        { op: "setOptions", options: { header: header === "a" ? "b" : "a", footer: null } },
        call("reconcileHeaderFooterHosts"),
        { op: "setOptions", options: { header: null, footer: "a" } },
        call("reconcileHeaderFooterHosts"),
        { op: "setOptions", options: { header: "a", footer: "a" } },
        call("reconcileHeaderFooterHosts"),
        { op: "setHeight", target: "header", height: 0 },
        call("measureMountedHosts"),
        { op: "cleanUp" },
        call("reconcileHeaderFooterHosts"),
      ],
    });
  }
  // Host resizes: the header re-anchors, the footer only changes the range.
  for (const entryTarget of ["header", "footer"]) {
    scenarios.push({
      name: `host resize ${entryTarget}`,
      options: { header: "a", footer: "b" },
      steps: [
        { op: "setItems", items: ABC },
        recompute,
        { op: "world", scrollTop: 200, scrollHeight: 4000, viewportHeight: 500 },
        { op: "setup" },
        call("reconcileHeaderFooterHosts"),
        { op: "setHeight", target: "header", height: 30 },
        { op: "setHeight", target: "footer", height: 20 },
        call("measureMountedHosts"),
        { op: "setState", state: { renderState: { firstIndex: 0, lastIndex: 1 }, scrollDirty: true, heightDirty: true } },
        { op: "resize", entries: [{ target: entryTarget, blockSize: 30 }] },
        { op: "resize", entries: [{ target: entryTarget, blockSize: 77 }] },
        { op: "resize", entries: [{ target: entryTarget, blockSize: 5 }] },
        { op: "resize", entries: [{ target: "root", blockSize: 500 }] },
        { op: "resize", entries: [{ target: "header", blockSize: 90 }, { target: "footer", blockSize: 33 }, { target: "root", blockSize: 1 }] },
        { op: "cleanUp" },
      ],
    });
  }
  return scenarios;
}

function slab() {
  const scenarios = [];
  const metrics = ["default", "codeView", "fractional"];
  for (const itemMetrics of metrics) {
    for (const randomValue of [0, 0.31, 0.999]) {
      const windows = [
        { top: 0, bottom: 400 },
        { top: 90, bottom: 700 },
        { top: 5000, bottom: 5400 },
        { top: 260.5, bottom: 261 },
      ];
      const steps = [{ op: "setItems", items: ABC }, recompute, ...SETUP];
      steps.push(call("getStickyBounds"));
      for (const window of windows) {
        steps.push(call("getStickyBounds", { window }));
        steps.push({ op: "random", values: [randomValue] }, call("syncPagedScrollScaffolding", { window }));
      }
      steps.push(
        { op: "setState", state: { renderState: { firstIndex: 0, lastIndex: 2 } } },
        call("getStickyBounds"),
        { op: "random", values: [randomValue] },
        call("updateStickyPositioning"),
        call("updateStickyPositioning"),
        { op: "setState", state: { renderState: { firstIndex: 1, lastIndex: 1 } } },
        { op: "random", values: [randomValue] },
        call("updateStickyPositioning"),
        { op: "setState", state: { renderState: { firstIndex: -1, lastIndex: -1 } } },
        call("updateStickyPositioning"),
        call("syncContainerHeight"),
        { op: "random", values: [randomValue] },
        call("applyStickyPositioning", { bounds: { stickyTop: 120.5, stickyBottom: 80 } }),
        { op: "cleanUp" },
      );
      scenarios.push({ name: `slab ${itemMetrics} random ${randomValue}`, options: { itemMetrics }, steps });
    }
  }
  return scenarios;
}

function reconcileAndFix() {
  const scenarios = [];
  for (const dpr of [1, 1.5]) {
    scenarios.push({
      name: `reconcile and scroll fix dpr ${dpr}`,
      env: { dpr },
      options: { layout: "roomy" },
      steps: [
        { op: "setItems", items: ABC },
        recompute,
        { op: "world", scrollTop: 100, scrollHeight: 3000, viewportHeight: 500 },
        { op: "setup" },
        { op: "setState", state: { renderState: { firstIndex: 0, lastIndex: 1 }, scrollTop: 100, scrollDirty: false, scrollHeight: 3000 } },
        call("reconcileRenderedItems"),
        call("reconcileRenderedItems", { updated: [0] }),
        call("reconcileRenderedItems", { updated: [0, 1, 2] }),
        call("applyScrollFix", { target: 250.3, synced: 100, window: { top: 0, bottom: 800 } }),
        call("applyScrollFix", { target: 250.3, synced: 250.3, window: { top: 0, bottom: 800 } }),
        call("applyScrollFix", { target: 100, synced: 250.3, window: { top: 0, bottom: 800 } }),
        call("applyScrollFix", { target: 1e9, synced: 100, window: { top: 0, bottom: 800 } }),
        { op: "advance", ms: 200 },
        { op: "cleanUp" },
        call("applyScrollFix", { target: 5, synced: 100, window: { top: 0, bottom: 800 } }),
      ],
    });
  }
  // The sticky container branch of `handleResize`.
  scenarios.push({
    name: "sticky container resize",
    options: {},
    steps: [
      { op: "setItems", items: ABC },
      recompute,
      { op: "world", scrollTop: 300, scrollHeight: 3000, viewportHeight: 500 },
      { op: "setup" },
      { op: "setState", state: { renderState: { firstIndex: 0, lastIndex: 2, stickyHeight: 400, stickyTop: 0, stickyBottom: 400 }, scrollDirty: true, heightDirty: true } },
      { op: "resize", entries: [{ target: "sticky", blockSize: 400.5 }] },
      { op: "resize", entries: [{ target: "sticky", blockSize: 402 }] },
      { op: "setState", state: { scrollAnimation: { position: 300, velocity: 0.5, lastTimestamp: 10 } } },
      { op: "resize", entries: [{ target: "sticky", blockSize: 440 }] },
      { op: "clearPendingScroll" },
      { op: "cleanUp" },
    ],
  });
  return scenarios;
}

function pool() {
  const scenarios = [];
  const many = (count) => Array.from({ length: count }, (_, index) => spec(`i${index}`, POOL[index % POOL.length]));
  // Release into a pool of limit 8 (default metrics, a 600 px viewport: 600 + 400 over 44 is 24).
  for (const viewportHeight of [100, 600]) {
    const steps = [{ op: "setItems", items: many(30) }, recompute, { op: "world", scrollTop: 0, scrollHeight: 20000, viewportHeight }, { op: "setup" }, call("getElementPoolLimit")];
    for (let index = 0; index < 30; index++) {
      steps.push({ op: "mount", index, ...(index > 0 ? { after: index - 1 } : {}) });
      if (index % 3 === 0) steps.push({ op: "decorate", index, shadow: ["sprite", "theme", "junkStyle", "junkDiv", "header", "unsafe", "core"], light: 2 });
    }
    for (let index = 0; index < 30; index++) steps.push({ op: "release", index });
    steps.push({ op: "mount", index: 0 }, { op: "mount", index: 1, after: 0 }, call("acquireAndDrop", { name: "x" }), call("acquireAndDrop", { name: "y" }), { op: "cleanUp" });
    scenarios.push({ name: `pool viewport ${viewportHeight}`, options: {}, steps });
  }
  // Generation: options that clear the pool, and a release of a shell made before the clear.
  scenarios.push({
    name: "pool generation",
    options: {},
    steps: [
      { op: "setItems", items: many(6) },
      recompute,
      WORLD,
      { op: "setup" },
      { op: "mount", index: 0 },
      { op: "mount", index: 1, after: 0 },
      { op: "mount", index: 2, after: 1 },
      { op: "decorate", index: 0, shadow: ["sprite", "theme"] },
      { op: "release", index: 2 },
      { op: "setOptions", options: { diff: { themeType: "dark" } } },
      { op: "release", index: 0 },
      { op: "mount", index: 3, after: 1 },
      { op: "setOptions", options: { diff: { disableFileHeader: true } } },
      { op: "release", index: 1 },
      { op: "mount", index: 4 },
      { op: "reset" },
      { op: "mount", index: 0 },
      { op: "cleanUp" },
    ],
  });
  // Focus: a shell that owns focus asks for the root to take it back.
  for (const focus of ["self", "light", "shadow"]) {
    scenarios.push({
      name: `pool focus ${focus}`,
      options: {},
      steps: [
        { op: "setItems", items: many(3) },
        recompute,
        WORLD,
        { op: "setup" },
        { op: "mount", index: 0 },
        { op: "mount", index: 1, after: 0 },
        { op: "decorate", index: 0, shadow: ["sprite"], light: 1, focus },
        { op: "release", index: 1 },
        call("fixContainerFocus"),
        { op: "release", index: 0 },
        call("fixContainerFocus"),
        call("fixContainerFocus"),
        { op: "mount", index: 0 },
        { op: "reset" },
        { op: "cleanUp" },
      ],
    });
  }
  // Reorder: the shells move into place.
  scenarios.push({
    name: "pool order",
    options: {},
    steps: [
      { op: "setItems", items: many(4) },
      recompute,
      WORLD,
      { op: "setup" },
      { op: "mount", index: 2 },
      { op: "mount", index: 0 },
      { op: "mount", index: 1, after: 0 },
      { op: "mount", index: 3, after: 1 },
      { op: "release", index: 1 },
      { op: "mount", index: 1, after: 0 },
      { op: "cleanUp" },
    ],
  });
  return scenarios;
}

function randomScenario(random, index) {
  const pick = random.pick;
  const ids = Array.from({ length: 6 }, (_, at) => `i${at}`);
  // The first three are always there: the render state the steps write stays inside the list.
  const items = () => ids.filter((_, at) => at < 3 || random.next() < 0.8).map((id) => spec(id, pick(POOL)));
  const noRender = { op: "setState", state: { renderState: { firstIndex: -1, lastIndex: -1 } } };
  const steps = [{ op: "setItems", items: items() }, recompute, { op: "world", scrollTop: random.int(1000), scrollHeight: 2000 + random.int(5000), viewportHeight: 200 + random.int(600) }, { op: "setup" }];
  const mounted = new Set();
  const count = 16 + random.int(14);
  for (let step = 0; step < count; step++) {
    const roll = random.int(14);
    if (roll < 2) steps.push({ op: "scroll", top: random.int(3000) });
    else if (roll < 3) steps.push({ op: "advance", ms: random.int(160) });
    else if (roll < 4) steps.push({ op: "setOptions", options: { header: pick([null, "a", "b", "c"]), footer: pick([null, "a", "b"]) } }, call("reconcileHeaderFooterHosts"));
    else if (roll < 5) steps.push({ op: "setHeight", target: pick(["header", "footer", "sticky"]), height: random.int(80) }, call("measureMountedHosts"));
    else if (roll < 6) {
      const window = { top: random.int(2000), bottom: 0 };
      window.bottom = window.top + 200 + random.int(800);
      steps.push({ op: "random", values: [random.next()] }, call("syncPagedScrollScaffolding", { window }));
    } else if (roll < 7) steps.push({ op: "setState", state: { renderState: { firstIndex: random.int(3), lastIndex: 2 } } }, { op: "random", values: [random.next()] }, call("updateStickyPositioning"));
    else if (roll < 8) steps.push({ op: "resize", entries: [{ target: pick(["sticky", "root", "header", "footer"]), blockSize: random.int(500) }] });
    else if (roll < 10) {
      const at = random.int(4);
      if (!mounted.has(at)) {
        mounted.add(at);
        steps.push({ op: "mount", index: at }, { op: "decorate", index: at, shadow: random.next() < 0.5 ? ["sprite", "theme", "junkDiv"] : [], light: random.int(2) });
      }
    } else if (roll < 12) {
      const at = random.int(4);
      if (mounted.has(at)) {
        mounted.delete(at);
        steps.push({ op: "release", index: at });
      }
    } else if (roll < 13) steps.push({ op: "setItems", items: items() }, recompute, noRender);
    else steps.push({ op: "clearPendingScroll" }, call("restoreScrollInteractions"));
  }
  steps.push({ op: "cleanUp" });
  return { name: `random${index}`, options: { header: pick([undefined, "a"]), footer: pick([undefined, "b"]) }, steps };
}

/**
 * Runs `makeScenarios({ random, diffs, pool })` over the published class in this world and
 * returns the golden's content. `pool` is the names of the diffs a render may be given.
 */
export async function runScenarios(oracle, random, corpusMocks, makeScenarios) {
  const { JSDOM } = await import(oracle.jsdom);
  const modules = {
    ...(await import(oracle.pierre("components/CodeView.js"))),
    ...(await import(oracle.pierre("managers/ResizeManager.js"))),
    ...(await import(oracle.pierre("managers/UniversalRenderingManager.js"))),
    ...(await import(oracle.pierre("utils/platform.js"))),
    ...(await import(oracle.pierre("constants.js"))),
    ...(await import(oracle.pierre("utils/isDiffPlainText.js"))),
    ...(await import(oracle.pierre("utils/renderDiffWithHighlighter.js"))),
  };
  const diffs = await buildDiffPool(oracle, random, corpusMocks);
  POOL = QUERY_DIFFS_SMALL.filter((name) => !diffs[name].isPartial);
  const scenarios = makeScenarios({ random, diffs, pool: POOL });
  const cases = [];
  for (const scenario of scenarios) {
    if (process.env.S12E_ONLY && scenario.name !== process.env.S12E_ONLY) continue;
    const own = {};
    for (const step of scenario.steps) for (const entry of step.items ?? [step.item].filter(Boolean)) own[entry.diff] ??= clone(diffs[entry.diff]);
    cases.push({ scenario: clone(scenario), steps: await runScenario(modules, JSDOM, own, scenario, oracle) });
  }
  const metrics = Object.fromEntries(Object.entries(METRICS).filter(([, value]) => value !== undefined));
  return { diffs, metrics, layouts: LAYOUT_TABLE, slots: SLOT_CALLBACKS, cases };
}

export function buildS12eCodeViewShell(oracle, random, corpusMocks) {
  return runScenarios(oracle, random, corpusMocks, ({ random: draws }) => {
    const scenarios = [...pointerEvents(), ...hosts(), ...slab(), ...reconcileAndFix(), ...pool()];
    for (let index = 0; index < 80; index++) scenarios.push(randomScenario(draws, index));
    return scenarios;
  });
}
