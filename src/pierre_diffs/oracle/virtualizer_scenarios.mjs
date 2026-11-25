// Differential scenarios for the Virtualizer port (S10).
//
// Runs the published `Virtualizer` class (and `areVirtualWindowSpecsEqual`)
// from the pinned @pierre/diffs dist against a scripted fake DOM: declared
// geometry (no layout), observers and listeners that only record, a
// `requestAnimationFrame` that runs when a step says so, and scripted
// `SubscribedInstance`s. Each scenario is a list of steps; the result is the
// trace of everything the class did to the world and to its instances plus the
// value of every query step. The Rust port runs the same scenario against
// `fake_dom::FakeHost` and must produce the same trace
// (`components/virtualizer_oracle_tests.rs`).
//
// What this checks: when windows recompute, dirty-flag handling, observer and
// listener lifecycle, connect queueing, anchor selection and scroll fixing,
// reconcile-queue draining, render-queue frame requests. What it cannot check:
// a real browser's observers, layout and scroll behavior (the fake declares
// them), so DOM-bound behavior stays unverified here (see PARITY.md).

const ROOT = "root";

// ---- the fake world, one run ---------------------------------------------

function runScenario(api, scenario) {
  const { Virtualizer, clearRenderQueue } = api;
  const trace = [];
  const rec = (...entry) => trace.push(entry);

  // A previous scenario may leave a frame requested; clear it while the old
  // globals (and so the old trace) are still installed.
  clearRenderQueue();

  const world = {
    scrollTop: 0,
    viewportHeight: 0,
    scrollHeight: 0,
    rootTop: 0,
    elements: {},
  };
  const documentRoot = scenario.root === "document";
  function applyWorld(patch) {
    if (patch === undefined) return;
    for (const key of ["scrollTop", "viewportHeight", "scrollHeight", "rootTop"]) {
      if (patch[key] !== undefined) world[key] = patch[key];
    }
    for (const [name, element] of Object.entries(patch.elements ?? {})) {
      const slot = (world.elements[name] ??= {
        docTop: 0,
        height: 0,
        lines: [],
        firstChild: null,
      });
      if (element.docTop !== undefined) slot.docTop = element.docTop;
      if (element.height !== undefined) slot.height = element.height;
      if (element.lines !== undefined) slot.lines = element.lines;
      if (element.firstChild !== undefined) slot.firstChild = element.firstChild;
    }
  }
  applyWorld(scenario.world);

  const effectiveRootTop = () => (documentRoot ? 0 : world.rootTop);
  const scrollTo = (top) => {
    const max = Math.max(0, world.scrollHeight - world.viewportHeight);
    world.scrollTop = Math.min(Math.max(top, 0), max);
  };

  // Elements.
  class FakeHTMLElement {}
  class FakeDocument {
    constructor() {
      this.documentElement = element("documentElement");
    }
    addEventListener() {}
    removeEventListener() {}
  }
  const elements = new Map();
  function element(name) {
    let value = elements.get(name);
    if (value === undefined) {
      value = new FakeHTMLElement();
      value.name = name;
      value.dataset = {};
      elements.set(name, value);
      const isRoot = name === ROOT;
      const isDocumentElement = name === "documentElement";
      const hash = name.indexOf("#");
      if (hash !== -1) {
        // `file#lineIndex`
        const file = name.slice(0, hash);
        const lineIndex = name.slice(hash + 1);
        value.dataset.lineIndex = lineIndex;
        value.getBoundingClientRect = () => {
          const line = world.elements[file].lines.find(
            ([index]) => index === lineIndex,
          );
          return { top: line[1] - world.scrollTop + effectiveRootTop() };
        };
        return value;
      }
      Object.defineProperties(value, {
        scrollTop: { get: () => world.scrollTop },
        scrollHeight: { get: () => world.scrollHeight },
        offsetHeight: { get: () => world.elements[name].height },
        firstElementChild: {
          get: () => {
            const child = world.elements[name]?.firstChild;
            return child == null ? null : element(child);
          },
        },
      });
      value.getBoundingClientRect = () => {
        if (isRoot) {
          return {
            top: effectiveRootTop(),
            height: world.viewportHeight,
          };
        }
        const data = world.elements[name];
        return {
          top: data.docTop - world.scrollTop + effectiveRootTop(),
          height: data.height,
        };
      };
      value.scrollTo = ({ top, behavior }) => {
        rec("element.scrollTo", name, top, behavior);
        scrollTo(top);
      };
      value.addEventListener = (type, listener) => {
        if (isDocumentElement) return;
        rec("element.addEventListener", name, type);
        elementListeners.set(type, listener);
      };
      value.removeEventListener = (type) => {
        rec("element.removeEventListener", name, type);
        elementListeners.delete(type);
      };
      value.shadowRoot = {
        querySelectorAll: () =>
          (world.elements[name]?.lines ?? []).map(([index]) =>
            element(`${name}#${index}`),
          ),
        querySelector: (selector) => {
          const match = /data-line-index="([^"]*)"/.exec(selector);
          const line = world.elements[name]?.lines.find(
            ([index]) => index === match[1],
          );
          return line === undefined ? null : element(`${name}#${line[0]}`);
        },
      };
    }
    return value;
  }

  const elementListeners = new Map();
  const windowListeners = new Map();
  const fakeWindow = {
    addEventListener(type, listener) {
      rec("window.addEventListener", type);
      windowListeners.set(type, listener);
    },
    removeEventListener(type) {
      rec("window.removeEventListener", type);
      windowListeners.delete(type);
    },
    get scrollY() {
      return world.scrollTop;
    },
    get innerHeight() {
      return world.viewportHeight;
    },
    scrollTo({ top, behavior }) {
      rec("window.scrollTo", top, behavior);
      scrollTo(top);
    },
  };

  let resizeObserver;
  let intersectionObserver;
  class FakeResizeObserver {
    constructor(callback) {
      this.callback = callback;
      resizeObserver = this;
    }
    observe(target) {
      rec("resize.observe", target.name);
    }
    disconnect() {
      rec("resize.disconnect");
      resizeObserver = undefined;
    }
  }
  class FakeIntersectionObserver {
    constructor(callback, options) {
      this.callback = callback;
      intersectionObserver = this;
      rec("observers.create", {
        root: options.root instanceof FakeDocument ? "document" : options.root.name,
        threshold: options.threshold,
        rootMargin: options.rootMargin,
      });
    }
    observe(target) {
      rec("intersection.observe", target.name);
    }
    unobserve(target) {
      rec("intersection.unobserve", target.name);
    }
    disconnect() {
      rec("intersection.disconnect");
      intersectionObserver = undefined;
    }
  }

  // rAF.
  const frames = new Map();
  let nextFrameId = 0;
  const runRound = (time) => {
    const pending = [...frames.values()];
    frames.clear();
    for (const callback of pending) callback(time);
    return pending.length > 0;
  };

  const logged = [];
  const globals = {
    HTMLElement: FakeHTMLElement,
    Document: FakeDocument,
    ResizeObserver: FakeResizeObserver,
    IntersectionObserver: FakeIntersectionObserver,
    window: fakeWindow,
    requestAnimationFrame(callback) {
      const id = ++nextFrameId;
      frames.set(id, callback);
      rec("raf.request", id);
      return id;
    },
    cancelAnimationFrame(id) {
      rec("raf.cancel", id);
      frames.delete(id);
    },
  };
  const saved = new Map();
  for (const [key, value] of Object.entries(globals)) {
    saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
    Object.defineProperty(globalThis, key, {
      configurable: true,
      writable: true,
      value,
    });
  }
  saved.set("innerHeight", Object.getOwnPropertyDescriptor(globalThis, "innerHeight"));
  Object.defineProperty(globalThis, "innerHeight", {
    configurable: true,
    get: () => world.viewportHeight,
  });
  const savedLog = console.log;
  console.log = (message, id, payload) => {
    logged.push(id);
    rec("log", message, payload.sizeChange, payload.newSize);
  };

  try {
    const rootObject = documentRoot ? new FakeDocument() : element(ROOT);
    const virtualizer = new Virtualizer(scenario.config);
    const instances = new Map();
    const disposers = new Map();

    const instanceFor = (name) => {
      let instance = instances.get(name);
      if (instance !== undefined) return instance;
      const spec = scenario.instances[name];
      const calls = { render: 0, reconcile: 0 };
      instance = {
        spec,
        onRender(dirty) {
          rec("onRender", name, dirty);
          const call = calls.render++;
          const effect = spec.renderEffects?.[call];
          if (effect !== undefined) {
            applyWorld(effect.world);
            for (const op of effect.ops ?? []) run(op);
          }
          return spec.render?.[call] ?? false;
        },
        reconcileHeights() {
          rec("reconcileHeights", name);
          const call = calls.reconcile++;
          const effect = spec.reconcileEffects?.[call];
          if (effect !== undefined) {
            applyWorld(effect.world);
            for (const op of effect.ops ?? []) run(op);
          }
          return spec.reconcile?.[call] ?? false;
        },
        setVisibility(visible) {
          rec("setVisibility", name, visible);
        },
      };
      instances.set(name, instance);
      return instance;
    };

    function run(op) {
      switch (op.op) {
        case "setup":
          virtualizer.setup(
            rootObject,
            op.content === undefined || op.content === null
              ? undefined
              : element(op.content),
          );
          break;
        case "connect": {
          const instance = instanceFor(op.instance);
          disposers.set(
            op.instance,
            virtualizer.connect(element(instance.spec.container), instance),
          );
          break;
        }
        case "dispose":
          disposers.get(op.instance)();
          break;
        case "disconnect":
          virtualizer.disconnect(element(op.container));
          break;
        case "world":
          applyWorld(op.set);
          break;
        case "scrollEvent":
          elementListeners.get("scroll")?.();
          windowListeners.get("scroll")?.();
          break;
        case "windowResizeEvent":
          windowListeners.get("resize")?.();
          break;
        case "resize":
          resizeObserver?.callback(
            op.entries.map(({ target, blockSize }) => ({
              target: element(target),
              borderBoxSize: [{ blockSize }],
            })),
          );
          break;
        case "intersect":
          intersectionObserver?.callback(
            op.entries.map(({ target, isIntersecting }) => ({
              target: element(target),
              isIntersecting,
            })),
          );
          break;
        case "frame":
          runRound(op.time);
          break;
        case "frames": {
          let rounds = 0;
          while (rounds < op.limit && runRound(op.time + 16 * (rounds + 1))) {
            rounds++;
          }
          break;
        }
        case "instanceChanged":
          virtualizer.instanceChanged(instanceFor(op.instance), op.domDirty);
          break;
        case "requestHeightReconcile":
          virtualizer.requestHeightReconcile(instanceFor(op.instance));
          break;
        case "getWindowSpecs":
          rec("result", op.op, virtualizer.getWindowSpecs());
          break;
        case "getScrollTop":
          rec("result", op.op, virtualizer.getScrollTop());
          break;
        case "isInstanceVisible":
          rec("result", op.op, virtualizer.isInstanceVisible(op.top, op.height));
          break;
        case "getOffsetInScrollContainer":
          rec(
            "result",
            op.op,
            virtualizer.getOffsetInScrollContainer(element(op.element)),
          );
          break;
        case "scrollTo":
          virtualizer.scrollTo(
            op.behavior === undefined
              ? { top: op.top }
              : { top: op.top, behavior: op.behavior },
          );
          break;
        case "markDomDirty":
          virtualizer.markDOMDirty();
          break;
        case "cleanUp":
          virtualizer.cleanUp();
          break;
        default:
          throw new Error(`unknown op ${op.op}`);
      }
    }

    for (const op of scenario.steps) run(op);
  } finally {
    console.log = savedLog;
    for (const [key, descriptor] of saved) {
      if (descriptor === undefined) delete globalThis[key];
      else Object.defineProperty(globalThis, key, descriptor);
    }
  }
  return trace;
}

// ---- scenarios ------------------------------------------------------------

function stackedFiles(sizes, lineCount = 4) {
  const elements = {};
  let top = 0;
  sizes.forEach((height, index) => {
    elements[`f${index}`] = {
      docTop: top,
      height,
      lines: Array.from({ length: lineCount }, (_, line) => [
        String(line),
        top + 40 + line * 20,
      ]),
    };
    top += height;
  });
  return { elements, total: top };
}

function threeFileScenario(name, root, extra = {}) {
  const { elements, total } = stackedFiles([850, 1200, 3000]);
  return {
    name,
    root,
    world: {
      scrollTop: 0,
      viewportHeight: 800,
      scrollHeight: total,
      rootTop: 10,
      elements: { content: { firstChild: null }, ...elements },
    },
    instances: {
      a: { container: "f0", render: [true, false], reconcile: [true] },
      b: { container: "f1", render: [true], reconcile: [] },
      c: { container: "f2", render: [false, true], reconcile: [true, true] },
    },
    steps: [],
    ...extra,
  };
}

export function handwrittenScenarios() {
  const scenarios = [];
  const setupElement = { op: "setup", content: "content" };

  scenarios.push(
    threeFileScenario("element root connect, intersect, scroll", "element", {
      steps: [
        setupElement,
        { op: "connect", instance: "a" },
        { op: "connect", instance: "b" },
        { op: "connect", instance: "c" },
        { op: "frames", limit: 10, time: 0 },
        {
          op: "intersect",
          entries: [
            { target: "f0", isIntersecting: true },
            { target: "f1", isIntersecting: true },
          ],
        },
        { op: "frames", limit: 10, time: 100 },
        { op: "world", set: { scrollTop: 900 } },
        { op: "scrollEvent" },
        { op: "frames", limit: 10, time: 200 },
        { op: "getWindowSpecs" },
        { op: "getScrollTop" },
        {
          op: "intersect",
          entries: [
            { target: "f0", isIntersecting: false },
            { target: "f2", isIntersecting: true },
          ],
        },
        { op: "frames", limit: 10, time: 300 },
        { op: "isInstanceVisible", top: 9000, height: 100 },
        { op: "isInstanceVisible", top: -9000, height: 100 },
        { op: "getOffsetInScrollContainer", element: "f1" },
        { op: "cleanUp" },
      ],
    }),
  );

  scenarios.push(
    threeFileScenario("document root", "document", {
      steps: [
        { op: "setup" },
        { op: "connect", instance: "a" },
        { op: "connect", instance: "b" },
        { op: "frames", limit: 10, time: 0 },
        {
          op: "intersect",
          entries: [{ target: "f0", isIntersecting: true }],
        },
        { op: "world", set: { viewportHeight: 700 } },
        { op: "windowResizeEvent" },
        { op: "world", set: { scrollTop: 2500 } },
        { op: "scrollEvent" },
        { op: "frames", limit: 10, time: 100 },
        { op: "getWindowSpecs" },
        {
          op: "resize",
          entries: [{ target: "documentElement", blockSize: 6000 }],
        },
        { op: "frames", limit: 10, time: 200 },
        { op: "scrollTo", top: 99999, behavior: "instant" },
        { op: "scrollTo", top: -5 },
        { op: "getScrollTop" },
        { op: "cleanUp" },
      ],
    }),
  );

  scenarios.push(
    threeFileScenario("connect before setup is queued; disconnect drops it", "element", {
      steps: [
        { op: "connect", instance: "a" },
        { op: "connect", instance: "b" },
        { op: "dispose", instance: "b" },
        setupElement,
        { op: "frames", limit: 10, time: 0 },
        { op: "dispose", instance: "a" },
        { op: "dispose", instance: "a" },
        { op: "frames", limit: 10, time: 100 },
        { op: "cleanUp" },
        { op: "connect", instance: "c" },
        setupElement,
        { op: "frames", limit: 10, time: 200 },
        { op: "cleanUp" },
        { op: "cleanUp" },
      ],
    }),
  );

  scenarios.push(
    threeFileScenario("content container defaults to the first element child", "element", {
      world: {
        scrollTop: 0,
        viewportHeight: 800,
        scrollHeight: 5050,
        elements: { root: { firstChild: "content" }, content: {} },
      },
      steps: [
        { op: "setup" },
        { op: "setup" },
        {
          op: "resize",
          entries: [
            { target: "root", blockSize: 800 },
            { target: "content", blockSize: 5050 },
            { target: "root", blockSize: 640 },
          ],
        },
        { op: "frames", limit: 10, time: 0 },
        { op: "cleanUp" },
      ],
    }),
  );

  // requestHeightReconcile: the upstream reconcileQueue test, plus a second
  // request while the first is pending.
  scenarios.push(
    threeFileScenario("request height reconcile", "element", {
      steps: [
        setupElement,
        { op: "connect", instance: "b" },
        { op: "frames", limit: 10, time: 0 },
        { op: "requestHeightReconcile", instance: "b" },
        { op: "requestHeightReconcile", instance: "b" },
        { op: "frames", limit: 10, time: 100 },
        { op: "frames", limit: 10, time: 200 },
        { op: "requestHeightReconcile", instance: "c" },
        { op: "dispose", instance: "b" },
        { op: "frames", limit: 10, time: 300 },
        { op: "cleanUp" },
      ],
    }),
  );

  // A render that moves content above the viewport: the line anchor (first
  // visible line) restores the scroll position.
  scenarios.push(
    threeFileScenario("scroll fix keeps the first visible line", "element", {
      world: {
        scrollTop: 300,
        viewportHeight: 800,
        scrollHeight: 5050,
        rootTop: 10,
        elements: {
          content: {},
          ...stackedFiles([850, 1200, 3000], 12).elements,
        },
      },
      instances: {
        a: {
          container: "f0",
          render: [true, true],
          reconcile: [true],
          renderEffects: [
            // f0 grows by 120 px: everything below it moves down.
            {
              world: {
                elements: {
                  f0: { height: 970 },
                  f1: { docTop: 970 },
                  f2: { docTop: 2170 },
                },
              },
            },
          ],
        },
        b: { container: "f1", render: [false], reconcile: [] },
        c: { container: "f2", render: [false], reconcile: [] },
      },
      steps: [
        setupElement,
        { op: "connect", instance: "a" },
        { op: "connect", instance: "b" },
        { op: "frames", limit: 10, time: 0 },
        {
          op: "intersect",
          entries: [
            { target: "f0", isIntersecting: true },
            { target: "f1", isIntersecting: true },
          ],
        },
        { op: "frames", limit: 10, time: 100 },
        { op: "instanceChanged", instance: "a", domDirty: true },
        { op: "frames", limit: 10, time: 200 },
        { op: "getScrollTop" },
        { op: "cleanUp" },
      ],
    }),
  );

  // Files only (no lines): top anchor with the file in view, bottom anchor
  // when the whole file is above the viewport.
  for (const [label, scrollTop, shifts] of [
    ["top anchor", 100, { f0: { height: 1000 }, f1: { docTop: 1000 }, f2: { docTop: 2200 } }],
    ["bottom anchor", 1200, { f0: { height: 1000 }, f1: { docTop: 1000 }, f2: { docTop: 2200 } }],
  ]) {
    const { elements } = stackedFiles([850, 1200, 3000], 0);
    scenarios.push({
      name: `scroll fix file ${label}`,
      root: "element",
      world: {
        scrollTop,
        viewportHeight: 800,
        scrollHeight: 5050,
        elements: { content: {}, ...elements },
      },
      instances: {
        a: {
          container: "f0",
          render: [true],
          reconcile: [true],
          renderEffects: [{ world: { elements: shifts } }],
        },
        b: { container: "f1", render: [false], reconcile: [] },
        c: { container: "f2", render: [false], reconcile: [] },
      },
      steps: [
        setupElement,
        { op: "connect", instance: "a" },
        { op: "connect", instance: "b" },
        { op: "frames", limit: 10, time: 0 },
        {
          op: "intersect",
          entries: [
            { target: "f0", isIntersecting: true },
            { target: "f1", isIntersecting: true },
          ],
        },
        { op: "frames", limit: 10, time: 100 },
        { op: "instanceChanged", instance: "a", domDirty: true },
        { op: "frames", limit: 10, time: 200 },
        { op: "getScrollTop" },
        { op: "cleanUp" },
      ],
    });
  }

  // Boundary geometry. Anchor choice only shows in the scroll fix when the
  // render moves candidates by different amounts, so each scenario moves them
  // non-uniformly, once downwards and once upwards (the fix is signed). The
  // visible files are intersecting before the first frame, so the first pass
  // reads its anchor at the boundary geometry; the effects then alternate
  // between the shifted and the original (absolute) geometry on every render
  // call, so later passes fix in both directions.
  const boundaryScenario = (label, world, visible, shifted) => {
    const original = {
      elements: Object.fromEntries(
        Object.keys(shifted.elements).map((name) => [
          name,
          Object.fromEntries(
            Object.keys(shifted.elements[name]).map((key) => [
              key,
              world.elements[name][key],
            ]),
          ),
        ]),
      ),
    };
    return {
      name: `anchor boundary: ${label}`,
      root: "element",
      world: { viewportHeight: 800, scrollHeight: 5050, rootTop: 0, ...world },
      instances: {
        a: {
          container: "f0",
          render: [true, true, true, true, true, true],
          reconcile: [true],
          renderEffects: Array.from({ length: 12 }, (_, call) => ({
            world: call % 2 === 0 ? shifted : original,
          })),
        },
        b: { container: "f1", render: [false], reconcile: [] },
      },
      steps: [
        setupElement,
        { op: "connect", instance: "a" },
        { op: "connect", instance: "b" },
        {
          op: "intersect",
          entries: visible.map((target) => ({ target, isIntersecting: true })),
        },
        { op: "frames", limit: 12, time: 0 },
        { op: "getScrollTop" },
        { op: "cleanUp" },
      ],
    };
  };
  for (const delta of [30, -30]) {
    const sign = delta > 0 ? "down" : "up";
    // f0's bottom is exactly at the viewport top: a bottom anchor.
    scenarios.push(
      boundaryScenario(
        `file bottom at the viewport top, content moves ${sign}`,
        {
          scrollTop: 850,
          elements: {
            content: {},
            f0: { docTop: 0, height: 850, lines: [] },
            f1: { docTop: 2000, height: 500, lines: [] },
          },
        },
        ["f0"],
        { elements: { f0: { height: 850 + delta } } },
      ),
    );
    // Two files without lines; f1's top is exactly at the viewport top while
    // f0 starts above it.
    scenarios.push(
      boundaryScenario(
        `file top exactly at the viewport top, content moves ${sign}`,
        {
          scrollTop: 100,
          elements: {
            content: {},
            f0: { docTop: 0, height: 850, lines: [] },
            f1: { docTop: 100, height: 500, lines: [] },
          },
        },
        ["f0", "f1"],
        { elements: { f1: { docTop: 100 + delta } } },
      ),
    );
    // Lines at offsets -0.5, 0, 0.5 and a line just below 1.
    for (const [label, lines] of [
      ["line exactly at the top", [["0", 889.5], ["1", 890], ["2", 890.5]]],
      ["line just below the top", [["0", 889.75], ["1", 890.5], ["2", 891.5]]],
    ]) {
      scenarios.push(
        boundaryScenario(
          `${label}, content moves ${sign}`,
          {
            scrollTop: 890,
            elements: {
              content: {},
              f0: { docTop: 0, height: 850, lines: [] },
              f1: { docTop: 850, height: 1200, lines },
            },
          },
          ["f1"],
          {
            elements: {
              f1: {
                lines: lines.map(([index, top], position) => [
                  index,
                  top + delta + 7 * position,
                ]),
              },
            },
          },
        ),
      );
    }
  }

  scenarios.push(
    threeFileScenario("resize debugging logs and custom config", "element", {
      config: {
        overscrollSize: 200,
        intersectionObserverMargin: 500.5,
        resizeDebugging: true,
      },
      steps: [
        setupElement,
        { op: "connect", instance: "a" },
        {
          op: "resize",
          entries: [{ target: "content", blockSize: 4000 }],
        },
        {
          op: "resize",
          entries: [{ target: "content", blockSize: 4200 }],
        },
        { op: "frames", limit: 10, time: 0 },
        { op: "getWindowSpecs" },
        { op: "cleanUp" },
      ],
    }),
  );
  scenarios.push(
    threeFileScenario("resize debugging with a document root", "document", {
      config: { resizeDebugging: true },
      steps: [
        { op: "setup" },
        {
          op: "resize",
          entries: [{ target: "documentElement", blockSize: 4000 }],
        },
        {
          op: "resize",
          entries: [{ target: "documentElement", blockSize: 4000 }],
        },
        { op: "frames", limit: 10, time: 0 },
        { op: "cleanUp" },
      ],
    }),
  );

  // An intersection entry for a container that was already disconnected.
  scenarios.push(
    threeFileScenario("late intersection entry after disconnect", "element", {
      steps: [
        setupElement,
        { op: "connect", instance: "a" },
        { op: "frames", limit: 10, time: 0 },
        { op: "dispose", instance: "a" },
        {
          op: "intersect",
          entries: [{ target: "f0", isIntersecting: true }],
        },
        { op: "frames", limit: 10, time: 100 },
        { op: "cleanUp" },
      ],
    }),
  );

  // A render callback that re-enters the virtualizer.
  scenarios.push(
    threeFileScenario("on render re-enters the virtualizer", "element", {
      instances: {
        a: {
          container: "f0",
          render: [true, false, false],
          reconcile: [false, true],
          renderEffects: [
            {
              ops: [
                { op: "instanceChanged", instance: "b", domDirty: false },
                { op: "requestHeightReconcile", instance: "c" },
              ],
            },
          ],
          reconcileEffects: [
            { ops: [{ op: "requestHeightReconcile", instance: "a" }] },
          ],
        },
        b: { container: "f1", render: [true, true], reconcile: [true] },
        c: { container: "f2", render: [true], reconcile: [false, true] },
      },
      steps: [
        setupElement,
        { op: "connect", instance: "a" },
        { op: "connect", instance: "b" },
        { op: "connect", instance: "c" },
        { op: "frames", limit: 12, time: 0 },
        {
          op: "intersect",
          entries: [
            { target: "f0", isIntersecting: true },
            { target: "f2", isIntersecting: true },
          ],
        },
        { op: "frames", limit: 12, time: 100 },
        { op: "markDomDirty" },
        { op: "instanceChanged", instance: "a", domDirty: false },
        { op: "frames", limit: 12, time: 200 },
        { op: "cleanUp" },
      ],
    }),
  );
  return scenarios;
}

/** Seeded random scenarios over the same step vocabulary. */
export function fuzzScenarios(random, count) {
  const scenarios = [];
  for (let index = 0; index < count; index++) {
    const documentRoot = random.next() < 0.3;
    const fileCount = 2 + random.int(5);
    const sizes = Array.from({ length: fileCount }, () =>
      random.pick([120, 400, 850, 1200, 2400, 5000]),
    );
    const lineCount = random.int(7);
    const { elements, total } = stackedFiles(sizes, lineCount);
    const viewportHeight = random.pick([300, 600, 800, 900]);
    const scrollHeight = random.pick([total, total + 40, Math.floor(total / 2), 500]);
    const names = Object.keys(elements);
    const instances = {};
    for (const name of names) {
      const flags = (count) =>
        Array.from({ length: count }, () => random.next() < 0.5);
      const effect = () => {
        const shifted = {};
        const from = random.int(names.length);
        const delta = random.pick([-60, -20, 20, 60, 140]);
        for (let file = from; file < names.length; file++) {
          const base = elements[names[file]];
          shifted[names[file]] = {
            docTop: base.docTop + delta,
            ...(file === from
              ? { height: Math.max(40, base.height + delta) }
              : {}),
          };
        }
        const ops = [];
        if (random.next() < 0.25) {
          ops.push({
            op: "instanceChanged",
            instance: random.pick(names),
            domDirty: random.next() < 0.5,
          });
        }
        if (random.next() < 0.25) {
          ops.push({
            op: "requestHeightReconcile",
            instance: random.pick(names),
          });
        }
        return { world: { elements: shifted }, ops };
      };
      instances[name] = {
        container: name,
        render: flags(6),
        reconcile: flags(6),
        // An empty effect stands for "none" (JSON has no undefined slots).
        renderEffects: Array.from({ length: 6 }, () =>
          random.next() < 0.3 ? effect() : {},
        ),
        reconcileEffects: [],
      };
    }
    const model = {
      observing: false,
      observers: new Set(),
      queued: new Set(),
      connectedOnce: new Set(),
    };
    const steps = [];
    const maxScroll = Math.max(0, scrollHeight - viewportHeight);
    const push = (step) => steps.push(step);
    const setupStep = () =>
      documentRoot ? { op: "setup" } : { op: "setup", content: "content" };
    const doSetup = () => {
      push(setupStep());
      if (!model.observing) {
        model.observing = true;
        for (const name of model.queued) model.observers.add(name);
        model.queued.clear();
      }
    };
    const stepCount = 20 + random.int(30);
    if (random.next() < 0.7) doSetup();
    for (let step = 0; step < stepCount; step++) {
      const roll = random.next();
      if (roll < 0.12) {
        const free = names.filter(
          (name) => !model.observers.has(name),
        );
        if (free.length > 0) {
          const name = random.pick(free);
          push({ op: "connect", instance: name });
          model.connectedOnce.add(name);
          if (model.observing) model.observers.add(name);
          else model.queued.add(name);
        }
      } else if (roll < 0.17) {
        const name = random.pick(names);
        push(
          model.connectedOnce.has(name) && random.next() < 0.5
            ? { op: "dispose", instance: name }
            : { op: "disconnect", container: name },
        );
        model.observers.delete(name);
        model.queued.delete(name);
      } else if (roll < 0.3) {
        push({
          op: "world",
          set: { scrollTop: random.int(maxScroll + 200) },
        });
        push({ op: "scrollEvent" });
      } else if (roll < 0.35) {
        push({
          op: "world",
          set: {
            viewportHeight: random.pick([300, 600, 800, 900]),
          },
        });
        push(
          random.next() < 0.5
            ? { op: "windowResizeEvent" }
            : {
                op: "resize",
                entries: [{ target: ROOT, blockSize: random.pick([300, 600, 800]) }],
              },
        );
      } else if (roll < 0.4) {
        const targets = documentRoot
          ? ["documentElement"]
          : ["content", ROOT];
        push({
          op: "resize",
          entries: [
            {
              target: random.pick(targets),
              blockSize: random.pick([total, total + 100, 800, 3000]),
            },
          ],
        });
        if (random.next() < 0.5) {
          push({ op: "world", set: { scrollHeight: random.pick([total, total + 100]) } });
        }
      } else if (roll < 0.55) {
        const entries = [];
        for (const name of names) {
          if (random.next() < 0.4) {
            entries.push({ target: name, isIntersecting: random.next() < 0.5 });
          }
        }
        if (entries.length > 0) push({ op: "intersect", entries });
      } else if (roll < 0.7) {
        push(
          random.next() < 0.6
            ? { op: "frame", time: step * 16 }
            : { op: "frames", limit: 1 + random.int(8), time: step * 16 },
        );
      } else if (roll < 0.76) {
        push({
          op: "instanceChanged",
          instance: random.pick(names),
          domDirty: random.next() < 0.5,
        });
      } else if (roll < 0.8) {
        push({ op: "requestHeightReconcile", instance: random.pick(names) });
      } else if (roll < 0.86) {
        push(
          random.pick([
            { op: "getWindowSpecs" },
            { op: "getScrollTop" },
            {
              op: "isInstanceVisible",
              top: random.int(scrollHeight + 6000) - 3000,
              height: random.pick([0, 100, 850]),
            },
            { op: "getOffsetInScrollContainer", element: random.pick(names) },
          ]),
        );
      } else if (roll < 0.9) {
        push({
          op: "scrollTo",
          top: random.int(scrollHeight + 2000) - 500,
          ...(random.next() < 0.5
            ? { behavior: random.pick(["auto", "instant", "smooth"]) }
            : {}),
        });
      } else if (roll < 0.93) {
        push({ op: "markDomDirty" });
      } else if (roll < 0.96) {
        push({ op: "cleanUp" });
        model.observing = false;
        model.observers.clear();
        model.queued.clear();
      } else {
        doSetup();
      }
    }
    push({ op: "frames", limit: 20, time: 5000 });
    scenarios.push({
      name: `fuzz ${index}`,
      root: documentRoot ? "document" : "element",
      config:
        random.next() < 0.3
          ? {
              overscrollSize: random.pick([0, 200, 1000]),
              intersectionObserverMargin: random.pick([0, 300, 4000]),
              resizeDebugging: random.next() < 0.5,
            }
          : undefined,
      world: {
        // Aligned to the 20 px line grid often, so lines sit exactly on the
        // viewport top and the boundary comparisons are exercised.
        scrollTop:
          random.next() < 0.6
            ? Math.floor(random.int(maxScroll + 1) / 20) * 20
            : random.int(maxScroll + 1),
        viewportHeight,
        scrollHeight,
        rootTop: random.pick([0, 0, 10, 37.5]),
        elements: {
          content: {},
          ...(documentRoot ? {} : { root: { firstChild: random.next() < 0.5 ? "content" : null } }),
          ...elements,
        },
      },
      instances,
      steps,
    });
  }
  return scenarios;
}

/** The golden: every scenario with the trace the published class produces. */
export async function buildVirtualizer(oracle, random) {
  const { Virtualizer } = await import(oracle.pierre("components/Virtualizer.js"));
  const { clearRenderQueue } = await import(
    oracle.pierre("managers/UniversalRenderingManager.js")
  );
  const { areVirtualWindowSpecsEqual } = await import(
    oracle.pierre("utils/areVirtualWindowSpecsEqual.js")
  );
  const api = { Virtualizer, clearRenderQueue };
  const scenarios = [...handwrittenScenarios(), ...fuzzScenarios(random, 120)];
  const cases = scenarios.map((scenario) => {
    // Round-trip through JSON so the Rust side reads exactly what ran.
    const plain = JSON.parse(JSON.stringify(scenario));
    return { scenario: plain, trace: runScenario(api, plain) };
  });
  clearRenderQueue();

  const specs = [
    undefined,
    { top: 0, bottom: 0 },
    { top: 0, bottom: 10 },
    { top: 5, bottom: 10 },
    { top: 5, bottom: 10 },
  ];
  const windowSpecsEqual = [];
  for (const left of specs) {
    for (const right of specs) {
      windowSpecsEqual.push({
        left: left ?? null,
        right: right ?? null,
        expected: areVirtualWindowSpecsEqual(left, right),
      });
    }
  }
  return { cases, windowSpecsEqual };
}
