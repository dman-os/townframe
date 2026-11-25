// Differential scenarios for ResizeManager and ScrollSyncManager (S11a).
//
// The published classes run against a scripted fake DOM: a tree of named
// elements with declared heights, attributes, connectivity and scroll
// offsets, a `ResizeObserver` that only records, and fake timers that fire
// when a step says so. The trace is everything the managers do to the world
// (observer calls, style writes, scroll writes, listeners, timers, reports).
// The Rust ports replay the same scenarios (`managers/oracle_tests.rs`).
// What this cannot establish: a browser's observer delivery and layout.

import { clone } from "./s11a_cases.mjs";

class FakeHTMLElement {}

function makeWorld(rec, initial) {
  const world = { elements: {} };
  const cache = new Map();
  const listeners = new Map();

  function patch(elements) {
    for (const [name, value] of Object.entries(elements ?? {})) {
      const slot = (world.elements[name] ??= {
        tag: "div",
        children: [],
        attrs: {},
        height: 0,
        connected: true,
        scrollLeft: 0,
        dataset: {},
      });
      for (const key of ["tag", "children", "height", "connected", "scrollLeft"])
        if (value[key] !== undefined) slot[key] = clone(value[key]);
      if (value.attrs !== undefined) slot.attrs = clone(value.attrs);
      if (value.dataset !== undefined) slot.dataset = clone(value.dataset);
    }
  }
  patch(initial);

  function element(name) {
    let value = cache.get(name);
    if (value !== undefined) return value;
    value = new FakeHTMLElement();
    value.name = name;
    cache.set(name, value);
    const data = () => world.elements[name];
    Object.defineProperties(value, {
      tagName: { get: () => data().tag.toUpperCase() },
      children: { get: () => data().children.map(element) },
      firstElementChild: {
        get: () => (data().children.length > 0 ? element(data().children[0]) : null),
      },
      isConnected: { get: () => data().connected },
      dataset: { get: () => data().dataset },
      scrollLeft: { get: () => data().scrollLeft },
    });
    value.getAttribute = (attribute) => data().attrs[attribute] ?? null;
    value.getBoundingClientRect = () => ({ height: data().height });
    value.querySelectorAll = (selector) => {
      if (selector !== '[data-line-annotation*=","]')
        throw new Error(`fake DOM: unsupported selector ${selector}`);
      const found = [];
      const visit = (current) => {
        for (const child of world.elements[current].children) {
          if ((world.elements[child].attrs["data-line-annotation"] ?? "").includes(","))
            found.push(element(child));
          visit(child);
        }
      };
      visit(name);
      return found;
    };
    value.style = {
      setProperty: (property, v) => rec("style.set", name, property, v),
      removeProperty: (property) => rec("style.remove", name, property),
    };
    value.scrollTo = ({ left }) => rec("scrollTo", name, left ?? null);
    value.addEventListener = (type, handler) => {
      rec("listener.add", name, type);
      listeners.set(`${name}:${type}`, handler);
    };
    value.removeEventListener = (type) => {
      rec("listener.remove", name, type);
      listeners.delete(`${name}:${type}`);
    };
    return value;
  }
  return { patch, element, listeners };
}

function runScenario(api, scenario) {
  const trace = [];
  const rec = (...entry) => trace.push(entry);
  const { patch, element, listeners } = makeWorld(rec, scenario.world.elements);

  // Fake observer and timers.
  let observer;
  class FakeResizeObserver {
    constructor(callback) {
      this.callback = callback;
      observer = this;
      rec("observer.create");
    }
    observe(target) {
      rec("observe", target.name);
    }
    unobserve(target) {
      rec("unobserve", target.name);
    }
    disconnect() {
      rec("observer.disconnect");
      observer = undefined;
    }
  }
  const timers = new Map();
  let nextTimer = 0;
  const globals = {
    HTMLElement: FakeHTMLElement,
    ResizeObserver: FakeResizeObserver,
    setTimeout(callback, milliseconds) {
      const id = ++nextTimer;
      timers.set(id, callback);
      rec("setTimeout", id, milliseconds);
      return id;
    },
    clearTimeout(id) {
      rec("clearTimeout", id);
      timers.delete(id);
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
  const savedError = console.error;
  console.error = (message, detail) => {
    const extra =
      typeof detail === "string"
        ? detail
        : (detail?.lineAnnotation ?? null);
    rec("report", message, extra);
  };

  try {
    const managers = scenario.managers.map(() =>
      scenario.kind === "resize"
        ? new api.ResizeManager()
        : new api.ScrollSyncManager(),
    );
    for (const step of scenario.steps) {
      try {
        switch (step.op) {
          case "setup":
            if (scenario.kind === "resize") {
              managers[step.manager].setup(element(step.pre), {
                disableAnnotations: step.disableAnnotations,
                ...(step.columnVariables !== undefined
                  ? { columnVariables: step.columnVariables }
                  : {}),
              });
            } else {
              managers[step.manager].setup(
                element(step.pre),
                step.deletions == null ? undefined : element(step.deletions),
                step.additions == null ? undefined : element(step.additions),
              );
            }
            break;
          case "cleanUp":
            managers[step.manager].cleanUp();
            break;
          case "world":
            patch(step.set.elements);
            break;
          case "resize":
            observer?.callback(
              step.entries.map(({ target, blockSize, inlineSize }) => ({
                target: element(target),
                borderBoxSize: [{ blockSize }],
                contentBoxSize: [{ inlineSize }],
              })),
            );
            break;
          case "scrollEvent":
            listeners.get(`${step.target}:scroll`)?.();
            break;
          case "fireTimers": {
            const pending = [...timers.entries()];
            timers.clear();
            for (const [, callback] of pending) callback();
            break;
          }
          case "state":
            rec(
              "state",
              managers[step.manager].isDeletionsScrolling,
              managers[step.manager].isAdditionsScrolling,
            );
            break;
          default:
            throw new Error(`unknown op ${step.op}`);
        }
      } catch (error) {
        rec("throws", error.message);
      }
    }
  } finally {
    // The statics are shared by every manager in the process; a scenario that
    // throws mid-setup can leave elements owned, so reset them (Rust builds a
    // fresh `ResizeObserverHub` per scenario).
    api.ResizeManager.managersByElement.clear();
    api.ResizeManager.resizeObserver = undefined;
    console.error = savedError;
    for (const [key, descriptor] of saved) {
      if (descriptor === undefined) delete globalThis[key];
      else Object.defineProperty(globalThis, key, descriptor);
    }
  }
  return trace;
}

// ---- scenario construction ----------------------------------------------------

function el(tag, children = [], extra = {}) {
  return { tag, children, ...extra };
}

/** A diff `pre`: two code columns, each with a number gutter, annotation rows. */
function diffPre(prefix, { annotationPairs = 1, extra = {} } = {}) {
  const elements = {};
  const colChildren = (column) => {
    const names = [`${prefix}-${column}-num`];
    elements[names[0]] = el("div", [], { height: 10 });
    for (let pair = 0; pair < annotationPairs; pair++) {
      const container = `${prefix}-${column}-ann${pair}`;
      const child = `${prefix}-${column}-ann${pair}-child`;
      elements[container] = el("div", [child], {
        attrs: { "data-line-annotation": `${pair + 1},${pair + 1}` },
      });
      elements[child] = el("div", [], { height: 20 + pair * 5 + (column === "b" ? 7 : 0) });
      names.push(container);
    }
    return names;
  };
  elements[`${prefix}-a`] = el("code", colChildren("a"), { dataset: { deletions: "" } });
  elements[`${prefix}-b`] = el("code", colChildren("b"), { dataset: { additions: "" } });
  elements[prefix] = el("pre", [`${prefix}-a`, `${prefix}-b`]);
  return { ...elements, ...extra };
}

function handwritten() {
  const scenarios = [];
  const resize = (name, world, steps, managers = 1) =>
    scenarios.push({ name, kind: "resize", world: { elements: world }, managers: Array(managers).fill(0), steps });
  const setup = (manager, pre, options = {}) => ({
    op: "setup",
    manager,
    pre,
    disableAnnotations: false,
    ...options,
  });

  resize("setup observes code, number and annotation children", diffPre("p"), [
    setup(0, "p"),
    { op: "cleanUp", manager: 0 },
  ]);
  resize("setup without annotations", diffPre("p"), [
    setup(0, "p", { disableAnnotations: true }),
    { op: "cleanUp", manager: 0 },
  ]);
  resize(
    "resize entries drive column variables and annotation heights",
    diffPre("p"),
    [
      setup(0, "p"),
      {
        op: "resize",
        entries: [
          { target: "p-a", blockSize: 0, inlineSize: 480.7 },
          { target: "p-a-num", blockSize: 0, inlineSize: 33.2 },
          { target: "p-b", blockSize: 0, inlineSize: 0.4 },
          { target: "p-a-ann0-child", blockSize: 31, inlineSize: 0 },
          { target: "p-b-ann0-child", blockSize: 12, inlineSize: 0 },
          { target: "unknown", blockSize: 1, inlineSize: 1 },
        ],
      },
      {
        op: "resize",
        entries: [
          { target: "p-a", blockSize: 0, inlineSize: 480.7 },
          { target: "p-a-num", blockSize: 0, inlineSize: 40 },
          { target: "p-b-ann0-child", blockSize: 50, inlineSize: 0 },
        ],
      },
      { op: "cleanUp", manager: 0 },
    ],
  );
  resize("measure mode then apply mode", diffPre("p"), [
    setup(0, "p", { columnVariables: "measure" }),
    { op: "resize", entries: [{ target: "p-a", blockSize: 0, inlineSize: 300 }] },
    setup(0, "p", { columnVariables: "apply" }),
    setup(0, "p", { columnVariables: "measure" }),
    { op: "cleanUp", manager: 0 },
  ]);
  resize(
    "re-setup after the number gutter changed or vanished",
    diffPre("p"),
    [
      setup(0, "p"),
      { op: "world", set: { elements: { "p-a": { children: ["p-a-ann0"] } } } },
      setup(0, "p"),
      { op: "world", set: { elements: { "p-a": { children: ["p-a-num", "p-a-ann0"] } } } },
      setup(0, "p"),
      { op: "world", set: { elements: { "p-b": { children: [] } } } },
      setup(0, "p"),
      { op: "cleanUp", manager: 0 },
    ],
  );
  resize(
    "stale items are cleaned when nodes disappear, connected or not",
    {
      ...diffPre("p", { annotationPairs: 2 }),
      ...diffPre("q"),
    },
    [
      setup(0, "p"),
      { op: "world", set: { elements: { "p-a-ann1": { connected: false }, "p-a": { children: ["p-a-num", "p-a-ann0"] }, "p-b": { children: ["p-b-num", "p-b-ann0"] } } } },
      setup(0, "p"),
      { op: "world", set: { elements: { p: { children: ["p-a"] } } } },
      setup(0, "p"),
      { op: "cleanUp", manager: 0 },
    ],
  );
  resize(
    "invalid and unpaired annotations are reported",
    diffPre("p", {
      annotationPairs: 1,
      extra: {
        bad1: el("div", ["bad1c"], { attrs: { "data-line-annotation": "x,y" } }),
        bad1c: el("div"),
        lone: el("div", ["lonec"], { attrs: { "data-line-annotation": "9,9" } }),
        lonec: el("div"),
        childless: el("div", [], { attrs: { "data-line-annotation": "7,7" } }),
        childless2: el("div", [], { attrs: { "data-line-annotation": "7,7" } }),
        "p-a": el("code", ["p-a-num", "p-a-ann0", "bad1", "lone", "childless"], {}),
        "p-b": el("code", ["p-b-num", "p-b-ann0", "childless2"], {}),
        "p-a-num": el("div"),
        "p-b-num": el("div"),
      },
    }),
    [setup(0, "p"), { op: "cleanUp", manager: 0 }],
  );
  resize(
    "two managers share one observer and cannot steal elements",
    { ...diffPre("p"), ...diffPre("q") },
    [
      setup(0, "p"),
      setup(1, "q"),
      setup(1, "p"),
      { op: "resize", entries: [{ target: "p-a", blockSize: 0, inlineSize: 100 }, { target: "q-a", blockSize: 0, inlineSize: 200 }, { target: "q-a-ann0-child", blockSize: 40, inlineSize: 0 }] },
      { op: "cleanUp", manager: 0 },
      { op: "cleanUp", manager: 1 },
    ],
    2,
  );
  resize("single column does not observe annotations", diffPre("p"), [
    { op: "world", set: { elements: { p: { children: ["p-a"] } } } },
    setup(0, "p"),
    { op: "cleanUp", manager: 0 },
  ]);
  resize("annotation heights equalise and clamp at zero", diffPre("p"), [
    { op: "world", set: { elements: { "p-a-ann0-child": { height: 0 }, "p-b-ann0-child": { height: 0 } } } },
    setup(0, "p"),
    { op: "resize", entries: [{ target: "p-a-ann0-child", blockSize: -5, inlineSize: 0 }, { target: "p-b-ann0-child", blockSize: -9, inlineSize: 0 }] },
    { op: "resize", entries: [{ target: "p-a-ann0-child", blockSize: 12, inlineSize: 0 }] },
    { op: "cleanUp", manager: 0 },
  ]);

  resize(
    "a third code column is ignored",
    {
      ...diffPre("p"),
      "p-c": el("code", [], {}),
      p: el("pre", ["p-a", "p-b", "p-c"]),
    },
    [setup(0, "p"), { op: "resize", entries: [{ target: "p-c", blockSize: 0, inlineSize: 9 }] }, { op: "cleanUp", manager: 0 }],
  );
  resize(
    "three elements with one annotation key are a bad pair",
    {
      ...diffPre("p"),
      "p-a-ann0b": el("div", ["p-a-ann0b-c"], { attrs: { "data-line-annotation": "1,1" } }),
      "p-a-ann0b-c": el("div", [], { height: 3 }),
      "p-a": el("code", ["p-a-num", "p-a-ann0", "p-a-ann0b"], {}),
    },
    [setup(0, "p"), { op: "cleanUp", manager: 0 }],
  );
  resize(
    "stale code items of a disconnected column keep their variables",
    diffPre("p"),
    [
      setup(0, "p"),
      { op: "resize", entries: [{ target: "p-a", blockSize: 0, inlineSize: 300 }, { target: "p-b", blockSize: 0, inlineSize: 200 }] },
      { op: "world", set: { elements: { "p-a": { connected: false }, "p-b": { connected: true }, p: { children: [] } } } },
      setup(0, "p"),
      { op: "cleanUp", manager: 0 },
    ],
  );

  const sync = (name, world, steps) =>
    scenarios.push({ name, kind: "scrollSync", world: { elements: world }, managers: [0], steps });
  sync("scroll sync finds columns, mirrors scrolling, times out", diffPre("p"), [
    { op: "setup", manager: 0, pre: "p" },
    { op: "world", set: { elements: { "p-a": { scrollLeft: 120 }, "p-b": { scrollLeft: 7 } } } },
    { op: "scrollEvent", target: "p-a" },
    { op: "state", manager: 0 },
    { op: "scrollEvent", target: "p-b" },
    { op: "fireTimers" },
    { op: "state", manager: 0 },
    { op: "scrollEvent", target: "p-b" },
    { op: "state", manager: 0 },
    { op: "fireTimers" },
    { op: "cleanUp", manager: 0 },
    { op: "scrollEvent", target: "p-a" },
    { op: "cleanUp", manager: 0 },
  ]);
  sync("scroll sync explicit columns and partial discovery", diffPre("p"), [
    { op: "setup", manager: 0, pre: "p", deletions: "p-b", additions: "p-a" },
    { op: "setup", manager: 0, pre: "p", deletions: "p-b" },
    { op: "world", set: { elements: { p: { children: ["p-a"] } } } },
    { op: "setup", manager: 0, pre: "p" },
    { op: "setup", manager: 0, pre: "p", deletions: "p-b", additions: null },
    { op: "cleanUp", manager: 0 },
  ]);
  return scenarios;
}

function fuzz(random, count) {
  const scenarios = [];
  for (let index = 0; index < count; index++) {
    const kind = random.next() < 0.7 ? "resize" : "scrollSync";
    const pres = ["p", "q"];
    const pairs = { p: random.int(3), q: random.int(3) };
    let world = {};
    for (const pre of pres) world = { ...world, ...diffPre(pre, { annotationPairs: pairs[pre] }) };
    const names = Object.keys(world);
    const managers = kind === "resize" ? [0, 0] : [0];
    const steps = [];
    const stepCount = 6 + random.int(14);
    for (let step = 0; step < stepCount; step++) {
      const roll = random.next();
      const manager = random.int(managers.length);
      if (kind === "scrollSync") {
        if (roll < 0.3) steps.push({ op: "setup", manager: 0, pre: random.pick(pres), ...(random.next() < 0.4 ? { deletions: random.pick(["p-a", "p-b", "q-a", "q-b"]) } : {}), ...(random.next() < 0.4 ? { additions: random.pick(["p-a", "p-b", "q-a", "q-b"]) } : {}) });
        else if (roll < 0.5) steps.push({ op: "scrollEvent", target: random.pick(["p-a", "p-b", "q-a", "q-b"]) });
        else if (roll < 0.62) steps.push({ op: "fireTimers" });
        else if (roll < 0.72) steps.push({ op: "state", manager: 0 });
        else if (roll < 0.8) steps.push({ op: "cleanUp", manager: 0 });
        else steps.push({ op: "world", set: { elements: { [random.pick(["p-a", "p-b", "q-a", "q-b"])]: { scrollLeft: random.int(500) } } } });
        continue;
      }
      if (roll < 0.28) {
        steps.push({
          op: "setup",
          manager,
          pre: random.pick(pres),
          disableAnnotations: random.next() < 0.25,
          ...(random.next() < 0.4 ? { columnVariables: random.pick(["apply", "measure"]) } : {}),
        });
      } else if (roll < 0.6) {
        const entries = [];
        for (const name of names) {
          if (random.next() < 0.18) {
            entries.push({
              target: name,
              blockSize: random.pick([0, 5, 12.5, 30, -2]),
              inlineSize: random.pick([0, 0.4, 33.2, 100, 480.7]),
            });
          }
        }
        if (entries.length > 0) steps.push({ op: "resize", entries });
      } else if (roll < 0.75) {
        const name = random.pick(names);
        const patch = {};
        if (random.next() < 0.5) patch.height = random.pick([0, 10, 40]);
        if (random.next() < 0.3) patch.connected = random.next() < 0.5;
        if (random.next() < 0.3 && world[name].children.length > 0) {
          patch.children = world[name].children.filter(() => random.next() < 0.7);
        }
        if (random.next() < 0.15 && world[name].attrs?.["data-line-annotation"]) {
          patch.attrs = { "data-line-annotation": random.pick(["1,1", "x", "-2,3", "4,5,6", ""]) };
        }
        steps.push({ op: "world", set: { elements: { [name]: patch } } });
      } else {
        steps.push({ op: "cleanUp", manager });
      }
    }
    // Static observer state is shared across scenarios: leave nothing observed.
    for (const manager of managers.keys()) steps.push({ op: "cleanUp", manager });
    scenarios.push({
      name: `fuzz ${kind} ${index}`,
      kind,
      world: { elements: world },
      managers,
      steps,
    });
  }
  return scenarios;
}

export async function buildS11aManagers(oracle, random) {
  const { ResizeManager } = await import(oracle.pierre("managers/ResizeManager.js"));
  const { ScrollSyncManager } = await import(
    oracle.pierre("managers/ScrollSyncManager.js"),
  );
  const api = { ResizeManager, ScrollSyncManager };
  const scenarios = [...handwritten(), ...fuzz(random, 150)];
  return scenarios.map((scenario) => {
    const plain = JSON.parse(JSON.stringify(scenario));
    return { scenario: plain, trace: runScenario(api, plain) };
  });
}
