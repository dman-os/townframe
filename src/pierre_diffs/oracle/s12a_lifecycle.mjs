// Differential scenarios for the first lifecycle seam of FileDiff (S12a-2a).
//
// The published `FileDiff` class is constructed over a scripted fake DOM (the
// S11b world, extended with what ResizeManager and ScrollSyncManager read:
// heights, `style`, `dataset`, scroll listeners; a fake `ResizeObserver`, timers
// and `requestAnimationFrame`) and driven through the calls this seam owns:
// `setOptions`, `setThemeType`, `setLineAnnotations`, `setSelectedLines`,
// `flushDeferredLineState`, `flushManagers`, `getLineIndex` and the
// annotation/diff bookkeeping queries. `render` is not exercised (2b), so the
// fields it writes (`pre`, `codeDeletions`, `codeAdditions`, `managersDirty`,
// `fileDiff`, `renderedData`, `appliedThemeCSS`, `fileContainer`,
// `lineStateRefreshPending`) are set directly by a `set` step, the way the
// class's own subclasses and render would. `applyThemeState` (S12a-3) is
// replaced on the instance by a recorder.
//
// The trace is everything the instance does to the world (attribute and style
// writes, listener and observer registration, timers, frames), to the owner's
// callbacks, and the value of every query and snapshot step. Arrays with
// identity (annotations) and diffs are named in the scenario, and snapshots
// report which name a value is. The Rust port replays the same scenarios
// (`components/file_diff_lifecycle_oracle_tests.rs`).
//
// What this establishes: the instance's decisions and call order into its
// managers and renderer for the same option, annotation and diff inputs. What
// it cannot: rendering, or a browser's layout and event delivery.

import { clone } from "./s11a_cases.mjs";
import {
  DIFF_ROWS,
  FakeButton,
  FakeHTMLElement,
  diffFixture,
  makeWorld,
} from "./s11b_interaction.mjs";

function jsonable(value) {
  // Functions become `true`, `undefined` entries drop out.
  return JSON.parse(
    JSON.stringify(value, (_, entry) => (typeof entry === "function" ? true : entry)),
  );
}

// ---- options -------------------------------------------------------------------

function buildOptions(spec, rec, fns) {
  const options = {};
  for (const key of [
    "themeType",
    "overflow",
    "diffStyle",
    "lineHoverHighlight",
    "enableGutterUtility",
    "enableLineSelection",
    "controlledSelection",
    "disableFileHeader",
  ]) {
    if (spec[key] !== undefined) options[key] = spec[key];
  }
  if (spec.theme !== undefined) options.theme = clone(spec.theme);
  const range = (value) => (value == null ? null : clone(value));
  for (const name of [
    "onLineSelected",
    "onLineSelectionStart",
    "onLineSelectionChange",
    "onLineSelectionEnd",
  ]) {
    if (spec[name]) options[name] = (value) => rec("cb", name, range(value));
  }
  if (spec.onGutterUtilityClick) {
    options.onGutterUtilityClick = (value) => rec("cb", "onGutterUtilityClick", range(value));
  }
  if (spec.hasRenderGutterUtility) options.renderGutterUtility = () => null;
  if (spec.hunkSeparators !== undefined) {
    if (typeof spec.hunkSeparators === "string") options.hunkSeparators = spec.hunkSeparators;
    else {
      const key = `hunkSeparators:${spec.hunkSeparators.fn}`;
      if (!fns.has(key)) fns.set(key, () => undefined);
      options.hunkSeparators = fns.get(key);
    }
  }
  return options;
}

// ---- running one scenario ---------------------------------------------------------

function runScenario(api, scenario) {
  const trace = [];
  const rec = (...entry) => trace.push(entry);
  api.clearRenderQueue();
  const world = makeWorld(scenario.world, rec);
  const { byName, nm, state, fakeDocument } = world;
  for (const [name, height] of Object.entries(scenario.world.heights ?? {})) {
    byName.get(name).height = height;
  }

  const frames = new Map();
  let nextFrameId = 0;
  const runRound = (time) => {
    const pending = [...frames.values()];
    frames.clear();
    for (const callback of pending) callback(time);
    return pending.length > 0;
  };
  let observer;
  class FakeResizeObserver {
    constructor(callback) {
      this.callback = callback;
      observer = this;
      rec("observer.create");
    }
    observe(target) {
      rec("observe", nm(target));
    }
    unobserve(target) {
      rec("unobserve", nm(target));
    }
    disconnect() {
      rec("observer.disconnect");
      observer = undefined;
    }
  }
  let nextTimer = 0;
  const globals = {
    HTMLElement: FakeHTMLElement,
    HTMLButtonElement: FakeButton,
    Element: FakeHTMLElement,
    document: fakeDocument,
    ResizeObserver: FakeResizeObserver,
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
    setTimeout(_callback, milliseconds) {
      const id = ++nextTimer;
      rec("setTimeout", id, milliseconds);
      return id;
    },
    clearTimeout(id) {
      rec("clearTimeout", id);
    },
  };
  const saved = new Map();
  for (const [key, value] of Object.entries(globals)) {
    saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
    Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
  }
  const savedError = console.error;
  console.error = (message, detail) => {
    // InteractionManager's debugging `console.error({ rowRange, ... })` before
    // it throws has no host counterpart in the port (S11b silences it too); the
    // string messages are ResizeManager's reports.
    if (typeof message !== "string") return;
    const extra = typeof detail === "string" ? detail : (detail?.lineAnnotation ?? null);
    rec("report", message, extra);
  };

  // Named values with identity.
  const diffs = new Map(Object.entries(api.diffs).map(([name, diff]) => [name, clone(diff)]));
  const annotations = new Map(
    Object.entries(api.annotations).map(([name, list]) => [name, clone(list)]),
  );
  const nameOfDiff = (diff) => {
    if (diff == null) return null;
    for (const [name, candidate] of diffs) if (candidate === diff) return name;
    return "?";
  };
  const nameOfAnnotations = (list) => {
    if (list === api.EMPTY_LINE_ANNOTATIONS) return "EMPTY";
    for (const [name, candidate] of annotations) if (candidate === list) return name;
    return "?";
  };
  const el = (name) => {
    if (name == null) return undefined;
    if (!byName.has(name)) throw new Error(`scenario names no node ${name}`);
    return byName.get(name);
  };
  const fns = new Map();

  function snapshot(instance) {
    const interaction = instance.interactionManager.options;
    const options = instance.options;
    const separators = options.hunkSeparators;
    return {
      options: {
        themeType: options.themeType ?? null,
        theme: instance.getTheme(),
        overflow: options.overflow ?? null,
        diffStyle: options.diffStyle ?? null,
        hunkSeparators: separators === undefined ? null : typeof separators === "string" ? separators : "fn",
      },
      renderer: jsonable(instance.hunksRenderer.options),
      interaction: {
        enableGutterUtility: interaction.enableGutterUtility ?? null,
        usesCustomGutterUtility: interaction.usesCustomGutterUtility ?? null,
        hasOnHunkExpand: interaction.onHunkExpand != null,
        hasGetLineIndex: interaction.getLineIndex != null,
        enableLineSelection: interaction.enableLineSelection ?? null,
        controlledSelection: interaction.controlledSelection ?? null,
        lineHoverHighlight: interaction.lineHoverHighlight ?? null,
      },
      selection: instance.interactionManager.getSelection() ?? null,
      headerCache: {
        html: instance.headerCache.html ?? null,
        fileDiff: nameOfDiff(instance.headerCache.fileDiff),
      },
      managersDirty: instance.managersDirty,
      lineStateRefreshPending: instance.lineStateRefreshPending,
      // The write options as the manager reads them: `undefined` and `{}` alike.
      deferredSelectedLines: instance.deferredSelectedLines
        ? [
            clone(instance.deferredSelectedLines[0]) ?? null,
            {
              notify: instance.deferredSelectedLines[1]?.notify ?? null,
              activeLineSide: instance.deferredSelectedLines[1]?.activeLineSide ?? null,
              lineNumberOnly: instance.deferredSelectedLines[1]?.lineNumberOnly ?? false,
            },
          ]
        : null,
      latestAnnotations: nameOfAnnotations(instance.getLatestAnnotations()),
      lineAnnotations: nameOfAnnotations(instance.lineAnnotations),
      pendingData: instance.pendingData
        ? {
            fileDiff: nameOfDiff(instance.pendingData.fileDiff),
            lineAnnotations: nameOfAnnotations(instance.pendingData.lineAnnotations),
          }
        : null,
      renderedData: instance.renderedData
        ? {
            fileDiff: nameOfDiff(instance.renderedData.fileDiff),
            lineAnnotations: nameOfAnnotations(instance.renderedData.lineAnnotations),
          }
        : null,
      inputDiff: nameOfDiff(instance.getInputDiff()),
      workingDiff: nameOfDiff(instance.getWorkingDiff()),
      renderedDiff: nameOfDiff(instance.getRenderedDiff()),
    };
  }

  try {
    let instance;
    try {
      instance = new api.FileDiff(buildOptions(scenario.options ?? {}, rec, fns));
    } catch (error) {
      rec("throws", error.message);
      return trace;
    }
    instance.applyThemeState = (container, themeStyles, themeType, baseThemeType) =>
      rec("applyThemeState", nm(container), themeStyles, themeType, baseThemeType ?? null);
    state.recording = true;

    for (const step of scenario.steps) {
      try {
        switch (step.op) {
          case "setOptions":
            instance.setOptions(step.options == null ? undefined : buildOptions(step.options, rec, fns));
            break;
          case "setThemeType":
            instance.setThemeType(step.themeType);
            break;
          case "set": {
            const fields = step.fields;
            for (const key of ["pre", "codeDeletions", "codeAdditions", "fileContainer"]) {
              if (key in fields) instance[key] = el(fields[key]);
            }
            if ("fileDiff" in fields) instance.fileDiff = diffs.get(fields.fileDiff);
            if ("renderedData" in fields) {
              const data = fields.renderedData;
              instance.renderedData =
                data == null
                  ? undefined
                  : {
                      fileDiff: diffs.get(data.fileDiff),
                      lineAnnotations:
                        data.lineAnnotations === "EMPTY"
                          ? api.EMPTY_LINE_ANNOTATIONS
                          : annotations.get(data.lineAnnotations),
                    };
            }
            if ("appliedThemeCSS" in fields) {
              instance.appliedThemeCSS = fields.appliedThemeCSS == null ? undefined : clone(fields.appliedThemeCSS);
            }
            // What `render` caches for the header (`headerCache`).
            if ("headerHtml" in fields) instance.headerCache.html = fields.headerHtml ?? undefined;
            if ("headerDiff" in fields) instance.headerCache.fileDiff = diffs.get(fields.headerDiff);
            if ("managersDirty" in fields) instance.managersDirty = fields.managersDirty;
            if ("lineStateRefreshPending" in fields) {
              instance.lineStateRefreshPending = fields.lineStateRefreshPending;
            }
            break;
          }
          case "setLineAnnotations":
            instance.setLineAnnotations(annotations.get(step.annotations));
            break;
          case "setSelectedLines":
            instance.setSelectedLines(clone(step.range), step.options ?? undefined);
            break;
          case "flushDeferredLineState":
            instance.flushDeferredLineState();
            break;
          case "flushManagers":
            instance.flushManagers();
            break;
          case "frames": {
            let rounds = 0;
            while (rounds < step.limit && runRound(step.time + 16 * (rounds + 1))) rounds++;
            break;
          }
          case "query": {
            let value;
            switch (step.what) {
              case "getLatestAnnotations":
                value = nameOfAnnotations(instance.getLatestAnnotations());
                break;
              case "getAnnotationsForDiff":
                value = nameOfAnnotations(instance.getAnnotationsForDiff(diffs.get(step.diff)));
                break;
              case "isNewAnnotations":
                value = instance.isNewAnnotations(annotations.get(step.annotations));
                break;
              case "getWorkingDiff":
                value = nameOfDiff(
                  step.diff === undefined ? instance.getWorkingDiff() : instance.getWorkingDiff(diffs.get(step.diff)),
                );
                break;
              case "hasThemeChanged":
                value = instance.hasThemeChanged();
                break;
              case "shouldApplyColumnVariables":
                value = instance.shouldApplyColumnVariables(step.overflow);
                break;
              case "getLineIndex":
                value = instance.getLineIndex(step.lineNumber, step.side ?? undefined) ?? null;
                break;
              case "getHoveredLine":
                value = instance.getHoveredLine() ?? null;
                break;
              default:
                throw new Error(`unknown query ${step.what}`);
            }
            rec("result", step.what, value);
            break;
          }
          case "attr": {
            // A silent edit of the world (no trace entry), as in S11b.
            const node = el(step.target);
            if (step.value == null) node.attrs = node.attrs.filter(([k]) => k !== step.name);
            else {
              const found = node.attrs.find(([k]) => k === step.name);
              if (found) found[1] = step.value;
              else node.attrs.push([step.name, step.value]);
            }
            break;
          }
          case "resize":
            // The observer exists only between the first `observe` and the last
            // `unobserve`; entries delivered outside are dropped, as in a browser.
            observer?.callback(
              step.entries.map(({ target, blockSize, inlineSize }) => ({
                target: el(target),
                borderBoxSize: [{ blockSize }],
                contentBoxSize: [{ inlineSize }],
              })),
            );
            break;
          case "snapshot":
            rec("snapshot", snapshot(instance));
            break;
          default:
            throw new Error(`unknown op ${step.op}`);
        }
      } catch (error) {
        rec("throws", error.message);
      }
    }
    // Leave the statics (observer hub, render queue) as found.
    instance.resizeManager.cleanUp();
    instance.scrollSyncManager.cleanUp();
    instance.interactionManager.cleanUp();
    rec("snapshot", snapshot(instance));
  } finally {
    console.error = savedError;
    for (const [key, descriptor] of saved) {
      if (descriptor === undefined) delete globalThis[key];
      else Object.defineProperty(globalThis, key, descriptor);
    }
  }
  return trace;
}

// ---- fixtures ------------------------------------------------------------------

// Old lines 5, new lines 6: the geometry of DIFF_ROWS (two context lines, one
// deletion replaced by two additions, two context lines).
const OLD_LINES = ["one", "two", "old three", "four", "five"];
const NEW_LINES = ["one", "two", "new three", "new four", "four", "five"];

function sceneInputs(parseDiffFromFile) {
  const file = (name, lines, cacheKey) => ({
    name,
    contents: `${lines.join("\n")}\n`,
    ...(cacheKey === undefined ? {} : { cacheKey }),
  });
  const parsed = (cacheKey) =>
    JSON.parse(
      JSON.stringify(
        parseDiffFromFile(
          file("a.ts", OLD_LINES, cacheKey === undefined ? undefined : `${cacheKey}:old`),
          file("a.ts", NEW_LINES, cacheKey === undefined ? undefined : `${cacheKey}:new`),
        ),
      ),
    );
  const unkeyed = parsed();
  const keyed = parsed("k");
  return {
    // Same content, different identity, no cache key: never "equal targets".
    D1: unkeyed,
    D2: clone(unkeyed),
    // Same cache key: equal targets whatever their identity.
    K1: keyed,
    K2: clone(keyed),
    // A different cache key.
    K3: parsed("z"),
  };
}

const ANNOTATIONS = {
  A: [{ side: "additions", lineNumber: 2, metadata: 1 }],
  A2: [{ side: "additions", lineNumber: 2, metadata: 1 }],
  B: [
    { side: "deletions", lineNumber: 2 },
    { side: "additions", lineNumber: 2 },
  ],
  E: [],
};

function worldFor(split, annotated, heights) {
  const rows = DIFF_ROWS.map((row, index) => ({ ...row, annotation: annotated.includes(index) }));
  const world = diffFixture(split, rows);
  world.heights = heights;
  world.hits = {};
  return world;
}

/** Heights for the annotation slots a world has, by name. */
function slotNames(world) {
  const names = [];
  const visit = (node) => {
    if (node.name.startsWith("slot-")) names.push(node.name);
    node.children.forEach(visit);
  };
  world.tree.forEach(visit);
  return names;
}

// ---- scenarios -------------------------------------------------------------------

const SETUP_FIELDS = {
  pre: "pre",
  codeDeletions: "code-deletions",
  codeAdditions: "code-additions",
  managersDirty: true,
};
const RENDERED = { fileDiff: "D1", lineAnnotations: "EMPTY" };

function handwritten() {
  const scenarios = [];
  const add = (name, world, options, steps) =>
    scenarios.push({ name, world, options, steps });
  const split = worldFor(true, [1], { "slot-d2": 20, "slot-a2": 30 });
  const unified = worldFor(false, [1], { "slot-u2": 20 });
  const snap = { op: "snapshot" };

  add("defaults", split, {}, [snap]);
  add("constructed with options", split, {
    theme: "pierre-dark", themeType: "dark", overflow: "wrap", diffStyle: "unified",
    enableLineSelection: true, enableGutterUtility: true, onLineSelected: true,
    hunkSeparators: { fn: "a" },
  }, [snap]);
  add("setOptions undefined is a no-op", split, { themeType: "dark" }, [
    { op: "setOptions", options: null }, snap,
  ]);
  add("setOptions clears the header cache and syncs the managers", split, { enableLineSelection: true }, [
    { op: "set", fields: { headerHtml: "<header>", headerDiff: "D1" } },
    snap,
    { op: "setOptions", options: { enableGutterUtility: true, hasRenderGutterUtility: true, hunkSeparators: "simple" } },
    snap,
    { op: "setOptions", options: { hunkSeparators: "line-info", overflow: "wrap" } },
    snap,
    { op: "setOptions", options: {} },
    snap,
  ]);
  add("setThemeType merges and skips the equal value", split, { theme: { dark: "pierre-dark", light: "pierre-light" } }, [
    { op: "setThemeType", themeType: "system" }, snap,
    { op: "setThemeType", themeType: "dark" }, snap,
    { op: "setThemeType", themeType: "dark" },
    { op: "setThemeType", themeType: "light" }, snap,
  ]);
  const cached = (themeType, baseThemeType, extra = {}) => ({
    theme: { dark: "pierre-dark", light: "pierre-light" },
    themeStyles: "styles",
    themeType,
    baseThemeType,
    scrollbarGutter: 0,
    ...extra,
  });
  add("setThemeType applies the cached theme state", split, { theme: { dark: "pierre-dark", light: "pierre-light" }, themeType: "light" }, [
    { op: "set", fields: { fileContainer: "pre", appliedThemeCSS: cached("light") } },
    { op: "setThemeType", themeType: "dark" },
    { op: "setThemeType", themeType: "system" },
    { op: "setThemeType", themeType: "light" },
    // The base theme type wins over the requested one.
    { op: "set", fields: { appliedThemeCSS: cached("dark", "dark") } },
    { op: "setThemeType", themeType: "light" },
    { op: "set", fields: { appliedThemeCSS: cached("light", "dark") } },
    { op: "setThemeType", themeType: "system" },
    snap,
  ]);
  add("setThemeType leaves a named theme and a missing container alone", split, { theme: "pierre-dark" }, [
    { op: "set", fields: { fileContainer: "pre", appliedThemeCSS: cached("light") } },
    { op: "setThemeType", themeType: "dark" },
    { op: "setOptions", options: { theme: { dark: "pierre-dark", light: "pierre-light" } } },
    { op: "set", fields: { fileContainer: null } },
    { op: "setThemeType", themeType: "light" },
    { op: "set", fields: { fileContainer: "pre", appliedThemeCSS: null } },
    { op: "setThemeType", themeType: "dark" },
    snap,
  ]);
  add("hasThemeChanged compares the applied theme with the current one", split, { theme: { dark: "pierre-dark", light: "pierre-light" } }, [
    { op: "query", what: "hasThemeChanged" },
    { op: "set", fields: { appliedThemeCSS: cached("light") } },
    { op: "query", what: "hasThemeChanged" },
    { op: "setOptions", options: { theme: { dark: "pierre-light", light: "pierre-light" } } },
    { op: "query", what: "hasThemeChanged" },
    { op: "setOptions", options: { theme: "pierre-dark" } },
    { op: "query", what: "hasThemeChanged" },
    { op: "setOptions", options: {} },
    { op: "query", what: "hasThemeChanged" },
  ]);
  add("annotations without a working diff stay unset", split, {}, [
    { op: "setLineAnnotations", annotations: "A" },
    snap,
    { op: "query", what: "getLatestAnnotations" },
    { op: "query", what: "isNewAnnotations", annotations: "A" },
  ]);
  add("annotations pend against the input diff until rendered", split, {}, [
    { op: "set", fields: { fileDiff: "D1" } },
    { op: "query", what: "getLatestAnnotations" },
    { op: "setLineAnnotations", annotations: "A" },
    snap,
    { op: "query", what: "getLatestAnnotations" },
    { op: "query", what: "isNewAnnotations", annotations: "A" },
    { op: "query", what: "isNewAnnotations", annotations: "A2" },
    { op: "query", what: "isNewAnnotations", annotations: "E" },
    { op: "query", what: "getAnnotationsForDiff", diff: "D1" },
    { op: "query", what: "getAnnotationsForDiff", diff: "D2" },
    { op: "set", fields: { renderedData: RENDERED } },
    { op: "query", what: "getAnnotationsForDiff", diff: "D2" },
    { op: "query", what: "getAnnotationsForDiff", diff: "D1" },
    { op: "setLineAnnotations", annotations: "B" },
    snap,
  ]);
  add("pending annotations are matched by cache key", split, {}, [
    { op: "set", fields: { fileDiff: "K1", renderedData: { fileDiff: "D1", lineAnnotations: "E" } } },
    { op: "setLineAnnotations", annotations: "B" },
    { op: "query", what: "getAnnotationsForDiff", diff: "K1" },
    { op: "query", what: "getAnnotationsForDiff", diff: "K2" },
    { op: "query", what: "getAnnotationsForDiff", diff: "K3" },
    { op: "query", what: "getAnnotationsForDiff", diff: "D1" },
    { op: "query", what: "getLatestAnnotations" },
    snap,
  ]);
  add("working and rendered diff", split, {}, [
    { op: "query", what: "getWorkingDiff" },
    { op: "query", what: "getWorkingDiff", diff: "D2" },
    { op: "set", fields: { fileDiff: "D1" } },
    { op: "query", what: "getWorkingDiff" },
    { op: "query", what: "getWorkingDiff", diff: "K1" },
    { op: "set", fields: { renderedData: { fileDiff: "K2", lineAnnotations: "A" } } },
    snap,
    { op: "query", what: "shouldApplyColumnVariables", overflow: "scroll" },
    { op: "query", what: "shouldApplyColumnVariables", overflow: "wrap" },
  ]);
  add("getLineIndex resolves against the rendered diff", split, {}, [
    { op: "query", what: "getLineIndex", lineNumber: 2 },
    { op: "set", fields: { renderedData: RENDERED } },
    ...[0, 1, 2, 3, 4, 5, 6, 9].flatMap((lineNumber) => [
      { op: "query", what: "getLineIndex", lineNumber },
      { op: "query", what: "getLineIndex", lineNumber, side: "deletions" },
      { op: "query", what: "getLineIndex", lineNumber, side: "additions" },
    ]),
  ]);
  add("flushManagers without a pre clears the dirty flag only", split, {}, [
    { op: "set", fields: { managersDirty: true } },
    { op: "flushManagers" },
    snap,
    { op: "set", fields: { pre: "pre" } },
    { op: "flushManagers" },
    snap,
  ]);
  const both = { onGutterUtilityClick: true, hasRenderGutterUtility: true };
  add("constructing with both gutter utility APIs throws", split, both, []);
  add("setOptions with both gutter utility APIs throws after the options are replaced", split, { enableLineSelection: true }, [
    { op: "setOptions", options: { ...both, overflow: "wrap" } },
    snap,
    { op: "setOptions", options: { overflow: "scroll" } },
    snap,
  ]);
  const entries = (...list) => ({
    op: "resize",
    entries: list.map(([target, blockSize, inlineSize]) => ({ target, blockSize, inlineSize })),
  });
  add("column variables are applied when the instance says so", split, {}, [
    { op: "set", fields: { ...SETUP_FIELDS, renderedData: { fileDiff: "D1", lineAnnotations: "A" } } },
    { op: "flushManagers" },
    entries(["code-deletions", 0, 400], ["gutter-deletions", 0, 32], ["code-additions", 0, 420], ["gutter-additions", 0, 36]),
    { op: "set", fields: { renderedData: RENDERED, managersDirty: true } },
    { op: "flushManagers" },
    entries(["code-deletions", 0, 410], ["gutter-deletions", 0, 33], ["code-additions", 0, 430], ["gutter-additions", 0, 37]),
  ]);
  add("column variables are measured, not applied, without annotations", split, {}, [
    { op: "set", fields: { ...SETUP_FIELDS, renderedData: RENDERED } },
    { op: "flushManagers" },
    entries(["code-deletions", 0, 400], ["gutter-deletions", 0, 32], ["code-additions", 0, 420], ["gutter-additions", 0, 36]),
  ]);
  add("a merge conflict marker on the pre applies column variables", split, {}, [
    { op: "set", fields: { ...SETUP_FIELDS, renderedData: RENDERED } },
    { op: "query", what: "shouldApplyColumnVariables", overflow: "scroll" },
    { op: "attr", target: "pre", name: "data-has-merge-conflict", value: "" },
    { op: "query", what: "shouldApplyColumnVariables", overflow: "scroll" },
    { op: "query", what: "shouldApplyColumnVariables", overflow: "wrap" },
    { op: "flushManagers" },
    entries(["code-deletions", 0, 400], ["gutter-deletions", 0, 32]),
  ]);
  add("annotation heights follow the observer after setup", split, {}, [
    { op: "set", fields: SETUP_FIELDS },
    { op: "flushManagers" },
    entries(["slot-d2", 44, 0], ["slot-a2", 12, 0]),
    entries(["slot-d2", 10, 0], ["slot-a2", 12, 0]),
  ]);
  add("flushManagers sets the managers up (split, scroll)", split, { enableLineSelection: true, enableGutterUtility: true }, [
    { op: "set", fields: SETUP_FIELDS },
    { op: "flushManagers" },
    { op: "flushManagers" },
    snap,
  ]);
  add("flushManagers measures column variables unless annotations are rendered", split, {}, [
    { op: "set", fields: { ...SETUP_FIELDS, renderedData: { fileDiff: "D1", lineAnnotations: "A" } } },
    { op: "flushManagers" },
    { op: "set", fields: { renderedData: RENDERED, managersDirty: true } },
    { op: "flushManagers" },
  ]);
  add("flushManagers with a separators function applies column variables", split, { hunkSeparators: { fn: "a" } }, [
    { op: "set", fields: SETUP_FIELDS },
    { op: "flushManagers" },
  ]);
  add("flushManagers wraps (no annotations, no scroll sync)", split, { overflow: "wrap" }, [
    { op: "set", fields: SETUP_FIELDS },
    { op: "flushManagers" },
    { op: "setOptions", options: { overflow: "scroll", diffStyle: "unified" } },
    { op: "set", fields: { managersDirty: true } },
    { op: "flushManagers" },
    { op: "setOptions", options: { overflow: "scroll" } },
    { op: "set", fields: { managersDirty: true } },
    { op: "flushManagers" },
  ]);
  add("flushManagers tears the scroll sync down when it no longer applies", split, {}, [
    { op: "set", fields: SETUP_FIELDS },
    { op: "flushManagers" },
    { op: "setOptions", options: { overflow: "wrap" } },
    { op: "set", fields: { managersDirty: true } },
    { op: "flushManagers" },
    snap,
    { op: "setOptions", options: { overflow: "scroll", diffStyle: "unified" } },
    { op: "set", fields: { managersDirty: true } },
    { op: "flushManagers" },
    snap,
  ]);
  add("flushManagers on a unified pre", unified, { diffStyle: "unified" }, [
    { op: "set", fields: { pre: "pre", managersDirty: true, codeDeletions: null, codeAdditions: null } },
    { op: "flushManagers" },
  ]);
  add("flushManagers finds the code columns by itself", split, {}, [
    { op: "set", fields: { pre: "pre", managersDirty: true } },
    { op: "flushManagers" },
  ]);
  const selected = (start, end, side) => ({ start, end, ...(side ? { side } : {}) });
  add("setSelectedLines writes through to the interaction manager", split, {
    enableLineSelection: true, onLineSelected: true, onLineSelectionChange: true,
  }, [
    { op: "set", fields: { ...SETUP_FIELDS, renderedData: RENDERED } },
    { op: "flushManagers" },
    { op: "setSelectedLines", range: selected(2, 3, "additions") },
    { op: "frames", limit: 3, time: 0 },
    { op: "setSelectedLines", range: selected(1, 1, "deletions"), options: { notify: false, activeLineSide: "deletions" } },
    { op: "frames", limit: 3, time: 100 },
    { op: "setSelectedLines", range: null, options: { lineNumberOnly: true } },
    { op: "frames", limit: 3, time: 200 },
    snap,
  ]);
  add("setSelectedLines is deferred while a refresh is pending", split, {
    enableLineSelection: true, onLineSelected: true,
  }, [
    { op: "set", fields: { ...SETUP_FIELDS, renderedData: RENDERED } },
    { op: "flushManagers" },
    { op: "set", fields: { lineStateRefreshPending: true } },
    { op: "setSelectedLines", range: selected(2, 2, "additions") },
    { op: "setSelectedLines", range: selected(3, 4, "additions"), options: { notify: false } },
    snap,
    { op: "frames", limit: 2, time: 0 },
    { op: "flushDeferredLineState" },
    { op: "frames", limit: 3, time: 50 },
    snap,
    { op: "flushDeferredLineState" },
    snap,
  ]);
  add("flushDeferredLineState without a deferred write only clears the flag", split, {}, [
    { op: "set", fields: { lineStateRefreshPending: true } },
    { op: "flushDeferredLineState" },
    snap,
  ]);
  add("getHoveredLine reads the interaction manager", split, {}, [
    { op: "query", what: "getHoveredLine" },
  ]);
  return scenarios;
}

function fuzz(random, count) {
  const scenarios = [];
  const pick = random.pick;
  const bool = (chance = 0.5) => random.next() < chance;
  for (let index = 0; index < count; index++) {
    const split = bool(0.7);
    const annotated = [0, 1, 2, 3, 4, 5, 6].filter(() => bool(0.25));
    const world = worldFor(split, annotated, {});
    for (const name of slotNames(world)) {
      if (bool(0.8)) world.heights[name] = 10 + random.int(40);
    }
    const optionSpec = () => {
      const spec = {};
      if (bool(0.4)) spec.theme = pick(["pierre-dark", { dark: "pierre-dark", light: "pierre-light" }, { dark: "pierre-light", light: "pierre-light" }]);
      if (bool(0.5)) spec.themeType = pick(["system", "light", "dark"]);
      if (bool(0.5)) spec.overflow = pick(["scroll", "wrap"]);
      if (bool(0.5)) spec.diffStyle = pick(["split", "unified"]);
      if (bool(0.4)) spec.lineHoverHighlight = pick(["disabled", "both", "number", "line"]);
      for (const flag of ["enableGutterUtility", "enableLineSelection", "controlledSelection", "disableFileHeader"]) {
        if (bool(0.4)) spec[flag] = bool();
      }
      for (const flag of ["onLineSelected", "onLineSelectionStart", "onLineSelectionChange", "onLineSelectionEnd"]) {
        if (bool(0.35)) spec[flag] = true;
      }
      // The two gutter utility APIs exclude each other (the constructor throws
      // on both); the conflict is a handwritten scenario and a rare draw.
      const gutter = bool(0.03) ? "both" : pick([null, null, "onGutterUtilityClick", "hasRenderGutterUtility"]);
      if (gutter === "both") spec.onGutterUtilityClick = spec.hasRenderGutterUtility = true;
      else if (gutter !== null) spec[gutter] = true;
      if (bool(0.5)) spec.hunkSeparators = pick(["simple", "metadata", "line-info", "line-info-basic", { fn: "a" }, { fn: "b" }]);
      return spec;
    };
    const cached = () => ({
      theme: pick([{ dark: "pierre-dark", light: "pierre-light" }, { dark: "pierre-light", light: "pierre-light" }, "pierre-dark"]),
      themeStyles: pick(["s1", "s2"]),
      themeType: pick(["system", "light", "dark"]),
      baseThemeType: pick([undefined, "light", "dark"]),
      scrollbarGutter: 0,
    });
    const diffName = () => pick(["D1", "D2", "K1", "K2", "K3"]);
    const annotationName = () => pick(["A", "A2", "B", "E"]);
    const range = () => ({
      start: 1 + random.int(6),
      end: 1 + random.int(6),
      ...(bool(0.6) ? { side: pick(["additions", "deletions"]) } : {}),
    });
    // Only the columns this world has (a unified pre has one code column).
    const columns = split ? ["code-deletions", "code-additions"] : ["code-unified"];
    const nodeFields = () => {
      const fields = {};
      if (bool(0.5)) fields.pre = pick(["pre", null]);
      if (bool(0.3)) fields.codeDeletions = pick([...columns, null]);
      if (bool(0.3)) fields.codeAdditions = pick([...columns, null]);
      if (bool(0.7)) fields.managersDirty = bool(0.8);
      return fields;
    };
    const steps = [];
    const stepCount = 10 + random.int(30);
    for (let step = 0; step < stepCount; step++) {
      const roll = random.next();
      if (roll < 0.1) steps.push({ op: "setOptions", options: bool(0.1) ? null : optionSpec() });
      else if (roll < 0.18) steps.push({ op: "setThemeType", themeType: pick(["system", "light", "dark"]) });
      else if (roll < 0.28) steps.push({ op: "set", fields: nodeFields() });
      else if (roll < 0.34) steps.push({ op: "set", fields: { fileDiff: bool(0.1) ? null : diffName() } });
      else if (roll < 0.4) {
        steps.push({
          op: "set",
          fields: { renderedData: bool(0.15) ? null : { fileDiff: diffName(), lineAnnotations: bool(0.2) ? "EMPTY" : annotationName() } },
        });
      } else if (roll < 0.43) {
        steps.push({
          op: "set",
          fields: { headerHtml: pick([null, "<h>"]), headerDiff: bool(0.3) ? null : diffName() },
        });
      } else if (roll < 0.45) steps.push({ op: "set", fields: { appliedThemeCSS: bool(0.2) ? null : cached(), ...(bool(0.7) ? { fileContainer: pick(["pre", null]) } : {}) } });
      else if (roll < 0.52) steps.push({ op: "setLineAnnotations", annotations: annotationName() });
      else if (roll < 0.62) steps.push({ op: "flushManagers" });
      else if (roll < 0.7) steps.push({ op: "setSelectedLines", range: bool(0.15) ? null : range(), options: bool(0.4) ? { notify: pick([true, false]), lineNumberOnly: bool(0.3), ...(bool(0.3) ? { activeLineSide: pick(["additions", "deletions"]) } : {}) } : undefined });
      else if (roll < 0.74) steps.push({ op: "set", fields: { lineStateRefreshPending: bool(0.6) } });
      else if (roll < 0.77) steps.push({ op: "flushDeferredLineState" });
      else if (roll < 0.8) {
        const targets = ["pre", ...columns, ...columns.map((name) => name.replace("code-", "gutter-")), ...slotNames(world)];
        steps.push({
          op: "resize",
          entries: Array.from({ length: 1 + random.int(3) }, () => ({
            target: pick(targets),
            blockSize: random.int(60),
            inlineSize: random.int(500),
          })),
        });
      } else if (roll < 0.82) steps.push({ op: "attr", target: "pre", name: "data-has-merge-conflict", value: pick([null, ""]) });
      else if (roll < 0.83) steps.push({ op: "frames", limit: 1 + random.int(3), time: step * 16 });
      else if (roll < 0.9) {
        const what = pick(["getLatestAnnotations", "getAnnotationsForDiff", "isNewAnnotations", "getWorkingDiff", "hasThemeChanged", "shouldApplyColumnVariables", "getLineIndex", "getHoveredLine"]);
        steps.push({
          op: "query",
          what,
          diff: bool(0.2) && what === "getWorkingDiff" ? undefined : diffName(),
          annotations: annotationName(),
          overflow: pick(["scroll", "wrap"]),
          lineNumber: random.int(8),
          side: pick([null, "additions", "deletions"]),
        });
      } else steps.push({ op: "snapshot" });
    }
    steps.push({ op: "frames", limit: 10, time: 9000 });
    scenarios.push({
      name: `fuzz ${split ? "split" : "unified"} ${index}`,
      world,
      options: optionSpec(),
      steps,
    });
  }
  return scenarios;
}

export async function buildS12aLifecycle(oracle, random) {
  const { FileDiff } = await import(oracle.pierre("components/FileDiff.js"));
  const { clearRenderQueue } = await import(oracle.pierre("managers/UniversalRenderingManager.js"));
  const { EMPTY_LINE_ANNOTATIONS } = await import(oracle.pierre("constants.js"));
  const { parseDiffFromFile } = await import(oracle.pierre("utils/parseDiffFromFile.js"));
  const diffs = sceneInputs(parseDiffFromFile);
  const api = { FileDiff, clearRenderQueue, EMPTY_LINE_ANNOTATIONS, diffs, annotations: ANNOTATIONS };
  const scenarios = [...handwritten(), ...fuzz(random, 220)];
  const cases = scenarios.map((scenario) => {
    // JSON round trip: `undefined` fields drop out, as they do in the golden.
    const plain = JSON.parse(JSON.stringify(scenario));
    const trace = runScenario(api, plain);
    // Determinism: the statics (observer hub, render queue) are left as found.
    const again = JSON.stringify(runScenario(api, plain));
    if (again !== JSON.stringify(trace)) {
      throw new Error(`scenario "${scenario.name}" is not repeatable`);
    }
    return { scenario: plain, trace };
  });
  clearRenderQueue();
  return { diffs, annotations: ANNOTATIONS, cases };
}
