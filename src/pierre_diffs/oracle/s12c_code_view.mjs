// Differential scenarios for the item model of `CodeView` (C2): the item
// operations (`setItems`, `addItems`, `removeItem`, `updateItem`,
// `updateItemId`), the layout bookkeeping (`markLayoutDirtyFromIndex`,
// `recomputeLayout`, `findFirstVisibleIndex`, `findLastVisibleIndex`), the
// option diffing of `setOptions` (the pending layout reset, the render option
// revision, the render requests), `instanceChanged`, the selection, and the
// per-item option callback wrappers (the selection ones and the shared ones),
// including the item context every one of them appends to its arguments.
//
// The published `CodeView` is constructed over a bare `document` (it makes
// three `div`s and writes styles on them; nothing renders) and never `setup`.
// `render` is replaced on the instance by a recorder (it would queue a frame),
// and the layout half of `computeRenderRangeAndEmit` is the `recompute` step
// (`recomputeLayout` over the dirty index and the pending reset, then both are
// cleared, as the render does). Each step records its result (or the message it
// threw) and the whole observable state afterwards: the items (id, index, top,
// height, version, rendered options revision, instance number, collapsed), the
// lookup map's ids, `scrollHeight`, `scrollDirty`, `layoutDirtyIndex`,
// `pendingLayoutReset`, `renderOptionsRevision`, the selection, each instance's
// layout cache, the `render` calls and the option callbacks called since the
// previous step. The Rust port replays them in `components/code_view/oracle_tests.rs`.
//
// What this establishes: that the item bookkeeping, the layout arithmetic over
// the items and the option diffing do the same thing as the published class for
// the same inputs. What it cannot: anything with a root (`setup`, scrolling,
// rendering: C3-C5), and the context's `instance` (parity/divergences.psv; the
// calls record the context's id, type, index, top, height, version, item
// snapshot fields and whether an element is mounted). Object-valued options compare by value in the port
// and by reference upstream, so a scenario names its `layout` and `itemMetrics`
// values and reuses one object per name.

import { clone } from "./s11a_cases.mjs";
import { buildOptions } from "./s12a_file_diff.mjs";
import { METRICS, QUERY_DIFFS_SMALL, buildDiffPool } from "./s12b_layout.mjs";

export const LAYOUTS = {
  tight: { paddingTop: 2, paddingBottom: 3, gap: 1 },
  roomy: { paddingTop: 12, paddingBottom: 20, gap: 16 },
  fractional: { paddingTop: 3.5, paddingBottom: 9.25, gap: 6.5 },
};
const ID_POOL = ["a", "b", "c", "d", "e", "f"];
const SELECTION_CALLBACKS = ["onLineSelected", "onLineSelectionStart", "onLineSelectionChange", "onLineSelectionEnd"];
// Shared callbacks with a different argument shape, named only by the scenarios
// that fire them (the random scenarios do not).
const CONTEXT_CALLBACKS = ["onLineClick", "onLineEnter", "onGutterUtilityClick", "renderCustomHeader", "renderAnnotation"];
const CALLBACK_NAMES = ["onSelectedLinesChange", ...SELECTION_CALLBACKS, "onLineClick", "header", "footer"];
export const LISTS = {
  A: [
    { side: "additions", lineNumber: 0, metadata: 1 },
    { side: "deletions", lineNumber: 3, metadata: 1 },
  ],
  B: [{ side: "additions", lineNumber: 2, metadata: 1 }],
};

// ---- one run --------------------------------------------------------------------

function encodeRange(range) {
  return range == null ? null : clone(range);
}

function encodeSelection(selection) {
  return selection == null ? null : { id: selection.id, range: encodeRange(selection.range) };
}

function encodeReset(reset) {
  return reset == null ? null : clone(reset);
}

async function runScenario(api, diffs, scenario) {
  const { CodeView } = api;
  const savedDocument = Object.getOwnPropertyDescriptor(globalThis, "document");
  const noop = () => {};
  const makeElement = () => ({ style: { setProperty: noop, removeProperty: noop }, appendChild: noop, remove: noop });
  Object.defineProperty(globalThis, "document", { configurable: true, writable: true, value: { createElement: makeElement } });
  const savedError = console.error;
  console.error = noop;
  try {
    const calls = [];
    const renders = [];
    const functions = new Map();
    // What an option callback sees of the item: the trailing `context` argument.
    const encodeContext = (context) => ({
      type: context.type,
      id: context.item.id,
      itemVersion: context.item.version ?? null,
      itemCollapsed: context.item.collapsed ?? null,
      index: context.index,
      top: context.top,
      height: context.height,
      version: context.version ?? null,
      renderedOptionsRevision: context.renderedOptionsRevision,
      mounted: context.element != null,
    });
    const fn = (name) => {
      if (!functions.has(name)) {
        let record;
        if (name === "onSelectedLinesChange") record = (selection) => calls.push([name, encodeSelection(selection)]);
        else if (SELECTION_CALLBACKS.includes(name) || name === "onGutterUtilityClick")
          record = (range, context) => calls.push([name, encodeRange(range), encodeContext(context)]);
        else if (name === "onLineClick" || name === "onLineEnter")
          record = (props, context) => calls.push([name, props.lineNumber, encodeContext(context)]);
        else if (name === "renderCustomHeader")
          record = (fileDiff, context) => {
            calls.push([name, fileDiff.name, encodeContext(context)]);
            return undefined;
          };
        else if (name === "renderAnnotation")
          record = (annotation, fileDiff, context) => {
            calls.push([name, annotation.lineNumber, fileDiff.name, encodeContext(context)]);
            return undefined;
          };
        else if (name === "header" || name === "footer") record = () => calls.push([name]);
        else throw new Error(`unknown callback ${name}`);
        functions.set(name, record);
      }
      return functions.get(name);
    };
    const buildCodeViewOptions = (spec) => {
      const options = buildOptions(spec.diff ?? {}, new Map());
      if (spec.hunkSeparators !== undefined) options.hunkSeparators = spec.hunkSeparators;
      if (spec.stickyHeaders !== undefined) options.stickyHeaders = spec.stickyHeaders;
      if (spec.controlledSelection !== undefined) options.controlledSelection = spec.controlledSelection;
      if (spec.itemMetrics !== undefined) options.itemMetrics = metricsObjects[spec.itemMetrics];
      if (spec.layout !== undefined) options.layout = layoutObjects[spec.layout];
      if (spec.pointerEventsOnScroll !== undefined) options.pointerEventsOnScroll = spec.pointerEventsOnScroll;
      for (const name of spec.callbacks ?? []) {
        if (name === "header") options.renderCodeViewHeader = fn("header");
        else if (name === "footer") options.renderCodeViewFooter = fn("footer");
        else options[name] = fn(name);
      }
      return options;
    };
    const metricsObjects = {};
    for (const [name, metrics] of Object.entries(METRICS)) if (metrics !== undefined) metricsObjects[name] = clone(metrics);
    const layoutObjects = clone(LAYOUTS);

    const cv = new CodeView(buildCodeViewOptions(scenario.options));
    cv.render = (immediate = false) => {
      renders.push(immediate);
    };
    const instanceNumbers = new Map();
    const instanceNumber = (instance) => {
      if (!instanceNumbers.has(instance)) instanceNumbers.set(instance, instanceNumbers.size);
      return instanceNumbers.get(instance);
    };
    const item = (spec) => ({
      id: spec.id,
      type: "diff",
      fileDiff: diffs[spec.diff],
      ...(spec.version != null ? { version: spec.version } : {}),
      ...(spec.collapsed != null ? { collapsed: spec.collapsed } : {}),
      ...(spec.annotations != null ? { annotations: LISTS[spec.annotations] } : {}),
    });
    const snapshot = () => {
      const items = cv.items.map((record) => {
        const { instance } = record;
        const { cache } = instance;
        return {
          id: record.item.id,
          type: record.type,
          index: record.index,
          top: record.top,
          height: record.height,
          version: record.version ?? null,
          renderedOptionsRevision: record.renderedOptionsRevision,
          instance: instanceNumber(instance),
          collapsed: record.item.collapsed ?? null,
          instanceHeight: instance.height,
          instanceTop: instance.top ?? null,
          layoutDirty: instance.layoutDirty,
          estimatedSplitHeight: cache.estimatedSplitHeight ?? null,
          estimatedUnifiedHeight: cache.estimatedUnifiedHeight ?? null,
          totalLines: cache.totalLines,
          fileAnnotationHeight: cache.fileAnnotationHeight,
        };
      });
      const state = {
        items,
        ids: [...cv.idToItem.keys()].sort(),
        instances: [...cv.instanceToItem.values()].map((record) => record.item.id).sort(),
        scrollHeight: cv.scrollHeight,
        scrollDirty: cv.scrollDirty,
        layoutDirtyIndex: cv.layoutDirtyIndex ?? null,
        pendingLayoutReset: encodeReset(cv.pendingLayoutReset),
        renderOptionsRevision: cv.renderOptionsRevision,
        selection: encodeSelection(cv.getSelectedLines()),
        renders: renders.splice(0),
        calls: calls.splice(0),
      };
      return state;
    };
    const apply = (step) => {
      switch (step.op) {
        case "setItems":
          cv.setItems(step.items.map(item));
          return null;
        case "addItems":
          cv.addItems(step.items.map(item));
          return null;
        case "addItem":
          cv.addItem(item(step.item));
          return null;
        case "updateItem":
          return cv.updateItem(item(step.item));
        case "updateItemId":
          return cv.updateItemId(step.from, step.to);
        case "removeItem":
          return cv.removeItem(step.id);
        case "setOptions":
          cv.setOptions(buildCodeViewOptions(step.options));
          return null;
        case "instanceChanged":
          cv.instanceChanged(cv.idToItem.get(step.id).instance, step.layoutDirty);
          return null;
        case "recompute":
          if (cv.layoutDirtyIndex != null) {
            cv.recomputeLayout(cv.layoutDirtyIndex, cv.pendingLayoutReset);
            cv.layoutDirtyIndex = undefined;
            cv.pendingLayoutReset = undefined;
          }
          return null;
        case "setSelectedLines":
          cv.setSelectedLines(step.selection, step.notify === undefined ? undefined : { notify: step.notify });
          return null;
        case "clearSelectedLines":
          cv.clearSelectedLines();
          return null;
        case "fire": {
          // The callback the item's instance holds under `key`.
          const options = cv.idToItem.get(step.id).instance.options;
          if (step.key === "onLineClick" || step.key === "onLineEnter") options[step.key]({ lineNumber: step.lineNumber });
          else if (step.key === "renderCustomHeader") options[step.key](diffs[step.diff]);
          else if (step.key === "renderAnnotation") options[step.key]({ side: "additions", lineNumber: step.lineNumber, metadata: 1 }, diffs[step.diff]);
          else if (step.key === "onGutterUtilityClick") options[step.key](step.range);
          else options[step.key](step.range ?? null);
          return null;
        }
        case "firstVisible":
          return cv.findFirstVisibleIndex(step.top);
        case "lastVisible": {
          const index = cv.findLastVisibleIndex(step.bottom);
          return index === -1 ? null : index;
        }
        case "topForItem":
          return cv.getTopForItem(step.id) ?? null;
        case "getItem": {
          const found = cv.getItem(step.id);
          return found == null ? null : { id: found.id, version: found.version ?? null, collapsed: found.collapsed ?? null };
        }
        default:
          throw new Error(`unknown step ${step.op}`);
      }
    };

    const trace = [snapshot()];
    const results = [];
    for (const step of scenario.steps) {
      let result;
      try {
        result = { value: apply(step) };
      } catch (error) {
        result = { throws: error.message };
      }
      results.push(result);
      trace.push(snapshot());
    }
    return { results, trace };
  } finally {
    console.error = savedError;
    if (savedDocument === undefined) delete globalThis.document;
    else Object.defineProperty(globalThis, "document", savedDocument);
  }
}

// ---- scenarios ------------------------------------------------------------------

const spec = (id, diff, extra = {}) => ({ id, diff, ...extra });
const recompute = { op: "recompute" };

function handwritten() {
  const abc = [spec("a", "twoHunks"), spec("b", "multi3"), spec("c", "gaps1")];
  const scenarios = [];
  const add = (name, options, steps) => scenarios.push({ name, options, steps });

  add("append only", {}, [
    { op: "setItems", items: [abc[0]] },
    { op: "setItems", items: [abc[0], abc[1]] },
    recompute,
    { op: "setItems", items: [{ ...abc[0], version: 1, collapsed: true }, abc[1], abc[2]] },
    recompute,
    { op: "setItems", items: [{ ...abc[0], version: 1, collapsed: false }, abc[1], abc[2]] },
    recompute,
  ]);
  add("reorder insert remove", {}, [
    { op: "setItems", items: abc },
    recompute,
    { op: "setItems", items: [abc[2], abc[0], abc[1]] },
    recompute,
    { op: "setItems", items: [abc[2], spec("d", "uneven"), abc[0], abc[1]] },
    recompute,
    { op: "setItems", items: [abc[2], abc[1]] },
    recompute,
    { op: "setItems", items: [abc[1]] },
    recompute,
    { op: "setItems", items: [] },
    recompute,
    { op: "setItems", items: abc },
  ]);
  add("replace by version", {}, [
    { op: "setItems", items: abc },
    recompute,
    { op: "setItems", items: [abc[0], spec("b", "uneven", { version: 1 }), abc[2]] },
    recompute,
    { op: "setItems", items: [abc[0], spec("b", "gaps0", { version: 1 }), abc[2]] },
    recompute,
    { op: "setItems", items: [abc[0], spec("b", "gaps0", { version: 2, annotations: "A" }), abc[2]] },
    recompute,
  ]);
  add("duplicate ids throw", {}, [
    { op: "setItems", items: [abc[0], abc[1]] },
    { op: "addItems", items: [abc[2], abc[0]] },
    { op: "setItems", items: [abc[0], abc[0]] },
    { op: "setItems", items: [abc[0], abc[1], abc[2], abc[2]] },
    { op: "addItem", item: abc[1] },
    recompute,
  ]);
  add("add items", {}, [
    { op: "addItem", item: abc[0] },
    { op: "addItems", items: [] },
    { op: "addItems", items: [abc[1], abc[2]] },
    recompute,
    { op: "topForItem", id: "b" },
    { op: "topForItem", id: "zzz" },
    { op: "getItem", id: "c" },
    { op: "getItem", id: "zzz" },
  ]);
  add("update item", { layout: "roomy" }, [
    { op: "setItems", items: abc },
    recompute,
    { op: "updateItem", item: { ...abc[1], collapsed: true } },
    { op: "updateItem", item: { ...abc[1], collapsed: true, version: 1 } },
    recompute,
    { op: "updateItem", item: { ...abc[1], collapsed: true, version: 1 } },
    { op: "updateItem", item: spec("zzz", "multi3", { version: 3 }) },
    { op: "updateItem", item: { ...abc[1], collapsed: false, version: 2, annotations: "B" } },
    recompute,
    { op: "updateItem", item: spec("c", "noeofBoth", { version: 4 }) },
    recompute,
  ]);
  add("update item id", {}, [
    { op: "setItems", items: abc },
    { op: "updateItemId", from: "a", to: "a" },
    { op: "updateItemId", from: "zzz", to: "y" },
    { op: "updateItemId", from: "a", to: "b" },
    { op: "updateItemId", from: "a", to: "renamed" },
    { op: "getItem", id: "a" },
    { op: "getItem", id: "renamed" },
    recompute,
    { op: "setItems", items: [spec("renamed", "twoHunks"), abc[1], abc[2]] },
    recompute,
    { op: "removeItem", id: "renamed" },
    recompute,
  ]);
  add("remove item", {}, [
    { op: "setItems", items: abc },
    { op: "removeItem", id: "b" },
    { op: "removeItem", id: "b" },
    recompute,
    { op: "removeItem", id: "c" },
    { op: "removeItem", id: "a" },
    recompute,
    { op: "removeItem", id: "zzz" },
  ]);

  // Selection: the instance callbacks, the rename and the removal of the selected item.
  const selectionOptions = { callbacks: ["onSelectedLinesChange", ...SELECTION_CALLBACKS] };
  const range = { start: 2, end: 3 };
  add("selection moves", selectionOptions, [
    { op: "setItems", items: abc },
    { op: "setSelectedLines", selection: { id: "a", range }, notify: false },
    { op: "setSelectedLines", selection: { id: "a", range }, notify: false },
    { op: "setSelectedLines", selection: { id: "b", range: { start: 1, end: 4, side: "additions" } } },
    { op: "setSelectedLines", selection: { id: "zzz", range } },
    { op: "clearSelectedLines" },
    { op: "clearSelectedLines" },
    { op: "setSelectedLines", selection: { id: "c", range } },
  ]);
  add("selection across removal and rename", selectionOptions, [
    { op: "setItems", items: abc },
    { op: "setSelectedLines", selection: { id: "b", range }, notify: false },
    { op: "removeItem", id: "a" },
    { op: "updateItemId", from: "b", to: "moved" },
    { op: "setItems", items: [spec("moved", "multi3"), abc[2]] },
    { op: "removeItem", id: "moved" },
    { op: "setItems", items: [abc[0]] },
  ]);
  add("selection callbacks", selectionOptions, [
    { op: "setItems", items: abc },
    { op: "fire", id: "a", key: "onLineSelected", range },
    { op: "fire", id: "b", key: "onLineSelectionStart", range: { start: 1, end: 1 } },
    { op: "fire", id: "b", key: "onLineSelectionChange", range: { start: 1, end: 5 } },
    { op: "fire", id: "b", key: "onLineSelectionEnd", range: null },
    { op: "fire", id: "a", key: "onLineSelected", range: null },
    { op: "fire", id: "c", key: "onLineSelected", range },
    { op: "updateItemId", from: "c", to: "d" },
    { op: "fire", id: "d", key: "onLineSelectionEnd", range },
  ]);
  // The item context: every callback gets the item's record as it stands when the callback runs.
  const contextOptions = { callbacks: ["onSelectedLinesChange", ...SELECTION_CALLBACKS, ...CONTEXT_CALLBACKS] };
  add("callback context", contextOptions, [
    { op: "setItems", items: abc },
    { op: "fire", id: "a", key: "onLineClick", lineNumber: 3 },
    recompute,
    { op: "fire", id: "b", key: "onLineClick", lineNumber: 7 },
    { op: "fire", id: "c", key: "onLineEnter", lineNumber: 1 },
    { op: "fire", id: "a", key: "onGutterUtilityClick", range },
    { op: "fire", id: "b", key: "renderCustomHeader", diff: "multi3" },
    { op: "fire", id: "c", key: "renderAnnotation", diff: "gaps1", lineNumber: 4 },
    { op: "fire", id: "b", key: "onLineSelected", range },
    { op: "fire", id: "c", key: "onLineSelectionEnd", range: null },
    { op: "setItems", items: [{ ...abc[0], version: 4, collapsed: true }, abc[2], abc[1]] },
    recompute,
    { op: "fire", id: "a", key: "onLineClick", lineNumber: 9 },
    { op: "fire", id: "b", key: "onLineClick", lineNumber: 2 },
    { op: "fire", id: "c", key: "onLineSelected", range },
    { op: "updateItemId", from: "c", to: "renamed" },
    { op: "fire", id: "renamed", key: "onLineClick", lineNumber: 5 },
    { op: "fire", id: "renamed", key: "onLineSelectionStart", range },
    { op: "updateItem", item: { ...abc[1], version: 2, collapsed: true } },
    recompute,
    { op: "fire", id: "b", key: "onGutterUtilityClick", range },
  ]);
  add("callback context with layout options", { ...contextOptions, layout: "roomy", itemMetrics: "fractional", diff: { diffStyle: "unified" } }, [
    { op: "setItems", items: abc },
    recompute,
    { op: "fire", id: "c", key: "onLineClick", lineNumber: 0 },
    { op: "setOptions", options: { ...contextOptions, layout: "tight" } },
    recompute,
    { op: "fire", id: "c", key: "onLineClick", lineNumber: 1 },
    { op: "fire", id: "a", key: "renderCustomHeader", diff: "twoHunks" },
  ]);
  add("controlled selection", { ...selectionOptions, controlledSelection: true }, [
    { op: "setItems", items: abc },
    { op: "fire", id: "a", key: "onLineSelected", range },
    { op: "fire", id: "a", key: "onLineSelected", range: null },
  ]);
  add("selection callbacks without the user callbacks", { callbacks: [] }, [
    { op: "setItems", items: abc },
    { op: "fire", id: "a", key: "onLineSelected", range },
    { op: "fire", id: "a", key: "onLineSelectionEnd", range: null },
  ]);

  // Options: every layout option flipped, the estimate-affecting ones, the metrics, the layout.
  const layoutFlips = [
    { diffStyle: "unified" },
    { overflow: "wrap" },
    { disableLineNumbers: true },
    { disableFileHeader: true },
    { unsafeCSS: "a{b:c}" },
    { diffIndicators: "classic" },
    { expandUnchanged: true },
    { collapsedContextThreshold: 5 },
    { themeType: "dark" },
    { disableBackground: true },
  ];
  for (const flip of layoutFlips) {
    add(`option ${Object.keys(flip)[0]}`, {}, [
      { op: "setItems", items: abc },
      recompute,
      { op: "setOptions", options: { diff: flip } },
      recompute,
      { op: "setOptions", options: { diff: flip } },
      { op: "setOptions", options: {} },
      recompute,
    ]);
  }
  for (const [name, change] of Object.entries({
    separators: { hunkSeparators: "simple" },
    separatorsInfo: { hunkSeparators: "line-info" },
    sticky: { stickyHeaders: true },
    metrics: { itemMetrics: "upstreamTest" },
    metricsFractional: { itemMetrics: "fractional" },
    layoutTight: { layout: "tight" },
    layoutRoomy: { layout: "roomy" },
    pointer: { pointerEventsOnScroll: true },
    header: { callbacks: ["header"] },
    footer: { callbacks: ["footer"] },
    callback: { callbacks: ["onLineClick"] },
  })) {
    add(`option ${name}`, {}, [
      { op: "setOptions", options: change },
      { op: "setItems", items: abc },
      recompute,
      { op: "setOptions", options: change },
      { op: "setOptions", options: {} },
      recompute,
      { op: "setOptions", options: { ...change, diff: { disableFileHeader: true } } },
      { op: "setOptions", options: { ...change, itemMetrics: "zeroHunk", diff: { disableFileHeader: true } } },
      recompute,
    ]);
  }
  add("options accumulate a pending reset", {}, [
    { op: "setItems", items: abc },
    recompute,
    { op: "setOptions", options: { diff: { diffStyle: "unified" } } },
    { op: "setOptions", options: { diff: { diffStyle: "unified" }, itemMetrics: "codeView" } },
    { op: "setOptions", options: { diff: { diffStyle: "unified", expandUnchanged: true }, itemMetrics: "codeView" } },
    { op: "setOptions", options: { diff: { diffStyle: "split" }, itemMetrics: "codeView" } },
    recompute,
  ]);
  add("options without items", { callbacks: ["header"] }, [
    { op: "setOptions", options: { callbacks: ["header"], layout: "tight" } },
    { op: "setOptions", options: { callbacks: ["footer"], layout: "tight" } },
    { op: "setOptions", options: { callbacks: [] } },
    { op: "setOptions", options: { callbacks: [] } },
  ]);

  add("instance changed", {}, [
    { op: "setItems", items: abc },
    recompute,
    { op: "instanceChanged", id: "b", layoutDirty: false },
    { op: "instanceChanged", id: "b", layoutDirty: true },
    recompute,
    { op: "instanceChanged", id: "a", layoutDirty: true },
    recompute,
  ]);
  add("visible index queries", { layout: "roomy" }, [
    { op: "setItems", items: abc },
    recompute,
    ...[-5, 0, 1, 25, 60, 200, 1e9].flatMap((top) => [{ op: "firstVisible", top }, { op: "lastVisible", bottom: top }]),
  ]);
  return scenarios;
}

// ---- random ---------------------------------------------------------------------

const OPTION_FIELDS = {
  diffStyle: ["split", "unified"],
  expandUnchanged: [true, false],
  collapsedContextThreshold: [0, 1, 2, 5],
  disableFileHeader: [true, false],
  overflow: ["scroll", "wrap"],
  disableLineNumbers: [true, false],
  diffIndicators: ["bars", "classic", "none"],
  unsafeCSS: ["", "a{b:c}"],
  themeType: ["system", "light", "dark"],
  disableBackground: [true, false],
};

function randomOptions(random, previous) {
  const options = previous === undefined ? {} : clone(previous);
  options.diff ??= {};
  // Mostly one-field mutations, so that equal and near-equal pairs are common.
  const mutations = random.next() < 0.1 ? 0 : 1 + random.int(2);
  for (let count = 0; count < mutations; count++) {
    switch (random.int(8)) {
      case 0:
        options.itemMetrics = random.pick([undefined, "upstreamTest", "fractional", "zeroHunk", "codeView"]);
        break;
      case 1:
        options.layout = random.pick([undefined, ...Object.keys(LAYOUTS)]);
        break;
      case 2:
        options.hunkSeparators = random.pick([undefined, "simple", "metadata", "line-info", "line-info-basic"]);
        break;
      case 3:
        options.stickyHeaders = random.pick([undefined, true, false]);
        break;
      case 4:
        options.callbacks = CALLBACK_NAMES.filter(() => random.next() < 0.4);
        break;
      default: {
        const field = random.pick(Object.keys(OPTION_FIELDS));
        options.diff[field] = random.pick(OPTION_FIELDS[field]);
      }
    }
  }
  for (const key of Object.keys(options)) if (options[key] === undefined) delete options[key];
  return options;
}

function randomScenario(random, index, large) {
  const pool = large ? ["long", "longTail", "twoHunks", "multi3"] : QUERY_DIFFS_SMALL;
  const versions = new Map();
  let current = [];
  let options = randomOptions(random);
  const randomItem = (id) => {
    const version = random.next() < 0.5 ? undefined : random.int(3);
    return spec(id, random.pick(pool), {
      ...(version === undefined ? {} : { version }),
      ...(random.next() < 0.2 ? { collapsed: random.next() < 0.5 } : {}),
      ...(random.next() < 0.2 ? { annotations: random.pick(["A", "B"]) } : {}),
    });
  };
  const steps = [];
  const stepCount = 12 + random.int(14);
  for (let count = 0; count < stepCount; count++) {
    const roll = random.int(20);
    if (roll < 4) {
      // A controlled list: reuse some current items (possibly bumped), add and drop some.
      const next = [];
      for (const existing of random.next() < 0.5 ? current : current.slice().reverse()) {
        if (random.next() < 0.2) continue;
        next.push(random.next() < 0.4 ? { ...existing, version: (existing.version ?? 0) + 1, collapsed: random.next() < 0.5 } : existing);
      }
      const extra = random.int(3);
      for (let add = 0; add < extra; add++) {
        const id = random.pick(ID_POOL);
        if (!next.some((entry) => entry.id === id)) next.splice(random.int(next.length + 1), 0, randomItem(id));
      }
      current = next;
      steps.push({ op: "setItems", items: next });
    } else if (roll < 6) {
      const id = random.pick(ID_POOL);
      const items = current.some((entry) => entry.id === id) ? [] : [randomItem(id)];
      current = [...current, ...items];
      steps.push({ op: "addItems", items });
    } else if (roll < 8 && current.length > 0) {
      const target = random.pick(current);
      const next = { ...target, version: (versions.get(target.id) ?? target.version ?? 0) + 1, collapsed: random.next() < 0.5 };
      versions.set(target.id, next.version);
      current = current.map((entry) => (entry.id === target.id ? next : entry));
      steps.push({ op: "updateItem", item: next });
    } else if (roll < 9 && current.length > 0) {
      const from = random.pick(current).id;
      const to = random.pick([...ID_POOL, from]);
      steps.push({ op: "updateItemId", from, to });
      if (from !== to && !current.some((entry) => entry.id === to)) current = current.map((entry) => (entry.id === from ? { ...entry, id: to } : entry));
    } else if (roll < 10) {
      const id = random.pick(ID_POOL);
      steps.push({ op: "removeItem", id });
      current = current.filter((entry) => entry.id !== id);
    } else if (roll < 12) {
      options = randomOptions(random, options);
      steps.push({ op: "setOptions", options });
    } else if (roll < 13 && current.length > 0) {
      steps.push({ op: "instanceChanged", id: random.pick(current).id, layoutDirty: random.next() < 0.7 });
    } else if (roll < 15 && current.length > 0) {
      const id = random.next() < 0.9 ? random.pick(current).id : "zzz";
      steps.push({ op: "setSelectedLines", selection: { id, range: { start: 1 + random.int(5), end: 1 + random.int(9) } }, ...(random.next() < 0.5 ? { notify: false } : {}) });
    } else if (roll < 16) {
      steps.push({ op: "clearSelectedLines" });
    } else if (roll < 17 && current.length > 0) {
      steps.push({ op: "fire", id: random.pick(current).id, key: random.pick(SELECTION_CALLBACKS), range: random.next() < 0.2 ? null : { start: 1, end: 1 + random.int(6) } });
    } else if (roll < 18) {
      steps.push(random.next() < 0.5 ? { op: "firstVisible", top: random.int(400) } : { op: "lastVisible", bottom: random.int(400) });
    } else {
      steps.push(recompute);
    }
  }
  steps.push(recompute);
  return { name: `random${index}`, options: randomOptions(random), steps };
}

export async function buildS12cCodeView(oracle, random, corpusMocks) {
  const { CodeView } = await import(oracle.pierre("components/CodeView.js"));
  const diffs = await buildDiffPool(oracle, random, corpusMocks);
  const scenarios = handwritten();
  for (let index = 0; index < 160; index++) scenarios.push(randomScenario(random, index, index % 16 === 0));
  const cases = [];
  for (const scenario of scenarios) {
    // Items hand the pool's diff objects to the instances: a fresh copy per
    // scenario, of the diffs it names.
    const own = {};
    for (const step of scenario.steps) {
      for (const entry of [...(step.items ?? []), ...(step.item ? [step.item] : [])]) own[entry.diff] ??= clone(diffs[entry.diff]);
    }
    cases.push({ scenario: clone(scenario), ...(await runScenario({ CodeView }, own, scenario)) });
  }
  // The named values the scenarios refer to, so the replay needs no copy.
  const metrics = Object.fromEntries(Object.entries(METRICS).filter(([, value]) => value !== undefined));
  return { diffs, metrics, layouts: LAYOUTS, lists: LISTS, cases };
}
