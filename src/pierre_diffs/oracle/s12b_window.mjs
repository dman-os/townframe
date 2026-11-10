// Differential scenarios for the window and measurement half of
// `VirtualizedFileDiff` (S12b-2): `computeRenderRangeFromWindow`,
// `getLinePosition`, `reconcileHeights` (with `measureFileAnnotationHeight`),
// `onRender`, `setVisibility`, `syncVirtualizedTop`, `updatePendingRender`,
// `syncLineAnnotations` and `updateCodeViewLayout`.
//
// The published `VirtualizedFileDiff` runs over the published `Virtualizer`
// (never `setup`: no root, so its scroll top is 0 and `getOffsetInScrollContainer`
// is the container's declared top; `instanceChanged`, which `setOptions` calls,
// is a stub because it would queue a frame) and a declared DOM: the elements
// `reconcileHeights` reads (`children`, `dataset`, `nextElementSibling`,
// `getBoundingClientRect().height`), as upstream's own tests fake them
// (`FakeHTMLElement` with `HTMLElement` replaced), with heights and the
// container's top declared by the scenario and changed by steps. `render` and
// `rerender`, which `onRender` and `setVisibility` call and S12b-3 owns, are
// recorded stubs. After every step the oracle records its result and the whole
// observable state: the layout (height, top, layoutDirty, the cache), whether
// and what `renderRange` is, `isVisible`, `forceRenderOverride`, the pending
// render (which pool diffs it holds) and which annotation list the instance
// holds. The Rust port replays the same scenarios against its
// `VirtualizedFileDiff` over `FakeDom`
// (`components/virtualized_file_diff/window_oracle_tests.rs`).
//
// What this establishes: window arithmetic and measurement bookkeeping for the
// same declared geometry. What it cannot: that a browser reports those
// geometries (declared here), or the `render` integration (S12b-3).

import { clone } from "./s11a_cases.mjs";
import { buildOptions } from "./s12a_file_diff.mjs";
import {
  METRICS,
  QUERY_DIFFS_LARGE,
  QUERY_DIFFS_SMALL,
  buildDiffPool,
  mutateSpec,
  randomSpec,
} from "./s12b_layout.mjs";

const GROUPS = ["deletions", "additions", "unified"];
const FILE_ANNOTATION_KEY = "-1,-1";

// ---- one run -------------------------------------------------------------------

class FakeEl {
  constructor() {
    this.children = [];
    this.dataset = {};
    this.nextElementSibling = undefined;
    this.height = 0;
    this.top = 0;
  }
  append(...elements) {
    this.children.push(...elements);
  }
  getBoundingClientRect() {
    return { height: this.height, top: this.top };
  }
}

/** The declared DOM of one code group: `[gutter, content[...rows]]`. */
function buildGroup(spec) {
  const group = new FakeEl();
  const gutter = new FakeEl();
  const content = new FakeEl();
  group.append(gutter, content);
  const built = { group, content, rows: [], fileAnnotation: undefined };
  if (spec.fileAnnotation != null) {
    const element = new FakeEl();
    element.dataset.lineAnnotation = spec.fileAnnotation.key ?? FILE_ANNOTATION_KEY;
    element.height = spec.fileAnnotation.height;
    content.append(element);
    built.fileAnnotation = element;
  }
  for (const row of spec.rows) {
    const line = new FakeEl();
    if (row.lineIndex !== null) line.dataset.lineIndex = row.lineIndex;
    line.height = row.height;
    content.append(line);
    let extra;
    if (row.annotation != null || row.noNewline != null) {
      extra = new FakeEl();
      if (row.annotation != null) {
        extra.dataset.lineAnnotation = row.annotation.key ?? "0,0";
        extra.height = row.annotation.height;
      }
      if (row.noNewline != null) {
        extra.dataset.noNewline = "";
        extra.height = row.noNewline.height;
      }
      content.append(extra);
    }
    built.rows.push({ line, extra });
  }
  for (let index = 0; index + 1 < content.children.length; index++) {
    content.children[index].nextElementSibling = content.children[index + 1];
  }
  return built;
}

function snapshot(instance, names) {
  const { cache } = instance;
  const { renderRange } = instance;
  return {
    height: instance.height,
    top: instance.top ?? null,
    layoutDirty: instance.layoutDirty,
    renderRange:
      renderRange == null
        ? null
        : {
            startingLine: renderRange.startingLine,
            totalLines: renderRange.totalLines,
            bufferBefore: renderRange.bufferBefore,
            bufferAfter: renderRange.bufferAfter,
          },
    isVisible: instance.isVisible,
    forceRenderOverride: instance.forceRenderOverride === true,
    pendingRender:
      instance.pendingRender == null
        ? null
        : {
            workingDiff: names.diff(instance.pendingRender.workingDiff),
            diff: names.diff(instance.pendingRender.diff),
          },
    inputDiff: names.diff(instance.getInputDiff()),
    renderedDiff: names.diff(instance.getRenderedDiff()),
    annotations: names.list(instance.getLatestAnnotations()),
    heightDeltas: [...cache.heightDeltas].sort((a, b) => a[0] - b[0]),
    measuredHeightDeltaTotal: cache.measuredHeightDeltaTotal,
    estimatedSplitHeight: cache.estimatedSplitHeight ?? null,
    estimatedUnifiedHeight: cache.estimatedUnifiedHeight ?? null,
    checkpoints: cache.checkpoints.map(({ renderedLineIndex, lineIndex, top }) => ({ renderedLineIndex, lineIndex, top })),
    totalLines: cache.totalLines,
    fileAnnotationHeight: cache.fileAnnotationHeight,
  };
}

function rangeOf(range) {
  return {
    startingLine: range.startingLine,
    totalLines: range.totalLines,
    bufferBefore: range.bufferBefore,
    bufferAfter: range.bufferAfter,
  };
}

function applyStep(world, step) {
  const { instance, diffs, virtualizer, functions, lists, names } = world;
  const diff = (name) => (name == null ? undefined : diffs[name]);
  const list = (name) => (name == null ? undefined : lists[name]);
  switch (step.op) {
    case "options": {
      const next = buildOptions(step.spec, functions);
      // `VirtualizedFileDiff.setOptions` without the `instanceChanged` call
      // (stubbed: it would queue a frame).
      instance.setOptions(next);
      return null;
    }
    case "metrics":
      instance.setMetrics(step.metrics ?? undefined, step.force);
      return null;
    case "expand":
      instance.hunksRenderer.expandHunk(step.hunkIndex, step.direction, step.count ?? undefined);
      instance.resetLayoutCache({ includeEstimatedHeights: true });
      instance.computeApproximateSize();
      return null;
    case "deltas":
      for (const [lineIndex, delta] of step.entries) {
        const next = (instance.cache.heightDeltas.get(lineIndex) ?? 0) + delta;
        if (next === 0) instance.cache.heightDeltas.delete(lineIndex);
        else instance.cache.heightDeltas.set(lineIndex, next);
        instance.cache.measuredHeightDeltaTotal += delta;
      }
      return null;
    case "renderRange":
      instance.renderRange = step.set ? { ...step.range } : undefined;
      return null;
    case "layout":
      return instance.updateCodeViewLayout(diffs[step.diff], step.top, undefined, list(step.annotations));
    case "pendingRender": {
      const update = instance.updatePendingRender(diffs[step.diff], list(step.annotations));
      return {
        pendingRenderDiff: names.diff(update.pendingRenderDiff),
        layoutDiffChanged: update.layoutDiffChanged,
        renderedDiffChanged: update.renderedDiffChanged,
        annotationsChanged: update.annotationsChanged,
      };
    }
    case "syncLineAnnotations":
      return instance.syncLineAnnotations(list(step.annotations));
    case "install": {
      instance.renderedData = { fileDiff: diffs[step.diff], lineAnnotations: instance.getLatestAnnotations() };
      world.container = step.container ? new FakeEl() : undefined;
      if (world.container !== undefined) world.container.top = step.containerTop;
      instance.fileContainer = world.container;
      instance.placeHolder = step.placeholder ? new FakeEl() : undefined;
      world.groups = {};
      for (const name of GROUPS) {
        const spec = step.groups[name];
        world.groups[name] = spec == null ? undefined : buildGroup(spec);
      }
      instance.codeDeletions = world.groups.deletions?.group;
      instance.codeAdditions = world.groups.additions?.group;
      instance.codeUnified = world.groups.unified?.group;
      return null;
    }
    case "height": {
      const built = world.groups[step.group];
      const row = built.rows[step.row];
      const target = step.target === "fileAnnotation" ? built.fileAnnotation : row[step.target];
      target.height = step.height;
      return null;
    }
    case "containerTop":
      world.container.top = step.top;
      return null;
    case "reconcile":
      return instance.reconcileHeights();
    case "window":
      return rangeOf(instance.computeRenderRangeFromWindow(diffs[step.diff], step.fileTop, { top: step.top, bottom: step.bottom }));
    case "linePosition": {
      const position = instance.getLinePosition(step.line, step.side);
      return position === undefined ? null : { top: position.top, height: position.height };
    }
    case "onRender": {
      let calls = 0;
      instance.render = () => {
        calls++;
        return step.renderResult;
      };
      const result = instance.onRender(step.dirty);
      delete instance.render;
      return { result, renderCalls: calls };
    }
    case "visibility": {
      let calls = 0;
      instance.rerender = () => {
        calls++;
      };
      instance.setVisibility(step.visible);
      delete instance.rerender;
      return { rerenderCalls: calls };
    }
    case "syncTop":
      instance.syncVirtualizedTop();
      return null;
    default:
      throw new Error(`unknown step ${step.op}`);
  }
}

function runScenario(api, diffs, scenario) {
  const { VirtualizedFileDiff, Virtualizer } = api;
  const functions = new Map();
  const saved = Object.getOwnPropertyDescriptor(globalThis, "HTMLElement");
  Object.defineProperty(globalThis, "HTMLElement", { configurable: true, writable: true, value: FakeEl });
  const savedLog = console.log;
  console.log = () => {}; // `resizeDebugging`'s size validation logs
  try {
    const virtualizer = new Virtualizer({ resizeDebugging: scenario.resizeDebugging === true });
    virtualizer.instanceChanged = () => {};
    const instance = new VirtualizedFileDiff(buildOptions(scenario.options, functions), virtualizer, scenario.metrics ?? undefined);
    const diffNames = new Map(Object.entries(diffs).map(([name, value]) => [value, name]));
    const lists = scenario.lists;
    const listNames = new Map(Object.entries(lists).map(([name, value]) => [value, name]));
    const names = {
      diff: (value) => (value == null ? null : (diffNames.get(value) ?? "?")),
      list: (value) => listNames.get(value) ?? (value.length === 0 ? "empty" : "?"),
    };
    const world = { instance, diffs, virtualizer, functions, lists, names, container: undefined, groups: {} };
    const trace = [snapshot(instance, names)];
    const results = [];
    for (const step of scenario.steps) {
      let result;
      try {
        result = { value: applyStep(world, step) };
      } catch (error) {
        result = { throws: error.message };
      }
      results.push(result);
      trace.push(snapshot(instance, names));
    }
    return { results, trace };
  } finally {
    console.log = savedLog;
    if (saved === undefined) delete globalThis.HTMLElement;
    else Object.defineProperty(globalThis, "HTMLElement", saved);
  }
}

// ---- scenario generation ---------------------------------------------------------

const ROW_HEIGHTS = [0, 10, 18, 20, 20, 21.5, 40, 62.5];

/** A group of `count` rows from `start`; `lineIndex` is `unified,split` with the
 * two numbers apart so that choosing the wrong one shows. */
function rowsSpec(random, start, count, flags = {}) {
  return Array.from({ length: count }, (_, offset) => {
    const index = start + offset;
    const row = {
      lineIndex: random.next() < 0.03 ? null : `${index},${index + random.pick([0, 0, 1, 3])}`,
      height: flags.height ?? random.pick(ROW_HEIGHTS),
      annotation: null,
      noNewline: null,
    };
    if (random.next() < (flags.annotated ?? 0.1)) row.annotation = { height: random.pick([0, 12, 25.5, 40]), key: random.pick([null, null, null, "3,3", FILE_ANNOTATION_KEY]) };
    if (random.next() < (flags.noNewline ?? 0.05)) row.noNewline = { height: random.pick([0, 10, 20]) };
    return row;
  });
}

function groupSpec(random, start, count, flags = {}) {
  const spec = { rows: rowsSpec(random, start, count, flags), fileAnnotation: null };
  if (random.next() < (flags.fileAnnotation ?? 0.3)) {
    spec.fileAnnotation = { height: random.pick([0, 12, 25, 31.5]), key: random.next() < 0.1 ? "0,0" : null };
  }
  return spec;
}

function installStep(random, state, flags = {}) {
  const style = state.spec.diffStyle ?? "split";
  const rows = state.rows;
  const start = random.next() < 0.5 ? 0 : random.int(Math.max(rows, 1));
  const count = random.pick([0, 1, 2, 5, 12, 30]);
  const groups = {};
  if (style === "split") {
    if (random.next() < 0.9) groups.deletions = groupSpec(random, start, count, flags);
    if (random.next() < 0.9) groups.additions = groupSpec(random, start, count, flags);
  } else if (random.next() < 0.95) {
    groups.unified = groupSpec(random, start, count, flags);
  }
  const container = random.next() < 0.92;
  // What a later `height` or `containerTop` step may address.
  state.hasContainer = container;
  state.heightTargets = [];
  for (const [group, spec] of Object.entries(groups)) {
    spec.rows.forEach((row, index) => {
      state.heightTargets.push({ group, row: index, target: "line" });
      if (row.annotation != null || row.noNewline != null) state.heightTargets.push({ group, row: index, target: "extra" });
    });
    if (spec.fileAnnotation != null) state.heightTargets.push({ group, row: 0, target: "fileAnnotation" });
  }
  return {
    op: "install",
    diff: state.renderedDiff ?? state.diff,
    container,
    containerTop: random.pick([0, 0, 120, 500.5, 3000]),
    placeholder: random.next() < 0.1,
    groups,
  };
}

function randomWindow(random, state) {
  const height = state.height ?? 1000;
  const viewport = random.pick([1, 30, 200, 800, 5000]);
  const top = random.pick([-100, 0, 0, 1, 50, height / 2, height - viewport, height, height + 10, random.int(Math.max(1, Math.ceil(height)))]);
  return { fileTop: random.pick([0, 0, 0, 40, 500, -200]), top, bottom: top + viewport };
}

function rowsOf(diffs, name, style) {
  const diff = diffs[name];
  return style === "split" ? diff.splitLineCount : diff.unifiedLineCount;
}

function randomStep(random, diffs, state) {
  switch (random.int(30)) {
    case 0:
    case 1:
      state.diff = random.pick(state.pool);
      state.rows = rowsOf(diffs, state.diff, state.spec.diffStyle ?? "split");
      return { op: "layout", diff: state.diff, top: random.pick([0, 0, 100, 750.5]), annotations: random.pick([null, null, "A", "B", "empty", "A"]) };
    case 2:
      return { op: "pendingRender", diff: random.next() < 0.7 ? state.diff : random.pick(state.pool), annotations: random.pick([null, "A", "B", "empty"]) };
    case 3:
      return { op: "syncLineAnnotations", annotations: random.pick([null, "A", "B", "empty"]) };
    case 4:
      state.spec = mutateSpec(random, state.spec);
      state.rows = rowsOf(diffs, state.diff, state.spec.diffStyle ?? "split");
      return { op: "options", spec: state.spec };
    case 5:
      return { op: "metrics", metrics: random.pick([null, ...Object.values(METRICS).filter((metrics) => metrics !== undefined && metrics.hunkLineCount > 0)]), force: random.next() < 0.3 };
    case 6:
    case 7:
      return { op: "expand", hunkIndex: random.int(diffs[state.diff].hunks.length + 1), direction: random.pick(["up", "down", "both"]), count: random.pick([null, 1, 2, 5, 10, 100]) };
    case 8:
      return { op: "deltas", entries: [[random.int(Math.min(Math.max(state.rows, 1), 100)), random.pick([-5, 3, 7, 12.5, 0.25])]] };
    case 9:
      return { op: "renderRange", set: random.next() < 0.6, range: random.pick([{ startingLine: 0, totalLines: 5, bufferBefore: 0, bufferAfter: 0 }, { startingLine: 3, totalLines: 5, bufferBefore: 10, bufferAfter: 10 }, { startingLine: 0, totalLines: 0, bufferBefore: 0, bufferAfter: 50 }]) };
    case 10:
      return { op: "reconcile" };
    case 11:
    case 12:
    case 13:
      state.renderedDiff = random.next() < 0.8 ? state.diff : random.pick(state.pool);
      return installStep(random, state);
    case 14:
      return state.heightTargets.length === 0
        ? { op: "reconcile" }
        : { op: "height", ...random.pick(state.heightTargets), height: random.pick(ROW_HEIGHTS) };
    case 15:
      return state.hasContainer ? { op: "containerTop", top: random.pick([0, 33, 800.5]) } : { op: "syncTop" };
    case 16:
    case 17:
    case 18:
      return { op: "reconcile" };
    case 19:
    case 20:
    case 21:
      return { op: "window", diff: random.next() < 0.85 ? state.diff : random.pick(state.pool), ...randomWindow(random, state) };
    case 22:
    case 23:
      return { op: "linePosition", line: random.pick([0, 1, 2, 5, 17, 40, 99, 1000, 1 + random.int(Math.max(state.rows, 1))]), side: random.pick(["additions", "additions", "deletions"]) };
    case 24:
      return { op: "onRender", dirty: random.next() < 0.5, renderResult: random.next() < 0.5 };
    case 25:
      return { op: "visibility", visible: random.next() < 0.5 };
    case 26:
      return { op: "syncTop" };
    default:
      return { op: "reconcile" };
  }
}

function annotationLists(random) {
  const make = () =>
    Array.from({ length: random.int(4) }, () => ({
      side: random.pick(["additions", "deletions"]),
      lineNumber: random.pick([0, 0, 1, 3, 7]),
      metadata: 1,
    }));
  return { A: make().concat([{ side: "additions", lineNumber: 0, metadata: 1 }]), B: make(), empty: [] };
}

function randomScenario(random, diffs, index) {
  const large = index % 10 === 0;
  const pool = large ? QUERY_DIFFS_LARGE : QUERY_DIFFS_SMALL;
  const spec = randomSpec(random);
  if (random.next() < 0.5) spec.overflow = "wrap";
  const state = { diff: random.pick(pool), pool, spec, rows: 0, renderedDiff: null, height: null, heightTargets: [], hasContainer: false };
  state.rows = rowsOf(diffs, state.diff, spec.diffStyle ?? "split");
  const lists = annotationLists(random);
  const steps = [{ op: "layout", diff: state.diff, top: 0, annotations: random.pick([null, "A", "B"]) }];
  steps.push(installStep(random, state));
  const count = large ? 14 + random.int(8) : 16 + random.int(28);
  for (let step = 0; step < count; step++) steps.push(randomStep(random, diffs, state));
  return {
    name: `random${index}`,
    metrics: random.pick(Object.values(METRICS).filter((metrics) => metrics === undefined || metrics.hunkLineCount > 0)) ?? null,
    options: spec,
    resizeDebugging: random.next() < 0.05,
    lists,
    steps,
  };
}

const EMPTY_LISTS = () => ({ A: [{ side: "additions", lineNumber: 0, metadata: 1 }], B: [], empty: [] });
const GEOMETRY_METRICS = [METRICS.upstreamTest, METRICS.fractional, METRICS.codeView, null];

/** Window sweeps: the window at the top, the bottom, past either end, larger than
 * the file, tiny, with the file displaced and with measured heights in between. */
function sweeps(diffs) {
  const scenarios = [];
  const diffList = ["twoHunks", "multi3", "gaps1", "thresholds", "uneven", "noeofBoth", "partial0", "new", "noHunks", "huge8", "long", "longTail", "huge10k", "huge1m", "oneLine", "empty"];
  const optionSets = [{}, { diffStyle: "unified" }, { expandUnchanged: true }, { loadDiffFiles: true }, { collapsed: true }, { disableFileHeader: true, collapsedContextThreshold: 1 }, { hunkSeparators: "metadata", diffStyle: "unified" }];
  for (const diff of diffList) {
    for (const [metricsIndex, metrics] of GEOMETRY_METRICS.entries()) {
      const spec = optionSets[(metricsIndex + diff.length) % optionSets.length];
      const steps = [{ op: "layout", diff, top: 0, annotations: null }];
      const windows = [];
      for (const fileTop of [0, 500]) {
        for (const [top, size] of [[-50, 20], [0, 1], [0, 100], [0, 100000], [0, 800], [50, 800], [200, 5000], [1e9, 100]]) {
          windows.push({ fileTop, top: top + fileTop, bottom: top + fileTop + size });
        }
      }
      for (const window of windows) steps.push({ op: "window", diff, ...window });
      // Windows at the bottom: derived from the height after layout is unknown here,
      // so sweep a grid across the plausible range instead.
      for (const top of [100, 1000, 4000, 20000, 90000, 400000, 3e6, 9e6, 1.2e7]) {
        steps.push({ op: "window", diff, fileTop: 0, top, bottom: top + 900 });
      }
      // Windows whose edges fall exactly on row boundaries (header 30, rows of 10).
      for (let edge = 30; edge <= 420; edge += 10) {
        steps.push({ op: "window", diff, fileTop: 0, top: edge, bottom: edge + 40 });
        steps.push({ op: "window", diff, fileTop: 0, top: edge - 40, bottom: edge });
      }
      for (const line of [0, 1, 2, 10, 60, 999, 1500, 6000, 7000, 900000, 1000000, 1000001]) {
        for (const side of ["additions", "deletions"]) steps.push({ op: "linePosition", line, side });
      }
      scenarios.push({ name: `edge:sweep:${diff}:${metricsIndex}`, metrics, options: spec, lists: EMPTY_LISTS(), steps });
    }
  }
  return scenarios;
}

function fixedGroup(count, heights) {
  return {
    rows: Array.from({ length: count }, (_, index) => ({ lineIndex: `${index},${index}`, height: heights(index), annotation: null, noNewline: null })),
    fileAnnotation: null,
  };
}

/** Estimates corrected by measurement, annotations growing above the viewport. */
function measurementScenarios() {
  const scenarios = [];
  const lists = EMPTY_LISTS();
  for (const [name, style, overflow] of [["wrap", "split", "wrap"], ["scroll", "split", "scroll"], ["unified", "unified", "wrap"]]) {
    const spec = { diffStyle: style, overflow };
    const group = (height, extra = {}) => ({ ...fixedGroup(8, () => height), ...extra });
    const groups = (spec) => (style === "split" ? { deletions: spec, additions: spec } : { unified: spec });
    const steps = [
      { op: "layout", diff: "twoHunks", top: 0, annotations: null },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 0, bottom: 300 },
      { op: "renderRange", set: true, range: { startingLine: 0, totalLines: 8, bufferBefore: 0, bufferAfter: 0 } },
      { op: "install", diff: "twoHunks", container: true, containerTop: 0, placeholder: false, groups: groups(group(30)) },
      { op: "reconcile" },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 0, bottom: 300 },
      { op: "reconcile" },
      { op: "height", group: style === "split" ? "additions" : "unified", row: 2, target: "line", height: 20 },
      { op: "reconcile" },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 600, bottom: 900 },
      { op: "linePosition", line: 40, side: "additions" },
      { op: "linePosition", line: 100, side: "additions" },
      // A file annotation appears above the viewport and grows.
      { op: "layout", diff: "twoHunks", top: 0, annotations: "A" },
      { op: "install", diff: "twoHunks", container: true, containerTop: 0, placeholder: false, groups: groups({ ...group(20), fileAnnotation: { height: 25, key: null } }) },
      { op: "renderRange", set: true, range: { startingLine: 0, totalLines: 8, bufferBefore: 0, bufferAfter: 0 } },
      { op: "reconcile" },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 200, bottom: 330 },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 55, bottom: 100 },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 0, bottom: 30 },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 54, bottom: 100 },
      { op: "height", group: style === "split" ? "additions" : "unified", row: 0, target: "fileAnnotation", height: 80 },
      { op: "reconcile" },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 200, bottom: 330 },
      { op: "window", diff: "twoHunks", fileTop: 0, top: 110, bottom: 300 },
      { op: "linePosition", line: 100, side: "additions" },
      { op: "height", group: style === "split" ? "additions" : "unified", row: 0, target: "fileAnnotation", height: 0 },
      { op: "reconcile" },
      // The annotation is removed; measured rows are corrected anyway.
      { op: "layout", diff: "twoHunks", top: 0, annotations: "empty" },
      { op: "reconcile" },
      { op: "renderRange", set: true, range: { startingLine: 3, totalLines: 8, bufferBefore: 40, bufferAfter: 40 } },
      { op: "reconcile" },
      { op: "renderRange", set: true, range: { startingLine: 0, totalLines: 8, bufferBefore: 0, bufferAfter: 0 } },
      { op: "install", diff: "twoHunks", container: true, containerTop: 0, placeholder: false, groups: groups(group(20)) },
      { op: "reconcile" },
      { op: "reconcile" },
      // No container and a placeholder.
      { op: "install", diff: "twoHunks", container: false, containerTop: 0, placeholder: false, groups: groups(group(50)) },
      { op: "reconcile" },
      { op: "install", diff: "twoHunks", container: true, containerTop: 0, placeholder: true, groups: groups(group(50)) },
      { op: "reconcile" },
      { op: "install", diff: "oneLine", container: true, containerTop: 12, placeholder: false, groups: groups(group(50)) },
      { op: "syncTop" },
      { op: "reconcile" },
      { op: "visibility", visible: true },
      { op: "visibility", visible: true },
      { op: "onRender", dirty: true, renderResult: true },
      { op: "visibility", visible: false },
      { op: "visibility", visible: false },
      { op: "onRender", dirty: false, renderResult: false },
      { op: "install", diff: "oneLine", container: false, containerTop: 0, placeholder: false, groups: {} },
      { op: "onRender", dirty: true, renderResult: true },
      { op: "visibility", visible: true },
      { op: "syncTop" },
    ];
    scenarios.push({ name: `edge:measure:${name}`, metrics: METRICS.upstreamTest, options: spec, lists, steps });
  }
  return scenarios;
}

export async function buildS12bWindow(oracle, random, corpusMocks) {
  const { VirtualizedFileDiff } = await import(oracle.pierre("components/VirtualizedFileDiff.js"));
  const { Virtualizer } = await import(oracle.pierre("components/Virtualizer.js"));
  const diffs = await buildDiffPool(oracle, random, corpusMocks);
  const scenarios = [...sweeps(diffs), ...measurementScenarios()];
  for (let index = 0; index < 300; index++) scenarios.push(randomScenario(random, diffs, index));
  const api = { VirtualizedFileDiff, Virtualizer };
  const cases = scenarios.map((scenario) => ({ scenario: clone(scenario), ...runScenario(api, diffs, scenario) }));
  return { diffs, cases };
}
