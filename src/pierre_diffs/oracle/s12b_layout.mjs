// Differential scenarios for the layout model of `VirtualizedFileDiff` (S12b-1).
//
// The published `VirtualizedFileDiff` is constructed without a DOM over a stub
// virtualizer (`type: 'simple'`, a mutable `config`, `instanceChanged`), the
// way its own `virtualizedFileDiffEstimatedHeights` test does. Its layout
// methods are private in TypeScript but plain methods at runtime, so scenarios
// call them directly: the layout diff is set as the instance's `pendingRender`
// (what `getLayoutDiff()` reads first), options go through the published
// `setOptions` (which exercises `hasDiffLayoutOptionChanged`,
// `hasDiffEstimateOptionChanged` and `resetLayoutCache({forceSimpleRecompute})`),
// expansions through the instance's renderer (`hunksRenderer.expandHunk`, as the
// simple-mode `expandHunk` does) followed by `resetLayoutCache` and
// `computeApproximateSize`, and measured deltas are written into `cache` the way
// `reconcileHeights` does. After every step the oracle records the step's
// return value (or the message it threw) and every derived value: `height`,
// `top`, `layoutDirty`, whether `renderRange` is set, and the whole cache (the
// height deltas sorted by index, the measured total, both estimates, the
// checkpoints, `totalLines` and the file annotation height). The Rust port
// replays the same scenarios against `DiffLayout`
// (`components/virtualized_file_diff/layout_oracle_tests.rs`).
//
// Diffs come from the corpus and seeded synthetic files (several hunks with
// collapsed context, no newline at end of file on either side, new, deleted and
// renamed files, empty sides, a one-line file, partial diffs parsed from
// patches, files long enough for layout checkpoints, a one-million-line block,
// a diff without hunks and a diff whose trailing context disagrees between the
// sides so that the throw paths are compared too). What this establishes: the
// layout arithmetic, cache invalidation and the checkpoint searches for the same
// inputs. What it cannot: that the DOM measurements feeding the cache are right
// (S12b-2).

import { clone } from "./s11a_cases.mjs";
import { buildOptions } from "./s12a_file_diff.mjs";

// ---- the pools scenarios name ----------------------------------------------------

export const METRICS = {
  default: undefined,
  upstreamTest: { hunkLineCount: 2, lineHeight: 10, diffHeaderHeight: 30, spacing: 4 },
  fractional: {
    hunkLineCount: 7,
    lineHeight: 18.5,
    diffHeaderHeight: 41.25,
    hunkSeparatorHeight: 27.5,
    spacing: 6.5,
    paddingTop: 3.5,
    paddingBottom: 9,
  },
  zeroHunk: { hunkLineCount: 0, lineHeight: 12, diffHeaderHeight: 20, spacing: 2 },
  codeView: { hunkLineCount: 1, lineHeight: 22, diffHeaderHeight: 36, spacing: 10 },
};

function numbered(count, edits, newline = true) {
  const lines = Array.from({ length: count }, (_, index) =>
    edits.includes(index + 1) ? `edited ${index + 1}` : `line ${index + 1}`,
  );
  return lines.join("\n") + (newline ? "\n" : "");
}

function file(name, contents) {
  return { name, contents };
}

/** Every named diff, as JSON data. */
export async function buildDiffPool(oracle, random, corpusMocks) {
  const { parseDiffFromFile } = await import(oracle.pierre("utils/parseDiffFromFile.js"));
  const { parsePatchFiles } = await import(oracle.pierre("utils/parsePatchFiles.js"));
  const jsdiff = await import(oracle.jsdiff);
  const json = (value) => JSON.parse(JSON.stringify(value));
  const diffs = {};
  const parse = (name, oldFile, newFile, options) => {
    diffs[name] = json(parseDiffFromFile(oldFile, newFile, options));
  };

  let index = 0;
  for (const diff of Object.values(corpusMocks.mockDiffs)) {
    parse(`mock${index++}`, diff.oldFile, diff.newFile);
  }
  parse(`mock${index++}`, corpusMocks.mockFiles.file1, { ...corpusMocks.mockFiles.file2, name: corpusMocks.mockFiles.file1.name });

  for (const context of [0, 1, 3, 8]) {
    parse(`multi${context}`, file("m.txt", numbered(60, [])), file("m.txt", numbered(60, [5, 20, 21, 44, 58])), { context });
    parse(`gaps${context}`, file("g.txt", numbered(40, [])), file("g.txt", numbered(40, [3, 9, 30])), { context });
  }
  // Leading and trailing gaps of exactly the collapsed context threshold.
  parse("thresholds", file("t.txt", numbered(30, [])), file("t.txt", numbered(30, [7, 24])));
  // Insertions and removals in the middle (unified and split row counts differ).
  {
    const old = numbered(50, []).split("\n");
    const changed = old.slice();
    changed.splice(10, 4);
    changed.splice(30, 0, "inserted a", "inserted b", "inserted c");
    changed[40] = "replaced";
    parse("uneven", file("u.txt", old.join("\n")), file("u.txt", changed.join("\n")));
  }
  for (let draw = 0; draw < 4; draw++) {
    const count = 20 + random.int(80);
    const edits = Array.from({ length: 1 + random.int(5) }, () => 1 + random.int(count));
    parse(`random${draw}`, file("r.txt", numbered(count, [])), file("r.txt", numbered(count, edits)), {
      context: random.pick([0, 1, 2, 3]),
    });
  }

  // No newline at end of file: the last row gets a metadata line.
  const base = numbered(12, []);
  const baseNoEof = numbered(12, [], false);
  parse("noeofBoth", file("n.txt", baseNoEof), file("n.txt", numbered(12, [4], false)));
  parse("noeofBothTail", file("n.txt", baseNoEof), file("n.txt", numbered(12, [12], false)));
  parse("noeofAdditions", file("n.txt", base), file("n.txt", numbered(12, [12], false)));
  parse("noeofDeletions", file("n.txt", baseNoEof), file("n.txt", numbered(12, [12])));
  parse("noeofAdditionsMid", file("n.txt", base), file("n.txt", numbered(12, [5], false)));
  parse("noeofDeletionsMid", file("n.txt", baseNoEof), file("n.txt", numbered(12, [5])));
  parse("noeofAdd", file("n.txt", numbered(12, [])), file("n.txt", numbered(13, [], false)));
  parse("noeofDel", file("n.txt", numbered(13, [], false)), file("n.txt", numbered(12, [])));
  parse("noeofFlip", file("n.txt", baseNoEof), file("n.txt", base));
  parse("noeofSame", file("n.txt", baseNoEof), file("n.txt", baseNoEof.replace("line 6", "line six")));

  // Change types and degenerate sides.
  parse("new", file("a.txt", ""), file("a.txt", numbered(9, [])));
  parse("deleted", file("a.txt", numbered(9, [])), file("a.txt", ""));
  parse("empty", file("a.txt", ""), file("a.txt", ""));
  parse("oneLine", file("a.txt", "one\n"), file("a.txt", "two\n"));
  parse("oneLineNoEof", file("a.txt", "one"), file("a.txt", "two"));
  parse("renamePure", file("old.txt", numbered(9, [])), file("new.txt", numbered(9, [])));
  parse("renameChanged", file("old.txt", numbered(20, [])), file("new.txt", numbered(20, [8])));

  // Long enough for layout checkpoints (one every 3,000 rendered rows).
  parse("long", file("l.txt", numbered(7_000, [])), file("l.txt", numbered(7_000, [1_200, 3_100, 6_500])), { context: 3 });
  parse("longNoEof", file("l.txt", numbered(7_000, [])), file("l.txt", numbered(7_000, [10, 6_990], false)), { context: 2 });
  parse("longTail", file("l.txt", numbered(6_100, [])), file("l.txt", numbered(6_100, [6_099])), { context: 5 });
  parse("twoHunks", file("two.ts", numbered(140, [])), file("two.ts", numbered(140, [40, 100])));
  // Thousands of trailing (or leading) unchanged rows: a checkpoint falls inside them.
  parse("longTrailing", file("l.txt", numbered(7_000, [])), file("l.txt", numbered(7_000, [10])), { context: 3 });
  parse("longLeading", file("l.txt", numbered(7_000, [])), file("l.txt", numbered(7_000, [6_990])), { context: 3 });
  // One hunk that rewrites every line: unified rows pass the checkpoint interval
  // inside the change block, so the no-newline metadata rows shift later rows.
  {
    const everyLine = Array.from({ length: 3_500 }, (_, line) => line + 1);
    parse("bigChange", file("c.txt", numbered(3_500, [], false)), file("c.txt", numbered(3_500, everyLine, false)));
    parse("bigChangeNew", file("c.txt", numbered(3_500, [])), file("c.txt", numbered(3_500, everyLine, false)));
    // 2,500 unchanged rows first (no context lines), so that the checkpoint at row
    // 6,000 falls exactly on the row after the change block's deletion metadata row.
    parse(
      "bigChangeAligned",
      file("c.txt", numbered(6_000, [], false)),
      file("c.txt", numbered(6_000, Array.from({ length: 3_500 }, (_, line) => 2_501 + line), false)),
      { context: 0 },
    );
    parse("bigChangeOld", file("c.txt", numbered(3_500, [], false)), file("c.txt", numbered(3_500, everyLine)));
  }

  // Partial diffs: parsed from a patch, one line of context (and none).
  index = 0;
  for (const [oldText, newText, context] of [
    [numbered(40, []), numbered(40, [5, 25]), 1],
    [numbered(40, []), numbered(40, [5, 25]), 0],
    [numbered(30, []), numbered(30, [2, 3, 4, 28]), 2],
    [numbered(15, [], false), numbered(15, [3], false), 1],
  ]) {
    const patch = jsdiff.createTwoFilesPatch("p.txt", "p.txt", oldText, newText, undefined, undefined, { context });
    diffs[`partial${index++}`] = json(parsePatchFiles(patch)[0].files[0]);
  }
  {
    const patch = jsdiff.createTwoFilesPatch("old.txt", "new.txt", numbered(40, []), numbered(40, [5, 25]), undefined, undefined, { context: 1 });
    diffs.partialRename = json(parsePatchFiles(patch)[0].files[0]);
  }

  // Synthetic metadata: one huge context block (upstream's own helper).
  const hugeBlock = (name, lineCount, isPartial, type = "change") => {
    diffs[name] = {
      name: "huge.ts",
      type,
      hunks: [
        {
          collapsedBefore: 0,
          additionStart: 1,
          additionCount: lineCount,
          additionLines: 0,
          additionLineIndex: 0,
          deletionStart: 1,
          deletionCount: lineCount,
          deletionLines: 0,
          deletionLineIndex: 0,
          hunkContent: [{ type: "context", lines: lineCount, additionLineIndex: 0, deletionLineIndex: 0 }],
          splitLineStart: 0,
          splitLineCount: lineCount,
          unifiedLineStart: 0,
          unifiedLineCount: lineCount,
          noEOFCRDeletions: false,
          noEOFCRAdditions: false,
        },
      ],
      splitLineCount: lineCount,
      unifiedLineCount: lineCount,
      isPartial,
      deletionLines: [],
      additionLines: [],
      deletionCharacterCount: 0,
      additionCharacterCount: 0,
    };
  };
  hugeBlock("huge8", 8, true);
  hugeBlock("huge8Renamed", 8, true, "rename-changed");
  hugeBlock("huge8New", 8, true, "new");
  hugeBlock("huge1m", 1_000_000, true);
  hugeBlock("huge10k", 10_000, true);

  // A diff without hunks, and one whose sides disagree about the trailing context.
  diffs.noHunks = {
    name: "renamed.ts",
    prevName: "old-name.ts",
    type: "rename-pure",
    hunks: [],
    splitLineCount: 0,
    unifiedLineCount: 0,
    isPartial: false,
    deletionLines: [],
    additionLines: [],
    deletionCharacterCount: 0,
    additionCharacterCount: 0,
  };
  // Line arrays longer than anything the hunks account for (a stale layout after
  // an edit): `getLayoutLineCount` takes the longer array. Partial diffs and equal
  // extra lines on both sides do not trip the trailing context check.
  diffs.extraAdd = clone(diffs.partial0);
  diffs.extraAdd.additionLines = diffs.extraAdd.additionLines.concat(Array.from({ length: 30 }, (_, line) => `extra ${line}\n`));
  diffs.extraDel = clone(diffs.partial0);
  diffs.extraDel.deletionLines = diffs.extraDel.deletionLines.concat(Array.from({ length: 30 }, (_, line) => `extra ${line}\n`));
  diffs.extraBoth = clone(diffs.multi1);
  for (const key of ["additionLines", "deletionLines"]) {
    diffs.extraBoth[key] = diffs.extraBoth[key].concat(Array.from({ length: 20 }, (_, line) => `extra ${line}\n`));
  }
  diffs.mismatch = clone(diffs.multi1);
  diffs.mismatch.additionLines = diffs.mismatch.additionLines.concat(["extra\n", "extra\n"]);
  return diffs;
}

// ---- running one scenario --------------------------------------------------------

const RENDER_RANGE = { startingLine: 0, totalLines: 1, bufferBefore: 0, bufferAfter: 0 };

function snapshot(instance) {
  const { cache } = instance;
  return {
    height: instance.height,
    top: instance.top ?? null,
    layoutDirty: instance.layoutDirty,
    renderRangeSet: instance.renderRange != null,
    heightDeltas: [...cache.heightDeltas].sort((a, b) => a[0] - b[0]),
    measuredHeightDeltaTotal: cache.measuredHeightDeltaTotal,
    estimatedSplitHeight: cache.estimatedSplitHeight ?? null,
    estimatedUnifiedHeight: cache.estimatedUnifiedHeight ?? null,
    checkpoints: cache.checkpoints.map(({ renderedLineIndex, lineIndex, top }) => ({ renderedLineIndex, lineIndex, top })),
    totalLines: cache.totalLines,
    fileAnnotationHeight: cache.fileAnnotationHeight,
  };
}

function checkpointOrNull(checkpoint) {
  return checkpoint == null
    ? null
    : { renderedLineIndex: checkpoint.renderedLineIndex, lineIndex: checkpoint.lineIndex, top: checkpoint.top };
}

function applyStep(world, step) {
  const { instance, diffs, virtualizer, functions } = world;
  const diff = (name) => (name == null ? undefined : diffs[name]);
  switch (step.op) {
    case "layoutDiff":
      instance.pendingRender = step.diff == null ? undefined : { workingDiff: diffs[step.diff], diff: diffs[step.diff] };
      return null;
    case "options":
      instance.setOptions(buildOptions(step.spec, functions));
      return null;
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
    case "fileAnnotationHeight":
      return instance.setFileAnnotationHeight(step.height);
    case "annotations":
      // What a render leaves in `lineAnnotations` (a getter over `renderedData`;
      // `setLineAnnotations` only queues them as `pendingData` once there is a diff
      // to pair them with). No rendered diff, so `getLayoutDiff()` is unchanged.
      instance.renderedData = { fileDiff: undefined, lineAnnotations: step.list };
      return null;
    case "hasFileAnnotations":
      return instance.hasFileAnnotations(diffs[step.diff]);
    case "renderRange":
      instance.renderRange = step.set ? { ...RENDER_RANGE } : undefined;
      return null;
    case "resizeDebugging":
      virtualizer.config.resizeDebugging = step.on;
      return null;
    case "setTotalLines":
      instance.cache.totalLines = step.totalLines;
      return null;
    case "compute":
      instance.computeApproximateSize(step.force, diff(step.diff));
      return null;
    case "checkpoints":
      instance.approximateLayoutCheckpoints(diffs[step.diff]);
      return null;
    case "activeEstimatedHeight":
      return instance.getActiveEstimatedHeight(diff(step.diff));
    case "ensureEstimated":
      instance.ensureEstimatedDiffHeights(diff(step.diff));
      return null;
    case "reset":
      instance.resetLayoutCache(step.options);
      return null;
    case "invalidate":
      instance.invalidateDerivedLayoutCache(step.includeEstimated, step.resetRenderRange);
      return null;
    case "lineHeight":
      return instance.getLineHeight(step.index, step.meta);
    case "estimatedLineHeight":
      return instance.getEstimatedLineHeight(step.meta);
    case "checkpointBeforeLine":
      return checkpointOrNull(instance.getLayoutCheckpointBeforeLineIndex(step.index));
    case "checkpointBeforeTop":
      return checkpointOrNull(instance.getLayoutCheckpointBeforeTop(step.top, step.hunkLineCount ?? undefined));
    case "expandedLineCount":
      return instance.getExpandedLineCount(diffs[step.diff], step.style);
    case "layoutLineCount":
      return instance.getLayoutLineCount(diffs[step.diff], step.style);
    default:
      throw new Error(`unknown step ${step.op}`);
  }
}

function runScenario(VirtualizedFileDiff, diffs, scenario) {
  const functions = new Map();
  const virtualizer = {
    type: "simple",
    config: { resizeDebugging: scenario.resizeDebugging === true },
    instanceChanged() {},
  };
  const instance = new VirtualizedFileDiff(buildOptions(scenario.options, functions), virtualizer, scenario.metrics ?? undefined);
  const world = { instance, diffs, virtualizer, functions };
  const trace = [snapshot(instance)];
  const results = [];
  for (const step of scenario.steps) {
    let result;
    try {
      result = { value: applyStep(world, step) };
    } catch (error) {
      result = { throws: error.message };
    }
    results.push(result);
    trace.push(snapshot(instance));
  }
  return { results, trace };
}

// ---- scenario generation ---------------------------------------------------------

const LAYOUT_FIELDS = {
  diffStyle: ["split", "unified"],
  collapsed: [true, false],
  expandUnchanged: [true, false],
  collapsedContextThreshold: [0, 1, 2, 5, 100],
  disableFileHeader: [true, false],
  overflow: ["scroll", "wrap"],
  disableLineNumbers: [true, false],
  diffIndicators: ["bars", "classic", "none"],
  unsafeCSS: ["", "a{b:c}"],
  // Not layout options: they change the options without changing the layout.
  themeType: ["system", "light", "dark"],
  disableBackground: [true, false],
};
const SEPARATORS = [undefined, "simple", "metadata", "line-info", "line-info-basic", { fn: "a" }, { fn: "b" }];

export function randomSpec(random) {
  const spec = {};
  const chance = random.next() < 0.5 ? 0.3 : 0.7;
  for (const [field, values] of Object.entries(LAYOUT_FIELDS)) {
    if (random.next() < chance) spec[field] = random.pick(values);
  }
  const separators = random.pick(SEPARATORS);
  if (separators !== undefined && random.next() < chance) spec.hunkSeparators = separators;
  if (random.next() < chance * 0.4) spec.loadDiffFiles = true;
  return spec;
}

/** One-field mutation of a spec, so that equal and near-equal option pairs are common. */
export function mutateSpec(random, spec) {
  const copy = clone(spec);
  const roll = random.next();
  if (roll < 0.15) return copy;
  if (roll < 0.25) return randomSpec(random);
  const fresh = randomSpec(random);
  const keys = [...new Set([...Object.keys(fresh), ...Object.keys(copy)])];
  const key = random.pick(keys);
  if (fresh[key] === undefined) delete copy[key];
  else copy[key] = fresh[key];
  return copy;
}

export const QUERY_DIFFS_SMALL = [
  "mock0", "mock1", "multi0", "multi1", "multi3", "multi8", "gaps0", "gaps1", "gaps3", "thresholds", "uneven",
  "random0", "random1", "random2", "random3", "noeofBoth", "noeofBothTail", "noeofAdditions", "noeofDeletions",
  "noeofAdditionsMid", "noeofDeletionsMid", "noeofAdd", "noeofDel", "noeofFlip", "noeofSame", "new",
  "deleted", "empty", "oneLine", "oneLineNoEof", "renamePure", "renameChanged", "twoHunks", "partial0", "partial1",
  "partial2", "partial3", "partialRename", "extraAdd", "extraDel", "extraBoth", "huge8", "huge8Renamed", "huge8New", "noHunks",
];
export const QUERY_DIFFS_LARGE = ["long", "longNoEof", "longTail", "longTrailing", "longLeading", "bigChange", "bigChangeNew", "bigChangeOld", "bigChangeAligned", "huge10k", "huge1m"];

function rowCountOf(diffs, name, style) {
  const diff = diffs[name];
  return style === "split" ? diff.splitLineCount : diff.unifiedLineCount;
}

function randomStep(random, diffs, state) {
  const style = random.pick(["split", "unified"]);
  const name = state.diff ?? random.pick(state.pool);
  const rows = Math.max(rowCountOf(diffs, name, style), 1);
  const hunks = diffs[name].hunks.length;
  switch (random.int(24)) {
    case 0:
      state.diff = random.next() < 0.1 ? null : random.pick(state.pool);
      return { op: "layoutDiff", diff: state.diff };
    case 1:
      state.spec = mutateSpec(random, state.spec);
      return { op: "options", spec: state.spec };
    case 2:
      return {
        op: "metrics",
        metrics: random.pick([null, ...Object.values(METRICS).filter((metrics) => metrics !== undefined)]),
        force: random.next() < 0.3,
      };
    case 3:
    case 4:
      return {
        op: "expand",
        hunkIndex: random.int(hunks + 1),
        direction: random.pick(["up", "down", "both"]),
        count: random.pick([null, 1, 2, 5, 10, 100]),
      };
    case 5:
    case 6: {
      const entries = Array.from({ length: 1 + random.int(4) }, () => [
        random.next() < 0.7 ? random.int(Math.min(rows, 200)) : random.int(rows),
        random.pick([-5, -2.5, 3, 7, 12.5, 0.25, -7]),
      ]);
      return { op: "deltas", entries };
    }
    case 7:
      return { op: "fileAnnotationHeight", height: random.pick([0, 0, 24, 31.5, 12]) };
    case 8: {
      const list = [];
      for (const _ of Array.from({ length: random.int(3) })) {
        list.push({
          side: random.pick(["additions", "deletions"]),
          lineNumber: random.pick([0, 0, 1, 3, 7]),
          metadata: 1,
        });
      }
      return { op: "annotations", list };
    }
    case 9:
      return { op: "hasFileAnnotations", diff: name };
    case 10:
      return { op: "renderRange", set: random.next() < 0.6 };
    case 11:
      return { op: "compute", force: random.next() < 0.4, diff: random.next() < 0.5 ? name : null };
    case 12:
    case 13:
      return { op: "checkpoints", diff: name };
    case 14:
      return { op: "activeEstimatedHeight", diff: random.next() < 0.4 ? name : null };
    case 15:
      return random.next() < 0.5
        ? { op: "ensureEstimated", diff: random.next() < 0.5 ? name : null }
        : { op: "reset", options: randomResetOptions(random) };
    case 16:
      return { op: "invalidate", includeEstimated: random.next() < 0.5, resetRenderRange: random.next() < 0.6 };
    case 17:
      return random.next() < 0.7
        ? { op: "lineHeight", index: random.int(Math.min(rows, 50)), meta: random.next() < 0.3 }
        : { op: "estimatedLineHeight", meta: random.next() < 0.5 };
    case 18:
      return { op: "checkpointBeforeLine", index: random.pick([-1, 0, 1, rows, random.int(rows + 5), random.int(rows * 2)]) };
    case 19:
      return {
        op: "checkpointBeforeTop",
        top: random.pick([-10, 0, 30, random.int(rows * 20), random.int(rows * 40) / 3]),
        hunkLineCount: random.pick([null, null, 1, 2, 7, 50, 3000]),
      };
    case 20:
      return { op: "expandedLineCount", diff: name, style };
    case 21:
      return { op: "layoutLineCount", diff: name, style };
    case 22:
      return { op: "setTotalLines", totalLines: random.pick([0, 1, rows, rows + 10, rows * 2]) };
    default:
      return { op: "resizeDebugging", on: random.next() < 0.5 };
  }
}

function randomResetOptions(random) {
  const options = {};
  if (random.next() < 0.5) options.forceSimpleRecompute = random.next() < 0.6;
  if (random.next() < 0.5) options.includeEstimatedHeights = random.next() < 0.6;
  if (random.next() < 0.5) options.resetRenderRange = random.next() < 0.6;
  return options;
}

function randomScenario(random, diffs, index) {
  const large = index % 12 === 0;
  const pool = large ? QUERY_DIFFS_LARGE : QUERY_DIFFS_SMALL;
  const spec = randomSpec(random);
  const state = { diff: random.pick(pool), pool, spec };
  const steps = [{ op: "layoutDiff", diff: state.diff }];
  const count = large ? 8 + random.int(6) : 10 + random.int(24);
  // Mostly keep one diff; `layoutDiff` steps change it.
  for (let step = 0; step < count; step++) {
    steps.push(randomStep(random, diffs, state));
  }
  return {
    name: `random${index}`,
    metrics: random.pick(Object.values(METRICS)) ?? null,
    options: spec,
    resizeDebugging: random.next() < 0.1,
    steps,
  };
}

/** Handwritten edge cases: each runs the whole layout surface over one diff. */
function handwritten(diffs) {
  const scenarios = [];
  const full = (diff) => [
    { op: "layoutDiff", diff },
    { op: "compute", force: true, diff: null },
    { op: "checkpoints", diff },
    { op: "expandedLineCount", diff, style: "split" },
    { op: "expandedLineCount", diff, style: "unified" },
    { op: "layoutLineCount", diff, style: "split" },
    { op: "layoutLineCount", diff, style: "unified" },
    { op: "activeEstimatedHeight", diff: null },
    { op: "options", spec: { diffStyle: "unified" } },
    { op: "checkpoints", diff },
    { op: "expandedLineCount", diff, style: "unified" },
    { op: "options", spec: { diffStyle: "unified", collapsed: true } },
    { op: "checkpoints", diff },
    { op: "options", spec: { expandUnchanged: true } },
    { op: "checkpoints", diff },
    { op: "layoutLineCount", diff, style: "split" },
    { op: "options", spec: { hunkSeparators: "simple", disableFileHeader: true } },
    { op: "checkpoints", diff },
    { op: "options", spec: { hunkSeparators: { fn: "a" }, loadDiffFiles: true } },
    { op: "compute", force: true, diff: null },
  ];
  for (const diff of Object.keys(diffs)) {
    if (diff === "huge1m" || diff === "mismatch") continue;
    scenarios.push({
      name: `edge:${diff}`,
      metrics: diff.startsWith("huge") || diff.startsWith("long") ? METRICS.upstreamTest : null,
      options: {},
      steps: full(diff),
    });
  }
  scenarios.push({
    name: "edge:mismatch",
    metrics: null,
    options: {},
    steps: [
      { op: "layoutDiff", diff: "mismatch" },
      { op: "compute", force: true, diff: null },
      { op: "checkpoints", diff: "mismatch" },
      { op: "expandedLineCount", diff: "mismatch", style: "split" },
      { op: "layoutLineCount", diff: "mismatch", style: "unified" },
      { op: "options", spec: { expandUnchanged: true } },
      { op: "ensureEstimated", diff: "mismatch" },
    ],
  });
  // No diff at all: the estimate is missing, `activeEstimatedHeight` throws.
  scenarios.push({
    name: "edge:noLayoutDiff",
    metrics: null,
    options: {},
    steps: [
      { op: "compute", force: true, diff: null },
      { op: "activeEstimatedHeight", diff: null },
      { op: "ensureEstimated", diff: null },
      { op: "checkpoints", diff: "multi1" },
      { op: "setTotalLines", totalLines: 5 },
      { op: "compute", force: true, diff: "multi1" },
      { op: "layoutDiff", diff: null },
      { op: "compute", force: true, diff: null },
    ],
  });
  // The one-million-line block: checkpoints jump through the uniform block.
  scenarios.push({
    name: "edge:huge1m",
    metrics: METRICS.upstreamTest,
    options: {},
    steps: [
      { op: "layoutDiff", diff: "huge1m" },
      { op: "compute", force: true, diff: null },
      { op: "checkpoints", diff: "huge1m" },
      { op: "checkpointBeforeLine", index: 900_000 },
      { op: "checkpointBeforeTop", top: 9_000_000, hunkLineCount: null },
      { op: "checkpointBeforeTop", top: 9_000_000, hunkLineCount: 3_000 },
      { op: "checkpointBeforeTop", top: 9_000_000, hunkLineCount: 7 },
      { op: "checkpointBeforeTop", top: 9_000_000, hunkLineCount: 0 },
      { op: "deltas", entries: [[0, 7], [2_999, -2], [3_000, 4], [500_000, 1.5]] },
      { op: "checkpoints", diff: "huge1m" },
      { op: "compute", force: true, diff: null },
      { op: "layoutLineCount", diff: "huge1m", style: "split" },
      { op: "options", spec: { diffStyle: "unified" } },
      { op: "checkpoints", diff: "huge1m" },
    ],
  });
  // Measured deltas inside and between checkpoint blocks (upstream's partial-sum path).
  for (const diff of ["long", "longNoEof", "longTail"]) {
    scenarios.push({
      name: `edge:deltas:${diff}`,
      metrics: METRICS.fractional,
      options: { expandUnchanged: true },
      steps: [
        { op: "layoutDiff", diff },
        { op: "compute", force: true, diff: null },
        { op: "deltas", entries: [[0, 7], [1, -2.5], [2_999, 3], [3_000, 12.5], [3_001, -7], [5_999, 0.25], [6_000, 3]] },
        { op: "compute", force: false, diff: null },
        { op: "checkpoints", diff },
        { op: "fileAnnotationHeight", height: 31.5 },
        { op: "checkpoints", diff },
        { op: "compute", force: true, diff: null },
        { op: "checkpoints", diff },
        { op: "checkpointBeforeLine", index: 2_999 },
        { op: "checkpointBeforeLine", index: 3_000 },
        { op: "checkpointBeforeLine", index: 6_001 },
        { op: "checkpointBeforeTop", top: 60_000, hunkLineCount: 3_000 },
        { op: "checkpointBeforeTop", top: 60_000, hunkLineCount: 2 },
        { op: "layoutLineCount", diff, style: "split" },
        { op: "setTotalLines", totalLines: 9_000 },
        { op: "layoutLineCount", diff, style: "split" },
        { op: "metrics", metrics: METRICS.upstreamTest, force: false },
        { op: "checkpoints", diff },
        { op: "metrics", metrics: METRICS.upstreamTest, force: false },
        { op: "metrics", metrics: METRICS.upstreamTest, force: true },
      ],
    });
  }
  // Partial expansions of long gaps: a `fromEnd` block carries its separator, the
  // hunk body after it must not add it again, and a checkpoint lands in a later block.
  scenarios.push({
    name: "edge:expandBig:long",
    metrics: METRICS.fractional,
    options: {},
    steps: [
      { op: "layoutDiff", diff: "long" },
      { op: "expand", hunkIndex: 1, direction: "down", count: 1_500 },
      { op: "expand", hunkIndex: 2, direction: "up", count: 3_300 },
      { op: "checkpoints", diff: "long" },
      { op: "expand", hunkIndex: 2, direction: "down", count: 40 },
      { op: "checkpoints", diff: "long" },
      { op: "expand", hunkIndex: 3, direction: "up", count: 20 },
      { op: "checkpoints", diff: "long" },
      { op: "options", spec: { hunkSeparators: "simple" } },
      { op: "checkpoints", diff: "long" },
      { op: "expandedLineCount", diff: "long", style: "split" },
    ],
  });
  for (const diff of ["longTrailing", "longLeading", "bigChange", "bigChangeNew", "bigChangeOld", "bigChangeAligned"]) {
    for (const spec of [{ expandUnchanged: true }, { diffStyle: "unified" }, { diffStyle: "unified", expandUnchanged: true }]) {
      scenarios.push({
        name: `edge:checkpoints:${diff}:${spec.diffStyle ?? "split"}:${spec.expandUnchanged ?? false}`,
        metrics: METRICS.fractional,
        options: spec,
        steps: [
          { op: "layoutDiff", diff },
          { op: "deltas", entries: [[5, 7], [2_990, -2.5], [3_100, 4]] },
          { op: "checkpoints", diff },
          { op: "layoutLineCount", diff, style: "split" },
          { op: "layoutLineCount", diff, style: "unified" },
          { op: "checkpointBeforeLine", index: 4_000 },
          { op: "checkpointBeforeTop", top: 1_000_000, hunkLineCount: 3_000 },
        ],
      });
    }
  }
  for (const diff of ["extraAdd", "extraDel", "extraBoth"]) {
    scenarios.push({
      name: `edge:staleLines:${diff}`,
      metrics: null,
      options: {},
      steps: [
        { op: "layoutDiff", diff },
        { op: "layoutLineCount", diff, style: "split" },
        { op: "layoutLineCount", diff, style: "unified" },
        { op: "expandedLineCount", diff, style: "split" },
        { op: "checkpoints", diff },
        { op: "layoutLineCount", diff, style: "split" },
      ],
    });
  }
  // The file annotation: sides must match for one-sided diffs.
  for (const [diff, side] of [["new", "additions"], ["new", "deletions"], ["deleted", "additions"], ["deleted", "deletions"], ["multi1", "additions"], ["multi1", "deletions"], ["noHunks", "additions"]]) {
    scenarios.push({
      name: `edge:fileAnnotation:${diff}:${side}`,
      metrics: null,
      options: {},
      steps: [
        { op: "layoutDiff", diff },
        { op: "hasFileAnnotations", diff },
        { op: "annotations", list: [{ side, lineNumber: 0, metadata: 1 }] },
        { op: "hasFileAnnotations", diff },
        { op: "annotations", list: [{ side, lineNumber: 2, metadata: 1 }] },
        { op: "hasFileAnnotations", diff },
        { op: "annotations", list: [{ side, lineNumber: 2, metadata: 1 }, { side: "additions", lineNumber: 0, metadata: 2 }] },
        { op: "hasFileAnnotations", diff },
        { op: "fileAnnotationHeight", height: 24 },
        { op: "fileAnnotationHeight", height: 24 },
        { op: "compute", force: true, diff: null },
        { op: "checkpoints", diff },
        { op: "fileAnnotationHeight", height: 0 },
        { op: "compute", force: true, diff: null },
      ],
    });
  }
  // Option sequences: every layout-relevant field flipped and flipped back, with a
  // measured delta and a checkpoint build in between so that what survives shows.
  const flips = [
    { diffStyle: "unified" },
    { diffStyle: "split" },
    { overflow: "wrap" },
    { collapsed: true },
    { collapsed: false },
    { disableLineNumbers: true },
    { disableFileHeader: true },
    { diffIndicators: "classic" },
    { hunkSeparators: "metadata" },
    { hunkSeparators: { fn: "a" } },
    { hunkSeparators: { fn: "a" } },
    { hunkSeparators: { fn: "b" } },
    { hunkSeparators: "line-info" },
    { loadDiffFiles: true },
    { expandUnchanged: true },
    { collapsedContextThreshold: 5 },
    { collapsedContextThreshold: 1 },
    { unsafeCSS: "" },
    { unsafeCSS: "a{b:c}" },
    { themeType: "dark" },
    { themeType: "light", disableBackground: true },
    {},
  ];
  for (const diff of ["twoHunks", "partial0", "noeofBoth"]) {
    const steps = [{ op: "layoutDiff", diff }, { op: "compute", force: true, diff: null }];
    for (const spec of flips) {
      steps.push({ op: "renderRange", set: true });
      steps.push({ op: "deltas", entries: [[0, 7]] });
      steps.push({ op: "checkpoints", diff });
      steps.push({ op: "options", spec });
    }
    scenarios.push({ name: `edge:flips:${diff}`, metrics: METRICS.upstreamTest, options: {}, steps });
  }
  // Expansion of every hunk, including the trailing pseudo-hunk.
  for (const diff of ["twoHunks", "multi3", "gaps1", "thresholds", "long"]) {
    const count = diffsOf(diff, diffs).hunks.length;
    const steps = [{ op: "layoutDiff", diff }, { op: "compute", force: true, diff: null }];
    for (let hunkIndex = 0; hunkIndex <= count; hunkIndex++) {
      for (const direction of ["up", "down", "both"]) {
        steps.push({ op: "expand", hunkIndex, direction, count: 5 });
        steps.push({ op: "checkpoints", diff });
        steps.push({ op: "layoutLineCount", diff, style: hunkIndex % 2 === 0 ? "split" : "unified" });
      }
    }
    steps.push({ op: "expand", hunkIndex: 0, direction: "both", count: null });
    steps.push({ op: "expandedLineCount", diff, style: "split" });
    steps.push({ op: "expandedLineCount", diff, style: "unified" });
    scenarios.push({ name: `edge:expand:${diff}`, metrics: METRICS.upstreamTest, options: {}, steps });
  }
  return scenarios;
}

function diffsOf(name, diffs) {
  return diffs[name];
}

export async function buildS12bLayout(oracle, random, corpusMocks) {
  const { VirtualizedFileDiff } = await import(oracle.pierre("components/VirtualizedFileDiff.js"));
  const diffs = await buildDiffPool(oracle, random, corpusMocks);
  const scenarios = [...handwritten(diffs)];
  for (let index = 0; index < 260; index++) {
    scenarios.push(randomScenario(random, diffs, index));
  }
  const cases = scenarios.map((scenario) => ({ scenario, ...runScenario(VirtualizedFileDiff, diffs, scenario) }));
  return { diffs, cases };
}
