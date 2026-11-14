// Differential cases for the options seam of FileDiff (S12a-1).
//
// The published `FileDiff` class is constructed without a DOM (its option
// handling touches none) and asked what it derives from options: the renderer
// option snapshot, the interaction wiring (`enableGutterUtility`,
// `usesCustomGutterUtility`, whether `handleExpandHunk` is attached), the
// theme, `canPartiallyRender`, `shouldApplyColumnVariables`, and
// `getLineIndexForDiff` over parsed diffs. `areOptionsEqual` runs from the
// dist util over random option pairs. Callbacks are named in the spec; the same
// name in the same field is the same function in JavaScript and the same `Rc`
// in Rust, so identity comparison is exercised.

import { clone } from "./s11a_cases.mjs";

const PLAIN_FIELDS = [
  "themeType",
  "disableLineNumbers",
  "overflow",
  "collapsed",
  "disableFileHeader",
  "disableVirtualizationBuffers",
  "stickyHeader",
  "preferredHighlighter",
  "useCSSClasses",
  "useTokenTransformer",
  "tokenizeMaxLineLength",
  "tokenizeMaxLength",
  "unsafeCSS",
  "diffStyle",
  "diffIndicators",
  "disableBackground",
  "expandUnchanged",
  "collapsedContextThreshold",
  "lineDiffType",
  "maxLineDiffLength",
  "expansionLineCount",
  "lineHoverHighlight",
  "enableGutterUtility",
  "enableLineSelection",
  "controlledSelection",
  "disableErrorHandling",
];

const CALLBACK_FIELDS = [
  "onGutterUtilityClick",
  "onLineClick",
  "onLineNumberClick",
  "onLineEnter",
  "onLineLeave",
  "onLineSelected",
  "onLineSelectionStart",
  "onLineSelectionChange",
  "onLineSelectionEnd",
  "getLineIndex",
  "renderHeaderPrefix",
  "renderHeaderFilenameSuffix",
  "renderHeaderMetadata",
  "renderCustomHeader",
  "renderAnnotation",
  "renderGutterUtility",
  "onPostRender",
];

const VALUES = {
  themeType: ["system", "light", "dark"],
  disableLineNumbers: [true, false],
  overflow: ["scroll", "wrap"],
  collapsed: [true, false],
  disableFileHeader: [true, false],
  disableVirtualizationBuffers: [true, false],
  stickyHeader: [true, false],
  preferredHighlighter: ["shiki-js", "shiki-wasm", "highlights"],
  useCSSClasses: [true, false],
  useTokenTransformer: [true, false],
  tokenizeMaxLineLength: [100, 1000],
  tokenizeMaxLength: [5000, 100000],
  unsafeCSS: ["a{b:c}", ""],
  diffStyle: ["split", "unified"],
  diffIndicators: ["bars", "classic", "none"],
  disableBackground: [true, false],
  expandUnchanged: [true, false],
  collapsedContextThreshold: [1, 2, 5],
  lineDiffType: ["word-alt", "word-line", "word", "char", "none"],
  maxLineDiffLength: [100, 1000],
  expansionLineCount: [10, 100],
  lineHoverHighlight: ["disabled", "both", "number", "line"],
  enableGutterUtility: [true, false],
  enableLineSelection: [true, false],
  controlledSelection: [true, false],
  disableErrorHandling: [true, false],
};

function randomSpec(random) {
  const spec = {};
  const chance = random.next() < 0.5 ? 0.25 : 0.6;
  for (const field of PLAIN_FIELDS) {
    if (random.next() < chance) spec[field] = random.pick(VALUES[field]);
  }
  for (const field of CALLBACK_FIELDS) {
    if (random.next() < chance * 0.5) spec[field] = random.pick(["a", "b"]);
  }
  const theme = random.pick([
    undefined,
    "pierre-dark",
    "pierre-light",
    { dark: "pierre-dark", light: "pierre-light" },
    { dark: "pierre-light", light: "pierre-dark" },
  ]);
  if (theme !== undefined) spec.theme = theme;
  const separators = random.pick([
    undefined,
    "simple",
    "metadata",
    "line-info",
    "line-info-basic",
    { fn: "a" },
    { fn: "b" },
  ]);
  if (separators !== undefined) spec.hunkSeparators = separators;
  if (random.next() < chance * 0.5) {
    const parse = {};
    if (random.next() < 0.5) parse.disableLineMatching = random.pick([true, false]);
    if (random.next() < 0.5) parse.context = random.pick([1, 3]);
    if (random.next() < 0.5) parse.ignoreWhitespace = true;
    spec.parseDiffOptions = parse;
  }
  if (random.next() < chance * 0.3) spec.loadDiffFiles = true;
  return spec;
}

/** One-field mutation of a copy, or an unrelated spec. */
function mutateSpec(random, spec) {
  const copy = clone(spec);
  const roll = random.next();
  if (roll < 0.25) return copy;
  if (roll < 0.35) return randomSpec(random);
  const fresh = randomSpec(random);
  const keys = [...new Set([...Object.keys(fresh), ...Object.keys(copy)])];
  const key = random.pick(keys);
  if (fresh[key] === undefined) delete copy[key];
  else copy[key] = fresh[key];
  return copy;
}

export function buildOptions(spec, functions) {
  const fn = (field, name) => {
    const key = `${field}:${name}`;
    if (!functions.has(key)) functions.set(key, () => undefined);
    return functions.get(key);
  };
  const options = {};
  for (const field of PLAIN_FIELDS) if (spec[field] !== undefined) options[field] = spec[field];
  for (const field of CALLBACK_FIELDS) if (spec[field] !== undefined) options[field] = fn(field, spec[field]);
  if (spec.theme !== undefined) options.theme = clone(spec.theme);
  if (spec.hunkSeparators !== undefined) {
    options.hunkSeparators =
      typeof spec.hunkSeparators === "string"
        ? spec.hunkSeparators
        : fn("hunkSeparators", spec.hunkSeparators.fn);
  }
  if (spec.parseDiffOptions !== undefined) options.parseDiffOptions = clone(spec.parseDiffOptions);
  if (spec.loadDiffFiles) options.loadDiffFiles = fn("loadDiffFiles", "a");
  return options;
}

function jsonable(value) {
  // Functions become `true`, `undefined` entries drop out.
  return JSON.parse(
    JSON.stringify(value, (_, entry) => (typeof entry === "function" ? true : entry)),
  );
}

export async function buildS12aOptions(oracle, random, diffs) {
  const { FileDiff } = await import(oracle.pierre("components/FileDiff.js"));
  const { areOptionsEqual } = await import(oracle.pierre("utils/areOptionsEqual.js"));
  const functions = new Map();

  const derived = [];
  for (let index = 0; index < 160; index++) {
    const spec = index === 0 ? {} : randomSpec(random);
    const options = buildOptions(spec, functions);
    let result;
    try {
      const instance = new FileDiff(options);
      const interaction = instance.interactionManager.options;
      result = {
        rendererOptions: jsonable(instance.getHunksRendererOptions(options)),
        theme: jsonable(instance.getTheme()),
        enableGutterUtility: interaction.enableGutterUtility,
        usesCustomGutterUtility: interaction.usesCustomGutterUtility,
        hasOnHunkExpand: interaction.onHunkExpand != null,
        canPartiallyRender: [false, true].flatMap((force) =>
          [false, true].flatMap((annotations) =>
            [false, true].map((content) =>
              instance.canPartiallyRender(force, annotations, content),
            ),
          ),
        ),
        shouldApplyColumnVariables: ["scroll", "wrap"].map((overflow) =>
          instance.shouldApplyColumnVariables(overflow),
        ),
      };
    } catch (error) {
      result = { throws: error.message };
    }
    derived.push({ spec, result });
  }
  // Only the default-options constructor: `new FileDiff()`.
  const defaulted = new FileDiff();
  const defaultTheme = jsonable(defaulted.getTheme());

  const equality = [];
  for (let index = 0; index < 400; index++) {
    const left = random.next() < 0.05 ? undefined : randomSpec(random);
    const right = random.next() < 0.05 ? undefined : left === undefined ? randomSpec(random) : mutateSpec(random, left);
    const optionsLeft = left === undefined ? undefined : buildOptions(left, functions);
    const optionsRight = right === undefined ? undefined : buildOptions(right, functions);
    equality.push({
      left: left ?? null,
      right: right ?? null,
      expected: areOptionsEqual(optionsLeft, optionsRight),
    });
  }

  const instance = new FileDiff();
  const lineIndex = diffs.map((fileDiff) => {
    const lastLine = Math.max(
      fileDiff.additionLines.length,
      fileDiff.deletionLines.length,
    );
    const rows = [];
    for (const side of ["additions", "deletions"]) {
      for (let lineNumber = 0; lineNumber <= lastLine + 6; lineNumber++) {
        rows.push([
          side,
          lineNumber,
          instance.getLineIndexForDiff(fileDiff, lineNumber, side) ?? null,
        ]);
      }
    }
    return { rows };
  });
  const noDiff = instance.getLineIndexForDiff(undefined, 1, "additions") ?? null;

  return { derived, defaultTheme, equality, diffs, lineIndex, noDiff };
}
