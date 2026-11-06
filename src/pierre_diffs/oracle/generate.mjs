// Differential oracle generator for the pierre_diffs port.
//
// Runs the published `diff@9.0.0` and `@pierre/diffs@1.6.4` dist (pinned by
// sha256 in pins.json) over a corpus plus seeded fuzz inputs and writes JSON
// goldens next to this file. The Rust tests read the goldens and compare
// structurally; they never run JavaScript.
//
//   node oracle/generate.mjs           write goldens
//   node oracle/generate.mjs --check   fail if the checked-in goldens differ
//
// Inputs never contain lone surrogates: Rust `String` cannot carry them.
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { ensureOracle, pins } from "./ensure.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const goldenDir = join(here, "golden");
const check = process.argv.includes("--check");

const oracle = await ensureOracle();
const jsdiff = await import(oracle.jsdiff);
const constants = await import(oracle.pierre("constants.js"));
const { parsePatchFiles } = await import(
  oracle.pierre("utils/parsePatchFiles.js")
);
const { parseDiffFromFile } = await import(
  oracle.pierre("utils/parseDiffFromFile.js")
);
const corpusMocks = await import("./corpus/mocks.mjs");

// Pierre logs recoverable parse problems with console.error; the goldens only
// need the return values.
console.error = () => {};

function mulberry32(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(state ^ (state >>> 15), state | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

function makeRandom(seed) {
  const next = mulberry32(seed);
  const int = (bound) => Math.floor(next() * bound);
  const pick = (items) => items[int(items.length)];
  return { next, int, pick };
}

/** Random token sequence mutated into a second, related sequence. */
function mutate(random, tokens, alphabet) {
  const out = tokens.slice();
  const edits = random.int(4);
  for (let edit = 0; edit < edits; edit++) {
    const at = random.int(out.length + 1);
    switch (random.int(4)) {
      case 0:
        out.splice(at, 1);
        break;
      case 1:
        out.splice(at, 0, random.pick(alphabet));
        break;
      case 2:
        out[at] = random.pick(alphabet);
        break;
      default: {
        const other = random.int(out.length + 1);
        [out[at], out[other]] = [out[other], out[at]];
      }
    }
  }
  return out.filter((token) => token !== undefined);
}

function tokenPair(random, alphabet, maxLength) {
  const length = random.int(maxLength + 1);
  const base = Array.from({ length }, () => random.pick(alphabet));
  const changed =
    random.next() < 0.1
      ? Array.from({ length: random.int(maxLength + 1) }, () =>
          random.pick(alphabet),
        )
      : mutate(random, base, alphabet);
  return [base.join(""), changed.join("")];
}

const CHAR_ALPHABET = [
  "a",
  "b",
  "c",
  "A",
  " ",
  "é",
  "😀",
  " ",
  "\n",
  "\r",
  "\t",
  "Σ",
  "ß",
];
const WORD_ALPHABET = [
  "\u0085",
  "\u180e",
  "\u200a",
  "foo",
  "bar",
  "baz",
  "Foo",
  " ",
  "  ",
  "\t",
  "\n",
  "\r\n",
  "\r",
  ",",
  ".",
  "é",
  "_",
  "x1",
  " ",
  " ",
  "　",
  "😀",
  "­",
  "×",
  "﻿",
  "Ā",
  "(",
  ")",
  "̀",
];
const LINE_ALPHABET = [
  "a",
  "b",
  "c",
  "d",
  "",
  " a",
  "a ",
  "  ",
  "\ta",
  "A",
  "😀 x",
  "foo bar",
  "}",
  "\r",
];

function lineText(random, lines) {
  const separator = random.next() < 0.2 ? "\r\n" : "\n";
  let text = lines.join(separator);
  if (lines.length > 0 && random.next() < 0.7) {
    text += random.next() < 0.1 ? "\r\n" : "\n";
  }
  return text;
}

function linePair(random) {
  const length = random.int(9);
  const base = Array.from({ length }, () => random.pick(LINE_ALPHABET));
  const changed = mutate(random, base, LINE_ALPHABET);
  return [lineText(random, base), lineText(random, changed)];
}

const LINE_OPTIONS = [
  {},
  { ignoreWhitespace: true },
  { newlineIsToken: true },
  { stripTrailingCr: true },
  { ignoreNewlineAtEof: true },
  { oneChangePerToken: true },
  { maxEditLength: 3 },
  { ignoreCase: true },
  { ignoreWhitespace: true, newlineIsToken: true },
  { stripTrailingCr: true, ignoreNewlineAtEof: true },
];
const CHAR_OPTIONS = [
  {},
  {},
  { ignoreCase: true },
  { oneChangePerToken: true },
  { maxEditLength: 2 },
];
const WORD_OPTIONS = [
  {},
  {},
  { ignoreCase: true },
  { oneChangePerToken: true },
  { maxEditLength: 2 },
];
const CONTEXT_OPTIONS = [
  {},
  {},
  { context: 0 },
  { context: 1 },
  { context: 3 },
  { context: 4 },
  { ignoreWhitespace: true },
  { stripTrailingCr: true },
];
const HEADER_OPTIONS = [
  undefined,
  undefined,
  { includeIndex: false, includeUnderline: false, includeFileHeaders: true },
  { includeIndex: false, includeUnderline: false, includeFileHeaders: false },
];
const FILE_NAMES = [
  "a.ts",
  "dir/b c.txt",
  "/dev/null",
  'weird "é".md',
  "tab\tname",
  "😀.rs",
  "back\\slash",
];

function jsonClone(value) {
  return value === undefined ? null : JSON.parse(JSON.stringify(value));
}

function run(fn, args) {
  return jsonClone(jsdiff[fn](...args));
}

function buildJsdiff() {
  const random = makeRandom(0x5eed01);
  const cases = [];
  const push = (fn, oldText, newText, options) => {
    // Clone: patch generation writes defaults (`context: 4`) into the options it is handed.
    cases.push({
      fn,
      old: oldText,
      new: newText,
      options: jsonClone(options),
      expected: run(fn, [oldText, newText, jsonClone(options)]),
    });
  };

  const corpusLines = Object.values(corpusMocks.mockFiles)
    .flatMap((file) => file.contents.split("\n"))
    .filter((line) => line.length > 0);
  for (let index = 0; index + 1 < corpusLines.length; index++) {
    push("diffWordsWithSpace", corpusLines[index], corpusLines[index + 1], {});
    push("diffChars", corpusLines[index], corpusLines[index + 1], {});
    push("diffWords", corpusLines[index], corpusLines[index + 1], {});
  }
  const mockPairs = Object.values(corpusMocks.mockDiffs).map((diff) => [
    diff.oldFile.contents,
    diff.newFile.contents,
  ]);
  const files = Object.values(corpusMocks.mockFiles).map(
    (file) => file.contents,
  );
  mockPairs.push(
    [files[0], files[1]],
    [
      readFileSync(join(here, "corpus/trim.patch"), "utf8"),
      readFileSync(join(here, "corpus/file.patch"), "utf8"),
    ],
  );
  // Large pairs get a reduced option matrix to keep the checked-in goldens small.
  const isLarge = (oldText, newText) => oldText.length + newText.length > 3000;
  for (const [oldText, newText] of mockPairs) {
    for (const options of isLarge(oldText, newText)
      ? LINE_OPTIONS.slice(0, 2)
      : LINE_OPTIONS) {
      push("diffLines", oldText, newText, options);
    }
    push("diffTrimmedLines", oldText, newText, {});
  }

  for (let count = 0; count < 150; count++) {
    const [oldText, newText] = tokenPair(random, CHAR_ALPHABET, 10);
    push("diffChars", oldText, newText, random.pick(CHAR_OPTIONS));
  }
  for (let count = 0; count < 200; count++) {
    const [oldText, newText] = tokenPair(random, WORD_ALPHABET, 9);
    push("diffWordsWithSpace", oldText, newText, random.pick(WORD_OPTIONS));
  }
  for (let count = 0; count < 200; count++) {
    const [oldText, newText] = tokenPair(random, WORD_ALPHABET, 9);
    const options = random.pick([...WORD_OPTIONS, { ignoreWhitespace: false }]);
    push("diffWords", oldText, newText, options);
  }
  for (let count = 0; count < 150; count++) {
    const [oldText, newText] = linePair(random);
    push(
      random.next() < 0.1 ? "diffTrimmedLines" : "diffLines",
      oldText,
      newText,
      random.pick(LINE_OPTIONS),
    );
  }

  const patches = [];
  const pushPatch = (oldText, newText, options, headerOptions) => {
    const oldName = random.pick(FILE_NAMES);
    const newName = random.next() < 0.5 ? oldName : random.pick(FILE_NAMES);
    const oldHeader = random.next() < 0.3 ? "2020-01-01" : undefined;
    const newHeader = random.next() < 0.3 ? "" : undefined;
    const fullOptions =
      headerOptions === undefined ? options : { ...options, headerOptions };
    const patchCase = {
      oldName,
      newName,
      old: oldText,
      new: newText,
      oldHeader: oldHeader ?? null,
      newHeader: newHeader ?? null,
      options: jsonClone(fullOptions),
    };
    patchCase.expected = jsonClone(
      jsdiff.createTwoFilesPatch(
        oldName,
        newName,
        oldText,
        newText,
        oldHeader,
        newHeader,
        jsonClone(fullOptions),
      ),
    );
    patchCase.structured = jsonClone(
      jsdiff.structuredPatch(
        oldName,
        newName,
        oldText,
        newText,
        oldHeader,
        newHeader,
        jsonClone(options),
      ),
    );
    patches.push(patchCase);
  };
  for (const [oldText, newText] of mockPairs) {
    for (const options of isLarge(oldText, newText)
      ? CONTEXT_OPTIONS.slice(0, 3)
      : CONTEXT_OPTIONS) {
      pushPatch(oldText, newText, options, random.pick(HEADER_OPTIONS));
    }
  }
  for (let count = 0; count < 150; count++) {
    const [oldText, newText] = linePair(random);
    pushPatch(
      oldText,
      newText,
      random.pick(CONTEXT_OPTIONS),
      random.pick(HEADER_OPTIONS),
    );
  }
  // Larger line counts exercise the longer edit-length and hunk-merging paths.
  for (let count = 0; count < 6; count++) {
    const base = Array.from(
      { length: 120 },
      (_, index) => `line ${index} ${random.pick(["x", "y", "z"])}`,
    );
    const changed = mutate(
      random,
      mutate(random, mutate(random, base, LINE_ALPHABET), LINE_ALPHABET),
      LINE_ALPHABET,
    );
    pushPatch(
      lineText(random, base),
      lineText(random, changed),
      random.pick(CONTEXT_OPTIONS),
      undefined,
    );
  }
  return {
    pins: { diff: pins.diff.version, sha256: pins.diff.sha256 },
    cases,
    patches,
  };
}

const SCANNER_FRAGMENTS = [
  "diff --git a/x b/x",
  "From abc123 x",
  "From deadbeef ",
  "From abc ",
  "From G x",
  "---",
  "--- ",
  "--- a",
  "+++ b",
  "+++ ",
  "@@ -1,2 +3,4 @@ ctx",
  "@@ -1 +1 @@",
  "@@ ",
  "@@ -1,2 +3,4 @@",
  "@@ -1,2 +3,4 @@x",
  "@@ -",
  "index 1a2b..3c4d 100644",
  "INDEX AB..cd",
  "index ab..cd 12x",
  "\n",
  "\n",
  "\r",
  "\r\n",
  " ",
  " ",
  "\u0085",
  " ",
  "  ",
  "\t",
  "a",
  "<<<<<<<",
  "<<<<<<",
  "=======",
  ">>>>>>>",
  "|||||||",
  "<",
  "=",
  "é",
  "😀",
  " ",
  "﻿",
  "　",
  "-",
  "..",
  "0",
  "9",
  ",",
  "+",
  "f",
  "F",
  "\u000b",
  "\u000c",
  "᠎",
  "​",
];
const SCANNER_REGEX_EXPORTS = {
  commitMetadataSplit: "COMMIT_METADATA_SPLIT",
  gitDiffFileBreak: "GIT_DIFF_FILE_BREAK_REGEX",
  unifiedDiffFileBreak: "UNIFIED_DIFF_FILE_BREAK_REGEX",
  fileContextBlob: "FILE_CONTEXT_BLOB",
};

function fresh(regex) {
  // The exported `g`-flag regexes are stateful under `.test`; clone to start
  // from lastIndex 0, which is what a single call sees.
  return new RegExp(regex.source, regex.flags);
}

function matchRecord(match) {
  if (match === null) {
    return null;
  }
  return {
    index: match.index,
    length: match[0].length,
    groups: match.slice(1).map((group) => group ?? null),
  };
}

function scannerRecord(text) {
  const splits = {};
  for (const [key, exportName] of Object.entries(SCANNER_REGEX_EXPORTS)) {
    splits[key] = text.split(fresh(constants[exportName]));
  }
  return {
    text,
    splits,
    splitWithNewlines: text.split(fresh(constants.SPLIT_WITH_NEWLINES)),
    gitDiffFileBreakTest: fresh(constants.GIT_DIFF_FILE_BREAK_REGEX).test(text),
    hunkHeader: matchRecord(text.match(fresh(constants.HUNK_HEADER))),
    filenameHeader: matchRecord(
      text.match(fresh(constants.FILENAME_HEADER_REGEX)),
    ),
    indexLine: matchRecord(text.match(fresh(constants.INDEX_LINE_METADATA))),
    mergeConflict: {
      start: fresh(constants.MERGE_CONFLICT_START_MARKER_REGEX).test(text),
      base: fresh(constants.MERGE_CONFLICT_BASE_MARKER_REGEX).test(text),
      separator: fresh(constants.MERGE_CONFLICT_SEPARATOR_MARKER_REGEX).test(
        text,
      ),
      end: fresh(constants.MERGE_CONFLICT_END_MARKER_REGEX).test(text),
    },
  };
}

function buildScanners() {
  const random = makeRandom(0x5eed02);
  const texts = new Set([
    "",
    "\n",
    "\n\n",
    "a\n",
    "a\nb",
    "diff --git a b\n",
    "\ndiff --git a b",
    "---\n\nfoo",
    "--- ",
    "---   ",
    "---  \tfoo",
    "<<<<<<<\n",
    "<<<<<<<\n\nfoo",
    "@@ -1,2 +3,4 @@",
  ]);
  for (const text of [
    readFileSync(join(here, "corpus/file.patch"), "utf8"),
    readFileSync(join(here, "corpus/trim.patch"), "utf8"),
    corpusMocks.formatPatchWithVersionTrailer,
    corpusMocks.malformedPatch,
    corpusMocks.finalBlankLinePatch,
  ]) {
    texts.add(text);
  }
  while (texts.size < 800) {
    const length = 1 + random.int(8);
    texts.add(
      Array.from({ length }, () => random.pick(SCANNER_FRAGMENTS)).join(""),
    );
  }
  const cases = [...texts].map(scannerRecord);
  return {
    pins: {
      pierre: pins["@pierre/diffs"].version,
      sha256: pins["@pierre/diffs"].sha256,
    },
    cases,
  };
}

function buildTypes() {
  const patches = {
    "file.patch": readFileSync(join(here, "corpus/file.patch"), "utf8"),
    "trim.patch": readFileSync(join(here, "corpus/trim.patch"), "utf8"),
    formatPatchWithVersionTrailer: corpusMocks.formatPatchWithVersionTrailer,
    malformedPatch: corpusMocks.malformedPatch,
    finalBlankLinePatch: corpusMocks.finalBlankLinePatch,
  };
  const parsed = Object.entries(patches).map(([name, patch]) => ({
    name,
    patch,
    options: { cacheKeyPrefix: "ck" },
    expected: jsonClone(parsePatchFiles(patch, { cacheKeyPrefix: "ck" })),
  }));
  const fromFiles = Object.values(corpusMocks.mockDiffs).map((diff) => ({
    oldFile: { ...diff.oldFile, cacheKey: "old" },
    newFile: { ...diff.newFile, cacheKey: "new" },
  }));
  const files = Object.values(corpusMocks.mockFiles);
  fromFiles.push({ oldFile: files[0], newFile: files[1] });
  const diffs = fromFiles.map((input) => ({
    ...input,
    expected: jsonClone(parseDiffFromFile(input.oldFile, input.newFile)),
  }));
  return {
    pins: {
      pierre: pins["@pierre/diffs"].version,
      sha256: pins["@pierre/diffs"].sha256,
    },
    parsed,
    diffs,
  };
}

const outputs = {
  "jsdiff.json": buildJsdiff(),
  "scanners.json": buildScanners(),
  "types.json": buildTypes(),
};

let drift = false;
for (const [name, value] of Object.entries(outputs)) {
  const text = `${JSON.stringify(value)}\n`;
  const path = join(goldenDir, name);
  if (check) {
    let current = "";
    try {
      current = readFileSync(path, "utf8");
    } catch {
      // missing golden counts as drift
    }
    if (current !== text) {
      console.error(`golden drift: ${name}`);
      drift = true;
    }
  } else {
    writeFileSync(path, text);
    console.log(`${name}: ${text.length} bytes`);
  }
}
if (drift) {
  process.exit(1);
}
