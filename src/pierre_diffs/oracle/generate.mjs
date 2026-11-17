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
import { crc32, deflateRawSync } from "node:zlib";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { ensureOracle, pins } from "./ensure.mjs";
import { buildVirtualizer } from "./virtualizer_scenarios.mjs";
import { buildS11aUtils } from "./s11a_cases.mjs";
import { buildS11aManagers } from "./s11a_managers.mjs";
import { buildS11bInteraction } from "./s11b_interaction.mjs";
import { buildS12aOptions } from "./s12a_file_diff.mjs";
import { buildS12aLifecycle } from "./s12a_lifecycle.mjs";
import { buildS12bRender } from "./s12b_render.mjs";
import { buildS12bVirtualized } from "./s12b_virtualized.mjs";
import { buildS12bLayout } from "./s12b_layout.mjs";
import { buildS12bWindow } from "./s12b_window.mjs";
import { stubHighlighter } from "./stub_highlighter.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const goldenDir = join(here, "golden");
const check = process.argv.includes("--check");

const oracle = ensureOracle();
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
      ...(fn === "diffLines"
        ? { expected: run(fn, [oldText, newText, jsonClone(options)]) }
        : {}),
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

  for (let count = 0; count < 60; count++) {
    const [oldText, newText] = tokenPair(random, CHAR_ALPHABET, 10);
    push("diffChars", oldText, newText, random.pick(CHAR_OPTIONS));
  }
  for (let count = 0; count < 70; count++) {
    const [oldText, newText] = tokenPair(random, WORD_ALPHABET, 9);
    push("diffWordsWithSpace", oldText, newText, random.pick(WORD_OPTIONS));
  }
  for (let count = 0; count < 200; count++) {
    const [oldText, newText] = tokenPair(random, WORD_ALPHABET, 9);
    const options = random.pick([...WORD_OPTIONS, { ignoreWhitespace: false }]);
    push("diffWords", oldText, newText, options);
  }
  for (let count = 0; count < 45; count++) {
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
    patches.push(patchCase);
  };
  for (const [oldText, newText] of mockPairs) {
    for (const options of isLarge(oldText, newText)
      ? CONTEXT_OPTIONS.slice(0, 3)
      : CONTEXT_OPTIONS) {
      pushPatch(oldText, newText, options, random.pick(HEADER_OPTIONS));
    }
  }
  for (let count = 0; count < 35; count++) {
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
  while (texts.size < 300) {
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

// ---- S3: patch parsing -------------------------------------------------

const { parseQuotedDiffFileName } = await import(
  oracle.pierre("utils/parseQuotedDiffFileName.js")
);
const { getFiletypeFromFileName, EXTENSION_TO_FILE_FORMAT } = await import(
  oracle.pierre("utils/getFiletypeFromFileName.js")
);
const { cleanLastNewline } = await import(
  oracle.pierre("utils/cleanLastNewline.js")
);
const { getLineEndingType } = await import(
  oracle.pierre("utils/getLineEndingType.js")
);
const { composeCacheKey } = await import(
  oracle.pierre("utils/composeCacheKey.js")
);

/** Runs `fn`, counting console.error calls (Pierre's recoverable-problem channel). */
function withErrorCount(fn) {
  const original = console.error;
  let count = 0;
  console.error = () => {
    count += 1;
  };
  try {
    return { value: fn(), errors: count };
  } catch (error) {
    return { thrown: String(error?.message ?? error), errors: count };
  } finally {
    console.error = original;
  }
}

const PATCH_NAMES = [
  "a.ts",
  "dir/b c.txt",
  "sp ace/x y.md",
  '"q.txt"',
  "é.rs",
  "new.txt",
  "x",
];
const QUOTED_NAMES = [
  '"a/q\\"x.txt"',
  '"b/\\303\\251.txt"',
  '"a/tab\\tname"',
  '"b/bad\\q"',
  '"a/unterminated',
  '"a/\\"x\\" y"',
];

function fuzzPatch(random) {
  const eol = random.next() < 0.15 ? "\r\n" : "\n";
  const lines = [];
  if (random.next() < 0.2) {
    lines.push(
      `From ${random.pick(["abc123", "deadbeef01", "0f"])} Mon Sep 17 00:00:00 2001`,
      "Subject: [PATCH] x",
      "",
    );
  }
  const files = 1 + random.int(3);
  const git = random.next() < 0.7;
  for (let file = 0; file < files; file++) {
    const name = random.pick(PATCH_NAMES);
    const other = random.next() < 0.25 ? random.pick(PATCH_NAMES) : name;
    if (git) {
      const quoted = random.next() < 0.2;
      lines.push(
        quoted
          ? `diff --git ${random.pick(QUOTED_NAMES)} ${random.pick(QUOTED_NAMES)}`
          : `diff --git a/${name} b/${other}`,
      );
      const kind = random.int(6);
      if (kind === 0) lines.push("new file mode 100644");
      if (kind === 1) lines.push("deleted file mode 100644");
      if (kind === 2) lines.push("old mode 100644", "new mode 100755");
      if (kind === 3)
        lines.push(
          "similarity index 100%",
          `rename from ${name}`,
          `rename to ${other}`,
        );
      if (kind === 4)
        lines.push(
          "similarity index 80%",
          `rename from ${name}`,
          `rename to ${other}`,
        );
      if (random.next() < 0.7)
        lines.push(
          `index ${random.pick(["1a2b3c4", "ABC123"])}..${random.pick(["5d6e7f8", "0000000"])}${random.next() < 0.5 ? " 100644" : ""}`,
        );
      if (kind === 3) continue;
    }
    lines.push(
      `--- ${random.next() < 0.1 ? "/dev/null" : (git ? "a/" : "") + name}${random.next() < 0.2 ? "\t2020-01-01" : ""}`,
    );
    lines.push(`+++ ${git ? "b/" : ""}${other}`);
    const hunks = 1 + random.int(3);
    let oldLine = 1 + random.int(5);
    for (let hunk = 0; hunk < hunks; hunk++) {
      const body = [];
      const bodyLength = 1 + random.int(8);
      let oldCount = 0;
      let newCount = 0;
      for (let line = 0; line < bodyLength; line++) {
        const kind = random.pick([" ", " ", "-", "+", "-", "+"]);
        body.push(
          kind +
            random.pick([
              "x",
              "foo bar",
              "  indented",
              "}",
              "",
              "é",
              "const a = 1;",
            ]),
        );
        if (kind !== "+") oldCount += 1;
        if (kind !== "-") newCount += 1;
        if (random.next() < 0.05) body.push("\\ No newline at end of file");
      }
      const skew = random.next() < 0.12 ? 1 : 0;
      const formatCount = (value) => (value === 1 ? "" : `,${value}`);
      lines.push(
        `@@ -${oldLine}${formatCount(oldCount + skew)} +${oldLine}${formatCount(newCount)} @@${random.next() < 0.3 ? " fn main()" : ""}`,
      );
      lines.push(...body);
      oldLine += oldCount + 1 + random.int(10);
    }
  }
  if (random.next() < 0.1) lines.push("-- ", "2.43.0", "");
  let text = lines.join(eol);
  if (random.next() < 0.85) text += eol;
  return text;
}

function buildPatches() {
  const random = makeRandom(0x5eed03);
  const cases = [];
  const optionSets = [
    {},
    {},
    { disableLineMatching: true },
    { cacheKeyPrefix: "ck" },
    { throwOnError: true },
  ];
  const push = (patch, options) => {
    const result = withErrorCount(() => parsePatchFiles(patch, options));
    cases.push({
      patch,
      options,
      expected: result.value === undefined ? null : jsonClone(result.value),
      error: result.thrown ?? null,
      errors: result.errors,
    });
  };
  const corpus = [
    readFileSync(join(here, "corpus/file.patch"), "utf8"),
    readFileSync(join(here, "corpus/trim.patch"), "utf8"),
    corpusMocks.formatPatchWithVersionTrailer,
    corpusMocks.malformedPatch,
    corpusMocks.finalBlankLinePatch,
  ];
  for (const patch of corpus) {
    for (const options of optionSets) push(patch, options);
  }
  for (let count = 0; count < 50; count++) {
    push(fuzzPatch(random), random.pick(optionSets));
  }
  return {
    pins: {
      pierre: pins["@pierre/diffs"].version,
      sha256: pins["@pierre/diffs"].sha256,
    },
    cases,
    fromFiles: buildFromFiles(),
  };
}

/**
 * parseDiffFromFile over pairs where a side is empty or absent: upstream's
 * hunk line indexes are `start - 1`, so they are -1 for a side that starts at
 * line 0, and the port must produce the same values.
 */
function buildFromFiles() {
  const file = (contents) => ({ name: "a.ts", contents });
  const texts = ["", "x\n", "x\ny\nz\n", "no newline"];
  const cases = [];
  for (const oldText of texts) {
    for (const newText of texts) {
      cases.push({
        kind: "both",
        oldFile: file(oldText),
        newFile: file(newText),
        expected: jsonClone(parseDiffFromFile(file(oldText), file(newText))),
      });
    }
  }
  for (const text of texts) {
    cases.push({
      kind: "added",
      newFile: file(text),
      expected: jsonClone(parseDiffFromFile(null, file(text))),
    });
    cases.push({
      kind: "deleted",
      oldFile: file(text),
      expected: jsonClone(parseDiffFromFile(file(text), null)),
    });
  }
  return cases;
}

function buildUtilities() {
  const random = makeRandom(0x5eed04);
  const quotedFragments = [
    '"',
    '"',
    "\\",
    "\\\\",
    '\\"',
    "\\303",
    "\\251",
    "\\000",
    "\\377",
    "\\400",
    "\\n",
    "\\q",
    "\\4",
    "a",
    "b/",
    " ",
    "é",
    "😀",
  ];
  const quoted = [];
  for (let count = 0; count < 300; count++) {
    const text = Array.from({ length: 1 + random.int(8) }, () =>
      random.pick(quotedFragments),
    ).join("");
    quoted.push({
      input: text,
      expected: jsonClone(parseQuotedDiffFileName(text)),
    });
  }
  for (const name of QUOTED_NAMES)
    quoted.push({
      input: name,
      expected: jsonClone(parseQuotedDiffFileName(name)),
    });

  const names = new Set([
    "",
    "Dockerfile",
    "docker/Dockerfile",
    "docker/Dockerfile.ts",
    "a.b.c",
    "x/.y.z",
    "dir.d/file",
    "a\\b\\Makefile",
    "constructor",
    "foo.constructor",
    "end.",
    ".hidden",
    "a..b",
    "x.blade.php",
  ]);
  for (const key of Object.keys(EXTENSION_TO_FILE_FORMAT)) {
    names.add(key);
    names.add(`src/file.${key}`);
    names.add(`a.${key}.${key}`);
  }
  for (let count = 0; count < 200; count++) {
    names.add(
      Array.from({ length: 1 + random.int(5) }, () =>
        random.pick([
          "a",
          ".",
          "/",
          "\\",
          "ts",
          "py",
          "blade",
          "php",
          "component",
          "x",
        ]),
      ).join(""),
    );
  }
  const filetypes = [...names].map((name) => ({
    name,
    expected: getFiletypeFromFileName(name),
  }));

  const endings = [
    "",
    "a",
    "a\n",
    "a\r\n",
    "a\r",
    "\n\r",
    "a\r\n\r\n",
    "\r",
    "x\ny\r\n",
    "\n",
  ].map((text) => ({
    text,
    cleanLastNewline: cleanLastNewline(text),
    lineEnding: getLineEndingType(text),
  }));
  const cacheKeys = [
    ["diff", ["a", "b"]],
    ["s", ['q"uote', "back\\slash", "tab\t", "nl\n", "é😀", " ", "\u0001"]],
    ["only", []],
  ].map(([scope, segments]) => ({
    scope,
    segments,
    expected: composeCacheKey(scope, ...segments),
  }));
  return { quoted, filetypes, endings, cacheKeys };
}

// ---- S4: line iteration and layout math --------------------------------

const { iterateOverDiff } = await import(
  oracle.pierre("utils/iterateOverDiff.js")
);
const layout = await import(oracle.pierre("utils/virtualDiffLayout.js"));
const { computeEstimatedDiffHeights } = await import(
  oracle.pierre("utils/computeEstimatedDiffHeights.js")
);
const { createWindowFromScrollPosition } = await import(
  oracle.pierre("utils/createWindowFromScrollPosition.js")
);
const offsets = await import(oracle.pierre("utils/computeFileOffsets.js"));
const { trimPatchContext } = await import(
  oracle.pierre("utils/trimPatchContext.js")
);
const { hydratePartialDiff } = await import(
  oracle.pierre("utils/hydratePartialDiff.js")
);
const { computeVirtualFileMetrics } = await import(
  oracle.pierre("utils/computeVirtualFileMetrics.js")
);

/** `null | true | [[hunkIndex, {fromStart, fromEnd}]]` as the Rust tests read it. */
function encodeExpanded(expanded) {
  if (expanded === undefined) return null;
  if (expanded === true) return true;
  return [...expanded.entries()];
}

function randomExpanded(random, hunkCount) {
  const kind = random.int(4);
  if (kind === 0) return undefined;
  if (kind === 1) return true;
  const map = new Map();
  const entries = 1 + random.int(3);
  for (let entry = 0; entry < entries; entry++) {
    map.set(random.int(hunkCount + 1), {
      fromStart: random.int(4),
      fromEnd: random.int(4),
    });
  }
  return map;
}

/** Seeded old/new file pair with scattered edits, a trailing-context tail, and optional missing final newline. */
function syntheticPair(random, lineCount) {
  const old = Array.from(
    { length: lineCount },
    (_, index) => `line ${index + 1}`,
  );
  const changed = old.slice();
  const edits = 2 + random.int(5);
  for (let edit = 0; edit < edits; edit++) {
    const at = random.int(changed.length);
    switch (random.int(3)) {
      case 0:
        changed[at] = `edited ${at}`;
        break;
      case 1:
        changed.splice(at, 0, `inserted ${at}`, `inserted ${at}b`);
        break;
      default:
        changed.splice(at, 1 + random.int(2));
    }
  }
  const join = (lines, trailing) => lines.join("\n") + (trailing ? "\n" : "");
  const trailing = random.next() < 0.75;
  return [join(old, trailing), join(changed, trailing || random.next() < 0.3)];
}

function rowArray(props) {
  const line = (metadata) =>
    metadata === undefined
      ? null
      : [
          metadata.unifiedLineIndex,
          metadata.splitLineIndex,
          metadata.lineIndex,
          metadata.lineNumber,
          metadata.noEOFCR,
        ];
  return [
    props.type,
    props.hunkIndex,
    props.collapsedBefore,
    props.collapsedAfter,
    line(props.deletionLine),
    line(props.additionLine),
  ];
}

function buildS4() {
  const random = makeRandom(0x5eed05);
  const diffs = [];
  const pairs = [
    ...Object.values(corpusMocks.mockDiffs).map((diff) => [
      diff.oldFile.contents,
      diff.newFile.contents,
    ]),
    ...Array.from({ length: 5 }, (_, index) =>
      syntheticPair(random, [30, 45, 60, 90, 25][index]),
    ),
  ];
  for (const [oldText, newText] of pairs) {
    const diff = parseDiffFromFile(
      { name: "a.txt", contents: oldText },
      { name: "a.txt", contents: newText },
    );
    diffs.push(jsonClone(diff));
  }
  // A partial diff (parsed from patch text only) exercises the isPartial branches.
  for (const [oldText, newText] of pairs.slice(1, 4)) {
    const patch = jsdiff.createTwoFilesPatch(
      "p.txt",
      "p.txt",
      oldText,
      newText,
      undefined,
      undefined,
      {},
    );
    diffs.push(jsonClone(parsePatchFiles(patch)[0].files[0]));
  }

  const iterate = [];
  for (let diffIndex = 0; diffIndex < diffs.length; diffIndex++) {
    const diff = diffs[diffIndex];
    const cases = diff.hunks.length === 0 ? 0 : 16;
    for (let count = 0; count < cases; count++) {
      const diffStyle = random.pick(["unified", "split", "both"]);
      const windowed = random.next() < 0.7;
      const startingLine = windowed
        ? random.int(Math.max(diff.unifiedLineCount, 3))
        : 0;
      const totalLines =
        windowed && random.next() < 0.8 ? 1 + random.int(10) : undefined;
      const threshold = random.pick([undefined, 1, 3]);
      const expanded = randomExpanded(random, diff.hunks.length);
      const rows = [];
      iterateOverDiff({
        diff: structuredClone(diff),
        diffStyle,
        startingLine: startingLine === 0 ? undefined : startingLine,
        totalLines,
        expandedHunks: expanded,
        collapsedContextThreshold: threshold,
        callback: (props) => {
          rows.push(rowArray(props));
        },
      });
      iterate.push({
        diff: diffIndex,
        diffStyle,
        startingLine,
        totalLines: totalLines ?? null,
        threshold: threshold ?? null,
        expanded: encodeExpanded(expanded),
        rows,
      });
    }
  }

  const expandedRegions = [];
  for (let rangeSize = -1; rangeSize <= 12; rangeSize += 3) {
    for (const isPartial of [false, true]) {
      for (const threshold of [1, 3]) {
        for (const expanded of [
          undefined,
          true,
          new Map([[2, { fromStart: 2, fromEnd: 1 }]]),
          new Map([[2, { fromStart: 9, fromEnd: 9 }]]),
          new Map([[2, { fromStart: 0, fromEnd: 0 }]]),
        ]) {
          expandedRegions.push({
            isPartial,
            rangeSize,
            threshold,
            expanded: encodeExpanded(expanded),
            expected: layout.getExpandedRegion({
              isPartial,
              rangeSize,
              expandedHunks: expanded,
              hunkIndex: 2,
              collapsedContextThreshold: threshold,
            }),
          });
        }
      }
    }
  }

  const renderable = [];
  for (let diffIndex = 0; diffIndex < diffs.length; diffIndex++) {
    const diff = diffs[diffIndex];
    for (const threshold of [1, 3]) {
      for (let variant = 0; variant < 3; variant++) {
        const expanded =
          variant === 0 ? undefined : randomExpanded(random, diff.hunks.length);
        const lines = [];
        const downs = [];
        const ups = [];
        for (
          let lineNumber = 0;
          lineNumber <= diff.additionLines.length + 3;
          lineNumber++
        ) {
          const args = {
            fileDiff: structuredClone(diff),
            lineNumber,
            expandedHunks: expanded,
            collapsedContextThreshold: threshold,
          };
          lines.push(layout.isAdditionLineRenderable(args));
          downs.push(
            layout.getNearestRenderableAdditionLine({
              ...args,
              direction: "down",
            }) ?? null,
          );
          ups.push(
            layout.getNearestRenderableAdditionLine({
              ...args,
              direction: "up",
            }) ?? null,
          );
        }
        renderable.push({
          diff: diffIndex,
          threshold,
          expanded: encodeExpanded(expanded),
          renderable: lines,
          down: downs,
          up: ups,
        });
      }
    }
  }

  const heights = [];
  const separators = [
    "simple",
    "metadata",
    "line-info",
    "line-info-basic",
    "custom",
  ];
  for (let diffIndex = 0; diffIndex < diffs.length; diffIndex++) {
    for (let count = 0; count < 8; count++) {
      const diff = diffs[diffIndex];
      const metricsInput =
        random.next() < 0.5
          ? {}
          : {
              lineHeight: 18,
              diffHeaderHeight: 30,
              spacing: 4,
              paddingTop: random.pick([undefined, 2]),
              paddingBottom: random.pick([undefined, 6]),
              hunkSeparatorHeight: random.pick([undefined, 24]),
            };
      const options = {
        fileDiff: structuredClone(diff),
        metrics: computeVirtualFileMetrics(metricsInput),
        disableFileHeader: random.next() < 0.3,
        hunkSeparators: random.pick(separators),
        expandUnchanged: random.next() < 0.2,
        expandedHunks: randomExpanded(random, diff.hunks.length),
        collapsedContextThreshold: random.pick([1, 3]),
        canHydratePartialDiff: random.next() < 0.5,
      };
      heights.push({
        diff: diffIndex,
        metrics: jsonClone(options.metrics),
        disableFileHeader: options.disableFileHeader,
        hunkSeparators: options.hunkSeparators,
        expandUnchanged: options.expandUnchanged,
        expanded: encodeExpanded(options.expandedHunks),
        threshold: options.collapsedContextThreshold,
        canHydratePartialDiff: options.canHydratePartialDiff,
        expected: computeEstimatedDiffHeights(options),
      });
    }
  }

  const windows = [];
  for (let count = 0; count < 100; count++) {
    const input = {
      scrollTop: random.int(5000) + (random.next() < 0.3 ? 0.5 : 0),
      height: 100 + random.int(900),
      scrollHeight: random.int(8000),
      fitPerfectly: random.next() < 0.3 ? true : undefined,
      fitPerfectlyOverscroll: random.next() < 0.3 ? random.int(200) : undefined,
      overscrollSize: random.int(800),
    };
    windows.push({
      input: jsonClone(input),
      expected: createWindowFromScrollPosition(input),
    });
  }

  const offsetTexts = [
    "",
    "a",
    "a\n",
    "a\r\nb\r\n",
    "a\rb\r\nc\n",
    "\n",
    "\r",
    "\r\r\n\n",
    "a\nb\nc\nd\ne\nf\n",
    "x\r\n".repeat(20) + "y",
  ];
  for (let count = 0; count < 80; count++) {
    offsetTexts.push(
      Array.from({ length: random.int(40) }, () =>
        random.pick(["a", "b", "\n", "\r", "\r\n", " ", "é"]),
      ).join(""),
    );
  }
  const lineOffsets = offsetTexts.map((text) => ({
    text,
    offsets: offsets.computeLineOffsets(text),
    breaks: offsets.countLineBreaks(text),
    lines: offsets.linesFromFileContents(text),
  }));

  const trims = [];
  const trimInputs = [
    readFileSync(join(here, "corpus/trim.patch"), "utf8"),
    readFileSync(join(here, "corpus/file.patch"), "utf8"),
    corpusMocks.formatPatchWithVersionTrailer,
    corpusMocks.finalBlankLinePatch,
  ];
  for (let count = 0; count < 6; count++) trimInputs.push(fuzzPatch(random));
  for (const input of trimInputs) {
    for (const contextSize of [0, 3, 10]) {
      trims.push({
        input,
        contextSize,
        expected: trimPatchContext(input, contextSize),
      });
    }
  }

  const hydrations = [];
  for (const [oldText, newText] of pairs.slice(1, 5)) {
    for (const kind of ["clone", "merge"]) {
      for (const cacheKeys of kind === "clone" ? [false, true] : [false]) {
        const patch = jsdiff.createTwoFilesPatch(
          "p.txt",
          "p.txt",
          oldText,
          newText,
          undefined,
          undefined,
          {},
        );
        const partial = parsePatchFiles(patch, {
          cacheKeyPrefix: cacheKeys ? "pk" : undefined,
        })[0].files[0];
        const files = {
          oldFile: {
            name: "p.txt",
            contents: oldText,
            cacheKey: cacheKeys ? "old" : undefined,
          },
          newFile: {
            name: "p.txt",
            contents: newText,
            cacheKey: cacheKeys ? "new" : undefined,
          },
        };
        const options = { disableLineMatching: random.next() < 0.5 };
        const input = jsonClone(partial);
        const result = hydratePartialDiff(
          kind,
          structuredClone(partial),
          files,
          options,
        );
        hydrations.push({
          kind,
          partial: input,
          files: jsonClone(files),
          options,
          expected: jsonClone(result),
        });
      }
    }
  }
  // A pure rename hydrates from the new file alone.
  const renamePatch =
    "diff --git a/old.txt b/new.txt\nsimilarity index 100%\nrename from old.txt\nrename to new.txt\n";
  const renamePartial = parsePatchFiles(renamePatch)[0].files[0];
  const renameFiles = {
    oldFile: null,
    newFile: { name: "new.txt", contents: "x\ny\n", cacheKey: "nk" },
  };
  hydrations.push({
    kind: "clone",
    partial: jsonClone(renamePartial),
    files: jsonClone(renameFiles),
    options: {},
    expected: jsonClone(
      hydratePartialDiff(
        "clone",
        structuredClone(renamePartial),
        renameFiles,
        {},
      ),
    ),
  });

  return {
    pins: {
      pierre: pins["@pierre/diffs"].version,
      sha256: pins["@pierre/diffs"].sha256,
    },
    diffs,
    iterate,
    expandedRegions,
    renderable,
    heights,
    windows,
    lineOffsets,
    trims,
    hydrations,
  };
}

// ---- S5/S6: hast rendering with a deterministic plain-text highlighter ---

const { renderTokenLines } = await import(
  oracle.pierre("utils/renderTokenLines.js")
);
const { createFileHeaderElement } = await import(
  oracle.pierre("utils/createFileHeaderElement.js")
);
const { renderDiffWithHighlighter } = await import(
  oracle.pierre("utils/renderDiffWithHighlighter.js")
);

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonical(value[key])]),
    );
  }
  return value;
}

/** FNV-1a 64 over the canonical JSON; `JSON.stringify` writes holes as null. */
function fnv64(value) {
  let hash = 0xcbf29ce484222325n;
  for (const byte of Buffer.from(JSON.stringify(canonical(value)), "utf8")) {
    hash ^= BigInt(byte);
    hash = (hash * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  return hash.toString(16).padStart(16, "0");
}

function textContent(node) {
  if (node === undefined || node === null) return "";
  if (node.type === "text") return node.value;
  return (node.children ?? []).map(textContent).join("");
}

/** Same line element with its token/decoration children replaced by their text. */
function flattenLine(node) {
  if (node === undefined || node === null) return node;
  if (node.type !== "element") return node;
  return { ...node, children: [{ type: "text", value: textContent(node) }] };
}

function sharedDiffs() {
  // Same construction (and seed) as buildS4, so these are the diffs of layout.json.
  const random = makeRandom(0x5eed05);
  const diffs = [];
  const pairs = [
    ...Object.values(corpusMocks.mockDiffs).map((diff) => [
      diff.oldFile.contents,
      diff.newFile.contents,
    ]),
    ...Array.from({ length: 5 }, (_, index) =>
      syntheticPair(random, [30, 45, 60, 90, 25][index]),
    ),
  ];
  for (const [oldText, newText] of pairs) {
    diffs.push(
      jsonClone(
        parseDiffFromFile(
          { name: "a.txt", contents: oldText },
          { name: "a.txt", contents: newText },
        ),
      ),
    );
  }
  for (const [oldText, newText] of pairs.slice(1, 4)) {
    const patch = jsdiff.createTwoFilesPatch(
      "p.txt",
      "p.txt",
      oldText,
      newText,
      undefined,
      undefined,
      {},
    );
    diffs.push(jsonClone(parsePatchFiles(patch)[0].files[0]));
  }
  return diffs;
}

function buildTokenLines() {
  const random = makeRandom(0x5eed06);
  const pieces = [
    " ",
    "  ",
    "\t",
    "foo",
    "bar",
    "é",
    "😀",
    "a b",
    "x",
    "ab",
    "cd",
  ];
  const cases = [];
  for (let count = 0; count < 140; count++) {
    const lineCount = 1 + random.int(3);
    const lines = [];
    const boundaries = [];
    for (let line = 0; line < lineCount; line++) {
      let offset = 0;
      const tokens = [];
      const lineBoundaries = [0];
      for (
        let token = 0, tokenCount = random.int(6);
        token < tokenCount;
        token++
      ) {
        const content = random.pick(pieces);
        const entry = { content, offset };
        if (random.next() < 0.3) entry.color = random.pick(["#f00", "#0f0"]);
        if (random.next() < 0.1) entry.bgColor = "#00f";
        if (random.next() < 0.2)
          entry.fontStyle = random.pick([1, 2, 4, 8, 5, 12, 15]);
        if (random.next() < 0.1)
          entry.htmlStyle = [
            ["--a", "1"],
            ["color", "red"],
          ];
        if (random.next() < 0.1) entry.htmlAttrs = [["data-k", "v"]];
        tokens.push(entry);
        for (const ch of content) {
          offset += ch.length;
          lineBoundaries.push(offset);
        }
      }
      lines.push(tokens);
      boundaries.push(lineBoundaries);
    }
    const decorations = [];
    for (let index = 0, total = random.int(5); index < total; index++) {
      const startLine = random.int(lineCount);
      const endLine =
        random.next() < 0.8
          ? startLine
          : Math.min(lineCount - 1 + random.int(2), startLine + random.int(2));
      const startBoundary = random.pick(boundaries[startLine]);
      const endBoundaries = boundaries[Math.min(endLine, lineCount - 1)];
      let endCharacter = random.pick(endBoundaries);
      if (endLine === startLine && endCharacter < startBoundary)
        endCharacter = startBoundary;
      decorations.push({
        start: { line: startLine, character: startBoundary },
        end: { line: endLine, character: endCharacter },
        properties: { "data-range": `r${index}` },
      });
    }
    const withState = random.next() < 0.6;
    const state = withState
      ? lines.map((_, index) => ({
          type: random.pick([
            "context",
            "change-addition",
            "change-deletion",
            "context-expanded",
          ]),
          lineNumber: index + 1,
          altLineNumber: random.next() < 0.5 ? index + 2 : undefined,
          lineIndex: random.next() < 0.5 ? `${index},${index}` : index,
        }))
      : undefined;
    const mergeWhitespaces = random.pick(["always", "never"]);
    const jsLines = lines.map((line) =>
      line.map((entry) => ({
        ...entry,
        htmlStyle: entry.htmlStyle
          ? Object.fromEntries(entry.htmlStyle)
          : undefined,
        htmlAttrs: entry.htmlAttrs
          ? Object.fromEntries(entry.htmlAttrs)
          : undefined,
      })),
    );
    const expected = renderTokenLines(jsLines, {
      state: state === undefined ? undefined : { lineInfo: state },
      mergeWhitespaces,
      decorations: decorations.map((decoration) => ({ ...decoration })),
    });
    cases.push({
      lines,
      decorations,
      state: state === undefined ? null : jsonClone(state),
      mergeWhitespaces,
      expected: jsonClone(expected),
    });
  }
  return cases;
}

function buildHeaders() {
  const files = {
    old: { name: "src/a.ts", contents: "one\ntwo\n" },
    changed: { name: "src/a.ts", contents: "one\n2\nthree\n" },
    renamed: { name: "src/b.ts", contents: "one\ntwo\n" },
    empty: { name: "src/e.ts", contents: "" },
  };
  const diffs = {
    change: parseDiffFromFile(files.old, files.changed),
    added: parseDiffFromFile(files.empty, {
      ...files.changed,
      name: "src/e.ts",
    }),
    deleted: parseDiffFromFile(files.old, { ...files.empty, name: "src/a.ts" }),
    renamedChanged: parseDiffFromFile(files.old, {
      ...files.changed,
      name: "src/b.ts",
    }),
    renamedPure: parseDiffFromFile(files.old, files.renamed),
    unchanged: parseDiffFromFile(files.old, files.old),
  };
  const cases = [];
  const push = (label, input) => {
    for (const mode of ["default", "custom"]) {
      for (const stickyHeader of [false, true]) {
        cases.push({
          label,
          input: jsonClone(input),
          mode,
          stickyHeader,
          expected: jsonClone(
            createFileHeaderElement({
              fileOrDiff: structuredClone(input),
              mode,
              stickyHeader,
            }),
          ),
        });
      }
    }
  };
  for (const [label, diff] of Object.entries(diffs)) push(label, diff);
  push("file", { name: "src/index.ts", contents: "export {}\n" });
  return cases;
}

function buildRenderDiffs(diffs) {
  const random = makeRandom(0x5eed07);
  const cases = [];
  for (let diffIndex = 0; diffIndex < diffs.length; diffIndex++) {
    const diff = diffs[diffIndex];
    for (let count = 0; count < 20; count++) {
      const theme =
        random.next() < 0.5
          ? "pierre-dark"
          : { dark: "pierre-dark", light: "pierre-light" };
      const lineDiffType = random.pick([
        "none",
        "word",
        "word-alt",
        "word-line",
        "char",
      ]);
      const maxLineDiffLength = random.pick([1000, 1000, 12]);
      const kind = random.int(3);
      const plain =
        kind === 0
          ? undefined
          : kind === 1
            ? { forcePlainText: true }
            : {
                forcePlainText: true,
                startingLine: random.int(8),
                totalLines:
                  random.next() < 0.7 ? 1 + random.int(12) : undefined,
                expandedHunks: randomExpanded(random, diff.hunks.length),
                collapsedContextThreshold: random.pick([1, 3]),
              };
      const result = renderDiffWithHighlighter(
        structuredClone(diff),
        stubHighlighter,
        {
          theme,
          useTokenTransformer: false,
          tokenizeMaxLineLength: 1000,
          lineDiffType,
          maxLineDiffLength,
        },
        plain === undefined ? undefined : { ...plain },
      );
      const lines = (code) => code;
      cases.push({
        diff: diffIndex,
        theme: jsonClone(theme),
        lineDiffType,
        maxLineDiffLength,
        plain:
          plain === undefined
            ? null
            : {
                forcePlainText: plain.forcePlainText,
                startingLine: plain.startingLine ?? null,
                totalLines: plain.totalLines ?? null,
                expanded: encodeExpanded(plain.expandedHunks),
                collapsedContextThreshold:
                  plain.collapsedContextThreshold ?? null,
              },
        themeStyles: result.themeStyles,
        baseThemeType: result.baseThemeType ?? null,
        deletionCount: result.code.deletionLines.length,
        additionCount: result.code.additionLines.length,
        exact: [
          fnv64(lines(result.code.deletionLines)),
          fnv64(lines(result.code.additionLines)),
        ],
        flat: [
          fnv64(result.code.deletionLines.map(flattenLine)),
          fnv64(result.code.additionLines.map(flattenLine)),
        ],
      });
    }
  }
  return cases;
}

function buildRenderS5() {
  const diffs = sharedDiffs();
  return {
    pins: { pierre: pins["@pierre/diffs"].version },
    diffs,
    tokenLines: buildTokenLines(),
    headers: buildHeaders(),
    renderDiffs: buildRenderDiffs(diffs),
  };
}

// ---- S6: DiffHunksRenderer over the stub highlighter -------------------------

const { DiffHunksRenderer } = await import(
  oracle.pierre("renderers/DiffHunksRenderer.js")
);
const { isDiffPlainText } = await import(
  oracle.pierre("utils/isDiffPlainText.js")
);

function injectedRow(label, key) {
  return {
    content: {
      type: "element",
      tagName: "div",
      properties: { "data-injected": `${label}-${key}` },
      children: [{ type: "text", value: label }],
    },
    gutter: {
      type: "element",
      tagName: "div",
      properties: { "data-injected-gutter": `${label}-${key}` },
      children: [],
    },
  };
}

/**
 * Injected rows chosen by a fixed function of (hunkIndex, lineIndex), the same
 * function as `OracleHooks` in renderers/diff_hunks_renderer/oracle_tests.rs.
 */
class HookedRenderer extends DiffHunksRenderer {
  getUnifiedInjectedRowsForLine(context) {
    const key = context.hunkIndex * 31 + context.lineIndex;
    switch (key % 4) {
      case 1:
        return { before: [injectedRow("ub", key)] };
      case 2:
        return { after: [injectedRow("ua1", key), injectedRow("ua2", key)] };
      case 3:
        return {
          before: [injectedRow("ub", key)],
          after: [injectedRow("ua", key)],
        };
      default:
        return undefined;
    }
  }

  getSplitInjectedRowsForLine(context) {
    const key = context.hunkIndex * 31 + context.lineIndex;
    switch (key % 5) {
      case 1:
        return {
          before: [{ deletion: injectedRow("sbd", key), addition: undefined }],
        };
      case 2:
        return {
          after: [{ deletion: undefined, addition: injectedRow("saa", key) }],
        };
      case 3:
        return {
          before: [
            {
              deletion: injectedRow("sbd", key),
              addition: injectedRow("sba", key),
            },
          ],
          after: [
            { deletion: injectedRow("sad", key), addition: undefined },
            { deletion: undefined, addition: injectedRow("saa", key) },
          ],
        };
      case 4:
        return { before: [{ deletion: undefined, addition: undefined }] };
      default:
        return undefined;
    }
  }
}

/** Every element carrying `data-line` (a rendered code line) reduced to its text. */
function flattenCodeLines(node) {
  if (Array.isArray(node)) return node.map(flattenCodeLines);
  if (node === null || typeof node !== "object") return node;
  if (node.type === "element" && node.properties?.["data-line"] !== undefined) {
    return { ...node, children: [{ type: "text", value: textContent(node) }] };
  }
  return Object.fromEntries(
    Object.entries(node).map(([key, value]) => [key, flattenCodeLines(value)]),
  );
}

function randomRendererCase(random, diffIndex, diff) {
  const lineLimit =
    Math.max(diff.additionLines.length, diff.deletionLines.length) + 3;
  const annotations = [];
  for (let index = 0, total = random.int(5); index < total; index++) {
    annotations.push({
      side: random.pick(["additions", "deletions"]),
      lineNumber: random.next() < 0.15 ? 0 : 1 + random.int(lineLimit),
      metadata: { n: index },
    });
  }
  const expansions = [];
  for (let index = 0, total = random.int(3); index < total; index++) {
    expansions.push({
      index: random.int(diff.hunks.length + 1),
      direction: random.pick(["up", "down", "both"]),
      count: random.pick([undefined, 1, 3]),
    });
  }
  const windowed = random.next() < 0.5;
  return {
    diff: diffIndex,
    lang: random.next() < 0.35 ? "typescript" : null,
    mode: random.pick(["process", "render"]),
    plain: random.next() < 0.5,
    options: {
      theme:
        random.next() < 0.5
          ? "pierre-dark"
          : { dark: "pierre-dark", light: "pierre-light" },
      diffStyle: random.pick(["unified", "split", "split"]),
      hunkSeparators: random.pick([
        "simple",
        "metadata",
        "line-info",
        "line-info-basic",
        "custom",
      ]),
      expandUnchanged: random.next() < 0.2,
      expansionLineCount: random.pick([1, 3, 100]),
      collapsedContextThreshold: random.pick([0, 1, 3]),
      disableFileHeader: random.next() < 0.3,
      headerRenderMode: random.pick(["default", "custom"]),
      stickyHeader: random.next() < 0.5,
      diffIndicators: random.pick(["bars", "classic", "none"]),
      disableBackground: random.next() < 0.3,
      disableLineNumbers: random.next() < 0.3,
      overflow: random.pick(["scroll", "wrap"]),
      lineDiffType: random.pick([
        "none",
        "word",
        "word-alt",
        "word-line",
        "char",
      ]),
      maxLineDiffLength: 1000,
    },
    hasLoader: random.next() < 0.4,
    annotations,
    expansions,
    hooks: random.next() < 0.4,
    range: windowed
      ? {
          startingLine: random.int(6),
          totalLines: random.next() < 0.8 ? 1 + random.int(12) : null,
          bufferBefore: random.pick([0, 0, 20]),
          bufferAfter: random.pick([0, 0, 40]),
        }
      : { startingLine: 0, totalLines: null, bufferBefore: 0, bufferAfter: 0 },
  };
}

function runRendererCase(diff, rendererCase) {
  const { options, range } = rendererCase;
  const renderRange = {
    ...range,
    totalLines: range.totalLines ?? Number.POSITIVE_INFINITY,
  };
  const rendererOptions = { ...options };
  if (rendererCase.hasLoader)
    rendererOptions.loadDiffFiles = async () => undefined;
  const Renderer = rendererCase.hooks ? HookedRenderer : DiffHunksRenderer;
  const renderer = new Renderer(rendererOptions);
  renderer.setLineAnnotations(
    rendererCase.annotations.map((annotation) => ({ ...annotation })),
  );
  for (const expansion of rendererCase.expansions) {
    renderer.expandHunk(expansion.index, expansion.direction, expansion.count);
  }
  const fileDiff = structuredClone(diff);
  if (rendererCase.lang !== null) fileDiff.lang = rendererCase.lang;
  const renderOptions = {
    theme: options.theme,
    useTokenTransformer: false,
    tokenizeMaxLineLength: 1000,
    lineDiffType: options.lineDiffType,
    maxLineDiffLength: options.maxLineDiffLength,
  };
  const threshold = options.collapsedContextThreshold;
  let themed;
  if (rendererCase.mode === "process") {
    themed = renderDiffWithHighlighter(
      fileDiff,
      stubHighlighter,
      renderOptions,
      rendererCase.plain
        ? {
            forcePlainText: true,
            expandedHunks: true,
            collapsedContextThreshold: threshold,
          }
        : { forcePlainText: false, collapsedContextThreshold: threshold },
    );
  } else {
    // What `renderDiff` does for a diff it renders as plain text through a
    // finite range (the worker's getPlainDiffAST); anything else is whole.
    const hasContent =
      fileDiff.additionLines.length > 0 || fileDiff.deletionLines.length > 0;
    const forcePlainText = !hasContent || isDiffPlainText(fileDiff);
    const defaultRange =
      range.startingLine === 0 &&
      range.totalLines === null &&
      range.bufferBefore === 0 &&
      range.bufferAfter === 0;
    themed = renderDiffWithHighlighter(
      fileDiff,
      stubHighlighter,
      renderOptions,
      forcePlainText
        ? {
            forcePlainText: true,
            startingLine: renderRange.startingLine,
            totalLines: renderRange.totalLines,
            expandedHunks:
              defaultRange || options.expandUnchanged
                ? true
                : renderer.expandedHunks,
            collapsedContextThreshold: threshold,
          }
        : { forcePlainText: false, collapsedContextThreshold: threshold },
    );
  }
  const result = renderer.processDiffResult(fileDiff, renderRange, themed);
  delete result.fileDiff;
  return {
    result: jsonClone(result),
    fullAst: jsonClone(renderer.renderFullAST(result)),
  };
}

function buildRenderS6(diffs) {
  const random = makeRandom(0x5eed08);
  const cases = [];
  for (let diffIndex = 0; diffIndex < diffs.length; diffIndex++) {
    for (let count = 0; count < 40; count++) {
      const rendererCase = randomRendererCase(
        random,
        diffIndex,
        diffs[diffIndex],
      );
      const { result, fullAst } = runRendererCase(
        diffs[diffIndex],
        rendererCase,
      );
      const entry = {
        ...rendererCase,
        expected: {
          rowCount: result.rowCount,
          totalLines: result.totalLines,
          hunkData: result.hunkData,
          exact: [fnv64(result), fnv64(fullAst)],
          flat: [
            fnv64(flattenCodeLines(result)),
            fnv64(flattenCodeLines(fullAst)),
          ],
        },
      };
      // A few whole results stay in the golden so a mismatch can be diffed.
      if (JSON.stringify(result).length < 6000) entry.expected.full = result;
      cases.push(entry);
    }
  }
  return { pins: { pierre: pins["@pierre/diffs"].version }, cases };
}

// ---- S7: the highlights backend ----------------------------------------------
//
// Data the Rust adapter embeds (languages, token slots, the pierre themes) is
// exported from the pinned @pierre/highlights dist, and the golden compares the
// dist's end-to-end token output (diffs' HighlightsHighlighter over the
// unmodified wasm, run in node) with our glue fed the same wasm records.

const highlightsDist = (relative) =>
  pathToFileURL(join(oracle.modules, "@pierre/highlights/dist", relative)).href;
const { default: languageTable } = await import(highlightsDist("languages.js"));
const { default: tokenTypeNames } = await import(
  highlightsDist("token-types.js")
);
const { default: pierreDark } = await import(
  highlightsDist("themes/pierre-dark.js")
);
const { default: pierreLight } = await import(
  highlightsDist("themes/pierre-light.js")
);
const { default: wasmBytes } = await import(
  highlightsDist("highlights.wasm.mjs")
);
const highlightsLib = await import(highlightsDist("highlighter.js"));
const { HighlightsHighlighter } = await import(
  oracle.pierre("highlighter/backends/highlights.js")
);
const { getTokenOptions } = await import(
  oracle.pierre("utils/getTokenOptions.js")
);

const highlightsPackage = JSON.parse(
  readFileSync(join(oracle.modules, "@pierre/highlights/package.json"), "utf8"),
);

function highlightsData() {
  return {
    languages: Object.entries(languageTable),
    tokenTypes: tokenTypeNames,
    themes: { "pierre-dark": pierreDark, "pierre-light": pierreLight },
  };
}

const highlightSamples = {
  rust: 'use std::fmt;\n\n/// Doc comment\nfn main() {\n    let s = "héllo 😀"; // trailing\n    println!("{}", s.len() + 0x1f);\n}\n',
  typescript:
    'import { a } from "./a";\nconst x: number = 1 + 2;\nexport function f(y: string) { return "t" + y; }\n/* block\n   comment */\n',
  json: '{\n  "name": "x",\n  "n": [1, 2.5, true, null]\n}\n',
  python: 'def f(x):\n    """doc"""\n    return x ** 2  # square\n',
  text: "plain text line\n\nsecond   line\twith tab\n",
  crlf: "let a = 1;\r\nlet b = 'two';\r\n",
  empty: "",
  unicode: "const café = '日本語'; // コメント\n",
  longline: `const long = "${"x".repeat(60)}";\nshort\n`,
};

const highlightLanguages = {
  rust: "rust",
  typescript: "typescript",
  json: "json",
  python: "py",
  text: "text",
  crlf: "typescript",
  empty: "rust",
  unicode: "javascript",
  longline: "typescript",
};

function buildHighlights() {
  const low = highlightsLib.createHighlighter(
    new WebAssembly.Module(wasmBytes),
  );
  const themeSets = [
    "pierre-dark",
    "pierre-light",
    { dark: "pierre-dark", light: "pierre-light" },
  ];
  const cases = [];
  const highlighter = new HighlightsHighlighter();
  return Promise.all(
    ["pierre-dark", "pierre-light"].map((name) =>
      highlighter.themeResolver.resolveTheme(name),
    ),
  ).then(() => {
    const themes = {};
    for (const name of ["pierre-dark", "pierre-light"]) {
      const theme = highlighter.getTheme(name);
      themes[name] = jsonClone({
        name: theme.name,
        type: theme.type,
        fg: theme.fg,
        bg: theme.bg,
        colors: theme.colors,
      });
    }
    const langCases = [
      ...Object.keys(highlightSamples).map((key) => [
        key,
        highlightLanguages[key],
      ]),
      ["rust", "nonexistent-lang"],
      ["rust", "RUST"],
      ["text", "tfvars"],
    ];
    for (const [sample, lang] of langCases) {
      for (const theme of themeSets) {
        for (const maxLength of [undefined, 20]) {
          const code = highlightSamples[sample];
          const options = getTokenOptions(lang, theme, maxLength);
          const result = highlighter.codeToTokens(code, options);
          const supported = highlightsLib.isSupportedLanguage(lang);
          const langId = highlightsLib.langIdOf(supported ? lang : "text");
          const records = Array.from(
            low.tokenizeLineRecords(langId, low.writeInput(code)),
          );
          cases.push({
            sample,
            code,
            lang,
            theme: jsonClone(theme),
            tokenizeMaxLineLength: maxLength ?? null,
            supported,
            langId,
            records,
            expected: jsonClone({
              tokens: result.tokens,
              fg: result.fg,
              bg: result.bg,
              themeName: result.themeName,
              rootStyle: result.rootStyle,
            }),
          });
        }
      }
    }
    return {
      pins: {
        highlights: highlightsPackage.version,
        pierre: pins["@pierre/diffs"].version,
      },
      themes,
      isAnsi: highlightsLib.isSupportedLanguage("ansi"),
      cases,
    };
  });
}

const outputs = {
  // First: the published dist caches the measured scrollbar gutter for the process
  // (`measuredScrollbarGutter`), and the render oracle needs it unmeasured.
  "s12b_render.json": await buildS12bRender(oracle, makeRandom(0x5eed0f)),
  // After the render oracle and before any oracle that measures the scrollbar
  // gutter: the published module caches the measurement for the process.
  "s12b_virtualized.json": await buildS12bVirtualized(oracle, makeRandom(0x5eed12)),
  "jsdiff.json": buildJsdiff(),
  "scanners.json": buildScanners(),
  "types.json": buildTypes(),
  "patches.json": buildPatches(),
  "utilities.json": buildUtilities(),
  "layout.json": buildS4(),
  "render_s5.json": buildRenderS5(),
  "render_s6.json": buildRenderS6(sharedDiffs()),
  "highlights.json": await buildHighlights(),
  "virtualizer.json": await buildVirtualizer(oracle, makeRandom(0x5eed09)),
  "s11a.json": {
    ...(await buildS11aUtils(oracle, makeRandom(0x5eed0a))),
    managers: await buildS11aManagers(oracle, makeRandom(0x5eed0b)),
  },
  "s12a.json": await buildS12aOptions(oracle, makeRandom(0x5eed0d), sharedDiffs().slice(0, 6)),
  "s11b.json": await buildS11bInteraction(oracle, makeRandom(0x5eed0c)),
  "s12a_lifecycle.json": await buildS12aLifecycle(oracle, makeRandom(0x5eed0e)),
  "s12b_layout.json": await buildS12bLayout(oracle, makeRandom(0x5eed10), corpusMocks),
  "s12b_window.json": await buildS12bWindow(oracle, makeRandom(0x5eed11), corpusMocks),
  "../../highlights/data/highlights_data.json": highlightsData(),
};

/**
 * A gzip member that depends only on the text: a fixed header (no mtime, no
 * file name, no extra flags, OS "unknown"), raw deflate at a fixed level, and
 * the CRC-32 and length trailer. Goldens are stored this way so that a
 * regeneration with unchanged content is byte-identical in git.
 */
function gzipDeterministic(text) {
  const bytes = Buffer.from(text, "utf8");
  const header = Buffer.from([0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 2, 0xff]);
  const trailer = Buffer.alloc(8);
  trailer.writeUInt32LE(crc32(bytes), 0);
  trailer.writeUInt32LE(bytes.length >>> 0, 4);
  return Buffer.concat([header, deflateRawSync(bytes, { level: 9 }), trailer]);
}

let drift = false;
for (const [name, value] of Object.entries(outputs)) {
  const text = `${JSON.stringify(value)}\n`;
  // The goldens next to this file are gzip (`name.json.gz`); the export that
  // lives elsewhere (`../..`) stays plain text.
  const compressed = !name.startsWith("..");
  const file = compressed ? `${name}.gz` : name;
  const content = compressed ? gzipDeterministic(text) : text;
  const path = join(goldenDir, file);
  if (check) {
    let current = null;
    try {
      current = readFileSync(path, compressed ? undefined : "utf8");
    } catch {
      // missing golden counts as drift
    }
    const same = compressed ? current !== null && Buffer.compare(current, content) === 0 : current === content;
    if (!same) {
      console.error(`golden drift: ${file}`);
      drift = true;
    }
  } else {
    writeFileSync(path, content);
    console.log(`${file}: ${text.length} bytes (${content.length} stored)`);
  }
}
// Raw (non-JSON) assets exported from the pinned dist: the package stylesheet
// (`src/style.css?inline`), embedded by utils/dom/css_wrappers.rs.
{
  const { default: styleCss } = await import(oracle.pierre("style.js"));
  const path = join(here, "..", "assets/style.css");
  if (check) {
    let current = "";
    try {
      current = readFileSync(path, "utf8");
    } catch {
      // missing asset counts as drift
    }
    if (current !== styleCss) {
      console.error("asset drift: assets/style.css");
      drift = true;
    }
  } else {
    writeFileSync(path, styleCss);
    console.log(`assets/style.css: ${styleCss.length} bytes`);
  }
}
if (drift) {
  process.exit(1);
}
