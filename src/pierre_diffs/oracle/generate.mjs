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
  };
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

const outputs = {
  "jsdiff.json": buildJsdiff(),
  "scanners.json": buildScanners(),
  "types.json": buildTypes(),
  "patches.json": buildPatches(),
  "utilities.json": buildUtilities(),
  "layout.json": buildS4(),
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
