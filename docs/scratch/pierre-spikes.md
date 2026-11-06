# Pierre spikes: findings

Append-only ledger, one section per lane. Evidence plus options with costs; the user decides.

## P3 highlight

Lane: blocker 4 (highlighting compute and where it runs). Branch `spike/pierre-highlight`.
Everything below was measured by the programs in `bench/highlight/` (committed with raw results in
`bench/highlight/results/`). Machine: AMD Ryzen 9 7940HS (16 threads, laptop, frequency scaling on),
Linux, Node v24.21.0 (V8), wasmtime 49.0.2, rustc 1.100.0-nightly (2026-09-16). The host is shared with
other agents: every pass records `loadavg` (`results/run-info.txt`). Everything except `syntect` is from a quiet
second pass (loadavg about 4.5); the `syntect` rows are from the loaded first pass (loadavg 5-25) and were not re-run
(each takes minutes). Treat absolute milliseconds as +-30%; ratios between engines are the finding.

### Answer in six lines

1. Pierre 1.5.2 highlights with Shiki (TextMate, JS-regex engine by default), **whole file only**, in an 8-worker
   pool, plain text first. 1.6.x adds a second backend, `@pierre/highlights` (Apache-2.0): **hand-written WAT
   lexers, 152 languages, 333 KB raw / 147 KB gzip, not TextMate**, with a streaming mode and an incremental
   "live" mode (resumable lexer state, 1 ms slices).
2. That wasm module is a *core* module (two imports, SIMD). A guest component cannot instantiate it; a host can
   (browser `WebAssembly`, or wasmtime on desktop/Android). It lexes 100k lines of Rust in
   22 ms under wasmtime and 14.4 MiB, 36-230x faster than native `irosashi` on the same files, and ships
   theme-independent token records (about 12 KiB per 200-line window).
3. Fidelity against Shiki with the same Pierre theme: Pierre's own `highlights` backend is **not** Shiki-identical
   (18-68 % of lines visually identical). `irosashi` (pure-Rust TextMate, MIT) is **100 % visually identical** on
   every corpus once one theme tie-break bug is patched (one line, found here); unpatched it is 78-100 %.
4. Pure-Rust TextMate in the guest is viable only incrementally: whole-file `irosashi` costs 1.7 s native / 3.1 s
   wasip3 per 100k Rust lines (TypeScript 4.7 s / 8.3 s), but its `Session` API resumes from a saved `StateStack`:
   a 200-line window is 3-10 ms native / 6-18 ms wasip3 (Rust, TypeScript) from a checkpoint, versus a median
   1.0-2.5 s native / 1.9-4.6 s wasip3 to reach a window from line 0 in a 100k-line file.
5. tree-sitter (via `arborium`, the only wasm-friendly packaging found) builds for wasm32-unknown-unknown but **does
   not link into wasm32-wasip3** with this toolchain (C objects use global thread context, the p3 link uses
   `--cooperative-threading`); its colours need a lossy capture-to-scope table (0.5-53 % of lines visibly identical, 23-86 % of characters).
6. Where it runs is a product decision; options with measured costs are in "Part 4".

### Part 1. Pierre's highlighting, from source

Citations are `packages/diffs/src` of the vendored 1.5.2 (`~/repos/rust/intest/vendor/pierre_repo`) unless marked 1.6.4.

- **Shiki, engine, themes.** `highlighter/shared_highlighter.ts:3-7` imports `createHighlighter`; `:41-56` builds one shared
  highlighter with `createJavaScriptRegexEngine()` by default (`preferredHighlighter = 'shiki-js'`, `:44`) or the
  Oniguruma wasm engine for `'shiki-wasm'`. Default themes `pierre-dark`/`pierre-light` (`constants.ts:44-47`); themes
  and languages are attached lazily (`shared_highlighter.ts:71-88`). Shiki is pinned `4.4.1` in the repo workspace.
- **Call shape.** `utils/renderFileWithHighlighter.ts:90` calls `highlighter.codeToHast(...)` with
  `defaultColor: false`, `cssVariablePrefix: '--diffs-token-'` (`:73-74`, `utils/formatCSSVariablePrefix.ts:2`),
  `tokenizeMaxLineLength` and `tokenizeTimeLimit: 0` (`:61-65` explains: Shiki's silent 500 ms/line abort smears colours,
  long lines are guarded by the length limit instead). Diffs do the same twice, once per side
  (`utils/renderDiffWithHighlighter.ts:489-540`).
- **Token output shape.** A hast tree; every token is a `span` whose `style` carries both themes as custom properties,
  `--diffs-token-dark:#FF678D;--diffs-token-light:#D32A61` (snapshot `test/__snapshots__/FileRenderer.test.ts.snap:411`);
  the transformer forces `mergeWhitespaces = 'never'` (`utils/createTransformerWithState.ts:65`) and tags `data-char`
  (`:67-72`). Equivalent flat form: per line `(offset, content, color-light, color-dark, fontStyle)`.
- **Whole file vs window.** Highlighting is whole-file by construction: when not forced to plain text the renderer
  overwrites any window with `startingLine = 0, totalLines = Infinity` (`renderFileWithHighlighter.ts:37-43`) because a
  TextMate state cannot start mid-file. Windowing exists only for the plain-text first paint
  (`worker/WorkerPoolManager.ts:717`, `{ forcePlainText: true, startingLine, totalLines, lines }`), after which the
  highlighted result swaps in.
- **Worker pool.** `worker/WorkerPoolManager.ts:494` spawns `poolSize ?? 8` workers (`worker/worker.ts:2-3,194-195` creates
  the JS or Oniguruma engine per worker); results are cached in two LRU maps of 100 ASTs
  (`WorkerPoolManager.ts:150-151`).
- **The "100k" cutoff is lines, not bytes.** `constants.ts:59` `DEFAULT_TOKENIZE_MAX_LENGTH = 100_000`; `renderers/FileRenderer.ts:448-451,
  729-730,847-849,1148-1167`: `isFileMassive(lines.length, tokenizeMaxLength)` forces plain text for files over 100,000 lines
  (`File.ts:1394`, `FileDiff.ts:2544` read the same option). The separate per-*line* limit is `tokenizeMaxLineLength`
  (`types.ts:436`).
- **Editor path (not used by the diff viewer).** `editor/tokenizer.ts` drives TextMate `IGrammar`/`StateStack` directly with a
  500 ms limit (`TOKENIZE_TIME_LIMIT`) and is the only incremental tokenizer in 1.5.2.

**Pierre 1.6.4 (npm, fetched with `npm pack`, `scratchpad/p164/`).** `@pierre/diffs@1.6.4` depends on `@pierre/highlights@0.4.0`.
`dist/highlighter/backends/highlights.js` is a second `DiffsHighlighter` next to `backends/shiki.js`; `highlighterType.js` still
defaults to `"shiki-js"` (`resolveHighlighterType`), so highlights is opt-in; `utils/canUseWASM.js` probes WebAssembly **and SIMD**
and says to fall back to `shiki-js` when absent (for example a CSP without `wasm-unsafe-eval`, Safari before 16.4).
`resolveOptions` maps unsupported languages to `text` and needs a Zed-format theme (`theme.zed`).

What `@pierre/highlights` is (package files plus `packages/highlights/ARCHITECTURE.md` from the public repo, read in full):

| fact | value |
|---|---|
| licence | Apache-2.0 (`LICENSE.md`); bundled themes carry per-theme licences listed in `NOTICE.md` (pinned Zed/Shiki sources) |
| implementation | **WAT** lexers (`src/langs/*.wat`, `highlights.wat`, `live.wat`), compiled by wabt + binaryen, no syntax tree, classify tokens from nearby tokens and saved state |
| size | `highlights.wasm` 332,924 B, 146,985 B gzip per `package.json` (re-measured here: 332,924 / 147,526 gzip -9 / 125,586 brotli-11) |
| coverage | 153 lexer ids = 152 languages plus `plain` (349 names and aliases), `ansi` handled in JS. Shiki 4.4.1 bundles 242; irosashi 257 |
| ABI | core module, imports `env.is_id_start`, `env.is_id_continue` only; exports `memory`, `highlight`, `highlightStream`, `live*`. Needs SIMD and bulk memory |
| output | mode 0-2 HTML, mode 3 **UTF-16 line records** `(endUtf16:u32, tokenId:u32)`, id `0xFFFFFFFF` ends a line; token ids are theme-independent, the host applies a theme afterwards |
| incremental | `highlightStream` keeps lexer state between chunks; `LiveTokenizer` owns an editable document in wasm, re-lexes dirty lines, stops when the outgoing state equals the old one, 1 ms synchronous slices with `onDeferTokenize` batches (`ARCHITECTURE.md` "Streaming", "Live documents") |
| runs in | Node, browsers, workerd; loaded synchronously (`new WebAssembly.Module(bytes)` at import) |
| inside our guest? | **No**, not as is: it is a core module. A guest component has no wasm engine; embedding an interpreter in the guest was not tried. It runs in the **host**: browser `WebAssembly` (web) or wasmtime (desktop/Android), see `bench/highlight/pierre-wasmtime` |

### Part 2. Candidates, measured

Corpus (`bench/highlight/make-corpus.ts`, hashes in `bench/highlight/corpus-MANIFEST.txt`): real files concatenated and cut at exactly
1,000 / 10,000 / 100,000 lines: Rust (`src/**/*.rs` of this repo), TypeScript (Pierre's packages), JSON (crates.io registry `.json`
joined as an array), Markdown (registry `.md`). Lines over 2000 chars excluded. Theme for every engine: `pierre-dark` (VS Code form from
`@pierre/theme@2.0.0`, Zed form from `@pierre/highlights/themes`). Every engine returns per-line styled runs; conversion into runs is
inside the timed region. Timings are wall clock of one whole-file tokenization inside the process: first rep = cold (grammar compile,
page faults), "warm" = median of the remaining reps (n = 4 for 1k and 10k, 2 for 100k, 1 warm rep for syntect 100k). Memory: native =
process `VmHWM` (includes the binary); wasm = final linear-memory size (it never shrinks, so it is the high water); Node end-to-end =
RSS delta.

Targets: **native** = release binary; **wasip3** = `wasm32-wasip3` component run by `wasmtime run -S p3` (the guest runtime);
**web** = `wasm32-unknown-unknown` + `wasm-bindgen --target nodejs`, run in Node/V8 (Chromium itself was not driven: unverified, V8 is the
same wasm engine family). Pierre's wasm has no Rust build: it is driven by a Rust+wasmtime replay of its JS ABI (`pierre-wasmtime`) and by
Node directly.

Candidate table (licence, coverage, size). Sizes are of the artifact as built (`cargo build --release`, LTO, `panic=abort`, no `wasm-opt`
unless stated):

| candidate | licence | languages | tokenizer | wasm raw | gzip -9 | brotli-11 | builds for wasip3 |
|---|---|---|---|---:|---:|---:|---|
| `@pierre/highlights` 0.4.0 | Apache-2.0 | 152 | hand-written WAT lexers (not TextMate) | 332,924 | 147,526 | 125,586 | n/a, core module (host-side only) |
| `irosashi` 0.3.0, embedded assets (all 257 grammars, 65 themes) | MIT; Oniguruma port `ferroni` BSD-2-Clause; grammars/themes derived from Shiki's `textmate-grammars-themes` (`assets/provenance.lock.json`); per-grammar licences not audited | 257 | TextMate, byte-identical to vscode-textmate (claim; see fidelity) | 10,290,785 (wasip3) / 10,106,482 (web); 9,887,253 after `wasm-opt -O3` | 1,927,582 / 1,877,477 | 1,284,606 / 1,242,352 | yes |
| `irosashi` 0.3.0, only 5 grammars (`irosashi-min`: rust, typescript, javascript, json, markdown) | as above | 5 | same | 2,114,273 (wasip3) / 1,918,763 (web) | 593,704 / 540,354 | not measured | yes |
| `syntect` 5.3.0 + `two-face` 0.5.2 (pure-Rust `fancy-regex`) | MIT; two-face MIT OR Apache-2.0; bundled syntaxes carry their own licences (bat) | not counted | Sublime syntaxes (not TextMate scopes) | 2,902,625 / 2,701,879; 2,389,612 after `wasm-opt -O3` | 1,454,832 / 1,409,071 | 1,247,307 / 1,218,643 | yes |
| tree-sitter via `arborium` 2.18.2 (4 grammars) | MIT OR Apache-2.0; grammars permissive per its `LICENSES.md` | 4 built, ~70 default | tree-sitter + highlight queries | web only: 5,348,441; 3,746,079 after `wasm-opt -O3` | 838,894 / 948,402 | 594,478 / 659,986 | **no** (see Findings) |
| `tree-sitter-highlight` 0.27 directly | MIT | per grammar crate | same family | not built separately: `arborium` already bundles a fork of the tree-sitter runtime plus highlight queries and is the wasm-oriented packaging; the plain crate needs the same C sysroot on wasip3 (unverified) | | | no (assumed, unverified) |
| Shiki 4.4.1 (the oracle; JS) | MIT | 242 | TextMate, Oniguruma wasm or JS-regex | n/a | n/a | n/a | n/a |

Not run: `giallo` and other Rust TextMate crates named in the post-mortem (not needed to answer the question; irosashi already gives the
byte-identical-to-vscode-textmate class), `inkjet`/`syntastica` (tree-sitter packaging; same C-link problem on wasip3 assumed, unverified).

**Warm-median wall ms** (whole file, one call; `irosashi`=full registry, `irosashi-min`=5 grammars; `highlights.*` = Pierre's WAT lexers producing line records only, `highlights.node-e2e` = Pierre's `codeToTokens` with JS token objects; `shiki-*` = Node end to end; `syntect.*` = loaded pass)

| engine / target | rust-1k | rust-10k | rust-100k | ts-1k | ts-10k | ts-100k | json-1k | json-10k | json-100k | md-1k | md-10k | md-100k |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| arborium.native | 10.97 | 211.11 | 2079.69 | 8.19 | 69.77 | 787.21 | 2.21 | 36.6 | 393.32 | 18.34 | 149.4 | 1769.86 |
| arborium.web | 14.87 | 264.31 | 2229.93 | 11.41 | 78.66 | 1035.24 | 4.58 | 47.77 | 505.02 | 27.58 | 193.25 | 1939.45 |
| highlights.raw-node | 0.25 | 3 | 21.74 | 0.28 | 2.45 | 26.23 | 0.12 | 0.98 | 9.51 | 0.16 | 0.88 | 12.1 |
| highlights.wasmtime | 0.17 | 2.38 | 21.97 | 0.22 | 2.39 | 31.1 | 0.05 | 0.73 | 8.99 | 0.08 | 0.7 | 10.55 |
| irosashi-min.native | 14.35 | 173.23 | 1636.98 | 29.72 | 347.74 | 3113.07 | 2.49 | 34.71 | 341.88 | 10.48 | 74.93 | 809.1 |
| irosashi-min.wasip3 | 29.38 | 342.29 | 2927.77 | 65.16 | 669.63 | 6339.16 | 4.9 | 51.34 | 584.24 | 16.13 | 121.92 | 1480.37 |
| irosashi-min.web | 28.32 | 299.98 | 2464.08 | 58.21 | 551.77 | 5553.5 | 6.08 | 58.27 | 534.14 | 14.56 | 122.45 | 1159.04 |
| irosashi.native | 15.58 | 186.07 | 1664.56 | 41.74 | 424.94 | 4720.76 | 2.71 | 37.4 | 342.65 | 33.66 | 245.67 | 2789.82 |
| irosashi.wasip3 | 26.74 | 345.22 | 3142.34 | 76.16 | 761.3 | 8255.2 | 4.39 | 55.89 | 594.79 | 60.87 | 409.77 | 4555.73 |
| irosashi.web | 27.6 | 284.54 | 2800.28 | 63.69 | 662.94 | 6369.33 | 6.8 | 51.94 | 558.9 | 63.43 | 364.43 | 3557.76 |
| highlights.node-e2e | 2.8 | 17.01 | 103.42 | 2.39 | 8.7 | 102.89 | 0.94 | 9.54 | 68.31 | 1.47 | 6.61 | 62.25 |
| shiki-onig.node-e2e | 49.89 | 596.98 | 5186.51 | 68.81 | 664.64 | 6595.24 | 11.32 | 106.1 | 962.47 | 30.64 | 225.96 | 2355.62 |
| shiki-js.node-e2e | 39.49 | 580.44 | 5073.98 | 93.53 | 2503.71 | 23899.08 | 12.21 | 91.1 | 867.84 | 21.12 | 112.18 | 1730.15 |
| syntect.native | 72.53 | 957.08 | 16717.58 | 374.6 | 4440.24 | 74327.42 | 16.57 | 226.39 | 2340.11 | 320.75 | 1393.43 | 25948.3 |
| syntect.wasip3 | 127.91 | 2359.15 | 27590.57 | 951.64 | 7735.8 | 83047.03 | 30.42 | 398.23 | 4021.57 | 582.79 | 2589.38 | 41353.55 |
| syntect.web | 108.87 | 2261.42 | 19447.97 | 783.13 | 6101.78 | 126716.63 | 25.68 | 264.33 | 2612.88 | 502.56 | 2175.79 | 33832.44 |

**Peak memory MiB** (native: VmHWM of the process; wasm: linear memory; node-e2e: RSS delta)

| engine / target | rust-1k | rust-10k | rust-100k | ts-1k | ts-10k | ts-100k | json-1k | json-10k | json-100k | md-1k | md-10k | md-100k |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| arborium.native | 9.6 | 29.9 | 205.4 | 7.9 | 22.5 | 174.7 | 4 | 10.6 | 82.2 | 12.2 | 31.3 | 245.1 |
| arborium.web | 9.4 | 29.4 | 206.8 | 9.3 | 20.8 | 152.3 | 5.6 | 13.3 | 97.3 | 11.2 | 29.7 | 246.8 |
| highlights.raw-node | 0.5 | 1.8 | 14.4 | 0.5 | 1.3 | 11.9 | 0.2 | 1.1 | 9.1 | 0.5 | 1.2 | 8.6 |
| highlights.wasmtime | 0.5 | 1.8 | 14.4 | 0.5 | 1.3 | 11.9 | 0.2 | 1.1 | 9.1 | 0.5 | 1.2 | 8.6 |
| irosashi-min.native | 6.8 | 13.2 | 70.1 | 15.6 | 20.9 | 72.6 | 5.9 | 10.9 | 62.6 | 13.3 | 16.4 | 55.4 |
| irosashi-min.wasip3 | 3.6 | 9.1 | 57.5 | 11.2 | 15.8 | 60.2 | 2.9 | 7.2 | 50.9 | 9.9 | 11.9 | 44.4 |
| irosashi-min.web | 3.6 | 9 | 56.1 | 11.1 | 15.6 | 58.8 | 2.9 | 7.1 | 49.4 | 10.2 | 13.1 | 54.7 |
| irosashi.native | 10.9 | 17.3 | 74.2 | 21.5 | 27 | 79.3 | 8.7 | 13.7 | 65.3 | 59.1 | 62.4 | 111.1 |
| irosashi.wasip3 | 11.2 | 16.7 | 65 | 19.6 | 24.1 | 68.9 | 10.5 | 14.8 | 58.4 | 62.4 | 65.2 | 106 |
| irosashi.web | 11.2 | 16.6 | 63.6 | 19.4 | 23.9 | 67.4 | 10.5 | 14.6 | 56.9 | 62.4 | 66.3 | 115.8 |
| highlights.node-e2e | 24.7 | 75.9 | 254 | 25.1 | 54.4 | 219.2 | 18.6 | 52 | 199.1 | 25 | 47.1 | 145.1 |
| shiki-onig.node-e2e | 72.6 | 146.8 | 419.8 | 112.9 | 182.8 | 450.1 | 78.7 | 127.6 | 338.3 | 95.4 | 135 | 357.9 |
| shiki-js.node-e2e | 65.4 | 139.7 | 405.2 | 74.8 | 171.9 | 404 | 65.3 | 122.2 | 300.2 | 74 | 129.4 | 360.7 |
| syntect.native | 19.4 | 23.4 | 49 | 119.5 | 132 | 159.7 | 8.1 | 10.3 | 36.8 | 32 | 33.8 | 124 |
| syntect.wasip3 | 14.1 | 17.8 | 42.6 | 96.9 | 108.4 | 133.6 | 4.3 | 6.6 | 31.9 | 22.9 | 24.6 | 102.3 |
| syntect.web | 14.1 | 17.7 | 51.8 | 97.9 | 109.4 | 143.1 | 4.5 | 6.6 | 31.3 | 23.6 | 26.3 | 113.3 |


Reading the tables:
- `highlights.*` (Pierre wasm, lex only, records ready for the wire) is the only candidate that tokenizes a 100k-line file inside a
  couple of frames: 22 ms for Rust, 9-31 ms for the others under wasmtime (25 warm reps; p95 within 12 % of the median at 100k, see
  `results/highlights.wasmtime.jsonl`); V8 gives the same (22-26 ms Rust/TS). It is 36-230x faster than native `irosashi` on the same files.
  The `node-e2e` row adds JS token objects (what Pierre's JS does today) and costs 4-7x the lex. One-off wasmtime JIT of the module:
  0.37-0.65 s (`compileMs`; AOT precompilation would remove it, unmeasured).
- TextMate engines need seconds at 100k lines: native `irosashi` is 7-16x faster than `syntect` (fancy-regex), 1.2-3x faster than Shiki/Onig
  in Node on Rust, TypeScript and JSON, and 1.2x slower on Markdown. wasm32 costs 1.6-1.9x over native for wasip3 and 1.5-1.7x for the
  web build (Node/V8). No candidate except Pierre's lexers can do a whole large file in one dispatch.
- Memory: `highlights` 8.6-14.4 MiB of wasm linear memory at 100k lines against 57-116 MiB for irosashi and 150-250 MiB for arborium
  (which keeps a paint array plus spans). TS and Markdown pay a one-off grammar compile on the first call (cold is 2-3x warm at 1k lines, about
  45-70 ms; see the `ms[0]` column in `results/`). `irosashi-min` (5 grammars, no embedded-language grammars) is about 5x smaller than the full engine; on TypeScript and Markdown it is also faster than the full registry (see tables), on Rust/JSON equal, and its Markdown output has no highlighting inside fences of other languages.
- `syntect` rows come from the loaded pass (see `run-info.txt`); do not quote them to better than 2x. `syntect` 100k TypeScript/Markdown have one warm rep.

### Part 3. Fidelity against Shiki (the ratchet)

Oracle: Shiki 4.4.1, `pierre-dark` VS Code theme, `codeToTokensBase` with `tokenizeTimeLimit: 0` (Pierre's setting). Shiki's JS-regex
engine and Oniguruma engine produce **identical** tokens on all four 10k files (100 %/100 %/100 %/100 %: rust, ts, json, md), so
Pierre's default (`shiki-js`) loses nothing against Oniguruma here. `bench/highlight/fidelity.mjs` compares per line:

- `visible`: identical `(colour, fontStyle)` for every non-whitespace character (what the eye sees; whitespace has no foreground);
- `runs`: identical after merging adjacent equal-style tokens; `strict`: identical token boundaries (Shiki merges adjacent same-style tokens,
  TextMate engines do not, so `strict` is a framing artefact and is reported only for completeness);
- `chars`: share of non-whitespace characters with the right style.

| engine | metric | rust-1k | rust-10k | ts-1k | ts-10k | json-1k | json-10k | md-1k | md-10k |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| shiki-js (shiki, JS-regex engine) | visible lines % | 100 | 100 | 100 | 100 | 100 | 100 | 100 | 100 |
| shiki-js (shiki, JS-regex engine) | chars % | 100 | 100 | 100 | 100 | 100 | 100 | 100 | 100 |
| shiki-js (shiki, JS-regex engine) | strict lines % | 100 | 100 | 100 | 100 | 100 | 100 | 100 | 100 |
| highlights (Pierre WAT lexers, Zed theme) | visible lines % | 51.35 | 42.77 | 66.33 | 67.8 | 47.05 | 17.66 | 46.15 | 45.45 |
| highlights (Pierre WAT lexers, Zed theme) | chars % | 87.21 | 86.31 | 83.16 | 87.95 | 76.7 | 60.85 | 75.31 | 73.27 |
| highlights (Pierre WAT lexers, Zed theme) | strict lines % | 12.59 | 10.56 | 5.69 | 14.01 | 12.09 | 3.19 | 45.75 | 45.41 |
| irosashi | visible lines % | 89.01 | 87.3 | 78.22 | 89.61 | 100 | 100 | 99.6 | 99.96 |
| irosashi | chars % | 98.16 | 98.46 | 95.07 | 97.94 | 100 | 100 | 99.98 | 100 |
| irosashi | strict lines % | 50.95 | 42.62 | 40.36 | 58.36 | 18.18 | 6.88 | 46.75 | 45.59 |
| irosashi-min | visible lines % | 89.01 | 87.3 | 78.22 | 89.61 | 100 | 100 | 99.6 | 99.96 |
| irosashi-min | chars % | 98.16 | 98.46 | 95.07 | 97.94 | 100 | 100 | 99.98 | 100 |
| irosashi-min | strict lines % | 50.95 | 42.62 | 40.36 | 58.36 | 18.18 | 6.88 | 46.75 | 45.59 |
| irosashi-patched (+tie-break patch) | visible lines % | 100 | 100 | 100 | 100 | 100 | 100 | 100 | 100 |
| irosashi-patched (+tie-break patch) | chars % | 100 | 100 | 100 | 100 | 100 | 100 | 100 | 100 |
| irosashi-patched (+tie-break patch) | strict lines % | 54.05 | 45.48 | 50.45 | 64.02 | 18.18 | 6.88 | 46.85 | 45.6 |
| arborium | visible lines % | 41.16 | 33.16 | 39.86 | 52.99 | 0.5 | 0.45 | 46.95 | 45.53 |
| arborium | chars % | 64.38 | 64.17 | 78.79 | 85.57 | 66.04 | 44.97 | 22.79 | 28.44 |
| arborium | strict lines % | 30.87 | 27.26 | 22.38 | 37.3 | 0.5 | 0.45 | 46.25 | 45.46 |
| syntect | visible lines % | 35.06 | 29.38 | 73.93 | 87.47 | 47.05 | 17.66 | 46.65 | 45.5 |
| syntect | chars % | 46.43 | 51.53 | 93.14 | 97.32 | 76.81 | 60.86 | 86.16 | 89.85 |
| syntect | strict lines % | 24.78 | 18.99 | 39.16 | 57.42 | 18.18 | 6.88 | 45.05 | 45.34 |

Findings from the fidelity run:
- **irosashi theme tie-break bug (pinned, patch measured).** With the Pierre theme irosashi 0.3.0 colours `mod` in `pub mod json;` `#ff678d`
  where Shiki gives `#d568ea`. Cause: the theme has two rules for the scope `storage.type`; vscode-textmate lets the *later* rule win a tie,
  `irosashi-0.3.0/src/theme/matcher.rs` `Theme::resolve` documents and implements "Ties keep the earlier rule"
  (`score.greater_than(fg_score)`). Changing the three comparisons to `!fg_score.greater_than(score)` (and bg, font style) took visible-line
  identity to **100 % on all 8 cells**. The crate's own 468/468 github-dark/github-light checks do not hit duplicate selectors. Patch used:
  `bench/highlight/irosashi-theme-tiebreak.patch`; not pushed upstream (needs a decision).
- **Pierre's own `highlights` is a different highlighter, not a faster Shiki.** Visible-line identity against Shiki is 43-51 % on Rust, 66-68 % on TypeScript, 18-47 % on JSON, 45-46 % on Markdown (character-level style match 61-88 %). The top disagreements are palette-level
  (`#ffab16 -> #ffa359`, `#d568ea -> #ff678d`, `#68cdf2 -> #ffd452`: the Zed theme conversion and a different scope vocabulary), so the number is
  lexer plus theme together; tests ported from 1.5.2 that assert Shiki colours will not pass on this backend.
- **tree-sitter (arborium)** reaches only 0.5-53 % visible-line identity (23-86 % of characters) with a hand-written capture-name-to-TextMate-scope table (`arborium_engine::scope_of`); the table is the lossy
  step, so read the number as a lower bound, not as the ceiling of tree-sitter.
- **syntect** needs its own theme object (built from the same tokenColors) and Sublime scopes differ from TextMate's: 18-87 % visible-line identity (46-97 % of characters).

### Part 4. Where it runs (measure, do not decide)

What decides it, measured:

**4a. Can a guest tokenize incrementally inside a dispatch budget, resumable per window?** (irosashi `Session`, `bench/highlight/src/probe.rs`;
theme lookup excluded, it is a memoised per-scope table.) Chunks of 256 lines, `StateStack` checkpoint after every chunk, then a 200-line
window at ten offsets, resumed from the nearest checkpoint versus from line 0:

| file | 256-line chunk p50 / p95 / max ms, native | same, wasip3 | 200-line window from checkpoint, median / max ms, native | same, wasip3 | same window from line 0, median / max ms, native | same, wasip3 |
|---|---|---|---|---|---|---|
| rust-10k | 4.47 / 7.35 / 8.19 | 7.43 / 10.01 / 13.77 | 3.19 / 3.68 | 5.92 / 7.05 | 99.03 / 164.13 | 173.41 / 314.36 |
| rust-100k | 3.77 / 5.08 / 7.43 | 7.34 / 9.47 / 14.81 | 3.2 / 5.96 | 6.0 / 11.23 | 969.62 / 1509.68 | 1872.18 / 3023.72 |
| ts-10k | 11.4 / 17.24 / 39.13 | 20.51 / 24.81 / 59.82 | 9.13 / 11.3 | 16.39 / 21.35 | 274.85 / 437.32 | 462.3 / 784.5 |
| ts-100k | 11.16 / 15.2 / 40.53 | 20.23 / 26.04 / 52.57 | 9.97 / 12.33 | 17.58 / 20.99 | 2500.62 / 4299.92 | 4572.12 / 7773.94 |
| json-10k | 0.8 / 0.86 / 1.3 | 1.33 / 1.44 / 2.07 | 0.6 / 0.66 | 1.09 / 1.15 | 16.67 / 29.86 | 30.01 / 51.3 |
| json-100k | 0.84 / 0.9 / 2.19 | 1.5 / 1.58 / 2.93 | 0.66 / 0.76 | 1.22 / 1.24 | 201.38 / 342.73 | 356.49 / 599.9 |
| md-10k | 4.84 / 9.34 / 75.81 | 8.94 / 16.03 / 87.02 | 4.73 / 6.77 | 8.79 / 11.7 | 147.08 / 203.5 | 285.88 / 429.74 |
| md-100k | 6.34 / 11.58 / 67.54 | 11.23 / 20.34 / 89.97 | 4.82 / 6.56 | 8.0 / 10.18 | 1529.6 / 2525.23 | 2432.77 / 4251.52 |

- Yes for the mechanism: state is an explicit immutable handle, so a guest can tokenize N lines per dispatch and resume. Cost at 100k lines:
  a 256-line chunk is 3.8 ms (Rust), 11 ms (TypeScript), 0.8 ms (JSON), 6 ms (Markdown) native and 7.3 / 20 / 1.5 / 12 ms in wasip3, with
  max chunks of 15 ms (Rust), 53 ms (TypeScript) and up to 87 ms (Markdown, cause not isolated) in wasip3. A 16 ms budget therefore fits
  about 256 Rust lines or about 64 TypeScript lines per dispatch with headroom. Starting at an arbitrary window **without a checkpoint costs
  the whole prefix**: median over ten window offsets of a 100k-line file is 0.97 s (Rust) / 2.5 s (TypeScript) native, 1.9 s / 4.6 s wasip3,
  so a first view deep in a file needs a background pass from the top: the same constraint Pierre states by refusing windowed highlighting
  (`renderFileWithHighlighter.ts:37-43`).
- Total CPU is not reduced, only sliced, and it stays on the UI thread (ADR 018: no fuel on web, no suspension in presentation calls).
- Pierre's `LiveTokenizer` (`bench/highlight/probe-live.mjs`, Node): the constructor of a 100k-line Rust doc takes about 24 ms
  (loads the text into wasm), then background batches of 16 lines arrive in order from line 0; the full drain of 100k lines takes 0.17-0.25 s
  (`drainMs`, quiet pass), and an edit at line 3 (opening a `/* ` comment) returns in 0.8-1.9 ms with 114-189 ms of background re-lex for
  Rust/TypeScript/JSON (Markdown: no state ripple, nothing pending). All in `results/probe-live.jsonl`; the loaded first pass saw 0.45-0.54 s
  for the drain. A `renderRange` in the middle of the file is **not** served ahead of the prefix (observed: first delivery is lines 0-4;
  `syncWindowLinesDelivered` 0 after the edit). Raw whole-file lex is only ~22 ms per 100k Rust lines (wasmtime), so the slicing machinery is
  mostly overhead in that regime.

**4b. What does a host-side highlight capability cost on the wire?** Pierre's records are theme-independent token ids, so a theme change needs no
re-lex. Measured record sizes (`recordBytes` / lines, 100k files), per 200-line window:

| content | tokens/line (Pierre lexer) | mode-3 records `(u32 end, u32 id)` + line terminator | `packed24` (4 B/token, from `live.d.ts`) | Shiki-grade runs at 4 B (tokens/line) |
|---|---:|---:|---:|---:|
| Rust | 6.96 | 12.4 KiB | 5.4 KiB | 6.2 KiB (7.93) |
| TypeScript | 5.55 | 10.2 KiB | 4.3 KiB | 5.3 KiB (6.82) |
| JSON | 4.77 | 9.0 KiB | 3.7 KiB | 3.7 KiB (4.72) |
| Markdown | 2.66 | 5.7 KiB | 2.1 KiB | 2.8 KiB (3.55) |

These are below the 256 KiB batch cap by 20-45x; the data volume is not the problem. The cost of a *capability* is the contract and the
widened guest-visible surface that ADR 015 narrowed. The capability bank already carries async byte calls under an NSID contract
(`src/clay_guest_session/wit/capabilities.wit:40,71-77`, `publish-call(name, contract)` served through `micro-lifecycle.call`), so a
highlight call needs a Lexicon contract and a provider, not necessarily a new WIT interface; whether the *host* can itself be the provider
was not checked. Per call the guest pays a copy of the file text or an id the provider can resolve, plus one more artefact to ship. The cost that does bite is turning runs into elements: a 200-line window is about 1,400 token spans
(`6.96 x 200`); at element + `style` + text + append per span that is roughly 5,600 mutations, over `MAX_BATCH_MUTATIONS = 4096`
(`src/clay_surface/tree.rs:84`), which is blocker 3's lane, not this one (arithmetic from the measured token counts, not measured on the wire).

**4c. Options (no choice made).** Costs are the measured ones above.

| option | what runs where | measured cost | open cost |
|---|---|---|---|
| A. Host capability, Pierre `highlights.wasm` | host (browser `WebAssembly`, wasmtime on desktop/Android) lexes; guest gets records/runs | 22 ms per 100k Rust lines lex + a few KiB per window; module 147 KiB gz; wasmtime JIT compile of the module 0.37-0.65 s once (AOT precompilation would remove it, unmeasured); 14 MiB for 100k lines | a capability contract plus a provider; Apache-2.0 attribution and `NOTICE.md` theme licences; a 152-language, non-TextMate lexer: 18-68 % of lines visibly identical to Shiki on our corpus (61-88 % of characters), so colours differ from Pierre 1.5.2; needs SIMD (web CSP: `wasm-unsafe-eval`); one more wasm artefact per host |
| B. Host capability, irosashi (patched) in the host | host lexes with a TextMate-identical engine, guest gets runs; checkpoints live host-side | whole file 1.7 s native / 3.1 s wasip3-class per 100k Rust lines (TypeScript 4.7 / 8.3 s), windows 3-10 ms native from a checkpoint | same contract as A; fork/patch of irosashi (tie-break) or upstream PR; 10 MiB wasm unless trimmed to needed grammars (`irosashi-min` 1.9-2.1 MB, 0.54-0.59 MB gzip) |
| C. In-guest irosashi, cooperative chunks | guest tokenizes N lines per dispatch from `Session` checkpoints | about 256 Rust or 64 TypeScript lines per 16 ms dispatch (wasip3); whole 100k lines is 3.1-8.3 s of UI-thread time spread across many frames; guest +2.1 MB raw (min) or +10 MB (full) | needs a yielding point between chunks (no such import in `capabilities.wit` today); no parallelism; mid-file first view needs a prefix pass |
| D. In-guest, Pierre WAT lexers transpiled or re-hosted | guest embeds the lexers (wasm interpreter or wasm-to-Rust AOT of the WAT) | unmeasured (unverified) | engineering unknown; interpreter would be 10-50x slower than the 22 ms figure (assumption, unmeasured) |
| G. A highlighter *micro* (separate guest instance) that publishes a `highlight` call | irosashi (min or full) compiled as a guest provider; consumers call it through the capability bank (async, bytes in/out) | same engine numbers as C inside the provider; the provider may `await` between chunks, so the consumer's dispatches never block | does a provider instance run off the UI thread on desktop/Android, and does it on web (same thread, almost certainly): unverified; Pierre's WAT lexers cannot be a guest (core module), so G is TextMate/Rust only |
| E. Precomputed tokens with the data | token records computed where the file lives (node/build step) and stored beside it | zero client CPU | couples review data to a highlighter+theme version; fine for immutable commits, wrong for live editing; Pierre's own windowing would still apply to display |
| F. Web Worker pool in `dayframe_web` hosting A or B | host-managed workers lex off the UI thread | removes the UI-thread cost entirely; needs measured postMessage cost (unmeasured) | ADR 015 rejected Web-Worker *guests*, not host workers; native Blitz needs its own implementation (threads exist natively) |

Not measured: Chromium (only V8-in-Node), Android, a Worker pool, `postMessage` transfer cost, a wasm interpreter inside a guest, host-side
checkpointing of Pierre's `LiveTokenizer`, `tree-sitter` incremental edits.

### Findings and pins

1. irosashi 0.3.0 theme tie-break (above): one-line patch, 100 % visible parity afterwards. Pinned in
   `bench/highlight/irosashi-theme-tiebreak.patch`; upstream report not filed.
2. `arborium` (tree-sitter) on `wasm32-wasip3`: with `CC_wasm32_wasip3=clang` the build stops because there is no wasi sysroot (`stdio.h`
   not found). Forcing `--target=wasm32-unknown-unknown` for the C objects plus a local patch of `arborium-tree-sitter/binding_rust/build.rs`
   (`starts_with("wasm32-unknown")` to `"wasm32"`) gets the C compiled, then `wasm-component-ld` fails: `object file uses globals for thread
   context, but --cooperative-threading was specified` (every grammar `parser.o`/`scanner.o`). So C-based tree-sitter cannot be linked into a
   guest until clang emits the component-model thread-context ABI; wasm32-unknown-unknown (the web *host*) builds. The nix dev shell exports
   `CC=gcc`: the `cc` crate then builds C as x86 objects for wasm targets; `build-matrix.sh` sets `CC_wasm32_unknown_unknown=clang`.
3. Pierre's `highlights` ids in `languages.js` are generated and order-dependent (`ARCHITECTURE.md`); pin the package version together with
   any host that stores token ids.
4. The 100k-line corpus is exactly Pierre's own cutoff (`isFileMassive`): it is the largest file Pierre 1.5.2 would ever highlight, so the 100k rows are the worst case it accepts.

### Reproduce

```
deno run --allow-read --allow-write --allow-env bench/highlight/make-corpus.ts $CORPUS     # hashes: bench/highlight/corpus-MANIFEST.txt
(cd bench/highlight && npm install --ignore-scripts)                                          # shiki 4.4.1, @pierre/highlights 0.4.0, @pierre/theme 2.0.0
bench/highlight/build-matrix.sh $BIN irosashi irosashi-min syntect arborium                   # native + wasip3 + web per engine
./x/agent-build.ts -- cargo build --release --manifest-path bench/highlight/pierre-wasmtime/Cargo.toml
(cd bench/highlight && ./run-matrix.sh $BIN $CORPUS $OUT irosashi irosashi-min syntect arborium && ./run-node-matrix.sh $CORPUS $DUMP $OUT/node-matrix.jsonl)
node bench/highlight/fidelity.mjs <shiki.tokens.json> <candidate.tokens.json>
node bench/highlight/table.mjs $OUT
```

## H1 highlights component

Lane: wrap Pierre's unmodified `highlights.wasm` (@pierre/highlights 0.4.0, sha256 2fca61b6...1290) as a component
and compose it into a guest micro. Branch `spike/pierre-highlight`. STATUS: done except the Chromium/Playwright run (see Q1 resume).

### What was built

- `bench/highlight/component/wit/highlights.wit`: `highlight: func(text: string, lang: u8) -> list<u32>` (mode 3
  records), plus `ident` (`is-id-start/continue: func(c: char) -> bool`). Language is the numeric id (Rust = 120), not
  a name: the name table lives in Pierre's `languages.js`, not in the wasm, so a string would mean generating a lookup
  into the adapter. Design question below.
- `wrapper.wat.in` + `build.ts`: component WAT = three core modules. `$Pierre` is `wasm-tools print` of the input
  embedded as text (WAT components have no binary-module form); `wasm-tools print|parse` round-trips it byte-identical
  and `build.ts` asserts the output contains the input bytes verbatim. `$Table` owns the two `env.is_id_*` functions
  (call_indirect through a 2-slot table), `$Glue` fills the table with functions that decode the code point from
  Pierre's memory and call the lowered `ident` import. Core instances cannot import their own exports, hence the table.
  `$Glue` also exports `cabi_realloc`, which returns Pierre's input address (81920) after growing memory, and
  `highlight`, which writes the control block, calls Pierre, and puts the (ptr, len) result pair just past the records.
  The canonical ABI lifts `text` straight into Pierre's memory (`memory` option = Pierre's) and lowers the records
  from Pierre's output buffer: the text is copied once and the records once.
- `ident/`: tiny Rust cdylib over `unicode-ident` (same tables as the P3 replay), built for wasm32-unknown-unknown and
  `wasm-tools component new`. wasm32-wasip3 does not work for it: its std emits `context.get`, which a plain wasmtime
  engine rejects ("requires the component model async feature", main.rs component load).
  `wac plug` composes it into the wrapper (`highlighter.component.wasm`, imports nothing).
- `src/micro_highlight_spike`: Dioxus micro (workspace member, wasm32 only) that highlights the committed 1k-line
  snippet (`snippet.rs.txt`, identical to P3 `rust-1k.rs`, sha256 4fa7ec1e...) and renders lines, records, FNV-1a digest of
  the record bytes, token histogram and per-rep guest microseconds. `HIGHLIGHT_SPIKE_CORPUS` / `HIGHLIGHT_SPIKE_REPS`
  env at build time swap the text. `x/dayframe-packages.ts` (test fixtures only) builds the wrapper, builds the
  micro and `wac plug`s them into `highlight-spike.component.wasm` (`DAYFRAME_HIGHLIGHT_SPIKE_COMPONENT`).
- Golden: `snippet.golden.txt`, written by `PIERRE_GOLDEN_OUT=... pierre-wasmtime` from the DIRECT path's records.
  Tests `highlight_spike_records_equal_the_direct_path_golden` in `src/clay_rt_wasmtime/tests.rs` and
  `src/clay_rt_wcmp_wasmi/tests.rs` mount the view and compare. Both unrun (unverified).
- `pierre-wasmtime` takes an optional 5th arg (the component) and compares record bytes.

Commands: `deno run -A bench/highlight/component/build.ts <highlights.wasm> <outdir>` (via `./x/agent-build.ts --`);
`pierre-wasmtime <wasm> 120 <file> <reps> <highlighter.component.wasm>`. Upstream update = new wasm + rerun; the
adapter depends only on the control block layout (lang@0, mode@1, len@2, out ptr@6, out len@10, input 81920).

### Measurements (all from commands run)

Q4 byte identity (host harness, wasmtime 49, component API vs direct core module, full record streams compared):
`identical: true` for rust-1k, rust-10k, rust-100k (same corpus hashes as P3), a Rust snippet with non-ASCII idents, and
ts-100k (`results/h1-component.wasmtime.jsonl` has the Rust rows). `is_id_*` was called 0 times for Rust and
9 times on a small TypeScript file with non-ASCII identifiers (identical there too), so the lowered `ident` path was
exercised only by that small file.

Q2 time, host harness, 21 reps, median excluding rep 0, same process, machine at loadavg 36-43 (P3 ran near 4.5, so
absolute ms are ~3x P3's; trust only the ratio):

| file | direct ms | via wrapper ms | first call direct / wrapper |
|---|---|---|---|
| rust-1k | 0.615 | 0.73 | 0.91 / 1.0 |
| rust-10k | 9.19 | 7.47 | 12.1 / 10.4 |
| rust-100k | 65.25 | 69.1 | 100.4 / 101.8 |

The wrapper adds the two copies (text in, records out) and no measurable cost beyond noise. In-guest times (the micro's
own `Instant`) were not measured (unverified). Note the guest's per-call fuel is 50,000,000 (`FUEL_PER_CALL`,
`src/clay_rt_wasmtime/lib.rs:730`); the 100k run would need `fuel_budget()` raised (unverified whether 1k fits).

Q3 size: highlights.wasm 332,924 B (147,526 gz) -> highlighter component 365,101 B (158,412 gz): +32,177 raw
(ident 30,531 of it, std-linked; glue ~1.6 KB) / +10,886 gz. Debug micro 14,596,460 B (2,875,574 gz) ->
composed 14,966,046 B (3,036,524 gz): +369,586 raw / +160,950 gz. (Debug profile, the packaging default.)

Q1 engines:
- Host wasmtime 49 component API: instantiates and runs (above).
- `wasmtime compile -W component-model-async=y` of the composed micro: succeeds (compile only).
- wasmtime in clay_rt_wasmtime, wasmi, wcmp in Chromium: NOT RUN (unverified). Risks to check first: nested core instances
  and a funcref table imported across modules (wcmp), `memory`+`realloc` canon options pointing at a non-first
  instance's memory, SIMD + bulk memory in the wasmi config.
- Web needs the micro published as a package (manifest sidecar) in x/dayframe-web.ts; no Playwright spec written.

### Findings and options

- Ident cost: the 30 KB `ident` component only exists because Pierre wants Unicode ID classes for non-ASCII. Option:
  an ASCII-only/approximate WAT table (-30 KB, behaviour differs from Pierre on exotic identifiers, would show up as a
  record diff) versus keeping unicode-ident.
- Language by name: option to generate a name->id table from `languages.js` into the adapter (string API, adapter
  changes each upstream update) versus numeric id (current).
- Debug builds of the spike micro used for size; release sizes unverified.
- Strict component WAT syntax in wasm-tools 1.261 needs nested `(core memory $i "x")` forms.

### H1 resume (guest engines)

Decisions applied: unicode-ident kept; `build.ts` now also generates `src/micro_highlight_spike/languages.rs`
(name -> id, parsed as data from `languages.js` beside the wasm, sorted), the micro calls `language_id("rust")`;
the wasm interface stays `lang: u8`.

Q1 results (current wcmp pin a5db353b; not re-pinned):
- wasmtime (clay_rt_wasmtime): `cargo nextest run -p clay_rt_wasmtime highlight_spike` PASS (3.0 s): the composed micro
  validates, instantiates and its rendered records/digest/histogram equal the direct-path golden.
- wasmi via wcmp (clay_rt_wcmp_wasmi): same test PASS (16.0 s, 22.4 s in the second run; mostly loading the 15 MB debug
  micro). Nested core instances, the cross-module funcref table, SIMD and Pierre-memory canon options all worked; no
  error to pin.
- wcmp in Chromium: NOT RUN. Needs the micro published as a package in `x/dayframe-web.ts` plus a shell route to
  mount a `view`; I judged that too large to do with the disk floor. Since wcmp and wasmi share the component
  lowering, the remaining risk is the browser core engine (SIMD is on by default in Chromium). Unverified.
- Fixtures: `./x/agent-build.ts -- deno run --allow-all x/check-dayframe.ts --prepare-only`; I killed it during the
  host-side `emit_manifest` steps (unneeded here) after `DAYFRAME_HIGHLIGHT_SPIKE_COMPONENT` was printed.

Q2 in-guest (micro's own `Instant`, 5 reps, wasmtime in clay_rt_wasmtime, loadavg high):
- rust-1k: not printed (test passes so the view text is not shown); host harness above is the 1k number.
- rust-10k: 10754, 8554, 8008, 7935, 7961 us (median 8.0 ms).
- rust-100k: 84138, 63272, 54460, 55605, 56807 us (median 55.6 ms; P3 direct 22 ms at loadavg 4.5, host harness here 65-69 ms at
  loadavg ~40, so the guest is not measurably slower than the host call).
- FINDING: the host's per-call fuel (`FUEL_PER_CALL = 50_000_000`, `src/clay_rt_wasmtime/lib.rs:730`) is exhausted by the
  10k-line run ("wasm trap: all fuel consumed by WebAssembly", trapped during initialization); 1k fits. The numbers above
  came from a temporary local edit to 50_000_000_000, reverted. So in production a 10k-line file in one
  call would trap: highlighting must be sliced (live/streaming API) or the budget raised.
- wasmi: 1k fits in ~6 s over the load baseline; rust-10k did NOT finish inside nextest's 120 s hard timeout
  (interpreter, 1 rep). Highlighting in a wasmi guest is only practical for small inputs.
