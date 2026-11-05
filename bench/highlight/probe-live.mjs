// Step-4 probe: does @pierre/highlights' LiveTokenizer (wasm, own instance) tokenize incrementally,
// inside a small synchronous budget, resumable per window?
//   node probe-live.mjs <corpus-dir> <lang> <size>
// Measures: constructor with a renderRange (1 ms sync budget by the package's contract), how many
// lines it completed synchronously, then the background deferral: every onDeferTokenize delivery is
// timestamped, so batch size (lines) and inter-batch time show the chunking granularity. All
// timings are wall-clock in this Node process; the package schedules batches on its own timers.
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { LiveTokenizer } from "@pierre/highlights";
import { pierreDark } from "@pierre/highlights/themes";

const [corpus, lang, size] = process.argv.slice(2);
const ext = { rust: "rs", ts: "ts", json: "json", md: "md" }[lang];
const hl = { rust: "rust", ts: "typescript", json: "json", md: "markdown" }[lang];
const code = readFileSync(`${corpus}/${lang}-${size}.${ext}`, "utf8");
const nLines = code.split("\n").length;
const mid = Math.floor(nLines / 2);

const batches = [];
let t0 = performance.now();
let last = t0;
const t = new LiveTokenizer({
  lang: hl, theme: pierreDark, code,
  renderRange: [mid, mid + 200],
  onDeferTokenize: (lines) => {
    const now = performance.now();
    const keys = [...lines.keys()];
    batches.push({ atMs: +(now - t0).toFixed(2), sinceLastMs: +(now - last).toFixed(2), lines: lines.size, firstLine: Math.min(...keys), lastLine: Math.max(...keys) });
    last = now;
  },
});
const ctorMs = performance.now() - t0;
const pendingAfterCtor = t.pendingTokenization;
// Drain in the background exactly as an app would: yield to the event loop until done.
const drainStart = performance.now();
while (t.pendingTokenization) await new Promise((r) => setTimeout(r, 0));
const drainMs = performance.now() - drainStart;

// Direct window request after full tokenization, then after an edit near the top (state ripple).
const edit = { range: { start: { line: 2, character: 0 }, end: { line: 2, character: 0 } }, newText: "/* " };
const te = performance.now();
const upd = t.applyEdits([edit], { renderRange: [mid, mid + 200] });
const editSyncMs = performance.now() - te;
const syncLines = upd.lines.size;
const pendingAfterEdit = t.pendingTokenization;
const td = performance.now();
while (t.pendingTokenization) await new Promise((r) => setTimeout(r, 0));
const editDrainMs = performance.now() - td;

const ms = batches.map((b) => b.sinceLastMs).sort((a, b) => a - b);
const lens = batches.map((b) => b.lines).sort((a, b) => a - b);
console.log(JSON.stringify({
  probe: "highlights-live", lang, size, lines: nLines,
  ctorMs: +ctorMs.toFixed(2), ctorPendingAfter: pendingAfterCtor,
  firstBatches: batches.slice(0, 3), deferredBatches: batches.length, batchLines: { p50: lens[lens.length >> 1], max: lens[lens.length - 1] },
  batchIntervalMs: { p50: ms[ms.length >> 1], max: ms[ms.length - 1] }, drainMs: +drainMs.toFixed(1),
  editAtLine3: { syncMs: +editSyncMs.toFixed(2), syncWindowLinesDelivered: syncLines, pendingAfter: pendingAfterEdit, drainMs: +editDrainMs.toFixed(1) },
}));
t.dispose();
