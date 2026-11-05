// @pierre/highlights raw wasm lex in Node/V8: writeInput + tokenizeLineRecords only (mode 3, UTF-16
// line records), no JS token objects. This is the cost a host pays to produce the wire payload; the
// end-to-end codeToTokens figure in bench-js.mjs adds JS object creation on top.
//   node bench-highlights-raw.mjs <corpus-dir> <lang> <size> <reps>
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import * as hls from "@pierre/highlights";
import languages from "./node_modules/@pierre/highlights/dist/languages.js";
const [corpus, lang, size, repsArg] = process.argv.slice(2);
const ext = { rust: "rs", ts: "ts", json: "json", md: "md" }[lang];
const id = languages[{ rust: "rust", ts: "typescript", json: "json", md: "markdown" }[lang]];
const text = readFileSync(`${corpus}/${lang}-${size}.${ext}`, "utf8");
const h = hls.createHighlighter();
const ms = [];
let recBytes = 0, lines = 0;
for (let i = 0; i < Number(repsArg); i++) {
  const t = performance.now();
  const n = h.writeInput(text);
  const rec = h.tokenizeLineRecords(id, n);
  ms.push(+(performance.now() - t).toFixed(2));
  recBytes = rec.byteLength;
  lines = 0;
  for (let k = 1; k < rec.length; k += 2) if (rec[k] === 0xffffffff) lines++;
}
console.log(JSON.stringify({ engine: "highlights-raw", target: "node/V8", lang, size, lines, recordBytes: recBytes, ms, wasmMemMiB: +(h.memory.buffer.byteLength / 1048576).toFixed(1) }));
