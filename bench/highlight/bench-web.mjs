// wasm32-unknown-unknown candidate in Node/V8 (the web build's engine family) via wasm-bindgen's nodejs glue.
//   node bench-web.mjs <web-engine-dir> <engine> <corpus-dir> <lang> <size> <reps>
// Times `new Hl(engine)` (registry init) and each `tokenize` call with performance.now; linear memory
// comes from the module itself (memory never shrinks, so its size is the high water).
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { createRequire } from "node:module";
const [dir, engine, corpus, lang, size, repsArg] = process.argv.slice(2);
const { Hl } = createRequire(import.meta.url)(`${dir}/highlight_bench.js`);
const ext = { rust: "rs", ts: "ts", json: "json", md: "md" }[lang];
const text = readFileSync(`${corpus}/${lang}-${size}.${ext}`, "utf8");
let t = performance.now();
const hl = new Hl(engine);
const initMs = performance.now() - t;
const ms = [];
let tokens = 0;
for (let i = 0; i < Number(repsArg); i++) {
  t = performance.now();
  tokens = hl.tokenize(lang, text);
  ms.push(+(performance.now() - t).toFixed(2));
}
console.log(JSON.stringify({ engine, target: "wasm32-unknown-unknown/node", lang, size, tokens, initMs: +initMs.toFixed(2), ms, peakMiB: +(Hl.memory_bytes() / 1048576).toFixed(1) }));
