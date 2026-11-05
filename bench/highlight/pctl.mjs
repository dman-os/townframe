// median and p95 over the warm reps (drops rep 0) of every row; prints a compact table.
//   node pctl.mjs <file.jsonl>...
import { readFileSync } from "node:fs";
const q = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.round((s.length - 1) * p))]; };
for (const f of process.argv.slice(2)) for (const l of readFileSync(f, "utf8").trim().split("\n").filter(Boolean)) {
  const r = JSON.parse(l), w = r.ms.slice(1);
  console.log([r.lang ?? r.file, r.size ?? "", `n=${w.length}`, `cold=${r.ms[0]}`, `median=${q(w, 0.5)}`, `p95=${q(w, 0.95)}`, `recKiB=${r.recordBytes ? (r.recordBytes / 1024).toFixed(0) : ""}`, `mem=${r.wasmMemMiB ?? r.peakMiB}MiB`].join("\t"));
}
