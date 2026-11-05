// Markdown tables of warm-median ms per (lang,size) per engine/target from a results dir.
//   node table.mjs <results-dir>
import { readdirSync, readFileSync } from "node:fs";
const dir = process.argv[2];
const med = (a) => { const s = [...a].sort((x, y) => x - y); return s[s.length >> 1]; };
const cols = []; for (const l of ["rust", "ts", "json", "md"]) for (const s of ["1k", "10k", "100k"]) cols.push(`${l}-${s}`);
const out = ["| engine / target | " + cols.join(" | ") + " |", "|---|" + cols.map(() => "---:").join("|") + "|"];
const memOut = [...out];
for (const f of readdirSync(dir).filter((f) => !f.startsWith("probe") && /\.(native|wasip3|web|node|wasmtime|raw-node)\.jsonl$/.test(f) || f === "node-matrix.jsonl").sort()) {
  const rows = new Map(); const mem = new Map();
  for (const l of readFileSync(`${dir}/${f}`, "utf8").trim().split("\n").filter(Boolean)) {
    const r = JSON.parse(l);
    const lang = r.lang ?? r.file.split("-")[0]; const size = r.size ?? r.file.split("-")[1].split(".")[0];
    const key = (r.engine && f === "node-matrix.jsonl" ? r.engine + ".node-e2e" : f.replace(".jsonl", "")) ;
    const k = `${key}|${lang}-${size}`;
    const warm = r.ms.length > 1 ? med(r.ms.slice(1)) : r.ms[0];
    rows.set(k, warm); mem.set(k, r.peakMiB ?? r.wasmMemMiB ?? r.rssDeltaMiB);
  }
  const keys = [...new Set([...rows.keys()].map((k) => k.split("|")[0]))];
  for (const key of keys) {
    out.push(`| ${key} | ` + cols.map((c) => rows.get(`${key}|${c}`) ?? "").join(" | ") + " |");
    memOut.push(`| ${key} | ` + cols.map((c) => mem.get(`${key}|${c}`) ?? "").join(" | ") + " |");
  }
}
console.log("warm-median wall ms\n\n" + out.join("\n") + "\n\npeak memory MiB (native: VmHWM of the process; wasm: linear memory; node-e2e: RSS delta)\n\n" + memOut.join("\n"));
