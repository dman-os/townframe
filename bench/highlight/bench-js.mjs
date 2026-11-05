// JS-side tokenizer benchmark and oracle dump.
//   node bench-js.mjs <engine> <corpus-dir> <lang> <size> <reps> [dump-dir]
// engine: shiki-onig | shiki-js | highlights (end to end codeToTokens: wasm lex + JS token objects)
// Prints one JSON line: wall ms per rep (all reps, first is cold), token count, rss/heap deltas.
// With dump-dir, also writes <engine>.<lang>-<size>.tokens.json: per line [[text,"#rrggbb"|null,fontStyle],...]
// from the LAST rep, so fidelity.mjs can diff engines on identical input.
import { readFileSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";

const [engine, corpus, lang, size, repsArg, dumpDir] = process.argv.slice(2);
const reps = Number(repsArg);
const ext = { rust: "rs", ts: "ts", json: "json", md: "md" }[lang];
const shikiLang = { rust: "rust", ts: "typescript", json: "json", md: "markdown" }[lang];
const text = readFileSync(`${corpus}/${lang}-${size}.${ext}`, "utf8");

// Shiki loads embedded grammars only if they are registered; markdown fences need these.
const EMBEDDED = ["rust", "typescript", "tsx", "javascript", "json", "jsonc", "bash", "shellscript", "toml",
  "yaml", "markdown", "python", "c", "cpp", "html", "css", "diff", "sql", "ini", "kotlin", "xml", "nix"];

const baseEngine = engine;
const rss0 = process.memoryUsage().rss;
let run;
if (baseEngine.startsWith("shiki")) {
  const { createHighlighter } = await import("shiki");
  const themeJson = JSON.parse(readFileSync("node_modules/@pierre/theme/themes/pierre-dark.json", "utf8"));
  const eng = baseEngine === "shiki-js"
    ? (await import("shiki/engine/javascript")).createJavaScriptRegexEngine()
    : (await import("shiki/engine/oniguruma")).createOnigurumaEngine(import("shiki/wasm"));
  const hl = await createHighlighter({ themes: [themeJson], langs: [...new Set([shikiLang, ...EMBEDDED])], engine: eng });
  run = () => hl.codeToTokensBase(text, { lang: shikiLang, theme: "pierre-dark", tokenizeTimeLimit: 0 });
} else {
  const hls = await import("@pierre/highlights");
  const { pierreDark } = await import("@pierre/highlights/themes");
  const hlsLang = { rust: "rust", ts: "typescript", json: "json", md: "markdown" }[lang];
  if (baseEngine !== "highlights") throw new Error(`unknown engine ${engine}`);
  run = () => hls.codeToTokens(text, { lang: hlsLang, theme: pierreDark }).tokens;
}
const times = [];
let out;
for (let i = 0; i < reps; i++) {
  const t0 = performance.now();
  out = run();
  times.push(performance.now() - t0);
}
const mem = process.memoryUsage();
const lines = out.length;
let tokens = 0;
for (const l of out) tokens += l.length;
console.log(JSON.stringify({
  engine, lang, size, lines, tokens, bytes: Buffer.byteLength(text),
  ms: times.map((t) => +t.toFixed(2)),
  rssDeltaMiB: +((mem.rss - rss0) / 1048576).toFixed(1), heapMiB: +(mem.heapUsed / 1048576).toFixed(1),
}));
if (dumpDir) {
  const norm = out.map((l) => l.map((t) => [t.content, t.color?.toLowerCase() ?? null, t.fontStyle ?? 0]));
  writeFileSync(`${dumpDir}/${engine}.${lang}-${size}.tokens.json`, JSON.stringify(norm));
}
