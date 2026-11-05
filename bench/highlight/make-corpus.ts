// Builds the benchmark corpus: for each of Rust/TS/JSON/Markdown, real files from fixed roots,
// sorted by path, concatenated and cut at exactly N lines (1k, 10k, 100k). Files with a line
// longer than MAX_LINE bytes (minified bundles) are skipped so a single pathological line does
// not dominate a per-line tokenizer. Writes <out>/<lang>-<size>.<ext> and <out>/MANIFEST.txt
// with the sha256 of every output so a run can prove which corpus it measured.
//
//   deno run --allow-read --allow-write --allow-env bench/highlight/make-corpus.ts <out-dir>
import { walk } from "jsr:@std/fs@1/walk";
import { encodeHex } from "jsr:@std/encoding@1/hex";

const MAX_LINE = 2000;
const SIZES: Record<string, number> = { "1k": 1_000, "10k": 10_000, "100k": 100_000 };
const home = Deno.env.get("HOME")!;
const repoRoot = new URL("../../", import.meta.url).pathname;
const roots: Record<string, { exts: string[]; dirs: string[]; ext: string; skip?: RegExp }> = {
  rust: { exts: [".rs"], dirs: [`${repoRoot}src`], ext: "rs", skip: /\/(target|node_modules)\// },
  ts: {
    exts: [".ts", ".tsx"],
    dirs: [`${home}/repos/rust/intest/vendor/pierre_repo/packages`],
    ext: "ts",
    skip: /\/node_modules\/|\.d\.ts$/,
  },
  json: { exts: [".json"], dirs: [`${home}/.cargo/registry/src`], ext: "json" },
  md: { exts: [".md"], dirs: [`${home}/.cargo/registry/src`], ext: "md" },
};

const out = Deno.args[0];
if (!out) throw new Error("usage: make-corpus.ts <out-dir>");
await Deno.mkdir(out, { recursive: true });
const manifest: string[] = [];

for (const [lang, spec] of Object.entries(roots)) {
  const paths: string[] = [];
  for (const dir of spec.dirs) {
    for await (const e of walk(dir, { exts: spec.exts, includeDirs: false, skip: spec.skip ? [spec.skip] : undefined })) {
      paths.push(e.path);
    }
  }
  paths.sort();
  const lines: string[] = [];
  const max = SIZES["100k"];
  for (const p of paths) {
    if (lines.length >= max) break;
    const text = await Deno.readTextFile(p).catch(() => undefined);
    if (text === undefined || text.includes("\r") || text.length === 0) continue;
    const ls = text.replace(/\n$/, "").split("\n");
    if (ls.some((l) => l.length > MAX_LINE)) continue;
    // JSON: join documents as array elements so the stream stays one JSON value up to the cut.
    if (lang === "json") {
      if (lines.length === 0) lines.push("[");
      else lines[lines.length - 1] += ",";
      for (const l of ls) lines.push(l);
    } else {
      for (const l of ls) lines.push(l);
    }
  }
  if (lines.length < max) throw new Error(`${lang}: only ${lines.length} lines available`);
  for (const [size, n] of Object.entries(SIZES)) {
    const body = lines.slice(0, n).join("\n") + "\n";
    const file = `${out}/${lang}-${size}.${spec.ext}`;
    await Deno.writeTextFile(file, body);
    const digest = encodeHex(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(body)));
    manifest.push(`${digest}  ${lang}-${size}.${spec.ext}  ${n} lines  ${body.length} bytes`);
  }
}
await Deno.writeTextFile(`${out}/MANIFEST.txt`, manifest.join("\n") + "\n");
console.log(manifest.join("\n"));
