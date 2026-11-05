// Regenerates corpus/tokens.json: the first 1000 lines of P3's rust and ts benchmark files,
// tokenized by Pierre's own highlighter (@pierre/highlights, pierre-dark). Each line is
// [[text, colorIndex], ...] with Pierre's raw token runs (no merging).
//   node gen.mjs <highlight-bench-dir> <corpus-dir> > tokens.json
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
const [bench, corpus] = process.argv.slice(2);
const require = createRequire(`${bench}/package.json`);
const hls = await import(require.resolve("@pierre/highlights"));
const { pierreDark } = await import(require.resolve("@pierre/highlights/themes"));
const colors = [];
const out = {};
for (const [lang, file, id] of [["rust", "rust-1k.rs", "rust"], ["ts", "ts-1k.ts", "typescript"]]) {
  const text = readFileSync(`${corpus}/${file}`, "utf8");
  out[lang] = hls.codeToTokens(text, { lang: id, theme: pierreDark }).tokens.map((line) =>
    line.map((t) => {
      const c = `${t.color ?? ""}|${t.fontStyle ?? 0}`;
      let i = colors.indexOf(c);
      if (i < 0) i = colors.push(c) - 1;
      return [t.content, i];
    })
  );
}
console.log(JSON.stringify(out));
