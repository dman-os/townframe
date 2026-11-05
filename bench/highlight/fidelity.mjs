// Compare token dumps from two engines on the same input.
//   node fidelity.mjs <oracle.tokens.json> <candidate.tokens.json>
// Per line checks:
//   strict: identical token arrays (text, color, fontStyle) boundary for boundary
//   runs:   identical after merging adjacent tokens with equal (color, fontStyle): what the eye sees
//   visible: identical (color, fontStyle) for every NON-whitespace character; whitespace carries no
//            foreground so engines that glue it to a neighbour (or split it out) still compare equal
//   chars:   percentage of non-whitespace characters, over the whole file, whose style matches
// A missing colour (null) is the theme foreground (#fafafa), since engines differ in whether they
// emit the foreground explicitly.
import { readFileSync } from "node:fs";
const [oraclePath, candPath, showN = "3"] = process.argv.slice(2);
// syntect tokenizes lines WITH their terminator (LinesWithEndings); every other engine excludes it.
// Strip a trailing line terminator from the last token so the comparison is about style, not framing.
const load = (p) => JSON.parse(readFileSync(p, "utf8")).map((line) => {
  const l = line.map((t) => [...t]);
  const last = l[l.length - 1];
  if (last) { last[0] = last[0].replace(/\r?\n$/, ""); if (last[0] === "") l.pop(); }
  return l;
});
const A = load(oraclePath);
const B = load(candPath);
const fg = "#fafafa"; // pierre-dark editor.foreground: what a token with no explicit colour renders as
const norm = (l) => l.map(([t, c, f]) => [t, c ?? fg, f & 3]); // italic|bold only: all Pierre themes use these
const runs = (l) => {
  const out = [];
  for (const [t, c, f] of norm(l)) {
    const last = out[out.length - 1];
    if (last && last[1] === c && last[2] === f) last[0] += t; else out.push([t, c, f]);
  }
  return out;
};
const eq = (x, y) => JSON.stringify(x) === JSON.stringify(y);
const n = Math.max(A.length, B.length);
const perChar = (l) => {
  const out = [];
  for (const [t, c, f] of norm(l)) for (const ch of t) if (!/\s/u.test(ch)) out.push(`${c}|${f}`);
  return out;
};
let strict = 0, run = 0, vis = 0, textOk = 0, shown = 0, chars = 0, charsOk = 0;
const bad = [];
for (let i = 0; i < n; i++) {
  const a = A[i] ?? [], b = B[i] ?? [];
  if (a.map((t) => t[0]).join("") === b.map((t) => t[0]).join("")) textOk++;
  if (eq(norm(a), norm(b))) strict++;
  const pa = perChar(a), pb = perChar(b);
  const sameVisible = pa.length === pb.length && pa.every((x, k) => x === pb[k]);
  if (sameVisible) vis++;
  chars += pa.length;
  for (let k = 0; k < pa.length; k++) if (pa[k] === pb[k]) charsOk++;
  if (eq(runs(a), runs(b))) run++;
  if (!sameVisible && shown++ < Number(showN)) bad.push({ line: i + 1, oracle: runs(a), cand: runs(b) });
}
console.log(JSON.stringify({ lines: n, textIdenticalPct: +(100 * textOk / n).toFixed(2), strictPct: +(100 * strict / n).toFixed(2), runsPct: +(100 * run / n).toFixed(2), visiblePct: +(100 * vis / n).toFixed(2), charsPct: +(100 * charsOk / chars).toFixed(2), fg }));
if (bad.length) console.error(JSON.stringify(bad, null, 0));
