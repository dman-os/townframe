// X4: what a stateless, whole-batch ingress check costs in V8, measured over the batches the browser
// run captured (window.__clayBatches, direct.js's flat op form). It mirrors the cheap half of
// `clay_surface::SurfaceTree`'s validation: the tag / attribute / listener vocabulary (read from
// render.rs), stack arity of every op, string and batch budgets. It does NOT do the structural half
// (cycles, tree depth, attachment targets), which needs the retained tree or DOM walks.
//   deno run --allow-read bench/ladder/validate-cost.mjs <batches.json> [<batches.json>...]
const render = await Deno.readTextFile(new URL("../../src/clay_surface/render.rs", import.meta.url));
const quoted = (s) => [...s.matchAll(/"([^"]+)"/g)].map((m) => m[1]);
const vocab = render.slice(render.indexOf("vocabulary!("), render.indexOf("macro_rules! expand_surface_event_listener"));
const attrBlocks = vocab.split("=> [").slice(1).map((b) => new Set(quoted(b.slice(0, b.indexOf("]")))));
const attrNone = attrBlocks[0], attrStyle = attrBlocks[1], attrXlink = attrBlocks[2];
const tagSrc = render.slice(render.indexOf("surface_tags!(\n"), render.indexOf("#[cfg(test)]"));
const tagBlocks = tagSrc.split("=> [").slice(1).map((b) => new Set(b.slice(0, b.indexOf("]")).split(/[\s,]+/).filter(Boolean)));
const tagsHtml = tagBlocks[0], tagsSvg = tagBlocks[1];
const listeners = new Set(["click", "scroll", "mounted", "mousedown", "mouseup", "dblclick", "wheel", "keydown", "keyup", "input", "change", "focus", "blur"]);
const SVG = "http://www.w3.org/2000/svg";
const MAX_STRING = 64 * 1024, MAX_BYTES = 16 * 1024 * 1024, MAX_OPS = 128 * 1024, MAX_STACK = 256;

function validate(ops, s) {
  if (ops.length > MAX_OPS * 4) throw new Error("batch ops");
  let i = 0, depth = 1, bytes = 0;
  const str = () => { const o = ops[i++], l = ops[i++]; if (l > MAX_STRING) throw new Error("string"); bytes += l; if (bytes > MAX_BYTES) throw new Error("bytes"); return s.substring(o, o + l); };
  const optStr = () => { const o = ops[i++], l = ops[i++]; return l === 0xffffffff ? null : (bytes += l, s.substring(o, o + l)); };
  while (i < ops.length) {
    switch (ops[i++]) {
      case 0: i++; depth++; break;
      case 1: i++; break;
      case 2: i++; break;
      case 3: if (--depth < 1) throw new Error("pop"); break;
      case 4: { const tag = str(), ns = optStr(); if (!(ns === null ? tagsHtml : ns === SVG ? tagsSvg : null)?.has(tag)) throw new Error("tag " + tag); depth++; break; }
      case 5: str(); depth++; break;
      case 6: break;
      case 7: { const m = ops[i++]; if (m >= depth) throw new Error("arity"); depth -= m; break; }
      case 8: { const m = ops[i++]; if (m + 1 > depth - 1) throw new Error("arity"); depth -= m + 1; break; }
      case 9: case 10: { const m = ops[i++]; if (m >= depth) throw new Error("arity"); depth -= m; break; }
      case 11: { const name = str(), ns = optStr(), kind = ops[i++]; if (kind === 0) str(); const set = ns === null ? attrNone : ns === "style" ? attrStyle : attrXlink; if (!set.has(name)) throw new Error("attr " + name); break; }
      case 12: str(); break;
      case 13: case 14: { const name = str(); if (!listeners.has(name)) throw new Error("listener " + name); break; }
      case 15: depth--; break;
      default: throw new Error("op");
    }
    if (depth > MAX_STACK) throw new Error("stack");
  }
  if (depth !== 1) throw new Error("unbalanced " + depth);
}

for (const path of Deno.args) {
  const { mark, batches } = JSON.parse(await Deno.readTextFile(path));
  const commits = batches.slice(mark);
  const times = [];
  for (let rep = 0; rep < 20; rep++) {
    for (const [ops, s] of commits) {
      const t0 = performance.now();
      validate(ops, s);
      times.push(performance.now() - t0);
    }
  }
  const warm = times.slice(times.length / 2).sort((a, b) => a - b);
  console.log(`${path.split("/").pop()}: ops/commit ${commits[0][0].length} (u32), validate median ${warm[warm.length >> 1].toFixed(3)} ms, p95 ${warm[Math.floor(warm.length * 0.95)].toFixed(3)} ms (n=${warm.length}, second half of 20 reps, Deno ${Deno.version.deno} / V8 ${Deno.version.v8})`);
}
