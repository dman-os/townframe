// R0: real @pierre/diffs 1.6.4 (`Virtualizer` + `VirtualizedFile`, the file viewer's own
// windowing) rendering `rust-1k.rs` repeated to 8000 lines, pierre-dark, main-thread Shiki
// highlighting (no worker pool), default options except `disableFileHeader` (the other rungs
// have no header row). The scroller / content pair is the shape `react/Virtualizer.js` builds.
import { VirtualizedFile, Virtualizer } from "@pierre/diffs";

const q = new URLSearchParams(location.search);
const scroller = document.getElementById("scroller");
const content = document.getElementById("content");

const source = await (await fetch("../corpus/rust-1k.rs")).text();
const lines = source.split("\n");
if (lines[lines.length - 1] === "") lines.pop();
const total = Number(q.get("lines") ?? 8000);
const contents = Array.from({ length: total }, (_, i) => lines[i % lines.length]).join("\n") + "\n";

const virtualizer = new Virtualizer();
virtualizer.setup(scroller);
const file = new VirtualizedFile(
  { theme: "pierre-dark", themeType: "dark", disableFileHeader: true },
  virtualizer,
);

// Timers around the render entry points, for `Ladder.rung.timers`.
const renders = [];
const originalRender = file.render.bind(file);
file.render = (props) => {
  const t0 = performance.now();
  const r = originalRender(props);
  renders.push({ t0, ms: performance.now() - t0 });
  return r;
};
file.render({ file: { name: "bench.rs", contents }, containerWrapper: content });

const shadow = () => content.querySelector("diffs-container")?.shadowRoot ?? null;
const firstLine = () => {
  const root = shadow();
  if (!root) return null;
  const el = root.querySelector("[data-line]");
  return el ? Number(el.getAttribute("data-line")) - 1 : null;
};

window.__r0 = { virtualizer, file, shadow, firstLine, renders };
window.Ladder.rung = {
  name: "r0",
  scroller,
  windowStart: () => firstLine() ?? -1,
  censusShadow: () => shadow(),
  mutationRootShadow: () => shadow(),
  timers: (from) => renders.filter((r) => r.t0 >= from).map((r) => r.ms),
};
// Ready once the first window is in the DOM (highlighting is async).
const wait = async () => {
  for (let i = 0; i < 600; i++) {
    if (firstLine() !== null) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error("pierre never rendered a line");
};
await wait();
window.__ready = true;
