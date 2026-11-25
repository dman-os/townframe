// node measure.mjs <engine> <url> <label> <window|root> [readySelector]
import { createRequire } from "node:module";
const pw = createRequire(`${process.env.PLAYWRIGHT_CORE}/`)(process.env.PLAYWRIGHT_CORE);
const [, , engine, url, label, scroller] = process.argv;
const browser = await pw[engine].launch();
const page = await browser.newPage({ viewport: { width: 1280, height: Number(process.env.VH ?? 800) } });
page.on("pageerror", (e) => console.log("pageerror", String(e).slice(0, 200)));
if (process.env.THROTTLE && engine === "chromium") { const c = await page.context().newCDPSession(page); await c.send("Emulation.setCPUThrottlingRate", { rate: Number(process.env.THROTTLE) }); }
await page.goto(url);
// wait until rows exist in some shadow root
await page.waitForFunction(() => {
  const w = (n) => { for (const e of n.querySelectorAll("*")) { if (e.shadowRoot && e.shadowRoot.querySelector("[data-line]")) return true; } return false; };
  return w(document);
}, null, { timeout: 180000 });
await page.evaluate((scroller) => {
  const root = scroller === "root" ? document.getElementById("root") : null;
  const top = () => (root ? root.scrollTop : window.scrollY);
  // Row geometry, not hit testing: both viewers suspend pointer events while scrolling, which
  // makes elementFromPoint see only the scroller. Rendered rows are the elements with
  // `data-line` (or a separator) in the document and in every shadow root.
  const rowRects = () => {
    const rects = [];
    const visit = (scope) => {
      for (const e of scope.querySelectorAll("[data-line], [data-separator-wrapper]")) {
        const r = e.getBoundingClientRect(); if (r.height > 0) rects.push([r.top, r.bottom]);
      }
      for (const h of scope.querySelectorAll("diffs-container, [data-dioxus-shadow]")) if (h.shadowRoot) visit(h.shadowRoot);
    };
    visit(document);
    return rects;
  };
  const covered = (y, rects) => rects.some(([t, b]) => t <= y && y <= b);
  const m = (window.__m = { frames: [], scrolls: 0, scrollTimes: [], muts: [] });
  // DOM edits, in the document and in every shadow root: when a batch lands.
  const seen = new WeakSet();
  const mo = new MutationObserver((list) => { m.muts.push([performance.now(), list.length]); });
  const watch = (scope) => {
    if (!seen.has(scope)) { seen.add(scope); mo.observe(scope, { subtree: true, childList: true, attributes: true, characterData: true }); }
    for (const h of scope.querySelectorAll("diffs-container, [data-dioxus-shadow]")) if (h.shadowRoot) watch(h.shadowRoot);
  };
  m.extent = () => { const r = rowRects(); return r.length ? [Math.round(Math.max(...r.map((x) => x[1])) - innerHeight), Math.round(-Math.min(...r.map((x) => x[0])))] : null; };
  m.long = [];
  try { new PerformanceObserver((l) => { for (const e of l.getEntries()) m.long.push([e.startTime, e.duration]); }).observe({ entryTypes: ["longtask"] }); } catch {}
  m.watch = () => watch(root ?? document.body);
  (root ?? window).addEventListener("scroll", () => { m.scrolls++; m.scrollTimes.push(performance.now()); }, { passive: true });
  const mc = new MessageChannel();
  let n = 0, lastScrolls = 0;
  mc.port1.onmessage = (e) => {
    const rects = rowRects();
    m.frames[e.data].post = { t: performance.now(), top: top(), bottom: covered(innerHeight - 3, rects), edge: covered(innerHeight - 40, rects), topc: covered(3, rects), rows: rects.length, last: rects.length ? Math.max(...rects.map((r) => r[1])) : null, first: rects.length ? Math.min(...rects.map((r) => r[0])) : null, vh: innerHeight };
  };
  const tick = () => {
    const i = n++;
    m.watch();
    m.frames.push({ pre: { t: performance.now(), top: top(), bottom: covered(innerHeight - 3, rowRects()) }, events: m.scrolls - lastScrolls });
    lastScrolls = m.scrolls;
    mc.port2.postMessage(i);
    requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
}, scroller);
await page.mouse.move(640, 400);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function phase(name, steps, dy, gap) {
  const start = await page.evaluate(() => window.__m.frames.length);
  for (let i = 0; i < steps; i++) { await page.mouse.wheel(0, dy); await sleep(gap); }
  const settle = [];
  for (let i = 0; i < 12; i++) { await sleep(100); settle.push((await page.evaluate(() => window.__m.extent()))?.join("/")); }
  const end = await page.evaluate(() => window.__m.frames.length);
  return { name, start, end, settle };
}
const phases = [await phase("steady", 80, 120, 16), await phase("burst", 50, 700, 8), await phase("steady-slow", 60, 60, 16)];
{ const start = await page.evaluate(() => window.__m.frames.length); for (let i = 0; i < 40; i++) { await page.mouse.wheel(0, Math.round(900 * Math.exp(-i / 12))); await sleep(8); } await sleep(600); phases.push({ name: "fling-decay", start, end: await page.evaluate(() => window.__m.frames.length) }); }
const frames = await page.evaluate(() => window.__m.frames);
const scrollTimes = await page.evaluate(() => window.__m.scrollTimes);
const muts = await page.evaluate(() => window.__m.muts);
const rafTimes = frames.map((f) => f.pre.t);
const longs = await page.evaluate(() => window.__m.long);
const total = await page.evaluate(() => window.__m.scrolls);
const out = { label, engine, total_scroll_events: total, phases: {} };
for (const p of phases) {
  const fs = frames.slice(p.start, p.end).filter((f) => f.post);
  const moving = fs.filter((f, i) => i > 0 && f.post.top !== fs[i - 1].post.top);
  const unc = moving.filter((f) => !f.post.bottom);
  const uncPre = moving.filter((f) => !f.pre.bottom);
  let run = 0, maxRun = 0;
  for (const f of fs) { if (!f.post.bottom) { run++; maxRun = Math.max(maxRun, run); } else run = 0; }
  const t0 = frames[p.start]?.pre.t ?? 0, t1 = frames[p.end - 1]?.pre.t ?? 0;
  const lags = [], sameFrame = [];
  for (const st of scrollTimes.filter((x) => x >= t0 && x <= t1)) {
    const mu = muts.find((mm) => mm[0] >= st);
    if (!mu) continue;
    lags.push(mu[0] - st);
    // the next rAF callback start after the scroll event
    const k = frames.findIndex((f) => f.pre.t >= st);
    const post = (i) => frames[i]?.post?.t;
    // 0: the edits land before the paint of the first frame after the scroll event; 1: one frame late; 2: later
    const cls = k < 0 || post(k) === undefined ? 3 : mu[0] <= post(k) ? 0 : post(k + 1) !== undefined && mu[0] <= post(k + 1) ? 1 : 2;
    sameFrame.push(cls);
  }
  lags.sort((a, b) => a - b);
  const q = (x) => (lags.length ? +lags[Math.min(lags.length - 1, Math.floor(lags.length * x))].toFixed(1) : null);
  const dts = frames.slice(p.start + 1, p.end).map((f, i) => f.pre.t - frames[p.start + i].pre.t).sort((a, b) => a - b);
  const lt = longs.filter(([st]) => st >= t0 && st <= t1);
  const below = fs.filter((f) => f.post.last !== null).map((f) => f.post.last - f.post.vh).sort((a, b) => a - b);
  const above = fs.filter((f) => f.post.first !== null).map((f) => -f.post.first).sort((a, b) => a - b);
  const qq = (a, x) => (a.length ? Math.round(a[Math.min(a.length - 1, Math.floor(a.length * x))]) : null);
  const lastf = fs.at(-1)?.post;
  out.phases[p.name] = { settle_below_above_every_100ms: p.settle?.join(";"), rest_below_above_rows: lastf ? [Math.round(lastf.last - lastf.vh), Math.round(-lastf.first), lastf.rows].join("/") : null, buffer_below_px_p5_p50: [qq(below, 0.05), qq(below, 0.5)].join("/"), buffer_above_px_p5_p50: [qq(above, 0.05), qq(above, 0.5)].join("/"), frame_dt_p50_p95_max: [0.5, 0.95, 1].map((x) => +dts[Math.min(dts.length - 1, Math.floor(dts.length * x))]?.toFixed(1)).join("/"), long_tasks: lt.length + "x" + Math.round(lt.reduce((a, [, d]) => a + d, 0)) + "ms", edit_lag_ms_p50: q(0.5), edit_lag_ms_p95: q(0.95), edit_lag_ms_max: q(1), landing_frames_0_1_2plus: [0, 1, 2].map((c) => sameFrame.filter((x) => x === c).length).join("/"), frames: fs.length, moving_frames: moving.length, uncovered_bottom_post_paint: unc.length, uncovered_bottom_at_raf_start: uncPre.length, max_consecutive_uncovered: maxRun, edge40_uncovered: moving.filter((f) => !f.post.edge).length, top_uncovered: moving.filter((f) => !f.post.topc).length, scroll_events: fs.reduce((a, f) => a + f.events, 0), total_dy: fs.length ? fs.at(-1).post.top - fs[0].post.top : 0 };
}
console.log(JSON.stringify(out, null, 1));
await browser.close();
