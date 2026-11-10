import { expect, test } from "@playwright/test";
import * as fs from "node:fs";
import { load, stats } from "./lib";
import { isR3, openRung } from "./open";
import { backgroundOf, blankRuns, decodePng, type Rect } from "./x5lib";

// X5 blank frames (docs/scratch/pierre-spikes.md, "X5 blank frames"). For one variant (rung +
// window profile) and each fling cell, a fresh page is flung while a CDP screencast records the
// frames the compositor produced; every frame is classified for horizontal bands of pure
// background, and a per-rAF probe compares the viewport with the rows in the DOM.
// `X5_VARIANTS` (comma list), `X5_ROUND`, `X5_OUT`; ports `LADDER_PORT_A` / `LADDER_PORT_R3`.

const VARIANTS: Record<string, { rung: string; profile: string; workload: string }> = {
  "r0": { rung: "r0", profile: "code-view", workload: "corpus-rust" },
  "r3": { rung: "r3", profile: "code-view", workload: "corpus" },
  "r3a": { rung: "r3a", profile: "code-view", workload: "corpus" },
  "r3-pierre": { rung: "r3", profile: "pierre", workload: "corpus" },
  "r3a-pierre": { rung: "r3a", profile: "pierre", workload: "corpus" },
};
const SELECTED = (process.env.X5_VARIANTS ?? "r0").split(",");
const ROUND = process.env.X5_ROUND ?? "0";
const OUT = process.env.X5_OUT ?? "/run/media/asdf/p3N/tmp/x5/results.jsonl";
const FRAMES_DIR = process.env.X5_FRAMES_DIR;
const BASE = `http://127.0.0.1:${process.env.LADDER_PORT_A ?? "4711"}`;
const BASE_R3 = `http://127.0.0.1:${process.env.LADDER_PORT_R3 ?? "4712"}`;
const START_TOP = 3000;
const DURATION_MS = Number(process.env.X5_DURATION_MS ?? "3000");
const ROW = 20;
const VIEWPORT = { width: Number(process.env.X5_WIDTH ?? "1280"), height: 1000 };
// Pixels on the right of the scroller that are not content (scrollbar), excluded from the bands.
const SCROLLBAR = 24;
const WHEEL_INTERVAL_MS = 8;
// Control for the detector: `X5_STALL_MS=n` blocks the page's main thread for n ms one second into
// a wheel fling. The queued wheel events are then applied in a burst by the compositor, which
// can outrun the rows in the DOM until the main thread catches up.
const STALL_MS = Number(process.env.X5_STALL_MS ?? "0");

type Cell = { mode: "wheel" | "script"; speed: number };
const CELLS: Cell[] = [
  { mode: "wheel", speed: 1000 },
  { mode: "wheel", speed: 3000 },
  { mode: "wheel", speed: 8000 },
  { mode: "wheel", speed: 16000 },
  { mode: "script", speed: 3000 },
  { mode: "script", speed: 8000 },
];

for (const name of SELECTED) {
  const variant = VARIANTS[name];
  if (!variant) throw new Error(`unknown X5 variant ${name}`);
  test.describe.configure({ timeout: 30 * 60_000 });
  for (const cell of CELLS) {
    test(`x5 ${name} ${cell.mode}-${cell.speed} r${ROUND}`, async ({ browser }) => {
      const loadStart = load();
      const { page, failures } = await openRung(browser, {
        rung: variant.rung,
        workload: variant.workload,
        base: BASE,
        baseR3: BASE_R3,
        profile: variant.profile,
        startTop: START_TOP,
        viewport: VIEWPORT,
      });
      const r3 = isR3(variant.rung);

      // Page-side probe: one rAF loop that (script mode) applies the fling and samples the
      // viewport against the rendered rows, plus a MutationObserver counting added rows.
      await page.evaluate(([r3, rowPx]) => {
        const L = (window as any).Ladder;
        const sc = L.rung.scroller as HTMLElement;
        const rows = (): ArrayLike<Element> =>
          r3
            ? document.querySelector('[data-testid="window"]')!.children
            : L.rung.censusShadow().querySelectorAll("[data-line]");
        const observed: Node = r3 ? document.querySelector('[data-testid="window"]')! : L.rung.censusShadow();
        const x5: any = (window as any).__x5 = { samples: [], commits: [], running: false, fling: null };
        // R3: a row is a child of the window element (a removed node no longer has a parent, so the
        // record's target is what says where it was). R0: a `data-line` element anywhere below.
        const countRows = (nodes: NodeList, target: Node) => {
          let n = 0;
          for (const node of nodes) {
            if (node.nodeType !== 1) continue;
            if (r3) n += target === observed ? 1 : 0;
            else n += ((node as Element).hasAttribute("data-line") ? 1 : 0) + (node as Element).querySelectorAll("[data-line]").length;
          }
          return n;
        };
        new MutationObserver((list) => {
          if (!x5.running) return;
          let added = 0, removed = 0;
          for (const rec of list) {
            added += countRows(rec.addedNodes, rec.target);
            removed += countRows(rec.removedNodes, rec.target);
          }
          if (added || removed) x5.commits.push({ t: performance.now(), added, removed });
        }).observe(observed, { subtree: true, childList: true });
        const sample = (now: number) => {
          const rs = rows();
          const sr = sc.getBoundingClientRect();
          const top = sr.top + sc.clientTop;
          const bottom = top + sc.clientHeight;
          const first = rs[0].getBoundingClientRect();
          const last = rs[rs.length - 1].getBoundingClientRect();
          // above / below: px of rendered rows beyond the viewport edge (negative = uncovered).
          x5.samples.push([now, sc.scrollTop, top - first.top, last.bottom - bottom, rs.length]);
        };
        x5.start = () => {
          x5.running = true;
          const t0 = performance.now();
          const s0 = sc.scrollTop;
          let prev = t0;
          const tick = (now: number) => {
            if (!x5.running) return;
            const f = x5.fling;
            if (f) sc.scrollTop = s0 + (f.speed * (now - f.t0)) / 1000;
            sample(now);
            prev = now;
            requestAnimationFrame(tick);
          };
          requestAnimationFrame(tick);
        };
        x5.stop = () => {
          x5.running = false;
          return { samples: x5.samples, commits: x5.commits };
        };
        void rowPx;
      }, [r3, ROW] as const);

      const rect = await page.evaluate(() => {
        const sc = (window as any).Ladder.rung.scroller as HTMLElement;
        const r = sc.getBoundingClientRect();
        return { x: r.x + sc.clientLeft, y: r.y + sc.clientTop, w: sc.clientWidth, h: sc.clientHeight };
      });
      const crop: Rect = {
        x0: Math.ceil(rect.x),
        x1: Math.floor(rect.x + rect.w) - SCROLLBAR,
        y0: Math.ceil(rect.y),
        y1: Math.floor(rect.y + rect.h),
      };

      const cdp = await page.context().newCDPSession(page);
      const frames: { ts: number; data: Buffer }[] = [];
      cdp.on("Page.screencastFrame", (e: any) => {
        frames.push({ ts: e.metadata.timestamp, data: Buffer.from(e.data, "base64") });
        cdp.send("Page.screencastFrameAck", { sessionId: e.sessionId }).catch((err: Error) => failures.push(String(err)));
      });
      await cdp.send("Page.startScreencast", { format: "png", everyNthFrame: 1 });
      // The rest frame: wait until the screencast has delivered one.
      for (let i = 0; i < 100 && frames.length === 0; i++) await page.waitForTimeout(50);
      expect(frames.length).toBeGreaterThan(0);
      const restCount = frames.length;

      await page.mouse.move(rect.x + rect.w / 2, rect.y + rect.h / 2);
      await page.evaluate(() => (window as any).__x5.start());
      const flingStart = Date.now() / 1000;
      let wheelEvents = 0, wheelSent = 0;
      let stall: Promise<void> | undefined;
      if (cell.mode === "wheel") {
        const t0 = performance.now();
        for (;;) {
          const elapsed = performance.now() - t0;
          if (elapsed >= DURATION_MS) break;
          if (STALL_MS > 0 && !stall && elapsed >= 1000) {
            stall = page.evaluate((ms) => { const t = performance.now(); while (performance.now() - t < ms); }, STALL_MS);
          }
          const delta = Math.round((cell.speed * elapsed) / 1000) - wheelSent;
          if (delta > 0) {
            await page.mouse.wheel(0, delta);
            wheelSent += delta;
            wheelEvents++;
          }
          await page.waitForTimeout(WHEEL_INTERVAL_MS);
        }
      } else {
        await page.evaluate(([speed]) => { (window as any).__x5.fling = { speed, t0: performance.now() }; }, [cell.speed]);
        await page.waitForTimeout(DURATION_MS);
      }
      await stall;
      const flingEnd = Date.now() / 1000;
      // Let the last frames land, then stop.
      await page.waitForTimeout(400);
      const probe = await page.evaluate(() => (window as any).__x5.stop());
      await cdp.send("Page.stopScreencast");
      expect(failures).toEqual([]);

      // Classify (after the run: decoding costs nothing the page can see).
      const rest = decodePng(frames[restCount - 1].data);
      const bg = backgroundOf(rest, crop);
      const restRuns = blankRuns(rest, crop, bg);
      const during = frames.slice(restCount).filter((f) => f.ts >= flingStart - 0.05 && f.ts <= flingEnd + 0.3);
      const classified = during.map((f) => ({ ts: f.ts, ...blankRuns(decodePng(f.data), crop, bg) }));
      const blank = classified.map((c) => c.maxRun >= ROW);
      const episodes: { frames: number; ms: number; maxPx: number }[] = [];
      for (let i = 0; i < classified.length; i++) {
        if (!blank[i]) continue;
        let j = i, maxPx = 0;
        while (j < classified.length && blank[j]) maxPx = Math.max(maxPx, classified[j++].maxRun);
        // From the first blank frame to the first clean one after it (or the last frame).
        const end = classified[Math.min(j, classified.length - 1)].ts;
        episodes.push({ frames: j - i, ms: Math.round((end - classified[i].ts) * 10000) / 10, maxPx });
        i = j;
      }
      const gaps = classified.slice(1).map((c, i) => (c.ts - classified[i].ts) * 1000);

      // Probe summary: samples are [t, scrollTop, aboveRowsPx, belowRowsPx, rowCount].
      const samples = probe.samples as number[][];
      const uncovered = samples.map((s) => Math.min(rect.h, Math.max(0, -s[2]) + Math.max(0, -s[3])));
      const flung = samples.filter((s) => s[0] <= samples[0][0] + DURATION_MS);
      const travel = flung.length > 1 ? flung[flung.length - 1][1] - flung[0][1] : 0;
      const span = flung.length > 1 ? (flung[flung.length - 1][0] - flung[0][0]) / 1000 : 1;
      const commits = probe.commits as { t: number; added: number; removed: number }[];
      const record = {
        variant: name,
        rung: variant.rung,
        profile: variant.profile,
        workload: variant.workload,
        mode: cell.mode,
        speed: cell.speed,
        round: ROUND,
        durationMs: DURATION_MS,
        stallMs: STALL_MS,
        crop,
        wheelEvents,
        wheelSentPx: wheelSent,
        scrollTravelPx: travel,
        scrollPxPerSecMeasured: Math.round(travel / span),
        framesTotal: classified.length,
        framesPerSecond: Math.round((classified.length / ((flingEnd - flingStart) + 0.3)) * 10) / 10,
        blankFrames: blank.filter(Boolean).length,
        maxBlankPx: Math.max(0, ...classified.map((c) => c.maxRun)),
        restMaxRunPx: restRuns.maxRun,
        episodes,
        frameGapMs: stats(gaps),
        probe: {
          rafSamples: samples.length,
          rafUncoveredSamples: uncovered.filter((u) => u > 0).length,
          rafMaxUncoveredPx: Math.max(0, ...uncovered),
          rafSumUncoveredPx: uncovered.reduce((a, b) => a + b, 0),
          aboveRowsPx: stats(samples.map((s) => s[2])),
          belowRowsPx: stats(samples.map((s) => s[3])),
          aboveRowsPxMin: Math.min(...samples.map((s) => s[2])),
          belowRowsPxMin: Math.min(...samples.map((s) => s[3])),
          aboveRowsPxRest: samples[0]?.[2],
          belowRowsPxRest: samples[0]?.[3],
          rowCount: stats(samples.map((s) => s[4])),
          rowCountFirst: samples[0]?.[4],
        },
        rowsAdded: {
          commits: commits.length,
          addedPerCommit: stats(commits.map((c) => c.added)),
          removedPerCommit: stats(commits.map((c) => c.removed)),
          addedTotal: commits.reduce((a, c) => a + c.added, 0),
        },
        loadStart,
        loadEnd: load(),
      };
      fs.appendFileSync(OUT, JSON.stringify(record) + "\n");
      console.log(`X5 ${name} ${cell.mode}-${cell.speed} r${ROUND} frames=${record.framesTotal} blank=${record.blankFrames} maxPx=${record.maxBlankPx} restRun=${record.restMaxRunPx} rafUncovered=${record.probe.rafUncoveredSamples}/${samples.length} v=${record.scrollPxPerSecMeasured}`);
      if (FRAMES_DIR && record.blankFrames > 0) {
        fs.mkdirSync(FRAMES_DIR, { recursive: true });
        const worst = classified.reduce((a, c, i) => (c.maxRun > classified[a].maxRun ? i : a), 0);
        fs.writeFileSync(`${FRAMES_DIR}/${name}-${cell.mode}-${cell.speed}-r${ROUND}.png`, during[worst].data);
      }
      await page.context().close();
    });
  }
}
