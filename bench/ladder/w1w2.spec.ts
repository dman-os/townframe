import { expect, test } from "@playwright/test";
import * as fs from "node:fs";
import { load } from "./lib";
import { openRung } from "./open";
import { decodePng, type Image } from "./x5lib";

// W1 (event latency anatomy) and W2 (late correction) on the web host
// (docs/scratch/pierre-spikes.md, "W1/W2 micro scroll latency"). `W_MODE=w1|w2|resize`,
// `W_ROUND`, `W_OUT`; the Clay site is served on `LADDER_PORT_R3`.
const MODE = process.env.W_MODE ?? "w1";
const ROUND = process.env.W_ROUND ?? "0";
const OUT = process.env.W_OUT ?? `/run/media/asdf/p3N/tmp/w1w2/${MODE}.jsonl`;
const BASE_R3 = `http://127.0.0.1:${process.env.LADDER_PORT_R3 ?? "4722"}`;
const VIEWPORT = { width: 1280, height: 1000 };
const SCROLLBAR = 24;
const DURATION_MS = 3000;
const LINE = 20;

type Cell = { profile: string; mode: "wheel" | "script"; speed: number };
const W1_CELLS: Cell[] = [
  { profile: "pierre", mode: "script", speed: 3000 },
  { profile: "pierre", mode: "script", speed: 8000 },
  { profile: "pierre", mode: "wheel", speed: 3000 },
  { profile: "code-view", mode: "script", speed: 3000 },
  { profile: "code-view", mode: "wheel", speed: 3000 },
];

async function openMicro(browser: any, profile: string, threads: boolean) {
  const { page, failures } = await openRung(browser, {
    rung: "r3", workload: "corpus", base: "", baseR3: BASE_R3, profile, startTop: 3000, viewport: VIEWPORT,
  });
  page.on("console", (m) => { if (true) console.log(`PAGE ${m.type()}: ${m.text().slice(0, 600)}`); });
  if (threads) {
    await page.getByTestId("threads").click();
    await page.evaluate(async () => {
      const L = (window as any).Ladder;
      L.rung.scroller.scrollTop = 3000;
      for (let i = 0; i < 8; i++) await L.nextFrame();
    });
  }
  return { page, failures };
}

const phasesOf = async (page: any) => {
  await page.getByTestId("phases-dump").click();
  await page.waitForTimeout(50);
  const text = await page.getByTestId("phases").textContent();
  return text ? JSON.parse(text).phases as number[][] : [];
};

if (MODE === "w1") {
  test.describe.configure({ timeout: 10 * 60_000 });
  for (const cell of W1_CELLS) {
    test(`w1 ${cell.profile} ${cell.mode}-${cell.speed} r${ROUND}`, async ({ browser }) => {
      const loadStart = load();
      const { page, failures } = await openMicro(browser, cell.profile, false);
      await page.evaluate(() => {
        const w: any = window;
        const sc = w.Ladder.rung.scroller as HTMLElement;
        const win = document.querySelector('[data-testid="window"]')!;
        const w1: any = w.__w1 = { native: [], frames: [], muts: [], raf: [], running: false, fling: null };
        const ch = new MessageChannel();
        // A task posted from the rAF callback runs after that frame's main-thread work: its
        // arrival time is the frame's end.
        ch.port1.onmessage = () => { if (w1.running) w1.frames.push(performance.now()); };
        sc.addEventListener("scroll", () => { if (w1.running) w1.native.push(performance.now()); }, { capture: true });
        new MutationObserver(() => { if (w1.running) w1.muts.push(performance.now()); })
          .observe(win, { subtree: true, childList: true, attributes: true });
        w1.start = () => {
          w1.running = true;
          const s0 = sc.scrollTop;
          const tick = (now: number) => {
            if (!w1.running) return;
            w1.raf.push(performance.now());
            ch.port2.postMessage(0);
            if (w1.fling) sc.scrollTop = s0 + (w1.fling.speed * (now - w1.fling.t0)) / 1000;
            requestAnimationFrame(tick);
          };
          requestAnimationFrame(tick);
        };
      });
      await phasesOf(page); // drain what the setup dispatched
      const rect = await page.evaluate(() => {
        const r = (window as any).Ladder.rung.scroller.getBoundingClientRect();
        return { x: r.x, y: r.y, w: r.width, h: r.height };
      });
      await page.mouse.move(rect.x + rect.w / 2, rect.y + rect.h / 2);
      await page.evaluate(() => { (window as any).__clayTiming = []; (window as any).__w1.start(); });
      if (cell.mode === "wheel") {
        const t0 = performance.now();
        let sent = 0;
        for (;;) {
          const elapsed = performance.now() - t0;
          if (elapsed >= DURATION_MS) break;
          const delta = Math.round((cell.speed * elapsed) / 1000) - sent;
          if (delta > 0) { await page.mouse.wheel(0, delta); sent += delta; }
          await page.waitForTimeout(8);
        }
      } else {
        await page.evaluate(([speed]) => { (window as any).__w1.fling = { speed, t0: performance.now() }; }, [cell.speed]);
        await page.waitForTimeout(DURATION_MS);
      }
      await page.waitForTimeout(300);
      const raw = await page.evaluate(() => {
        const w1 = (window as any).__w1;
        w1.running = false;
        return { native: w1.native, frames: w1.frames, muts: w1.muts, raf: w1.raf, timing: (window as any).__clayTiming };
      });
      const phases = await phasesOf(page);
      expect(failures).toEqual([]);
      fs.appendFileSync(OUT, JSON.stringify({ ...cell, round: ROUND, loadStart, loadEnd: load(), ...raw, phases }) + "\n");
      console.log(`W1 ${cell.profile} ${cell.mode}-${cell.speed} r${ROUND} native=${raw.native.length} phases=${phases.length} frames=${raw.frames.length}`);
      await page.context().close();
    });
  }
}

type Case = { name: string; anchor: boolean; correct: number; sibling?: boolean };
const W2_CASES: Case[] = [
  { name: "none", anchor: false, correct: 0 },
  { name: "guest-lastknown", anchor: false, correct: 1 },
  { name: "guest-readwrite", anchor: false, correct: 2 },
  { name: "native-anchor", anchor: true, correct: 0 },
  { name: "guest-resize", anchor: false, correct: 3 },
  { name: "native-anchor-sibling-spacer", anchor: true, correct: 0, sibling: true },
];
const W2_N = (process.env.W2_N ?? "0,1,4,12").split(",").map(Number);
const W2_SPEED = Number(process.env.W2_SPEED ?? "3000");

// Where a frame's counter cells and marker are, read from the screencast PNG.
const CELL = 8;
const counterOf = (img: Image): number => {
  let n = 0;
  for (let i = 0; i < 16; i++) {
    const o = (4 * img.width + CELL * i + 4) * img.bpp;
    if (img.rows[o] < 128) n |= 1 << i;
  }
  return n;
};
// Marker band: pure magenta at the left edge of the crop. Returns the top y of the 20 px row
// (extrapolated from the visible edge when clipped by the crop) or null.
const markerOf = (img: Image, x: number, y0: number, y1: number): number | null => {
  let first = -1, last = -1;
  for (let y = y0; y < y1; y++) {
    const o = (y * img.width + x) * img.bpp;
    if (img.rows[o] === 255 && img.rows[o + 1] === 0 && img.rows[o + 2] === 255) {
      if (first < 0) first = y;
      last = y;
    }
  }
  if (first < 0) return null;
  if (first === y0 && last - first + 1 < LINE) return last + 1 - LINE;
  return first;
};

if (MODE === "w2") {
  test.describe.configure({ timeout: 10 * 60_000 });
  for (const n of W2_N) {
    for (const c of W2_CASES) {
      if (n === 0 && !["none", "native-anchor", "native-anchor-sibling-spacer"].includes(c.name)) continue;
      if (c.name === "guest-resize" && n === 0) continue;
      test(`w2 ${c.name} N${n} r${ROUND}`, async ({ browser }) => {
        const loadStart = load();
        const { page, failures } = await openMicro(browser, "pierre", true);
        await page.getByTestId(c.anchor ? "anchor-on" : "anchor-off").click();
        await page.getByTestId(`correct-${c.correct}`).click();
        if (c.correct === 3) await page.getByTestId("resize-probe").click();
        if (c.sibling) await page.getByTestId("spacer-sibling").click();
        await page.evaluate(async () => { for (let i = 0; i < 6; i++) await (window as any).Ladder.nextFrame(); });
        await page.evaluate(([n, speed]) => {
          const w: any = window;
          const sc = w.Ladder.rung.scroller as HTMLElement;
          const bar = document.createElement("div");
          bar.style.cssText = "position:fixed;left:0;top:0;z-index:2147483647;display:flex";
          const cells = Array.from({ length: 16 }, () => {
            const d = document.createElement("div");
            d.style.cssText = "width:8px;height:8px";
            bar.appendChild(d);
            return d;
          });
          document.body.appendChild(bar);
          const w2: any = w.__w2 = { samples: [], scrolls: [], running: false, marker: null, n: 0, clickN: -1, domGrow: [] };
          sc.addEventListener("scroll", () => { if (w2.running) w2.scrolls.push([performance.now(), w2.n, sc.scrollTop]); }, { capture: true });
          new MutationObserver((list) => {
            for (const rec of list) if ((rec.target as Element).closest?.("[data-testid=\"window\"] [data-state]") && rec.addedNodes.length) w2.domGrow.push([performance.now(), w2.n]);
          }).observe(sc, { subtree: true, childList: true });
          w2.arm = () => {
            const top = sc.getBoundingClientRect().top + sc.clientTop;
            const want = top + sc.clientHeight - 140;
            let best: HTMLElement | null = null, bd = 1e9;
            for (const el of sc.querySelectorAll<HTMLElement>("[data-testid=\"window\"] [data-value]")) {
              const t = el.getBoundingClientRect().top;
              if (t < top || t + 20 > top + sc.clientHeight) continue;
              if (Math.abs(t - want) < bd) { bd = Math.abs(t - want); best = el; }
            }
            w2.marker = best!;
            best!.style.background = "#ff00ff";
            w2.clickN = w2.n;
            w2.clickAt = performance.now();
            w2.y0 = best!.getBoundingClientRect().top - top;
            w2.st0 = sc.scrollTop;
          };
          w2.start = () => {
            w2.running = true;
            const t0 = performance.now();
            let prev = t0;
            const tick = (now: number) => {
              if (!w2.running) return;
              w2.n++;
              const v = (speed as number);
              cells.forEach((d, i) => { d.style.background = (w2.n >> i) & 1 ? "#000" : "#fff"; });
              const before = sc.scrollTop;
              sc.scrollTop = before + (v * Math.min(now - prev, 50)) / 1000;
              const inc = sc.scrollTop - before;
              prev = now;
              const m: HTMLElement | null = w2.marker;
              const y = m && m.isConnected ? m.getBoundingClientRect().top - (sc.getBoundingClientRect().top + sc.clientTop) : null;
              w2.samples.push([w2.n, now, sc.scrollTop, y, inc]);
              requestAnimationFrame(tick);
            };
            requestAnimationFrame(tick);
          };
          void n;
        }, [n, W2_SPEED] as const);
        const rect = await page.evaluate(() => {
          const sc = (window as any).Ladder.rung.scroller as HTMLElement;
          const r = sc.getBoundingClientRect();
          return { x: r.x + sc.clientLeft, y: r.y + sc.clientTop, w: sc.clientWidth, h: sc.clientHeight };
        });
        const crop = { x0: Math.ceil(rect.x), x1: Math.floor(rect.x + rect.w) - SCROLLBAR, y0: Math.ceil(rect.y), y1: Math.floor(rect.y + rect.h) };
        const cdp = await page.context().newCDPSession(page);
        const frames: { ts: number; data: Buffer }[] = [];
        cdp.on("Page.screencastFrame", (e: any) => {
          frames.push({ ts: e.metadata.timestamp, data: Buffer.from(e.data, "base64") });
          cdp.send("Page.screencastFrameAck", { sessionId: e.sessionId }).catch((err: Error) => failures.push(String(err)));
        });
        await cdp.send("Page.startScreencast", { format: "png", everyNthFrame: 1 });
        for (let i = 0; i < 100 && frames.length === 0; i++) await page.waitForTimeout(50);
        await page.evaluate(() => { (window as any).__clayTiming = []; (window as any).__w2.start(); });
        await page.waitForTimeout(1000);
        const clickInfo = await page.evaluate((n) => {
          const w2 = (window as any).__w2;
          w2.arm();
          if (n > 0) (document.querySelector(`[data-testid="grow-${n}"]`) as HTMLElement).click();
          return { clickN: w2.clickN, clickAt: w2.clickAt, y0: w2.y0, st0: w2.st0 };
        }, n);
        await page.waitForTimeout(DURATION_MS - 1000);
        await page.waitForTimeout(200);
        const raw = await page.evaluate(() => {
          const w2 = (window as any).__w2;
          w2.running = false;
          return { samples: w2.samples, scrolls: w2.scrolls, domGrow: w2.domGrow, timing: (window as any).__clayTiming };
        });
        await cdp.send("Page.stopScreencast");
        expect(failures).toEqual([]);
        const painted = frames.map((f) => {
          const img = decodePng(f.data);
          return { ts: f.ts, n: counterOf(img), y: markerOf(img, crop.x0 + 2, crop.y0, crop.y1) };
        });
        fs.appendFileSync(OUT, JSON.stringify({ case: c.name, n, speed: W2_SPEED, round: ROUND, ...clickInfo, crop, loadStart, loadEnd: load(), ...raw, painted }) + "\n");
        console.log(`W2 ${c.name} N${n} r${ROUND} samples=${raw.samples.length} painted=${painted.length} withMarker=${painted.filter((p) => p.y !== null).length}`);
        await page.context().close();
      });
    }
  }
}

if (MODE === "resize") {
  test("resize event on web", async ({ browser }) => {
    const { page, failures } = await openMicro(browser, "pierre", true);
    await page.getByTestId("anchor-off").click();
    await page.getByTestId("resize-probe").click();
    await page.evaluate(async () => { for (let i = 0; i < 10; i++) await (window as any).Ladder.nextFrame(); });
    // Scroll so a thread is in the window, then grow it with the viewport at rest.
    await page.evaluate(async () => {
      const L = (window as any).Ladder;
      L.rung.scroller.scrollTop = 6000;
      for (let i = 0; i < 10; i++) await L.nextFrame();
      (window as any).__clayTiming = [];
    });
    const before = await page.getByTestId("resize-log").textContent();
    await page.evaluate(() => { (window as any).__t = performance.now(); (document.querySelector('[data-testid="grow-4"]') as HTMLElement).click(); });
    await page.waitForTimeout(500);
    const out = await page.evaluate(() => ({ t: (window as any).__t, timing: (window as any).__clayTiming }));
    const after = await page.getByTestId("resize-log").textContent();
    fs.appendFileSync(OUT, JSON.stringify({ before, after, ...out }) + "\n");
    console.log(`RESIZE before=${before?.length} after=${after?.length} :: ${after?.slice(-120)}`);
    expect(failures).toEqual([]);
  });
}

if (MODE === "dbg") {
  test("dbg", async ({ browser }) => {
    const { page } = await openMicro(browser, "pierre", false);
    await page.getByTestId("threads").click();
    await page.waitForTimeout(1500);
    await page.getByRole("button", { name: "Show Settings", exact: true }).click();
    await page.getByRole("button", { name: /Micro lifecycle/ }).click();
    await page.waitForTimeout(800);
    const text = await page.evaluate(() => document.body.innerText);
    console.log("DBG " + text.replace(/\n+/g, " | ").slice(0, 3000));
  });
}
