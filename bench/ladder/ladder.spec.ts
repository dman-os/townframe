import { expect, type Browser, type Page, test } from "@playwright/test";
import { execSync } from "node:child_process";
import * as fs from "node:fs";
import { commitBreakdowns, frameBusy, load, stats, traceOf } from "./lib";
import { isR3, openRung } from "./open";

// The perf ladder (docs/scratch/pierre-spikes.md, "X1 ladder"). One test per (rung, workload);
// `LADDER_RUNG` picks the rung, `LADDER_WORKLOADS` the workloads, `LADDER_ROUND` labels the run
// so rounds can be interleaved by the caller. Appends one JSON line per test to `LADDER_OUT`.

const RUNG = process.env.LADDER_RUNG ?? "r1-dom";
const WORKLOADS = (process.env.LADDER_WORKLOADS ?? "syn7,syn18,corpus").split(",");
const ROUND = process.env.LADDER_ROUND ?? "0";
const N = Number(process.env.LADDER_COMMITS ?? "70");
const WARM = 10;
const OUT = process.env.LADDER_OUT ?? "/run/media/asdf/p3N/tmp/x1/results.jsonl";
const BASE = `http://127.0.0.1:${process.env.LADDER_PORT_A ?? "4701"}`;
const BASE_R3 = `http://127.0.0.1:${process.env.LADDER_PORT_R3 ?? "4702"}`;
const FRAMES = 100;
const START_TOP = 3000;
const IS_R3 = isR3(RUNG);


async function open(browser: Browser, workload: string): Promise<{ page: Page; failures: string[] }> {
  return openRung(browser, { rung: RUNG, workload, base: BASE, baseR3: BASE_R3, profile: "pierre", startTop: START_TOP });
}

const uptime = () => execSync("uptime").toString().trim();

test.describe.configure({ timeout: 30 * 60_000 });

for (const workload of WORKLOADS) {
  test(`${RUNG} ${workload} r${ROUND}`, async ({ browser }) => {
    const loadStart = load();
    const record: Record<string, unknown> = { rung: RUNG, workload, round: ROUND, commitsRequested: N, warm: WARM };

    // Pass A: wall clock per commit (input -> rendered window moved -> two frames), no tracing;
    // then the DOM mutation census of one more commit.
    {
      const { page, failures } = await open(browser, workload);
      if (IS_R3) await page.getByTestId("phases-dump").click();
      const t0 = await page.evaluate(() => performance.now());
      const captureMark = await page.evaluate(() => (window as any).__clayBatches?.length ?? 0);
      const commits: { i: number; t0: number; t1: number | null; t2: number; start: number }[] =
        await page.evaluate(([n, w]) => (window as any).Ladder.commits(n, w), [N, WARM]);
      if (process.env.LADDER_CAPTURE) {
        // X4: the wire batches of this run for the Blitz harness (bench/blitz-x4); r3a only.
        const batches = await page.evaluate(() => (window as any).__clayBatches);
        fs.writeFileSync(process.env.LADDER_CAPTURE, JSON.stringify({ mark: captureMark, batches }));
      }
      record.commits = commits.length;
      record.completed = commits.filter((c) => c.t1 !== null).length;
      record.wallToWindowMs = stats(commits.filter((c) => c.t1 !== null).map((c) => c.t1! - c.t0));
      record.wallToSettledMs = stats(commits.map((c) => c.t2 - c.t0));
      const first = commits[0]?.t0 ?? t0;
      if (RUNG === "r1-dom" || RUNG === "r1-html") {
        const phases: { total: number; build: number; attach: number }[] =
          await page.evaluate(() => (window as any).Ladder.rung.phases());
        const used = phases.slice(-commits.length);
        record.r1PhasesMs = {
          total: stats(used.map((p) => p.total)),
          build: stats(used.map((p) => p.build)),
          attach: stats(used.map((p) => p.attach)),
        };
      }
      if (RUNG === "r0" || RUNG === "r2") {
        record.rungTimers = await page.evaluate((from) => (window as any).Ladder.rung.timers?.(from) ?? null, first);
      }
      if (IS_R3) {
        const timing: [string, number, number, number, number][] =
          await page.evaluate(() => (window as any).__clayTiming);
        const since = timing.filter((r) => r[1] >= first);
        const dur = (kind: string) => stats(since.filter((r) => r[0] === kind).map((r) => r[2] - r[1]));
        record.hostTimingMs = {
          dispatchScroll: dur("dispatch:scroll"),
          apply: dur("apply"),
          render: dur("render"),
          hostRender: dur("host-render"),
          directEncode: dur("direct-encode"),
          directJs: dur("direct-js"),
          directRust: dur("direct-rust"),
          kinds: [...new Set(since.map((r) => r[0]))],
        };
        await page.getByTestId("phases-dump").click();
        const parsed = JSON.parse((await page.getByTestId("phases").textContent())!) as { phases: number[][] };
        const rows = parsed.phases.filter((r) => r[4] > 0);
        const col = (i: number) => stats(rows.map((r) => r[i]));
        record.guestPhasesMs = {
          committingDispatches: rows.length,
          mutations: col(4),
          parseAndHandler: col(0),
          vdomDiff: col(1),
          batchTake: col(2),
          applyCallIncludingBridge: col(3),
        };
      }
      // Census of the settled window and the mutations of one more commit.
      record.census = await page.evaluate(() => {
        const L = (window as any).Ladder;
        const r = L.rung;
        return L.census(r.censusShadow ? r.censusShadow() : r.censusRoot);
      });
      record.mutationsOneCommit = await page.evaluate(async () => {
        const L = (window as any).Ladder;
        const r = L.rung;
        return await L.countMutations(r.mutationRootShadow ? r.mutationRootShadow() : r.mutationRoot, async () => {
          r.scroller.scrollTop += 1000;
          for (let i = 0; i < 6; i++) await L.nextFrame();
        });
      });
      expect(failures).toEqual([]);
      await page.context().close();
    }

    // Pass B: the same commits under a Chromium trace, sliced per commit.
    {
      const { page, failures } = await open(browser, workload);
      const events = await traceOf(browser, page, async () => {
        await page.evaluate(([n, w]) => (window as any).Ladder.commits(n, w), [N, WARM]);
      });
      const rows = commitBreakdowns(events, WARM);
      const cls = (k: string) => stats(rows.map((r) => r.classes[k]));
      record.trace = {
        commits: rows.length,
        wall: stats(rows.map((r) => r.wallMs)),
        busy: cls("busy"),
        script: cls("script"),
        gc: cls("gc"),
        style: cls("style"),
        layout: cls("layout"),
        paint: cls("paint"),
        other: cls("other"),
        topScript: Object.fromEntries(
          [...new Set(rows.flatMap((r) => Object.keys(r.topScript)))].map((k) => [k, stats(rows.map((r) => r.topScript[k] ?? 0))]),
        ),
        topNames: Object.entries(
          rows.reduce<Record<string, number>>((acc, r) => {
            for (const [k, v] of Object.entries(r.names)) acc[k] = (acc[k] ?? 0) + v / rows.length;
            return acc;
          }, {}),
        ).sort((a, b) => b[1] - a[1]).slice(0, 14).map(([k, v]) => [k, Math.round(v * 1000) / 1000]),
      };
      expect(failures).toEqual([]);
      await page.context().close();
    }

    // Pass C: continuous scroll, fast (one 50-row chunk per frame) and slow (a chunk every 5th).
    for (const [name, px] of [["fast", 1000], ["slow", 200]] as const) {
      const { page, failures } = await open(browser, workload);
      let run: { intervals: number[]; longtasks: number[]; endScrollTop: number } | undefined;
      const events = await traceOf(browser, page, async () => {
        run = await page.evaluate(([f, p, from]) => (window as any).Ladder.scrollRun(f, p, from), [FRAMES, px, START_TOP]);
      });
      const { busy, classes } = frameBusy(events);
      const over = (xs: number[], t: number) => xs.filter((v) => v > t).length;
      record[`scroll_${name}`] = {
        pxPerFrame: px,
        frames: busy.length,
        busyMs: stats(busy),
        busyOver8_3: over(busy, 8.333),
        busyOver16_7: over(busy, 16.667),
        rafIntervalMs: stats(run!.intervals),
        rafOver16_7: over(run!.intervals, 16.667 * 1.05),
        rafOver25: over(run!.intervals, 25),
        longtasks: { n: run!.longtasks.length, ...stats(run!.longtasks) },
        busyByClassMedian: Object.fromEntries(
          Object.keys(classes[0] ?? {}).map((k) => [k, stats(classes.map((c) => c[k])).median]),
        ),
        endScrollTop: run!.endScrollTop,
      };
      expect(failures).toEqual([]);
      await page.context().close();
    }

    // Pass E: a V8 CPU profile (100 us sampling) over the same commits, self time by category.
    {
      const { page, failures } = await open(browser, workload);
      const cdp = await page.context().newCDPSession(page);
      await cdp.send("Profiler.enable");
      await cdp.send("Profiler.setSamplingInterval", { interval: 100 });
      await cdp.send("Profiler.start");
      const done: { i: number }[] = await page.evaluate(([n, w]) => (window as any).Ladder.commits(n, w), [N, WARM]);
      const { profile } = await cdp.send("Profiler.stop");
      const byId = new Map(profile.nodes.map((n) => [n.id, n]));
      const cat = new Map<string, number>();
      const fn = new Map<string, number>();
      profile.samples!.forEach((id, i) => {
        const dt = (profile.timeDeltas![i] ?? 0) / 1000;
        const n = byId.get(id)!;
        const name = n.callFrame.functionName || "(anonymous)";
        const url = n.callFrame.url;
        let c: string;
        if (name === "(idle)") return;
        else if (name === "(program)" || name === "(root)") c = "program (browser: style/layout/paint/other native)";
        else if (name === "(garbage collector)") c = "gc";
        else if (url.startsWith("wasm://") || url.endsWith(".wasm")) c = "wasm";
        else if (url === "") c = "native api (DOM etc.)";
        else c = "js";
        cat.set(c, (cat.get(c) ?? 0) + dt);
        const key = `${c.split(" ")[0]}:${name}${url ? " @" + url.split("/").pop() : ""}`;
        fn.set(key, (fn.get(key) ?? 0) + dt);
      });
      const per = (v: number) => Math.round((v / done.length) * 1000) / 1000;
      record.cpuProfilePerCommitMs = {
        commits: done.length,
        categories: Object.fromEntries([...cat.entries()].map(([k, v]) => [k, per(v)])),
        top: [...fn.entries()].sort((a, b) => b[1] - a[1]).slice(0, 250).map(([k, v]) => [k, per(v)]),
      };
      expect(failures).toEqual([]);
      await page.context().close();
    }

    record.loadStart = loadStart;
    record.loadEnd = load();
    record.uptime = uptime();
    fs.appendFileSync(OUT, JSON.stringify(record) + "\n");
    console.log(`LADDER ${RUNG} ${workload} r${ROUND} ${JSON.stringify(record)}`);
  });
}
