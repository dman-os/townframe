import type { Browser, Page } from "@playwright/test";
import * as os from "node:os";

export const stats = (values: number[]) => {
  const s = [...values].sort((a, b) => a - b);
  const at = (q: number) => (s.length === 0 ? null : s[Math.min(s.length - 1, Math.floor(q * s.length))]);
  const r = (v: number | null) => (v === null ? null : Math.round(v * 1000) / 1000);
  return { n: s.length, median: r(at(0.5)), p95: r(at(0.95)), max: r(at(1)) };
};

export const load = () => os.loadavg().map((v) => Math.round(v * 100) / 100);

type TraceEvent = {
  name: string;
  cat?: string;
  ph: string;
  ts: number;
  dur?: number;
  pid: number;
  tid: number;
  args?: Record<string, unknown>;
};

const CATEGORIES = [
  "-*",
  "devtools.timeline",
  "disabled-by-default-devtools.timeline",
  "blink.user_timing",
  "toplevel",
  "v8",
];

export async function traceOf(browser: Browser, page: Page, body: () => Promise<void>): Promise<TraceEvent[]> {
  await browser.startTracing(page, { categories: CATEGORIES });
  await body();
  const buffer = await browser.stopTracing();
  const parsed = JSON.parse(buffer.toString("utf8"));
  return (Array.isArray(parsed) ? parsed : parsed.traceEvents) as TraceEvent[];
}

/** Trace event name -> breakdown class. Anything else on the main thread is "other". */
const CLASS: Record<string, string> = {
  FunctionCall: "script",
  EvaluateScript: "script",
  "v8.callFunction": "script",
  "v8.run": "script",
  "v8.compile": "script",
  "v8.compileModule": "script",
  RunMicrotasks: "script",
  "V8.RunMicrotasks": "script",
  TimerFire: "script",
  FireAnimationFrame: "script",
  EventDispatch: "script",
  FireIdleCallback: "script",
  MinorGC: "gc",
  MajorGC: "gc",
  V8Consumer: "gc",
  UpdateLayoutTree: "style",
  RecalculateStyles: "style",
  Layout: "layout",
  PrePaint: "paint",
  Paint: "paint",
  Layerize: "paint",
  UpdateLayer: "paint",
  UpdateLayerTree: "paint",
  Commit: "paint",
  CompositeLayers: "paint",
  HitTest: "paint",
};

/** Per-event self time (ms) on one thread: duration minus its direct children's durations. */
function selfTimes(events: TraceEvent[]) {
  const sorted = events
    .filter((e) => e.ph === "X" && e.dur !== undefined)
    .sort((a, b) => a.ts - b.ts || (b.dur ?? 0) - (a.dur ?? 0));
  const self = new Map<TraceEvent, number>();
  const stack: TraceEvent[] = [];
  for (const e of sorted) {
    while (stack.length && stack[stack.length - 1].ts + (stack[stack.length - 1].dur ?? 0) <= e.ts) stack.pop();
    self.set(e, (e.dur ?? 0) / 1000);
    if (stack.length) {
      const parent = stack[stack.length - 1];
      self.set(parent, (self.get(parent) ?? 0) - (e.dur ?? 0) / 1000);
    }
    stack.push(e);
  }
  return { self, sorted };
}

/** Find the renderer main thread that emitted the `performance.mark`s: `{pid, tid, marks}`. */
function mainThread(events: TraceEvent[]) {
  const marks = events.filter((e) => e.cat?.includes("blink.user_timing") && /^(c\d+-(start|end)|f\d+|f-end)$/.test(e.name));
  if (marks.length === 0) throw new Error("no performance marks in the trace");
  const { pid, tid } = marks[0];
  return { pid, tid, marks: marks.filter((m) => m.pid === pid && m.tid === tid) };
}

export type Breakdown = Record<string, number>;

const SCRIPT_CLASSES = new Set(["script"]);

/**
 * Inclusive duration (ms) of the top-level script events (those with no script-class ancestor) in
 * [t0, t1], keyed by event name, with the DOM event type for `EventDispatch`. X4: this separates
 * the task that runs the guest (the scroll `EventDispatch`) from the tasks that run the host's own
 * rendering, by what the browser reported rather than by subtraction.
 */
export function topLevelScript(sorted: TraceEvent[], t0: number, t1: number): Breakdown {
  const out: Breakdown = {};
  let until = -Infinity;
  for (const e of sorted) {
    if (e.ts < t0 || e.ts > t1 || e.dur === undefined) continue;
    if (!SCRIPT_CLASSES.has(CLASS[e.name] ?? "")) continue;
    if (e.ts < until) continue; // nested in a script event already counted
    until = e.ts + e.dur;
    const type = e.name === "EventDispatch" ? `:${(e.args as any)?.data?.type ?? "?"}` : "";
    const key = `${e.name}${type}`;
    out[key] = (out[key] ?? 0) + e.dur / 1000;
  }
  return out;
}

/** Per-commit breakdown (ms) from marks `c<i>-start` .. `c<i>-end`, plus the unclassified names. */
export function commitBreakdowns(events: TraceEvent[], warm: number) {
  const { pid, tid, marks } = mainThread(events);
  const thread = events.filter((e) => e.pid === pid && e.tid === tid);
  const { self, sorted } = selfTimes(thread);
  const start = new Map<number, number>(), end = new Map<number, number>();
  for (const m of marks) {
    const [, i, kind] = m.name.match(/^c(\d+)-(start|end)$/) ?? [];
    if (i === undefined) continue;
    (kind === "start" ? start : end).set(Number(i), m.ts);
  }
  const rows: { i: number; wallMs: number; classes: Breakdown; names: Breakdown; topScript: Breakdown }[] = [];
  for (const [i, t0] of [...start.entries()].sort((a, b) => a[0] - b[0])) {
    const t1 = end.get(i);
    if (t1 === undefined || i < warm) continue;
    const classes: Breakdown = { script: 0, gc: 0, style: 0, layout: 0, paint: 0, other: 0, busy: 0 };
    const names: Breakdown = {};
    const topScript = topLevelScript(sorted, t0, t1);
    for (const e of sorted) {
      if (e.ts < t0 || e.ts > t1) continue;
      const s = self.get(e) ?? 0;
      if (e.name === "RunTask" || e.name === "ThreadControllerImpl::RunTask") {
        // Containers: their own self time is the unattributed rest of the task. The two names are
        // the same task seen at two trace layers (RunTask wraps ThreadControllerImpl::RunTask), so
        // only RunTask counts toward the busy total.
        classes.other += s;
        if (e.name === "RunTask") classes.busy += (e.dur ?? 0) / 1000;
        continue;
      }
      const cls = CLASS[e.name];
      names[e.name] = (names[e.name] ?? 0) + s;
      if (cls) classes[cls] += s; else classes.other += s;
    }
    rows.push({ i, wallMs: (t1 - t0) / 1000, classes, names, topScript });
  }
  return rows;
}

/**
 * Per-frame main-thread busy time (ms) of a scroll run, from marks `f<i>`: every `RunTask` is
 * assigned to the latest mark that started before the task ended, so the task that ran frame i's
 * callbacks and the paint tasks after it both land on frame i.
 */
export function frameBusy(events: TraceEvent[]) {
  const { pid, tid, marks } = mainThread(events);
  const frames = marks.filter((m) => /^f\d+$/.test(m.name)).sort((a, b) => a.ts - b.ts);
  const end = marks.find((m) => m.name === "f-end")?.ts ?? Infinity;
  const tasks = events.filter(
    (e) => e.pid === pid && e.tid === tid && e.ph === "X" && e.name === "RunTask" && e.ts + (e.dur ?? 0) >= frames[0].ts && e.ts < end,
  );
  const busy = new Array(frames.length).fill(0);
  const classes: Breakdown[] = frames.map(() => ({ script: 0, gc: 0, style: 0, layout: 0, paint: 0, other: 0 }));
  const thread = events.filter((e) => e.pid === pid && e.tid === tid);
  const { self, sorted } = selfTimes(thread);
  const frameOf = (ts: number, endTs: number) => {
    let lo = 0, hi = frames.length - 1, ans = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (frames[mid].ts <= endTs) { ans = mid; lo = mid + 1; } else hi = mid - 1;
    }
    return ans;
  };
  for (const t of tasks) {
    const f = frameOf(t.ts, t.ts + (t.dur ?? 0));
    if (f >= 0) busy[f] += (t.dur ?? 0) / 1000;
  }
  for (const e of sorted) {
    if (e.ts < frames[0].ts || e.ts > end || e.name === "RunTask" || e.name === "ThreadControllerImpl::RunTask") continue;
    const f = frameOf(e.ts, e.ts + (e.dur ?? 0));
    if (f < 0) continue;
    const cls = CLASS[e.name] ?? "other";
    classes[f][cls] += self.get(e) ?? 0;
  }
  return { busy, classes };
}
