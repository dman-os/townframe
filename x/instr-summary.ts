#!/usr/bin/env -S deno run --allow-read
// Summarize INSTR lines from a captured test log (see .agents/skills/perf-investigation).
//
//   deno run --allow-read x/instr-summary.ts <log> [--skip N]
//
// Two line shapes are read; ANSI colour codes are stripped first.
// - Per-sample lines, one per scripted operation, as costs.rs prints them:
//     INSTR create note 12: wall 761.7ms cpu 720 ms allocations 794888 sql write_leases 659 ...
//   Every `<word> <number>[unit]` pair is a metric. Reported per operation: p50 over all samples,
//   p50 of the first tenth and of the last tenth (a cost that grows with the data shows as
//   last > first), and max.
// - Timing lines, one per phase, as the `// INSTR` tracing lines print them:
//     ... INSTR causal task timing kind=ensure_coverage ... wall=220.5ms
//   Every `key=<duration>` is a metric, in ms. Reported per timing name: count and p50.
// `--skip N` drops samples with ordinal < N, and timing lines before the `INSTR ... begin
// ordinal=N` marker, to leave warm-up out.

const args = Deno.args;
const path = args.find((arg) => !arg.startsWith("--"));
if (!path) {
  console.error("usage: x/instr-summary.ts <log> [--skip N]");
  Deno.exit(2);
}
const skipAt = args.indexOf("--skip");
const skip = skipAt >= 0 ? Number(args[skipAt + 1]) : 0;

const UNITS: Record<string, number> = {
  ns: 1e-6,
  "µs": 1e-3,
  us: 1e-3,
  ms: 1,
  s: 1e3,
};
const toMs = (value: string, unit: string) =>
  Number(value) * (UNITS[unit] ?? 1);

const median = (values: number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};
const fmt = (value: number) =>
  Math.abs(value) >= 100 ? value.toFixed(0) : value.toFixed(2);

// op -> metric -> samples by ordinal
const samples = new Map<string, Map<string, [number, number][]>>();
const timings = new Map<string, Map<string, number[]>>();
let started = skip === 0;

const ANSI = new RegExp(String.fromCharCode(27) + "\\[[0-9;]*m", "g");
const text = Deno.readTextFileSync(path).replace(ANSI, "");
for (const line of text.split("\n")) {
  const begin = line.match(/INSTR .* begin ordinal=(\d+)/);
  if (begin) {
    started ||= Number(begin[1]) >= skip;
    continue;
  }
  const sample = line.match(/INSTR (.+?) (\d+): (.*)$/);
  if (sample) {
    const [, op, ordinal, rest] = sample;
    if (Number(ordinal) < skip) continue;
    const metrics = samples.get(op) ?? new Map();
    samples.set(op, metrics);
    for (
      const [, name, value, unit] of rest.matchAll(
        /([a-z_]+) ([\d.]+)\s?(ns|µs|us|ms|s)?\b/g,
      )
    ) {
      const key = unit ? `${name} (ms)` : name;
      const list = metrics.get(key) ?? [];
      list.push([Number(ordinal), unit ? toMs(value, unit) : Number(value)]);
      metrics.set(key, list);
    }
    continue;
  }
  const timing = line.match(/INSTR (\S+(?: \S+)*?) timing (.*)$/);
  if (timing && started) {
    const [, name, rest] = timing;
    const metrics = timings.get(name) ?? new Map();
    timings.set(name, metrics);
    for (
      const [, key, value, unit] of rest.matchAll(
        /(\w+)=([\d.]+)(ns|µs|us|ms|s)\b/g,
      )
    ) {
      const list = metrics.get(key) ?? [];
      list.push(toMs(value, unit));
      metrics.set(key, list);
    }
  }
}

for (const [op, metrics] of samples) {
  const n = [...metrics.values()][0]?.length ?? 0;
  if (n === 0) continue;
  console.log(`## ${op} (n=${n})`);
  console.log("metric\tp50\tfirst tenth p50\tlast tenth p50\tmax");
  for (const [metric, list] of metrics) {
    list.sort((a, b) => a[0] - b[0]);
    const values = list.map(([, value]) => value);
    const tenth = Math.max(1, Math.floor(values.length / 10));
    console.log(
      [
        metric,
        fmt(median(values)),
        fmt(median(values.slice(0, tenth))),
        fmt(median(values.slice(-tenth))),
        fmt(Math.max(...values)),
      ].join("\t"),
    );
  }
}
for (const [name, metrics] of timings) {
  console.log(`## ${name} timing (ms)`);
  console.log("key\tn\tp50");
  for (const [key, values] of metrics) {
    console.log(`${key}\t${values.length}\t${fmt(median(values))}`);
  }
}
