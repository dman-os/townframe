#!/usr/bin/env -S deno run --allow-read
// Census aggregator: turns TASK_CENSUS_FILE records (see src/utils_rs/census.rs)
// into (1) a top-N busy-time table by span name and (2) an icicle tree.
//
//   deno run --allow-read x/census-report.ts /tmp/hunt11-census.tsv [--top 25] [--tree] [--grep SUBSTR]
//
// Lines: uptime_millis \t name \t duration_millis \t parent \t worker \t task_id \t peer_id \t doc_id \t obj_id
// uptime is per-test-process (nextest runs each test in its own process), so
// cross-process ordering is meaningless; aggregate per process by splitting on
// resets (uptime decreasing) when --tree spans processes.

interface Record {
  up: number;
  name: string;
  dur: number;
  parent: string;
  worker: string;
  taskId: string;
  peerId: string;
  docId: string;
  objId: string;
}

function parse(path: string): Record[] {
  const text = Deno.readTextFileSync(path);
  const out: Record[] = [];
  for (const line of text.split("\n")) {
    if (!line) continue;
    const c = line.split("\t");
    if (c.length < 9) continue;
    const up = Number(c[0]);
    const dur = Number(c[2]);
    if (!Number.isFinite(up) || !Number.isFinite(dur)) continue;
    out.push({
      up,
      name: c[1],
      dur,
      parent: c[3],
      worker: c[4],
      taskId: c[5],
      peerId: c[6],
      docId: c[7],
      objId: c[8],
    });
  }
  return out;
}

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const i = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[i];
}

function main() {
  const args = Deno.args;
  const path = args[0];
  if (!path) {
    console.error(
      "usage: census-report.ts <census.tsv> [--top N] [--tree] [--grep SUBSTR] [--task-id-reuse]",
    );
    Deno.exit(2);
  }
  function flagValue(flag: string, fallback: string): string | undefined {
    const i = args.indexOf(flag);
    return i === -1 ? undefined : (args[i + 1] ?? fallback);
  }
  const top = Number(flagValue("--top", "25"));
  const grep = flagValue("--grep", "");
  const tree = args.includes("--tree");
  const taskIdReuse = args.includes("--task-id-reuse");
  const all = parse(path);
  const rows = grep ? all.filter((r) => r.name.includes(grep)) : all;
  if (rows.length === 0) {
    console.log("no census records");
    return;
  }

  // --- busy-time table by span name ---
  const byName = new Map<
    string,
    {
      count: number;
      total: number;
      durs: number[];
      fanout: Map<string, number>;
    }
  >();
  for (const r of rows) {
    let e = byName.get(r.name);
    if (!e) {
      e = { count: 0, total: 0, durs: [], fanout: new Map() };
      byName.set(r.name, e);
    }
    e.count++;
    e.total += r.dur;
    e.durs.push(r.dur);
    if (r.docId !== "-" && r.objId !== "-") {
      const key = `${r.worker}/${r.docId}`;
      e.fanout.set(key, (e.fanout.get(key) ?? 0) + 1);
    } else if (r.docId !== "-") {
      e.fanout.set(r.docId, (e.fanout.get(r.docId) ?? 0) + 1);
    }
  }
  const ranked = [...byName.entries()]
    .map(([name, e]) => ({
      name,
      ...e,
      p50: pct(e.durs.sort((a, b) => a - b), 50),
      p99: pct(e.durs, 99),
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, top);

  console.log(
    `busy time by span name (top ${top} of ${byName.size}), ${rows.length} records, total ${
      rows.reduce((s, r) => s + r.dur, 0)
    }ms`,
  );
  console.log("total_ms\tcount\tp50_ms\tp99_ms\tname");
  for (const e of ranked) {
    console.log(`${e.total}\t${e.count}\t${e.p50}\t${e.p99}\t${e.name}`);
  }

  // --- task activity by available instance identity ---
  const taskish = rows.filter((r) => r.name.endsWith("_task"));
  if (taskish.length > 0) {
    const perObj = new Map<string, { count: number; total: number }>();
    for (const r of taskish) {
      const object = r.docId !== "-" ? r.docId : r.objId;
      const key = `${r.name}|${r.worker}|${r.peerId}|${object}`;
      const e = perObj.get(key) ?? { count: 0, total: 0 };
      e.count++;
      e.total += r.dur;
      perObj.set(key, e);
    }
    const worst = [...perObj.entries()]
      .map(([k, v]) => ({ k, ...v }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);
    console.log(
      "\ntask activity by (task|worker|peer|object); counts alone do not establish retries:",
    );
    for (const w of worst) console.log(`${w.count}\t${w.total}ms\t${w.k}`);
  }

  // --- repeated task_id tags: only a candidate reuse signal, not proof of retries ---
  if (taskIdReuse) {
    const byTask = new Map<string, number>();
    for (const r of taskish) {
      if (r.taskId === "-") continue;
      const object = r.docId !== "-" ? r.docId : r.objId;
      const key = `${r.worker}/${r.peerId}/${object}/${r.taskId}`;
      byTask.set(key, (byTask.get(key) ?? 0) + 1);
    }
    const reused = [...byTask.entries()]
      .filter(([, n]) => n > 1)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 15);
    console.log(
      "\nrepeated task_id tags by worker/peer/object (candidate reuse, not proof of retries):",
    );
    for (const [k, n] of reused) console.log(`${n}x\t${k}`);
  }
  // --- icicle tree by (parent -> name) ---
  if (tree) {
    console.log("\nicicle: parent -> name (count, total_ms, p99_ms)");
    const edges = new Map<
      string,
      { count: number; total: number; durs: number[] }
    >();
    for (const r of rows) {
      const key = `${r.parent} -> ${r.name}`;
      let e = edges.get(key);
      if (!e) {
        e = { count: 0, total: 0, durs: [] };
        edges.set(key, e);
      }
      e.count++;
      e.total += r.dur;
      e.durs.push(r.dur);
    }
    const lines = [...edges.entries()]
      .map(([k, e]) => ({ k, e, p99: pct(e.durs.sort((a, b) => a - b), 99) }))
      .sort((a, b) => b.e.total - a.e.total)
      .slice(0, 40);
    for (const l of lines) {
      console.log(`${l.e.count}x ${l.e.total}ms p99=${l.p99}  ${l.k}`);
    }
  }
}

main();
