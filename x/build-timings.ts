#!/usr/bin/env -S deno run --allow-read --allow-write --allow-run --allow-env
// Append build wait times to one JSONL file we can trend (see docs/scratch/mbx-hit-rate.md).
//
//   x/build-timings.ts mbx [--out FILE] [--host NAME]      local cargo sessions from mbx's recordings
//   x/build-timings.ts gh  [--out FILE] [--limit N] [--repo OWNER/NAME]   GitHub Actions job timings
//
// Default FILE is $BUILD_TIMINGS or ~/.local/state/townframe/build-timings.jsonl. Rows are keyed by
// (`src`, `id`); a key already in FILE is skipped, so both modes are safe to run from cron or a
// hook on every lane. Read-only against mbx and GitHub.
//
// mbx source: every cargo invocation through the mbx shim is one file in
// `$(mbx cache dir)/sessions/v1/`: a `session_started` line (workspace, cargo argv) and, when the
// invocation finished, a `session_finished` line whose `stats` carry
//   session_duration_ns  wall time of the invocation as the cache agent saw it
//   wrapper_phases_ns    cumulative per-phase time over every parallel rustc wrapper:
//                        `permit_wait` is the scheduler (CPU/memory admission) wait,
//                        `flight_wait` is waiting on another build's identical compilation,
//                        `compiler` is time spent in rustc itself
//   compiler.{miss,unconsulted,bypass}.{invocations,duration_ns}   uncached compiler work
//   hits, misses, unconsulted, estimated_compiler_duration_avoided_ns
// Sessions older than `gc.events_max_age` (7d) / `gc.events_max_sessions` (256) are pruned by mbx,
// so run this at least daily.
//
// gh source: `gh run list` then `gh run view --json jobs`; one row per job with queue time
// (createdAt to startedAt) and run time, plus the slowest steps.

const args = Deno.args;
const mode = args[0];
const opt = (name: string): string | undefined => {
  const at = args.indexOf(name);
  return at >= 0 ? args[at + 1] : undefined;
};
const home = Deno.env.get("HOME") ?? ".";
const out = opt("--out") ?? Deno.env.get("BUILD_TIMINGS") ??
  `${home}/.local/state/townframe/build-timings.jsonl`;
if (mode !== "mbx" && mode !== "gh") {
  console.error("usage: x/build-timings.ts <mbx|gh> [--out FILE] [--host NAME] [--limit N] [--repo OWNER/NAME]");
  Deno.exit(2);
}

async function run(cmd: string, cmdArgs: string[]): Promise<string> {
  const res = await new Deno.Command(cmd, { args: cmdArgs, stdout: "piped", stderr: "inherit" }).output();
  if (!res.success) throw new Error(`${cmd} ${cmdArgs.join(" ")} failed (${res.code})`);
  return new TextDecoder().decode(res.stdout);
}

const seen = new Set<string>();
try {
  for (const line of Deno.readTextFileSync(out).split("\n")) {
    if (!line) continue;
    const row = JSON.parse(line);
    seen.add(`${row.src}/${row.id}`);
  }
} catch (err) {
  if (!(err instanceof Deno.errors.NotFound)) throw err;
}
await Deno.mkdir(out.replace(/\/[^/]*$/, ""), { recursive: true });
const rows: Record<string, unknown>[] = [];
const push = (row: Record<string, unknown>) => {
  if (!seen.has(`${row.src}/${row.id}`)) rows.push(row);
};

if (mode === "mbx") {
  const host = opt("--host") ?? (await run("hostname", [])).trim();
  const dir = `${(await run("mbx", ["cache", "dir"])).trim()}/sessions/v1`;
  const secs = (ns: number | undefined) => Math.round((ns ?? 0) / 1e6) / 1e3;
  for (const entry of Deno.readDirSync(dir)) {
    if (!entry.name.endsWith(".jsonl")) continue;
    const path = `${dir}/${entry.name}`;
    const id = entry.name.slice(0, -".jsonl".length);
    if (seen.has(`mbx/${host}:${id}`)) continue;
    // Sessions are capped at events_max_size (16 MiB): read the head for the start line and the
    // tail for the finish line instead of the whole file.
    const file = Deno.openSync(path);
    const size = file.statSync().size;
    const head = new Uint8Array(Math.min(size, 16384));
    file.readSync(head);
    const tail = new Uint8Array(Math.min(size, 262144));
    file.seekSync(size - tail.length, Deno.SeekMode.Start);
    file.readSync(tail);
    file.close();
    const dec = new TextDecoder();
    const started = dec.decode(head).split("\n").find((l) => l.includes('"type":"session_started"'));
    const finished = dec.decode(tail).split("\n").find((l) => l.includes('"type":"session_finished"'));
    if (!started || !finished) continue; // still running, or killed before it could write stats
    const start = JSON.parse(started);
    const stats = JSON.parse(finished).stats;
    const phases = stats.wrapper_phases_ns ?? {};
    push({
      src: "mbx",
      id: `${host}:${id}`,
      ts: new Date(start.ts_ms).toISOString(),
      host,
      workspace: start.workspace_root,
      command: start.command.join(" "),
      mbx_version: start.mbx_version,
      wall_s: secs(stats.session_duration_ns),
      scheduler_wait_s: secs(phases.permit_wait),
      flight_wait_s: secs(phases.flight_wait),
      compiler_cpu_s: secs(phases.compiler),
      hits: stats.hits,
      misses: stats.misses,
      unconsulted: stats.unconsulted,
      miss_compiler_s: secs(stats.compiler?.miss?.duration_ns),
      unconsulted_compiler_s: secs(stats.compiler?.unconsulted?.duration_ns),
      bypass_compiler_s: secs(stats.compiler?.bypass?.duration_ns),
      avoided_compiler_s: secs(stats.estimated_compiler_duration_avoided_ns),
    });
  }
} else {
  const repo = opt("--repo");
  const repoArgs = repo ? ["--repo", repo] : [];
  const runs = JSON.parse(
    await run("gh", ["run", "list", ...repoArgs, "--status", "completed", "--limit", opt("--limit") ?? "30",
      "--json", "databaseId,name,event,headBranch,headSha,createdAt,conclusion"]),
  );
  const ms = (iso: string) => Date.parse(iso);
  for (const r of runs) {
    if ([...seen].some((k) => k.startsWith(`gh/${r.databaseId}:`))) continue;
    const view = JSON.parse(await run("gh", ["run", "view", String(r.databaseId), ...repoArgs, "--json", "jobs"]));
    for (const job of view.jobs) {
      if (!job.startedAt || !job.completedAt || job.startedAt.startsWith("0001")) continue; // skipped
      const steps = (job.steps ?? [])
        .filter((s: { startedAt?: string; completedAt?: string }) => s.startedAt && s.completedAt)
        .map((s: { name: string; startedAt: string; completedAt: string }) => ({
          name: s.name,
          s: (ms(s.completedAt) - ms(s.startedAt)) / 1e3,
        }))
        .sort((a: { s: number }, b: { s: number }) => b.s - a.s)
        .slice(0, 3);
      push({
        src: "gh",
        id: `${r.databaseId}:${job.databaseId}`,
        ts: r.createdAt,
        workflow: r.name,
        job: job.name,
        event: r.event,
        branch: r.headBranch,
        sha: r.headSha.slice(0, 8),
        conclusion: job.conclusion,
        queue_s: (ms(job.startedAt) - ms(r.createdAt)) / 1e3,
        run_s: (ms(job.completedAt) - ms(job.startedAt)) / 1e3,
        slowest_steps: steps,
      });
    }
  }
}

if (rows.length) {
  Deno.writeTextFileSync(out, rows.map((r) => JSON.stringify(r)).join("\n") + "\n", { append: true });
}
console.error(`${mode}: appended ${rows.length} rows to ${out}`);
