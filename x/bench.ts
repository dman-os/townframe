#!/usr/bin/env -S deno run --allow-all

// Track performance across builds, for every crate. Orchestration around `x/bench-lib.ts`;
// see `.agents/skills/benchmarking/SKILL.md`.
//
//   x/bench.ts run <crate[:tier]>... [--release] [--out FILE]   run benches, append records to the history
//   x/bench.ts compare <rev-a> <rev-b> [--tier T] [--gate] [--force-cross-machine]
//   x/bench.ts report [--last N] [--bench SUBSTR] [--metric M]   trend table per bench and metric
//   x/bench.ts gate --base <rev> [crate[:tier]...]               CI: base then head in one checkout, compare
//
// Tiers a crate declares in `[package.metadata.bench]` of its Cargo.toml:
//   counts  a nextest filter over tests that print `BENCH_RECORD {json}` lines (allocation
//           counts, simulated time): deterministic, gated
//   instr   a Gungraun bench target (callgrind instruction counts): deterministic, gated
//   wall    a bench target printing `BENCH_RECORD` lines in real time: exploration only
//   web     a Playwright spec printing `BENCH_RECORD` lines from the browser: exploration only
//
// History is per machine, append-only JSON lines in `$BENCH_HISTORY_DIR` (default
// `~/.local/share/townframe-bench/<hostname>.jsonl`), outside the worktree.

import {
  type BenchRecord,
  compare,
  CrossBasisError,
  type Env,
  failures,
  formatParams,
  fromDivanOutput,
  fromGungraunSummary,
  overScaled,
  parseAccepts,
  parseRecord,
  type Partial,
  percent,
  recordsFromOutput,
  scalings,
  trends,
  withEnv,
} from "./bench-lib.ts";

const TIERS = ["counts", "instr", "wall", "web"] as const;
type Tier = typeof TIERS[number];

// Tiers a gate compares: the ones whose numbers do not depend on how busy the machine is.
const GATED: Tier[] = ["counts", "instr"];

interface Selection {
  crate: string;
  tier: Tier;
  spec: Record<string, unknown>;
  targetDirectory: string;
}

async function run(
  cmd: string[],
  options: { env?: Record<string, string>; cwd?: string; allowFail?: boolean } =
    {},
): Promise<{ code: number; output: string }> {
  const child = new Deno.Command(cmd[0], {
    args: cmd.slice(1),
    env: options.env,
    cwd: options.cwd,
    stdout: "piped",
    stderr: "piped",
  }).spawn();
  const chunks: string[] = [];
  const pump = async (
    stream: ReadableStream<Uint8Array>,
    sink: typeof Deno.stderr,
  ) => {
    const decoder = new TextDecoder();
    for await (const chunk of stream) {
      await sink.write(chunk);
      chunks.push(decoder.decode(chunk, { stream: true }));
    }
  };
  // Both streams go to stderr so a command's own stdout stays the tool's report.
  await Promise.all([
    pump(child.stdout, Deno.stderr),
    pump(child.stderr, Deno.stderr),
  ]);
  const status = await child.status;
  if (status.code !== 0 && !options.allowFail) {
    throw new Error(`${cmd.join(" ")} exited with ${status.code}`);
  }
  return { code: status.code, output: chunks.join("") };
}

async function text(cmd: string[]): Promise<string> {
  const out = await new Deno.Command(cmd[0], {
    args: cmd.slice(1),
    stdout: "piped",
    stderr: "piped",
  })
    .output();
  if (out.code !== 0) {
    throw new Error(
      `${cmd.join(" ")}: ${new TextDecoder().decode(out.stderr)}`,
    );
  }
  return new TextDecoder().decode(out.stdout).trim();
}

async function machine(): Promise<string> {
  let cpu = "unknown cpu";
  try {
    const info = await Deno.readTextFile("/proc/cpuinfo");
    cpu = /model name\s*:\s*(.+)/.exec(info)?.[1]?.trim() ?? cpu;
  } catch {
    // not Linux: the hostname alone names the machine
  }
  return `${Deno.hostname()} (${cpu})`;
}

async function environment(
  profile: string,
  target: Env["target"],
): Promise<Env> {
  return {
    commit: await text(["git", "rev-parse", "HEAD"]),
    dirty:
      (await text(["git", "status", "--porcelain", "--untracked-files=no"])) !==
        "",
    machine: await machine(),
    toolchain: await text(["rustc", "--version"]),
    profile,
    target,
    ts: new Date().toISOString(),
  };
}

const historyDir = () =>
  Deno.env.get("BENCH_HISTORY_DIR") ??
    `${Deno.env.get("HOME")}/.local/share/townframe-bench`;
const historyFile = () => `${historyDir()}/${Deno.hostname()}.jsonl`;

async function append(records: BenchRecord[]) {
  await Deno.mkdir(historyDir(), { recursive: true });
  const lines = records.map((record) => JSON.stringify(record)).join("\n") +
    "\n";
  await Deno.writeTextFile(historyFile(), lines, { append: true });
}

async function readHistory(files: string[]): Promise<BenchRecord[]> {
  const records: BenchRecord[] = [];
  for (const file of files) {
    let body: string;
    try {
      body = await Deno.readTextFile(file);
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) continue;
      throw error;
    }
    for (const line of body.split("\n")) {
      if (line.trim()) records.push(parseRecord(JSON.parse(line)));
    }
  }
  return records;
}

async function crates(): Promise<Selection[]> {
  const meta = JSON.parse(
    await text(["cargo", "metadata", "--no-deps", "--format-version", "1"]),
  ) as {
    target_directory: string;
    packages: {
      name: string;
      metadata?: { bench?: Record<string, Record<string, unknown>> };
    }[];
  };
  const found: Selection[] = [];
  for (const pkg of meta.packages) {
    for (const [tier, spec] of Object.entries(pkg.metadata?.bench ?? {})) {
      if (!(TIERS as readonly string[]).includes(tier)) {
        throw new Error(`${pkg.name}: unknown tier ${tier}`);
      }
      found.push({
        crate: pkg.name,
        tier: tier as Tier,
        spec,
        targetDirectory: meta.target_directory,
      });
    }
  }
  return found;
}

function select(
  all: Selection[],
  selectors: string[],
  defaults: readonly Tier[],
): Selection[] {
  if (selectors.length === 0 || selectors.includes("all")) {
    return all.filter((entry) => defaults.includes(entry.tier));
  }
  const chosen = new Set<Selection>();
  for (const selector of selectors) {
    const [crate, tier] = selector.split(":");
    const hits = all.filter((entry) =>
      entry.crate === crate &&
      (tier ? entry.tier === tier : defaults.includes(entry.tier))
    );
    if (hits.length === 0) {
      throw new Error(
        `nothing declares ${selector} in [package.metadata.bench]`,
      );
    }
    hits.forEach((hit) => chosen.add(hit));
  }
  return [...chosen];
}

/** The gungraun runner must match the library's version, which Cargo.lock pins. */
async function ensureGungraunRunner(selection: Selection) {
  const lock = await Deno.readTextFile("Cargo.lock");
  const version = /name = "gungraun"\nversion = "([^"]+)"/.exec(lock)?.[1];
  if (!version) throw new Error("Cargo.lock has no gungraun");
  const root = `${selection.targetDirectory}/gungraun-tools`;
  const have = await new Deno.Command(`${root}/bin/gungraun-runner`, {
    args: ["--version"],
    stdout: "piped",
    stderr: "null",
  })
    .output().then((out) => new TextDecoder().decode(out.stdout), () => "");
  if (!have.includes(version)) {
    await run([
      "cargo",
      "install",
      "gungraun-runner",
      "--version",
      version,
      "--locked",
      "--root",
      root,
    ]);
  }
  Deno.env.set("PATH", `${root}/bin:${Deno.env.get("PATH")}`);
  // valgrind comes from the dev shell; a missing one is the common failure, so say so.
  await run(["valgrind", "--version"]).catch(() => {
    throw new Error(
      "valgrind is not on PATH: run inside `nix develop` (it is in the dev and ci-rust shells)",
    );
  });
}

const features = (spec: Record<string, unknown>) =>
  Array.isArray(spec.features) && spec.features.length > 0
    ? ["--features", (spec.features as string[]).join(",")]
    : [];

/** Run one selected tier; the partial records it produced and the profile they were taken in. */
async function runTier(
  selection: Selection,
  release: boolean,
): Promise<{ partials: Partial[]; profile: string; target: Env["target"] }> {
  const { crate, tier, spec } = selection;
  switch (tier) {
    case "counts": {
      const filter = String(spec.filter);
      const { output } = await run([
        "cargo",
        "nextest",
        "run",
        "--locked",
        "-p",
        crate,
        ...features(spec),
        ...(release ? ["--release"] : []),
        "-E",
        filter,
        "--no-capture",
        "--no-tests=fail",
      ]);
      return {
        partials: recordsFromOutput(output),
        profile: release ? "release" : "dev",
        target: "native",
      };
    }
    case "instr": {
      await ensureGungraunRunner(selection);
      const started = Date.now();
      const bench = String(spec.bench);
      await run([
        "cargo",
        "bench",
        "--locked",
        "-p",
        crate,
        "--bench",
        bench,
        ...features(spec),
        "--",
        "--save-summary=json",
      ]);
      const partials: Partial[] = [];
      const root = `${selection.targetDirectory}/gungraun/${crate}/${bench}`;
      const walk = async (dir: string) => {
        for await (const entry of Deno.readDir(dir)) {
          const path = `${dir}/${entry.name}`;
          if (entry.isDirectory) await walk(path);
          else if (
            entry.name === "summary.json" &&
            (await Deno.stat(path)).mtime!.getTime() >= started - 1000
          ) {
            partials.push(
              ...fromGungraunSummary(
                JSON.parse(await Deno.readTextFile(path)),
                crate,
              ),
            );
          }
        }
      };
      await walk(root);
      if (partials.length === 0) {
        throw new Error(`gungraun left no summaries under ${root}`);
      }
      return { partials, profile: "bench", target: "native" };
    }
    case "wall": {
      const { output } = await run([
        "cargo",
        "bench",
        "--locked",
        "-p",
        crate,
        "--bench",
        String(spec.bench),
        ...features(spec),
      ]);
      return {
        partials: [
          ...recordsFromOutput(output),
          ...fromDivanOutput(output, crate),
        ],
        profile: "bench",
        target: "native",
      };
    }
    case "web": {
      // Playwright starts the page's server itself (see the package's config); the spec prints
      // the page's `BENCH_RECORD` lines. Needs the `ci-desktop` or dev shell for the browsers.
      const { output } = await run([
        "pnpm",
        "-C",
        String(spec.dir),
        "exec",
        "playwright",
        "test",
        String(spec.spec),
        "--reporter=line",
      ]);
      return {
        partials: recordsFromOutput(output),
        profile: "wasm-release",
        target: "wasm-browser",
      };
    }
  }
}

async function runSelected(
  selections: Selection[],
  release: boolean,
): Promise<BenchRecord[]> {
  const records: BenchRecord[] = [];
  for (const selection of selections) {
    console.error(`== ${selection.crate}:${selection.tier}`);
    const { partials, profile, target } = await runTier(selection, release);
    if (partials.length === 0) {
      throw new Error(
        `${selection.crate}:${selection.tier} printed no records`,
      );
    }
    records.push(...withEnv(partials, await environment(profile, target)));
  }
  return records;
}

function printComparison(
  comparison: ReturnType<typeof compare>,
  accepted: (bench: string) => boolean,
) {
  if (comparison.forcedAcross.length > 0) {
    console.log(
      "!!! CROSS-MACHINE COMPARISON, FORCED: the sides differ in " +
        comparison.forcedAcross.join("; "),
    );
    console.log(
      "!!! counts may transfer between machines of one architecture, times do not",
    );
  }
  console.log(
    ["bench", "params", "metric", "before", "after", "change", "verdict"].join(
      "\t",
    ),
  );
  for (const change of comparison.changes) {
    if (change.verdict === "unchanged") continue;
    const verdict = change.verdict === "regression" && accepted(change.bench)
      ? "regression (accepted)"
      : change.verdict;
    console.log([
      change.bench,
      formatParams(change.params),
      change.metric,
      change.before,
      change.after,
      percent(change.relative),
      verdict,
    ].join("\t"));
  }
  const unchanged =
    comparison.changes.filter((change) => change.verdict === "unchanged")
      .length;
  console.log(
    `${unchanged} unchanged, ${comparison.onlyBefore.length} only before, ${comparison.onlyAfter.length} only after`,
  );
}

function flag(args: string[], name: string): string | undefined {
  const at = args.indexOf(name);
  return at >= 0 ? args[at + 1] : undefined;
}

const positional = (args: string[], valued: string[]) =>
  args.filter((arg, at) =>
    !arg.startsWith("--") && !valued.includes(args[at - 1] ?? "")
  );

async function recordsAt(
  rev: string,
  files: string[],
  dirty: boolean,
): Promise<BenchRecord[]> {
  const sha = await text(["git", "rev-parse", rev]);
  return (await readHistory(files)).filter((record) =>
    record.commit === sha && (dirty || !record.dirty)
  );
}

async function main() {
  const [command, ...args] = Deno.args;
  switch (command) {
    case "run": {
      const selections = select(await crates(), positional(args, ["--out"]), [
        "counts",
        "instr",
      ]);
      const records = await runSelected(selections, args.includes("--release"));
      await append(records);
      const out = flag(args, "--out");
      if (out) {
        await Deno.writeTextFile(
          out,
          records.map((record) => JSON.stringify(record)).join("\n") + "\n",
        );
      }
      console.log(`appended ${records.length} records to ${historyFile()}`);
      break;
    }
    case "compare": {
      const [revA, revB] = positional(args, ["--tier", "--history"]);
      if (!revA || !revB) throw new Error("usage: compare <rev-a> <rev-b>");
      const force = args.includes("--force-cross-machine");
      const dir = historyDir();
      const files = force
        ? [...Deno.readDirSync(dir)].filter((entry) =>
          entry.name.endsWith(".jsonl")
        ).map((entry) => `${dir}/${entry.name}`)
        : [flag(args, "--history") ?? historyFile()];
      const dirty = args.includes("--include-dirty");
      const comparison = compare(
        await recordsAt(revA, files, dirty),
        await recordsAt(revB, files, dirty),
        { force },
      );
      printComparison(comparison, () => false);
      if (args.includes("--gate") && failures(comparison).length > 0) {
        Deno.exit(1);
      }
      break;
    }
    case "report": {
      const last = Number(flag(args, "--last") ?? 10);
      const records =
        (await readHistory([flag(args, "--history") ?? historyFile()]))
          .filter((record) =>
            !flag(args, "--bench") ||
            record.bench.includes(flag(args, "--bench")!)
          )
          .filter((record) =>
            !flag(args, "--metric") || record.metric === flag(args, "--metric")
          );
      if (records.length === 0) {
        throw new Error(`no records in ${historyFile()}`);
      }
      const newest = [...records].sort((a, b) => a.ts.localeCompare(b.ts)).at(
        -1,
      )!;
      const commits = (await text([
        "git",
        "log",
        "--first-parent",
        `-${last}`,
        "--format=%H",
        "HEAD",
      ])).split("\n").reverse();
      console.log(
        `basis: ${newest.machine}, ${newest.toolchain}, ${newest.profile}, ${newest.target}; commits oldest first: ${
          commits.map((c) => c.slice(0, 8)).join(" ")
        }`,
      );
      for (
        const trend of trends(records, commits, {
          machine: newest.machine,
          toolchain: newest.toolchain,
          profile: newest.profile,
          target: newest.target,
        })
      ) {
        const cells = trend.steps.map((step) =>
          `${step.commit.slice(0, 8)}=${step.value}${
            step.flagged ? `[${percent(step.relative!)}]` : ""
          }`
        );
        console.log(
          `${trend.bench} ${
            formatParams(trend.params)
          } ${trend.metric} (${trend.unit}): ${cells.join("  ")}`,
        );
      }
      break;
    }
    case "gate": {
      const base = flag(args, "--base");
      if (!base) throw new Error("usage: gate --base <rev> [crate[:tier]...]");
      const selections = select(
        await crates(),
        positional(args, ["--base"]),
        GATED,
      );
      const head = await text(["git", "rev-parse", "HEAD"]);
      const baseSha = await text(["git", "rev-parse", base]);
      if (
        (await text([
          "git",
          "status",
          "--porcelain",
          "--untracked-files=no",
        ])) !== ""
      ) {
        throw new Error(
          "gate checks out other commits: the tree must be clean",
        );
      }
      const messages = await text([
        "git",
        "log",
        "--format=%B",
        `${baseSha}..${head}`,
      ]);
      let baseRecords: BenchRecord[] = [];
      try {
        await run(["git", "checkout", "--detach", baseSha]);
        // Only what the base commit can run is compared: a tier it does not declare yet (the
        // pull request that introduces it) has no base to regress from.
        const declared = await crates();
        const onBase = selections.filter((wanted) =>
          declared.some((entry) =>
            entry.crate === wanted.crate && entry.tier === wanted.tier
          )
        );
        for (
          const skipped of selections.filter((wanted) =>
            !onBase.includes(wanted)
          )
        ) {
          console.error(
            `gate: ${skipped.crate}:${skipped.tier} is new in this change, nothing to compare`,
          );
        }
        baseRecords = await runSelected(onBase, false);
        await append(baseRecords);
      } finally {
        await run(["git", "checkout", "--detach", head]);
      }
      let headRecords: BenchRecord[];
      try {
        headRecords = await runSelected(selections, false);
      } catch (error) {
        // a scaling test timing out is the loudest way a quadratic path shows
        console.log(
          `gate FAILED: the benches did not run to the end at head: ${error}`,
        );
        Deno.exit(1);
      }
      await append(headRecords);
      let failed = false;
      // Scaling needs no base: a count that grows more than linearly with its input fails on
      // the first commit that has it.
      const scaling = scalings(headRecords);
      for (const row of scaling) {
        console.log(
          `scaling ${row.bench} ${row.metric} n=${row.small}->${row.large}: x${
            row.ratio.toFixed(2)
          } (limit x${row.limit})`,
        );
      }
      for (const row of overScaled(scaling)) {
        failed = true;
        console.log(
          `gate FAILED: ${row.bench} ${row.metric} grew x${
            row.ratio.toFixed(2)
          } for a tenfold input (limit x${row.limit})`,
        );
      }
      if (baseRecords.length === 0) {
        console.log("gate: no base measurements, nothing to compare");
      } else {
        const comparison = compare(baseRecords, headRecords);
        const accepts = parseAccepts(messages);
        printComparison(
          comparison,
          (bench) => accepts.some((accept) => accept.pattern.test(bench)),
        );
        for (const accept of accepts) {
          console.log(`Bench-Accept ${accept.glob}: ${accept.reason}`);
        }
        const regressions = failures(comparison, accepts);
        for (const change of regressions) {
          failed = true;
          console.log(
            `gate FAILED: ${change.key.replaceAll("\u0000", " ")} ${
              percent(change.relative)
            } (limit ${
              percent(change.noise)
            }); if intentional, say so in the causing commit: Bench-Accept: <bench-glob> -- <reason>`,
          );
        }
      }
      if (failed) Deno.exit(1);
      console.log("gate passed");
      break;
    }
    default:
      console.error(
        "usage: x/bench.ts run|compare|report|gate (see the header of this file)",
      );
      Deno.exit(2);
  }
}

if (import.meta.main) {
  try {
    await main();
  } catch (error) {
    if (error instanceof CrossBasisError) {
      console.error(`refused: ${error.message}`);
      Deno.exit(3);
    }
    throw error;
  }
}
