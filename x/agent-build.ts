#!/usr/bin/env -S deno run --allow-all

// Host-wide build & test admission wrapper for multi-agent worktree development:
// - Creates or reuses a FIFO GNU Make jobserver (`--jobserver-auth=fifo:<path>`)
//   so concurrent Cargo builds across worktrees share a single host-wide token pool.
// - Wraps execution in a per-worktree transient user cgroup scope via
//   `systemd-run --user --scope` (`MemoryHigh` throttles, `MemoryMax` bounds runaway links).
// - Configures `sccache` (when installed and requested with `--sccache`) with
//   explicit `SCCACHE_DIR` on the high-capacity repo filesystem and `SCCACHE_BASEDIRS`.

import { $ } from "./utils.ts";

const DEFAULT_SLOTS = Number(Deno.env.get("TOWNFRAME_JOBSERVER_SLOTS") ?? "12");
const DEFAULT_MEM_HIGH = Deno.env.get("TOWNFRAME_CGROUP_MEMORY_HIGH") ?? "6G";
const DEFAULT_MEM_MAX = Deno.env.get("TOWNFRAME_CGROUP_MEMORY_MAX") ?? "10G";
const DEFAULT_SCCACHE_SIZE = Deno.env.get("SCCACHE_CACHE_SIZE") ?? "8G";

function runtimeFifoPath(): string {
  const explicit = Deno.env.get("TOWNFRAME_JOBSERVER_FIFO");
  if (explicit) return explicit;
  const xdg = Deno.env.get("XDG_RUNTIME_DIR");
  if (xdg) return `${xdg}/townframe-jobserver.fifo`;
  const uid = Deno.uid() ?? 1000;
  return `/tmp/townframe-jobserver-${uid}.fifo`;
}

async function ensureJobserverFifo(
  fifoPath: string,
  slots: number,
): Promise<string> {
  try {
    const stat = await Deno.stat(fifoPath);
    if (!stat.isFifo) {
      await Deno.remove(fifoPath);
    } else {
      return fifoPath;
    }
  } catch {
    // create fresh FIFO below
  }
  await $`mkfifo -m 0600 ${fifoPath}`.printCommand(false);
  const tokens = "+".repeat(Math.max(1, slots - 1));
  const keeper = new Deno.Command("bash", {
    args: [
      "-c",
      `exec 3<>${
        JSON.stringify(fifoPath)
      }; printf '%s' '${tokens}' >&3; sleep infinity`,
    ],
    stdin: "null",
    stdout: "null",
    stderr: "null",
  }).spawn();
  keeper.unref();
  return fifoPath;
}

async function commandExists(cmd: string): Promise<boolean> {
  const res = await $`sh -c ${"command -v " + cmd}`.noThrow().printCommand(
    false,
  ).quiet();
  return res.code === 0;
}

async function resolveSccacheEnv(
  worktreeRoot: string,
): Promise<Record<string, string>> {
  if (!(await commandExists("sccache"))) {
    throw new Error(
      "sccache was requested but is unavailable; enter the Nix shell or remove TOWNFRAME_SCCACHE=1",
    );
  }
  const parentDir = $.path(worktreeRoot).parent()?.toString() ?? worktreeRoot;
  const cacheDir = Deno.env.get("SCCACHE_DIR") ??
    await dotenvValue(worktreeRoot, "SCCACHE_DIR") ??
    $.path(parentDir).join(".sccache-cache").toString();
  const cacheSize = Deno.env.get("SCCACHE_CACHE_SIZE") ??
    await dotenvValue(worktreeRoot, "SCCACHE_CACHE_SIZE") ??
    DEFAULT_SCCACHE_SIZE;
  const baseDirs = Deno.env.get("SCCACHE_BASEDIRS") ??
    await dotenvValue(worktreeRoot, "SCCACHE_BASEDIRS") ??
    parentDir;
  await Deno.mkdir(cacheDir, { recursive: true });
  return {
    RUSTC_WRAPPER: "sccache",
    CARGO_INCREMENTAL: "0",
    SCCACHE_DIR: cacheDir,
    SCCACHE_CACHE_SIZE: cacheSize,
    SCCACHE_BASEDIRS: baseDirs,
  };
}

async function dotenvValue(
  worktreeRoot: string,
  key: string,
): Promise<string | undefined> {
  let text: string;
  try {
    text = await Deno.readTextFile(
      $.path(worktreeRoot).join(".env").toString(),
    );
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) return undefined;
    throw error;
  }
  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    if (line.startsWith("#") || !line.startsWith(`${key}=`)) continue;
    return line.slice(key.length + 1).replace(/^["']|["']$/g, "");
  }
  return undefined;
}

function resolveTargetSubdir(worktree: string): string {
  const explicitSide = Deno.env.get("CONTESTANT_SIDE");
  if (explicitSide === "a" || explicitSide === "b") {
    return `side-${explicitSide}`;
  }
  const match = worktree.match(/^generation-\d+-([ab])$/);
  if (match) {
    return `side-${match[1]}`;
  }
  return worktree;
}

async function resolveCargoTargetDir(
  worktreeRoot: string,
): Promise<string | undefined> {
  const configured = Deno.env.get("CARGO_TARGET_DIR") ??
    await dotenvValue(worktreeRoot, "CARGO_TARGET_DIR");
  if (!configured) return undefined;
  const worktree = $.path(worktreeRoot).basename();
  let absolute = $.path(configured).isAbsolute()
    ? configured
    : $.path(worktreeRoot).join(configured).toString();
  if (absolute === worktreeRoot || absolute.startsWith(`${worktreeRoot}/`)) {
    throw new Error(
      `CARGO_TARGET_DIR ${absolute} is inside the worktree: ` +
        `nix store copies and ntfs small-file churn make every build slower. ` +
        `Point it outside the checkout (see .env).`,
    );
  }
  if (/^generation-\d+-[ab]$/.test($.path(absolute).basename())) {
    absolute = $.path(absolute).parent()!.toString();
  }
  const targetSubdir = resolveTargetSubdir(worktree);
  const shared = Deno.env.get("TOWNFRAME_SHARED_TARGET_DIR") === "1" ||
    $.path(absolute).basename() === targetSubdir;
  const resolved = shared
    ? absolute
    : $.path(absolute).join(targetSubdir).toString();
  await Deno.mkdir(resolved, { recursive: true });
  return resolved;
}

async function printStatus(): Promise<void> {
  const fifo = runtimeFifoPath();
  let fifoReady = false;
  try {
    fifoReady = Boolean((await Deno.stat(fifo)).isFifo);
  } catch {
    fifoReady = false;
  }
  const hasSystemdRun = await commandExists("systemd-run");
  const hasSccache = await commandExists("sccache");
  console.log(
    JSON.stringify(
      {
        jobserver_fifo: fifo,
        jobserver_ready: fifoReady,
        default_slots: DEFAULT_SLOTS,
        systemd_run_available: hasSystemdRun,
        memory_high: DEFAULT_MEM_HIGH,
        memory_max: DEFAULT_MEM_MAX,
        sccache_available: hasSccache,
        sccache_opt_in: (Deno.env.get("TOWNFRAME_SCCACHE") ??
          await dotenvValue(Deno.cwd(), "TOWNFRAME_SCCACHE")) === "1",
        cargo_target_dir: await resolveCargoTargetDir(Deno.cwd()),
      },
      null,
      2,
    ),
  );
}

async function main(): Promise<void> {
  const args = [...Deno.args];
  if (args.length === 0 || args[0] === "--status") {
    await printStatus();
    return;
  }
  if (args[0] === "--init-jobserver") {
    const fifo = await ensureJobserverFifo(runtimeFifoPath(), DEFAULT_SLOTS);
    console.log(`export MAKEFLAGS="--jobserver-auth=fifo:${fifo}"`);
    return;
  }

  const configuredSccache = Deno.env.get("TOWNFRAME_SCCACHE") ??
    await dotenvValue(Deno.cwd(), "TOWNFRAME_SCCACHE");
  let useSccache = configuredSccache === "1";
  let sccacheOverride: boolean | undefined;
  let useCgroup = true;
  while (args.length > 0 && args[0].startsWith("--")) {
    const flag = args.shift()!;
    if (flag === "--") break;
    if (flag === "--sccache") {
      sccacheOverride = true;
    } else if (flag === "--no-sccache") {
      sccacheOverride = false;
    } else if (flag === "--no-cgroup") {
      useCgroup = false;
    } else {
      throw new Error(`unknown option: ${flag}`);
    }
  }
  if (args.length === 0) {
    throw new Error(
      "usage: x/agent-build.ts [--status | --init-jobserver | [--sccache|--no-sccache] [--no-cgroup] -- <cmd...>]",
    );
  }

  if (sccacheOverride !== undefined) useSccache = sccacheOverride;

  const cwd = Deno.cwd();
  const fifo = await ensureJobserverFifo(runtimeFifoPath(), DEFAULT_SLOTS);
  const existingMakeflags = Deno.env.get("MAKEFLAGS") ?? "";
  const makeflags = existingMakeflags.includes("--jobserver-auth=")
    ? existingMakeflags
    : `${existingMakeflags} --jobserver-auth=fifo:${fifo}`.trim();

  const sccacheEnv = useSccache
    ? await resolveSccacheEnv(cwd)
    : sccacheOverride === false
    ? { RUSTC_WRAPPER: "", CARGO_INCREMENTAL: "1" }
    : {};
  const extraEnv: Record<string, string> = {
    MAKEFLAGS: makeflags,
    ...sccacheEnv,
  };
  const targetDir = await resolveCargoTargetDir(cwd);
  if (targetDir) extraEnv.CARGO_TARGET_DIR = targetDir;

  let fullCmd = args;
  if (useCgroup && (await commandExists("systemd-run"))) {
    const worktreeName = $.path(cwd).basename().replace(
      /[^a-zA-Z0-9_-]/g,
      "-",
    );
    fullCmd = [
      "systemd-run",
      "--user",
      "--scope",
      "--quiet",
      `--unit=townframe-${worktreeName}-${Deno.pid}`,
      `-p`,
      `MemoryHigh=${DEFAULT_MEM_HIGH}`,
      `-p`,
      `MemoryMax=${DEFAULT_MEM_MAX}`,
      "--",
      ...args,
    ];
  }

  const child = new Deno.Command(fullCmd[0], {
    args: fullCmd.slice(1),
    cwd,
    env: { ...Deno.env.toObject(), ...extraEnv },
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  }).spawn();
  const status = await child.status;
  Deno.exit(status.code);
}

await main();
