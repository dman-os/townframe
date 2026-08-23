#!/usr/bin/env -S deno run --allow-all

// Disk-space watchdog: loops every 5s checking free space on
// the repo filesystem. When free drops below THRESHOLD_GIB and no rust build is
// running, runs `x/clean-rust.ts`. If clean-rust doesn't recover to at least
// OK_GIB, removes `<cargo target>/debug`. If that still doesn't reach OK_GIB,
// errors out (uncaught throw → non-zero exit) so the failure is noticed instead
// of silently churning.
//
// Run in the background, e.g.:
//   setsid deno run --allow-all x/disk-watch.ts >/tmp/disk-watch.log 2>&1 </dev/null & disown
//
// Disk pressure has killed async subagent runners (non-recoverable); this keeps
// space available without an LLM in the loop.

import { $ } from "./utils.ts";

const THRESHOLD_GIB = 4; // trigger cleanup below this
const OK_GIB = 10; // cleanup must recover to at least this
const INTERVAL_MS = 5000;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function freeGib(): Promise<number> {
  const out = await $`df --output=avail -B1G .`.printCommand(false).text();
  const last = out.trim().split("\n").at(-1);
  if (last === undefined) throw new Error("df returned no output");
  return parseInt(last.trim(), 10);
}

async function buildRunning(): Promise<boolean> {
  // Detect an active rust build. Excludes `cargo clean` (which clean-rust runs)
  // and this script's own deno process.
  const out = await $`ps -ef`.text();
  return /cargo (clippy|build|check|test|run)\b|rustc/.test(out);
}

async function targetDebugDir(): Promise<string> {
  const env = $.env.CARGO_TARGET_DIR;
  let base;
  if (env) {
    base = $.path(env);
  } else {
    let meta;
    try {
      meta = JSON.parse(
        await $`cargo metadata --no-deps --format-version 1`.text(),
      );
    } catch (e) {
      throw new Error(`failed to read cargo metadata for target dir: ${e}`);
    }
    base = $.path(meta.target_directory as string);
  }
  return base.join("debug").toString();
}

async function runCleanRust(): Promise<void> {
  await $`deno run --allow-all x/clean-rust.ts`;
}

// Returns a fatal Error if disk could not be recovered to >= OK_GIB, else null.
async function recover(): Promise<Error | null> {
  const before = await freeGib();
  console.log(
    `[disk-watch] ${before} GiB free (<${THRESHOLD_GIB}) — running clean-rust`,
  );
  try {
    await runCleanRust();
  } catch (e) {
    console.error(`[disk-watch] clean-rust failed: ${e}`);
  }

  let now = await freeGib();
  if (now >= OK_GIB) {
    console.log(`[disk-watch] clean-rust recovered to ${now} GiB`);
    return null;
  }

  const dbg = await targetDebugDir();
  // Safety guard: only remove a path that is absolute and ends in /debug.
  if (!dbg.startsWith("/") || !dbg.endsWith("/debug")) {
    return new Error(
      `[disk-watch] refusing to remove unexpected target/debug path: ${dbg}`,
    );
  }
  console.log(
    `[disk-watch] only ${now} GiB after clean-rust (<${OK_GIB}) — removing ${dbg}`,
  );
  try {
    await $`rm -rf ${dbg}`;
  } catch (e) {
    console.error(`[disk-watch] rm target/debug failed: ${e}`);
  }

  now = await freeGib();
  if (now >= OK_GIB) {
    console.log(`[disk-watch] target/debug removal recovered to ${now} GiB`);
    return null;
  }

  return new Error(
    `[disk-watch] CRITICAL: disk still ${now} GiB (<${OK_GIB}) after clean-rust + target/debug removal — erroring out`,
  );
}

console.log(
  `[disk-watch] every ${
    INTERVAL_MS / 1000
  }s; clean when <${THRESHOLD_GIB} GiB, must recover to >=${OK_GIB} GiB`,
);

let fatal: Error | null = null;
while (!fatal) {
  try {
    const avail = await freeGib();
    if (avail < THRESHOLD_GIB) {
      if (await buildRunning()) {
        console.log(
          `[disk-watch] ${avail} GiB free — build running, skipping clean`,
        );
      } else {
        fatal = await recover();
      }
    }
  } catch (e) {
    console.error(`[disk-watch] iteration failed: ${e}`);
  }
  if (fatal) break;
  await sleep(INTERVAL_MS);
}

if (fatal) {
  console.error(fatal.message);
  throw fatal;
}
