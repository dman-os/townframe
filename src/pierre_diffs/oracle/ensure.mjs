// Installs the pinned dependency closure the oracle runs against and returns
// file URLs into it.
//
// package-lock.json (checked in next to this file) pins every package,
// including @pierre/diffs 1.6.4 and diff 9.0.0, by sha512 integrity. The
// closure is installed with `npm ci --ignore-scripts` into a cache directory
// outside the checkout, so bare imports inside `@pierre/diffs/dist` (`diff`,
// `shiki`, `hast-util-to-html`, ...) resolve exactly as in a real install.
// pins.json additionally records the integrity strings of the two packages
// the goldens are about; ensure() fails if the lockfile disagrees.
//
// Cache directory: $PIERRE_ORACLE_CACHE, else $XDG_CACHE_HOME/pierre_diffs_oracle,
// else ~/.cache/pierre_diffs_oracle.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

export const pins = JSON.parse(readFileSync(join(here, "pins.json"), "utf8"));

function cacheRoot() {
  if (process.env.PIERRE_ORACLE_CACHE) {
    return process.env.PIERRE_ORACLE_CACHE;
  }
  const base = process.env.XDG_CACHE_HOME ?? join(homedir(), ".cache");
  return join(base, "pierre_diffs_oracle");
}

function checkLockAgainstPins(lock) {
  for (const name of [
    "@pierre/diffs",
    "diff",
    "@pierre/highlights",
    "@pierre/theming",
    "jsdom",
  ]) {
    const locked = lock.packages[`node_modules/${name}`];
    if (
      locked?.version !== pins[name].version ||
      locked?.integrity !== pins[name].npmIntegrity
    ) {
      throw new Error(`package-lock.json disagrees with pins.json for ${name}`);
    }
  }
}

/** Returns file URLs for the pinned `@pierre/diffs` dist and `diff` packages. */
export function ensureOracle() {
  const root = cacheRoot();
  const lockText = readFileSync(join(here, "package-lock.json"), "utf8");
  checkLockAgainstPins(JSON.parse(lockText));
  const stamp = join(root, ".oracle-lock-sha256");
  const lockDigest = createHash("sha256").update(lockText).digest("hex");
  if (!existsSync(stamp) || readFileSync(stamp, "utf8") !== lockDigest) {
    mkdirSync(root, { recursive: true });
    copyFileSync(join(here, "package.json"), join(root, "package.json"));
    copyFileSync(
      join(here, "package-lock.json"),
      join(root, "package-lock.json"),
    );
    execFileSync("npm", ["ci", "--ignore-scripts", "--no-audit", "--no-fund"], {
      cwd: root,
      stdio: "inherit",
    });
    writeFileSync(stamp, lockDigest);
  }
  const modules = join(root, "node_modules");
  return {
    pierre: (relative) =>
      pathToFileURL(join(modules, "@pierre/diffs/dist", relative)).href,
    jsdiff: pathToFileURL(join(modules, "diff/libesm/index.js")).href,
    jsdom: pathToFileURL(join(modules, "jsdom/lib/api.js")).href,
    modules,
  };
}
