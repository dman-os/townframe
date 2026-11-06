// Fetches the pinned npm tarballs the oracle runs against, verifies their
// sha256 against `pins.json`, and unpacks them into a node_modules layout
// outside the checkout so bare imports inside `@pierre/diffs/dist` (`diff`)
// resolve exactly as they do for a real install.
//
// Cache directory: $PIERRE_ORACLE_CACHE, else $XDG_CACHE_HOME/pierre_diffs_oracle,
// else ~/.cache/pierre_diffs_oracle. A tarball whose digest does not match the
// pin is never unpacked.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
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

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

async function ensurePackage(root, name, pin) {
  const target = join(root, "node_modules", ...name.split("/"));
  const stamp = join(target, ".oracle-sha256");
  if (existsSync(stamp) && readFileSync(stamp, "utf8") === pin.sha256) {
    return target;
  }
  const response = await fetch(pin.tarball);
  if (!response.ok) {
    throw new Error(`fetching ${pin.tarball}: HTTP ${response.status}`);
  }
  const bytes = Buffer.from(await response.arrayBuffer());
  const actual = sha256Hex(bytes);
  if (actual !== pin.sha256) {
    throw new Error(
      `${name}@${pin.version}: tarball sha256 ${actual} != pinned ${pin.sha256}`,
    );
  }
  rmSync(target, { recursive: true, force: true });
  mkdirSync(target, { recursive: true });
  const tarballPath = join(root, `${name.replace("/", "__")}.tgz`);
  writeFileSync(tarballPath, bytes);
  // npm tarballs wrap everything in a top-level `package/` directory.
  execFileSync("tar", [
    "-xzf",
    tarballPath,
    "-C",
    target,
    "--strip-components=1",
  ]);
  writeFileSync(stamp, pin.sha256);
  return target;
}

/** Returns file URLs for the pinned `@pierre/diffs` dist and `diff` packages. */
export async function ensureOracle() {
  const root = cacheRoot();
  mkdirSync(root, { recursive: true });
  const diffsDir = await ensurePackage(
    root,
    "@pierre/diffs",
    pins["@pierre/diffs"],
  );
  const jsdiffDir = await ensurePackage(root, "diff", pins.diff);
  return {
    pierre: (relative) => pathToFileURL(join(diffsDir, "dist", relative)).href,
    jsdiff: pathToFileURL(join(jsdiffDir, "libesm", "index.js")).href,
  };
}
