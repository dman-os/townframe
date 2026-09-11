#!/usr/bin/env -S deno run --allow-all

/**
 * Fetches the upstream Willow test vectors used by the `big_willow` conformance
 * suite.
 *
 * Why this is a script and not a git submodule: this repo is jj-managed and jj
 * has no submodule support. The vectors are therefore fetched into a gitignored
 * directory, which jj never sees, and pinned to an exact revision so the suite
 * is reproducible. Re-running is idempotent.
 *
 * Only the suites `big_willow` actually consumes are checked out. The `dbg`
 * directories alone are most of the repository's size and are only useful for
 * reading failures by hand.
 *
 * Usage:
 *   ./x/willow-vectors.ts [--rev <git-rev>] [--full]
 *
 * `--full` checks out the whole repository instead of the consumed suites.
 */

import { $ } from "./utils.ts";

const REPO = "https://codeberg.org/worm-blossom/willow_test_vectors.git";

/**
 * The revision the conformance suite is pinned to. The `data_model` suite only
 * exists in the Codeberg repository; the GitHub mirror's `main` predates it.
 * This is the same revision `willow-ts` pins.
 */
const DEFAULT_REV = "68117e0b07d451364f3e7361d9b3946bdcb12890";

/** Relative to the repository root, which is also the `$` working directory. */
const DEST = "src/big_willow/testdata/willow_test_vectors";

/** Suites the conformance suite reads. */
const SUITES = [
  // Codec vectors come first: every other vector is expressed through them, so
  // a failure here invalidates everything downstream.
  "codec/EncodeMeadowcapAuthorisedEntry",
  "codec/EncodeMeadowcapAuthorisationToken",
  "codec/encode_path",
  "codec/encode_entry",
  // Store semantics.
  "data_model/store_pruning",
  "data_model/entry_prunes",
  "data_model/entry_is_pruned_by",
  "data_model/entry_is_newer_than",
  // Path ordering, and the spec's own prefix-successor operation.
  "data_model/path_is_prefix_of",
  "data_model/path_least_path_lexicographically_greater_but_not_prefixed_by_original",
];

/** The repository root, as a URL, for the filesystem checks below. */
const ROOT = new URL("../", import.meta.resolve("./utils.ts"));

function isDir(relative: string): boolean {
  try {
    return Deno.statSync(new URL(relative, ROOT)).isDirectory;
  } catch {
    return false;
  }
}

function revFromArgs(): string {
  const i = $.argv.indexOf("--rev");
  if (i === -1) return DEFAULT_REV;
  const rev = $.argv[i + 1];
  if (!rev) throw new Error("--rev requires a value");
  return rev;
}

const rev = revFromArgs();
const full = $.argv.includes("--full");

if (!isDir(`${DEST}/.git`)) {
  await $`git init ${DEST}`;
}

// `git remote` writes one remote name per line; the shell commands run with the
// repository root as their working directory, so `DEST` is relative to it.
const remotes = (await $`git -C ${DEST} remote`.stdout("piped")).stdout
  .split("\n")
  .map((line) => line.trim());
if (remotes.includes("origin")) {
  await $`git -C ${DEST} remote set-url origin ${REPO}`;
} else {
  await $`git -C ${DEST} remote add origin ${REPO}`;
}

await $`git -C ${DEST} fetch --depth 1 origin ${rev}`;
await $`git -C ${DEST} checkout --detach FETCH_HEAD`;

// Sparse patterns are applied after the checkout so they resolve against a real
// commit. On a re-run, `set` plus `reapply` re-materialises them.
if (!full) {
  await $`git -C ${DEST} sparse-checkout init --cone`;
  await $`git -C ${DEST} sparse-checkout set ${SUITES}`;
  await $`git -C ${DEST} sparse-checkout reapply`;
}

if (!isDir(`${DEST}/codec`)) {
  throw new Error(`expected ${DEST}/codec after checkout; got an unexpected corpus layout`);
}
if (!isDir(`${DEST}/data_model`)) {
  throw new Error(`expected ${DEST}/data_model after checkout; the pinned revision is too old`);
}

const pruning = (await $`ls ${DEST}/data_model/store_pruning/input`.stdout("piped")).stdout
  .split("\n")
  .filter((line) => line.trim().length > 0).length;

console.log(`willow vectors at ${DEST}`);
console.log(`  revision ${rev}${full ? " (full checkout)" : ""}`);
console.log(`  store_pruning vectors: ${pruning}`);
console.log("\nRun the suite with:\n  cargo nextest run -p big_willow --features conformance");
