#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const repoRoot = $.relativeDir("../");
const generatedOutDir = $.relativeDir(
  "../src/daybook_compose/composeApp/src/commonMain/kotlin/",
).toString();
const targetMeta = await $`cargo metadata --format-version 1 --no-deps`.cwd(
  repoRoot,
).json();
const targetDir = $.path(targetMeta.target_directory);
const generatedLibraryPath = targetDir
  .join("debug", "libdaybook_ffi.so")
  .toString();

await $`cargo build -p daybook_ffi`.cwd(repoRoot);
await $`cargo run -p daybook_ffi generate --library ${generatedLibraryPath} --language kotlin --out-dir ${generatedOutDir} --no-format`
  .cwd(
    repoRoot,
  );
