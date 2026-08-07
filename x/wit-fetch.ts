#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const clean = $.argv.includes("--clean");

const dirs: string[] = [];
for await (const entry of Deno.readDir("src")) {
  if (!entry.isDirectory) continue;
  try {
    await Deno.stat(`src/${entry.name}/wkg.toml`);
    dirs.push(entry.name);
  } catch {
    // no wkg.toml in this crate
  }
}

if (dirs.length === 0) {
  console.error("no wkg.toml found under src/");
  Deno.exit(1);
}

for (const dir of dirs) {
  const args = clean ? ["wit", "fetch", "--clean"] : ["wit", "fetch"];
  console.log(`== wash wit fetch in ${dir} ==`);
  await $`wash ${args}`.cwd($.relativeDir(`../src/${dir}/`));
}
