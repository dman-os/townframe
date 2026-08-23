#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const clean = $.argv.includes("--clean");
const generateAuthTypes = $.argv.includes("--generate-auth-types");
const witConfigFiles = ["wkg.toml", "wkg.lock"];

const dirs: string[] = [];
for await (const entry of Deno.readDir("src")) {
  if (!entry.isDirectory) continue;

  for (const configFile of witConfigFiles) {
    try {
      await Deno.stat(`src/${entry.name}/${configFile}`);
      dirs.push(entry.name);
      break;
    } catch {
      // Try the next WIT package configuration file.
    }
  }
}
dirs.sort();

if (dirs.length === 0) {
  console.error("no wkg.toml or wkg.lock found under src/");
  Deno.exit(1);
}

for (const dir of dirs) {
  const args = clean ? ["wit", "fetch", "--clean"] : ["wit", "fetch"];
  const crateDir = $.relativeDir(`../src/${dir}/`);

  console.log(`== wash wit fetch in ${dir} ==`);
  await $`wash ${args}`.cwd(crateDir);

  if (dir === "btress_auth") {
    console.log("== patch btress_auth WIT dependencies ==");
    await $`node patch-wit-deps.mjs`.cwd(crateDir);

    if (generateAuthTypes) {
      console.log("== generate btress_auth WIT types ==");
      await $`pnpm run generate:types`.cwd(crateDir);
    }
  }
}
