#!/usr/bin/env -S deno run --allow-all

import $ from "jsr:@david/dax@0.45.0";

const root = $.path(new URL("../", import.meta.url));
const cargoToml = await root.join("Cargo.toml").readText();

const patchSection = cargoToml.match(
  /\[patch\.crates-io\]([\s\S]*?)(?=\n\[|$)/,
)?.[1];
if (!patchSection) {
  throw new Error("Cargo.toml is missing [patch.crates-io]");
}

const dioxusPatchMatch = patchSection.match(
  /^dioxus\s*=\s*\{([^}\n]*)\}/m,
);
if (!dioxusPatchMatch) {
  throw new Error("Cargo.toml [patch.crates-io] is missing dioxus");
}
const dioxusPatch = dioxusPatchMatch[1];

function patchString(name: string): string {
  const value = dioxusPatch.match(new RegExp(`\\b${name}\\s*=\\s*"([^"]+)"`))?.[1];
  if (!value) {
    throw new Error(`Cargo.toml dioxus patch is missing ${name}`);
  }
  return value;
}

const repository = patchString("git");
const revision = patchString("rev");
const installRoot = Deno.env.get("DIOXUS_CLI_ROOT")
  ? $.path(Deno.env.get("DIOXUS_CLI_ROOT")!)
  : root.join("target", "dioxus-cli");
const executableName = Deno.build.os === "windows" ? "dx.exe" : "dx";
const executable = installRoot.join("bin", executableName);

await $`cargo install --locked --git ${repository} --rev ${revision} dioxus-cli --features no-downloads --root ${installRoot} --force --debug`.cwd(root);

await $`${executable} --version`;

$.logLight(`Installed Dioxus CLI ${revision} at ${executable}`);
