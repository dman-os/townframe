#!/usr/bin/env -S deno run --allow-all

import { $, useDotenvTargetDir } from "./utils.ts";

const prepareOnly = Deno.args.includes("--prepare-only");
if (prepareOnly && Deno.args.length !== 1) {
  throw new Error("--prepare-only cannot be combined with nextest arguments");
}

await useDotenvTargetDir();

// Guests are wasm-only configurations: resolve their features in isolation so the root
// workspace's native closure (tokio/net, mio) does not leak into the wasm builds. See
// CONTRIBUTING.md on `resolver.feature-unification`.
const isolated = ["--config", 'resolver.feature-unification="selected"'];
await $`cargo build --locked ${isolated} -p micro_counter --target wasm32-wasip3`;
await $`cargo build --locked ${isolated} -p clay_mdk --example slots --target wasm32-wasip3`;
const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
  .json<{ target_directory: string; packages: { name: string }[] }>();
const target = $.path(metadata.target_directory);
const directory = target.join("dayframe");
await Deno.mkdir(directory.toString(), { recursive: true });

// Prepared guest components, keyed by the guest name `component_package`/env vars use.
const components: Record<string, [string, string]> = {
  counter: ["wasm32-wasip3/debug/micro_counter.wasm", "counter"],
  slots: ["wasm32-wasip3/debug/examples/slots.wasm", "slots"],
};
const componentEnv: Record<string, string> = {};
for (const name in components) {
  const [relative, variableName] = components[name];
  const artifact = directory.join(`${variableName}.component.wasm`);
  // rustc's wasm32-wasip3 target already emits a component; no adapter step.
  await $`cp ${target.join(relative)} ${artifact}`;
  const variable = `DAYFRAME_${
    variableName.toUpperCase().replaceAll("-", "_")
  }_COMPONENT`;
  componentEnv[variable] = artifact.toString();
  console.log(`${variable}=${artifact}`);
}

if (!prepareOnly) {
  // Every clay_*/micro_*/dayframe_* workspace crate is gated; a new micro crate joins by name,
  // no allowlist edit.
  const packages = metadata.packages
    .map((pkg) => pkg.name)
    .filter((name) => /^(clay|micro|dayframe)_/.test(name))
    .flatMap((name) => ["-p", name]);
  await $`cargo nextest run --locked ${packages} ${Deno.args}`.env(
    componentEnv,
  );
  await $`cargo clippy --locked --no-deps --all-targets --all-features ${packages}`
    .env(componentEnv);
}
