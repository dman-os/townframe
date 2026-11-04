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

// wit-bindgen's `std` feature moves the async export task state into a libc thread-local that
// is not swapped per component-model task, so a guest could hold only one suspended call.
// `clay_mdk` swaps that state itself through symbols named after the wit-bindgen version
// (see its `task_hook`), which only works with `std` off; any dependency's default features
// can turn it back on, so check what every wasip3 guest closure resolves to. The guest
// workspace has its own lockfile and resolution.
const witBindgen = ["-e", "features", "-i", "wit-bindgen@0.62.0"];
const resolutions = [
  ...["micro_counter", "micro_layout", "micro_collection_probe", "clay_mdk"]
    .map((guest) =>
      $`cargo tree --locked --offline ${isolated} -p ${guest} --target wasm32-wasip3 ${witBindgen}`
        .text()
    ),
  $`cargo tree --locked --offline --manifest-path src/wasm_utils_rs/Cargo.toml --workspace --target wasm32-wasip3 ${witBindgen}`
    .text(),
];
for (const resolved of await Promise.all(resolutions)) {
  if (resolved.includes('feature "std"')) {
    throw new Error(
      `wit-bindgen resolves with its \`std\` feature on a wasip3 guest; some dependency enables its default features:\n${resolved}`,
    );
  }
}

await $`cargo build --locked ${isolated} -p micro_counter --target wasm32-wasip3`;
await $`cargo build --locked ${isolated} -p micro_layout --target wasm32-wasip3`;
await $`cargo build --locked ${isolated} -p micro_collection_probe --target wasm32-wasip3`;
await $`cargo build --locked ${isolated} -p clay_mdk --example sleeper --target wasm32-wasip3`;
await $`cargo build --locked ${isolated} -p clay_mdk --example lifecycle --target wasm32-wasip3`;
await $`cargo build --locked ${isolated} -p clay_mdk --example context --target wasm32-wasip3`;
const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
  .json<{ target_directory: string; packages: { name: string }[] }>();
const target = $.path(metadata.target_directory);
const directory = target.join("dayframe");
await Deno.mkdir(directory.toString(), { recursive: true });

// Prepared guest components, keyed by the guest name `component_package`/env vars use.
const components: Record<string, [string, string]> = {
  counter: ["wasm32-wasip3/debug/micro_counter.wasm", "counter"],
  layout: ["wasm32-wasip3/debug/micro_layout.wasm", "layout"],
  collectionProbe: [
    "wasm32-wasip3/debug/micro_collection_probe.wasm",
    "collection-probe",
  ],
  sleeper: ["wasm32-wasip3/debug/examples/sleeper.wasm", "sleeper"],
  lifecycle: ["wasm32-wasip3/debug/examples/lifecycle.wasm", "lifecycle"],
  context: ["wasm32-wasip3/debug/examples/context.wasm", "context"],
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

// The counter and the layout are also packages: the desktop shell scans a package directory for
// `<name>.manifest.json` sidecars paired with `<name>.component.wasm`, so prepare that
// directory beside the raw fixtures. STAND-IN until `big_plugs` delivers manifests; each
// sidecar is emitted from the package's manifest crate.
const packagesDirectory = directory.join("packages");
await Deno.mkdir(packagesDirectory.toString(), { recursive: true });
for (
  const [name, manifestCrate] of [
    ["counter", "micro_counter_manifest"],
    ["layout", "micro_layout_manifest"],
  ]
) {
  await $`cp ${directory.join(`${name}.component.wasm`)} ${
    packagesDirectory.join(`${name}.component.wasm`)
  }`;
  await $`cargo run --locked -q -p ${manifestCrate} --example emit_manifest -- ${
    packagesDirectory.join(`${name}.manifest.json`)
  }`;
}
componentEnv["DAYFRAME_PACKAGE_DIR"] = packagesDirectory.toString();
console.log(`DAYFRAME_PACKAGE_DIR=${packagesDirectory}`);

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
