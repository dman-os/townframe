#!/usr/bin/env -S deno run --allow-all

import { prepareDayframePackages } from "./dayframe-packages.ts";
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
// The trees below are `--offline`, so every git rev and crate must already be in the package
// cache; fetch first (sequentially) so a fresh runner does not die with "you are in the offline
// mode", and so the parallel trees never race on the package-cache lock.
await $`cargo fetch --locked`;
await $`cargo fetch --locked --manifest-path src/wasm_utils_rs/Cargo.toml`;
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

const componentEnv = await prepareDayframePackages({ testFixtures: true });

if (!prepareOnly) {
  const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
    .json<{ packages: { name: string }[] }>();
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
